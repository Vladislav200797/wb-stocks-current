#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт: Загрузка текущих остатков Wildberries в Supabase (snapshot replace)
----------------------------------------------------------------------------
1) Создаёт отчёт WB (категория "Аналитика")
2) Ждёт готовность
3) Скачивает
4) Разворачивает по складам
5) UPSERT всего набора
6) Удаляет все строки не из текущего прогона (снимок без "призраков")

ENV (обязательные):
  WB_ANALYTICS_TOKEN
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY

ENV (опциональные, по умолчанию True):
  EXCLUDE_TOTAL_ROW=true|false          -- пропускать "Всего находится на складах"
  EXCLUDE_IN_TRANSIT=true|false         -- пропускать "В пути ..."
  EXCLUDE_RETURNS=true|false            -- пропускать "возвраты"

  SUPABASE_TABLE=wb_stocks_current
  LOG_LEVEL=INFO

requirements.txt:
  requests>=2.32.0
  supabase>=2.5.0
"""

import os
import sys
import time
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List

import requests
from supabase import create_client, Client


# ---------------------- Логирование ----------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ---------------------- Константы ------------------------

WB_BASE = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
POLL_INTERVAL_SEC = 5
POLL_TIMEOUT_SEC = 10 * 60   # максимум 10 минут
SESSION = requests.Session()
SESSION_TIMEOUT = (10, 90)
UPSERT_BATCH = 1000

TABLE = os.getenv("SUPABASE_TABLE", "wb_stocks_current")

# Флаги фильтрации (по умолчанию исключаем итоги/в пути/возвраты)
def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y")

EXCLUDE_TOTAL_ROW = _env_bool("EXCLUDE_TOTAL_ROW", True)
EXCLUDE_IN_TRANSIT = _env_bool("EXCLUDE_IN_TRANSIT", True)
EXCLUDE_RETURNS = _env_bool("EXCLUDE_RETURNS", True)


# ---------------------- Утилиты --------------------------

def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Required environment variable {name} is missing")
    return v.strip()


def batched(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for x in iterable:
        batch.append(x)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def retry_request(method, url, *, retries=3, backoff=2.0, **kwargs) -> requests.Response:
    """
    Повторяет запрос при сетевых/временных ошибках
    """
    attempt = 0
    while True:
        try:
            r = SESSION.request(method, url, timeout=SESSION_TIMEOUT, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}: {r.text}", response=r)
            return r
        except Exception as e:
            attempt += 1
            if attempt > retries:
                raise
            sleep_for = backoff ** (attempt - 1)
            log.warning("Request error (%s). Retry %d/%d in %.1fs", e, attempt, retries, sleep_for)
            time.sleep(sleep_for)


# ---------------------- API WB ---------------------------

def wb_headers(token: str) -> Dict[str, str]:
    return {"Authorization": token.strip()}


def wb_create_report(token: str) -> str:
    params = {
        "locale": "ru",
        "groupByNm": "true",
        "groupByBarcode": "true",
        "groupBySize": "true",
        "groupBySa": "true",   # нужен vendorCode
    }
    r = retry_request("GET", WB_BASE, headers=wb_headers(token), params=params)
    if r.status_code != 200:
        raise RuntimeError(f"WB create report failed: {r.status_code} {r.text}")
    task_id = r.json()["data"]["taskId"]
    log.info("Создан отчёт WB: %s", task_id)
    return task_id


def wb_wait_done(token: str, task_id: str) -> None:
    url = f"{WB_BASE}/tasks/{task_id}/status"
    deadline = time.time() + POLL_TIMEOUT_SEC
    while time.time() < deadline:
        r = retry_request("GET", url, headers=wb_headers(token))
        data = r.json().get("data", {})
        status = str(data.get("status", "")).lower()
        if status == "done":
            log.info("Отчёт WB готов")
            return
        elif status in ("failed", "error"):
            raise RuntimeError(f"WB report failed: {data}")
        log.debug("Статус WB: %s — ждём %ds", status or "unknown", POLL_INTERVAL_SEC)
        time.sleep(POLL_INTERVAL_SEC)
    raise TimeoutError("WB report generation timed out")


def wb_download(token: str, task_id: str) -> List[Dict[str, Any]]:
    url = f"{WB_BASE}/tasks/{task_id}/download"
    r = retry_request("GET", url, headers=wb_headers(token))
    if r.status_code != 200:
        raise RuntimeError(f"WB download failed: {r.status_code} {r.text}")
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Unexpected WB payload format")
    log.info("Получено %d строк из WB", len(data))
    return data


# ---------------------- Преобразование --------------------

def _is_total(name: str) -> bool:
    return isinstance(name, str) and name == "Всего находится на складах"

def _is_in_transit(name: str) -> bool:
    return isinstance(name, str) and name.startswith("В пути")

def _is_returns(name: str) -> bool:
    return isinstance(name, str) and ("возврат" in name.lower())

def flatten_rows(raw: List[Dict[str, Any]], fetched_at: dt.datetime) -> List[Dict[str, Any]]:
    """
    Разворачивает массив warehouses в строки. По умолчанию
    не возвращает "Всего находится на складах", "В пути ...", "возвраты ...".
    Всё настраивается флагами EXCLUDE_*.
    """
    rows: List[Dict[str, Any]] = []
    fetched_iso = fetched_at.replace(microsecond=0).isoformat() + "Z"

    for item in raw:
        brand = item.get("brand")
        subject = item.get("subjectName")
        vendor_code = item.get("vendorCode")      # артикул поставщика
        nm_id = item.get("nmId")
        barcode = item.get("barcode")
        tech_size = item.get("techSize")
        volume = item.get("volume")

        for wh in item.get("warehouses", []) or []:
            wh_name = str(wh.get("warehouseName") or "")
            if EXCLUDE_TOTAL_ROW and _is_total(wh_name):
                continue
            if EXCLUDE_IN_TRANSIT and _is_in_transit(wh_name):
                continue
            if EXCLUDE_RETURNS and _is_returns(wh_name):
                continue

            rows.append({
                "fetched_at": fetched_iso,
                "brand": brand,
                "subject_name": subject,
                "vendor_code": vendor_code,
                "nm_id": nm_id,
                "barcode": barcode,
                "tech_size": tech_size,
                "volume_l": volume,
                "warehouse_name": wh_name,
                "quantity": int(wh.get("quantity") or 0)
            })

    log.info("Преобразовано %d строк для записи (после фильтров)", len(rows))
    return rows


# ---------------------- Supabase --------------------------

def supabase_client() -> Client:
    url = getenv_required("SUPABASE_URL")
    key = getenv_required("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def upsert_rows(client: Client, table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        log.info("Нет данных для обновления")
        return
    t = client.table(table)
    total = 0
    for chunk in batched(rows, UPSERT_BATCH):
        t.upsert(chunk, on_conflict="nm_id,barcode,tech_size,warehouse_name").execute()
        total += len(chunk)
    log.info("Upsert завершён: %d строк обновлено/добавлено", total)


def delete_previous_runs(client: Client, table: str, run_ts_iso: str) -> None:
    """
    Удаляем все строки, которые не относятся к текущему прогону (snapshot replace).
    Так мы не оставляем "призраков" нулевых складов и др. устаревших строк.
    """
    log.info("Удаляем строки не из текущего прогона (fetched_at <> %s)", run_ts_iso)
    client.table(table).delete().neq("fetched_at", run_ts_iso).execute()


# ---------------------- MAIN ------------------------------

def main() -> int:
    try:
        wb_token = getenv_required("WB_ANALYTICS_TOKEN")
        supa = supabase_client()

        task_id = wb_create_report(wb_token)
        wb_wait_done(wb_token, task_id)
        raw = wb_download(wb_token, task_id)

        # Штамп прогона (единый для всех записей этого запуска)
        fetched_at = dt.datetime.utcnow().replace(microsecond=0)
        run_ts_iso = fetched_at.isoformat() + "Z"

        # Формируем строки с фильтрами
        rows = flatten_rows(raw, fetched_at)

        # 1) Записываем (upsert по ключу nm_id,barcode,tech_size,warehouse_name)
        upsert_rows(supa, TABLE, rows)

        # 2) Стираем всё, что не из этого прогона (snapshot без "призраков")
        delete_previous_runs(supa, TABLE, run_ts_iso)

        log.info("✅ Готово. task_id=%s, строк записано=%d. Снимок: %s", task_id, len(rows), run_ts_iso)
        return 0

    except Exception as e:
        log.exception("❌ Ошибка при выполнении: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
