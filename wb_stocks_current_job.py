#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WB -> Supabase: текущие остатки (replace-in-place)
Требует таблицу public.wb_stocks_current с уникальностью:
  unique (nm_id, barcode, tech_size, warehouse_name)

ENV:
  WB_ANALYTICS_TOKEN             — токен категории «Аналитика» (WB)
  SUPABASE_URL                   — https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY      — service role key (рекомендуется)
  SUPABASE_TABLE                 — имя таблицы (по умолчанию wb_stocks_current)

requirements.txt:
  requests>=2.32.0
  supabase>=2.5.0
"""

import os
import sys
import time
import json
import math
import hashlib
import logging
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from supabase import create_client, Client  # supabase-py v2

# --------------------------- Логирование ---------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# --------------------------- Константы -----------------------------

WB_BASE = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
SESSION = requests.Session()
SESSION_TIMEOUT = (10, 90)  # connect, read

# лимиты WB:
POLL_INTERVAL_SEC = 5       # статус: 1 запрос / 5 сек
POLL_TIMEOUT_SEC = 10 * 60  # макс ожидание генерации отчёта, 10 мин

# вставка в supabase батчами
UPSERT_BATCH = 1000

# при желании можно фильтровать только реальные склады
FILTER_ONLY_REAL_WAREHOUSES = False
REAL_WAREHOUSE_PREFIXES = {
    # пример:
    # "Коледино", "Невинномысск", "Электросталь"
}

# --------------------------- Утилиты -------------------------------

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
    Простой ретрай на сетевые/429/5xx ошибки.
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


# --------------------------- WB API --------------------------------

def wb_headers(token: str) -> Dict[str, str]:
    return {"Authorization": token.strip()}


def wb_create_report(token: str) -> str:
    """
    Создаёт задачу генерации отчёта. Возвращает taskId (uuid).
    """
    params = {
        "locale": "ru",
        "groupByNm": "true",
        "groupByBarcode": "true",
        "groupBySize": "true",
        # при необходимости можно включить и другие groupBy/*
        # фильтры filterPics/filterVolume не используются
    }
    r = retry_request("GET", WB_BASE, headers=wb_headers(token), params=params)
    if r.status_code != 200:
        raise RuntimeError(f"WB create report failed: {r.status_code} {r.text}")
    task_id = r.json()["data"]["taskId"]
    log.info("WB report task created: %s", task_id)
    return task_id


def wb_wait_done(token: str, task_id: str) -> None:
    """
    Ждёт статус done; учитывает лимит статуса (1/5 сек).
    """
    url = f"{WB_BASE}/tasks/{task_id}/status"
    deadline = time.time() + POLL_TIMEOUT_SEC
    while time.time() < deadline:
        r = retry_request("GET", url, headers=wb_headers(token))
        if r.status_code != 200:
            raise RuntimeError(f"WB status failed: {r.status_code} {r.text}")

        payload = r.json().get("data", {})
        status = str(payload.get("status", "")).lower()
        if status == "done":
            log.info("WB report ready")
            return
        if status in ("failed", "error"):
            raise RuntimeError(f"WB report failed: {payload}")

        log.debug("WB report status: %s; wait %ds", status or "unknown", POLL_INTERVAL_SEC)
        time.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError("WB report generation timed out")


def wb_download(token: str, task_id: str) -> List[Dict[str, Any]]:
    """
    Скачивает отчёт. Возвращает список позиций с массивом warehouses.
    """
    url = f"{WB_BASE}/tasks/{task_id}/download"
    r = retry_request("GET", url, headers=wb_headers(token))
    if r.status_code != 200:
        raise RuntimeError(f"WB download failed: {r.status_code} {r.text}")
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected WB payload (not a list): {type(data)}")
    log.info("WB report items: %d", len(data))
    return data


def flatten_rows(raw: List[Dict[str, Any]], fetched_at: dt.datetime) -> List[Dict[str, Any]]:
    """
    Разворачиваем warehouses -> строки.
    Сохраняем «виртуальные» склады (в пути/возвраты) как отдельные warehouse_name,
    если не включён фильтр только реальных складов.
    """
    rows: List[Dict[str, Any]] = []
    fetched_iso = fetched_at.replace(microsecond=0).isoformat() + "Z"

    for item in raw:
        brand = item.get("brand")
        subject = item.get("subjectName")
        vendor = item.get("vendorCode")
        nm_id = item.get("nmId")
        barcode = item.get("barcode")
        tech_size = item.get("techSize")
        volume = item.get("volume")

        warehouses = item.get("warehouses") or []
        for w in warehouses:
            wh_name = w.get("warehouseName")
            qty = int(w.get("quantity") or 0)

            if FILTER_ONLY_REAL_WAREHOUSES and REAL_WAREHOUSE_PREFIXES:
                if not any(str(wh_name).startswith(pfx) for pfx in REAL_WAREHOUSE_PREFIXES):
                    continue

            rows.append({
                "fetched_at": fetched_iso,
                "brand": brand,
                "subject_name": subject,
                "vendor_code": vendor,
                "nm_id": nm_id,
                "barcode": barcode,
                "tech_size": tech_size,
                "volume_l": volume,
                "warehouse_name": wh_name,
                "quantity": qty,
            })

    log.info("Flattened rows: %d", len(rows))
    return rows


# --------------------------- Supabase -------------------------------

def supabase_client() -> Client:
    url = getenv_required("SUPABASE_URL")
    key = getenv_required("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def upsert_rows(client: Client, table: str, rows: List[Dict[str, Any]]) -> None:
    """
    Upsert по составному ключу (nm_id, barcode, tech_size, warehouse_name).
    Требуется уникальный индекс/constraint на эти поля.
    """
    if not rows:
        log.info("No rows to upsert")
        return

    t = client.table(table)
    total = 0
    for chunk in batched(rows, UPSERT_BATCH):
        # supabase-py v2: on_conflict — одномоментно указываем список колонок
        res = t.upsert(chunk, on_conflict="nm_id,barcode,tech_size,warehouse_name").execute()
        total += len(chunk)
        log.debug("Upserted chunk: %d", len(chunk))
    log.info("Upserted total: %d", total)


# --------------------------- Основной сценарий ----------------------

def main() -> int:
    try:
        wb_token = getenv_required("WB_ANALYTICS_TOKEN")
        supa = supabase_client()
        table = os.getenv("SUPABASE_TABLE", "wb_stocks_current")

        # 1) создать отчёт
        task_id = wb_create_report(wb_token)

        # 2) дождаться
        wb_wait_done(wb_token, task_id)

        # 3) скачать
        raw = wb_download(wb_token, task_id)

        # 4) расплющить + штамп времени
        fetched_at = dt.datetime.utcnow()
        rows = flatten_rows(raw, fetched_at)

        # 5) upsert в supabase
        upsert_rows(supa, table, rows)

        log.info("Done. task_id=%s, rows=%d", task_id, len(rows))
        return 0

    except Exception as e:
        log.exception("Job failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
