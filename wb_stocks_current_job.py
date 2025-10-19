#!/usr/bin/env python3
import os
import time
import hashlib
import datetime as dt
from typing import List, Dict, Any

import requests
from supabase import create_client, Client  # pip install supabase
# Если будешь использовать прокси/корп.сеть: учитывай переменные среды HTTPS_PROXY/NO_PROXY

WB_BASE = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
WB_HEADERS = lambda token: {"Authorization": token.strip()}

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
TABLE = os.getenv("SUPABASE_TABLE", "wb_stocks_current")

# Настройки опроса
POLL_INTERVAL_SEC = 5          # лимит WB: 1 запрос/5 сек на статус
POLL_TIMEOUT_SEC = 600         # 10 минут на генерацию отчёта (обычно быстрее)
SESSION = requests.Session()
SESSION_TIMEOUT = (10, 60)     # connect, read timeouts


def create_report(token: str) -> str:
    """
    Создаёт задачу генерации отчёта. Возвращает taskId (uuid).
    """
    params = {
        "locale": "ru",
        "groupByNm": "true",
        "groupByBarcode": "true",
        "groupBySize": "true",
        # при необходимости можно добавить groupByBrand/Subject/Sa
        # фильтры: filterPics, filterVolume — по задаче не нужны
    }
    r = SESSION.get(WB_BASE, headers=WB_HEADERS(token), params=params, timeout=SESSION_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"WB create report failed: {r.status_code} {r.text}")
    task_id = r.json()["data"]["taskId"]
    return task_id


def wait_report_done(token: str, task_id: str) -> None:
    """
    Ждёт статуса done с учётом лимитов. Бросает исключение по таймауту/ошибке.
    """
    status_url = f"{WB_BASE}/tasks/{task_id}/status"
    deadline = time.time() + POLL_TIMEOUT_SEC
    while time.time() < deadline:
        r = SESSION.get(status_url, headers=WB_HEADERS(token), timeout=SESSION_TIMEOUT)
        if r.status_code != 200:
            # на всплеске 429 — чуть подождать и продолжить
            if r.status_code in (429, 503, 502, 504):
                time.sleep(POLL_INTERVAL_SEC)
                continue
            raise RuntimeError(f"WB status failed: {r.status_code} {r.text}")

        payload = r.json()["data"]
        status = payload.get("status", "").lower()
        if status == "done":
            return
        elif status in ("failed", "error"):
            raise RuntimeError(f"WB report failed: {payload}")

        time.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError("WB report generation timed out")


def download_report(token: str, task_id: str) -> List[Dict[str, Any]]:
    """
    Забирает готовый отчёт. Ответ — массив объектов с полем warehouses.
    """
    url = f"{WB_BASE}/tasks/{task_id}/download"
    r = SESSION.get(url, headers=WB_HEADERS(token), timeout=SESSION_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"WB download failed: {r.status_code} {r.text}")
    return r.json()


def flatten_rows(raw: List[Dict[str, Any]], task_id: str, fetched_at: dt.datetime) -> List[Dict[str, Any]]:
    """
    Разворачиваем warehouses в строки. В WB в массив попадают только записи с ненулевым quantity.
    WarehouseName может быть как реальный склад, так и «В пути ...».
    """
    rows = []
    fetched_iso = fetched_at.isoformat()
    for item in raw:
        brand = item.get("brand")
        subject = item.get("subjectName")
        vendor = item.get("vendorCode")
        nm_id = item.get("nmId")
        barcode = item.get("barcode")
        tech_size = item.get("techSize")
        volume = item.get("volume")

        for w in item.get("warehouses", []) or []:
            wh_name = w.get("warehouseName")
            qty = int(w.get("quantity") or 0)

            # делаем стабильный ключ; округляем fetched_at до минут, чтобы один прогон не плодил дубликаты
            fetched_minute = fetched_at.replace(second=0, microsecond=0).isoformat()
            key = f"{nm_id}|{barcode}|{tech_size}|{wh_name}|{fetched_minute}"
            h = hashlib.sha1(key.encode("utf-8")).hexdigest()

            rows.append({
                "report_task_id": task_id,
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
                "_hash": h,
            })
    return rows


def batched(iterable, size):
    batch = []
    for x in iterable:
        batch.append(x)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def upsert_to_supabase(client: Client, rows: List[Dict[str, Any]]) -> None:
    """
    Upsert пачками. На таблице есть unique (_hash), поэтому конфликтуем по нему.
    В supabase-py v2 конфликтное поле указывается через on_conflict.
    """
    if not rows:
        return
    table = client.table(TABLE)
    for chunk in batched(rows, 1000):
        # on_conflict должен совпадать с unique-индексом
        res = table.upsert(chunk, on_conflict="_hash").execute()
        # можно добавить валидацию res.count / res.data при необходимости


def main():
    wb_token = os.getenv("WB_ANALYTICS_TOKEN")
    if not wb_token:
        raise RuntimeError("WB_ANALYTICS_TOKEN is required")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1) создаём отчёт
    task_id = create_report(wb_token)

    # 2) ждём готовности
    wait_report_done(wb_token, task_id)

    # 3) скачиваем
    raw = download_report(wb_token, task_id)

    # 4) плоские строки
    fetched_at = dt.datetime.utcnow()
    rows = flatten_rows(raw, task_id, fetched_at)

    # 5) upsert в Supabase
    upsert_to_supabase(supabase, rows)

    print(f"Inserted/updated rows: {len(rows)}; task_id={task_id}")


if __name__ == "__main__":
    main()
