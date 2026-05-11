"""
Публичные страницы сервиса mpi.laprinta.ru: главная (остатки), очередь печати, очередь поставок.
Остатки кэшируются до 24 часов; обновление по кнопке или автоматически при устаревании кэша.
"""
from __future__ import annotations

import asyncio
import base64
import json as _json
import tempfile
import uuid
import math
import re
import time
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from collections import defaultdict
from copy import deepcopy
from collections.abc import Awaitable, Callable
from typing import Dict, Optional, Tuple

import httpx
from dateutil import tz as dateutil_tz
from dateutil.relativedelta import relativedelta
from fastapi import BackgroundTasks, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from loguru import logger

from app.config import settings
from app.modules.ozon.api_client import OzonAPIClient, activate_manual_supply_priority
from app.modules.wildberries.api_client import WildberriesAPIClient
from app.planner import plan_jobs as planner_plan_jobs
from app.planner import validate_no_collisions as planner_validate_no_collisions

MSK = dateutil_tz.gettz("Europe/Moscow")
CACHE_MAX_AGE = timedelta(hours=24)

# Кэш остатков раздельно: Ozon по кластерам, WB — плоский список
_stocks_cache = {
    "ozon": None,
    "wb": None,
    "wb_articles": None,
    "ozon_table": None,
    "last_updated_ozon": None,
    "last_updated_wb": None,
    "last_updated_wb_articles": None,
    "last_updated_ozon_table": None,
    # Для уголков матрицы остатков на /supply-queue (обновляются вместе с ozon_table)
    "macrolocal_resolve": None,
    "warehouse_norm_to_macrolocal": None,
    "ozon_product_id_to_offer": None,
}
_warehouse_stocks_refresh_lock = asyncio.Lock()
_supplies_sync_from_lk_lock = asyncio.Lock()
_supplies_sync_from_lk_state: Dict[str, object] = {
    "last_started_at": None,
    "last_finished_at": None,
    "last_error": "",
    "stage": "",
    "message": "",
    "total_order_ids": 0,
    "processed_order_ids": 0,
    "added": 0,
    "updated": 0,
    "composition_filled": 0,
    "cargo_rows_filled": 0,
    "cargo_items_total": 0,
}


def _set_supplies_sync_progress(**kwargs) -> None:
    """Обновить прогресс фоновой синхронизации поставок из ЛК."""
    try:
        for k, v in kwargs.items():
            _supplies_sync_from_lk_state[k] = v
    except Exception:
        pass

# Защита от повторного подтверждения одного и того же draft_id:
# если параллельно пришли 2 запроса (например, две вкладки/двойной клик),
# то только первый пойдёт в Ozon, остальные получат 409.
_confirm_inflight_draft_ids: set[int] = set()

# --- Printer telemetry for Gantt (Fluidd/Moonraker) ---
# Чтобы не дёргать принтеры слишком часто, держим короткий in-memory кеш.
_printers_status_cache: dict[str, object] = {"ts": 0.0, "data": []}
_printers_status_cache_lock = asyncio.Lock()
PRINTER_STATUS_CACHE_TTL_SEC = 15.0


def _printer_status_title(status: str) -> str:
    return {
        "free": "Свободен",
        "busy": "Занят",
        "error": "Ошибка",
        "disconnected": "Не подключен",
    }.get(status, "Неизвестно")


def _map_printer_status(klippy_state: Optional[str], print_stats_state: Optional[str]) -> str:
    """Маппинг Klipper/Moonraker состояний в статусы для Ганта."""
    st = (print_stats_state or klippy_state or "").strip().lower()
    if "error" in st:
        return "error"
    if st in {"printing", "paused", "resuming", "pausing", "startup"}:
        return "busy"
    if st in {"standby", "ready", "complete", "none", ""}:
        return "free"
    # На случай новых/неучтённых значений считаем "занят" (консервативно).
    return "busy"


async def _safe_http_json(client: httpx.AsyncClient, url: str) -> Optional[dict]:
    try:
        resp = await client.get(url)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


async def _http_try_json_with_status(client: httpx.AsyncClient, url: str) -> tuple[Optional[dict], Optional[int]]:
    """Пытаемся получить JSON и возвращаем (json_or_none, http_status_or_none)."""
    try:
        resp = await client.get(url)
        if resp.status_code != 200:
            return None, resp.status_code
        return resp.json(), resp.status_code
    except Exception:
        return None, None


def _extract_state_from_moonraker_objects_query(data: dict) -> tuple[Optional[str], Optional[str]]:
    """GET /printer/objects/query?print_stats -> (klippy_state, print_stats_state)."""
    result = data.get("result")
    if isinstance(result, dict):
        ps = result.get("print_stats")
        if isinstance(ps, dict):
            v = ps.get("state") if isinstance(ps.get("state"), str) else None
            return None, v
    ps2 = data.get("print_stats")
    if isinstance(ps2, dict):
        v = ps2.get("state") if isinstance(ps2.get("state"), str) else None
        return None, v
    return None, None


def _extract_state_from_moonraker_printer_info(data: dict) -> tuple[Optional[str], Optional[str]]:
    """GET /printer/info -> (klippy_state, print_stats_state)."""
    if not isinstance(data, dict):
        return None, None
    d = data["result"] if isinstance(data.get("result"), dict) else data
    klippy_state = d.get("state") if isinstance(d.get("state"), str) else None
    return klippy_state, None

templates_path = Path(__file__).resolve().parent.parent.parent / "templates" / "site"
templates = Jinja2Templates(directory=str(templates_path))

# Папка для сохранения PDF ШК грузомест; срок хранения 1 месяц
CARGO_LABELS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "cargo_labels"
CARGO_LABELS_MAX_AGE_DAYS = 30

STATIC_IMAGES = Path(__file__).resolve().parent.parent.parent / "static" / "site" / "images"


def _spool_svg_dataurl(
    hex_color: str,
    icon_type: str,
    size: int = 32,
    plastic_type: Optional[str] = None,
) -> Optional[str]:
    """Генерирует data URL для иконки катушки. Маска файла: Spool_<reach|midi|poor>_<Тип пластика>.svg.
    Если тип пластика задан и есть файл с таким именем — используем его, иначе Spool_<reach|midi|poor>.svg."""
    if not hex_color or icon_type not in ("reach", "midi", "poor"):
        return None
    plastic = (plastic_type or "").strip()
    fname = f"Spool_{icon_type}_{plastic}.svg" if plastic else f"Spool_{icon_type}.svg"
    path = STATIC_IMAGES / fname
    if not path.is_file() and plastic:
        fname = f"Spool_{icon_type}.svg"
        path = STATIC_IMAGES / fname
    if not path.is_file():
        return None
    try:
        svg = path.read_text(encoding="utf-8")
    except OSError:
        return None
    # Только подстановка цвета филамента (первый path с fill)
    svg = re.sub(r"fill:#[0-9a-fA-F]{3,8}\b", f"fill:{hex_color}", svg, count=1)
    svg = re.sub(r'fill="(?!none)[^"]*"', f'fill="{hex_color}"', svg, count=1)
    # Размер вывода и центрирование; viewBox не трогаем — масштабируем весь исходный SVG
    if "preserveAspectRatio" not in svg:
        svg = re.sub(r'(viewBox="[^"]*")', r'\1 preserveAspectRatio="xMidYMid meet"', svg, count=1)
    size_str = str(size)
    svg = re.sub(r'\bwidth="[^"]*"', f'width="{size_str}"', svg, count=1)
    svg = re.sub(r'\bheight="[^"]*"', f'height="{size_str}"', svg, count=1)
    # stroke-opacity 0.4 для контура катушки (filament_outline)
    svg = re.sub(
        r'(fill-opacity:0[^"]*stroke:#[^;]+;stroke-width:2\.4)(;stroke-opacity:\d+\.?\d*)?(")',
        r'\1;stroke-opacity:0.4\3',
        svg,
        count=1,
    )
    data = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{data}"


def _cleanup_old_cargo_labels() -> None:
    """Удаляет PDF ШК грузомест старше CARGO_LABELS_MAX_AGE_DAYS дней."""
    if not CARGO_LABELS_DIR.is_dir():
        return
    try:
        cutoff = datetime.now(timezone.utc).timestamp() - (CARGO_LABELS_MAX_AGE_DAYS * 24 * 3600)
        for p in CARGO_LABELS_DIR.iterdir():
            if p.is_file() and p.suffix.lower() == ".pdf" and p.stat().st_mtime < cutoff:
                try:
                    p.unlink()
                    logger.info("cargo-labels: удалён устаревший файл {}", p.name)
                except OSError as e:
                    logger.warning("cargo-labels: не удалось удалить {}: {}", p.name, e)
    except OSError as e:
        logger.warning("cargo-labels cleanup: {}", e)
if "tojson" not in templates.env.filters:
    templates.env.filters["tojson"] = lambda x: _json.dumps(x, ensure_ascii=False)

# Месяцы для отображения даты отправки: "10 мая"
_SHIPMENT_MONTH_NAMES = ("", "января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря")


def _material_name_without_weight(raw_name: str | None) -> str:
    """Возвращает название материала без хвостов веса вида '1 кг', '1000 г', '0.75 кг'."""
    name = (raw_name or "").strip()
    if not name:
        return ""
    # Срезаем только вес в конце строки, чтобы не затрагивать середину названия.
    cleaned = re.sub(r"\s*[0-9]+(?:[.,][0-9]+)?\s*(?:кг|kg|г|гр|g)\s*$", "", name, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"[\s,;]+$", "", cleaned).strip()
    return cleaned or name


def _format_shipment_date_display(shipment_date: str, timeslot_from: Optional[str], timeslot_to: Optional[str]) -> str:
    """Формат: «10 мая» или «10 мая с 11:00 до 12:00»."""
    if not shipment_date or len(shipment_date) < 10:
        return ""
    try:
        parts = shipment_date[:10].split("-")
        if len(parts) != 3:
            return shipment_date[:10]
        d, m = int(parts[2]), int(parts[1])
        if 1 <= m <= 12:
            date_str = f"{d} {_SHIPMENT_MONTH_NAMES[m]}"
        else:
            date_str = shipment_date[:10]
    except Exception:
        date_str = shipment_date[:10]
    time_from = time_to = ""
    if timeslot_from and "T" in timeslot_from:
        idx = timeslot_from.index("T") + 1
        if idx + 5 <= len(timeslot_from):
            time_from = timeslot_from[idx : idx + 5]
    if timeslot_to and "T" in timeslot_to:
        idx = timeslot_to.index("T") + 1
        if idx + 5 <= len(timeslot_to):
            time_to = timeslot_to[idx : idx + 5]
    if time_from and time_to:
        return f"{date_str} с {time_from} до {time_to} МСК"
    if time_from:
        return f"{date_str} с {time_from} МСК"
    return date_str


def _format_dt_as_msk(dt_value: datetime | None) -> str:
    """Форматирует дату/время в МСК (UTC+3) строкой dd.mm.yyyy HH:MM."""
    if not dt_value:
        return "—"
    try:
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        msk_dt = dt_value.astimezone(MSK)
        return msk_dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "—"


_MSK_MONTH_NAMES_GEN = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


def _format_stock_log_date_msk(dt_value: datetime | None) -> str:
    """Дата для журнала склада: «13 апреля» (год — если не текущий)."""
    if not dt_value:
        return "—"
    try:
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        msk_dt = dt_value.astimezone(MSK)
        d, y, m = msk_dt.day, msk_dt.year, msk_dt.month
        mon = _MSK_MONTH_NAMES_GEN.get(m, str(m))
        now_y = datetime.now(MSK).year
        if y == now_y:
            return f"{d} {mon}"
        return f"{d} {mon} {y}"
    except Exception:
        return "—"


def _format_stock_log_time_msk(dt_value: datetime | None) -> str:
    """Время журнала: ЧЧ:ММ:СС (локальное время сервиса)."""
    if not dt_value:
        return "—"
    try:
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        msk_dt = dt_value.astimezone(MSK)
        return msk_dt.strftime("%H:%M:%S")
    except Exception:
        return "—"


PRINTED_STOCK_LOG_KINDS = frozenset({"add", "remove", "defect", "defect_return", "assembly", "assembly_return"})

ASSEMBLY_BATCH_STATUS_VALUES = frozenset({"created", "in_progress", "completed"})
ASSEMBLED_ADJUST_ACTIONS = frozenset({"add", "write_off", "defect"})
ASSEMBLED_ACTION_LABELS_RU = {
    "assembly_complete": "Поступление из выполненной сборки",
    "manual_add": "Добавлено вручную",
    "write_off": "Списание",
    "defect": "Брак",
    "return_assembly": "Возврат в сборку (детали на склад напечатанных)",
}
_ASSEMBLED_RU_MONTHS = (
    "",
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


def _assembled_log_datetime_parts(value: datetime) -> tuple[str, str]:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    date_part = f"{value.day} {_ASSEMBLED_RU_MONTHS[value.month]}"
    time_part = value.strftime("%H:%M:%S")
    return date_part, time_part


def _product_assembled_label(product: Product) -> str:
    a = (product.article or "").strip()
    n = (product.name or "").strip() or "—"
    return ((a + " ") if a else "") + n


def _parse_stock_log_date_param(s: str | None) -> date | None:
    if not s or not str(s).strip():
        return None
    try:
        parts = str(s).strip()[:10].split("-")
        if len(parts) != 3:
            return None
        y, mo, d = int(parts[0]), int(parts[1]), int(parts[2])
        return date(y, mo, d)
    except Exception:
        return None


def _parse_stock_log_time_param(s: str | None) -> dt_time | None:
    if not s or not str(s).strip():
        return None
    try:
        raw = str(s).strip()
        parts = raw.split(":")
        h = int(parts[0])
        mi = int(parts[1]) if len(parts) > 1 else 0
        sec = int(parts[2]) if len(parts) > 2 else 0
        return dt_time(h, mi, sec)
    except Exception:
        return None


def _stock_log_row_matches_msk_datetime_filters(
    created_at: datetime | None,
    date_from: date | None,
    date_to: date | None,
    time_from: dt_time | None,
    time_to: dt_time | None,
) -> bool:
    """Фильтр по календарной дате и времени суток (та же зона, что и отображение)."""
    if not created_at:
        return False
    try:
        dtv = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
        msk = dtv.astimezone(MSK)
        d = msk.date()
        t = msk.time()
        if date_from is not None and d < date_from:
            return False
        if date_to is not None and d > date_to:
            return False
        if time_from is not None or time_to is not None:
            tf = time_from or dt_time.min
            tt = time_to or dt_time(23, 59, 59, 999999)
            if t < tf or t > tt:
                return False
        return True
    except Exception:
        return False


async def _fetch_ozon_clusters() -> list:
    """Остатки Ozon по кластерам: [ {"name": "Казань", "rows": [{article, name, stock}]}, ... ]."""
    try:
        ozon_client = OzonAPIClient()
        return await ozon_client.get_stocks_by_cluster()
    except Exception as e:
        logger.warning(f"Остатки Ozon для главной: {e}")
        return []


async def _fetch_wb_rows() -> list:
    """Остатки WB: список [{article, name, stock}]."""
    try:
        wb_client = WildberriesAPIClient()
        wb_qty, wb_article_to_nmid = await wb_client.get_stocks_with_nmids()
        nmids = list(set(wb_article_to_nmid.values())) if wb_article_to_nmid else []
        wb_nmid_to_name = await wb_client.get_product_names_by_nmids(nmids) if nmids else {}
        rows = []
        for article, qty in sorted(wb_qty.items()):
            nmid = wb_article_to_nmid.get(article)
            name = wb_nmid_to_name.get(nmid, "—") if nmid is not None else "—"
            rows.append({"article": article, "name": name, "stock": qty})
        return rows
    except Exception as e:
        logger.warning(f"Остатки WB для главной: {e}")
        return []


def _cache_fresh(updated: datetime | None) -> bool:
    if updated is None:
        return False
    return (datetime.now(MSK) - updated) < CACHE_MAX_AGE


async def get_cached_stocks(
    force_ozon: bool = False,
    force_wb: bool = False,
) -> tuple[list, list, datetime | None, datetime | None]:
    """ОСТАТКИ БОЛЬШЕ НЕ ИСПОЛЬЗУЮТСЯ НА ГЛАВНОЙ. ОСТАВЛЕНО ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ."""
    return [], [], None, None


import re

from fastapi import APIRouter, BackgroundTasks, Depends, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlalchemy import select, delete, func, text, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from app.auth import verify_site_user, verify_password
from app.db.database import get_db, AsyncSessionLocal
from app.db.models import (
    User, PrintJob, Printer, Material, Color, PrintQueueItem, Spool,
    Product, ProductPart, Part, ExtraMaterial, ProductExtraMaterial,
    WarehouseExtraStock, WrittenOffMaterial, PrintedPartStock, PrintedPartStockLog,
    AssembledProductStock, AssembledProductStockLog, WarehouseDefectRecord,
    WarehouseAssemblyBatch, WarehouseAssemblyBatchItem,
    PrintPlan, PrintPlanItem,
    SupplyQueueScan, SupplyQueueResult, OzonSupply, SupplyDraftConfig,
)

router = APIRouter(tags=["site"])


def _parse_execution_time_minutes(s: str) -> int:
    """Парсит строку длительности типа '2 ч 30 мин', '45 мин', '1 ч' в минуты."""
    if not s or not isinstance(s, str):
        return 0
    s = s.strip()
    total = 0
    # часы: "2 ч", "2ч"
    for m in re.finditer(r"(\d+)\s*ч", s, re.IGNORECASE):
        total += int(m.group(1)) * 60
    # минуты: "30 мин", "30мин"
    for m in re.finditer(r"(\d+)\s*мин", s, re.IGNORECASE):
        total += int(m.group(1))
    return total


def _round_start_to_15_min(dt) -> datetime:
    """Округляет время начала до начала 15-минутного интервала (вниз)."""
    from dateutil import tz as dateutil_tz
    tz = dateutil_tz.gettz("Europe/Moscow")
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=tz)
    minute = dt.minute + dt.second / 60.0 + dt.microsecond / 60000000.0
    new_minute = int(minute // 15) * 15
    return dt.replace(minute=new_minute, second=0, microsecond=0)


def _ceil_to_next_15_min(dt) -> datetime:
    """Округляет время вверх до начала следующего 15-минутного слота (10:07 -> 10:15, 10:15 -> 10:15)."""
    from dateutil import tz as dateutil_tz
    tz = dateutil_tz.gettz("Europe/Moscow")
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=tz)
    minute = dt.minute + dt.second / 60.0 + dt.microsecond / 60000000.0
    slot = int(math.ceil(minute / 15.0)) * 15
    if slot >= 60:
        return (dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return dt.replace(minute=slot, second=0, microsecond=0)


def _next_slot_after_end(end_dt: datetime, gap_minutes: int = 15) -> datetime:
    """Следующий допустимый старт: конец слота после end_dt + один целый слот (зазор)."""
    slot_end = _ceil_to_next_15_min(end_dt)
    return slot_end + timedelta(minutes=gap_minutes)


def _ensure_datetime_msk(value):
    """Приводит значение к datetime в MSK. SQLite может вернуть строку или naive;
    наивные считаем московским временем (в БД пишем MSK, таймзону SQLite не хранит)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=MSK)
        return value.astimezone(MSK) if value.tzinfo != MSK else value
    from dateutil.parser import isoparse
    try:
        dt = isoparse(str(value)) if isinstance(value, str) else value
    except Exception:
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=MSK)
    else:
        dt = dt.astimezone(MSK)
    return dt


def _run_deterministic_planner(
    expanded: list[tuple[int, int | None, int]],
    job_id_to_info: dict[int, tuple[list[int], int, str]],
    all_printers: list[int],
    next_free: dict[int, datetime],
    last_material: dict[int, int | None],
    week_start_dt: datetime,
    week_end_dt: datetime,
    gap_minutes: int,
    material_change_penalty: int = 86400,  # 24 ч в секундах — сильно предпочитаем не менять материал на принтере
) -> list[tuple[int, datetime, datetime, int, int | None, int]] | None:
    """
    Детерминированный планировщик: один вход → один план, без коллизий.
    Возвращает список (printer_id, start_dt, end_dt, job_id, material_id, duration_min) или None при ошибке.
    """
    # Любая неделя рассматривается одинаково: дни идут друг за другом, старт — понедельник 8:00.
    # Для текущей недели не планируем в прошлое: now_ts = max(понедельник 8:00, сейчас).
    week_first_slot = week_start_dt.replace(hour=8, minute=0, second=0, microsecond=0)
    now_dt = datetime.now(MSK)
    is_current_week = week_start_dt <= now_dt < week_end_dt
    if is_current_week:
        effective_start = max(week_first_slot, now_dt)
        hour_ts = effective_start.replace(minute=0, second=0, microsecond=0)
        if effective_start > hour_ts:
            hour_ts = hour_ts + timedelta(hours=1)
        now_ts = int(hour_ts.timestamp())
    else:
        now_ts = int(week_first_slot.timestamp())

    jobs: list[dict] = []
    for i, (jid, mat_id, dur_min) in enumerate(expanded):
        if jid not in job_id_to_info:
            continue
        pids, _, _ = job_id_to_info[jid]
        allowed = [p for p in pids if p in all_printers]
        if not allowed:
            continue
        jobs.append({
            "job_id": i,
            "duration_s": dur_min * 60,
            "material": mat_id,
            "allowed_printer_ids": allowed,
        })
    if not jobs:
        return []

    printers_list: list[dict] = []
    for pid in sorted(all_printers):
        dt = next_free.get(pid, week_start_dt)
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=MSK) if hasattr(dt, "replace") else week_start_dt
        if dt <= week_first_slot:
            dt = week_first_slot
        printers_list.append({
            "printer_id": pid,
            "available_at": int(dt.timestamp()),
            "current_material": last_material.get(pid),
        })

    gap_s = gap_minutes * 60
    try:
        plan = planner_plan_jobs(
            jobs, printers_list, now_ts, material_change_penalty,
            gap_after_s=gap_s, log_trace=False,
        )
        planner_validate_no_collisions(plan, gap_after_s=gap_s)
    except ValueError as e:
        logger.warning("Deterministic planner failed or collision: %s", e)
        return None

    out: list[tuple[int, datetime, datetime, int, int | None, int]] = []
    for a in plan:
        i = a["job_id"]
        if i >= len(expanded):
            continue
        jid, mat_id, dur_min = expanded[i]
        start_ts = a["start"]
        start_dt = datetime.fromtimestamp(start_ts, tz=MSK)
        # Округляем вверх до 15 мин, чтобы не сдвигать старт раньше расчётного и не создавать коллизии с зазором
        start_dt = _ceil_to_next_15_min(start_dt)
        end_dt = start_dt + timedelta(minutes=dur_min)
        out.append((a["printer_id"], start_dt, end_dt, jid, mat_id, dur_min))
    out.sort(key=lambda x: (x[1], x[0]))
    return out


def _norm_art(s: str) -> str:
    return " ".join((s or "").strip().split())


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Страница входа на сайт. Если уже авторизован — редирект на главную."""
    try:
        session = getattr(request, "session", None)
        if session and session.get("site_user_id"):
            return RedirectResponse(url="/", status_code=303)
    except Exception:
        pass
    error = request.query_params.get("error") == "1"
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": error, "site_username": None},
    )


@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    """Проверка логина/пароля по БД и создание сессии."""
    username = (username or "").strip()
    password = (password or "").strip()
    if not username or not password:
        return RedirectResponse(url="/login?error=1", status_code=303)
    result = await db.execute(select(User).where(User.username == username))
    user = result.scalar_one_or_none()
    if not user or not verify_password(password, user.password_hash):
        return RedirectResponse(url="/login?error=1", status_code=303)
    try:
        session = getattr(request, "session", None)
        if session is not None:
            session["site_user_id"] = user.id
            session["site_username"] = user.username
    except Exception as e:
        logger.warning("Сессия при входе: %s", e)
    return RedirectResponse(url="/", status_code=303)


@router.get("/logout")
async def logout(request: Request):
    """Выход: очистка сессии и редирект на страницу входа."""
    try:
        session = getattr(request, "session", None)
        if session is not None:
            session.pop("site_user_id", None)
            session.pop("site_username", None)
    except Exception:
        pass
    return RedirectResponse(url="/login", status_code=303)


@router.get("/api/payouts")
async def api_get_payouts(user: User = Depends(verify_site_user)):
    """Вернуть кэш заработка без обновления (для AJAX, без таймаута)."""
    from app.modules.notifications.reporter import get_payout_cached
    data = get_payout_cached()
    return JSONResponse(content=data)


@router.post("/api/refresh-payouts")
async def api_refresh_payouts(background_tasks: BackgroundTasks, user: User = Depends(verify_site_user)):
    """Запустить обновление кэша в фоне (1–2 мин); данные подтянуть через GET /api/payouts или опрос."""
    from app.modules.notifications.reporter import refresh_payout_cache
    try:
        background_tasks.add_task(refresh_payout_cache)
        return JSONResponse(status_code=202, content={
            "status": "started",
            "message": "Обновление запущено. Данные подтянутся через 1–2 мин.",
        })
    except Exception as e:
        logger.warning("api/refresh-payouts: %s", e, exc_info=True)
        return JSONResponse(status_code=500, content={"detail": str(e)})


_SALES_WEEK_TTL = timedelta(hours=1)
_sales_week_cache: dict[str, Any] = {
    "data": None,
    "updated_at": None,
}


async def _load_sales_week_data() -> tuple[list, int, datetime | None]:
    """Тяжёлый запрос продаж за неделю: отдельная функция, чтобы вызывать в фоне/по API."""
    from app.modules.notifications.reporter import get_daily_chart_data

    chart_data = await get_daily_chart_data()
    max_chart_qty = max(
        max((d["ozon_qty"] for d in chart_data), default=0),
        max((d["wb_qty"] for d in chart_data), default=0),
        1,
    )
    updated_at = datetime.now(MSK)
    _sales_week_cache["data"] = chart_data
    _sales_week_cache["updated_at"] = updated_at
    return chart_data, max_chart_qty, updated_at


def _sales_cache_fresh() -> bool:
    ts = _sales_week_cache.get("updated_at")
    if ts is None:
        return False
    return (datetime.now(MSK) - ts) < _SALES_WEEK_TTL


async def _get_sales_week_cached(force: bool = False) -> tuple[list, int]:
    """Вернуть (chart_data, max_chart_qty) из кэша; при необходимости обновить."""
    if (not force) and _sales_cache_fresh() and _sales_week_cache.get("data"):
        data = _sales_week_cache["data"] or []
        max_chart_qty = max(
            max((d["ozon_qty"] for d in data), default=0),
            max((d["wb_qty"] for d in data), default=0),
            1,
        )
        return data, max_chart_qty
    try:
        data, max_q, _ = await _load_sales_week_data()
        return data, max_q
    except Exception as e:
        logger.warning("sales-week cache refresh failed: %s", e, exc_info=True)
        data = _sales_week_cache.get("data") or []
        max_q = max(
            max((d.get("ozon_qty", 0) for d in data), default=0),
            max((d.get("wb_qty", 0) for d in data), default=0),
            1,
        )
        return data, max_q


@router.post("/api/sales-week/refresh", response_class=JSONResponse)
async def api_sales_week_refresh(
    request: Request,
    user: User = Depends(verify_site_user),
) -> JSONResponse:
    """Ручное обновление продаж за неделю по кнопке на главной."""
    try:
        data, max_q = await _get_sales_week_cached(True)
        updated_at = _sales_week_cache.get("updated_at")
        return JSONResponse(
            content={
                "ok": True,
                "items": len(data or []),
                "max_chart_qty": max_q,
                "updated_at": updated_at.isoformat() if updated_at else None,
            }
        )
    except Exception as e:
        logger.warning("api/sales-week/refresh failed: %s", e, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e)},
        )


@router.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    background_tasks: BackgroundTasks,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Главная — только диаграмма продаж за неделю, данные кэшируются и обновляются раз в час."""
    # На каждый заход, если кэш устарел, запускаем фоновое обновление (не блокируя открытие страницы).
    if not _sales_cache_fresh():
        background_tasks.add_task(_get_sales_week_cached, False)

    chart_data, max_chart_qty = await _get_sales_week_cached(False)
    updated_at = _sales_week_cache.get("updated_at")
    updated_at_str = updated_at.strftime("%d.%m.%Y %H:%M МСК") if updated_at else None

    # Ближайшие 7 дней: тот же снимок, что и страница «Очередь поставок»; иначе — БД ozon_supplies
    upcoming_supplies: list[dict] = []
    today = datetime.now(MSK).date()
    end_date = today + timedelta(days=7)
    from_queue = _index_upcoming_supplies_from_supply_queue_snapshot(request.session, today, end_date)
    if from_queue is not None:
        upcoming_supplies = from_queue
    else:
        try:
            r = await db.execute(select(OzonSupply).order_by(OzonSupply.id.desc()))
            rows = r.scalars().all()
            for s in rows:
                ship_raw = (s.shipment_date or "").strip()
                if not ship_raw or len(ship_raw) < 10:
                    continue
                try:
                    ship_date = datetime.strptime(ship_raw[:10], "%Y-%m-%d").date()
                except ValueError:
                    continue
                if ship_date < today or ship_date > end_date:
                    continue

                ts_from = getattr(s, "timeslot_from", None) or ""
                ts_to = getattr(s, "timeslot_to", None) or ""
                sort_dt = datetime.combine(ship_date, datetime.min.time())
                if "T" in ts_from:
                    try:
                        idx = ts_from.index("T") + 1
                        hhmm = ts_from[idx : idx + 5]
                        hh, mm = int(hhmm[:2]), int(hhmm[3:5])
                        sort_dt = sort_dt.replace(hour=hh, minute=mm)
                    except Exception:
                        pass

                display_dt = _format_shipment_date_display(ship_raw, ts_from, ts_to)
                order_id = (s.ozon_supply_id or "").strip()
                posting = (getattr(s, "posting_number", None) or "").strip()
                supply_disp = posting or order_id
                cid = getattr(s, "crossdock_cluster_id", None)
                cluster_disp = str(cid) if cid is not None else "—"
                lk_url = f"https://seller.ozon.ru/app/supply/orders/{order_id}" if order_id else ""
                upcoming_supplies.append(
                    {
                        "datetime_display": display_dt or ship_raw[:10],
                        "marketplace": "Ozon",
                        "order_id": order_id,
                        "order_number": order_id,
                        "supply_id": supply_disp,
                        "cluster": cluster_disp,
                        "lk_url": lk_url,
                        "is_today": ship_date == today,
                        "is_tomorrow": ship_date == today + timedelta(days=1),
                        "sort_dt": sort_dt,
                    }
                )
            upcoming_supplies.sort(key=lambda x: x["sort_dt"], reverse=False)
        except Exception as e:
            logger.warning("index: failed to load upcoming supplies from DB: {}", e, exc_info=True)
            upcoming_supplies = []

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "site_username": user.username,
            "chart_data": chart_data,
            "max_chart_qty": max_chart_qty,
            "sales_updated_at": updated_at_str,
            "upcoming_supplies": upcoming_supplies,
        },
    )


@router.get("/print-queue", response_class=HTMLResponse)
async def print_queue(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Очередь печати: диаграмма Ганта и таблица заданий."""
    printers = []
    print_jobs = []
    materials = []
    queue_items = []
    try:
        r = await db.execute(select(Printer).order_by(Printer.number, Printer.name))
        printers = list(r.scalars().all())
        r = await db.execute(select(PrintJob).order_by(PrintJob.name))
        jobs_raw = list(r.scalars().all())
        for job in jobs_raw:
            dur = _parse_execution_time_minutes(job.execution_time or "")
            pids = job.printer_ids if isinstance(job.printer_ids, list) else []
            print_jobs.append({
                "id": job.id,
                "name": job.name or "",
                "execution_time": job.execution_time or "",
                "duration_minutes": dur,
                "printer_ids": pids,
            })
        r = await db.execute(select(Material).order_by(Material.name, Material.color))
        materials = list(r.scalars().all())
        r = await db.execute(
            select(PrintQueueItem, PrintJob)
            .join(PrintJob, PrintQueueItem.print_job_id == PrintJob.id)
            .order_by(PrintQueueItem.sequence.desc())
        )
        rows = r.all()
        printers_by_id = {p.id: p for p in printers}
        materials_by_id = {m.id: m for m in materials}
        queue_items = []
        for item, job in rows:
            start = _ensure_datetime_msk(item.scheduled_start)
            if start is None:
                continue
            dur = _parse_execution_time_minutes(job.execution_time or "")
            end = start + timedelta(minutes=dur) if dur else start
            pr = printers_by_id.get(item.printer_id)
            mat = materials_by_id.get(item.material_id) if item.material_id else None
            queue_items.append({
                "id": item.id,
                "sequence": getattr(item, "sequence", 0) or 0,
                "print_job_id": item.print_job_id,
                "printer_id": item.printer_id,
                "material_id": item.material_id,
                "scheduled_start": start,
                "scheduled_end": end,
                "job_name": job.name or "",
                "duration_minutes": dur,
                "printer_number": pr.number if pr else "",
                "material_name": (mat.name or "") if mat else "",
            })
        # Для Ганта: катушка на принтере (иконка слева от номера)
        spool_by_id = {}
        spool_ids = [p.current_spool_id for p in printers if getattr(p, "current_spool_id", None) is not None]
        if spool_ids:
            r_col = await db.execute(select(Color))
            colors_list = r_col.scalars().all()
            color_hex_map = {c.name.strip().lower(): (c.hex or "#888888") for c in colors_list if c.name}
            r_sp = await db.execute(
                select(Spool, Material)
                .select_from(Spool)
                .outerjoin(Material, Spool.material_id == Material.id)
                .where(Spool.id.in_(spool_ids))
            )
            for s, m in r_sp.all():
                mat_color = (m.color or "").strip().lower() if m else ""
                hex_val = color_hex_map.get(mat_color, "#888888") if mat_color else "#888888"
                rem = getattr(s, "remaining_length_m", 0) or 0
                if rem >= 250:
                    icon = "reach"
                elif rem >= 50:
                    icon = "midi"
                else:
                    icon = "poor"
                plastic = (getattr(m, "plastic_type", None) or "").strip() if m else ""
                spool_by_id[s.id] = {"hex": hex_val, "icon": icon, "plastic_type": plastic}
    except Exception as e:
        logger.warning("print-queue data load: %s", e)
        spool_by_id = {}
    import json as _json

    def _printer_row(p):
        row = {"id": p.id, "name": p.name or "", "number": p.number or ""}
        sid = getattr(p, "current_spool_id", None)
        if sid is not None:
            row["spool_id"] = sid
        if sid and sid in spool_by_id:
            info = spool_by_id[sid]
            dataurl = _spool_svg_dataurl(
                info["hex"],
                info["icon"],
                size=55,
                plastic_type=info.get("plastic_type") or None,
            )
            if dataurl:
                row["spool_dataurl"] = dataurl
        return row

    return templates.TemplateResponse(
        "print_queue.html",
        {
            "request": request,
            "site_username": user.username,
            "printers": printers,
            "print_jobs": print_jobs,
            "materials": materials,
            "queue_items": queue_items,
            "printers_json": _json.dumps([_printer_row(p) for p in printers], ensure_ascii=False),
            "print_jobs_json": _json.dumps(print_jobs, ensure_ascii=False),
            "materials_json": _json.dumps([{"id": m.id, "name": m.name or "", "color": m.color or "", "plastic_type": getattr(m, "plastic_type", "") or ""} for m in materials], ensure_ascii=False),
        },
    )


@router.get("/api/print-queue/printers-status")
async def api_printers_status(
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Статус принтеров для Ганта:
    - свободен (green)
    - занят (yellow)
    - ошибка (red)
    - не подключен (gray)

    Источник: Z-Mod Moonraker на порту 7125 (попытки /printer/objects/query?print_stats и /printer/info).
    """
    async with _printers_status_cache_lock:
        now_ts = time.monotonic()
        cached_ts = float(_printers_status_cache.get("ts", 0.0) or 0.0)
        cached_data = _printers_status_cache.get("data")
        if (now_ts - cached_ts) < PRINTER_STATUS_CACHE_TTL_SEC and isinstance(cached_data, list):
            return JSONResponse(content=cached_data)

    r = await db.execute(select(Printer).order_by(Printer.number, Printer.name))
    printers = list(r.scalars().all())

    timeout = httpx.Timeout(connect=0.7, read=1.2, write=1.2, pool=0.5)
    async with httpx.AsyncClient(timeout=timeout) as client:
        sem = asyncio.Semaphore(6)

        async def worker(p: Printer) -> dict:
            ip = (getattr(p, "ip_address", None) or "").strip()
            if not ip:
                status = "disconnected"
                return {"printer_id": p.id, "status": status, "title": _printer_status_title(status)}

            async with sem:
                # 1) Основной запрос: print_stats (state printing/paused/standby/...)
                urls_objects = [
                    f"http://{ip}:7125/printer/objects/query?print_stats",
                    # иногда API проксируется на 80 (Fluidd/Mainsail)
                    f"http://{ip}/printer/objects/query?print_stats",
                    f"http://{ip}:80/printer/objects/query?print_stats",
                ]
                data_objects = None
                last_http_status_obj: Optional[int] = None
                klippy_state: Optional[str] = None
                print_stats_state: Optional[str] = None
                for u in urls_objects:
                    data_objects, last_http_status_obj = await _http_try_json_with_status(client, u)
                    if isinstance(data_objects, dict):
                        _, print_stats_state = _extract_state_from_moonraker_objects_query(data_objects)
                        if print_stats_state is not None:
                            break

                # 2) Фолбек: /printer/info
                if print_stats_state is None:
                    urls_info = [
                        f"http://{ip}:7125/printer/info",
                        f"http://{ip}/printer/info",
                        f"http://{ip}:80/printer/info",
                    ]
                    data_info = None
                    last_http_status_info: Optional[int] = None
                    for u in urls_info:
                        data_info, last_http_status_info = await _http_try_json_with_status(client, u)
                        if isinstance(data_info, dict):
                            klippy_state, _ = _extract_state_from_moonraker_printer_info(data_info)
                            if klippy_state is not None:
                                break

                if print_stats_state is None and klippy_state is None:
                    status = "disconnected"
                else:
                    status = _map_printer_status(klippy_state, print_stats_state)

                title = _printer_status_title(status)
                if status == "disconnected":
                    extra = []
                    if last_http_status_obj is not None:
                        extra.append(f"objects: http {last_http_status_obj}")
                    if "last_http_status_info" in locals() and last_http_status_info is not None:
                        extra.append(f"info: http {last_http_status_info}")
                    if extra:
                        title = title + " (" + "; ".join(extra) + ")"

                logger.debug(
                    "printer_status printer_id={} ip={} status={} klippy_state={} print_stats_state={}",
                    p.id,
                    ip,
                    status,
                    klippy_state,
                    print_stats_state,
                )
                return {"printer_id": p.id, "status": status, "title": title}

        results = await asyncio.gather(*[worker(p) for p in printers])

    async with _printers_status_cache_lock:
        _printers_status_cache["ts"] = time.monotonic()
        _printers_status_cache["data"] = results

    return JSONResponse(content=results)


@router.get("/api/print-queue/items")
async def api_print_queue_items(
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Список элементов очереди для Ганта (с вычисленным окончанием)."""
    r_mat = await db.execute(select(Material))
    materials_list = r_mat.scalars().all()
    r_col = await db.execute(select(Color))
    colors_list = r_col.scalars().all()
    color_name_to_hex = {c.name.strip().lower(): (c.hex or "#888888") for c in colors_list if c.name}
    material_id_to_hex = {}
    material_id_to_plastic = {}
    for m in materials_list:
        name = (m.color or "").strip().lower()
        material_id_to_hex[m.id] = color_name_to_hex.get(name, "#888888") if name else "#888888"
        material_id_to_plastic[m.id] = (getattr(m, "plastic_type", None) or "") or ""
    r = await db.execute(
        select(PrintQueueItem, PrintJob)
        .join(PrintJob, PrintQueueItem.print_job_id == PrintJob.id)
        .order_by(PrintQueueItem.sequence.desc())
    )
    rows = r.all()
    result = []
    for item, job in rows:
        start = _ensure_datetime_msk(item.scheduled_start)
        if start is None:
            continue
        dur = _parse_execution_time_minutes(job.execution_time or "")
        end = start + timedelta(minutes=dur) if dur else start
        material_hex = material_id_to_hex.get(item.material_id, "#cccccc") if item.material_id else "#cccccc"
        result.append({
            "id": item.id,
            "sequence": getattr(item, "sequence", 0) or 0,
            "print_job_id": item.print_job_id,
            "printer_id": item.printer_id,
            "material_id": item.material_id,
            "material_hex": material_hex,
            "plastic_type": material_id_to_plastic.get(item.material_id, ""),
            "scheduled_start": start.isoformat(),
            "scheduled_end": end.isoformat(),
            "duration_minutes": dur,
            "job_name": job.name or "",
            "printer_ids": job.printer_ids if isinstance(job.printer_ids, list) else [],
        })
    return JSONResponse(content=result)


@router.post("/api/print-queue/items")
async def api_print_queue_items_create(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Создать или обновить элемент очереди."""
    from dateutil import tz as dateutil_tz
    tz = dateutil_tz.gettz("Europe/Moscow")
    try:
        body = await request.json()
        item_id = body.get("id")
        print_job_id = int(body.get("print_job_id", 0))
        printer_id = int(body.get("printer_id", 0))
        material_id = body.get("material_id")
        if material_id is not None:
            material_id = int(material_id) if material_id != "" else None
        scheduled_start = (body.get("scheduled_start") or "").strip()
        if not print_job_id or not printer_id or not scheduled_start:
            return JSONResponse(status_code=400, content={"detail": "print_job_id, printer_id, scheduled_start required"})
        from dateutil.parser import isoparse
        start_dt = isoparse(scheduled_start)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=tz)
        else:
            start_dt = start_dt.astimezone(tz)
        start_dt = _round_start_to_15_min(start_dt)
        if item_id:
            r = await db.execute(select(PrintQueueItem).where(PrintQueueItem.id == int(item_id)))
            item = r.scalar_one_or_none()
            if not item:
                return JSONResponse(status_code=404, content={"detail": "not found"})
        else:
            from sqlalchemy import func
            r_max = await db.execute(select(func.coalesce(func.max(PrintQueueItem.sequence), 0)))
            next_seq = (r_max.scalar() or 0) + 1
            item = PrintQueueItem(sequence=next_seq, print_job_id=print_job_id, printer_id=printer_id, material_id=material_id, scheduled_start=start_dt)
            db.add(item)
        if item_id:
            item.print_job_id = print_job_id
            item.printer_id = printer_id
            item.material_id = material_id
            item.scheduled_start = start_dt
        await db.commit()
        if not item_id:
            await db.refresh(item)
        return JSONResponse(content={"id": item.id, "sequence": getattr(item, "sequence", 0), "ok": True})
    except Exception as e:
        logger.warning("print-queue item save: %s", e)
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.delete("/api/print-queue/items/{item_id}")
async def api_print_queue_items_delete(
    item_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Удалить элемент очереди."""
    r = await db.execute(select(PrintQueueItem).where(PrintQueueItem.id == item_id))
    item = r.scalar_one_or_none()
    if not item:
        return JSONResponse(status_code=404, content={"detail": "not found"})
    await db.delete(item)
    await db.commit()
    return JSONResponse(content={"ok": True})


@router.post("/api/print-queue/items/bulk-delete")
async def api_print_queue_items_bulk_delete(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Массовое удаление элементов очереди. Body: { "ids": [1, 2, 3, ...] }."""
    try:
        body = await request.json()
        ids_raw = body.get("ids") or []
        ids = [int(x) for x in ids_raw if x is not None and str(x).strip() != ""]
        if not ids:
            return JSONResponse(content={"ok": True, "deleted": 0})
        r = await db.execute(select(PrintQueueItem).where(PrintQueueItem.id.in_(ids)))
        items = list(r.scalars().all())
        for item in items:
            await db.delete(item)
        await db.commit()
        return JSONResponse(content={"ok": True, "deleted": len(items)})
    except Exception as e:
        logger.warning("print-queue bulk-delete: %s", e)
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.patch("/api/print-queue/items/{item_id}")
async def api_print_queue_items_patch(
    item_id: int,
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Обновить время начала и/или принтер (для перетаскивания на Ганте). При наложении сдвигаем старт до ближайшего свободного слота."""
    from dateutil import tz as dateutil_tz
    from dateutil.parser import isoparse
    tz = dateutil_tz.gettz("Europe/Moscow")
    r = await db.execute(
        select(PrintQueueItem, PrintJob)
        .join(PrintJob, PrintQueueItem.print_job_id == PrintJob.id)
        .where(PrintQueueItem.id == item_id)
    )
    row = r.one_or_none()
    if not row:
        return JSONResponse(status_code=404, content={"detail": "not found"})
    item, job = row
    try:
        body = await request.json()
        printer_id = int(body["printer_id"]) if body.get("printer_id") is not None else item.printer_id
        scheduled_start = (body.get("scheduled_start") or "").strip()
        if scheduled_start:
            start_dt = isoparse(scheduled_start)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=tz)
            else:
                start_dt = start_dt.astimezone(tz)
            start_dt = _round_start_to_15_min(start_dt)
            dur = _parse_execution_time_minutes(job.execution_time or "")
            if dur <= 0:
                dur = 60
            # Избежать наложения: сдвинуть старт, если на том же принтере уже есть задания в этом слоте
            r_others = await db.execute(
                select(PrintQueueItem, PrintJob)
                .join(PrintJob, PrintQueueItem.print_job_id == PrintJob.id)
                .where(PrintQueueItem.printer_id == printer_id, PrintQueueItem.id != item_id)
            )
            others_list = list(r_others.all())
            for _ in range(100):  # ограничение итераций
                our_end = start_dt + timedelta(minutes=dur)
                overlap = None
                for other_item, other_job in others_list:
                    other_start = _ensure_datetime_msk(other_item.scheduled_start)
                    if other_start is None:
                        continue
                    other_dur = _parse_execution_time_minutes(other_job.execution_time or "")
                    if other_dur <= 0:
                        other_dur = 60
                    other_end = other_start + timedelta(minutes=other_dur)
                    if start_dt < other_end and other_start < our_end:
                        overlap = _next_slot_after_end(other_end, 15)
                        break
                if overlap is None:
                    break
                start_dt = overlap
            item.scheduled_start = start_dt
        if body.get("printer_id") is not None:
            item.printer_id = printer_id
        await db.commit()
        return JSONResponse(content={"ok": True})
    except Exception as e:
        logger.warning("print-queue item patch: %s", e)
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.get("/print-plan", response_class=HTMLResponse)
async def print_plan(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """План печати на период: выбор изделий и количество, автоматический подсчёт деталей."""
    return templates.TemplateResponse(
        "print_plan.html",
        {"request": request, "site_username": user.username},
    )


@router.get("/api/print-plan/products")
async def api_print_plan_products(
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Список изделий для выбора в плане печати."""
    r = await db.execute(select(Product).order_by(Product.name))
    products = list(r.scalars().all())
    return JSONResponse(content=[
        {"id": p.id, "article": p.article or "", "name": p.name or ""}
        for p in products
    ])


@router.get("/api/print-plan/composition")
async def api_print_plan_composition(
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Состав всех изделий: детали с материалом (и цветом) и доп. материалы. Для фронта — агрегация по плану."""
    r_col = await db.execute(select(Color))
    colors = list(r_col.scalars().all())
    color_name_to_hex = {c.name.strip().lower(): (c.hex or "#888888") for c in colors if c.name}
    r_pr = await db.execute(select(Product).order_by(Product.name))
    products = list(r_pr.scalars().all())
    out = {}
    for prod in products:
        parts_list = []
        r_pp = await db.execute(
            select(ProductPart, Part, Material)
            .join(Part, ProductPart.part_id == Part.id)
            .outerjoin(Material, ProductPart.material_id == Material.id)
            .where(ProductPart.product_id == prod.id)
        )
        for pp, part, mat in r_pp.all():
            mat_name = (mat.name or "") if mat else ""
            mat_color = (mat.color or "").strip().lower() if mat else ""
            hex_val = color_name_to_hex.get(mat_color, "#888888") if mat_color else "#888888"
            parts_list.append({
                "part_name": part.name or "",
                "material_name": mat_name,
                "material_hex": hex_val,
                "quantity": pp.quantity or 1,
            })
        r_em = await db.execute(
            select(ProductExtraMaterial, ExtraMaterial)
            .join(ExtraMaterial, ProductExtraMaterial.extra_material_id == ExtraMaterial.id)
            .where(ProductExtraMaterial.product_id == prod.id)
        )
        extras_list = [
            {"extra_name": em.name or "", "quantity": pem.quantity or 1}
            for pem, em in r_em.all()
        ]
        out[prod.id] = {"parts": parts_list, "extras": extras_list}
    return JSONResponse(content=out)


# Хранение плана печати — 6 месяцев
PRINT_PLAN_RETENTION_DAYS = 180


def _week_start_from_date(d: datetime) -> str:
    """Понедельник недели для даты d в формате YYYY-MM-DD."""
    from datetime import date
    if isinstance(d, datetime):
        d = d.date()
    monday = d - timedelta(days=d.weekday())  # 0 = Monday
    return monday.strftime("%Y-%m-%d")


@router.get("/api/print-plan/plan")
async def api_print_plan_get(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Получить сохранённый план на неделю. week_start=YYYY-MM-DD (понедельник); если не передан — текущая неделя."""
    week = request.query_params.get("week_start", "").strip()
    if not week:
        week = _week_start_from_date(datetime.now(MSK))
    r = await db.execute(select(PrintPlan).where(PrintPlan.week_start == week))
    plan = r.scalar_one_or_none()
    if not plan:
        return JSONResponse(content={"week_start": week, "items": []})
    r_items = await db.execute(
        select(PrintPlanItem).where(PrintPlanItem.print_plan_id == plan.id).order_by(PrintPlanItem.id)
    )
    items = [
        {"product_id": it.product_id, "quantity": it.quantity}
        for it in r_items.scalars().all()
    ]
    return JSONResponse(content={"week_start": week, "items": items})


@router.post("/api/print-plan/plan")
async def api_print_plan_save(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Сохранить план на неделю. body: { week_start: "YYYY-MM-DD", items: [{ product_id, quantity }] }. Удаляются планы старше 6 месяцев."""
    try:
        body = await request.json()
        week = (body.get("week_start") or "").strip()
        if not week:
            week = _week_start_from_date(datetime.now(MSK))
        items_raw = body.get("items") or []
        items = []
        for it in items_raw:
            pid = it.get("product_id")
            qty = int(it.get("quantity") or 0)
            if pid is not None and qty > 0:
                items.append({"product_id": int(pid), "quantity": qty})

        # Удалить планы старше 6 месяцев
        from datetime import date
        cutoff = (date.today() - timedelta(days=PRINT_PLAN_RETENTION_DAYS)).strftime("%Y-%m-%d")
        await db.execute(delete(PrintPlan).where(PrintPlan.week_start < cutoff))

        r = await db.execute(select(PrintPlan).where(PrintPlan.week_start == week))
        plan = r.scalar_one_or_none()
        if plan:
            await db.execute(delete(PrintPlanItem).where(PrintPlanItem.print_plan_id == plan.id))
        else:
            plan = PrintPlan(week_start=week)
            db.add(plan)
            await db.flush()
        for it in items:
            db.add(PrintPlanItem(print_plan_id=plan.id, product_id=it["product_id"], quantity=it["quantity"]))
        await db.commit()
        return JSONResponse(content={"ok": True, "week_start": week})
    except Exception as e:
        logger.warning("print-plan save: %s", e)
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.post("/api/print-plan/transfer-preview")
async def api_print_plan_transfer_preview(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Превью: какие задания будут созданы при переносе плана (без записи в очередь)."""
    try:
        body = await request.json()
        items_raw = body.get("items") or []
        plan_items = []
        for it in items_raw:
            pid = it.get("product_id")
            qty = int(it.get("quantity") or 0)
            if pid is not None and qty > 0:
                plan_items.append({"product_id": int(pid), "quantity": qty})
        if not plan_items:
            return JSONResponse(content={"jobs": []})

        agg_parts: dict[tuple[int, int | None], int] = {}
        product_ids = [x["product_id"] for x in plan_items]
        qty_by_product = {x["product_id"]: x["quantity"] for x in plan_items}
        r_pp = await db.execute(
            select(ProductPart).where(ProductPart.product_id.in_(product_ids))
        )
        for pp in r_pp.scalars().all():
            key = (pp.part_id, pp.material_id)
            agg_parts[key] = agg_parts.get(key, 0) + qty_by_product.get(pp.product_id, 0) * (pp.quantity or 1)
        if not agg_parts:
            return JSONResponse(content={"jobs": []})

        r_jobs = await db.execute(select(PrintJob))
        jobs = list(r_jobs.scalars().all())
        part_id_to_jobs: dict[int, list[tuple[int, int]]] = {}
        for job in jobs:
            pqs = job.part_quantities
            if isinstance(pqs, str):
                try:
                    pqs = _json.loads(pqs)
                except Exception:
                    pqs = []
            if not isinstance(pqs, list):
                pqs = []
            for pq in pqs:
                pid = pq.get("part_id") if isinstance(pq, dict) else None
                q = int(pq.get("qty") or 0) if isinstance(pq, dict) else 0
                if pid is not None and q > 0:
                    part_id_to_jobs.setdefault(pid, []).append((job.id, q))

        job_material_runs: dict[tuple[int, int | None], int] = {}
        for (part_id, material_id), need in agg_parts.items():
            for job_id, qty_in_job in part_id_to_jobs.get(part_id, []):
                runs = math.ceil(need / qty_in_job)
                key = (job_id, material_id)
                job_material_runs[key] = max(job_material_runs.get(key, 0), runs)
        job_runs_total: dict[int, int] = {}
        for (job_id, _mat_id), runs in job_material_runs.items():
            job_runs_total[job_id] = job_runs_total.get(job_id, 0) + runs

        jobs_out = []
        for job in jobs:
            if job.id not in job_runs_total:
                continue
            pids = job.printer_ids if isinstance(job.printer_ids, list) else []
            if isinstance(job.printer_ids, str):
                try:
                    pids = _json.loads(job.printer_ids)
                except Exception:
                    pids = []
            if not pids:
                continue
            jobs_out.append({
                "job_id": job.id,
                "job_name": job.name or "",
                "runs": job_runs_total[job.id],
            })
        jobs_out.sort(key=lambda x: (x["job_name"], x["job_id"]))
        return JSONResponse(content={"jobs": jobs_out})
    except Exception as e:
        logger.warning("print-plan transfer-preview: %s", e)
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.post("/api/print-plan/transfer-to-queue")
async def api_print_plan_transfer_to_queue(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Перенести план в задания на печать: по потребности деталей вычислить кол-во заданий,
    распределить по принтерам на выбранную неделю (одинаковый материал — подряд).
    Body: { week_start, items, selected_job_ids }.
    """
    try:
        body = await request.json()
        week = (body.get("week_start") or "").strip()
        if not week:
            week = _week_start_from_date(datetime.now(MSK))
        items_raw = body.get("items") or []
        plan_items = []
        for it in items_raw:
            pid = it.get("product_id")
            qty = int(it.get("quantity") or 0)
            if pid is not None and qty > 0:
                plan_items.append({"product_id": int(pid), "quantity": qty})
        if not plan_items:
            return JSONResponse(status_code=400, content={"detail": "План пуст. Добавьте изделия и количество."})

        # 1) Агрегат деталей по плану: (part_id, material_id) -> суммарное кол-во
        agg_parts: dict[tuple[int, int | None], int] = {}
        part_id_to_material: dict[int, int | None] = {}
        product_ids = [x["product_id"] for x in plan_items]
        qty_by_product = {x["product_id"]: x["quantity"] for x in plan_items}
        r_pp = await db.execute(
            select(ProductPart).where(ProductPart.product_id.in_(product_ids))
        )
        for pp in r_pp.scalars().all():
            key = (pp.part_id, pp.material_id)
            agg_parts[key] = agg_parts.get(key, 0) + qty_by_product.get(pp.product_id, 0) * (pp.quantity or 1)
            if pp.part_id not in part_id_to_material:
                part_id_to_material[pp.part_id] = pp.material_id

        if not agg_parts:
            return JSONResponse(status_code=400, content={"detail": "По выбранным изделиям нет состава деталей."})

        # 2) Задания на печать: part_id -> [(job_id, qty в задании)]
        r_jobs = await db.execute(select(PrintJob))
        jobs = list(r_jobs.scalars().all())
        part_id_to_jobs: dict[int, list[tuple[int, int]]] = {}
        for job in jobs:
            pqs = job.part_quantities
            if isinstance(pqs, str):
                try:
                    pqs = _json.loads(pqs)
                except Exception:
                    pqs = []
            if not isinstance(pqs, list):
                pqs = []
            for pq in pqs:
                pid = pq.get("part_id") if isinstance(pq, dict) else None
                q = int(pq.get("qty") or 0) if isinstance(pq, dict) else 0
                if pid is not None and q > 0:
                    part_id_to_jobs.setdefault(pid, []).append((job.id, q))

        # 3) Запуски по (job_id, material_id): учитываем цвет/материал детали
        job_material_runs: dict[tuple[int, int | None], int] = {}
        for (part_id, material_id), need in agg_parts.items():
            for job_id, qty_in_job in part_id_to_jobs.get(part_id, []):
                runs = math.ceil(need / qty_in_job)
                key = (job_id, material_id)
                job_material_runs[key] = max(job_material_runs.get(key, 0), runs)

        if not job_material_runs:
            return JSONResponse(status_code=400, content={"detail": "Нет заданий на печать, содержащих нужные детали."})

        # 4) Список (job_id, material_id, duration_min); только задания с указанными принтерами
        GAP_AFTER_TASK_MINUTES = 15
        job_id_to_info: dict[int, tuple[list[int], int, str]] = {}
        job_ids_needed = {job_id for (job_id, _) in job_material_runs}
        for job in jobs:
            if job.id not in job_ids_needed:
                continue
            pids = job.printer_ids if isinstance(job.printer_ids, list) else []
            if isinstance(job.printer_ids, str):
                try:
                    pids = _json.loads(job.printer_ids)
                except Exception:
                    pids = []
            if not pids:
                continue
            dur = _parse_execution_time_minutes(job.execution_time or "")
            job_id_to_info[job.id] = (pids, dur, job.name or "")

        expanded: list[tuple[int, int | None, int]] = []
        for (job_id, material_id), runs in job_material_runs.items():
            if job_id not in job_id_to_info:
                continue
            _pids, dur, _name = job_id_to_info[job_id]
            for _ in range(runs):
                expanded.append((job_id, material_id, dur))
        expanded.sort(key=lambda x: (x[1] or 0, x[0]))

        selected_job_ids = body.get("selected_job_ids")
        if selected_job_ids is not None:
            selected_set = set(int(x) for x in selected_job_ids if x is not None)
            expanded = [x for x in expanded if x[0] in selected_set]

        # 5) Неделя: понедельник 00:00 и первый рабочий слот 8:00 MSK (одна логика для любой выбранной недели)
        week_start_dt = datetime.strptime(week, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=MSK)
        week_end_dt = week_start_dt + timedelta(days=7)
        week_first_slot = week_start_dt.replace(hour=8, minute=0, second=0, microsecond=0)
        r_printers = await db.execute(select(Printer))
        printers_list = r_printers.scalars().all()
        all_printers = [p.id for p in printers_list]
        if not all_printers:
            return JSONResponse(status_code=400, content={"detail": "Нет принтеров в системе."})
        material_ids = list({x[1] for x in expanded if x[1] is not None})
        materials_ref: dict[int, str] = {}
        if material_ids:
            r_mat = await db.execute(select(Material).where(Material.id.in_(material_ids)))
            for m in r_mat.scalars().all():
                label = (m.name or "").strip()
                if (m.color or "").strip():
                    label = f"{label} ({m.color.strip()})" if label else m.color.strip()
                materials_ref[m.id] = label or str(m.id)

        # 6) Следующий свободный слот по принтеру (база — понедельник 8:00 выбранной недели, без различия текущая/следующая)
        next_free: dict[int, datetime] = {pid: week_first_slot for pid in all_printers}
        last_material: dict[int, int | None] = {}
        r_existing = await db.execute(
            select(PrintQueueItem, PrintJob)
            .join(PrintJob, PrintQueueItem.print_job_id == PrintJob.id)
            .order_by(PrintQueueItem.printer_id, PrintQueueItem.scheduled_start)
        )
        # Минимальная длительность для уже стоящих в очереди заданий без execution_time — не считать слот свободным сразу после start
        MIN_DURATION_FOR_EXISTING_MINUTES = 60
        for item, job in r_existing.all():
            start = _ensure_datetime_msk(item.scheduled_start)
            if start is None:
                continue
            if start < week_start_dt or start >= week_end_dt:
                continue
            dur = _parse_execution_time_minutes(job.execution_time or "")
            if dur <= 0:
                dur = MIN_DURATION_FOR_EXISTING_MINUTES
            end = start + timedelta(minutes=dur)
            next_start = _next_slot_after_end(end, GAP_AFTER_TASK_MINUTES)
            pid = item.printer_id
            if pid in next_free and next_start > next_free[pid]:
                next_free[pid] = next_start
            last_material[pid] = item.material_id

        # 7) Максимальный sequence для новых записей
        r_max = await db.execute(select(func.coalesce(func.max(PrintQueueItem.sequence), 0)))
        next_seq = (r_max.scalar() or 0) + 1
        # 8) Расстановка по принтерам и времени (детерминированный планировщик).
        assignments = _run_deterministic_planner(
            expanded, job_id_to_info, all_printers, next_free, last_material,
            week_start_dt, week_end_dt, GAP_AFTER_TASK_MINUTES,
        )
        if assignments is None:
            return JSONResponse(status_code=400, content={"detail": "Не удалось построить план без коллизий."})

        # 10) Запись в БД
        created = 0
        for (printer_id, start_dt, _end_dt, job_id, material_id, _duration_min) in assignments:
            item = PrintQueueItem(
                sequence=next_seq,
                print_job_id=job_id,
                printer_id=printer_id,
                material_id=material_id,
                scheduled_start=start_dt,
            )
            db.add(item)
            next_seq += 1
            created += 1
        await db.commit()
        resp = {"ok": True, "created": created}
        return JSONResponse(content=resp)
    except Exception as e:
        logger.warning(f"print-plan transfer-to-queue: {e}")
        return JSONResponse(status_code=400, content={"detail": str(e)})


def _normalize_day_counts(day_counts):  # noqa: C901
    """Привести day_counts из БД к list[int]. В миграции колонка TEXT — может прийти строка JSON."""
    if day_counts is None:
        return []
    if isinstance(day_counts, list):
        return [int(x) if x is not None and str(x).lstrip("-").isdigit() else 0 for x in day_counts]
    if isinstance(day_counts, str):
        try:
            parsed = _json.loads(day_counts)
            if isinstance(parsed, list):
                return [int(x) if x is not None and str(x).lstrip("-").isdigit() else 0 for x in parsed]
        except Exception:
            return []
        return []
    return []


async def _load_supply_queue_cluster_scan(
    db: AsyncSession,
    *,
    max_days: int = 21,
) -> tuple[Optional[SupplyQueueScan], list[dict], list[date], str]:
    """Последний непустой скан: строки кластеров, даты колонок (от даты скана), подпись времени скана."""
    scan: Optional[SupplyQueueScan] = None
    results: list[dict] = []
    week_dates: list[date] = []
    scanned_at_str = ""
    r_scans = await db.execute(
        select(SupplyQueueScan).order_by(SupplyQueueScan.scanned_at.desc()).limit(30)
    )
    last_scan = None
    for candidate in r_scans.scalars().all():
        try:
            r_cnt = await db.execute(
                select(func.count(SupplyQueueResult.id)).where(SupplyQueueResult.scan_id == candidate.id)
            )
            cnt = int(r_cnt.scalar() or 0)
        except Exception:
            cnt = 0
        if cnt > 0:
            last_scan = candidate
            break
    if last_scan:
        scan = last_scan
        r_res = await db.execute(
            select(SupplyQueueResult)
            .where(SupplyQueueResult.scan_id == last_scan.id)
            .order_by(SupplyQueueResult.cluster_name)
        )
        raw_results = r_res.scalars().all()
        for r in raw_results:
            dc = _normalize_day_counts(getattr(r, "day_counts", None))
            if max_days > 0 and len(dc) > max_days:
                dc = dc[:max_days]
            results.append({
                "cluster_id": getattr(r, "cluster_id", None) or 0,
                "cluster_name": (getattr(r, "cluster_name", None) or "").strip(),
                "day_counts": dc,
            })
        if scan and scan.scanned_at:
            dt = scan.scanned_at
            if hasattr(dt, "astimezone"):
                dt = dt.astimezone(MSK)
            week_start = dt.date() if hasattr(dt, "date") else datetime.fromisoformat(str(dt)).date()
            n = max_days if max_days > 0 else 21
            week_dates = [week_start + timedelta(days=i) for i in range(n)]
    if scan and scan.scanned_at:
        _dt = scan.scanned_at
        if hasattr(_dt, "strftime"):
            if getattr(_dt, "tzinfo", None) is None:
                _dt = _dt.replace(tzinfo=timezone.utc)
            if hasattr(_dt, "astimezone"):
                _dt = _dt.astimezone(MSK)
            scanned_at_str = _dt.strftime("%d.%m.%Y %H:%M") + " МСК"
        else:
            scanned_at_str = str(scan.scanned_at)
    return scan, results, week_dates, scanned_at_str


# Запасной delivery_info, если в БД нет supply_draft_config (как в slots_tracker.DEFAULT_DRAFT_BODY).
_SUPPLY_DRAFT_FALLBACK_DELIVERY: Dict[str, object] = {
    "drop_off_warehouse_id": 1020005000295764,
    "warehouse_type": "CROSS_DOCK",
    "seller_warehouse_id": 1020005008005660,
}


async def _delivery_defaults_from_supply_draft_config(db: AsyncSession) -> dict:
    """Плоские поля для cluster_info.delivery_info в POST /api/supplies/draft."""
    r = await db.execute(select(SupplyDraftConfig).limit(1))
    row = r.scalar_one_or_none()
    di: dict = {}
    if row is not None and getattr(row, "draft_body", None):
        body = row.draft_body
        if isinstance(body, dict):
            raw = body.get("delivery_info")
            if isinstance(raw, dict):
                di = raw
    drop_id = di.get("drop_off_warehouse_id")
    wh_type = (di.get("warehouse_type") or "").strip() or None
    seller = di.get("seller_warehouse_id")
    dow = di.get("drop_off_warehouse")
    if isinstance(dow, dict):
        if drop_id in (None, 0, "0"):
            drop_id = dow.get("warehouse_id")
        if not wh_type:
            wh_type = (dow.get("warehouse_type") or "").strip() or None
    fb = _SUPPLY_DRAFT_FALLBACK_DELIVERY
    try:
        drop_i = int(drop_id) if drop_id not in (None, "", 0, "0") else int(fb["drop_off_warehouse_id"])
    except (TypeError, ValueError):
        drop_i = int(fb["drop_off_warehouse_id"])
    try:
        seller_i = int(seller) if seller not in (None, "", 0, "0") else int(fb["seller_warehouse_id"])
    except (TypeError, ValueError):
        seller_i = int(fb["seller_warehouse_id"])
    wt = wh_type or str(fb["warehouse_type"])
    return {
        "drop_off_warehouse_id": drop_i,
        "warehouse_type": wt,
        "seller_warehouse_id": seller_i,
    }


# POST /v3/supply-order/list — в filter.states нужно передать хотя бы один статус; собираем все из доки Ozon.
SUPPLY_ORDER_LIST_API_STATES = [
    "DATA_FILLING",
    "READY_TO_SUPPLY",
    "ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "REPORTS_CONFIRMATION_AWAITING",
    "REPORT_REJECTED",
    "COMPLETED",
    "REJECTED_AT_SUPPLY_WAREHOUSE",
    "CANCELLED",
    "OVERDUE",
]

# Подписи state из документации Ozon Seller API (v3/supply-order/get, поле state)
SUPPLY_ORDER_STATE_LABELS_RU: dict[str, str] = {
    "UNSPECIFIED": "не определён",
    "DATA_FILLING": "заполнение данных",
    "READY_TO_SUPPLY": "готова к отгрузке",
    "ACCEPTED_AT_SUPPLY_WAREHOUSE": "принята на точке отгрузки",
    "IN_TRANSIT": "в пути",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE": "приёмка на складе",
    "REPORTS_CONFIRMATION_AWAITING": "согласование актов",
    "REPORT_REJECTED": "спор",
    "COMPLETED": "завершена",
    "REJECTED_AT_SUPPLY_WAREHOUSE": "отказано в приёмке",
    "CANCELLED": "отменена",
    "OVERDUE": "просрочена",
}

# Порядок чекбоксов фильтра статусов на /supply-queue (все коды из list API + не определён).
SUPPLY_QUEUE_UI_STATUS_ORDER: tuple[str, ...] = tuple(SUPPLY_ORDER_LIST_API_STATES) + ("UNSPECIFIED",)


def _supply_queue_allowed_status_codes() -> frozenset[str]:
    return frozenset(SUPPLY_QUEUE_UI_STATUS_ORDER)


def _supply_queue_row_state_normalized(row: dict) -> str:
    st = str(row.get("state_code") or row.get("state") or "UNSPECIFIED").strip().upper()
    if st.startswith("ORDER_STATE_"):
        st = st[len("ORDER_STATE_") :]
    return st


def _supply_queue_row_filter_status_code(row: dict) -> str:
    """Код для фильтра/счётчиков: неизвестные Ozon-коды считаем UNSPECIFIED."""
    st = _supply_queue_row_state_normalized(row)
    if st in _supply_queue_allowed_status_codes():
        return st
    return "UNSPECIFIED"


def _supply_queue_default_included_statuses() -> set[str]:
    """По умолчанию не показываем завершённые заявки."""
    return set(SUPPLY_QUEUE_UI_STATUS_ORDER) - {"COMPLETED"}


def _supply_queue_resolve_included_statuses(sq_st_query: list[str] | None) -> set[str]:
    allowed = _supply_queue_allowed_status_codes()
    default = _supply_queue_default_included_statuses()
    if sq_st_query is None:
        return set(default)
    picked: set[str] = set()
    for raw in sq_st_query:
        s = str(raw or "").strip().upper()
        if s in allowed:
            picked.add(s)
    if not picked:
        return set(default)
    return picked


def _supply_queue_status_counts_map(rows: list[dict]) -> dict[str, int]:
    cnt: dict[str, int] = {code: 0 for code in SUPPLY_QUEUE_UI_STATUS_ORDER}
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        st = _supply_queue_row_filter_status_code(r)
        cnt[st] = cnt.get(st, 0) + 1
    return cnt


def _supply_queue_status_filter_chips(included: set[str], counts: dict[str, int]) -> list[dict]:
    out: list[dict] = []
    for code in SUPPLY_QUEUE_UI_STATUS_ORDER:
        label = SUPPLY_ORDER_STATE_LABELS_RU.get(code, code)
        out.append(
            {
                "code": code,
                "label": label,
                "count": int(counts.get(code, 0)),
                "checked": code in included,
            }
        )
    return out


def _supply_order_state_badge_class(state_code: str) -> str:
    """CSS-модификатор для цветного тега статуса (очередь поставок)."""
    s = (state_code or "").strip().upper()
    return {
        "UNSPECIFIED": "supply-state--muted",
        "DATA_FILLING": "supply-state--info",
        "READY_TO_SUPPLY": "supply-state--success",
        "ACCEPTED_AT_SUPPLY_WAREHOUSE": "supply-state--teal",
        "IN_TRANSIT": "supply-state--transit",
        "ACCEPTANCE_AT_STORAGE_WAREHOUSE": "supply-state--warehouse",
        "REPORTS_CONFIRMATION_AWAITING": "supply-state--awaiting",
        "REPORT_REJECTED": "supply-state--dispute",
        "COMPLETED": "supply-state--done",
        "REJECTED_AT_SUPPLY_WAREHOUSE": "supply-state--rejected",
        "CANCELLED": "supply-state--cancelled",
        "OVERDUE": "supply-state--overdue",
    }.get(s, "supply-state--unknown")


def _supply_queue_shipment_date_range_msk(
    date_from_q: str | None,
    date_to_q: str | None,
) -> tuple[date, date]:
    """
    Границы фильтра по дате отгрузки (таймслот, дата в МСК).
    По умолчанию: сегодня ± 30 дней.
    Свой диапазон применяется только если в запросе заданы обе даты (иначе — снова дефолт).
    Так не «липнет» одна граница из старого URL/bookmark к новым дефолтам.
    """
    today = datetime.now(MSK).date()
    default_from = today - timedelta(days=30)
    default_to = today + timedelta(days=30)

    has_from = bool(date_from_q and str(date_from_q).strip())
    has_to = bool(date_to_q and str(date_to_q).strip())
    if not has_from or not has_to:
        return (default_from, default_to)

    try:
        df = datetime.strptime(str(date_from_q).strip()[:10], "%Y-%m-%d").date()
        dt_end = datetime.strptime(str(date_to_q).strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return (default_from, default_to)
    if df > dt_end:
        df, dt_end = dt_end, df
    return (df, dt_end)


def _supply_queue_row_passes_filters(
    row: dict,
    date_from: date,
    date_to: date,
    show_cancelled: bool,
) -> bool:
    st = str(row.get("state_code") or "").strip().upper()
    if st == "CANCELLED" and not show_cancelled:
        return False
    d_msk = row.get("shipment_date_msk")
    if not isinstance(d_msk, date):
        return True
    return date_from <= d_msk <= date_to


# Снимок таблицы «Поставки» в сессии сайта (без повторных запросов к Ozon при F5).
SUPPLY_QUEUE_SESSION_SNAPSHOT_KEY = "supply_queue_supply_snapshot"
# Полный снимок хранится в файле (cookie только ref — иначе большой список заявок ломает ответ/редирект).
SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX = "sqs1:"
SUPPLY_QUEUE_SNAPSHOT_TTL_SEC = 86400
SUPPLY_QUEUE_COMPOSITION_JOB_BACKLOG_MAX = 500
SUPPLY_QUEUE_SHARED_SNAPSHOT_REF_FILENAME = "server_supply_snapshot_ref.txt"
SUPPLY_QUEUE_COMPOSITION_JOB_STATE_FILENAME = "supply_queue_composition_job_state.json"
# Устар.: используйте settings.supply_queue_bundle_delay_sec (оставлено для совместимости импортов).
SUPPLY_QUEUE_COMPOSITION_AJAX_DELAY_SEC = 0.15
# Текст шага «Обновить данные» для опроса /api/supply-queue/refresh-progress (in-process).
_SUPPLY_QUEUE_REFRESH_PROGRESS: dict[str, dict[str, str]] = {}


def _supply_queue_refresh_progress_user_key(user: User) -> str:
    u = getattr(user, "username", None)
    if u is not None and str(u).strip():
        return str(u).strip()
    return f"id:{getattr(user, 'id', '')}"


def _supply_queue_refresh_progress_set(user: User, message: str) -> None:
    _SUPPLY_QUEUE_REFRESH_PROGRESS[_supply_queue_refresh_progress_user_key(user)] = {
        "message": message,
        "at": datetime.now(MSK).replace(microsecond=0).isoformat(),
    }


def _supply_queue_refresh_progress_clear(user: User) -> None:
    _SUPPLY_QUEUE_REFRESH_PROGRESS.pop(_supply_queue_refresh_progress_user_key(user), None)
# В снимке сессии — только поля для таблицы «Поставки» и фильтров.
SUPPLY_QUEUE_SESSION_ROW_KEYS = (
    "order_id",
    "order_number",
    "primary_supply_id",
    "storage_cluster_line",
    "storage_warehouse_line",
    "shipment_at",
    "cargo_units",
    "state_code",
    "state_label_ru",
    "state_badge_class",
    "shipment_date_msk",
    "can_cancel",
    # Для уголков «Остатки по складам»: сопоставление с кластерами, когда строка кластера в снимке «—».
    "macrolocal_cluster_id",
    # Для фонового обновления грузомест без повторного v3/get.
    "supply_ids_for_cargoes",
    "order_bundle_id",
)


def _supply_queue_snapshot_dir() -> Path:
    return Path(tempfile.gettempdir()) / "mpinformer_supply_snapshots"


def _store_supply_queue_snapshot_data(payload: dict) -> str:
    d = _supply_queue_snapshot_dir()
    d.mkdir(parents=True, exist_ok=True)
    sid = uuid.uuid4().hex
    path = d / f"{sid}.json"
    tmp = d / f".{sid}.tmp"
    raw = _json.dumps(payload, ensure_ascii=False, default=str)
    tmp.write_text(raw, encoding="utf-8")
    tmp.replace(path)
    return f"{SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX}{sid}"


def _load_supply_queue_snapshot_data(ref: str) -> dict | None:
    if not isinstance(ref, str) or not ref.startswith(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX):
        return None
    sid = ref[len(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX) :]
    if not sid or len(sid) != 32 or any(c not in "0123456789abcdef" for c in sid):
        return None
    path = _supply_queue_snapshot_dir() / f"{sid}.json"
    if not path.is_file():
        return None
    try:
        age_sec = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
        if age_sec > SUPPLY_QUEUE_SNAPSHOT_TTL_SEC:
            path.unlink(missing_ok=True)
            return None
        data = _json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.warning("supply_queue: чтение снимка Поставок с диска: {}", e)
        return None


def _supply_queue_snapshot_from_session(session: dict) -> dict | None:
    raw = session.get(SUPPLY_QUEUE_SESSION_SNAPSHOT_KEY)
    if not isinstance(raw, dict):
        return None
    ref = raw.get("ref")
    if isinstance(ref, str) and ref.startswith(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX):
        loaded = _load_supply_queue_snapshot_data(ref)
        return loaded if isinstance(loaded, dict) else None
    if "rows" in raw or raw.get("list_error") or raw.get("get_error"):
        return raw
    return None


def _supply_queue_shared_ref_path() -> Path:
    return _supply_queue_snapshot_dir() / SUPPLY_QUEUE_SHARED_SNAPSHOT_REF_FILENAME


def _supply_queue_composition_job_state_path() -> Path:
    return _supply_queue_snapshot_dir() / SUPPLY_QUEUE_COMPOSITION_JOB_STATE_FILENAME


def _supply_queue_read_shared_snapshot_ref() -> str | None:
    p = _supply_queue_shared_ref_path()
    if not p.is_file():
        return None
    try:
        s = p.read_text(encoding="utf-8").strip()
        if s.startswith(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX):
            return s
    except OSError:
        pass
    return None


def _supply_queue_write_shared_snapshot_ref(ref: str | None) -> None:
    p = _supply_queue_shared_ref_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        if not ref:
            p.unlink(missing_ok=True)
            return
        p.write_text(ref.strip(), encoding="utf-8")
    except OSError as e:
        logger.warning("supply_queue: запись shared ref: {}", e)


def _supply_queue_default_composition_job_state() -> dict:
    return {
        "running": False,
        "last_trigger": "",
        "last_started_at": None,
        "last_finished_at": None,
        "last_error": "",
        "last_ref": "",
        "backlog": [],
    }


def _supply_queue_load_composition_job_state() -> dict:
    p = _supply_queue_composition_job_state_path()
    out = _supply_queue_default_composition_job_state()
    if not p.is_file():
        return out
    try:
        raw = _json.loads(p.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            out.update(raw)
            if not isinstance(out.get("backlog"), list):
                out["backlog"] = []
    except Exception as e:
        logger.warning("supply_queue: чтение job state: {}", e)
    return out


def _supply_queue_save_composition_job_state(st: dict) -> None:
    p = _supply_queue_composition_job_state_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        bl = st.get("backlog")
        if isinstance(bl, list) and len(bl) > SUPPLY_QUEUE_COMPOSITION_JOB_BACKLOG_MAX:
            st = dict(st)
            st["backlog"] = bl[-SUPPLY_QUEUE_COMPOSITION_JOB_BACKLOG_MAX :]
        p.write_text(_json.dumps(st, ensure_ascii=False, indent=0), encoding="utf-8")
    except OSError as e:
        logger.warning("supply_queue: запись job state: {}", e)


def _query_param_truthy(raw: str | None) -> bool:
    if raw is None:
        return False
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _supply_queue_rows_for_session(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        d: dict = {}
        for k in SUPPLY_QUEUE_SESSION_ROW_KEYS:
            v = r.get(k)
            if k == "primary_supply_id":
                v = str(v).strip() if v is not None else ""
            elif k == "shipment_date_msk":
                if isinstance(v, date):
                    v = v.isoformat()
                elif v is None:
                    v = None
                else:
                    v = str(v)[:32]
            elif k == "can_cancel":
                v = bool(v)
            elif k == "macrolocal_cluster_id":
                if v is None:
                    v = None
                else:
                    try:
                        v = int(v)
                    except (TypeError, ValueError):
                        v = None
            elif k == "supply_ids_for_cargoes":
                v = v if isinstance(v, list) else []
                v = [str(x).strip() for x in v if str(x).strip()]
            elif k == "order_bundle_id":
                v = str(v).strip() if v is not None and str(v).strip() else ""
            elif k == "order_id":
                v = str(v).strip() if v is not None else ""
            elif k == "cargo_units":
                v = v if isinstance(v, list) else []
            d[k] = v
        out.append(d)
    return out


def _normalize_supply_queue_order_id(raw: str | None) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if s.isdigit():
        return s.zfill(8)
    return s


def _cargo_units_have_line_items(cu: object) -> bool:
    """Есть ли в грузоместах позиции (после bundle), чтобы не затирать свежий ответ API старым кешем."""
    if not isinstance(cu, list):
        return False
    for c in cu:
        if not isinstance(c, dict):
            continue
        li = c.get("line_items") or c.get("items") or []
        if isinstance(li, list) and len(li) > 0:
            return True
    return False


def _supply_queue_row_state_normalized(row: dict) -> str:
    st = str(row.get("state_code") or row.get("state") or "").strip().upper()
    if st.startswith("ORDER_STATE_"):
        st = st[len("ORDER_STATE_") :]
    return st


def _supply_queue_row_is_completed(row: dict) -> bool:
    return _supply_queue_row_state_normalized(row) == "COMPLETED"


def _composition_cache_entry_for_oid(cache: dict | None, oid: str) -> dict | None:
    if not cache or not isinstance(cache, dict) or not oid:
        return None
    e = cache.get(oid)
    if e is None and oid.isdigit():
        e = cache.get(str(int(oid)))
    return e if isinstance(e, dict) else None


def _merge_completed_supply_rows_from_prev_cache(rows: list[dict], prev_cc: dict | None) -> None:
    """Для COMPLETED подставляем грузоместа с составом из прошлого снимка (не трогаем свежий API-скелет)."""
    if not rows or not prev_cc:
        return
    for row in rows:
        if not isinstance(row, dict) or not _supply_queue_row_is_completed(row):
            continue
        oid = _normalize_supply_queue_order_id(str(row.get("order_id") or ""))
        if not oid:
            continue
        pe = _composition_cache_entry_for_oid(prev_cc, oid)
        if not pe:
            continue
        cu = pe.get("cargo_units")
        if isinstance(cu, list) and _cargo_units_have_line_items(cu):
            row["cargo_units"] = deepcopy(cu)


def _apply_composition_cache_to_rows(rows: list[dict], cache: dict | None) -> None:
    """Подставляет в строки сохранённый товарный состав по грузоместам из снимка."""
    if not cache or not isinstance(cache, dict):
        return
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        oid = _normalize_supply_queue_order_id(str(row.get("order_id") or ""))
        if not oid:
            continue
        entry = cache.get(oid)
        if entry is None and oid.isdigit():
            entry = cache.get(str(int(oid)))
        if not isinstance(entry, dict):
            continue
        cu = entry.get("cargo_units")
        if isinstance(cu, list):
            if _cargo_units_have_line_items(row.get("cargo_units")):
                continue
            row["cargo_units"] = cu


def _supply_queue_row_shipment_date_msk_only(row: dict) -> date | None:
    """Дата отгрузки (МСК) из строки снимка очереди поставок."""
    d = row.get("shipment_date_msk")
    if isinstance(d, date):
        return d
    if isinstance(d, str) and len(d) >= 10:
        try:
            return date.fromisoformat(d[:10])
        except ValueError:
            return None
    return None


def _index_upcoming_supplies_from_supply_queue_snapshot(
    session: dict, today: date, end_date: date
) -> list[dict] | None:
    """
    Блок «Поставки на 7 дней» на главной — из того же снимка, что страница /supply-queue.
    Возвращает None, если пользователь ещё ни разу не загружал очередь (снимка нет) —
    тогда можно показать fallback из БД ozon_supplies.
    """
    snap = _supply_queue_snapshot_from_session(session)
    if not isinstance(snap, dict):
        return None
    rows = _supply_queue_rows_from_session(snap.get("rows") or [])
    cc = snap.get("composition_cache")
    if isinstance(cc, dict):
        _apply_composition_cache_to_rows(rows, cc)
    out: list[dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        st = str(r.get("state_code") or r.get("state") or "").strip().upper()
        if st.startswith("ORDER_STATE_"):
            st = st[len("ORDER_STATE_") :]
        if st == "CANCELLED":
            continue
        ship_date = _supply_queue_row_shipment_date_msk_only(r)
        if ship_date is None:
            continue
        if ship_date < today or ship_date > end_date:
            continue
        order_id = str(r.get("order_id") or "").strip()
        shipment_at = str(r.get("shipment_at") or "").strip()
        if not shipment_at or shipment_at == "—":
            datetime_display = ship_date.strftime("%d.%m.%Y")
        else:
            datetime_display = shipment_at
        lk_url = f"https://seller.ozon.ru/app/supply/orders/{order_id}" if order_id else ""
        sort_dt = datetime.combine(ship_date, datetime.min.time(), tzinfo=MSK)
        m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2})", shipment_at)
        if m:
            try:
                d_i, mo_i, y_i, hh, mm = (
                    int(m.group(1)),
                    int(m.group(2)),
                    int(m.group(3)),
                    int(m.group(4)),
                    int(m.group(5)),
                )
                sort_dt = datetime(y_i, mo_i, d_i, hh, mm, 0, tzinfo=MSK)
            except (ValueError, IndexError):
                pass
        psid = str(r.get("primary_supply_id") or "").strip()
        cluster = str(r.get("storage_cluster_line") or "").strip() or "—"
        out.append(
            {
                "datetime_display": datetime_display,
                "marketplace": "Ozon",
                "order_id": order_id,
                "order_number": str(r.get("order_number") or "").strip(),
                "supply_id": psid,
                "cluster": cluster,
                "lk_url": lk_url,
                "is_today": ship_date == today,
                "is_tomorrow": ship_date == today + timedelta(days=1),
                "sort_dt": sort_dt,
            }
        )
    out.sort(key=lambda x: x["sort_dt"], reverse=False)
    return out


def _get_supply_queue_snapshot_ref(session: dict) -> str | None:
    raw = session.get(SUPPLY_QUEUE_SESSION_SNAPSHOT_KEY)
    if not isinstance(raw, dict):
        return None
    ref = raw.get("ref")
    if isinstance(ref, str) and ref.startswith(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX):
        return ref
    return None


def _delete_supply_queue_snapshot_file_if_exists(ref: str | None) -> None:
    """Удаляет JSON-файл снимка по ref (sqs1: + 32 hex)."""
    if not ref or not isinstance(ref, str) or not ref.startswith(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX):
        return
    sid = ref[len(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX) :]
    if not sid or len(sid) != 32 or any(c not in "0123456789abcdef" for c in sid):
        return
    path = _supply_queue_snapshot_dir() / f"{sid}.json"
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        logger.warning("supply_queue: удаление файла снимка: {}", e)


def _write_supply_queue_snapshot_payload(ref: str, payload: dict) -> bool:
    """Перезаписывает JSON снимка по ref (тот же файл, что в сессии)."""
    if not isinstance(ref, str) or not ref.startswith(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX):
        return False
    sid = ref[len(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX) :]
    if not sid or len(sid) != 32 or any(c not in "0123456789abcdef" for c in sid):
        return False
    d = _supply_queue_snapshot_dir()
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{sid}.json"
    tmp = d / f".{sid}.tmp"
    try:
        raw = _json.dumps(payload, ensure_ascii=False, default=str)
        tmp.write_text(raw, encoding="utf-8")
        tmp.replace(path)
        return True
    except Exception as e:
        logger.warning("supply_queue: перезапись снимка: {}", e)
        return False


def _supply_queue_rows_from_session(rows: list) -> list[dict]:
    out: list[dict] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        d = dict(r)
        if "storage_cluster_line" not in d and "cluster_warehouse" in d:
            cw = str(d.get("cluster_warehouse") or "").strip()
            if not cw or cw == "—":
                d["storage_cluster_line"] = "—"
                d["storage_warehouse_line"] = "—"
            elif " / " in cw:
                parts = cw.split(" / ", 1)
                d["storage_cluster_line"] = parts[0].strip() or "—"
                d["storage_warehouse_line"] = (parts[1].strip() if len(parts) > 1 else "—") or "—"
            else:
                d["storage_cluster_line"] = "—"
                d["storage_warehouse_line"] = cw
        s = d.get("shipment_date_msk")
        if isinstance(s, str) and len(s) >= 10:
            try:
                d["shipment_date_msk"] = date.fromisoformat(s[:10])
            except ValueError:
                d["shipment_date_msk"] = None
        out.append(d)
    return out


async def _fetch_supply_order_ids_from_ozon_list(max_pages: int = 5) -> tuple[list[str], str]:
    """
    Список заявок (числовой order_id) через POST /v3/supply-order/list с пагинацией по last_id.
    Возвращает (order_ids, error_message). При частичном успехе error пустой.
    """
    client = OzonAPIClient()
    last_id: str | None = None
    raw: list[str] = []
    last_err = ""
    for _ in range(max(1, int(max_pages))):
        resp = await client.list_supply_orders(
            states=SUPPLY_ORDER_LIST_API_STATES,
            last_id=last_id,
            limit=100,
            sort_by="ORDER_CREATION",
            sort_dir="DESC",
        )
        if not isinstance(resp, dict):
            return [], "Некорректный ответ Ozon"
        if resp.get("_error"):
            last_err = str(resp.get("_error") or "ошибка API")
            try:
                oz = resp.get("ozon_response")
                if isinstance(oz, dict) and (oz.get("message") or oz.get("error")):
                    last_err = str(oz.get("message") or oz.get("error"))[:300]
            except Exception:
                pass
            break
        page_ids = resp.get("order_ids") or resp.get("orderIds") or []
        if not isinstance(page_ids, list):
            page_ids = []
        for oid in page_ids:
            s = str(oid).strip()
            if s.isdigit():
                raw.append(s)
        next_last = resp.get("last_id")
        if next_last is None:
            next_last = resp.get("lastId")
        if next_last is None:
            break
        ns = str(next_last).strip()
        if not ns or ns.lower() == "null":
            break
        if not page_ids:
            break
        last_id = ns
    seen: set[str] = set()
    out: list[str] = []
    for x in raw:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out, last_err


def _extract_orders_from_supply_order_get_response(data: dict) -> list[dict]:
    """Массив orders из ответа POST /v3/supply-order/get (с учётом обёртки result)."""
    if not isinstance(data, dict) or data.get("_error"):
        return []
    if isinstance(data.get("orders"), list):
        return [x for x in data["orders"] if isinstance(x, dict)]
    result = data.get("result")
    if isinstance(result, dict):
        inner = result.get("result") if isinstance(result.get("result"), dict) else result
        if isinstance(inner, dict):
            arr = inner.get("orders")
            if isinstance(arr, list):
                return [x for x in arr if isinstance(x, dict)]
        arr2 = result.get("orders")
        if isinstance(arr2, list):
            return [x for x in arr2 if isinstance(x, dict)]
    return []


def _format_supply_timeslot_from_iso(from_val: object) -> str:
    """Дата и время отгрузки: timeslot.timeslot.from → отображение в МСК."""
    if from_val is None:
        return "—"
    s = str(from_val).strip()
    if not s or s.lower() == "null":
        return "—"
    dt = _ensure_datetime_msk(s)
    if not dt:
        return s[:64]
    return dt.strftime("%d.%m.%Y %H:%M МСК")


def _format_datetime_msk_for_ui(value: object) -> str:
    """Отображение даты/времени в интерфейсе в московском поясе (UTC+3)."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        else:
            dt = dt.astimezone(MSK)
        return dt.strftime("%d.%m.%Y %H:%M МСК")
    s = str(value).strip()
    if not s:
        return ""
    try:
        raw = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(MSK).strftime("%d.%m.%Y %H:%M МСК")
    except Exception:
        return s


def _supply_queue_snapshot_last_refresh_display(session: dict, snap: dict | None) -> str:
    """Время последнего «Обновить данные»: из поля saved_at снимка или mtime файла."""
    if not isinstance(snap, dict):
        return ""
    raw = str(snap.get("saved_at") or "").strip()
    if raw:
        return _format_datetime_msk_for_ui(raw)
    ref = _get_supply_queue_snapshot_ref(session)
    if not ref or not isinstance(ref, str) or not ref.startswith(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX):
        return ""
    sid = ref[len(SUPPLY_QUEUE_SNAPSHOT_REF_PREFIX) :]
    if not sid or len(sid) != 32 or any(c not in "0123456789abcdef" for c in sid):
        return ""
    path = _supply_queue_snapshot_dir() / f"{sid}.json"
    if not path.is_file():
        return ""
    try:
        dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone(MSK)
        return dt.strftime("%d.%m.%Y %H:%M МСК")
    except OSError:
        return ""


def _storage_warehouse_name_or_city(supply_item: dict) -> str:
    """supplies[].storage_warehouse: название склада или город из address, если name пустой."""
    if not isinstance(supply_item, dict):
        return ""
    sw = supply_item.get("storage_warehouse")
    if not isinstance(sw, dict):
        return ""
    name = str(sw.get("name") or "").strip()
    if name:
        return name
    addr = str(sw.get("address") or "").strip()
    if not addr:
        return ""
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if not parts:
        return addr[:240]
    head = parts[0].replace(" ", "")
    if head.isdigit() and len(parts) >= 2:
        return parts[1]
    return parts[0]


def _build_cluster_name_lookups(
    cluster_rows: list[dict],
) -> tuple[dict[int, str], dict[int, str], dict[int, str]]:
    """id/macrolocal → имя кластера; warehouse_id → имя склада; warehouse_id → имя родительского кластера."""
    by_ml: dict[int, str] = {}
    by_wh: dict[int, str] = {}
    wh_to_cluster_name: dict[int, str] = {}
    for cl in cluster_rows or []:
        if not isinstance(cl, dict):
            continue
        cname = (cl.get("name") or "").strip()
        if not cname:
            continue
        # В заявке часто приходит число, совпадающее с clusters[].id, а не с macrolocal_cluster_id.
        cid_raw = cl.get("id")
        if cid_raw not in (None, ""):
            try:
                by_ml[int(cid_raw)] = cname
            except (TypeError, ValueError):
                pass
        ml = cl.get("macrolocal_cluster_id")
        if ml is not None:
            try:
                by_ml[int(ml)] = cname
            except (TypeError, ValueError):
                pass
        for w in cl.get("warehouses") or []:
            if not isinstance(w, dict):
                continue
            wid_raw = w.get("id")
            wname = (w.get("name") or "").strip()
            if wid_raw is None:
                continue
            try:
                wi = int(wid_raw)
            except (TypeError, ValueError):
                continue
            if wname:
                by_wh[wi] = wname
            wh_to_cluster_name[wi] = cname
    return by_ml, by_wh, wh_to_cluster_name


def _macrolocal_cluster_id_from_supply_order(o: dict) -> int | None:
    """
    Идентификатор кластера для сопоставления с /v1/cluster/list: как в заявке к ЛК —
    сверху supplies[].macrolocal_cluster_id или в storage_warehouse (macrolocal_cluster_id, cluster_id).
    """
    supplies = o.get("supplies")
    if not isinstance(supplies, list):
        return None
    for item in supplies:
        if not isinstance(item, dict):
            continue
        ml = item.get("macrolocal_cluster_id")
        if ml is not None:
            try:
                return int(ml)
            except (TypeError, ValueError):
                pass
        xd = item.get("crossdock_cluster_id") or item.get("crossdockClusterId")
        if xd is not None:
            try:
                iv = int(xd)
                if iv:
                    return iv
            except (TypeError, ValueError):
                pass
        sw = item.get("storage_warehouse")
        if isinstance(sw, dict):
            for ck in ("macrolocal_cluster_id", "cluster_id", "clusterId"):
                v = sw.get(ck)
                if v is not None:
                    try:
                        iv = int(v)
                        if iv:
                            return iv
                    except (TypeError, ValueError):
                        pass
    return None


async def _enrich_supply_order_rows_cluster_names(rows: list[dict], client: OzonAPIClient) -> None:
    """Названия кластера и склада из POST /v1/cluster/list (filter_by_supply_type + search), с fallback на cluster_type."""
    cluster_rows = await client.get_cluster_list_for_supply(["CREATE_TYPE_CROSSDOCK"], "")
    if not cluster_rows:
        cluster_rows = await client.get_cluster_list()
    # Точечная подгрузка по id из строк, если общий список не содержит нужных кластеров.
    need_ids: set[int] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ml = row.get("macrolocal_cluster_id")
        if ml is not None:
            try:
                need_ids.add(int(ml))
            except (TypeError, ValueError):
                pass
    by_ml, by_wh, wh_to_cluster_name = _build_cluster_name_lookups(cluster_rows)
    missing = [i for i in sorted(need_ids) if i and i not in by_ml]
    while missing:
        chunk = missing[:50]
        missing = missing[50:]
        extra = await client.get_cluster_list(cluster_ids=[str(x) for x in chunk])
        em, ew, ec = _build_cluster_name_lookups(extra)
        by_ml.update(em)
        by_wh.update(ew)
        wh_to_cluster_name.update(ec)
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ml = row.get("macrolocal_cluster_id")
        cluster_title = ""
        if ml is not None:
            try:
                mi = int(ml)
            except (TypeError, ValueError):
                mi = None
            if mi is not None:
                cluster_title = (by_ml.get(mi) or "").strip()
        wh_id = row.get("storage_warehouse_id")
        wi: int | None = None
        if wh_id is not None:
            try:
                wi = int(wh_id)
            except (TypeError, ValueError):
                wi = None
        if not cluster_title and wi is not None:
            cluster_title = (wh_to_cluster_name.get(wi) or "").strip()
        if cluster_title:
            row["storage_cluster_line"] = cluster_title
        if wi is not None and wi in by_wh:
            row["storage_warehouse_line"] = by_wh[wi]


def _supply_ids_from_order(o: dict) -> list[str]:
    """Идентификаторы поставок (supply_id) для POST /v1/cargoes/get — из supplies[]."""
    out: list[str] = []
    seen: set[str] = set()
    supplies = o.get("supplies")
    if not isinstance(supplies, list):
        return out
    for item in supplies:
        if not isinstance(item, dict):
            continue
        sid = item.get("supply_id") if item.get("supply_id") is not None else item.get("supplyId")
        if sid is None:
            sids = item.get("supply_ids") or item.get("supplyIds")
            if isinstance(sids, list) and sids:
                sid = sids[0]
        if sid is None:
            continue
        s = str(sid).strip()
        if not s or not s.isdigit():
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _primary_bundle_id_from_order(o: dict) -> str | None:
    """Первый bundle_id из supplies[] заявки — fallback, если у грузоместа в /v1/cargoes/get нет bundle_id."""
    supplies = o.get("supplies")
    if not isinstance(supplies, list):
        return None
    for item in supplies:
        if not isinstance(item, dict):
            continue
        bid = item.get("bundle_id") if item.get("bundle_id") is not None else item.get("bundleId")
        if bid is not None and str(bid).strip():
            return str(bid).strip()
    return None


async def _enrich_cargo_units_bundle_items(rows: list[dict], client: OzonAPIClient) -> None:
    """Товары по каждому грузоместу: POST /v1/supply-order/bundle по bundle_id из /v1/cargoes/get."""
    cache: dict[str, list[dict]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        oid = str(row.get("order_id") or "").strip()
        obid = row.get("order_bundle_id")
        obid_keep = str(obid).strip() if obid is not None and str(obid).strip() else ""
        for cu in row.get("cargo_units") or []:
            if not isinstance(cu, dict):
                continue
            bid = cu.get("bundle_id")
            if bid is None or str(bid).strip() == "":
                bid = obid
            if bid is None or str(bid).strip() == "":
                cu["line_items"] = []
                continue
            bkey = str(bid).strip()
            if bkey not in cache:
                logger.info(
                    "supply_queue: запрос supply-order/bundle order_id={} bundle_id={}",
                    oid,
                    bkey,
                )
                cache[bkey] = await client.get_supply_order_bundle_items_all_pages(
                    bkey,
                    item_tags_calculation=None,
                )
            cu["line_items"] = list(cache[bkey])
        cargo_list = [c for c in (row.get("cargo_units") or []) if isinstance(c, dict)]
        n_pos = sum(len(c.get("line_items") or []) for c in cargo_list)
        logger.info(
            "supply_queue: товарный состав заявка order_id={} грузомест={} позиций_всего={}",
            oid,
            len(cargo_list),
            n_pos,
        )
        row.pop("order_bundle_id", None)
        if obid_keep:
            row["order_bundle_id"] = obid_keep


async def _enrich_supply_order_rows_cargoes(rows: list[dict], client: OzonAPIClient) -> None:
    """Грузоместа из POST /v1/cargoes/get (supply_ids из заявки); товары — в _enrich_cargo_units_bundle_items."""
    all_ids: list[str] = []
    seen: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for s in row.get("supply_ids_for_cargoes") or []:
            ss = str(s).strip()
            if ss and ss not in seen:
                seen.add(ss)
                all_ids.append(ss)
    by_supply: dict[str, list[dict]] = {}
    # Док. Ozon: в POST /v1/cargoes/get — до 100 supply_ids за запрос.
    chunk_size = 100
    for i in range(0, len(all_ids), chunk_size):
        chunk = all_ids[i : i + chunk_size]
        resp = await client.get_cargoes_by_supply_ids(chunk)
        if resp.get("_error"):
            logger.warning("supply_queue: v1/cargoes/get: {}", resp.get("_error"))
            continue
        for s in resp.get("supply") or []:
            if not isinstance(s, dict):
                continue
            sid = s.get("supply_id") if s.get("supply_id") is not None else s.get("supplyId")
            if sid is None:
                continue
            sk = str(sid).strip()
            units: list[dict] = []
            for c in s.get("cargoes") or []:
                if not isinstance(c, dict):
                    continue
                ti = c.get("tracking_info") if isinstance(c.get("tracking_info"), dict) else {}
                units.append(
                    {
                        "cargo_id": c.get("cargo_id"),
                        "bundle_id": c.get("bundle_id"),
                        "type": str(c.get("type") or ""),
                        "content_type": str(c.get("content_type") or ""),
                        "placement_zone_type": str(c.get("placement_zone_type") or ""),
                        "tracking_status": str(ti.get("status") or ""),
                        "tracking_date": str(ti.get("date") or ""),
                    }
                )
            by_supply[sk] = units
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        merged: list[dict] = []
        for sid in row.get("supply_ids_for_cargoes") or []:
            sk = str(sid).strip()
            merged.extend(by_supply.get(sk) or [])
        row["cargo_units"] = merged
        ids_copy = [str(s).strip() for s in (row.get("supply_ids_for_cargoes") or []) if str(s).strip()]
        row.pop("supply_ids_for_cargoes", None)
        row["supply_ids_for_cargoes"] = ids_copy


def _row_from_supply_order_get(o: dict) -> dict | None:
    """Поля строки таблицы строго по схеме ответа v3/supply-order/get."""
    if not isinstance(o, dict):
        return None
    oid_raw = o.get("order_id")
    try:
        oi = int(oid_raw)
    except (TypeError, ValueError):
        return None
    order_id_str = str(oi).zfill(8) if str(oi).isdigit() else str(oi)

    order_number = str(o.get("order_number") or "").strip()
    display_number = order_number or order_id_str

    dropoff = o.get("dropoff_warehouse") if isinstance(o.get("dropoff_warehouse"), dict) else {}
    drop_name = str(dropoff.get("name") or "").strip()

    supplies = o.get("supplies")
    macrolocal = _macrolocal_cluster_id_from_supply_order(o)

    storage_wh = ""
    storage_wh_id: int | None = None
    if isinstance(supplies, list):
        for item in supplies:
            if not isinstance(item, dict):
                continue
            if not storage_wh:
                storage_wh = _storage_warehouse_name_or_city(item)
            if storage_wh_id is None:
                sw = item.get("storage_warehouse")
                if isinstance(sw, dict):
                    w = sw.get("warehouse_id")
                    if w is None:
                        w = sw.get("warehouseId")
                    if w is not None:
                        try:
                            storage_wh_id = int(w)
                        except (TypeError, ValueError):
                            pass
    cluster_line = "—"
    warehouse_line = "—"
    if macrolocal is not None:
        cluster_line = str(macrolocal)
    if storage_wh:
        warehouse_line = storage_wh
    elif drop_name:
        warehouse_line = drop_name

    ts_wrap = o.get("timeslot") if isinstance(o.get("timeslot"), dict) else {}
    inner = ts_wrap.get("timeslot") if isinstance(ts_wrap.get("timeslot"), dict) else {}
    from_s = inner.get("from") if isinstance(inner, dict) else None
    shipment_at = _format_supply_timeslot_from_iso(from_s)
    dt_msk = _ensure_datetime_msk(from_s) if from_s else None
    shipment_date_msk: date | None = dt_msk.date() if dt_msk else None

    state_raw = str(o.get("state") or "").strip()
    state_code = state_raw.upper() if state_raw else "UNSPECIFIED"
    state_label_ru = SUPPLY_ORDER_STATE_LABELS_RU.get(state_code, state_raw or "—")

    supply_ids_for_cargoes = _supply_ids_from_order(o)
    order_bundle_id = _primary_bundle_id_from_order(o)
    primary_supply_id = str(supply_ids_for_cargoes[0]).strip() if supply_ids_for_cargoes else ""

    return {
        "order_id": order_id_str,
        "order_number": display_number,
        "primary_supply_id": primary_supply_id,
        "storage_cluster_line": cluster_line,
        "storage_warehouse_line": warehouse_line,
        "shipment_at": shipment_at,
        "cargo_units": [],
        "supply_ids_for_cargoes": supply_ids_for_cargoes,
        "order_bundle_id": order_bundle_id,
        "state": state_code,
        "state_code": state_code,
        "state_label_ru": state_label_ru,
        "state_badge_class": _supply_order_state_badge_class(state_code),
        "shipment_date_msk": shipment_date_msk,
        "macrolocal_cluster_id": macrolocal,
        "storage_warehouse_id": storage_wh_id,
        "can_cancel": state_code in ("DATA_FILLING", "READY_TO_SUPPLY"),
    }


def _placeholder_supply_row(order_id: str) -> dict:
    oid = str(order_id).strip()
    return {
        "order_id": oid,
        "order_number": oid,
        "primary_supply_id": "",
        "storage_cluster_line": "—",
        "storage_warehouse_line": "—",
        "shipment_at": "—",
        "cargo_units": [],
        "state": "UNSPECIFIED",
        "state_code": "UNSPECIFIED",
        "state_label_ru": "—",
        "state_badge_class": "supply-state--muted",
        "shipment_date_msk": None,
        "macrolocal_cluster_id": None,
        "storage_warehouse_id": None,
        "can_cancel": False,
        "supply_ids_for_cargoes": [],
        "order_bundle_id": "",
    }


async def _fetch_supply_order_rows_via_get(order_ids: list[str]) -> tuple[list[dict], str]:
    """
    POST /v3/supply-order/get — до 50 order_ids за запрос, поля для таблицы очереди.
    Порядок строк как в order_ids.
    """
    if not order_ids:
        return [], ""
    client = OzonAPIClient()
    by_oid: dict[str, dict] = {}
    last_err = ""
    chunk_size = 50
    for i in range(0, len(order_ids), chunk_size):
        chunk = order_ids[i : i + chunk_size]
        resp = await client.get_supply_info_many(chunk)
        if not isinstance(resp, dict):
            last_err = "Некорректный ответ v3/supply-order/get"
            continue
        if resp.get("_error"):
            last_err = str(resp.get("_error") or "ошибка get")
            try:
                oz = resp.get("ozon_response")
                if isinstance(oz, dict) and (oz.get("message") or oz.get("error")):
                    last_err = str(oz.get("message") or oz.get("error"))[:400]
            except Exception:
                pass
            continue
        for o in _extract_orders_from_supply_order_get_response(resp):
            row = _row_from_supply_order_get(o)
            if row:
                k = str(row["order_id"]).strip()
                if k.isdigit():
                    k = k.zfill(8)
                by_oid[k] = row
    out: list[dict] = []
    for oid in order_ids:
        oks = str(oid).strip()
        if oks.isdigit():
            oks = oks.zfill(8)
        out.append(by_oid.get(oks) or _placeholder_supply_row(oks))
    await _enrich_supply_order_rows_cluster_names(out, client)
    await _enrich_supply_order_rows_cargoes(out, client)
    return out, last_err


async def _fetch_supply_order_composition_cargo_units(order_id: str) -> tuple[list[dict], str]:
    """Один запрос: get → грузоместа → состав по bundle (для карточки заявки)."""
    oid = _normalize_supply_queue_order_id(order_id)
    if not oid or not oid.isdigit():
        return [], "Некорректный order_id"
    client = OzonAPIClient()
    resp = await client.get_supply_info_many([oid])
    if not isinstance(resp, dict):
        return [], "Некорректный ответ v3/supply-order/get"
    if resp.get("_error"):
        err = str(resp.get("_error") or "ошибка get")
        try:
            oz = resp.get("ozon_response")
            if isinstance(oz, dict) and (oz.get("message") or oz.get("error")):
                err = str(oz.get("message") or oz.get("error"))[:400]
        except Exception:
            pass
        return [], err
    row = None
    for o in _extract_orders_from_supply_order_get_response(resp):
        row = _row_from_supply_order_get(o)
        if row:
            break
    if not row:
        return [], "Заявка не найдена в ответе Ozon"
    await _enrich_supply_order_rows_cluster_names([row], client)
    await _enrich_supply_order_rows_cargoes([row], client)
    await _enrich_cargo_units_bundle_items([row], client)
    return list(row.get("cargo_units") or []), ""


@router.get("/supply-queue", response_class=HTMLResponse)
async def supply_queue(
    request: Request,
    background_tasks: BackgroundTasks,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
    sq_from: str | None = Query(None, description="Начало интервала по дате отгрузки (YYYY-MM-DD), МСК"),
    sq_to: str | None = Query(None, description="Конец интервала по дате отгрузки (YYYY-MM-DD), МСК"),
    sq_show_cancelled: bool = Query(False, description="Показывать отменённые заявки"),
    sq_st: list[str] | None = Query(None, description="Показывать заявки с этими статусами (повтор параметра sq_st)"),
):
    """Очередь поставок: кластеры по скану; заявки — list, детали — get (до 50 id за запрос)."""
    scan, results, week_dates, scanned_at_str = await _load_supply_queue_cluster_scan(db, max_days=21)
    supply_order_list_rows: list[dict] = []
    supply_order_list_error = ""
    supply_order_get_error = ""
    sq_refresh = _query_param_truthy(request.query_params.get("sq_refresh"))

    if sq_refresh:
        try:
            prev_snap = _supply_queue_snapshot_from_session(request.session)
            prev_cc: dict = {}
            if isinstance(prev_snap, dict):
                _raw_cc = prev_snap.get("composition_cache")
                if isinstance(_raw_cc, dict):
                    prev_cc = _raw_cc
            _supply_queue_refresh_progress_set(user, "Список заявок: запрос supply-order/list…")
            await asyncio.sleep(0)
            try:
                ids, list_err = await _fetch_supply_order_ids_from_ozon_list()
                if list_err and not ids:
                    supply_order_list_error = list_err
                elif list_err and ids:
                    logger.warning("supply_queue: supply-order/list warning after partial data: {}", list_err)
                if ids:
                    _supply_queue_refresh_progress_set(
                        user,
                        "Карточки заявок и грузоместа: v3/supply-order/get и cargoes/get, заявок: {}…".format(len(ids)),
                    )
                    await asyncio.sleep(0)
                    rows, get_err = await _fetch_supply_order_rows_via_get(ids)
                    supply_order_list_rows = rows
                    _merge_completed_supply_rows_from_prev_cache(supply_order_list_rows, prev_cc)
                    if get_err:
                        supply_order_get_error = get_err
            except Exception as e:
                logger.warning("supply_queue: supply orders list/get exception: {}", e, exc_info=True)
                supply_order_list_error = str(e)

            sq_from_q = request.query_params.get("sq_from")
            sq_to_q = request.query_params.get("sq_to")
            sq_show_cancelled_q = _query_param_truthy(request.query_params.get("sq_show_cancelled"))
            dr_bundle_from, dr_bundle_to = _supply_queue_shipment_date_range_msk(sq_from_q, sq_to_q)
            rows_for_bundle = [
                r
                for r in (supply_order_list_rows or [])
                if isinstance(r, dict)
                and _supply_queue_row_passes_filters(r, dr_bundle_from, dr_bundle_to, sq_show_cancelled_q)
                and not _supply_queue_row_is_completed(r)
            ]
            if rows_for_bundle:
                try:
                    oz_client = OzonAPIClient()
                    logger.info(
                        "supply_queue: «Обновить данные» — товарный состав по bundle для {} заявок в диапазоне дат {} … {}",
                        len(rows_for_bundle),
                        dr_bundle_from,
                        dr_bundle_to,
                    )

                    async def _on_bundle_progress(msg: str) -> None:
                        _supply_queue_refresh_progress_set(user, msg)
                        await asyncio.sleep(0)

                    await _supply_queue_enrich_bundle_parallel_throttled(
                        rows_for_bundle,
                        oz_client,
                        float(settings.supply_queue_bundle_delay_sec),
                        max(1, int(settings.supply_queue_bundle_max_concurrent)),
                        "sq_refresh",
                        on_progress=_on_bundle_progress,
                    )
                except Exception as e:
                    logger.warning("supply_queue: ошибка подгрузки bundle при «Обновить данные»: {}", e, exc_info=True)

            _supply_queue_refresh_progress_set(user, "Сохранение снимка в кеш…")
            await asyncio.sleep(0)
            try:
                # Снимок на диске: mtime обновляется при записи — TTL 24 ч от последнего «Обновить данные».
                # Товарный состав подгружается только здесь (не при обычном открытии страницы).
                now_iso = datetime.now(MSK).replace(microsecond=0).isoformat()
                composition_cache_fresh: dict = {}
                for r in supply_order_list_rows or []:
                    if not isinstance(r, dict):
                        continue
                    oid = _normalize_supply_queue_order_id(str(r.get("order_id") or ""))
                    if not oid:
                        continue
                    if _supply_queue_row_is_completed(r):
                        pe = _composition_cache_entry_for_oid(prev_cc, oid)
                        if (
                            pe
                            and isinstance(pe.get("cargo_units"), list)
                            and _cargo_units_have_line_items(pe.get("cargo_units"))
                        ):
                            composition_cache_fresh[oid] = {
                                "cargo_units": deepcopy(pe["cargo_units"]),
                                "updated_at": str(pe.get("updated_at") or now_iso),
                            }
                            continue
                    composition_cache_fresh[oid] = {
                        "cargo_units": deepcopy(r.get("cargo_units") or []),
                        "updated_at": now_iso,
                    }
                payload = {
                    "rows": _supply_queue_rows_for_session(supply_order_list_rows),
                    "list_error": supply_order_list_error,
                    "get_error": supply_order_get_error,
                    "composition_cache": composition_cache_fresh,
                    "saved_at": now_iso,
                }
                ref = _store_supply_queue_snapshot_data(payload)
                request.session[SUPPLY_QUEUE_SESSION_SNAPSHOT_KEY] = {"ref": ref}
                _supply_queue_write_shared_snapshot_ref(ref)
            except Exception as e:
                logger.warning("supply_queue: сохранение снимка Поставок в сессию: {}", e, exc_info=True)
        finally:
            _supply_queue_refresh_progress_clear(user)
        qp = [(k, v) for k, v in request.query_params.multi_items() if k not in ("sq_refresh", "sq_apply_filter", "sq_refresh_json")]
        qs = urlencode(qp)
        loc = f"{request.url.path}?{qs}" if qs else str(request.url.path)
        # JSON вместо 303: после долгого refresh fetch+redirect часто даёт HTTP 0 (обрыв при редиректе).
        if _query_param_truthy(request.query_params.get("sq_refresh_json")):
            return JSONResponse({"ok": True, "redirect": loc})
        return RedirectResponse(url=loc, status_code=303)

    snap = _supply_queue_snapshot_from_session(request.session)
    if not isinstance(snap, dict):
        sref = _supply_queue_read_shared_snapshot_ref()
        if sref:
            loaded = _load_supply_queue_snapshot_data(sref)
            if isinstance(loaded, dict):
                request.session[SUPPLY_QUEUE_SESSION_SNAPSHOT_KEY] = {"ref": sref}
                snap = loaded
    composition_cache: dict | None = None
    if isinstance(snap, dict):
        supply_order_list_rows = _supply_queue_rows_from_session(snap.get("rows") or [])
        supply_order_list_error = str(snap.get("list_error") or "")
        supply_order_get_error = str(snap.get("get_error") or "")
        cc = snap.get("composition_cache")
        composition_cache = cc if isinstance(cc, dict) else None
        _apply_composition_cache_to_rows(supply_order_list_rows, composition_cache)

    dr_from, dr_to = _supply_queue_shipment_date_range_msk(sq_from, sq_to)
    supply_order_total = len(supply_order_list_rows)
    rows_after_date = [
        r
        for r in supply_order_list_rows
        if _supply_queue_row_passes_filters(r, dr_from, dr_to, sq_show_cancelled)
    ]
    included_statuses = _supply_queue_resolve_included_statuses(sq_st)
    supply_order_list_rows_filtered = [
        r
        for r in rows_after_date
        if _supply_queue_row_filter_status_code(r) in included_statuses
    ]
    status_counts_for_chips = _supply_queue_status_counts_map(rows_after_date)
    supply_queue_status_filter_chips = _supply_queue_status_filter_chips(
        included_statuses,
        status_counts_for_chips,
    )

    ozon_by_article: list = []
    last_updated_ozon = None
    try:
        if not _cache_fresh(_stocks_cache.get("last_updated_ozon_table")):
            background_tasks.add_task(_background_refresh_warehouse_stocks_cache)
        raw_w = _stocks_cache.get("ozon_table") or []
        last_updated_ozon = _stocks_cache.get("last_updated_ozon_table")
        ozon_by_article = deepcopy(raw_w) if raw_w else []
        _enrich_warehouse_stocks_supply_queue_corners(ozon_by_article, request.session)
    except Exception as e:
        logger.warning("supply_queue: матрица остатков для страницы: {}", e)

    return templates.TemplateResponse(
        "supply_queue.html",
        {
            "request": request,
            "site_username": user.username,
            "scan": scan,
            "scanned_at_str": scanned_at_str,
            "results": results,
            "week_dates": week_dates,
            "supply_order_list_rows": supply_order_list_rows_filtered,
            "supply_order_list_total": supply_order_total,
            "supply_order_list_error": supply_order_list_error,
            "supply_order_get_error": supply_order_get_error,
            "sq_show_cancelled": sq_show_cancelled,
            "sq_filter_date_from": dr_from,
            "sq_filter_date_to": dr_to,
            "supply_order_never_loaded": snap is None,
            "ozon_by_article": ozon_by_article,
            "last_updated_ozon": last_updated_ozon,
            "ws_stocks_refresh_btn_id": "ws-refresh-btn-sq",
            "ws_stocks_spinner_id": "ws-refresh-spinner-sq",
            "ws_stocks_compact": True,
            "supply_queue_status_filter_chips": supply_queue_status_filter_chips,
            "supply_order_rows_date_filtered_count": len(rows_after_date),
            "supply_queue_snapshot_at": _supply_queue_snapshot_last_refresh_display(
                request.session,
                snap if isinstance(snap, dict) else None,
            ),
        },
    )


@router.get("/supply-queue/create", response_class=HTMLResponse)
async def supply_queue_create(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Мастер создания заявки: товары, дата/кластер по матрице скана, затем слоты (через POST /api/supplies/draft)."""
    scan, results, week_dates, scanned_at_str = await _load_supply_queue_cluster_scan(db, max_days=21)
    r = await db.execute(select(Product).order_by(Product.name))
    products = list(r.scalars().all())
    supply_products = [
        {
            "id": p.id,
            "article": p.article or "",
            "name": p.name or "",
            "ozon_sku": p.ozon_sku,
            "photo_url": (f"/uploads/products/photos/{p.photo_filename}" if getattr(p, "photo_filename", None) else ""),
        }
        for p in products
    ]
    cluster_meta_by_id: dict[str, dict] = {}
    cluster_meta_by_name: dict[str, dict] = {}
    try:
        client = OzonAPIClient()
        cluster_list = await client.get_cluster_list(cluster_type="CLUSTER_TYPE_OZON")
        for cl in cluster_list or []:
            if not isinstance(cl, dict):
                continue
            cl_id = str(cl.get("id") or "").strip()
            cl_name = str(cl.get("name") or "").strip()
            raw_ml = cl.get("macrolocal_cluster_id")
            try:
                ml_id = int(raw_ml) if raw_ml not in (None, "", 0, "0") else None
            except (TypeError, ValueError):
                ml_id = None
            raw_wh = None
            whs = cl.get("warehouses") or []
            if isinstance(whs, list) and whs:
                raw_wh = (whs[0] or {}).get("id")
            try:
                storage_wh_id = int(raw_wh) if raw_wh not in (None, "", 0, "0") else None
            except (TypeError, ValueError):
                storage_wh_id = None
            meta = {
                "cluster_id": cl_id,
                "cluster_name": cl_name,
                "macrolocal_cluster_id": ml_id,
                "storage_warehouse_id": storage_wh_id,
            }
            if cl_id:
                cluster_meta_by_id[cl_id] = meta
            if cl_name:
                cluster_meta_by_name[_norm_ws_cluster_name_for_match(cl_name)] = meta
    except Exception as e:
        logger.warning("supply_queue_create: cluster meta load: {}", e)
    supply_cluster_days = []
    for r_item in (results or []):
        if not isinstance(r_item, dict):
            continue
        cluster_name = str(r_item.get("cluster_name") or "").strip()
        cluster_id = str(r_item.get("cluster_id") or "").strip()
        meta = cluster_meta_by_id.get(cluster_id) if cluster_id else None
        if meta is None and cluster_name:
            meta = cluster_meta_by_name.get(_norm_ws_cluster_name_for_match(cluster_name))
        ml_id = (meta or {}).get("macrolocal_cluster_id")
        storage_wh_id = (meta or {}).get("storage_warehouse_id")
        counts = list(r_item.get("day_counts") or [])
        if not cluster_name or not counts:
            continue
        for idx, day_obj in enumerate(week_dates or []):
            cnt = int(counts[idx]) if idx < len(counts) and counts[idx] is not None else 0
            supply_cluster_days.append(
                {
                    "cluster_name": cluster_name,
                    "date_iso": day_obj.strftime("%Y-%m-%d"),
                    "date_label": day_obj.strftime("%d.%m"),
                    "slots": cnt,
                    "is_green": cnt > 6,
                    "cluster_id": cluster_id,
                    "macrolocal_cluster_id": ml_id,
                    "storage_warehouse_id": storage_wh_id,
                }
            )
    return templates.TemplateResponse(
        "supply_queue_create.html",
        {
            "request": request,
            "site_username": user.username,
            "supply_products": supply_products,
            "scan": scan,
            "scanned_at_str": scanned_at_str,
            "results": results,
            "week_dates": week_dates,
            "supply_cluster_days": supply_cluster_days,
        },
    )


@router.get("/supply-queue/order/{order_id}", response_class=HTMLResponse)
async def supply_queue_order_card(
    request: Request,
    order_id: str,
    user: User = Depends(verify_site_user),
):
    """Карточка заявки: грузоместа и ленивая загрузка товарного состава."""
    oid = _normalize_supply_queue_order_id(order_id)
    if not oid:
        return Response(status_code=404)
    snap = _supply_queue_snapshot_from_session(request.session)
    if not isinstance(snap, dict):
        return RedirectResponse(url="/supply-queue", status_code=303)
    rows = _supply_queue_rows_from_session(snap.get("rows") or [])
    cc = snap.get("composition_cache")
    composition_cache = cc if isinstance(cc, dict) else None
    _apply_composition_cache_to_rows(rows, composition_cache)
    row = None
    for r in rows:
        if isinstance(r, dict) and _normalize_supply_queue_order_id(str(r.get("order_id") or "")) == oid:
            row = r
            break
    if not row:
        return Response(status_code=404)
    comp = (composition_cache or {}).get(oid)
    if comp is None and oid.isdigit():
        comp = (composition_cache or {}).get(str(int(oid)))
    updated_at = ""
    has_composition = False
    if isinstance(comp, dict):
        updated_at = _format_datetime_msk_for_ui(comp.get("updated_at") or "")
        cu = comp.get("cargo_units")
        if isinstance(cu, list) and any(
            isinstance(c, dict) and (c.get("line_items") or c.get("items")) for c in cu
        ):
            has_composition = True
    elif row.get("cargo_units"):
        for c in row.get("cargo_units") or []:
            if isinstance(c, dict) and (c.get("line_items") or c.get("items")):
                has_composition = True
                break
    return templates.TemplateResponse(
        "supply_queue_order.html",
        {
            "request": request,
            "site_username": user.username,
            "sq_row": row,
            "sq_order_id": oid,
            "composition_updated_at": updated_at,
            "has_composition_loaded": has_composition,
        },
    )


@router.post("/api/supply-queue/load-composition")
async def api_supply_queue_load_composition(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Загрузить товарный состав по заявке, сохранить в снимок сессии и вернуть JSON."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    oid = _normalize_supply_queue_order_id(str((body or {}).get("order_id") or ""))
    if not oid or not oid.isdigit():
        return JSONResponse({"ok": False, "error": "Некорректный order_id"}, status_code=400)
    ref = _get_supply_queue_snapshot_ref(request.session)
    if not ref:
        return JSONResponse(
            {"ok": False, "error": "Сначала загрузите список заявок на странице очереди"},
            status_code=400,
        )
    payload = _load_supply_queue_snapshot_data(ref)
    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "error": "Снимок устарел или отсутствует"}, status_code=400)
    rows_raw = payload.get("rows") or []
    idx: int | None = None
    for i, r in enumerate(rows_raw):
        if isinstance(r, dict) and _normalize_supply_queue_order_id(str(r.get("order_id") or "")) == oid:
            idx = i
            break
    if idx is None:
        return JSONResponse({"ok": False, "error": "Заявка не в текущем списке — обновите данные"}, status_code=404)
    cargo_units, err = await _fetch_supply_order_composition_cargo_units(oid)
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=502)
    updated_at = datetime.now(MSK).replace(microsecond=0).isoformat()
    rows_raw[idx]["cargo_units"] = cargo_units
    cache = payload.get("composition_cache")
    if not isinstance(cache, dict):
        cache = {}
    cache[oid] = {"cargo_units": cargo_units, "updated_at": updated_at}
    payload["composition_cache"] = cache
    payload["rows"] = rows_raw
    if not _write_supply_queue_snapshot_payload(ref, payload):
        return JSONResponse({"ok": False, "error": "Не удалось сохранить снимок"}, status_code=500)
    _supply_queue_write_shared_snapshot_ref(ref)
    logger.info("supply_queue: товарный состав сохранён в снимок order_id={}", oid)
    return JSONResponse(
        {
            "ok": True,
            "order_id": oid,
            "cargo_units": cargo_units,
            "updated_at": updated_at,
        }
    )


@router.get("/api/supply-queue/refresh-progress")
async def api_supply_queue_refresh_progress(user: User = Depends(verify_site_user)):
    """Текущий шаг «Обновить данные» (in-memory, тот же процесс uvicorn)."""
    key = _supply_queue_refresh_progress_user_key(user)
    data = _SUPPLY_QUEUE_REFRESH_PROGRESS.get(key)
    if not data:
        return JSONResponse({"ok": True, "active": False})
    return JSONResponse(
        {
            "ok": True,
            "active": True,
            "message": data.get("message") or "",
            "at": data.get("at") or "",
        }
    )


def _supply_ids_from_snapshot_row(row: dict) -> list[str]:
    ids = [str(s).strip() for s in (row.get("supply_ids_for_cargoes") or []) if str(s).strip()]
    if ids:
        return ids
    ps = str(row.get("primary_supply_id") or "").strip()
    if ps.isdigit():
        return [ps]
    return []


async def _supply_queue_enrich_bundle_parallel_throttled(
    rows_subset: list[dict],
    client: OzonAPIClient,
    delay_sec: float,
    max_concurrent: int,
    trigger: str,
    on_progress: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    bundle_cache: dict[str, list[dict]] = {}
    ordered_bundles: list[str] = []
    seen: set[str] = set()
    for row in rows_subset:
        if not isinstance(row, dict):
            continue
        obid = row.get("order_bundle_id")
        for cu in row.get("cargo_units") or []:
            if not isinstance(cu, dict):
                continue
            bid = cu.get("bundle_id")
            if bid is None or str(bid).strip() == "":
                bid = obid
            if bid is None or str(bid).strip() == "":
                continue
            bkey = str(bid).strip()
            if bkey not in seen:
                seen.add(bkey)
                ordered_bundles.append(bkey)
    ntot = len(ordered_bundles)
    logger.info(
        "supply_queue: bundle состав — уникальных bundle_id={}, trigger={}, параллель={}, delay_sec={}",
        ntot,
        trigger,
        max_concurrent,
        delay_sec,
    )
    if on_progress:
        if ntot == 0:
            await on_progress("Товарный состав (supply-order/bundle): нет уникальных bundle в отфильтрованных заявках…")
        else:
            await on_progress(
                "Товарный состав (supply-order/bundle): запросов к API: {}, до {} параллельно…".format(
                    ntot, max_concurrent
                )
            )
    sem = asyncio.Semaphore(max(1, int(max_concurrent)))
    done_lock = asyncio.Lock()
    done_count = 0

    async def _fetch_one_bundle(bkey: str, idx: int) -> tuple[str, list[dict]]:
        nonlocal done_count
        async with sem:
            logger.info("supply_queue: bundle {}/{} id={} trigger={}", idx, ntot, bkey, trigger)
            if on_progress:
                await on_progress(
                    "Товарный состав (bundle): запрос {} из {}, id {}…".format(idx, ntot, bkey),
                )
            items = await client.get_supply_order_bundle_items_all_pages(
                bkey,
                item_tags_calculation=None,
            )
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)
        async with done_lock:
            done_count += 1
            if on_progress and ntot > 0:
                await on_progress("Товарный состав (bundle): выполнено {} из {}…".format(done_count, ntot))
        return bkey, items

    if ordered_bundles:
        pairs = await asyncio.gather(
            *(_fetch_one_bundle(bkey, i + 1) for i, bkey in enumerate(ordered_bundles))
        )
        for bkey, items in pairs:
            bundle_cache[bkey] = items
    for row in rows_subset:
        if not isinstance(row, dict):
            continue
        obid = row.get("order_bundle_id")
        for cu in row.get("cargo_units") or []:
            if not isinstance(cu, dict):
                continue
            bid = cu.get("bundle_id")
            if bid is None or str(bid).strip() == "":
                bid = obid
            if bid is None or str(bid).strip() == "":
                cu["line_items"] = []
                continue
            bkey = str(bid).strip()
            cu["line_items"] = list(bundle_cache.get(bkey) or [])


@router.post("/api/supply-queue/composition-refresh-clear-cache")
async def api_supply_queue_composition_refresh_clear_cache(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Удалить снимки на диске (общий и сессионный), сбросить сессию таблицы и беклог лога."""
    refs = set()
    rs = _supply_queue_read_shared_snapshot_ref()
    if rs:
        refs.add(rs)
    r_sess = _get_supply_queue_snapshot_ref(request.session)
    if r_sess:
        refs.add(r_sess)
    for ref in refs:
        _delete_supply_queue_snapshot_file_if_exists(ref)
    _supply_queue_write_shared_snapshot_ref(None)
    request.session.pop(SUPPLY_QUEUE_SESSION_SNAPSHOT_KEY, None)
    st = _supply_queue_load_composition_job_state()
    st["backlog"] = []
    st["last_error"] = ""
    _supply_queue_save_composition_job_state(st)
    logger.info("supply_queue: кеш очереди поставок очищен (сессия + файлы), пользователь={}", user.username)
    return JSONResponse({"ok": True})


# ---------- API поставок (создание по черновику + таймслот, список, удаление, печать ШК) ----------

# Официальные статусы поставки Ozon → подпись на русском (документация API)
# Статусы записи грузомест (отображаются под статусом поставки)
CARGO_PLACES_STATUS_LABELS = {
    "": "—",
    "PENDING": "отправлено",
    "IN_PROGRESS": "формируются",
    "SUCCESS": "успешно",
    "FAILED": "ошибка",
}

SUPPLY_STATUS_LABELS = {
    **SUPPLY_ORDER_STATE_LABELS_RU,
    "created": "Создано",
}


@router.get("/api/supplies")
async def api_supplies_list(
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Список поставок Ozon для таблицы на странице очереди поставок."""
    try:
        r = await db.execute(select(OzonSupply).order_by(OzonSupply.created_at.desc()))
        rows = r.scalars().all()

        # По запросу пользователя: не фильтруем API-список очереди поставок.
        # Возвращаем все записи из БД.
        return JSONResponse(content={
            "supplies": [
                {
                    "id": s.id,
                    "ozon_supply_id": s.ozon_supply_id or "",
                    "posting_number": getattr(s, "posting_number", None) or "",
                    "crossdock_cluster_id": getattr(s, "crossdock_cluster_id", None),
                    "destination_warehouse": getattr(s, "destination_warehouse", None) or "",
                    "shipment_date": s.shipment_date or "",
                    "delivery_date_estimated": s.delivery_date_estimated or "",
                    "composition": s.composition if isinstance(s.composition, list) else [],
                    "status": _normalize_supply_status_for_ui(s.status or "created"),
                    "status_label": SUPPLY_STATUS_LABELS.get(
                        _normalize_supply_status_for_ui(s.status or "created"),
                        _normalize_supply_status_for_ui(s.status or "created"),
                    ),
                    "has_cargo_places": bool(getattr(s, "has_cargo_places", 0)),
                    "cargo_places_status": getattr(s, "cargo_places_status", None) or "",
                    "cargo_places_status_label": CARGO_PLACES_STATUS_LABELS.get(getattr(s, "cargo_places_status", None) or "", "—"),
                    "created_at": s.created_at.isoformat() if getattr(s.created_at, "isoformat", None) else str(s.created_at),
                }
                for s in rows
            ]
        })
    except Exception as e:
        logger.warning("api supplies list: %s", e)
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/api/supplies/sync-from-lk")
async def api_supplies_sync_from_lk(
    background_tasks: BackgroundTasks,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Подтянуть поставки, созданные в ЛК Ozon (не через MPI интерфейс), и upsert в `ozon_supplies`.

    Период: «последние 30 дней».
    Ключ идентификации: `posting_number` (13 цифр).
    """
    activate_manual_supply_priority()
    try:
        now_utc = datetime.now(timezone.utc)
        since_utc = now_utc - timedelta(days=30)
        since_iso = since_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(
            "api_supplies_sync_from_lk: enqueue background sync since_iso={} to_iso={}",
            since_iso,
            to_iso,
        )
        # Важно: не делаем вызовы к Ozon внутри HTTP обработчика.
        # Иначе запрос легко упирается в nginx timeout (504).
        if _supplies_sync_from_lk_lock.locked():
            return JSONResponse(
                content={
                    "ok": True,
                    "queued": False,
                    "already_running": True,
                    "period": {"since": since_iso, "to": to_iso},
                }
            )
        background_tasks.add_task(_background_run_supplies_sync_from_lk_period, since_iso, to_iso)
        return JSONResponse(
            content={
                "ok": True,
                "queued": True,
                "period": {"since": since_iso, "to": to_iso},
            }
        )

        client = OzonAPIClient()
        logger.info(
            "api_supplies_sync_from_lk: start user={} since_iso={} to_iso={}",
            getattr(user, "username", None) or getattr(user, "id", None),
            since_iso,
            to_iso,
        )

        # Источник кандидатов: v2/posting/fbo/list через get_orders().
        # Мы берём из ответа 13-значные `posting_number` и (если удаётся) 8-значные order_id.
        lk_postings = await client.get_orders(since=since_iso, to=to_iso, limit=1000)
        lk_postings = lk_postings if isinstance(lk_postings, list) else []
        logger.info("api_supplies_sync_from_lk: lk_postings_count={}", len(lk_postings))

        re_13 = re.compile(r"(?<!\d)\d{13}(?!\d)")
        re_8 = re.compile(r"(?<!\d)\d{8}(?!\d)")

        def _extract_date_str(raw) -> str:
            if raw is None:
                return ""
            s = str(raw).strip()
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                return s[:10]
            return ""

        def _extract_first_digits_from_keys(obj: object, keys: list[str], length: int) -> str:
            if not isinstance(obj, dict):
                return ""
            for k in keys:
                if k not in obj:
                    continue
                v = obj.get(k)
                if v is None:
                    continue
                s = str(v).strip()
                if s.isdigit() and len(s) == length:
                    return s
            return ""

        def _extract_posting_candidate(obj: dict) -> dict | None:
            pn = _extract_first_digits_from_keys(
                obj,
                ["posting_number", "postingNumber", "supply_id", "supplyId", "supply_id_str", "supplyIdStr"],
                13,
            )
            order_id = _extract_first_digits_from_keys(
                obj,
                ["ozon_supply_id", "order_id", "orderId", "supply_order_id", "supplyOrderId", "supplyOrderID"],
                8,
            )

            if not pn or not order_id:
                try:
                    text = _json.dumps(obj, ensure_ascii=False, default=str)
                except Exception:
                    text = str(obj)
                if not pn:
                    m13 = re_13.findall(text)
                    pn = m13[0] if m13 else ""
                if not order_id:
                    m8 = re_8.findall(text)
                    order_id = m8[0] if m8 else ""

            if not pn:
                return None

            dest = ""
            for dk in ("destination_warehouse", "drop_off_warehouse", "storage_warehouse", "dropOffWarehouse"):
                raw = obj.get(dk)
                if not raw:
                    continue
                if isinstance(raw, dict):
                    dest = (raw.get("name") or raw.get("warehouse_name") or "").strip()
                    if dest:
                        break
                elif isinstance(raw, str):
                    dest = raw.strip()
                    if dest:
                        break

            shipment_date = _extract_date_str(
                obj.get("shipment_date") or obj.get("shipmentDate") or obj.get("created_at") or obj.get("createdAt")
            )

            status = (obj.get("status") or obj.get("state") or obj.get("order_state") or "").strip()
            if status.startswith("ORDER_STATE_"):
                status = status.replace("ORDER_STATE_", "", 1)

            return {
                "posting_number": pn,
                "order_id": order_id or "",
                "destination_warehouse": dest,
                "shipment_date": shipment_date,
                "status": status,
            }

        candidates_by_pn: dict[str, dict] = {}
        for obj in lk_postings:
            if not isinstance(obj, dict):
                continue
            cand = _extract_posting_candidate(obj)
            if not cand:
                continue
            pn = cand["posting_number"]
            if not pn:
                continue
            prev = candidates_by_pn.get(pn)
            if prev is None:
                candidates_by_pn[pn] = cand
            else:
                if not prev.get("order_id") and cand.get("order_id"):
                    prev["order_id"] = cand.get("order_id")
                if not prev.get("destination_warehouse") and cand.get("destination_warehouse"):
                    prev["destination_warehouse"] = cand.get("destination_warehouse")
                if not prev.get("shipment_date") and cand.get("shipment_date"):
                    prev["shipment_date"] = cand.get("shipment_date")
                if (not prev.get("status") or prev.get("status") == "created") and cand.get("status"):
                    prev["status"] = cand.get("status")

        # Основная проблема: `posting_number` из v2/posting/fbo/list не является гарантированным
        # идентификатором "supply-order". Поэтому нельзя сразу upsert по pn — нужно верифицировать
        # через v3/supply-order/get по найденному `order_id` (8 цифр).

        order_ids_candidates: list[str] = []
        seen_oid: set[str] = set()
        for cand in candidates_by_pn.values():
            oid = str(cand.get("order_id") or "").strip()
            if not oid or not oid.isdigit() or len(oid) != 8:
                continue
            if oid in seen_oid:
                continue
            seen_oid.add(oid)
            order_ids_candidates.append(oid)

        max_order_ids = 25
        if len(order_ids_candidates) > max_order_ids:
            order_ids_candidates = order_ids_candidates[:max_order_ids]

        if not order_ids_candidates:
            return JSONResponse(
                content={
                    "ok": True,
                    "added": 0,
                    "updated": 0,
                    "queued_composition_fill_ids": [],
                    "period": {"since": since_iso, "to": to_iso},
                }
            )

        # Не делаем `get_supply_info` по каждому order_id внутри HTTP-запроса.
        # Иначе endpoint легко упрётся в Nginx timeout (504). Тяжёлую часть переносим в background.
        max_enqueue = 10
        order_ids_candidates = list(dict.fromkeys(order_ids_candidates))[:max_enqueue]
        background_tasks.add_task(_background_sync_supplies_from_order_ids, order_ids_candidates)
        return JSONResponse(
            content={
                "ok": True,
                "queued_order_ids_count": len(order_ids_candidates),
                "period": {"since": since_iso, "to": to_iso},
            }
        )

        r_exist = await db.execute(
            select(OzonSupply).where(OzonSupply.ozon_supply_id.in_(order_ids_candidates))
        )
        existing_rows = r_exist.scalars().all()
        existing_by_order_id = {str(getattr(r, "ozon_supply_id", "") or ""): r for r in existing_rows}

        verified: list[dict] = []
        skipped_invalid = 0
        skipped_existing = 0
        skipped_error = 0

        for order_id in order_ids_candidates:
            if order_id in existing_by_order_id:
                skipped_existing += 1
                continue

            info_data = await client.get_supply_info(order_id)
            if info_data.get("_error"):
                skipped_error += 1
                continue

            # Извлекаем supplies/supply_id (13 цифр) из ответа v3/supply-order/get
            result = info_data.get("result") or {}
            inner = result.get("result") if isinstance(result, dict) else result
            orders = (inner or {}).get("orders") or info_data.get("orders") or []
            if isinstance(orders, dict):
                orders = [orders]
            if not isinstance(orders, list) or not orders:
                skipped_invalid += 1
                continue

            order0 = orders[0] if isinstance(orders[0], dict) else {}
            supplies = order0.get("supplies") or []
            if isinstance(supplies, dict):
                supplies = [supplies]
            if not isinstance(supplies, list) or not supplies:
                skipped_invalid += 1
                continue

            sup0 = supplies[0] if isinstance(supplies[0], dict) else {}
            supply_id_13 = (
                sup0.get("supply_id")
                or sup0.get("supplyId")
                or (sup0.get("supply_ids")[0] if isinstance(sup0.get("supply_ids"), list) and sup0.get("supply_ids") else None)
                or (sup0.get("supply_ids")[0] if isinstance(sup0.get("supply_ids"), list) and sup0.get("supply_ids") else None)
            )
            supply_id_13 = str(supply_id_13).strip() if supply_id_13 is not None else ""
            if not supply_id_13 or not supply_id_13.isdigit() or len(supply_id_13) < 13:
                skipped_invalid += 1
                continue

            # best-effort поля: destination_warehouse и shipment_date
            dest = ""
            shipment_date = ""
            storage_wh = sup0.get("storage_warehouse") or sup0.get("drop_off_warehouse") or sup0.get("drop_off_warehouse")
            if isinstance(storage_wh, dict):
                dest = (storage_wh.get("name") or "").strip()
            shipment_date = str(
                sup0.get("shipment_date") or sup0.get("shipmentDate") or ""
            ).strip()[:10]

            # best-effort status: ищем order_state/state в items
            status = ""
            try:
                items_raw = (
                    inner.get("items")
                    or result.get("items")
                    or info_data.get("items")
                    or info_data.get("orders")
                    or result.get("orders")
                    or []
                )
                if isinstance(items_raw, dict):
                    items_raw = [items_raw]
                if isinstance(items_raw, list):
                    for it in items_raw:
                        if not isinstance(it, dict):
                            continue
                        st = (it.get("order_state") or it.get("state") or "").strip()
                        if st:
                            if st.startswith("ORDER_STATE_"):
                                st = st.replace("ORDER_STATE_", "", 1)
                            status = st
                            break
            except Exception:
                pass

            verified.append(
                {
                    "posting_number": supply_id_13[:13],
                    "order_id": order_id,
                    "destination_warehouse": dest,
                    "shipment_date": shipment_date,
                    "status": status,
                }
            )

        if not verified:
            return JSONResponse(
                content={
                    "ok": True,
                    "added": 0,
                    "updated": 0,
                    "queued_composition_fill_ids": [],
                    "period": {"since": since_iso, "to": to_iso},
                    "skipped_invalid": skipped_invalid,
                    "skipped_existing": skipped_existing,
                    "skipped_error": skipped_error,
                }
            )

        posting_numbers = [v["posting_number"] for v in verified if v.get("posting_number")]
        posting_numbers = list(dict.fromkeys(posting_numbers))[:50]

        r_exist2 = await db.execute(select(OzonSupply).where(OzonSupply.posting_number.in_(posting_numbers)))
        existing_rows2 = r_exist2.scalars().all()
        existing_by_pn = {str(getattr(r, "posting_number", "") or ""): r for r in existing_rows2}

        updated_count = 0
        added_count = 0
        queued_fill_ids: list[int] = []

        status_updated_count = 0
        for v in verified:
            pn = v["posting_number"]
            order_id = v["order_id"]
            dest = v.get("destination_warehouse") or ""
            shipment_date = v.get("shipment_date") or ""
            status = v.get("status") or ""

            if pn in existing_by_pn:
                row = existing_by_pn[pn]
                changed = False
                if (not getattr(row, "ozon_supply_id", None) or not str(row.ozon_supply_id).strip()) and order_id:
                    row.ozon_supply_id = order_id
                    changed = True
                if (not getattr(row, "destination_warehouse", None) or not str(row.destination_warehouse).strip()) and dest:
                    row.destination_warehouse = dest
                    changed = True
                if (not getattr(row, "shipment_date", None) or not str(row.shipment_date).strip()) and shipment_date:
                    row.shipment_date = shipment_date
                    changed = True
                if status:
                    new_status = _normalize_supply_status_for_ui(status)
                    cur_status = _normalize_supply_status_for_ui(getattr(row, "status", None) or "")
                    if new_status and new_status != cur_status:
                        row.status = new_status
                        changed = True

                row_comp = getattr(row, "composition", None)
                comp_is_empty = not isinstance(row_comp, list) or len(row_comp) == 0
                if comp_is_empty and order_id and order_id.isdigit() and changed:
                    queued_fill_ids.append(int(row.id))
                if changed:
                    updated_count += 1
                continue

            row = OzonSupply(
                ozon_supply_id=order_id,
                posting_number=pn,
                destination_warehouse=dest,
                shipment_date=shipment_date,
                timeslot_from=None,
                timeslot_to=None,
                delivery_date_estimated="",
                composition=[],
                status=_normalize_supply_status_for_ui(status or "created"),
                has_cargo_places=0,
                cargo_places_status="",
                cargo_places_data=[],
                draft_id=None,
            )
            db.add(row)
            await db.flush()
            added_count += 1
            queued_fill_ids.append(int(row.id))

        await db.commit()

        max_fill = 10
        queued_fill_ids = list(dict.fromkeys(queued_fill_ids))[:max_fill]
        if queued_fill_ids:
            background_tasks.add_task(_background_fill_composition_for_lk_supplies, queued_fill_ids)

        return JSONResponse(
            content={
                "ok": True,
                "added": added_count,
                "updated": updated_count,
                "queued_composition_fill_ids": queued_fill_ids,
                "period": {"since": since_iso, "to": to_iso},
            }
        )
    except Exception as e:
        logger.warning("api supplies sync-from-lk failed: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.get("/api/supplies/sync-from-lk-status")
async def api_supplies_sync_from_lk_status(
    user: User = Depends(verify_site_user),
):
    """Статус фоновой синхронизации поставок из ЛК Ozon (для спиннера в UI)."""
    started_at = _supplies_sync_from_lk_state.get("last_started_at")
    finished_at = _supplies_sync_from_lk_state.get("last_finished_at")
    return JSONResponse(
        content={
            "ok": True,
            "in_progress": _supplies_sync_from_lk_lock.locked(),
            "last_started_at": started_at.isoformat() if hasattr(started_at, "isoformat") else None,
            "last_finished_at": finished_at.isoformat() if hasattr(finished_at, "isoformat") else None,
            "last_error": _supplies_sync_from_lk_state.get("last_error") or "",
            "progress": {
                "stage": str(_supplies_sync_from_lk_state.get("stage") or ""),
                "message": str(_supplies_sync_from_lk_state.get("message") or ""),
                "total_order_ids": int(_supplies_sync_from_lk_state.get("total_order_ids") or 0),
                "processed_order_ids": int(_supplies_sync_from_lk_state.get("processed_order_ids") or 0),
                "added": int(_supplies_sync_from_lk_state.get("added") or 0),
                "updated": int(_supplies_sync_from_lk_state.get("updated") or 0),
                "composition_filled": int(_supplies_sync_from_lk_state.get("composition_filled") or 0),
                "cargo_rows_filled": int(_supplies_sync_from_lk_state.get("cargo_rows_filled") or 0),
                "cargo_items_total": int(_supplies_sync_from_lk_state.get("cargo_items_total") or 0),
            },
        }
    )


async def _background_run_supplies_sync_from_lk_period(since_iso: str, to_iso: str) -> None:
    """Обертка sync-from-lk: lock + статус выполнения для polling на фронте."""
    if _supplies_sync_from_lk_lock.locked():
        return
    async with _supplies_sync_from_lk_lock:
        _supplies_sync_from_lk_state["last_started_at"] = datetime.now(MSK)
        _supplies_sync_from_lk_state["last_error"] = ""
        _set_supplies_sync_progress(
            stage="START",
            message="Запуск синхронизации",
            total_order_ids=0,
            processed_order_ids=0,
            added=0,
            updated=0,
            composition_filled=0,
            cargo_rows_filled=0,
            cargo_items_total=0,
        )
        try:
            await _background_sync_supplies_from_lk_period(since_iso, to_iso)
            _set_supplies_sync_progress(stage="DONE", message="Синхронизация завершена")
        except Exception as e:
            _supplies_sync_from_lk_state["last_error"] = str(e)[:512]
            _set_supplies_sync_progress(stage="FAILED", message=str(e)[:180])
            logger.warning("background_run_supplies_sync_from_lk_period exception={}", e, exc_info=True)
        finally:
            _supplies_sync_from_lk_state["last_finished_at"] = datetime.now(MSK)


@router.get("/api/supplies/draft-options")
async def api_supplies_draft_options(
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Продукты с SKU Ozon, кластеры Ozon, дефолты delivery_info и компактная матрица скана очереди для страницы создания заявки."""
    try:
        result_pr = await db.execute(
            select(Product.id, Product.name, Product.article, Product.ozon_sku).where(
                Product.ozon_sku.isnot(None), Product.ozon_sku != 0
            ).order_by(Product.name)
        )
        rows = result_pr.all()
        products = [
            {
                "id": r.id,
                "name": r.name or "",
                "article": (r.article or "").strip(),
                "ozon_sku": r.ozon_sku,
            }
            for r in rows
        ]
        client = OzonAPIClient()
        clusters = await client.get_cluster_list(cluster_type="CLUSTER_TYPE_OZON")
        delivery_defaults = await _delivery_defaults_from_supply_draft_config(db)
        _scan, sq_results, week_dates, scanned_at_str = await _load_supply_queue_cluster_scan(db, max_days=14)
        cluster_scan = {
            "scanned_at_str": scanned_at_str,
            "week_dates": [d.isoformat() for d in week_dates],
            "results": sq_results,
        }
        return JSONResponse(
            content={
                "ok": True,
                "products": products,
                "clusters": clusters or [],
                "delivery_defaults": delivery_defaults,
                "cluster_scan": cluster_scan,
            }
        )
    except Exception as e:
        logger.warning("api supplies draft-options: {}", e)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": str(e),
                "products": [],
                "clusters": [],
                "delivery_defaults": {},
                "cluster_scan": {},
            },
        )


@router.post("/api/supplies/draft")
async def api_supplies_draft(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Создать черновик, пауза 3 с, опрос статуса до SUCCESS, запрос таймслотов. Возврат draft_id, timeslots, clusters."""
    flow_id = uuid.uuid4().hex[:12]
    flog = logger.bind(supply_draft_flow_id=flow_id)
    activate_manual_supply_priority()
    uname = (getattr(user, "username", None) or "").strip() or str(getattr(user, "id", "user"))
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    cluster_info = body.get("cluster_info")
    delivery_date = body.get("delivery_date") or ""
    if not cluster_info or not delivery_date:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужны cluster_info и delivery_date"})
    ci = cluster_info if isinstance(cluster_info, dict) else {}
    di = ci.get("delivery_info") if isinstance(ci.get("delivery_info"), dict) else {}
    macrolocal = ci.get("macrolocal_cluster_id") if ci.get("macrolocal_cluster_id") not in (None, "", 0, "0") else ci.get("crossdock_cluster_id")
    items_count = len(ci.get("items") or [])
    drop_off_wid = di.get("drop_off_warehouse_id")
    seller_wh = di.get("seller_warehouse_id")
    flog.info(
        "supply_draft: старт user={} delivery_date={} macrolocal_cluster_id={} items_count={} drop_off_warehouse_id={} seller_warehouse_id={}",
        uname,
        delivery_date,
        macrolocal,
        items_count,
        drop_off_wid,
        seller_wh,
    )
    attempts_trace = []
    try:
        client = OzonAPIClient()
        result = {}
        for attempt_idx in range(1, 4):
            result = await client.create_fbs_crossdock_draft(
                cluster_info=cluster_info,
                delivery_date=delivery_date,
                additional_cluster_id=body.get("additional_cluster_id"),
                stock_type=body.get("stock_type", "FIT"),
            )
            attempts_trace.append(f"draft_crossdock_create: попытка {attempt_idx}/3 -> HTTP {result.get('status_code') or 200}")
            if not result.get("_error"):
                break
            if attempt_idx < 3:
                await asyncio.sleep(2)
        if result.get("_error"):
            flog.warning(
                "supply_draft: POST /v1/draft/crossdock/create неуспех user={} http_status={} error={} hint={}",
                uname,
                result.get("status_code"),
                result.get("_error"),
                "По документации Ozon для crossdock/create: 2/мин, 50/час, 500/сутки; 429 также возможен от параллельных вкладок, слот-трекера/парсера очереди (каждый create считается) или ЛК с тем же аккаунтом.",
            )
            return JSONResponse(content={"ok": False, "error": result.get("_error") or "Ошибка создания черновика", "data": result, "attempts_trace": attempts_trace})
        data = result.get("data") or {}
        draft_id = data.get("draft_id")
        if not draft_id or draft_id == 0:
            err_msg = (
                "; ".join(
                    (e.get("message") or e.get("error_message") or str(e) for e in (data.get("errors") or []))
                )
                or (data.get("message") or "Черновик не создан")
            )
            flog.warning(
                "supply_draft: ответ create без draft_id user={} err_msg={!r} ozon_keys={}",
                uname,
                err_msg,
                list(data.keys()) if isinstance(data, dict) else None,
            )
            return JSONResponse(content={"ok": False, "error": err_msg, "draft_id": None})
        flog.info("supply_draft: create ok draft_id={} pause_2s_before_poll", draft_id)
        await asyncio.sleep(2)
        last_state = None
        info = {}
        for attempt in range(3):
            info = await client.get_draft_info(str(draft_id))
            if info.get("_error"):
                sc = info.get("status_code")
                attempts_trace.append(f"draft_create_info: попытка {attempt + 1}/3 -> HTTP {sc or 0}")
                oz_snip = info.get("ozon_response")
                if isinstance(oz_snip, dict):
                    oz_snip = str(oz_snip)[:500]
                else:
                    oz_snip = str(oz_snip or "")[:500]
                flog.warning(
                    "supply_draft: POST /v2/draft/create/info ошибка attempt={}/3 draft_id={} http_status={} _error={} ozon_snippet={!r}",
                    attempt + 1,
                    draft_id,
                    sc,
                    info.get("_error"),
                    oz_snip,
                )
                last_state = f"http_error:{sc}"
                if attempt < 2:
                    await asyncio.sleep(2)
                continue
            st = info.get("status") or info.get("state") or (info.get("result") and (info["result"].get("status") or info["result"].get("state")))
            last_state = st
            attempts_trace.append(f"draft_create_info: попытка {attempt + 1}/3 -> state={st!s}")
            if st == "SUCCESS":
                flog.info(
                    "supply_draft: черновик SUCCESS draft_id={} poll_attempts={}",
                    draft_id,
                    attempt + 1,
                )
                break
            if st == "FAILED":
                return JSONResponse(content={"ok": False, "error": "Черновик не создался (status=FAILED)", "draft_id": draft_id, "attempts_trace": attempts_trace, "data": info})
            if attempt < 2:
                await asyncio.sleep(2)
        else:
            flog.error(
                "supply_draft: таймаут статуса черновика draft_id={} last_state={!r}",
                draft_id,
                last_state,
            )
            return JSONResponse(content={"ok": False, "error": "Черновик не перешёл в SUCCESS за 3 попытки", "draft_id": draft_id, "attempts_trace": attempts_trace, "data": info})
        clusters = info.get("clusters") or (info.get("result") or {}).get("clusters") or []
        date_from = delivery_date
        date_to = delivery_date
        try:
            from datetime import datetime as _dt
            _d = _dt.strptime(delivery_date, "%Y-%m-%d").date()
            from datetime import timedelta as _td
            date_to = (_d + _td(days=7)).strftime("%Y-%m-%d")
        except Exception:
            pass
        selected_wh = []
        selected_cid = None
        try:
            selected_cid = int(macrolocal) if macrolocal not in (None, "", 0, "0") else None
        except Exception:
            selected_cid = None
        if selected_cid is not None:
            selected_wh = [{"macrolocal_cluster_id": selected_cid}]
        if not selected_wh:
            return JSONResponse(content={"ok": False, "error": "Не удалось определить выбранный кластер для запроса таймслотов", "draft_id": draft_id, "attempts_trace": attempts_trace})
        flog.info(
            "supply_draft: запрос таймслотов draft_id={} date_from={} date_to={} selected_warehouses={}",
            draft_id,
            date_from,
            date_to,
            len(selected_wh),
        )
        ts_resp = await client.get_draft_timeslots(
            draft_id=int(draft_id),
            date_from=date_from,
            date_to=date_to,
            supply_type="CROSSDOCK",
            selected_cluster_warehouses=selected_wh,
        )
        attempts_trace.extend([
            f"draft_timeslot_info: попытка {a.get('attempt')}/3 -> HTTP {a.get('status_code')}"
            for a in (ts_resp.get("attempts") or [])
            if isinstance(a, dict)
        ])
        if ts_resp.get("_error"):
            flog.warning(
                "supply_draft: таймслоты ошибка draft_id={} _error={} http_status={} ozon_snippet={!r}",
                draft_id,
                ts_resp.get("_error"),
                ts_resp.get("status_code"),
                str(ts_resp.get("ozon_response", ""))[:500],
            )
        else:
            res = ts_resp.get("result")
            drop_off = (res or {}).get("drop_off_warehouse_timeslots") if isinstance(res, dict) else None
            days = drop_off.get("days") if isinstance(drop_off, dict) else None
            flog.info(
                "supply_draft: таймслоты ok draft_id={} result_keys={} days_count={}",
                draft_id,
                list(res.keys()) if res else [],
                len(days) if isinstance(days, list) else 0,
            )
        return JSONResponse(content={
            "ok": True,
            "draft_id": draft_id,
            "timeslots": ts_resp,
            "clusters": clusters,
            "attempts_trace": attempts_trace,
        })
    except Exception as e:
        flog.exception("supply_draft: исключение в потоке создания черновика: {}", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/timeslot-info")
async def api_supplies_timeslot_info(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Прокси для POST /v2/draft/timeslot/info. Возвращает исходный ответ и плоский список слотов."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})

    date_from = str(body.get("date_from") or "").strip()
    date_to = str(body.get("date_to") or "").strip() or date_from
    supply_type = "CROSSDOCK"
    selected_cluster_warehouses = body.get("selected_cluster_warehouses")
    if not date_from:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен date_from (YYYY-MM-DD)"})
    try:
        d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        d_to = datetime.strptime(date_to or date_from, "%Y-%m-%d").date()
    except ValueError:
        return JSONResponse(status_code=400, content={"ok": False, "error": "date_from/date_to: формат YYYY-MM-DD"})
    if d_to < d_from:
        d_to = d_from
    # Один выбранный день: date_from и date_to совпадают (не расширяем период).
    d_to = d_from
    date_from = d_from.strftime("%Y-%m-%d")
    date_to = d_to.strftime("%Y-%m-%d")
    try:
        draft_id = int(body.get("draft_id") or 0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "draft_id должен быть числом"})
    if draft_id <= 0:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "draft_id должен быть > 0 (получите draft_id после создания черновика)"},
        )

    filtered_wh = []
    if isinstance(selected_cluster_warehouses, list):
        for item in selected_cluster_warehouses:
            if not isinstance(item, dict):
                continue
            try:
                ml = int(item.get("macrolocal_cluster_id"))
            except (TypeError, ValueError):
                continue
            if ml <= 0:
                continue
            # CROSSDOCK: только macrolocal_cluster_id — storage_warehouse_id в timeslot/info даёт 400 у Ozon.
            out_item = {"macrolocal_cluster_id": ml}
            filtered_wh.append(out_item)

    try:
        logger.info(
            "api_supplies_timeslot_info: user={} draft_id={} date_from={} date_to={} wh_count={} supply_type={}",
            getattr(user, "username", "unknown"),
            draft_id,
            date_from,
            date_to,
            len(filtered_wh),
            supply_type,
        )
        client = OzonAPIClient()
        raw = await client.get_draft_timeslots(
            draft_id=draft_id,
            date_from=date_from,
            date_to=date_to,
            supply_type=supply_type,
            selected_cluster_warehouses=filtered_wh if filtered_wh else None,
        )
        if raw.get("_error"):
            oz = raw.get("ozon_response")
            oz_snip = str(oz)[:800] if oz is not None else ""
            logger.warning(
                "api_supplies_timeslot_info: Ozon error draft_id={} http={} snippet={}",
                draft_id,
                raw.get("status_code"),
                oz_snip,
            )
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": raw.get("_error") or "Ошибка Ozon API",
                    "status_code": raw.get("status_code"),
                    "attempts": raw.get("attempts") or [],
                    "ozon_response": raw.get("ozon_response"),
                },
            )

        result = raw.get("result") if isinstance(raw.get("result"), dict) else {}
        drop_off = result.get("drop_off_warehouse_timeslots") if isinstance(result, dict) else {}
        days = drop_off.get("days") if isinstance(drop_off, dict) else []
        slots_flat: list[dict] = []
        want_day = date_from[:10]
        for day in days if isinstance(days, list) else []:
            if not isinstance(day, dict):
                continue
            date_tz = str(day.get("date_in_timezone") or "")
            day_key = date_tz[:10] if len(date_tz) >= 10 else ""
            if day_key and day_key != want_day:
                continue
            for ts in day.get("timeslots") or []:
                if not isinstance(ts, dict):
                    continue
                from_tz = str(ts.get("from_in_timezone") or "")
                to_tz = str(ts.get("to_in_timezone") or "")
                slots_flat.append(
                    {
                        "date_in_timezone": date_tz,
                        "from_in_timezone": from_tz,
                        "to_in_timezone": to_tz,
                    }
                )
        return JSONResponse(
            content={
                "ok": True,
                "result": result,
                "slots": slots_flat,
                "attempts": raw.get("attempts") or [],
            }
        )
    except Exception as e:
        logger.warning("api_supplies_timeslot_info: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/create-draft")
async def api_supplies_create_draft(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Создать черновик поставки через POST /v1/draft/crossdock/create."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})

    macrolocal_raw = body.get("macrolocal_cluster_id")
    items_raw = body.get("items")
    try:
        macrolocal_cluster_id = int(macrolocal_raw)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "macrolocal_cluster_id должен быть числом"})
    if macrolocal_cluster_id <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "macrolocal_cluster_id должен быть > 0"})
    if not isinstance(items_raw, list) or not items_raw:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен непустой массив items"})

    items = []
    for it in items_raw:
        if not isinstance(it, dict):
            continue
        try:
            sku = int(it.get("sku") or 0)
            qty = int(it.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if sku <= 0 or qty <= 0:
            continue
        items.append({"sku": sku, "quantity": qty})
    if not items:
        return JSONResponse(status_code=400, content={"ok": False, "error": "В items нет валидных sku/quantity"})

    try:
        delivery_defaults = await _delivery_defaults_from_supply_draft_config(db)
        cluster_info = {
            "items": items,
            "macrolocal_cluster_id": macrolocal_cluster_id,
            "delivery_info": delivery_defaults,
        }
        client = OzonAPIClient()
        draft_resp = await client.create_fbs_crossdock_draft(
            cluster_info=cluster_info,
            delivery_date=str(body.get("delivery_date") or date.today().strftime("%Y-%m-%d")),
        )
        if draft_resp.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": draft_resp.get("_error") or "Ошибка создания черновика",
                    "status_code": draft_resp.get("status_code"),
                    "response_text": draft_resp.get("response_text"),
                    "limits_hint": "Ограничения Ozon: 2/мин, 50/час, 500/день. Черновик живет 30 минут.",
                },
            )
        data = draft_resp.get("data") if isinstance(draft_resp.get("data"), dict) else {}
        draft_id = data.get("draft_id")
        try:
            draft_id_int = int(draft_id)
        except (TypeError, ValueError):
            draft_id_int = 0
        if draft_id_int <= 0:
            err_msg = "Черновик не создан"
            errors = data.get("errors") if isinstance(data, dict) else None
            if isinstance(errors, list) and errors:
                first = errors[0] if isinstance(errors[0], dict) else {}
                err_msg = str(first.get("message") or first.get("error_message") or err_msg)
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": err_msg,
                    "ozon_response": data,
                    "limits_hint": "Ограничения Ozon: 2/мин, 50/час, 500/день. Черновик живет 30 минут.",
                },
            )
        # Ozon: /v2/draft/timeslot/info ожидает черновик после успешного /v2/draft/create/info (как в supply_draft).
        logger.info("api_supplies_create_draft: pause 2s then poll v2/draft/create/info draft_id={}", draft_id_int)
        await asyncio.sleep(2)
        last_state = None
        info: dict = {}
        for attempt in range(3):
            info = await client.get_draft_info(str(draft_id_int))
            if info.get("_error"):
                logger.warning(
                    "api_supplies_create_draft: draft/create/info attempt={}/3 draft_id={} err={}",
                    attempt + 1,
                    draft_id_int,
                    info.get("_error"),
                )
                last_state = f"http_error:{info.get('status_code')}"
                if attempt < 2:
                    await asyncio.sleep(2)
                continue
            st = info.get("status") or info.get("state") or (
                info.get("result") and (info["result"].get("status") or info["result"].get("state"))
            )
            last_state = st
            if st == "SUCCESS":
                logger.info("api_supplies_create_draft: draft SUCCESS draft_id={}", draft_id_int)
                break
            if st == "FAILED":
                return JSONResponse(
                    status_code=502,
                    content={
                        "ok": False,
                        "error": "Черновик не создался (status=FAILED)",
                        "draft_id": draft_id_int,
                        "data": info,
                    },
                )
            if attempt < 2:
                await asyncio.sleep(2)
        else:
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": "Черновик не перешёл в SUCCESS за время ожидания; повторите создание или проверьте состав.",
                    "draft_id": draft_id_int,
                    "last_state": last_state,
                },
            )
        logger.info(
            "api_supplies_create_draft: user={} draft_id={} items={} macrolocal_cluster_id={}",
            getattr(user, "username", "unknown"),
            draft_id_int,
            len(items),
            macrolocal_cluster_id,
        )
        return JSONResponse(
            content={
                "ok": True,
                "draft_id": draft_id_int,
                "ozon_response": data,
                "draft_info": info,
                "limits_hint": "Ограничения Ozon: 2/мин, 50/час, 500/день. Черновик живет 30 минут.",
            }
        )
    except Exception as e:
        logger.warning("api_supplies_create_draft: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


# Подписи к error_reasons из POST /v2/draft/supply/create и /v2/draft/supply/create/status (док. Ozon).
DRAFT_SUPPLY_ERROR_REASON_RU: dict[str, str] = {
    "UNSPECIFIED": "Не определено",
    "SOME_SERVICE_ERROR": "Ошибка при редактировании поставки",
    "ORDER_SKU_LIMIT": "Количество товаров в поставке превышает 5000",
    "INVALID_QUANTITY_OR_UNITS": "Некорректное количество товаров или грузомест",
    "INVALID_QUANTITY_OR_QUANT": "Некорректное количество товаров или грузомест",
    "ORDER_ALREADY_CREATED": "Заявка уже создана",
    "ORDER_CREATION_IN_PROGRESS": "Создание заявки выполняется",
    "DRAFT_DOES_NOT_EXIST": "Черновик не существует",
    "CONTRACTOR_CAN_NOT_CREATE_ORDER": "Контрагент не может создать заявку",
    "INACTIVE_CONTRACT": "Нельзя редактировать состав при неактивном договоре",
    "DRAFT_INCORRECT_STATE": "Некорректный статус черновика",
    "INVALID_VOLUME": "Некорректный объём поставки",
    "INVALID_ROUTE": "Некорректный маршрут",
    "INVALID_STORAGE_WAREHOUSE": "Некорректный склад хранения",
    "INVALID_STORAGE_REGION": "Некорректный регион хранения",
    "INVALID_SPLITTING": "Некорректное разбиение",
    "INVALID_SUPPLY_CONTENT": "Некорректное содержимое поставки",
    "TIMESLOT_NOT_AVAILABLE": "Нет доступных таймслотов",
    "SKU_DISTRIBUTION_REQUIRED_BUT_NOT_POSSIBLE": "Требуется распределение SKU, но это невозможно",
    "DROPOFF_IN_DELIVERY_POINT_DISABLED_FOR_SELLER": "Кросс-док через пункт выдачи отключён для продавца",
    "XDOCK_IN_DELIVERY_POINT_DISABLED_FOR_SELLER": "Кросс-док через пункт выдачи отключён для продавца",
    "DRAFT_IS_LOCKED": "Черновик заблокирован",
    "INVALID_PACKAGE_UNITS_COUNTS": "Некорректное число грузомест",
    "SELLER_CONVERSATION_DOES_NOT_EXIST": "Точка отгрузки с таким ID не существует",
    "USER_CAN_NOT_CREATE_SELLER_CONVERSATION": "Пользователь не может написать продавцу",
    "SKU_WITH_ETTN_REQUIRED_TAG_NOT_ALLOWED_FOR_DROP_OFF_POINT": "Товар с признаком is_ettn_required недоступен для отгрузки в ПВЗ",
    "INVALID_SELLER_WAREHOUSE": "Склад продавца недоступен",
    "PICKUP_ORDER_LIMIT_EXCEEDED": "Превышен лимит заявок на самовывоз",
    "MINIMUM_VOLUME_IN_LITRES_INVALID": "Некорректный минимальный объём в литрах",
    "INVALID_CLUSTERS_COUNT": "Переданы не все кластеры из расчёта",
    "CAN_NOT_CREATE_ORDER": "Не удалось создать заявку",
    "UNDEFINED": "Неизвестная ошибка",
}

DRAFT_SUPPLY_CREATE_STATUS_RU: dict[str, str] = {
    "UNSPECIFIED": "Не определён",
    "SUCCESS": "Заявка создана",
    "IN_PROGRESS": "Создание заявки выполняется",
    "FAILED": "Не удалось создать заявку",
}


def _draft_supply_error_reasons_ru(reasons: object) -> list[str]:
    if not isinstance(reasons, list):
        return []
    out: list[str] = []
    for r in reasons:
        code = str(r or "").strip()
        if not code:
            continue
        out.append(DRAFT_SUPPLY_ERROR_REASON_RU.get(code, code))
    return out


def _draft_supply_unwrap_payload(data: dict) -> dict:
    res = data.get("result")
    if isinstance(res, dict):
        return res
    return data


def _extract_supply_id_from_supply_order_get(payload: dict) -> int | None:
    """Достаёт supply_id из ответа POST /v3/supply-order/get (с учётом вложенных массивов)."""
    if not isinstance(payload, dict):
        return None
    candidates = []
    result = payload.get("result")
    if isinstance(result, dict):
        # Частый формат: result.orders[]
        orders = result.get("orders")
        if isinstance(orders, list):
            candidates.extend(orders)
        # Иногда: result.items[]
        items = result.get("items")
        if isinstance(items, list):
            candidates.extend(items)
        # Иногда: result.result.items[]
        inner = result.get("result")
        if isinstance(inner, dict):
            i2 = inner.get("items")
            if isinstance(i2, list):
                candidates.extend(i2)
    elif isinstance(result, list):
        candidates.extend(result)
    # Редкий плоский формат
    direct_orders = payload.get("orders")
    if isinstance(direct_orders, list):
        candidates.extend(direct_orders)

    for obj in candidates:
        if not isinstance(obj, dict):
            continue
        raw_supply_id = obj.get("supply_id")
        if raw_supply_id in (None, "", 0, "0"):
            supplies = obj.get("supplies")
            if isinstance(supplies, list) and supplies:
                first = supplies[0] if isinstance(supplies[0], dict) else {}
                raw_supply_id = first.get("supply_id")
        try:
            sid = int(raw_supply_id)
            if sid > 0:
                return sid
        except (TypeError, ValueError):
            continue
    return None


@router.post("/api/supplies/draft-supply-create")
async def api_supplies_draft_supply_create(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Создать заявку на поставку: POST /v2/draft/supply/create (без сохранения в БД мастера)."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    try:
        draft_id = int(body.get("draft_id") or 0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "draft_id должен быть числом"})
    from_tz = str(body.get("from_in_timezone") or "").strip()
    to_tz = str(body.get("to_in_timezone") or "").strip()
    if draft_id <= 0 or not from_tz or not to_tz:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "Нужны draft_id, from_in_timezone, to_in_timezone"},
        )
    selected_wh: list[dict] = []
    raw_wh = body.get("selected_cluster_warehouses")
    if isinstance(raw_wh, list) and raw_wh:
        for item in raw_wh:
            if not isinstance(item, dict):
                continue
            try:
                ml = int(item.get("macrolocal_cluster_id") or 0)
            except (TypeError, ValueError):
                continue
            if ml <= 0:
                continue
            entry: dict = {"macrolocal_cluster_id": ml}
            try:
                sw = int(item.get("storage_warehouse_id") or 0)
                if sw > 0:
                    entry["storage_warehouse_id"] = sw
            except (TypeError, ValueError):
                pass
            selected_wh.append(entry)
    else:
        try:
            ml = int(body.get("macrolocal_cluster_id") or 0)
        except (TypeError, ValueError):
            ml = 0
        if ml > 0:
            selected_wh = [{"macrolocal_cluster_id": ml}]
    if not selected_wh:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "Нужен macrolocal_cluster_id или selected_cluster_warehouses"},
        )
    try:
        client = OzonAPIClient()
        data = await client.confirm_draft_supply(
            draft_id=draft_id,
            from_in_timezone=from_tz,
            to_in_timezone=to_tz,
            selected_cluster_warehouses=selected_wh,
            supply_type="CROSSDOCK",
        )
        if data.get("_error"):
            oz = data.get("ozon_response")
            reasons: list = []
            if isinstance(oz, dict):
                raw_r = oz.get("error_reasons")
                if isinstance(raw_r, list):
                    reasons = raw_r
            logger.warning(
                "api_supplies_draft_supply_create: user={} draft_id={} err={} reasons={}",
                getattr(user, "username", "unknown"),
                draft_id,
                data.get("_error"),
                reasons,
            )
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": data.get("_error") or "Ошибка Ozon API",
                    "status_code": data.get("status_code"),
                    "error_reasons": reasons,
                    "error_reasons_ru": _draft_supply_error_reasons_ru(reasons),
                    "ozon_response": oz,
                },
            )
        inner = _draft_supply_unwrap_payload(data)
        reasons = inner.get("error_reasons") or data.get("error_reasons") or []
        if not isinstance(reasons, list):
            reasons = []
        out_draft = inner.get("draft_id") if inner.get("draft_id") is not None else data.get("draft_id")
        logger.info(
            "api_supplies_draft_supply_create: user={} draft_id={} ozon_draft_id={} error_reasons_count={}",
            getattr(user, "username", "unknown"),
            draft_id,
            out_draft,
            len(reasons),
        )
        return JSONResponse(
            content={
                "ok": True,
                "draft_id": out_draft if out_draft is not None else draft_id,
                "error_reasons": reasons,
                "error_reasons_ru": _draft_supply_error_reasons_ru(reasons),
                "ozon_response": inner if inner else data,
            }
        )
    except Exception as e:
        logger.warning("api_supplies_draft_supply_create: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/draft-supply-create-status")
async def api_supplies_draft_supply_create_status(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Статус создания заявки: POST /v2/draft/supply/create/status."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    try:
        draft_id = int(body.get("draft_id") or 0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "draft_id должен быть числом"})
    if draft_id <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен draft_id > 0"})
    try:
        client = OzonAPIClient()
        data = await client.get_draft_supply_create_status(draft_id)
        if data.get("_error"):
            oz = data.get("ozon_response")
            reasons: list = []
            if isinstance(oz, dict):
                raw_r = oz.get("error_reasons")
                if isinstance(raw_r, list):
                    reasons = raw_r
            logger.warning(
                "api_supplies_draft_supply_create_status: user={} draft_id={} err={}",
                getattr(user, "username", "unknown"),
                draft_id,
                data.get("_error"),
            )
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": data.get("_error") or "Ошибка Ozon API",
                    "status_code": data.get("status_code"),
                    "error_reasons": reasons,
                    "error_reasons_ru": _draft_supply_error_reasons_ru(reasons),
                },
            )
        inner = _draft_supply_unwrap_payload(data)
        status = str(inner.get("status") or data.get("status") or "UNSPECIFIED").strip() or "UNSPECIFIED"
        order_id = inner.get("order_id")
        if order_id is None:
            order_id = data.get("order_id")
        reasons = inner.get("error_reasons") or data.get("error_reasons") or []
        if not isinstance(reasons, list):
            reasons = []
        status_ru = DRAFT_SUPPLY_CREATE_STATUS_RU.get(status, status)
        return JSONResponse(
            content={
                "ok": True,
                "status": status,
                "status_ru": status_ru,
                "order_id": order_id,
                "error_reasons": reasons,
                "error_reasons_ru": _draft_supply_error_reasons_ru(reasons),
            }
        )
    except Exception as e:
        logger.warning("api_supplies_draft_supply_create_status: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/cargoes-create")
async def api_supplies_cargoes_create(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Установка грузомест через /v1/cargoes/create; supply_id получаем из /v3/supply-order/get по order_id."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})

    try:
        order_id = int(body.get("order_id") or 0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "order_id должен быть числом"})
    if order_id <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен order_id > 0"})

    cargoes = body.get("cargoes")
    if not isinstance(cargoes, list) or not cargoes:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен непустой массив cargoes"})
    delete_current_version = bool(body.get("delete_current_version", True))

    try:
        client = OzonAPIClient()
        supply_info = await client.get_supply_info(str(order_id))
        if supply_info.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": supply_info.get("_error") or "Ошибка получения информации о заявке",
                    "ozon_response": supply_info.get("ozon_response"),
                },
            )
        supply_id = _extract_supply_id_from_supply_order_get(supply_info)
        if not supply_id:
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": "Не удалось извлечь supply_id из /v3/supply-order/get",
                    "ozon_response": supply_info,
                },
            )
        logger.info(
            "api_supplies_cargoes_create: user={} order_id={} supply_id={} cargoes_count={}",
            getattr(user, "username", "unknown"),
            order_id,
            supply_id,
            len(cargoes),
        )
        create_resp = await client.set_cargo_places(
            supply_id=supply_id,
            cargoes=cargoes,
            delete_current_version=delete_current_version,
        )
        if create_resp.get("_error"):
            oz = create_resp.get("ozon_response")
            err_obj = oz.get("errors") if isinstance(oz, dict) else None
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": create_resp.get("_error") or "Ошибка установки грузомест",
                    "status_code": create_resp.get("status_code"),
                    "supply_id": supply_id,
                    "errors": err_obj if isinstance(err_obj, dict) else {},
                    "ozon_response": oz,
                },
            )
        operation_id = str(create_resp.get("operation_id") or "").strip()
        errors_obj = create_resp.get("errors") if isinstance(create_resp.get("errors"), dict) else {}
        return JSONResponse(
            content={
                "ok": True,
                "order_id": order_id,
                "supply_id": supply_id,
                "operation_id": operation_id,
                "errors": errors_obj,
                "ozon_response": create_resp,
            }
        )
    except Exception as e:
        logger.warning("api_supplies_cargoes_create: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/cargoes-create-status")
async def api_supplies_cargoes_create_status(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Статус операции установки грузомест + актуальный состав: /v2/cargoes/create/info -> /v1/cargoes/get."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    operation_id = str(body.get("operation_id") or "").strip()
    if not operation_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен operation_id"})
    supply_id = str(body.get("supply_id") or "").strip()
    if not supply_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен supply_id"})
    try:
        client = OzonAPIClient()
        data = await client.get_cargoes_create_info(operation_id)
        if data.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": data.get("_error") or "Ошибка Ozon API",
                    "status_code": data.get("status_code"),
                    "ozon_response": data.get("ozon_response"),
                },
            )
        result = data.get("result") if isinstance(data.get("result"), dict) else data
        status = str((result or {}).get("status") or "UNSPECIFIED")
        errors = (result or {}).get("errors")
        cargo_places: list[dict] = []
        cargo_get_debug = None
        # Ozon может длительное время отдавать STATUS_UNSPECIFIED/IN_PROGRESS,
        # даже когда грузоместа уже фактически созданы. Проверяем фактические
        # данные через /v1/cargoes/get и считаем это финальным SUCCESS.
        if status != "FAILED":
            cargo_get = await client.get_cargoes_by_supply_ids([supply_id])
            if cargo_get.get("_error"):
                cargo_get_debug = cargo_get.get("_error")
                logger.warning(
                    "api_supplies_cargoes_create_status: user={} operation_id={} supply_id={} cargoes/get error={}",
                    getattr(user, "username", "unknown"),
                    operation_id,
                    supply_id,
                    cargo_get_debug,
                )
            else:
                cargo_places = _parse_cargoes_from_get_response(cargo_get, None)
                # Подтягиваем фактический товарный состав грузомест только из Ozon по bundle_id.
                bundle_cache: dict[str, list[dict]] = {}
                for cp in cargo_places:
                    if not isinstance(cp, dict):
                        continue
                    bid = cp.get("bundle_id")
                    bkey = str(bid).strip() if bid is not None else ""
                    if not bkey:
                        cp["line_items"] = []
                        continue
                    if bkey not in bundle_cache:
                        bundle_cache[bkey] = await client.get_supply_order_bundle_items_all_pages(
                            bkey,
                            item_tags_calculation=None,
                        )
                    cp["line_items"] = list(bundle_cache[bkey] or [])
                if cargo_places and status in {"UNSPECIFIED", "STATUS_UNSPECIFIED", "IN_PROGRESS", ""}:
                    status = "SUCCESS"
        logger.info(
            "api_supplies_cargoes_create_status: user={} operation_id={} supply_id={} status={} cargo_places={}",
            getattr(user, "username", "unknown"),
            operation_id,
            supply_id,
            status,
            len(cargo_places),
        )
        return JSONResponse(
            content={
                "ok": True,
                "operation_id": operation_id,
                "supply_id": supply_id,
                "status": status,
                "errors": errors if isinstance(errors, dict) else {},
                "cargo_places": cargo_places,
                "cargo_get_debug": cargo_get_debug,
                "ozon_response": result,
            }
        )
    except Exception as e:
        logger.warning("api_supplies_cargoes_create_status: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/order-composition-verify")
async def api_supplies_order_composition_verify(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Сверка заявленного состава с фактическим составом заявки в Ozon по order_id."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})

    try:
        order_id = int(body.get("order_id") or 0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "order_id должен быть числом"})
    if order_id <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен order_id > 0"})

    planned_items_raw = body.get("planned_items")
    if not isinstance(planned_items_raw, list) or not planned_items_raw:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен непустой массив planned_items"})

    planned_by_sku: dict[int, int] = {}
    meta_by_sku: dict[int, dict] = {}
    for it in planned_items_raw:
        if not isinstance(it, dict):
            continue
        try:
            sku = int(it.get("sku") or 0)
            qty = int(it.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if sku <= 0 or qty <= 0:
            continue
        planned_by_sku[sku] = planned_by_sku.get(sku, 0) + qty
        if sku not in meta_by_sku:
            meta_by_sku[sku] = {
                "article": str(it.get("article") or "").strip(),
                "name": str(it.get("name") or "").strip(),
            }
    if not planned_by_sku:
        return JSONResponse(status_code=400, content={"ok": False, "error": "В planned_items нет валидных sku/quantity"})

    try:
        client = OzonAPIClient()
        info_data = await client.get_supply_info(str(order_id))
        if info_data.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": info_data.get("_error") or "Ошибка получения заявки",
                    "ozon_response": info_data.get("ozon_response"),
                },
            )
        bundle_ids = _extract_bundle_ids_from_supply_order_response(info_data)
        if not bundle_ids:
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": "Не удалось получить bundle_id из заявки",
                    "ozon_response": info_data,
                },
            )

        actual_by_sku: dict[int, int] = {}
        for bid in bundle_ids:
            items = await client.get_supply_order_bundle_items_all_pages(str(bid))
            for it in items or []:
                if not isinstance(it, dict):
                    continue
                try:
                    sku = int(it.get("sku") or 0)
                    qty = int(it.get("quantity") or 0)
                except (TypeError, ValueError):
                    continue
                if sku <= 0 or qty < 0:
                    continue
                actual_by_sku[sku] = actual_by_sku.get(sku, 0) + qty

        all_skus = sorted(set(planned_by_sku.keys()) | set(actual_by_sku.keys()))
        mismatches: list[dict] = []
        for sku in all_skus:
            planned = int(planned_by_sku.get(sku, 0))
            actual = int(actual_by_sku.get(sku, 0))
            if planned != actual:
                md = meta_by_sku.get(sku) or {}
                mismatches.append(
                    {
                        "sku": sku,
                        "article": md.get("article") or "",
                        "name": md.get("name") or "",
                        "planned_quantity": planned,
                        "actual_quantity": actual,
                        "delta": actual - planned,
                    }
                )

        logger.info(
            "api_supplies_order_composition_verify: user={} order_id={} planned_skus={} actual_skus={} mismatches={}",
            getattr(user, "username", "unknown"),
            order_id,
            len(planned_by_sku),
            len(actual_by_sku),
            len(mismatches),
        )
        return JSONResponse(
            content={
                "ok": True,
                "order_id": order_id,
                "mismatch_found": bool(mismatches),
                "mismatches": mismatches,
                "planned_by_sku": planned_by_sku,
                "actual_by_sku": actual_by_sku,
            }
        )
    except Exception as e:
        logger.warning("api_supplies_order_composition_verify: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/order-content-update")
async def api_supplies_order_content_update(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Редактирование состава заявки по order_id через /v1/supply-order/content/update."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})

    try:
        order_id = int(body.get("order_id") or 0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "order_id должен быть числом"})
    if order_id <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен order_id > 0"})

    items_raw = body.get("items")
    if not isinstance(items_raw, list) or not items_raw:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен непустой массив items"})

    items: list[dict] = []
    for it in items_raw:
        if not isinstance(it, dict):
            continue
        try:
            sku = int(it.get("sku") or 0)
            qty = int(it.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if sku <= 0 or qty <= 0:
            continue
        # По документации верхняя граница количества на SKU: 1 000 000.
        if qty > 1_000_000:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Количество для sku {sku} > 1 000 000"})
        items.append({"sku": sku, "quantity": qty})
    if not items:
        return JSONResponse(status_code=400, content={"ok": False, "error": "В items нет валидных sku/quantity"})

    try:
        client = OzonAPIClient()
        info_data = await client.get_supply_info(str(order_id))
        if info_data.get("_error"):
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": info_data.get("_error") or "Ошибка получения заявки", "ozon_response": info_data.get("ozon_response")},
            )
        supply_id = _extract_supply_id_from_supply_order_get(info_data)
        if not supply_id:
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "Не удалось получить supply_id из /v3/supply-order/get", "ozon_response": info_data},
            )
        upd = await client.supply_order_content_update(order_id, supply_id, items)
        if upd.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": upd.get("_error") or "Ошибка редактирования состава",
                    "errors": upd.get("errors") if isinstance(upd.get("errors"), list) else [],
                    "ozon_response": upd.get("ozon_response"),
                },
            )
        operation_id = str(upd.get("operation_id") or "").strip()
        if not operation_id:
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "Ozon не вернул operation_id", "ozon_response": upd},
            )
        logger.info(
            "api_supplies_order_content_update: user={} order_id={} supply_id={} items_count={} operation_id={}",
            getattr(user, "username", "unknown"),
            order_id,
            supply_id,
            len(items),
            operation_id,
        )
        return JSONResponse(content={"ok": True, "order_id": order_id, "supply_id": supply_id, "operation_id": operation_id})
    except Exception as e:
        logger.warning("api_supplies_order_content_update: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/order-content-update-status")
async def api_supplies_order_content_update_status(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Статус редактирования состава заявки через /v1/supply-order/content/update/status."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    operation_id = str(body.get("operation_id") or "").strip()
    if not operation_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен operation_id"})
    try:
        client = OzonAPIClient()
        st = await client.supply_order_content_update_status(operation_id)
        if st.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": st.get("_error") or "Ошибка Ozon API",
                    "errors": st.get("errors") if isinstance(st.get("errors"), list) else [],
                    "ozon_response": st.get("ozon_response"),
                },
            )
        status = str(st.get("status") or "UNSPECIFIED").strip() or "UNSPECIFIED"
        errors = st.get("errors") if isinstance(st.get("errors"), list) else []
        new_bundle_id = st.get("new_bundle_id")
        logger.info(
            "api_supplies_order_content_update_status: user={} operation_id={} status={} errors={}",
            getattr(user, "username", "unknown"),
            operation_id,
            status,
            errors,
        )
        return JSONResponse(
            content={
                "ok": True,
                "operation_id": operation_id,
                "status": status,
                "errors": errors,
                "new_bundle_id": new_bundle_id,
            }
        )
    except Exception as e:
        logger.warning("api_supplies_order_content_update_status: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/order-content-update-validation")
async def api_supplies_order_content_update_validation(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Проверка товарного состава: POST /v1/supply-order/content/update/validation."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    try:
        supply_id = int(body.get("supply_id") or 0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "supply_id должен быть числом"})
    new_bundle_id = str(body.get("new_bundle_id") or "").strip()
    if supply_id <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен supply_id > 0"})
    if not new_bundle_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен new_bundle_id"})
    try:
        client = OzonAPIClient()
        val = await client.supply_order_content_update_validation(supply_id, new_bundle_id)
        if val.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": val.get("_error") or "Ошибка Ozon API",
                    "ozon_response": val.get("ozon_response"),
                },
            )
        logger.info(
            "api_supplies_order_content_update_validation: user={} supply_id={} new_bundle_id={} editing_errors={}",
            getattr(user, "username", "unknown"),
            supply_id,
            new_bundle_id[:40] + ("…" if len(new_bundle_id) > 40 else ""),
            val.get("editing_errors"),
        )
        return JSONResponse(content={"ok": True, "data": val})
    except Exception as e:
        logger.warning("api_supplies_order_content_update_validation: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/confirm")
async def api_supplies_confirm(
    request: Request,
    background_tasks: BackgroundTasks,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Подтвердить черновик (создать заявку на поставку), сохранить в БД. В фоне через 2 с запускается сверка состава по bundle_id."""
    started_at = time.perf_counter()
    flow_id = uuid.uuid4().hex[:10]
    flog = logger.bind(supply_confirm_flow_id=flow_id)
    try:
        body = await request.json()
    except Exception:
        flog.warning("supply_confirm: invalid json body")
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    draft_id = body.get("draft_id")
    from_in_timezone = (body.get("from_in_timezone") or "").strip()
    to_in_timezone = (body.get("to_in_timezone") or "").strip()
    selected_cluster_warehouses = body.get("selected_cluster_warehouses")
    composition = body.get("composition") or []
    flog.info(
        "supply_confirm: start user={} draft_id={} from={} to={} cluster_wh_count={} composition_count={}",
        getattr(user, "username", "unknown"),
        draft_id,
        from_in_timezone,
        to_in_timezone,
        len(selected_cluster_warehouses) if isinstance(selected_cluster_warehouses, list) else 0,
        len(composition) if isinstance(composition, list) else 0,
    )
    if not draft_id or not from_in_timezone or not to_in_timezone:
        flog.warning("supply_confirm: missing required fields draft_id={} from={} to={}", draft_id, from_in_timezone, to_in_timezone)
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужны draft_id, from_in_timezone, to_in_timezone"})
    if not selected_cluster_warehouses or not isinstance(selected_cluster_warehouses, list):
        flog.warning("supply_confirm: invalid selected_cluster_warehouses type={}", type(selected_cluster_warehouses).__name__)
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен selected_cluster_warehouses"})
    try:
        draft_id_int = int(draft_id)
    except (TypeError, ValueError):
        flog.warning("supply_confirm: draft_id not numeric draft_id={}", draft_id)
        return JSONResponse(status_code=400, content={"ok": False, "error": "draft_id должен быть числом"})
    activate_manual_supply_priority()
    if draft_id_int in _confirm_inflight_draft_ids:
        flog.warning("supply_confirm: duplicate inflight draft_id={}", draft_id_int)
        return JSONResponse(
            status_code=409,
            content={"ok": False, "error": "Подтверждение уже выполняется для этого draft_id. Подождите немного и повторите."},
        )
    _confirm_inflight_draft_ids.add(draft_id_int)
    attempts_trace = []
    try:
        client = OzonAPIClient()
        data = await client.confirm_draft_supply(
            draft_id=draft_id_int,
            from_in_timezone=from_in_timezone,
            to_in_timezone=to_in_timezone,
            selected_cluster_warehouses=selected_cluster_warehouses,
            supply_type="CROSSDOCK",
        )
        attempts_trace.extend([
            f"draft_supply_create: попытка {a.get('attempt')}/3 -> HTTP {a.get('status_code')}"
            for a in (data.get("attempts") or [])
            if isinstance(a, dict)
        ])
        flog.info(
            "supply_confirm: confirm_draft_supply done draft_id={} has_error={} attempts={}",
            draft_id_int,
            bool(data.get("_error")),
            len(data.get("attempts") or []) if isinstance(data, dict) else 0,
        )
        if data.get("_error"):
            flog.warning("supply_confirm: confirm api error draft_id={} error={}", draft_id_int, data.get("_error"))
            return JSONResponse(content={"ok": False, "error": data.get("_error") or "Ошибка API", "data": data, "attempts_trace": attempts_trace})
        supply_id = data.get("supply_id") or (data.get("result") or {}).get("supply_id") or data.get("id") or ""
        if isinstance(supply_id, (int, float)):
            supply_id = str(supply_id)
        flog.info("supply_confirm: parsed initial supply_id={} draft_id={}", supply_id, draft_id_int)
        # Важно: endpoint confirm должен отвечать быстро.
        # Финальный статус и order_id дополнительно подтверждаются на фронте через /api/supplies/draft-create-status.
        try:
            flog.info("supply_confirm: immediate status probe start draft_id={}", draft_id_int)
            status_data = await asyncio.wait_for(
                client.get_draft_supply_create_status(draft_id_int),
                timeout=6.0,
            )
            attempts_trace.extend([
                f"draft_supply_create_status: попытка {a.get('attempt')}/3 -> HTTP {a.get('status_code')}"
                for a in (status_data.get("attempts") or [])
                if isinstance(a, dict)
            ])
            if not status_data.get("_error"):
                order_id = status_data.get("order_id") or status_data.get("result", {}).get("order_id")
                if order_id is not None:
                    supply_id = str(order_id)
                st = status_data.get("status") or status_data.get("result", {}).get("status")
                flog.info(
                    "supply_confirm: immediate draft-create-status draft_id={} status={} order_id={}",
                    draft_id_int,
                    st,
                    order_id,
                )
                if st == "FAILED":
                    err_reasons = status_data.get("error_reasons") or status_data.get("result", {}).get("error_reasons") or []
                    flog.warning("supply_confirm: failed status draft_id={} reasons={}", draft_id_int, err_reasons)
                    return JSONResponse(content={
                        "ok": False,
                        "error": "Заявка не создана: " + ("; ".join(err_reasons) if err_reasons else st),
                        "data": status_data,
                        "attempts_trace": attempts_trace,
                    })
            flog.info("supply_confirm: immediate status probe done draft_id={}", draft_id_int)
        except asyncio.TimeoutError:
            # Не блокируем confirm дольше нескольких секунд.
            flog.warning("supply_confirm: immediate status probe timeout draft_id={} timeout_sec=6", draft_id_int)
        except Exception:
            # Не роняем confirm на промежуточной проверке статуса.
            flog.warning("supply_confirm: immediate status probe exception draft_id={}", draft_id_int, exc_info=True)
            pass
        shipment_date = from_in_timezone[:10] if len(from_in_timezone) >= 10 else ""
        delivery_estimated = to_in_timezone[:10] if len(to_in_timezone) >= 10 else (body.get("delivery_date_estimated") or "")
        row = OzonSupply(
            ozon_supply_id=supply_id,
            posting_number="",
            destination_warehouse="",
            shipment_date=shipment_date,
            timeslot_from=from_in_timezone or None,
            timeslot_to=to_in_timezone or None,
            delivery_date_estimated=delivery_estimated,
            composition=composition,
            status="created",
            draft_id=str(draft_id_int) if draft_id_int else None,
        )
        # Сохраняем выбранный кластер (macrolocal_cluster_id) — нужен для подсветки в таблице кластеров
        try:
            first_wh = selected_cluster_warehouses[0] if selected_cluster_warehouses else {}
            if isinstance(first_wh, dict):
                cid = first_wh.get("macrolocal_cluster_id") or first_wh.get("id")
                if cid is not None:
                    row.crossdock_cluster_id = int(cid)
        except Exception:
            pass
        db.add(row)
        await db.commit()
        await db.refresh(row)
        flog.info(
            "supply_confirm: db row saved row_id={} draft_id={} ozon_supply_id={} crossdock_cluster_id={}",
            row.id,
            row.draft_id,
            row.ozon_supply_id,
            row.crossdock_cluster_id,
        )
        order_id_for_check = (row.ozon_supply_id or supply_id or "").strip()
        if order_id_for_check and str(order_id_for_check).isdigit():
            background_tasks.add_task(_background_composition_check_after_confirm, row.id, order_id_for_check)
            flog.info("supply_confirm: background composition check queued row_id={} order_id={}", row.id, order_id_for_check)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        flog.info("supply_confirm: success row_id={} elapsed_ms={}", row.id, elapsed_ms)
        return JSONResponse(content={
            "ok": True,
            "supply_id": row.ozon_supply_id or supply_id,
            "posting_number": row.posting_number or "",
            "destination_warehouse": getattr(row, "destination_warehouse", None) or "",
            "id": row.id,
            "data": data,
            "attempts_trace": attempts_trace,
        })
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        flog.exception("supply_confirm: fatal error draft_id={} elapsed_ms={} error={}", draft_id, elapsed_ms, e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    finally:
        _confirm_inflight_draft_ids.discard(draft_id_int)


@router.post("/api/supplies/draft-create-status")
async def api_supplies_draft_create_status(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Проверка статуса создания заявки по draft_id через /v2/draft/supply/create/status."""
    started_at = time.perf_counter()
    flow_id = uuid.uuid4().hex[:10]
    flog = logger.bind(draft_create_status_flow_id=flow_id)
    try:
        body = await request.json()
    except Exception:
        flog.warning("draft_create_status: invalid json body")
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    draft_id = body.get("draft_id")
    flog.info("draft_create_status: start user={} draft_id={}", getattr(user, "username", "unknown"), draft_id)
    try:
        draft_id_int = int(draft_id)
    except (TypeError, ValueError):
        flog.warning("draft_create_status: draft_id not numeric draft_id={}", draft_id)
        return JSONResponse(status_code=400, content={"ok": False, "error": "draft_id должен быть числом"})
    if draft_id_int <= 0:
        flog.warning("draft_create_status: draft_id <= 0 draft_id={}", draft_id_int)
        return JSONResponse(status_code=400, content={"ok": False, "error": "draft_id должен быть > 0"})
    client = OzonAPIClient()
    try:
        status_data = await asyncio.wait_for(
            client.get_draft_supply_create_status(draft_id_int),
            timeout=8.0,
        )
    except asyncio.TimeoutError:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        flog.warning(
            "draft_create_status: timeout draft_id={} timeout_sec=8 elapsed_ms={}",
            draft_id_int,
            elapsed_ms,
        )
        # Не возвращаем 5xx, чтобы фронт мог корректно показать «ещё подтверждается».
        return JSONResponse(
            content={
                "ok": True,
                "draft_id": draft_id_int,
                "order_id": 0,
                "status": "PENDING",
                "error_reasons": [],
                "pending_reason": "timeout",
            }
        )
    if status_data.get("_error"):
        flog.warning("draft_create_status: ozon error draft_id={} error={}", draft_id_int, status_data.get("_error"))
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": status_data.get("_error") or "Ошибка проверки статуса",
                "detail": status_data.get("ozon_response") or status_data,
            },
        )
    order_id = status_data.get("order_id") or (status_data.get("result") or {}).get("order_id") or 0
    status_val = status_data.get("status") or (status_data.get("result") or {}).get("status") or "UNSPECIFIED"
    error_reasons = status_data.get("error_reasons") or (status_data.get("result") or {}).get("error_reasons") or []
    flog.info(
        "draft_create_status: ozon status draft_id={} status={} order_id={} error_reasons_count={}",
        draft_id_int,
        status_val,
        order_id,
        len(error_reasons) if isinstance(error_reasons, list) else 0,
    )
    # Синхронизируем order_id в нашей БД, чтобы фронт мог найти запись по /api/supplies/by-order/{order_id}.
    if order_id and str(order_id).isdigit():
        try:
            row_res = await db.execute(
                select(OzonSupply)
                .where(OzonSupply.draft_id == str(draft_id_int))
                .order_by(OzonSupply.id.desc())
            )
            row = row_res.scalars().first()
            if row:
                old_oid = row.ozon_supply_id
                row.ozon_supply_id = str(order_id)
                await db.commit()
                flog.info(
                    "draft_create_status: db sync order_id ok row_id={} draft_id={} old_ozon_supply_id={} new_ozon_supply_id={}",
                    row.id,
                    row.draft_id,
                    old_oid,
                    row.ozon_supply_id,
                )
            else:
                flog.warning("draft_create_status: db row by draft_id not found draft_id={}", draft_id_int)
        except Exception:
            await db.rollback()
            flog.warning("draft_create_status: db sync failed draft_id={} order_id={}", draft_id_int, order_id, exc_info=True)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    flog.info("draft_create_status: done draft_id={} elapsed_ms={}", draft_id_int, elapsed_ms)
    return JSONResponse(
        content={
            "ok": True,
            "draft_id": draft_id_int,
            "order_id": int(order_id or 0) if str(order_id).isdigit() else 0,
            "status": str(status_val or "UNSPECIFIED"),
            "error_reasons": error_reasons if isinstance(error_reasons, list) else [],
        }
    )


@router.get("/api/supplies/by-order/{order_id}")
async def api_supplies_find_by_order_id(
    order_id: str,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Найти id записи поставки в БД по order_id Ozon."""
    oid = str(order_id or "").strip()
    logger.info("supplies_by_order: request user={} order_id={}", getattr(user, "username", "unknown"), oid)
    if not oid:
        logger.warning("supplies_by_order: empty order_id")
        return JSONResponse(status_code=400, content={"ok": False, "error": "Некорректный order_id"})
    res = await db.execute(select(OzonSupply).where(OzonSupply.ozon_supply_id == oid).order_by(OzonSupply.id.desc()))
    row = res.scalars().first()
    if not row:
        logger.warning("supplies_by_order: not found order_id={}", oid)
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    logger.info("supplies_by_order: found order_id={} row_id={} draft_id={}", oid, row.id, row.draft_id)
    return JSONResponse(content={"ok": True, "id": int(row.id), "order_id": oid})


@router.delete("/api/supplies/{supply_id}")
async def api_supplies_delete(
    supply_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Отменить заявку в Ozon (POST /v1/supply-order/cancel), дождаться статуса SUCCESS, затем удалить строку из БД."""
    try:
        r = await db.execute(select(OzonSupply).where(OzonSupply.id == supply_id))
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(status_code=404, content={"detail": "Поставка не найдена"})
        order_id_str = (row.ozon_supply_id or "").strip()
        if not order_id_str:
            await db.delete(row)
            await db.commit()
            return JSONResponse(content={"ok": True})
        try:
            order_id = int(order_id_str)
        except (TypeError, ValueError):
            await db.delete(row)
            await db.commit()
            return JSONResponse(content={"ok": True})
        client = OzonAPIClient()
        cancel_resp = await client.cancel_supply_order(order_id)
        if cancel_resp.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": cancel_resp.get("_error") or "Ошибка отмены в Ozon",
                    "detail": cancel_resp.get("ozon_response"),
                },
            )
        operation_id = cancel_resp.get("operation_id") or (cancel_resp.get("result") or {}).get("operation_id")
        if not operation_id:
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": "Ozon не вернул operation_id для проверки отмены",
                    "detail": cancel_resp,
                },
            )
        for _ in range(30):
            await asyncio.sleep(2)
            status_resp = await client.get_supply_order_cancel_status(str(operation_id))
            if status_resp.get("_error"):
                continue
            st = status_resp.get("status") or (status_resp.get("result") or {}).get("status")
            result = status_resp.get("result") or {}
            if st == "SUCCESS" or result.get("is_order_cancelled"):
                await db.delete(row)
                await db.commit()
                return JSONResponse(content={"ok": True})
            if st == "ERROR":
                reasons = status_resp.get("error_reasons") or result.get("error_reasons") or []
                err_msg = "; ".join(reasons) if isinstance(reasons, list) else str(status_resp.get("message", "Ошибка отмены"))
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "Заявка не отменена в Ozon: " + err_msg},
                )
        return JSONResponse(
            status_code=504,
            content={"ok": False, "error": "Таймаут ожидания подтверждения отмены в Ozon"},
        )
    except Exception as e:
        logger.exception("api supplies delete: %s", e)
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/api/supply-queue/cancel-order")
async def api_supply_queue_cancel_order(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """
    Отмена заявки с экрана «Очередь поставок»: только DATA_FILLING и READY_TO_SUPPLY.
    POST /v1/supply-order/cancel, затем опрос POST /v1/supply-order/cancel/status.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Некорректное JSON-тело"})
    oid_raw = body.get("order_id")
    try:
        order_id = int(oid_raw)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен числовой order_id заявки"})
    if order_id < 1:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Некорректный order_id"})

    try:
        client = OzonAPIClient()
        info = await client.get_supply_info_many([str(order_id).zfill(8)])
        if info.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": "Не удалось проверить статус заявки в Ozon",
                    "detail": info.get("ozon_response"),
                },
            )
        orders = _extract_orders_from_supply_order_get_response(info)
        if not orders:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Заявка не найдена в Ozon"})
        st = str(orders[0].get("state") or "").strip().upper()
        if st not in ("DATA_FILLING", "READY_TO_SUPPLY"):
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error": "Отмена доступна только для статусов «Заполнение данных» и «Готова к отгрузке»",
                },
            )

        cancel_resp = await client.cancel_supply_order(order_id)
        if cancel_resp.get("_error"):
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "error": cancel_resp.get("_error") or "Ошибка отмены в Ozon",
                    "detail": cancel_resp.get("ozon_response"),
                },
            )
        operation_id = cancel_resp.get("operation_id") or (cancel_resp.get("result") or {}).get("operation_id")
        if not operation_id:
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "Ozon не вернул operation_id", "detail": cancel_resp},
            )

        for _ in range(30):
            await asyncio.sleep(2)
            status_resp = await client.get_supply_order_cancel_status(str(operation_id))
            if status_resp.get("_error"):
                continue
            st_cancel = status_resp.get("status")
            if isinstance(status_resp.get("result"), dict):
                st_cancel = st_cancel or (status_resp.get("result") or {}).get("status")
            result = status_resp.get("result") if isinstance(status_resp.get("result"), dict) else {}
            if st_cancel == "SUCCESS" or result.get("is_order_cancelled"):
                logger.info("api_supply_queue_cancel_order: SUCCESS order_id={}", order_id)
                return JSONResponse(content={"ok": True})
            if st_cancel == "ERROR":
                reasons = status_resp.get("error_reasons") or []
                if isinstance(reasons, list) and not reasons and isinstance(result, dict):
                    reasons = result.get("error_reasons") or []
                err_msg = "; ".join(reasons) if isinstance(reasons, list) and reasons else "Ошибка отмены"
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "Заявка не отменена: " + err_msg, "detail": status_resp},
                )
        return JSONResponse(
            status_code=504,
            content={"ok": False, "error": "Таймаут ожидания подтверждения отмены в Ozon"},
        )
    except Exception as e:
        logger.exception("api_supply_queue_cancel_order: {}", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/{supply_id}/refresh")
async def api_supplies_refresh(
    supply_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Обновить данные поставки: грузоместа и состав из ЛК, сверка нераспределённых; ЛК — точка правды.

    Описание общего сценария: docs/supply_queue_flow.md
    """
    try:
        r = await db.execute(select(OzonSupply).where(OzonSupply.id == supply_id))
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Поставка не найдена"})
        posting_number = (getattr(row, "posting_number", None) or row.ozon_supply_id or "").strip()
        order_id = (row.ozon_supply_id or "").strip()
        if not order_id or not order_id.isdigit():
            return JSONResponse(status_code=400, content={"ok": False, "error": "Нет идентификатора заявки (order_id) для API"})
        client = OzonAPIClient()
        supply_id_api = int(posting_number)
        errors = []
        composition_mismatch_after_refresh = None
        info_data = None
        # Кнопка "Синхронизировать состав" должна зависеть только от актуальной сверки состава.
        # Сбрасываем старое значение и заполняем его только после bundle-сверки с ЛК.
        await db.execute(
            text("UPDATE ozon_supplies SET composition_mismatch_message = NULL WHERE id = :sid"),
            {"sid": supply_id},
        )

        # Заявка из ЛК: статус и bundle_id для состава поставки
        if order_id and order_id.isdigit():
            info_data = await client.get_supply_info(order_id)
            if info_data.get("_error"):
                err_msg = _format_supply_order_get_error(info_data)
                errors.append("Статус: " + err_msg)
                await db.execute(
                    text("UPDATE ozon_supplies SET status_check_error = :err WHERE id = :sid"),
                    {"err": err_msg[:512], "sid": supply_id},
                )
        main_bundle_id = None
        declared_bundle_items = []  # состав поставки из ЛК (для сверки нераспределённых)
        if info_data and not info_data.get("_error"):
            orders = (info_data.get("result") or {}).get("orders") or info_data.get("orders") or []
            if isinstance(orders, list) and orders and isinstance(orders[0], dict):
                supplies = orders[0].get("supplies") or []
                if supplies and isinstance(supplies[0], dict):
                    main_bundle_id = supplies[0].get("bundle_id")
            if main_bundle_id:
                bundle_resp = await client.get_supply_order_bundle([str(main_bundle_id)])
                if not bundle_resp.get("_error") and isinstance(bundle_resp.get("items"), list):
                    for it in bundle_resp["items"]:
                        if not isinstance(it, dict):
                            continue
                        sku = it.get("sku")
                        qty = it.get("quantity") if it.get("quantity") is not None else it.get("quant")
                        if sku is not None:
                            declared_bundle_items.append({"sku": sku, "quantity": int(qty or 0)})

        # Грузоместа из ЛК: /v1/cargoes/get, затем состав по каждому bundle_id
        get_cargoes_resp = await client.get_cargoes(supply_id_api)
        if get_cargoes_resp.get("_error"):
            errors.append("Грузоместа: " + str(get_cargoes_resp.get("_error")))
        else:
            lk_cargoes = _parse_cargoes_from_get_response(get_cargoes_resp, None)
            # Привести SKU в items по offer_id/article из нашей БД (если Ozon вернул строковые идентификаторы)
            try:
                await _normalize_cargo_items_sku_by_article(db, lk_cargoes)
            except Exception:
                pass
            # Обогатить состав по грузоместам: если у грузоместа есть bundle_id и нет items — запросить /v1/supply-order/bundle
            for cargo in lk_cargoes:
                if not isinstance(cargo, dict):
                    continue
                bid = cargo.get("bundle_id")
                if not bid or not str(bid).strip():
                    continue
                if cargo.get("items"):
                    continue
                bundle_resp = await client.get_supply_order_bundle([str(bid).strip()])
                if bundle_resp.get("_error"):
                    continue
                items = []
                for it in (bundle_resp.get("items") or []):
                    if not isinstance(it, dict):
                        continue
                    sku = it.get("sku")
                    qty = it.get("quantity") if it.get("quantity") is not None else it.get("quant")
                    if sku is not None:
                        items.append({"sku": sku, "quantity": int(qty or 0)})
                cargo["items"] = items
            if lk_cargoes:
                table_cargoes = _current_cargo_places_from_row(row)
                if not _cargoes_list_equal(lk_cargoes, table_cargoes):
                    logger.info("api supplies refresh: состав грузомест от ЛК, обновляем таблицу supply_id={} lk_count={}", supply_id, len(lk_cargoes))
                    await db.execute(
                        text(
                            "UPDATE ozon_supplies SET cargo_places_data = :data, has_cargo_places = :has_any WHERE id = :sid"
                        ),
                        {"data": _json.dumps(lk_cargoes), "has_any": 1, "sid": supply_id},
                    )
                cargoes_list = lk_cargoes
            else:
                logger.warning("api supplies refresh: get_cargoes ok but parsed cargoes empty supply_id={}", supply_id)
                cargoes_list = _current_cargo_places_from_row(row)
            # Сверка нераспределённых: состав поставки из ЛК минус сумма по грузоместам
            declared_for_check = declared_bundle_items if declared_bundle_items else (list(row.composition) if isinstance(row.composition, list) else [])
            unallocated_msg = _compute_unallocated_remainder_message(declared_for_check, cargoes_list)
            if unallocated_msg:
                # Это отдельное состояние по грузоместам, не расхождение состава заявки vs ЛК.
                # Не используем его для composition_mismatch_message и кнопки синхронизации состава.
                pass
        # Обновить статус заявки из info_data (уже запрошено выше)
        if info_data and not info_data.get("_error"):
            result = info_data.get("result") or {}
            inner = result.get("result") or result
            items_raw = (
                inner.get("items") or result.get("items") or info_data.get("items")
                or info_data.get("orders") or []
            )
            if isinstance(items_raw, dict):
                items_raw = [items_raw]
            new_status = None
            for it in items_raw if isinstance(items_raw, list) else []:
                if not isinstance(it, dict):
                    continue
                st = (it.get("order_state") or it.get("state") or "").strip()
                if not st:
                    continue
                if st.startswith("ORDER_STATE_"):
                    new_status = st.replace("ORDER_STATE_", "")
                else:
                    new_status = st
                break
            if new_status:
                await db.execute(
                    text("UPDATE ozon_supplies SET status = :st, status_check_error = NULL WHERE id = :sid"),
                    {"st": new_status, "sid": supply_id},
                )
            bundle_ids = _extract_bundle_ids_from_supply_order_response(info_data)
            if bundle_ids:
                logger.info("api supplies refresh: сверка состава по bundle order_id=%s bundle_ids=%s", order_id, bundle_ids[:5])
                bundle_resp = await client.get_supply_order_bundle(bundle_ids)
                if bundle_resp.get("_error"):
                    logger.warning("api supplies refresh: bundle API ошибка, сверка пропущена supply_id=%s err=%s", supply_id, bundle_resp.get("_error"))
                else:
                    actual_by_sku = _parse_bundle_response_to_actual_by_sku(bundle_resp)
                    comp = list(row.composition) if isinstance(row.composition, list) else []
                    if not actual_by_sku and comp:
                        logger.warning("api supplies refresh: bundle ответ пустой или не распарсился supply_id=%s keys=%s", supply_id, list((bundle_resp or {}).keys())[:20])
                    mismatch_msg = _compute_composition_mismatch_from_actual(comp, actual_by_sku) if actual_by_sku or not comp else None
                    if mismatch_msg and not composition_mismatch_after_refresh:
                        logger.info("api supplies refresh: расхождение состава supply_id=%s msg=%s", supply_id, mismatch_msg[:200])
                        composition_mismatch_after_refresh = mismatch_msg
                        await db.execute(
                            text("UPDATE ozon_supplies SET composition_mismatch_message = :msg WHERE id = :sid"),
                            {"msg": mismatch_msg[:512], "sid": supply_id},
                        )
        await db.commit()

        # Снимок для AJAX-обновления строки в UI
        try:
            await db.refresh(row)
        except Exception:
            pass
        resp_supply = {
            "id": row.id,
            "ozon_supply_id": row.ozon_supply_id or "",
            "posting_number": getattr(row, "posting_number", None) or "",
            "destination_warehouse": getattr(row, "destination_warehouse", None) or "",
            "shipment_date": row.shipment_date or "",
            "timeslot_from": getattr(row, "timeslot_from", None) or "",
            "timeslot_to": getattr(row, "timeslot_to", None) or "",
            "status": _normalize_supply_status_for_ui(row.status or "created"),
            "status_label": SUPPLY_STATUS_LABELS.get(
                _normalize_supply_status_for_ui(row.status or "created"),
                _normalize_supply_status_for_ui(row.status or "created"),
            ),
            "has_cargo_places": bool(getattr(row, "has_cargo_places", 0)),
            "cargo_places_status": getattr(row, "cargo_places_status", None) or "",
            "cargo_places_status_label": CARGO_PLACES_STATUS_LABELS.get(getattr(row, "cargo_places_status", None) or "", "—"),
            "status_check_error": getattr(row, "status_check_error", None) or "",
            "composition_mismatch_message": getattr(row, "composition_mismatch_message", None) or "",
        }

        resp_content = {"ok": True, "updated": True, "supply": resp_supply}
        if errors:
            resp_content["warnings"] = errors
        if composition_mismatch_after_refresh:
            resp_content["composition_mismatch_message"] = composition_mismatch_after_refresh
        return JSONResponse(content=resp_content)
    except Exception as e:
        logger.warning("api supplies refresh: %s", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/{supply_id}/content-update")
async def api_supplies_content_update(
    supply_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Скорректировать товарный состав поставки под то, что оформилось в ЛК Озон.
    Получает фактический состав через /v1/supply-order/bundle, отправляет его в /v1/supply-order/content/update
    (полная замена состава), опрашивает статус; при SUCCESS обновляет composition и сбрасывает предупреждение.
    """
    try:
        r = await db.execute(select(OzonSupply).where(OzonSupply.id == supply_id))
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Поставка не найдена"})
        order_id_str = (row.ozon_supply_id or "").strip()
        posting_number = (getattr(row, "posting_number", None) or row.ozon_supply_id or "").strip()
        if not order_id_str or not order_id_str.isdigit():
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Нет order_id заявки для API"},
            )
        if not posting_number or not posting_number.isdigit():
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Нет идентификатора поставки (posting_number) для API"},
            )
        order_id = int(order_id_str)
        supply_id_ozon = int(posting_number)
        client = OzonAPIClient()
        info_data = await client.get_supply_info(order_id_str)
        if info_data.get("_error"):
            err = info_data.get("ozon_response") or info_data.get("_error")
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": f"Ошибка получения заявки: {err}"},
            )
        bundle_ids = _extract_bundle_ids_from_supply_order_response(info_data)
        if not bundle_ids:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Не удалось получить bundle_id из заявки"},
            )
        bundle_resp = await client.get_supply_order_bundle(bundle_ids)
        if bundle_resp.get("_error"):
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": f"Ошибка получения состава: {bundle_resp.get('_error')}"},
            )
        actual_by_sku = _parse_bundle_response_to_actual_by_sku(bundle_resp)
        items = [{"sku": sku, "quantity": qty} for sku, qty in actual_by_sku.items() if qty > 0]
        if not items:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "В ЛК нет товаров с количеством > 0 для подстановки"},
            )
        update_resp = await client.supply_order_content_update(order_id, supply_id_ozon, items)
        if update_resp.get("_error"):
            err_msg = update_resp.get("_error")
            ozon_errors = update_resp.get("errors") or []
            if ozon_errors:
                err_msg = "; ".join(str(e) for e in ozon_errors[:5])
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": f"Ошибка обновления состава: {err_msg}"},
            )
        operation_id = update_resp.get("operation_id")
        if not operation_id:
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "API не вернул operation_id"},
            )
        for _ in range(30):
            await asyncio.sleep(2)
            status_resp = await client.supply_order_content_update_status(str(operation_id))
            if status_resp.get("_error"):
                return JSONResponse(
                    status_code=502,
                    content={"ok": False, "error": f"Ошибка проверки статуса: {status_resp.get('_error')}"},
                )
            st = (status_resp.get("status") or "").strip()
            if st == "SUCCESS":
                old_comp = list(row.composition) if isinstance(row.composition, list) else []
                old_by_sku = {}
                for c in old_comp:
                    if isinstance(c, dict) and c.get("sku") is not None:
                        try:
                            old_by_sku[int(c.get("sku"))] = c
                        except (TypeError, ValueError):
                            pass
                new_composition = []
                for sku, qty in actual_by_sku.items():
                    if qty <= 0:
                        continue
                    prev = old_by_sku.get(sku) or {}
                    new_composition.append({
                        "sku": sku,
                        "quantity": qty,
                        "product_name": prev.get("product_name") or "",
                        "product_id": prev.get("product_id"),
                    })
                await db.execute(
                    text(
                        "UPDATE ozon_supplies SET composition = :comp, composition_mismatch_message = NULL WHERE id = :sid"
                    ),
                    {"comp": _json.dumps(new_composition), "sid": supply_id},
                )
                await db.commit()
                # Повторно запрашиваем данные по заявке через bundle_id и обновляем сверку состава
                try:
                    info_data_again = await client.get_supply_info(order_id_str)
                    if not info_data_again.get("_error"):
                        bundle_ids_again = _extract_bundle_ids_from_supply_order_response(info_data_again)
                        if bundle_ids_again:
                            bundle_resp_again = await client.get_supply_order_bundle(bundle_ids_again)
                            if not bundle_resp_again.get("_error"):
                                actual_again = _parse_bundle_response_to_actual_by_sku(bundle_resp_again)
                                mismatch_msg = _compute_composition_mismatch_from_actual(new_composition, actual_again) if actual_again else None
                                await db.execute(
                                    text("UPDATE ozon_supplies SET composition_mismatch_message = :msg WHERE id = :sid"),
                                    {"msg": (mismatch_msg[:512] if mismatch_msg else None), "sid": supply_id},
                                )
                                await db.commit()
                except Exception as sub_e:
                    logger.warning("api supplies content-update: повторная сверка по bundle: %s", sub_e)
                return JSONResponse(content={"ok": True, "message": "Состав скорректирован по данным ЛК Озон"})
            if st == "ERROR":
                ozon_errors = status_resp.get("errors") or []
                err_msg = "; ".join(str(e) for e in ozon_errors[:5]) if ozon_errors else "Ошибка изменения состава"
                return JSONResponse(status_code=502, content={"ok": False, "error": err_msg})
        return JSONResponse(
            status_code=504,
            content={"ok": False, "error": "Превышено время ожидания статуса обновления состава"},
        )
    except Exception as e:
        logger.warning("api supplies content-update: %s", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.get("/api/supplies/{supply_id}/cargo-places")
async def api_supplies_cargo_places_get(
    supply_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Вернуть сохранённый состав грузомест для поставки (для редактирования и удаления)."""
    try:
        r = await db.execute(select(OzonSupply).where(OzonSupply.id == supply_id))
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Поставка не найдена"})
        data = getattr(row, "cargo_places_data", None)
        if isinstance(data, str):
            try:
                data = _json.loads(data) if data.strip() else []
            except Exception:
                data = []
        if not isinstance(data, list):
            data = []
        return JSONResponse(content={"ok": True, "cargo_places": data})
    except Exception as e:
        logger.warning("api supplies cargo-places get: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/api/supplies/{supply_id}/cargo-places/delete")
async def api_supplies_cargo_places_delete(
    supply_id: int,
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Удаление грузомест в заявке на поставку.
    POST /v1/cargoes/delete + опрос /v1/cargoes/delete/status.
    Тело: {"cargo_ids": [int64, ...]} (если не передано — берём из сохранённого cargo_places_data).
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    cargo_ids = body.get("cargo_ids")
    try:
        r = await db.execute(select(OzonSupply).where(OzonSupply.id == supply_id))
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Поставка не найдена"})
        posting_number = (getattr(row, "posting_number", None) or row.ozon_supply_id or "").strip()
        if not posting_number or not posting_number.isdigit():
            return JSONResponse(status_code=400, content={"ok": False, "error": "Нет идентификатора поставки (posting_number) для API"})
        supply_id_api = int(posting_number)
        if not cargo_ids or not isinstance(cargo_ids, list):
            # Берём cargo_ids из сохранённого состава грузомест
            data = getattr(row, "cargo_places_data", None) or []
            if isinstance(data, str):
                try:
                    data = _json.loads(data) if data.strip() else []
                except Exception:
                    data = []
            if not isinstance(data, list):
                data = []
            cargo_ids = []
            for c in data:
                if not isinstance(c, dict):
                    continue
                cid = c.get("cargo_id")
                if cid is None:
                    val = c.get("value")
                    if isinstance(val, dict):
                        cid = val.get("cargo_id") or val.get("id")
                if cid is not None:
                    try:
                        cargo_ids.append(int(cid))
                    except (TypeError, ValueError):
                        pass
            if not cargo_ids and data:
                logger.warning(
                    "cargo-places delete: supply_id=%s has cargo_places_data len=%s but no cargo_id in items; keys of first=%s",
                    supply_id, len(data), list(data[0].keys()) if data and isinstance(data[0], dict) else None,
                )
            if not cargo_ids:
                return JSONResponse(status_code=400, content={"ok": False, "error": "Нет сохранённых грузомест для удаления. Укажите cargo_ids в теле запроса или сначала добавьте грузоместа."})
    except Exception as e:
        logger.warning("api supplies cargo-places delete load: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

    client = OzonAPIClient()
    delete_resp = await client.delete_cargoes(supply_id_api, cargo_ids)
    if delete_resp.get("_error"):
        err_detail = delete_resp.get("ozon_response")
        if isinstance(err_detail, dict):
            details = err_detail.get("details") or err_detail.get("errors") or []
            err_parts = [d.get("message") or d.get("error_message") or str(d) for d in details if isinstance(d, dict)]
            err_msg = "; ".join(err_parts) if err_parts else err_detail.get("message") or str(err_detail)
        else:
            err_msg = str(err_detail)
        status = 502 if delete_resp.get("status_code") != 400 else 400
        return JSONResponse(
            status_code=status,
            content={"ok": False, "error": delete_resp.get("_error"), "detail": err_msg or delete_resp},
        )

    # Подтверждение удаления только через /v1/cargoes/delete/status со статусом SUCCESS (в т.ч. при удалении последнего грузоместа)
    result = delete_resp.get("result")
    operation_id = delete_resp.get("operation_id") or (result.get("operation_id") if isinstance(result, dict) else None)
    cargo_ids_removed = set(int(cid) for cid in (cargo_ids or []) if cid is not None)

    if operation_id:
        # Соответствие кодов ошибок Ozon (cargoes/delete/status) человекочитаемым сообщениям
        _CARGO_DELETE_ERROR_LABELS = {
            "CARGO_NOT_FOUND": "Грузоместо не найдено",
            "SUPPLY_NOT_FOUND": "Поставка не найдена",
            "CANT_DELETE_ALL_CARGOES": "Нельзя удалять все грузоместа",
            "SUPPLY_DOES_NOT_BELONG_TO_THE_CONTRACTOR": "Поставка не принадлежит вашему юридическому лицу",
            "SUPPLY_DOES_NOT_BELONG_TO_THE_COMPANY": "Поставка не принадлежит вашему кабинету",
            "SUPPLY_CARGOES_IS_FINALIZED": "Грузоместа поставки нельзя редактировать",
            "SUPPLY_CARGOES_LOCKED": "Другой процесс блокирует редактирование грузомест поставки",
            "OPERATION_NOT_FOUND": "Операция не найдена",
        }

        def _format_cargo_delete_errors(errors_obj):
            if not errors_obj or not isinstance(errors_obj, dict):
                return []
            parts = []
            for cargo_item in (errors_obj.get("cargo_error_reasons") or []):
                if not isinstance(cargo_item, dict):
                    continue
                cid = cargo_item.get("cargo_id", "?")
                reasons = cargo_item.get("error_reasons") or []
                for r in reasons:
                    parts.append("Грузоместо %s: %s" % (cid, _CARGO_DELETE_ERROR_LABELS.get(r, r)))
            for code in (errors_obj.get("supply_error_reasons") or []):
                parts.append(_CARGO_DELETE_ERROR_LABELS.get(code, code))
            return parts

        # Ждём подтверждения удаления в Ozon (v1/cargoes/delete/status) — только после SUCCESS обновляем локальные данные
        final_status = ""
        final_errors_text = ""
        for _ in range(10):
            info_resp = await client.get_cargoes_delete_status(str(operation_id))
            if info_resp.get("_error"):
                logger.warning(
                    "api supplies cargo-places delete: get_cargoes_delete_status error="
                    + str(info_resp.get("_error"))
                )
                final_errors_text = str(info_resp.get("ozon_response", info_resp.get("_error")))
                break
            status_val = (info_resp.get("status") or (info_resp.get("result") or {}).get("status") or "").strip()
            final_status = status_val
            if status_val == "SUCCESS":
                break
            if status_val in ("ERROR", "FAILED"):
                err_parts = _format_cargo_delete_errors(info_resp.get("errors"))
                final_errors_text = "; ".join(err_parts) if err_parts else (status_val or "ERROR")
                break
            if status_val == "IN_PROGRESS":
                await asyncio.sleep(2)
                continue
            await asyncio.sleep(2)

        if final_status != "SUCCESS":
            detail = final_errors_text or final_status or "UNKNOWN"
            logger.warning(
                "api supplies cargo-places delete: supply_id=%s 502 status=%s detail=%s",
                supply_id, final_status, detail,
            )
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "Не удалось подтвердить удаление грузомест в Ozon", "detail": detail},
            )
    else:
        logger.info(
            "api supplies cargo-places delete: supply_id=%s no operation_id in response, refreshing composition from get_cargoes",
            supply_id,
        )

    # После SUCCESS (или при отсутствии operation_id) запрашиваем актуальный состав через /v1/cargoes/get и обновляем Состав (если грузомест нет — пустой список; в шаблоне выводится состав поставки без группировки по грузоместам)
    def _get_cargo_id(entry):
        if not isinstance(entry, dict):
            return None
        cid = entry.get("cargo_id")
        if cid is not None:
            return int(cid)
        val = entry.get("value")
        if isinstance(val, dict):
            cid = val.get("cargo_id") or val.get("id")
            return int(cid) if cid is not None else None
        return None

    def _local_cargo_list_minus_removed(data, cargo_ids_removed):
        if not isinstance(data, list):
            return []
        return [c for c in data if _get_cargo_id(c) not in cargo_ids_removed]

    async def _load_local_cargo_data(row, db, supply_id):
        data = getattr(row, "cargo_places_data", None) or []
        if isinstance(data, str):
            try:
                data = _json.loads(data) if (data or "").strip() else []
            except Exception:
                data = []
        if isinstance(data, list) and len(data) > 0:
            return data
        try:
            r_cargo = await db.execute(text("SELECT cargo_places_data FROM ozon_supplies WHERE id = :sid"), {"sid": supply_id})
            row_cargo = r_cargo.first()
            if row_cargo and row_cargo[0] is not None:
                raw_str = row_cargo[0]
                if isinstance(raw_str, bytes):
                    raw_str = raw_str.decode("utf-8", errors="replace")
                if isinstance(raw_str, str) and (raw_str or "").strip():
                    data = _json.loads(raw_str)
                    return data if isinstance(data, list) else []
        except Exception as parse_err:
            logger.warning("cargo-places delete: raw read cargo_places_data: %s", parse_err)
        return []

    try:
        new_list = []
        get_cargoes_resp = await client.get_cargoes(supply_id_api)
        if get_cargoes_resp.get("_error"):
            logger.warning("cargo-places delete: get_cargoes after delete failed: %s", get_cargoes_resp.get("_error"))
            local_data = await _load_local_cargo_data(row, db, supply_id)
            new_list = _local_cargo_list_minus_removed(local_data, cargo_ids_removed)
        else:
            new_list = _parse_cargoes_from_get_response(get_cargoes_resp, None)
            if not new_list:
                # Парсер вернул пустой список — структура ответа Ozon могла быть другой; не затираем все грузоместа, обновляем локально (убираем только удалённые)
                local_data = await _load_local_cargo_data(row, db, supply_id)
                new_list = _local_cargo_list_minus_removed(local_data, cargo_ids_removed)
                logger.info(
                    "cargo-places delete: get_cargoes ok but parsed empty, using local minus removed supply_id=%s remaining=%s",
                    supply_id, len(new_list),
                )

        # Состав поставки и нераспределённые: всего в composition минус в грузоместах
        composition = list(row.composition) if isinstance(getattr(row, "composition", None), list) else []
        total_in_composition = sum(int(c.get("quantity") or 0) for c in composition if isinstance(c, dict))
        allocated = 0
        for place in new_list:
            items = place.get("items") or (place.get("value") or {}).get("items") or []
            for it in items:
                if isinstance(it, dict):
                    allocated += int(it.get("quantity") or 0)
        unallocated = max(0, total_in_composition - allocated)
        notification_message = ("Товаров %s не распределено по грузоместам!" % unallocated) if unallocated > 0 else None

        has_any = 1 if new_list else 0
        payload = _json.dumps(new_list) if new_list else "[]"
        await db.execute(
            text(
                "UPDATE ozon_supplies SET cargo_places_data = :data, has_cargo_places = :has_any, cargo_places_status = CASE WHEN :has_any = 0 THEN '' ELSE cargo_places_status END WHERE id = :sid"
            ),
            {"data": payload, "has_any": has_any, "sid": supply_id},
        )
        await db.commit()
    except Exception as e:
        logger.warning("api supplies cargo-places delete: update flags: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

    return JSONResponse(content={"ok": True, "message": notification_message} if notification_message else {"ok": True})


def _is_cargo_like(obj) -> bool:
    """Проверяет, похож ли объект на грузоместо (есть cargo_id или value.cargo_id)."""
    if not isinstance(obj, dict):
        return False
    if obj.get("cargo_id") is not None or obj.get("id") is not None:
        return True
    val = obj.get("value")
    return isinstance(val, dict) and (val.get("cargo_id") is not None or val.get("id") is not None)


def _find_cargoes_list_anywhere(obj, depth: int = 0) -> Optional[list]:
    """Рекурсивно ищет в ответе API список объектов, похожих на грузоместа (глубина до 5)."""
    if depth > 5:
        return None
    if isinstance(obj, list):
        if obj and any(isinstance(x, dict) and _is_cargo_like(x) for x in obj):
            return obj
        for item in obj:
            found = _find_cargoes_list_anywhere(item, depth + 1)
            if found:
                return found
        return None
    if isinstance(obj, dict):
        for k in ("cargoes", "items", "cargo_list"):
            if k in obj and isinstance(obj[k], list) and obj[k]:
                if any(isinstance(x, dict) and _is_cargo_like(x) for x in obj[k]):
                    return obj[k]
        for v in obj.values():
            found = _find_cargoes_list_anywhere(v, depth + 1)
            if found:
                return found
    return None


def _cargoes_list_equal(a: list, b: list) -> bool:
    """Сравнивает два списка грузомест: совпадают ли id и состав (sku, quantity) в каждом."""
    if len(a) != len(b):
        return False
    by_id = {}
    for place in b:
        if isinstance(place, dict):
            cid = place.get("cargo_id") or place.get("id")
            if cid is not None:
                by_id[int(cid) if cid is not None else cid] = place
    for place in a:
        if not isinstance(place, dict):
            return False
        cid = place.get("cargo_id") or place.get("id")
        if cid is None:
            return False
        cid = int(cid) if cid is not None else cid
        other = by_id.get(cid)
        if other is None:
            return False
        items_a = {(int(x.get("sku") or 0), int(x.get("quantity") or x.get("quant") or 0)) for x in (place.get("items") or []) if isinstance(x, dict) and x.get("sku") is not None}
        items_b = {(int(x.get("sku") or 0), int(x.get("quantity") or x.get("quant") or 0)) for x in (other.get("items") or (other.get("value") or {}).get("items") or []) if isinstance(x, dict) and x.get("sku") is not None}
        if items_a != items_b:
            return False
    return True


def _current_cargo_places_from_row(row) -> list:
    """Из строки БД (OzonSupply) извлекает текущий список грузомест cargo_places_data."""
    raw = getattr(row, "cargo_places_data", None)
    if raw is None:
        return []
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", errors="replace")
        except Exception:
            return []
    if isinstance(raw, str):
        try:
            raw = _json.loads(raw) if (raw or "").strip() else []
        except Exception:
            return []
    return [p for p in raw if isinstance(p, dict)] if isinstance(raw, list) else []


def _cargo_items_count(cargoes: list) -> int:
    if not isinstance(cargoes, list):
        return 0
    total = 0
    for place in cargoes:
        if not isinstance(place, dict):
            continue
        items = place.get("items") or (place.get("value") or {}).get("items") or []
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                try:
                    sku_i = int(it.get("sku") or 0)
                    qty_i = int(it.get("quantity") or it.get("quant") or 0)
                except (TypeError, ValueError):
                    continue
                if sku_i > 0 and qty_i > 0:
                    total += 1
    return total


def _merge_cargo_items_from_existing(existing: list, parsed: list) -> list:
    """
    Если ЛК вернул cargo_id без items, не затираем уже известное распределение.
    Берем items из existing по совпавшему cargo_id.
    """
    if not isinstance(parsed, list):
        return []
    if not isinstance(existing, list) or not existing:
        return parsed

    by_id: dict[int, dict] = {}
    for place in existing:
        if not isinstance(place, dict):
            continue
        cid = place.get("cargo_id") or place.get("id")
        try:
            cid_i = int(cid)
        except (TypeError, ValueError):
            continue
        by_id[cid_i] = place

    out: list = []
    for place in parsed:
        if not isinstance(place, dict):
            continue
        items = place.get("items") or []
        if isinstance(items, list) and len(items) > 0:
            out.append(place)
            continue
        cid = place.get("cargo_id") or place.get("id")
        try:
            cid_i = int(cid)
        except (TypeError, ValueError):
            cid_i = 0
        prev = by_id.get(cid_i)
        prev_items = (prev or {}).get("items") or ((prev or {}).get("value") or {}).get("items") or []
        if isinstance(prev_items, list) and prev_items:
            cp = dict(place)
            cp["items"] = [dict(x) for x in prev_items if isinstance(x, dict)]
            out.append(cp)
        else:
            out.append(place)
    return out


def _inject_composition_into_cargoes_if_empty(parsed: list, composition: list) -> list:
    """
    Fallback: если ЛК вернул cargo_id, но не вернул items ни в одном грузоместе,
    распределяем состав по всем грузоместам (best-effort), чтобы не сваливать
    всё в первое грузоместо.
    """
    if not isinstance(parsed, list) or not parsed:
        return parsed if isinstance(parsed, list) else []
    if _cargo_items_count(parsed) > 0:
        return parsed
    if not isinstance(composition, list) or not composition:
        return parsed

    comp_items: list[dict] = []
    for c in composition:
        if not isinstance(c, dict):
            continue
        try:
            sku_i = int(c.get("sku") or 0)
            qty_i = int(c.get("quantity") or c.get("quant") or 0)
        except (TypeError, ValueError):
            continue
        if sku_i > 0 and qty_i > 0:
            comp_items.append({"sku": sku_i, "quantity": qty_i})
    if not comp_items:
        return parsed

    out = [dict(p) if isinstance(p, dict) else p for p in parsed]
    place_indexes = [i for i, p in enumerate(out) if isinstance(p, dict)]
    if not place_indexes:
        return parsed

    places_count = len(place_indexes)
    for pi in place_indexes:
        cur = dict(out[pi])
        cur["items"] = []
        out[pi] = cur

    # Раскладываем по каждому SKU равномерно по местам.
    for ci in comp_items:
        sku_i = int(ci.get("sku") or 0)
        qty_i = int(ci.get("quantity") or 0)
        if sku_i <= 0 or qty_i <= 0:
            continue
        base = qty_i // places_count
        rem = qty_i % places_count
        for idx, pi in enumerate(place_indexes):
            take = base + (1 if idx < rem else 0)
            if take <= 0:
                continue
            cur = dict(out[pi])
            items = cur.get("items") if isinstance(cur.get("items"), list) else []
            items = list(items)
            items.append({"sku": sku_i, "quantity": int(take)})
            cur["items"] = items
            out[pi] = cur
    return out


def _parse_cargoes_from_get_response(get_resp: dict, our_items_by_index: Optional[list] = None) -> list:
    """
    Парсит ответ /v1/cargoes/get в список { cargo_id, key, type, items: [{ sku, quantity }] }.
    Документация: запрос supply_ids[], ответ — result с cargoes (массив грузомест) или result — массив по supply.
    Каждое грузоместо: id/cargo_id, key, value: { type, items: [{ sku, quantity/barcode }] } или без value.
    """
    cargoes_raw = []
    result = get_resp.get("result")
    # Верхний уровень (ответ может содержать cargoes в корне)
    if isinstance(get_resp.get("cargoes"), list):
        cargoes_raw = get_resp["cargoes"]
    elif isinstance(get_resp.get("supply"), list) and get_resp["supply"]:
        # Некоторые ответы могут быть вида {"supply":[{"cargoes":[...]}]}
        for sup in get_resp["supply"]:
            if isinstance(sup, dict):
                part = sup.get("cargoes") or sup.get("items") or []
                if isinstance(part, list):
                    cargoes_raw.extend(part)
                elif isinstance(part, dict):
                    cargoes_raw.append(part)
    elif isinstance(result, list) and result:
        first = result[0] if result else None
        if isinstance(first, dict) and _is_cargo_like(first):
            cargoes_raw = result
        else:
            for elem in result:
                if not isinstance(elem, dict):
                    continue
                part = elem.get("cargoes") or elem.get("items") or []
                if isinstance(part, list):
                    cargoes_raw.extend(part)
                elif isinstance(part, dict):
                    cargoes_raw.append(part)
    elif isinstance(result, dict):
        cargoes_raw = result.get("cargoes") or result.get("items") or []
        # Часто cargoes приходят внутри result.supply (singular) или result.supplies (plural)
        if not cargoes_raw and isinstance(result.get("supply"), list) and result["supply"]:
            for sup in result["supply"]:
                if isinstance(sup, dict):
                    part = sup.get("cargoes") or sup.get("items") or []
                    if isinstance(part, list):
                        cargoes_raw.extend(part)
                    elif isinstance(part, dict):
                        cargoes_raw.append(part)
        if not cargoes_raw and isinstance(result.get("supplies"), list) and result["supplies"]:
            for sup in result["supplies"]:
                if isinstance(sup, dict):
                    part = sup.get("cargoes") or sup.get("items") or []
                    if isinstance(part, list):
                        cargoes_raw.extend(part)
                    elif isinstance(part, dict):
                        cargoes_raw.append(part)
        if not cargoes_raw:
            for _k, v in result.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    if _is_cargo_like(v[0]):
                        cargoes_raw = v
                        break
    if not cargoes_raw:
        found = _find_cargoes_list_anywhere(get_resp)
        if found:
            cargoes_raw = found
    if isinstance(cargoes_raw, dict):
        cargoes_raw = [cargoes_raw]
    if not isinstance(cargoes_raw, list):
        cargoes_raw = []
    out = []
    for i, api_cargo in enumerate(cargoes_raw):
        if not isinstance(api_cargo, dict):
            continue
        val = api_cargo.get("value") or {}
        cid = api_cargo.get("id") or api_cargo.get("cargo_id")
        if cid is None and isinstance(val, dict):
            cid = val.get("id") or val.get("cargo_id")
            if cid is None and isinstance(val.get("content"), dict):
                cid = val["content"].get("id") or val["content"].get("cargo_id")
        if cid is not None:
            try:
                cid = int(cid)
            except (TypeError, ValueError):
                cid = None
        ctype = (val.get("type") if isinstance(val, dict) else api_cargo.get("type")) or "BOX"
        key = api_cargo.get("key") or str(i + 1)
        bundle_id = api_cargo.get("bundle_id") or (val.get("bundle_id") if isinstance(val, dict) else None)
        if bundle_id is not None and not isinstance(bundle_id, str):
            bundle_id = str(bundle_id)
        api_items = (val.get("items", []) if isinstance(val, dict) else api_cargo.get("items")) or []
        if not api_items and isinstance(val, dict):
            content = val.get("content")
            if isinstance(content, dict):
                api_items = content.get("items") or content.get("products") or []
            elif isinstance(content, list):
                for c in content:
                    if isinstance(c, dict):
                        api_items = c.get("items") or c.get("products") or api_items
                        if api_items:
                            break
        api_items = api_items or (api_cargo.get("products") or [])
        if not isinstance(api_items, list):
            api_items = []
        items = []
        our_items = (our_items_by_index[i] if our_items_by_index and i < len(our_items_by_index) else []) or []
        if our_items:
            items = [{"sku": int(it.get("sku") or 0), "quantity": int(it.get("quantity") or it.get("quant") or 0)} for it in our_items if isinstance(it, dict)]
        if not items and api_items:
            for it in api_items:
                if not isinstance(it, dict):
                    continue
                qty = it.get("quantity") or it.get("quant")
                if qty is None:
                    continue
                try:
                    qty = int(qty)
                except (TypeError, ValueError):
                    continue
                sku_raw = it.get("sku") or it.get("sku_id")
                if sku_raw is None:
                    sku_raw = it.get("product_id") or it.get("barcode")
                offer_id = it.get("offer_id") or it.get("offerId") or it.get("article")
                barcode = it.get("barcode") or it.get("bar_code")
                sku = 0
                if sku_raw is not None:
                    try:
                        sku = int(sku_raw)
                    except (TypeError, ValueError):
                        sku = 0
                items.append({
                    "sku": sku,
                    "quantity": qty,
                    "offer_id": str(offer_id).strip() if offer_id is not None else "",
                    "barcode": str(barcode).strip() if barcode is not None else (str(sku_raw).strip() if sku == 0 and sku_raw is not None else ""),
                })
        out.append({
            "cargo_id": cid,
            "bundle_id": bundle_id,
            "key": str(key),
            "type": str(ctype) if ctype in ("BOX", "PALLET") else "BOX",
            "items": items,
        })
    if not out and not get_resp.get("_error"):
        try:
            preview = _json.dumps(get_resp, ensure_ascii=False, default=str)[:2500]
            logger.warning("_parse_cargoes_from_get_response: пустой результат, ответ ЛК (preview): {}", preview)
        except Exception:
            logger.warning("_parse_cargoes_from_get_response: пустой результат, keys={}", list(get_resp.keys()))
    return out


async def _normalize_cargo_items_sku_by_article(db: AsyncSession, cargoes: list) -> None:
    """
    Пытается привести items[].sku к реальному Ozon SKU (Product.ozon_sku),
    если в cargoes/get пришёл offer_id/article/barcode (строка) вместо sku.
    Меняет cargoes in-place.
    """
    if not isinstance(cargoes, list):
        return
    sku_ints: list[int] = []
    need_keys: list[str] = []
    for place in cargoes:
        if not isinstance(place, dict):
            continue
        for it in place.get("items") or []:
            if not isinstance(it, dict):
                continue
            key = (it.get("offer_id") or it.get("barcode") or "").strip()
            if key:
                need_keys.append(key)
            try:
                sku_i = int(it.get("sku") or 0)
            except (TypeError, ValueError):
                sku_i = 0
            if sku_i:
                sku_ints.append(sku_i)

    sku_ints = list(dict.fromkeys(sku_ints))[:500]
    need_keys = list(dict.fromkeys(need_keys))[:500]

    valid_ozon_skus: set[int] = set()
    if sku_ints:
        try:
            r = await db.execute(select(Product.ozon_sku).where(Product.ozon_sku.isnot(None), Product.ozon_sku.in_(sku_ints)))
            valid_ozon_skus = {int(x[0]) for x in r.all() if x and x[0] is not None}
        except Exception:
            valid_ozon_skus = set()

    # article в нашей БД соответствует offer_id в Ozon
    by_article: dict[str, int] = {}
    if need_keys:
        try:
            r = await db.execute(select(Product.article, Product.ozon_sku).where(Product.article.in_(need_keys)))
            rows = r.all()
        except Exception:
            rows = []
        by_article = {str(a).strip(): int(sku) for a, sku in rows if a and sku}

    if not by_article and not valid_ozon_skus:
        return

    for place in cargoes:
        if not isinstance(place, dict):
            continue
        for it in place.get("items") or []:
            if not isinstance(it, dict):
                continue
            try:
                sku_i = int(it.get("sku") or 0)
            except (TypeError, ValueError):
                sku_i = 0

            # Оставляем как есть, если SKU выглядит валидным Ozon SKU.
            if sku_i and valid_ozon_skus and (sku_i in valid_ozon_skus):
                continue

            key = (it.get("offer_id") or it.get("barcode") or "").strip()
            if not key:
                continue
            sku = by_article.get(key)
            if sku:
                it["sku"] = int(sku)


def _extract_bundle_ids_from_supply_order_response(data: dict) -> list:
    """
    Из ответа /v3/supply-order/get извлекает bundle_id для состава поставки.
    Документация: bundle_id в параметре ответа (orders.supplies или result.result.orders[].supplies[]).
    """
    out = []
    if not isinstance(data, dict):
        return out
    result = data.get("result") or {}
    inner = result.get("result") if isinstance(result, dict) else result
    orders = (inner or {}).get("orders") if isinstance(inner, dict) else data.get("orders") or []
    if not isinstance(orders, list):
        orders = [orders] if isinstance(orders, dict) else []
    for order in orders:
        if not isinstance(order, dict):
            continue
        supplies = order.get("supplies") or []
        if not isinstance(supplies, list):
            supplies = [supplies] if isinstance(supplies, dict) else []
        for sup in supplies:
            if not isinstance(sup, dict):
                continue
            bid = sup.get("bundle_id")
            if bid is not None and str(bid).strip():
                out.append(str(bid).strip())
    if not out and isinstance(result, dict):
        for key in ("bundles", "bundle_ids"):
            arr = result.get(key)
            if isinstance(arr, list):
                for b in arr:
                    if isinstance(b, dict) and b.get("bundle_id"):
                        out.append(str(b["bundle_id"]).strip())
                    elif isinstance(b, str) and b.strip():
                        out.append(b.strip())
                if out:
                    break
    return list(dict.fromkeys(out))


def _parse_bundle_response_to_actual_by_sku(bundle_resp: dict) -> Dict[int, int]:
    """
    Парсит ответ /v1/supply-order/bundle в словарь sku -> суммарное количество.
    Поддерживает: items[] (верхний уровень или result), result.bundles[], sku/sku_id/product_id, quantity/quant/quants.
    """
    actual: Dict[int, int] = {}
    if not isinstance(bundle_resp, dict) or bundle_resp.get("_error"):
        return actual

    def qty_from_item(it: dict) -> int:
        q = it.get("quantity") or it.get("quant")
        if q is not None:
            try:
                return int(q)
            except (TypeError, ValueError):
                pass
        quants = it.get("quants")
        if isinstance(quants, list):
            return sum(int(x) for x in quants if x is not None)
        if quants is not None:
            try:
                return int(quants)
            except (TypeError, ValueError):
                pass
        return 0

    def add_items_from_list(items_list):
        if not isinstance(items_list, list):
            return
        for it in items_list:
            if not isinstance(it, dict):
                continue
            sku = it.get("sku") or it.get("sku_id")
            if sku is None:
                sku = it.get("product_id") or it.get("barcode")
            if sku is None:
                continue
            try:
                sku_int = int(sku)
            except (TypeError, ValueError):
                continue
            qty = qty_from_item(it)
            actual[sku_int] = actual.get(sku_int, 0) + qty

    def collect_from_obj(obj, depth: int = 0):
        if depth > 5 or not isinstance(obj, dict):
            return
        add_items_from_list(obj.get("items"))
        add_items_from_list(obj.get("products"))
        content = obj.get("content")
        if isinstance(content, dict):
            collect_from_obj(content, depth + 1)
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict):
                    add_items_from_list(c.get("items") or c.get("products"))
                    collect_from_obj(c, depth + 1)
        for key in ("bundles", "result"):
            val = obj.get(key)
            if isinstance(val, list):
                for v in val:
                    if isinstance(v, dict):
                        add_items_from_list(v.get("items") or v.get("products"))
                        collect_from_obj(v, depth + 1)
            elif isinstance(val, dict):
                collect_from_obj(val, depth + 1)

    add_items_from_list(bundle_resp.get("items"))
    result = bundle_resp.get("result")
    if isinstance(result, dict):
        add_items_from_list(result.get("items"))
        collect_from_obj(result)
    return actual


def _compute_composition_mismatch_from_actual(
    declared_composition: list, actual_by_sku: Dict[int, int]
) -> Optional[str]:
    """Сравнивает заявленный состав с фактическим (из /v1/supply-order/bundle). Возвращает сообщение при расхождении."""
    if not isinstance(declared_composition, list):
        declared_composition = []
    declared_by_sku: Dict[int, int] = {}
    for c in declared_composition:
        if not isinstance(c, dict):
            continue
        sku = c.get("sku")
        if sku is None:
            continue
        try:
            sku_int = int(sku)
        except (TypeError, ValueError):
            continue
        qty = int(c.get("quantity") or 0)
        declared_by_sku[sku_int] = declared_by_sku.get(sku_int, 0) + qty
    all_skus = set(declared_by_sku) | set(actual_by_sku)
    if not all_skus:
        return None
    diffs = []
    for sku in sorted(all_skus):
        decl = declared_by_sku.get(sku, 0)
        act = actual_by_sku.get(sku, 0)
        if decl != act:
            diffs.append(f"SKU {sku}: заявлено {decl}, в ЛК Озон {act}")
    if not diffs:
        return None
    return "Товарный состав различается в заявке и в сформированной поставке (ЛК Озон). " + "; ".join(diffs[:5]) + ("…" if len(diffs) > 5 else "")


def _compute_unallocated_remainder_message(declared_composition: list, cargo_places_data: list) -> Optional[str]:
    """
    Считает нераспределённый остаток: заявлено в composition минус сумма по грузоместам.
    Возвращает сообщение вида «Нераспределенных товаров осталось: SKU 123 - 10 шт, SKU 456 - 13 шт» или None.
    """
    if not isinstance(declared_composition, list) or not isinstance(cargo_places_data, list):
        return None
    declared_by_sku: Dict[int, int] = {}
    for c in declared_composition:
        if not isinstance(c, dict):
            continue
        sku = c.get("sku")
        if sku is None:
            continue
        try:
            sku_int = int(sku)
        except (TypeError, ValueError):
            continue
        declared_by_sku[sku_int] = declared_by_sku.get(sku_int, 0) + int(c.get("quantity") or 0)
    in_cargo_by_sku: Dict[int, int] = {}
    for place in cargo_places_data:
        if not isinstance(place, dict):
            continue
        items = place.get("items") or (place.get("value") or {}).get("items") or place.get("products") or []
        for it in items:
            if not isinstance(it, dict):
                continue
            sku = it.get("sku") or it.get("sku_id")
            if sku is None:
                sku = it.get("product_id") or it.get("barcode")
            if sku is None:
                continue
            try:
                sku_int = int(sku)
            except (TypeError, ValueError):
                continue
            in_cargo_by_sku[sku_int] = in_cargo_by_sku.get(sku_int, 0) + int(it.get("quantity") or it.get("quant") or 0)
    unallocated = {}
    for sku, decl in declared_by_sku.items():
        in_cargo = in_cargo_by_sku.get(sku, 0)
        rest = decl - in_cargo
        if rest > 0:
            unallocated[sku] = rest
    if not unallocated:
        return None
    parts = [f"SKU {sku} - {qty} шт" for sku, qty in sorted(unallocated.items())]
    return "Нераспределенных товаров осталось: " + ", ".join(parts)


def _compute_composition_mismatch(declared_composition: list, cargo_places_data: list) -> Optional[str]:
    """
    Сравнивает заявленный состав (composition) с фактическим из грузомест (cargo_places_data).
    Возвращает сообщение о расхождении или None, если составы совпадают.
    """
    if not isinstance(declared_composition, list):
        declared_composition = []
    if not isinstance(cargo_places_data, list):
        return None
    declared_by_sku: Dict[int, int] = {}
    for c in declared_composition:
        if not isinstance(c, dict):
            continue
        sku = c.get("sku")
        if sku is None:
            continue
        try:
            sku_int = int(sku)
        except (TypeError, ValueError):
            continue
        qty = int(c.get("quantity") or 0)
        if sku_int not in declared_by_sku:
            declared_by_sku[sku_int] = 0
        declared_by_sku[sku_int] += qty
    actual_by_sku: Dict[int, int] = {}
    for place in cargo_places_data:
        if not isinstance(place, dict):
            continue
        items = place.get("items") or (place.get("value") or {}).get("items") or place.get("products") or []
        for it in items:
            if not isinstance(it, dict):
                continue
            sku = it.get("sku") or it.get("sku_id")
            if sku is None:
                sku = it.get("product_id") or it.get("barcode")
            if sku is None:
                continue
            try:
                sku_int = int(sku)
            except (TypeError, ValueError):
                continue
            qty = int(it.get("quantity") or it.get("quant") or 0)
            if sku_int not in actual_by_sku:
                actual_by_sku[sku_int] = 0
            actual_by_sku[sku_int] += qty
    all_skus = set(declared_by_sku) | set(actual_by_sku)
    if not all_skus:
        return None
    total_declared = sum(declared_by_sku.values())
    total_actual = sum(actual_by_sku.values())
    if total_actual == 0 and total_declared > 0 and len(cargo_places_data) > 0:
        return None
    diffs = []
    for sku in sorted(all_skus):
        decl = declared_by_sku.get(sku, 0)
        act = actual_by_sku.get(sku, 0)
        if decl != act:
            diffs.append(f"SKU {sku}: заявлено {decl}, в поставке {act}")
    if not diffs:
        return None
    prefix = "Не все товары распределены по грузоместам. " if total_actual < total_declared else ""
    return prefix + "Товарный состав различается в заявке и в сформированной поставке. " + "; ".join(diffs[:5]) + ("…" if len(diffs) > 5 else "")


async def _background_fill_composition_for_lk_supplies(supply_ids: list[int]) -> None:
    """
    Заполнить `composition` (и частично status/destination/shipment_date) для поставок,
    созданных в ЛК Ozon, чтобы они стали доступны для распределения по грузоместам в UI.
    """
    activate_manual_supply_priority()
    if not supply_ids:
        return

    # Ограничим объём, чтобы не устроить массовые 429/долгие цепочки запросов.
    try:
        supply_ids = list(dict.fromkeys(int(x) for x in supply_ids if x))[:15]
    except Exception:
        return

    client = OzonAPIClient()

    async with AsyncSessionLocal() as sess:
        r = await sess.execute(select(OzonSupply).where(OzonSupply.id.in_(supply_ids)))
        rows = r.scalars().all()
        rows_by_id = {int(row.id): row for row in rows if getattr(row, "id", None) is not None}

        for sid in supply_ids:
            row = rows_by_id.get(int(sid))
            if not row:
                continue

            try:
                comp = getattr(row, "composition", None)
                comp_is_empty = not isinstance(comp, list) or len(comp) == 0
                if not comp_is_empty:
                    continue

                order_id = (getattr(row, "ozon_supply_id", None) or "").strip()
                if not order_id or not order_id.isdigit():
                    continue

                info_data = await client.get_supply_info(order_id)
                if info_data.get("_error"):
                    err_msg = _format_supply_order_get_error(info_data)
                    row.status_check_error = err_msg[:512] if err_msg else "Ошибка get_supply_info"
                    await sess.commit()
                    logger.warning("lk composition fill: get_supply_info error sid={} err={}", sid, err_msg[:120])
                    continue

                # Статус (best-effort): как в /api/supplies/{id}/refresh
                result = info_data.get("result") or {}
                inner = result.get("result") or result if isinstance(result, dict) else result
                items_raw = (
                    inner.get("items") if isinstance(inner, dict) else None
                ) or (
                    result.get("items") if isinstance(result, dict) else None
                ) or (
                    info_data.get("items")
                ) or (
                    info_data.get("orders") if isinstance(info_data.get("orders"), list) else info_data.get("result", {}).get("orders")
                ) or []
                if isinstance(items_raw, dict):
                    items_raw = [items_raw]

                new_status = None
                if isinstance(items_raw, list):
                    for it in items_raw:
                        if not isinstance(it, dict):
                            continue
                        st = (it.get("order_state") or it.get("state") or "").strip()
                        if not st:
                            continue
                        if st.startswith("ORDER_STATE_"):
                            new_status = st.replace("ORDER_STATE_", "", 1)
                        else:
                            new_status = st
                        break

                # Состав: через bundle_id (v3/supply-order/get -> v1/supply-order/bundle)
                bundle_ids = _extract_bundle_ids_from_supply_order_response(info_data)
                if not bundle_ids:
                    continue

                bundle_resp = await client.get_supply_order_bundle([str(bundle_ids[0])])
                if bundle_resp.get("_error"):
                    continue

                actual_by_sku = _parse_bundle_response_to_actual_by_sku(bundle_resp)
                if not actual_by_sku:
                    continue

                skus = [int(k) for k in actual_by_sku.keys() if k is not None]
                if not skus:
                    continue

                r_pr = await sess.execute(select(Product).where(Product.ozon_sku.in_(skus)))
                products = r_pr.scalars().all()
                prod_by_sku = {p.ozon_sku: p for p in products if getattr(p, "ozon_sku", None) is not None}

                composition = []
                for sku in sorted(set(skus)):
                    qty = int(actual_by_sku.get(sku) or 0)
                    p = prod_by_sku.get(sku)
                    composition.append(
                        {
                            "sku": int(sku),
                            "quantity": qty,
                            "product_id": p.id if p else None,
                            "product_name": p.name if p else "",
                        }
                    )

                # destination/shipment_date/posting_number (best-effort из info_data)
                orders = (info_data.get("result") or {}).get("orders") or info_data.get("orders") or []
                if isinstance(orders, dict):
                    orders = [orders]
                dest = ""
                ship_date = ""
                pn_13 = ""
                if isinstance(orders, list) and orders and isinstance(orders[0], dict):
                    supplies = orders[0].get("supplies") or []
                    if isinstance(supplies, dict):
                        supplies = [supplies]
                    if isinstance(supplies, list) and supplies and isinstance(supplies[0], dict):
                        sp = supplies[0]
                        storage_wh = sp.get("storage_warehouse") or sp.get("drop_off_warehouse")
                        if isinstance(storage_wh, dict):
                            dest = (storage_wh.get("name") or "").strip()
                        ship_date = (
                            str(sp.get("shipment_date") or sp.get("shipmentDate") or "").strip()[:10]
                        )
                        pn_raw = sp.get("supply_id") or (sp.get("supply_ids")[0] if isinstance(sp.get("supply_ids"), list) and sp.get("supply_ids") else None)
                        pn_raw_s = str(pn_raw).strip() if pn_raw is not None else ""
                        if pn_raw_s.isdigit() and len(pn_raw_s) >= 13:
                            pn_13 = pn_raw_s[:13]

                # 1) Пытаемся подтянуть грузоместа из ЛК (/v1/cargoes/get) и сохранить их в БД.
                # Это нужно, чтобы в таблице сразу отрисовывались товары по грузоместам.
                supply_id_api = 0
                try:
                    if pn_13 and pn_13.isdigit():
                        supply_id_api = int(pn_13)
                    elif getattr(row, "posting_number", None) and str(row.posting_number).strip().isdigit():
                        supply_id_api = int(str(row.posting_number).strip())
                except Exception:
                    supply_id_api = 0

                if supply_id_api > 0:
                    get_cargoes_resp = await client.get_cargoes(supply_id_api)
                    if not get_cargoes_resp.get("_error"):
                        parsed_cargoes = _parse_cargoes_from_get_response(get_cargoes_resp, None)
                        if parsed_cargoes:
                            # Если sku пришли как article/offer_id — приводим к реальному Ozon SKU.
                            await _normalize_cargo_items_sku_by_article(sess, parsed_cargoes)
                            row.cargo_places_data = parsed_cargoes
                            row.has_cargo_places = 1
                            row.cargo_places_status = "SUCCESS"

                row.composition = composition
                row.status_check_error = None
                if new_status:
                    row.status = _normalize_supply_status_for_ui(new_status)
                if dest and (not getattr(row, "destination_warehouse", None) or not str(row.destination_warehouse).strip()):
                    row.destination_warehouse = dest
                if ship_date and (not getattr(row, "shipment_date", None) or not str(row.shipment_date).strip()):
                    row.shipment_date = ship_date
                if pn_13 and (not getattr(row, "posting_number", None) or not str(row.posting_number).strip()):
                    row.posting_number = pn_13

                await sess.commit()
                logger.info("lk composition fill: updated sid={} skus={}", sid, len(composition))
            except Exception as e:
                logger.warning("lk composition fill: sid={} exception={}", sid, e, exc_info=True)
                try:
                    await sess.commit()
                except Exception:
                    pass


async def _background_fill_cargo_places_for_lk_supplies(supply_ids: list[int]) -> None:
    """
    Заполнить `cargo_places_data` для поставок, созданных/подтянутых из ЛК Ozon.
    Делает v3/supply-order/get -> supply_id(13) -> v1/cargoes/get, сохраняет разложение по грузоместам.
    """
    activate_manual_supply_priority()
    if not supply_ids:
        return
    try:
        supply_ids = list(dict.fromkeys(int(x) for x in supply_ids if x))[:20]
    except Exception:
        return

    client = OzonAPIClient()
    immutable_statuses = {
        "ACCEPTED_AT_SUPPLY_WAREHOUSE",
        "IN_TRANSIT",
        "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
        "REPORTS_CONFIRMATION_AWAITING",
        "REPORT_REJECTED",
        "COMPLETED",
        "REJECTED_AT_SUPPLY_WAREHOUSE",
        "CANCELLED",
        "OVERDUE",
    }
    async with AsyncSessionLocal() as sess:
        r = await sess.execute(select(OzonSupply).where(OzonSupply.id.in_(supply_ids)))
        rows = r.scalars().all()
        rows_by_id = {int(row.id): row for row in rows if getattr(row, "id", None) is not None}

        for sid in supply_ids:
            row = rows_by_id.get(int(sid))
            if not row:
                continue
            try:
                existing_cargo = _current_cargo_places_from_row(row)
                status_ui = _normalize_supply_status_for_ui(getattr(row, "status", None) or "")
                # Для статусов после "принята на точке отгрузки" грузоместа больше
                # не меняются: если уже распределено — не дергаем ЛК повторно.
                if status_ui in immutable_statuses and existing_cargo:
                    unallocated_msg = _compute_unallocated_remainder_message(
                        getattr(row, "composition", None) or [],
                        existing_cargo,
                    )
                    if not unallocated_msg:
                        continue
                if isinstance(existing_cargo, list) and len(existing_cargo) > 0:
                    # Грузоместа уже есть, но иногда sku'ы внутри оказываются не Ozon SKU.
                    # Нормализуем и проверяем пересечение с composition.
                    await _normalize_cargo_items_sku_by_article(sess, existing_cargo)

                    comp_skus: set[int] = set()
                    if isinstance(getattr(row, "composition", None), list):
                        for c in row.composition:
                            if isinstance(c, dict):
                                try:
                                    sku_i = int(c.get("sku") or 0)
                                except (TypeError, ValueError):
                                    sku_i = 0
                                if sku_i:
                                    comp_skus.add(sku_i)

                    cargo_skus: set[int] = set()
                    for place in existing_cargo:
                        if not isinstance(place, dict):
                            continue
                        for it in place.get("items") or (place.get("value") or {}).get("items") or []:
                            if not isinstance(it, dict):
                                continue
                            try:
                                sku_i = int(it.get("sku") or 0)
                            except (TypeError, ValueError):
                                sku_i = 0
                            if sku_i:
                                cargo_skus.add(sku_i)

                    if not comp_skus or (comp_skus & cargo_skus):
                        # Для неизменяемых статусов и распределенного состава оставляем как есть.
                        if status_ui in immutable_statuses:
                            unallocated_msg = _compute_unallocated_remainder_message(
                                getattr(row, "composition", None) or [],
                                existing_cargo,
                            )
                            if not unallocated_msg:
                                row.cargo_places_data = existing_cargo
                                row.has_cargo_places = 1
                                row.cargo_places_status = "SUCCESS"
                                await sess.commit()
                                continue
                        row.cargo_places_data = existing_cargo
                        row.has_cargo_places = 1
                        row.cargo_places_status = "SUCCESS"
                        await sess.commit()
                        continue
                    # Если пересечения нет — скорее всего, cargoes из ЛК надо подтянуть заново.

                order_id = (getattr(row, "ozon_supply_id", None) or "").strip()
                posting_number = (getattr(row, "posting_number", None) or "").strip()
                supply_id_api = 0
                if posting_number.isdigit() and len(posting_number) >= 13:
                    supply_id_api = int(posting_number[:13])
                elif order_id.isdigit() and len(order_id) == 8:
                    info_data = await client.get_supply_info(order_id)
                    if info_data.get("_error"):
                        continue
                    result = info_data.get("result") or {}
                    inner = result.get("result") if isinstance(result, dict) else result
                    orders = (inner or {}).get("orders") or info_data.get("orders") or []
                    if isinstance(orders, dict):
                        orders = [orders]
                    if isinstance(orders, list) and orders and isinstance(orders[0], dict):
                        supplies = orders[0].get("supplies") or []
                        if isinstance(supplies, dict):
                            supplies = [supplies]
                        if isinstance(supplies, list) and supplies and isinstance(supplies[0], dict):
                            supply_raw = supplies[0].get("supply_id") or supplies[0].get("supplyId")
                            supply_s = str(supply_raw).strip() if supply_raw is not None else ""
                            if supply_s.isdigit() and len(supply_s) >= 13:
                                supply_id_api = int(supply_s[:13])

                if supply_id_api <= 0:
                    continue

                parsed: list = []
                last_err = ""
                # В ЛК грузоместа могут появляться с задержкой после создания/изменения поставки.
                for attempt in range(4):
                    cargo_resp = await client.get_cargoes(supply_id_api)
                    if cargo_resp.get("_error"):
                        last_err = str(cargo_resp.get("_error"))
                        if attempt < 3:
                            await asyncio.sleep(2)
                        continue
                    parsed = _parse_cargoes_from_get_response(cargo_resp, None)
                    if parsed:
                        break
                    if attempt < 3:
                        await asyncio.sleep(2)

                if not parsed:
                    logger.warning(
                        "lk cargo fill: empty cargoes sid={} order_id={} posting_number={} supply_id_api={} last_err={}",
                        sid,
                        order_id,
                        posting_number,
                        supply_id_api,
                        last_err,
                    )
                    continue
                await _normalize_cargo_items_sku_by_article(sess, parsed)
                parsed = _merge_cargo_items_from_existing(existing_cargo, parsed)
                parsed = _inject_composition_into_cargoes_if_empty(parsed, getattr(row, "composition", None) or [])

                row.cargo_places_data = parsed
                row.has_cargo_places = 1
                row.cargo_places_status = "SUCCESS"
                await sess.commit()
                items_now = _cargo_items_count(parsed)
                prev_rows = int(_supplies_sync_from_lk_state.get("cargo_rows_filled") or 0)
                prev_items = int(_supplies_sync_from_lk_state.get("cargo_items_total") or 0)
                _set_supplies_sync_progress(
                    cargo_rows_filled=prev_rows + 1,
                    cargo_items_total=prev_items + int(items_now or 0),
                    message=f"Грузоместа: обработано {prev_rows + 1}",
                )
                logger.info(
                    "lk cargo fill: updated sid={} cargoes={} items_total={}",
                    sid,
                    len(parsed),
                    items_now,
                )
            except Exception as e:
                logger.warning("lk cargo fill: sid={} exception={}", sid, e, exc_info=True)
                try:
                    await sess.commit()
                except Exception:
                    pass


async def _background_fill_cargo_places_for_recent_lk_supplies(days: int = 30, limit: int = 80) -> None:
    """
    Ретро-дозаполнение грузомест для уже существующих строк ozon_supplies за период,
    если они попали в БД ранее, но остались без cargo_places_data.
    """
    try:
        cutoff_msk = datetime.now(MSK) - timedelta(days=max(1, int(days)))
    except Exception:
        cutoff_msk = datetime.now(MSK) - timedelta(days=30)
    candidate_ids: list[int] = []
    async with AsyncSessionLocal() as sess:
        r = await sess.execute(select(OzonSupply).order_by(OzonSupply.created_at.desc()))
        rows = r.scalars().all()
        for row in rows:
            try:
                created = _ensure_datetime_msk(getattr(row, "created_at", None))
                if created is None or created < cutoff_msk:
                    continue
                has_cp = int(getattr(row, "has_cargo_places", 0) or 0) == 1
                cp_data = _current_cargo_places_from_row(row)
                if has_cp and cp_data:
                    comp_skus: set[int] = set()
                    if isinstance(getattr(row, "composition", None), list):
                        for c in row.composition:
                            if isinstance(c, dict):
                                try:
                                    sku_i = int(c.get("sku") or 0)
                                except (TypeError, ValueError):
                                    sku_i = 0
                                if sku_i:
                                    comp_skus.add(sku_i)

                    if not comp_skus:
                        continue

                    cargo_skus: set[int] = set()
                    for place in cp_data:
                        if not isinstance(place, dict):
                            continue
                        for it in place.get("items") or (place.get("value") or {}).get("items") or []:
                            if not isinstance(it, dict):
                                continue
                            try:
                                sku_i = int(it.get("sku") or 0)
                            except (TypeError, ValueError):
                                sku_i = 0
                            if sku_i:
                                cargo_skus.add(sku_i)

                    if comp_skus & cargo_skus:
                        continue
                oid = str(getattr(row, "ozon_supply_id", None) or "").strip()
                pn = str(getattr(row, "posting_number", None) or "").strip()
                if not ((oid.isdigit() and len(oid) == 8) or (pn.isdigit() and len(pn) >= 13)):
                    continue
                candidate_ids.append(int(row.id))
                if len(candidate_ids) >= max(1, int(limit)):
                    break
            except Exception:
                continue
    if candidate_ids:
        logger.info("lk cargo retrofill: candidates={}", len(candidate_ids))
        await _background_fill_cargo_places_for_lk_supplies(candidate_ids)


async def _background_sync_supplies_from_order_ids(
    order_ids: list[str],
    supply_info_by_order_id: dict[str, dict] | None = None,
) -> None:
    """
    Фоновая синхронизация поставок из ЛК Ozon по `order_id` (8 цифр).
    Заходит в v3/supply-order/get, извлекает posting_number (13 цифр) и upsert в ozon_supplies.
    """
    activate_manual_supply_priority()
    if not order_ids:
        return

    # Оставляем только валидные 8-значные order_id.
    # Не режем агрессивно: по запросу пользователя обрабатываем максимум за период.
    order_ids = [str(x).strip() for x in order_ids if str(x).strip() and str(x).strip().isdigit() and len(str(x).strip()) == 8]
    order_ids = list(dict.fromkeys(order_ids))[:1000]
    if not order_ids:
        return
    _set_supplies_sync_progress(
        stage="UPSERT",
        message="Обновление поставок в БД",
        total_order_ids=len(order_ids),
        processed_order_ids=0,
    )

    client = OzonAPIClient()

    # Для заявок из ЛК часто нет crossdock_cluster_id — подгружаем карту склад->кластер.
    def _norm_wh(v: str) -> str:
        v = (v or "").replace("\xa0", " ").strip().lower()
        v = v.replace("_", " ")
        v = re.sub(r"\s+", " ", v)
        return v

    wh_to_cluster: Dict[str, int] = {}
    wh_norms: list[str] = []
    try:
        clusters = await client.get_cluster_list(cluster_type="CLUSTER_TYPE_OZON")
        for c in clusters or []:
            if not isinstance(c, dict):
                continue
            macrolocal = c.get("macrolocal_cluster_id")
            try:
                macrolocal_id = int(macrolocal) if macrolocal is not None else 0
            except Exception:
                macrolocal_id = 0
            if not macrolocal_id:
                continue
            for w in c.get("warehouses") or []:
                if not isinstance(w, dict):
                    continue
                wn = _norm_wh(w.get("name") or "")
                if wn:
                    wh_to_cluster[wn] = macrolocal_id
                    wh_norms.append(wn)
    except Exception:
        wh_to_cluster = {}
        wh_norms = []

    async with AsyncSessionLocal() as sess:
        # На старте грузим существующие posting_number, чтобы избегать дубликатов.
        # Но posting_number мы узнаем только после v3/supply-order/get, поэтому делаем upsert позже.
        r_exist_by_oid = await sess.execute(
            select(OzonSupply).where(OzonSupply.ozon_supply_id.in_(order_ids))
        )
        existing_by_order_id = {
            str(getattr(r, "ozon_supply_id", "") or "").strip(): r
            for r in r_exist_by_oid.scalars().all()
            if str(getattr(r, "ozon_supply_id", "") or "").strip()
        }

        inserted_or_need_fill_supply_ids: list[int] = []
        touched_supply_ids: list[int] = []
        added_count = 0
        updated_count = 0

        # Сначала определим posting_number для каждого order_id.
        verified: list[dict] = []
        processed = 0
        for oid in order_ids:
            try:
                info_data = (supply_info_by_order_id or {}).get(oid)
                if not isinstance(info_data, dict):
                    info_data = await client.get_supply_info(oid)
                if not isinstance(info_data, dict) or info_data.get("_error"):
                    continue

                result = info_data.get("result") or {}
                inner = result.get("result") if isinstance(result, dict) else result
                orders = (inner or {}).get("orders") or info_data.get("orders") or []
                if isinstance(orders, dict):
                    orders = [orders]
                if not isinstance(orders, list) or not orders:
                    continue

                order0 = orders[0] if isinstance(orders[0], dict) else {}
                supplies = order0.get("supplies") or []
                if isinstance(supplies, dict):
                    supplies = [supplies]
                if not isinstance(supplies, list):
                    supplies = []

                sup0 = supplies[0] if (supplies and isinstance(supplies[0], dict)) else {}
                posting_raw = sup0.get("supply_id") or sup0.get("supplyId") or None
                posting_number = str(posting_raw).strip() if posting_raw is not None else ""
                if posting_number and posting_number.isdigit() and len(posting_number) >= 13:
                    posting_number = posting_number[:13]
                else:
                    posting_number = ""
                    # В bulk-ответе может не быть supplies; для существующей строки
                    # используем posting_number по order_id, чтобы обновить статус.
                    row_by_oid = existing_by_order_id.get(oid)
                    if row_by_oid is not None:
                        pn_existing = str(getattr(row_by_oid, "posting_number", "") or "").strip()
                        if pn_existing.isdigit() and len(pn_existing) >= 13:
                            posting_number = pn_existing[:13]
                if not posting_number:
                    continue

                dest = ""
                cluster_id = None
                storage_wh = sup0.get("storage_warehouse") or sup0.get("drop_off_warehouse") or {}
                if isinstance(storage_wh, dict):
                    dest = (storage_wh.get("name") or "").strip()
                    for ck in ("macrolocal_cluster_id", "cluster_id", "clusterId"):
                        if storage_wh.get(ck) is not None:
                            try:
                                cluster_id = int(storage_wh.get(ck))
                            except Exception:
                                cluster_id = None
                            if cluster_id:
                                break
                if not cluster_id and dest:
                    dest_n = _norm_wh(dest)
                    cid = int(wh_to_cluster.get(dest_n) or 0)
                    if not cid:
                        best_len = 0
                        best_cid = 0
                        for wn in wh_norms:
                            if wn and (wn in dest_n or dest_n in wn):
                                if len(wn) > best_len:
                                    best_len = len(wn)
                                    best_cid = int(wh_to_cluster.get(wn) or 0)
                        cid = best_cid
                    cluster_id = int(cid) if cid else None

                shipment_date_raw = sup0.get("shipment_date") or sup0.get("shipmentDate") or ""
                shipment_date = str(shipment_date_raw).strip()[:10] if shipment_date_raw else ""

                status = _extract_supply_status_from_info_data(info_data)
                if not status:
                    # Для редких кейсов, когда в bulk статус не пришел,
                    # добираем одиночным вызовом по order_id.
                    try:
                        info_single = await client.get_supply_info(oid)
                        status = _extract_supply_status_from_info_data(info_single)
                    except Exception:
                        status = ""

                verified.append(
                    {
                        "posting_number": posting_number,
                        "order_id": oid,
                        "destination_warehouse": dest,
                        "crossdock_cluster_id": cluster_id,
                        "shipment_date": shipment_date,
                        "status": status,
                    }
                )
            except Exception:
                continue
            finally:
                processed += 1
                _set_supplies_sync_progress(
                    processed_order_ids=processed,
                    total_order_ids=len(order_ids),
                    message=f"Проверено заявок: {processed}/{len(order_ids)}",
                )

        if not verified:
            _set_supplies_sync_progress(message="Нет валидных поставок за период")
            return

        posting_numbers = [v["posting_number"] for v in verified if v.get("posting_number")]
        posting_numbers = list(dict.fromkeys(posting_numbers))

        r_exist = await sess.execute(select(OzonSupply).where(OzonSupply.posting_number.in_(posting_numbers)))
        existing_rows = r_exist.scalars().all()
        existing_by_pn = {str(getattr(r, "posting_number", "") or ""): r for r in existing_rows}

        for v in verified:
            pn = v["posting_number"]
            order_id = v["order_id"]
            dest = v.get("destination_warehouse") or ""
            cluster_id = v.get("crossdock_cluster_id")
            shipment_date = v.get("shipment_date") or ""
            status = v.get("status") or ""

            row = None
            if order_id and order_id in existing_by_order_id:
                row = existing_by_order_id[order_id]
            elif pn in existing_by_pn:
                row = existing_by_pn[pn]

            if row is not None:
                touched_supply_ids.append(int(row.id))
                changed = False
                if (not getattr(row, "ozon_supply_id", None) or not str(row.ozon_supply_id).strip()) and order_id:
                    row.ozon_supply_id = order_id
                    changed = True
                if (not getattr(row, "destination_warehouse", None) or not str(row.destination_warehouse).strip()) and dest:
                    row.destination_warehouse = dest
                    changed = True
                if (getattr(row, "crossdock_cluster_id", None) in (None, 0)) and cluster_id:
                    row.crossdock_cluster_id = int(cluster_id)
                    changed = True
                if (not getattr(row, "shipment_date", None) or not str(row.shipment_date).strip()) and shipment_date:
                    row.shipment_date = shipment_date
                    changed = True
                if status:
                    new_status = _normalize_supply_status_for_ui(status)
                    cur_status = _normalize_supply_status_for_ui(getattr(row, "status", None) or "")
                    if new_status and new_status != cur_status:
                        row.status = new_status
                        status_updated_count += 1
                        changed = True

                row_comp = getattr(row, "composition", None)
                comp_is_empty = not isinstance(row_comp, list) or len(row_comp) == 0
                # Важно: даже если upsert ничего не поменял, но composition пустой,
                # всё равно ставим строку в очередь на дозаполнение состава из ЛК.
                if comp_is_empty:
                    inserted_or_need_fill_supply_ids.append(int(row.id))

                if changed:
                    updated_count += 1
                continue

            row = OzonSupply(
                ozon_supply_id=order_id,
                posting_number=pn,
                crossdock_cluster_id=(int(cluster_id) if cluster_id else None),
                destination_warehouse=dest,
                shipment_date=shipment_date,
                timeslot_from=None,
                timeslot_to=None,
                delivery_date_estimated="",
                composition=[],
                status=_normalize_supply_status_for_ui(status or "created"),
                has_cargo_places=0,
                cargo_places_status="",
                cargo_places_data=[],
                draft_id=None,
            )
            sess.add(row)
            await sess.flush()
            if order_id:
                existing_by_order_id[str(order_id).strip()] = row
            existing_by_pn[str(pn).strip()] = row
            inserted_or_need_fill_supply_ids.append(int(row.id))
            touched_supply_ids.append(int(row.id))
            added_count += 1

        await sess.commit()

        # Заполняем composition уже отдельной задачей.
        # Если всё сделалось быстро — всё равно ограничиваем количество.
        inserted_or_need_fill_supply_ids = list(dict.fromkeys(inserted_or_need_fill_supply_ids))[:500]
        if inserted_or_need_fill_supply_ids:
            _set_supplies_sync_progress(
                stage="COMPOSITION",
                message=f"Подтягиваю составы: {len(inserted_or_need_fill_supply_ids)}",
                composition_filled=len(inserted_or_need_fill_supply_ids),
            )
            await _background_fill_composition_for_lk_supplies(inserted_or_need_fill_supply_ids)
        touched_supply_ids = list(dict.fromkeys(touched_supply_ids))[:1000]
        if touched_supply_ids:
            _set_supplies_sync_progress(
                stage="CARGO",
                message=f"Подтягиваю грузоместа: {len(touched_supply_ids)}",
                cargo_rows_filled=len(touched_supply_ids),
            )
            await _background_fill_cargo_places_for_lk_supplies(touched_supply_ids)
        _set_supplies_sync_progress(
            stage="UPSERT_DONE",
            message=f"БД обновлена: добавлено {added_count}, обновлено {updated_count}",
            added=added_count,
            updated=updated_count,
        )

        logger.info(
            "background_sync_supplies_from_order_ids: queued={} added={} updated={} status_updated={}",
            len(inserted_or_need_fill_supply_ids),
            added_count,
            updated_count,
            status_updated_count,
        )


async def _background_sync_supplies_from_lk_period(since_iso: str, to_iso: str) -> None:
    """
    Фоновая синхронизация поставок из ЛК Ozon за период.
    Для получения списка заявок использует `POST /v3/supply-order/list` (по инструкции пользователя),
    затем верифицирует и upsert-ит в `ozon_supplies` через `_background_sync_supplies_from_order_ids`.
    """
    activate_manual_supply_priority()
    try:
        client = OzonAPIClient()
        _set_supplies_sync_progress(stage="LIST", message="Запрашиваю список поставок из ЛК")

        # cutoff по since_iso (30 дней).
        cutoff_msk = _ensure_datetime_msk(since_iso)
        if not cutoff_msk:
            cutoff_msk = datetime.now(MSK) - timedelta(days=30)

        # Грузим все основные статусы поставок, чтобы в таблицу попадали не только завершенные.
        states = [
            "DATA_FILLING",
            "READY_TO_SUPPLY",
            "ACCEPTED_AT_SUPPLY_WAREHOUSE",
            "IN_TRANSIT",
            "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
            "REPORTS_CONFIRMATION_AWAITING",
            "REPORT_REJECTED",
            "COMPLETED",
            "REJECTED_AT_SUPPLY_WAREHOUSE",
            "CANCELLED",
            "OVERDUE",
        ]
        sort_by = "ORDER_CREATION"
        sort_dir = "DESC"

        last_id: str | None = None
        collected_order_ids: list[str] = []
        supply_info_by_order_id: dict[str, dict] = {}

        # Ограничиваем синк 100 последними поставками в пределах 30 дней.
        max_to_collect = 100
        max_pages = 2
        bulk_get_size = 50

        def _extract_order_creation_dt(info_data: dict) -> datetime | None:
            result = info_data.get("result") or {}
            inner = result.get("result") if isinstance(result, dict) else None
            orders = None
            if isinstance(inner, dict):
                orders = inner.get("orders")
            if orders is None and isinstance(result, dict):
                orders = result.get("orders")
            if orders is None:
                orders = info_data.get("orders")
            if isinstance(orders, list) and orders:
                o0 = orders[0] if isinstance(orders[0], dict) else {}
                for key in (
                    "order_creation_date",
                    "orderCreationDate",
                    "order_creation",
                    "orderCreation",
                    "created_at",
                    "createdAt",
                    "order_created_at",
                    "orderCreatedAt",
                ):
                    if key in o0 and o0.get(key) is not None:
                        dt = _ensure_datetime_msk(o0.get(key))
                        if dt:
                            return dt
            return None

        def _split_bulk_supply_get_by_order_id(data: dict) -> dict[str, dict]:
            out: dict[str, dict] = {}
            if not isinstance(data, dict):
                return out
            result = data.get("result") or {}
            inner = result.get("result") if isinstance(result, dict) else result
            orders = []
            if isinstance(inner, dict):
                orders = inner.get("orders") or []
            if (not orders) and isinstance(result, dict):
                orders = result.get("orders") or []
            if (not orders) and isinstance(data.get("orders"), list):
                orders = data.get("orders") or []
            if isinstance(orders, dict):
                orders = [orders]
            if not isinstance(orders, list):
                orders = []
            for o in orders:
                if not isinstance(o, dict):
                    continue
                oid = str(
                    o.get("order_id")
                    or o.get("orderId")
                    or o.get("id")
                    or ""
                ).strip()
                if not (oid.isdigit() and len(oid) == 8):
                    continue
                out[oid] = {"result": {"result": {"orders": [o]}}}
            return out

        stop_pagination = False
        for _page in range(max_pages):
            _set_supplies_sync_progress(
                stage="LIST",
                message=f"Страница списка: {_page + 1}",
                total_order_ids=len(collected_order_ids),
            )
            list_resp = await client.list_supply_orders(
                states=states,
                last_id=last_id,
                limit=100,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
            if not isinstance(list_resp, dict) or list_resp.get("_error"):
                return

            page_order_ids = list_resp.get("order_ids") or list_resp.get("orderIds") or []
            if not isinstance(page_order_ids, list) or not page_order_ids:
                break

            page_ids: list[str] = []
            for oid in page_order_ids:
                oid_str = str(oid).strip()
                if oid_str and oid_str.isdigit() and len(oid_str) == 8:
                    page_ids.append(oid_str)

            for i in range(0, len(page_ids), bulk_get_size):
                chunk = page_ids[i:i + bulk_get_size]
                bulk_resp = await client.get_supply_info_many(chunk)
                split_map = _split_bulk_supply_get_by_order_id(bulk_resp)
                for oid_str in chunk:
                    info_data = split_map.get(oid_str)
                    if not isinstance(info_data, dict):
                        # fallback только для редких кейсов, которые не распарсились из bulk
                        info_data = await client.get_supply_info(oid_str)
                    if not isinstance(info_data, dict) or info_data.get("_error"):
                        continue
                    supply_info_by_order_id[oid_str] = info_data

                    created_dt = _extract_order_creation_dt(info_data)
                    # Если дату создания не смогли извлечь — не включаем поставку в sync,
                    # чтобы не подтягивать старые поставки вне периода.
                    if not created_dt:
                        continue
                    if created_dt < cutoff_msk:
                        stop_pagination = True
                        break

                    collected_order_ids.append(oid_str)
                    _set_supplies_sync_progress(
                        stage="LIST",
                        message=f"Найдено поставок за период: {len(collected_order_ids)}",
                        total_order_ids=len(collected_order_ids),
                    )
                    if len(collected_order_ids) >= max_to_collect:
                        stop_pagination = True
                        break
                if stop_pagination:
                    break

            if stop_pagination:
                break

            next_last_id = list_resp.get("last_id")
            if next_last_id is None:
                next_last_id = list_resp.get("lastId")
            if next_last_id is None:
                break
            if isinstance(next_last_id, str) and next_last_id.strip().lower() == "null":
                break
            last_id = str(next_last_id)

        # Дополнительно: принудительно обновляем все локальные поставки с ozon_supply_id,
        # чтобы статусы/данные обновлялись у всех строк в таблице.
        try:
            async with AsyncSessionLocal() as db:
                r_local = await db.execute(select(OzonSupply.ozon_supply_id))
                for (oid_raw,) in r_local.all():
                    oid = str(oid_raw or "").strip()
                    if oid.isdigit() and len(oid) == 8:
                        collected_order_ids.append(oid)
        except Exception as e:
            logger.warning("sync_from_lk append local order_ids failed: {}", e)

        collected_order_ids = list(dict.fromkeys(collected_order_ids))
        if not collected_order_ids:
            _set_supplies_sync_progress(stage="DONE", message="За период поставок не найдено")
            return

        _set_supplies_sync_progress(
            stage="UPSERT",
            message=f"Обрабатываю найденные поставки: {len(collected_order_ids)}",
            total_order_ids=len(collected_order_ids),
            processed_order_ids=0,
        )
        await _background_sync_supplies_from_order_ids(
            collected_order_ids[:max_to_collect],
            supply_info_by_order_id=supply_info_by_order_id,
        )
    except Exception as e:
        logger.warning("background_sync_supplies_from_lk_period v3/list exception={}", e, exc_info=True)


def _format_supply_order_get_error(info_data: dict) -> str:
    """Человекочитаемое сообщение об ошибке ответа v3/supply-order/get."""
    err = info_data.get("_error") or ""
    ozon = info_data.get("ozon_response")
    if isinstance(ozon, dict):
        msg = ozon.get("message") or ozon.get("error") or ""
        details = ozon.get("details") or ozon.get("errors") or []
        if isinstance(details, list) and details:
            parts = []
            for d in details:
                if isinstance(d, dict):
                    parts.append(d.get("message") or d.get("error_message") or str(d))
                else:
                    parts.append(str(d))
            if parts:
                msg = (msg + ": " if msg else "") + "; ".join(parts)
        if msg:
            return msg
    if isinstance(ozon, str):
        return ozon
    return str(err) or "Ошибка запроса статуса поставки"


async def _poll_supply_order_status_after_cargo_save(supply_id: int, order_id: str) -> None:
    """Фоновая задача: опрос v3/supply-order/get до READY_TO_SUPPLY; обновление status или status_check_error."""
    logger.info("poll_supply_order_status: started supply_id=%s order_id=%s", supply_id, order_id)
    try:
        for col_sql in (
            "ALTER TABLE ozon_supplies ADD COLUMN status_check_error VARCHAR(512)",
        ):
            try:
                async with AsyncSessionLocal() as sess:
                    await sess.execute(text(col_sql))
                    await sess.commit()
            except Exception:
                pass
        client = OzonAPIClient()
        for attempt in range(30):
            info_data = await client.get_supply_info(order_id)
            if info_data.get("_error"):
                msg = _format_supply_order_get_error(info_data)
                async with AsyncSessionLocal() as sess:
                    await sess.execute(
                        text("UPDATE ozon_supplies SET status_check_error = :err WHERE id = :sid"),
                        {"err": msg[:512] if msg else None, "sid": supply_id},
                    )
                    await sess.commit()
                logger.warning("poll_supply_order_status: supply_id=%s error=%s", supply_id, msg)
                return
            result = info_data.get("result") or {}
            # v3/supply-order/get может вернуть result.result.items или result.orders или result.items
            inner = result.get("result") or result
            items_raw = (
                inner.get("items")
                or result.get("items")
                or info_data.get("items")
                or info_data.get("orders")
                or result.get("orders")
                or []
            )
            if isinstance(items_raw, dict):
                items_raw = [items_raw]
            if attempt == 0 and items_raw:
                logger.info(
                    "poll_supply_order_status: supply_id=%s response keys=%s first_item keys=%s",
                    supply_id,
                    list(info_data.keys()),
                    list(items_raw[0].keys()) if isinstance(items_raw[0], dict) else None,
                )
            order_state = None
            for it in items_raw if isinstance(items_raw, list) else []:
                if not isinstance(it, dict):
                    continue
                st = (it.get("order_state") or it.get("state") or "").strip()
                if st in ("READY_TO_SUPPLY", "ORDER_STATE_READY_TO_SUPPLY"):
                    order_state = "READY_TO_SUPPLY"
                    break
            if order_state == "READY_TO_SUPPLY":
                async with AsyncSessionLocal() as sess:
                    await sess.execute(
                        text("UPDATE ozon_supplies SET status = :st, status_check_error = NULL WHERE id = :rid"),
                        {"st": "READY_TO_SUPPLY", "rid": supply_id},
                    )
                    await sess.commit()
                logger.info("poll_supply_order_status: supply_id=%s status=READY_TO_SUPPLY", supply_id)
                return
            await asyncio.sleep(2)
        logger.warning("poll_supply_order_status: supply_id=%s timeout (30 attempts)", supply_id)
    except Exception as e:
        logger.warning("poll_supply_order_status: supply_id=%s exception=%s", supply_id, e)
        try:
            async with AsyncSessionLocal() as sess:
                await sess.execute(
                    text("UPDATE ozon_supplies SET status_check_error = :err WHERE id = :sid"),
                    {"err": str(e)[:512], "sid": supply_id},
                )
                await sess.commit()
        except Exception:
            pass


async def _background_composition_check_after_confirm(supply_id: int, order_id: str) -> None:
    """
    Фоновая задача: через несколько секунд после создания заявки запрашивает состав по bundle_id,
    сверяет с заявленным и обновляет composition_mismatch_message в БД.
    При отсутствии bundle_id — одна повторная попытка через 5 сек.
    """
    if not order_id or not str(order_id).strip().isdigit():
        return
    order_id_str = str(order_id).strip()
    logger.info("background_composition_check: started supply_id=%s order_id=%s", supply_id, order_id_str)
    try:
        await asyncio.sleep(3)
        client = OzonAPIClient()
        info_data = await client.get_supply_info(order_id_str)
        if info_data.get("_error"):
            logger.warning("background_composition_check: get_supply_info error supply_id=%s", supply_id)
            return
        bundle_ids = _extract_bundle_ids_from_supply_order_response(info_data)
        if not bundle_ids:
            logger.info("background_composition_check: no bundle_ids yet supply_id=%s, retry in 5s", supply_id)
            await asyncio.sleep(5)
            info_data = await client.get_supply_info(order_id_str)
            if info_data.get("_error"):
                return
            bundle_ids = _extract_bundle_ids_from_supply_order_response(info_data)
        if not bundle_ids:
            logger.info("background_composition_check: no bundle_ids after retry supply_id=%s", supply_id)
            return
        bundle_resp = await client.get_supply_order_bundle(bundle_ids)
        if bundle_resp.get("_error"):
            logger.warning("background_composition_check: get_supply_order_bundle error supply_id=%s", supply_id)
            return
        actual_by_sku = _parse_bundle_response_to_actual_by_sku(bundle_resp)
        if not actual_by_sku:
            logger.info("background_composition_check: bundle response empty or unparsed, skip mismatch supply_id=%s", supply_id)
            return
        async with AsyncSessionLocal() as sess:
            r = await sess.execute(
                text("SELECT composition FROM ozon_supplies WHERE id = :sid"),
                {"sid": supply_id},
            )
            row = r.fetchone()
        if not row or not row[0]:
            return
        try:
            declared = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
        except Exception:
            declared = []
        mismatch_msg = _compute_composition_mismatch_from_actual(declared, actual_by_sku)
        async with AsyncSessionLocal() as sess:
            await sess.execute(
                text("UPDATE ozon_supplies SET composition_mismatch_message = :msg WHERE id = :sid"),
                {"msg": (mismatch_msg[:512] if mismatch_msg else None), "sid": supply_id},
            )
            await sess.commit()
        if mismatch_msg:
            logger.info("background_composition_check: mismatch set supply_id=%s msg=%s", supply_id, mismatch_msg[:100])
        else:
            logger.info("background_composition_check: no mismatch supply_id=%s", supply_id)
    except Exception as e:
        logger.warning("background_composition_check: supply_id=%s exception=%s", supply_id, e)


@router.post("/api/supplies/{supply_id}/cargo-places")
async def api_supplies_cargo_places(
    supply_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Установка грузомест. POST /v1/cargoes/create. При SUCCESS сохраняем грузоместа; затем в фоне опрашиваем v3/supply-order/get до READY_TO_SUPPLY."""
    started_at = time.perf_counter()
    flow_id = uuid.uuid4().hex[:10]
    flog = logger.bind(cargo_places_flow_id=flow_id, supply_row_id=supply_id)
    try:
        body = await request.json()
    except Exception:
        flog.warning("cargo_places: invalid json body")
        return JSONResponse(status_code=400, content={"ok": False, "error": "Неверный JSON"})
    cargo_places = body.get("cargo_places")
    flog.info(
        "cargo_places: start user={} supply_id={} places_count={}",
        getattr(user, "username", "unknown"),
        supply_id,
        len(cargo_places) if isinstance(cargo_places, list) else 0,
    )
    if not isinstance(cargo_places, list) or len(cargo_places) == 0:
        flog.warning("cargo_places: empty cargo_places payload")
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен непустой массив cargo_places"})
    if len(cargo_places) > 30:
        flog.warning("cargo_places: too many places count={}", len(cargo_places))
        return JSONResponse(status_code=400, content={"ok": False, "error": "Не более 30 коробок (или 40 палет)"})
    try:
        r = await db.execute(select(OzonSupply).where(OzonSupply.id == supply_id))
        row = r.scalar_one_or_none()
        if not row:
            flog.warning("cargo_places: supply row not found supply_id={}", supply_id)
            return JSONResponse(status_code=404, content={"ok": False, "error": "Поставка не найдена"})
        posting_number = (getattr(row, "posting_number", None) or row.ozon_supply_id or "").strip()
        if not posting_number or not posting_number.isdigit():
            flog.warning(
                "cargo_places: missing numeric posting_number supply_id={} posting_number={} ozon_supply_id={}",
                supply_id,
                posting_number,
                row.ozon_supply_id,
            )
            return JSONResponse(status_code=400, content={"ok": False, "error": "Нет идентификатора поставки (posting_number) для API"})
        supply_id_api = int(posting_number)
        composition = list(row.composition) if isinstance(row.composition, list) else []
        product_ids = [c.get("product_id") for c in composition if c.get("product_id")]
        products_map = {}
        if product_ids:
            r_pr = await db.execute(select(Product).where(Product.id.in_(product_ids)))
            for p in r_pr.scalars().all():
                products_map[p.id] = p
    except Exception as e:
        flog.warning("cargo_places: load failed error={}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    sku_to_offer_barcode = {}
    for c in composition:
        if not isinstance(c, dict):
            continue
        pid = c.get("product_id")
        sku = c.get("sku")
        if sku is not None:
            offer_id = ""
            barcode = str(c.get("barcode") or "").strip()
            if pid and products_map.get(pid):
                offer_id = (products_map[pid].article or "").strip()
                if not barcode:
                    barcode = offer_id
            sku_to_offer_barcode[str(sku)] = {"offer_id": offer_id, "barcode": barcode or str(sku)}
    cargoes = []
    cargo_places_storage = []  # для сохранения в БД: [{"key", "type", "items": [{"sku", "quantity"}]}]
    for place in cargo_places:
        key = str(place.get("cargo_place_id") or "")
        if not key:
            key = str(len(cargoes) + 1)
        place_type = (place.get("cargo_place_type") or "BOX").upper()
        if place_type not in ("BOX", "PALLET"):
            place_type = "BOX"
        items_raw = place.get("items") or []
        value_items = []
        storage_items = []
        for it in items_raw:
            sku = it.get("sku")
            qty = int(it.get("quantity") or 0)
            if not sku or qty <= 0:
                continue
            ob = sku_to_offer_barcode.get(str(sku)) or {}
            if isinstance(ob, dict):
                offer_id = ob.get("offer_id") or ""
                barcode = ob.get("barcode") or str(sku)
            else:
                offer_id = ""
                barcode = str(sku)
            value_items.append({
                "barcode": barcode,
                "quantity": qty,
                "offer_id": offer_id or barcode,
            })
            storage_items.append({"sku": int(sku), "quantity": qty})
        cargoes.append({
            "key": key,
            "value": {
                "type": place_type,
                "items": value_items,
            },
        })
        cargo_places_storage.append({"key": key, "type": place_type, "items": storage_items})
    if not cargoes:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нет товаров в грузоместах"})
    # При корректировке состава передаём delete_current_version: true — тогда Ozon удалит ранее созданные грузоместа и применит новый состав из запроса (POST /v1/cargoes/create + проверка через /v1/cargoes/create/info).
    has_existing_cargo = bool(getattr(row, "has_cargo_places", False))
    delete_current_version = has_existing_cargo or bool(body.get("delete_current_version"))
    client = OzonAPIClient()
    flog.info(
        "cargo_places: sending cargoes.create supply_id_api={} cargoes_count={} delete_current_version={}",
        supply_id_api,
        len(cargoes),
        delete_current_version,
    )
    resp = await client.set_cargo_places(
        supply_id=supply_id_api,
        cargoes=cargoes,
        delete_current_version=delete_current_version,
    )
    if resp.get("_error"):
        err_detail = resp.get("ozon_response")
        if isinstance(err_detail, dict):
            details = err_detail.get("details") or err_detail.get("errors") or []
            err_parts = [d.get("message") or d.get("error_message") or str(d) for d in details if isinstance(d, dict)]
            err_msg = "; ".join(err_parts) if err_parts else err_detail.get("message") or str(err_detail)
        else:
            err_msg = str(err_detail)
        status = 502 if resp.get("status_code") != 400 else 400
        flog.warning(
            "cargo_places: cargoes.create error supply_id_api={} status_code={} error={} detail={}",
            supply_id_api,
            resp.get("status_code"),
            resp.get("_error"),
            err_msg,
        )
        return JSONResponse(
            status_code=status,
            content={"ok": False, "error": resp.get("_error"), "detail": err_msg or resp},
        )
    flog.info(
        "cargo_places: cargoes.create ok resp_keys={} result_keys={}",
        list(resp.keys()),
        list((resp.get("result") or {}).keys()) if isinstance(resp.get("result"), dict) else [],
    )
    operation_id = resp.get("operation_id") or (resp.get("result") or {}).get("operation_id")
    if not operation_id:
        try:
            if getattr(row, "cargo_places_status", None) is not None:
                row.cargo_places_status = "FAILED"
            await db.commit()
        except Exception as e:
            flog.warning("cargo_places: failed to set FAILED status without operation_id error={}", e)
        return JSONResponse(
            status_code=502,
            content={"ok": False, "error": "Ozon не вернул operation_id для проверки статуса грузомест", "detail": resp},
        )
    # Временно сохраняем статус PENDING и отправленный состав
    try:
        if getattr(row, "cargo_places_data", None) is not None:
            row.cargo_places_data = cargo_places_storage
        if getattr(row, "cargo_places_status", None) is not None:
            row.cargo_places_status = "PENDING"
        await db.commit()
    except Exception as e:
        flog.warning("cargo_places: failed to set PENDING status error={}", e)
    # Пауза 3 секунды, затем запрос статуса по v2/cargoes/create/info
    await asyncio.sleep(3)
    for attempt in range(10):
        info_resp = await client.get_cargoes_create_info(str(operation_id))
        if info_resp.get("_error"):
            flog.warning("cargo_places: get_cargoes_create_info error operation_id={} error={}", operation_id, info_resp.get("_error"))
            try:
                if getattr(row, "cargo_places_status", None) is not None:
                    row.cargo_places_status = "FAILED"
                await db.commit()
            except Exception as e:
                flog.warning("cargo_places: failed to set FAILED on info error={}", e)
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "Ошибка при проверке статуса грузомест", "detail": info_resp.get("_error")},
            )
        result = info_resp.get("result") or {}
        status_val = (result.get("status") or "").strip()
        cargoes_list = result.get("cargoes") or []
        cargoes_len = len(cargoes_list) if isinstance(cargoes_list, list) else 0
        if cargoes_len > 0 and status_val in ("", "STATUS_UNSPECIFIED"):
            status_val = "SUCCESS"
        if status_val == "SUCCESS" or (cargoes_len > 0 and status_val in ("", "STATUS_UNSPECIFIED")):
            flog.info(
                "cargo_places: info success attempt={} status={} cargoes_len={}",
                attempt + 1,
                status_val,
                cargoes_len,
            )
            # После SUCCESS запрашиваем актуальные данные грузомест через /v1/cargoes/get и из них формируем Состав
            saved_cargoes = []
            get_cargoes_resp = await client.get_cargoes(supply_id_api)
            our_items_by_index = [place.get("items", []) for place in cargo_places_storage]
            if not get_cargoes_resp.get("_error"):
                parsed = _parse_cargoes_from_get_response(get_cargoes_resp, our_items_by_index)
                if parsed:
                    saved_cargoes = parsed
                elif cargoes_len > 0:
                    flog.info("cargo_places: get_cargoes empty, fallback to create/info")
            if not saved_cargoes:
                # Fallback: данные из create/info
                if cargoes_len > 0 and cargoes_list and isinstance(cargoes_list[0], dict):
                    flog.info("cargo_places: create/info first cargo keys={}", list(cargoes_list[0].keys()))
                saved_cargoes = []
                for i, api_cargo in enumerate(cargoes_list if isinstance(cargoes_list, list) else []):
                    if not isinstance(api_cargo, dict):
                        continue
                    val = api_cargo.get("value") or {}
                    cid = api_cargo.get("id") or api_cargo.get("cargo_id")
                    if cid is None and isinstance(val, dict):
                        cid = val.get("id") or val.get("cargo_id")
                    if cid is not None:
                        try:
                            cid = int(cid)
                        except (TypeError, ValueError):
                            cid = None
                    ctype = val.get("type") if isinstance(val, dict) else api_cargo.get("type") or "BOX"
                    key = api_cargo.get("key") or str(i + 1)
                    our_items = cargo_places_storage[i].get("items", []) if i < len(cargo_places_storage) else []
                    api_items = val.get("items", []) if isinstance(val, dict) else []
                    items = list(our_items) if our_items else []
                    if not items and api_items:
                        for it in api_items:
                            if isinstance(it, dict) and it.get("quantity") is not None:
                                items.append({"sku": it.get("sku") or 0, "quantity": int(it.get("quantity") or 0)})
                    saved_cargoes.append({
                        "cargo_id": int(cid) if cid is not None else None,
                        "key": str(key),
                        "type": str(ctype) if ctype in ("BOX", "PALLET") else "BOX",
                        "items": items,
                    })
            try:
                # На сервере колонки могут отсутствовать, если миграция не выполнялась — добавляем при необходимости
                for col_sql in (
                    "ALTER TABLE ozon_supplies ADD COLUMN cargo_places_data TEXT NOT NULL DEFAULT '[]'",
                    "ALTER TABLE ozon_supplies ADD COLUMN cargo_places_status VARCHAR(32) NOT NULL DEFAULT ''",
                    "ALTER TABLE ozon_supplies ADD COLUMN has_cargo_places INTEGER NOT NULL DEFAULT 0",
                ):
                    try:
                        await db.execute(text(col_sql))
                    except Exception:
                        pass
                # Обновляем только сырым UPDATE (не трогаем row, чтобы commit не делал flush по ORM)
                cargo_json = _json.dumps(saved_cargoes)
                stmt = text("UPDATE ozon_supplies SET cargo_places_data = :cargo_data, cargo_places_status = :cp_status, has_cargo_places = :has_cp WHERE id = :row_id")
                await db.execute(stmt, {
                    "cargo_data": cargo_json,
                    "cp_status": "SUCCESS",
                    "has_cp": 1,
                    "row_id": row.id,
                })
                comp = list(row.composition) if isinstance(row.composition, list) else []
                mismatch_msg = _compute_composition_mismatch(comp, saved_cargoes)
                await db.execute(
                    text("UPDATE ozon_supplies SET composition_mismatch_message = :msg WHERE id = :row_id"),
                    {"msg": (mismatch_msg[:512] if mismatch_msg else None), "row_id": row.id},
                )
                flog.info("cargo_places: saved cargo_places_data len={} supply_id={}", len(saved_cargoes), supply_id)
                await db.commit()
            except Exception as e:
                flog.warning("cargo_places: save from response failed error={}", repr(e), exc_info=True)
                return JSONResponse(
                    status_code=500,
                    content={"ok": False, "error": "Не удалось сохранить данные грузомест", "detail": str(e)},
                )
            # Запускаем в фоне опрос v3/supply-order/get до READY_TO_SUPPLY (ответ клиенту сразу)
            order_id_for_get = (row.ozon_supply_id or "").strip()
            if order_id_for_get and order_id_for_get.isdigit():
                flog.info("cargo_places: queue background poll supply_id={} order_id={}", row.id, order_id_for_get)
                background_tasks.add_task(_poll_supply_order_status_after_cargo_save, row.id, order_id_for_get)
            else:
                flog.warning(
                    "cargo_places: skip background poll no order_id supply_id={} ozon_supply_id={}",
                    row.id,
                    getattr(row, "ozon_supply_id", None),
                )
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            flog.info("cargo_places: success supply_id={} elapsed_ms={}", supply_id, elapsed_ms)
            return JSONResponse(content={"ok": True, "message": "Грузоместа сохранены.", "has_cargo_places": True})
        if status_val == "FAILED":
            try:
                if getattr(row, "cargo_places_status", None) is not None:
                    row.cargo_places_status = "FAILED"
                await db.commit()
            except Exception as e:
                flog.warning("cargo_places: failed to set FAILED status on rejected flow error={}", e)
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": "Ozon отклонил создание грузомест", "detail": result},
            )
        flog.info(
            "cargo_places: info pending attempt={} status={} cargoes_len={} operation_id={}",
            attempt + 1,
            status_val,
            cargoes_len,
            operation_id,
        )
        await asyncio.sleep(2)
    try:
        if getattr(row, "cargo_places_status", None) is not None:
            row.cargo_places_status = "FAILED"
        await db.commit()
    except Exception as e:
        flog.warning("cargo_places: timeout update failed error={}", e)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    flog.warning("cargo_places: timeout waiting for success supply_id={} elapsed_ms={}", supply_id, elapsed_ms)
    return JSONResponse(
        status_code=504,
        content={"ok": False, "error": "Таймаут ожидания подтверждения грузомест в Ozon", "detail": "IN_PROGRESS"},
    )


def _qr_data_url(text: str, size: int = 4) -> str:
    """Сгенерировать QR-код в виде data URL (для печати ШК поставки)."""
    try:
        import qrcode
        import io
        import base64
        qr = qrcode.QRCode(version=1, box_size=size, border=2)
        qr.add_data(text)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
    except Exception:
        return ""


@router.post("/api/supplies/{supply_id}/cargo-labels/request")
async def api_supplies_cargo_labels_request(
    supply_id: int,
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Три этапа по документации Ozon:
    1) POST /v1/cargoes-label/create — задание на формирование этикеток грузомест;
    2) POST /v1/cargoes-label/get — получить статус и file_guid;
    3) PDF по GET /v1/cargoes-label/file/{file_guid} — отдаётся отдельным endpoint-ом.
    Опрашиваем get до SUCCESS или ошибки, возвращаем file_guids для ссылок на PDF.
    """
    try:
        supply_id_str = str(supply_id).strip()

        # body опционально: cargo_ids — список cargo_id для печати.
        # Это позволяет не зависеть от наличия cargo_places_data в БД.
        try:
            body = await request.json()
        except Exception:
            body = {}
        cargo_ids: list[int] = []
        raw_cargo_ids = body.get("cargo_ids") or body.get("cargo_id") or []
        if isinstance(raw_cargo_ids, list):
            for cid in raw_cargo_ids:
                try:
                    cii = int(cid)
                    if cii > 0:
                        cargo_ids.append(cii)
                except (TypeError, ValueError):
                    continue

        # Если cargo_ids не передали — берём из БД (только как fallback).
        row = None
        if not cargo_ids:
            r = await db.execute(
                select(OzonSupply).where(
                    or_(
                        OzonSupply.id == supply_id,
                        OzonSupply.posting_number == supply_id_str,
                        OzonSupply.ozon_supply_id == supply_id_str,
                    )
                )
            )
            row = r.scalar_one_or_none()
            if row:
                raw = getattr(row, "cargo_places_data", None)
                data = raw
                if isinstance(data, bytes):
                    try:
                        data = data.decode("utf-8", errors="replace")
                    except Exception:
                        data = "[]"
                if isinstance(data, str):
                    try:
                        data = _json.loads(data) if (data or "").strip() else []
                    except Exception:
                        data = []
                if not isinstance(data, list):
                    data = []
                for c in data:
                    if not isinstance(c, dict):
                        continue
                    cid = c.get("cargo_id")
                    if cid is None:
                        val = c.get("value")
                        if isinstance(val, dict):
                            cid = val.get("cargo_id") or val.get("id")
                    if cid is not None:
                        try:
                            ci2 = int(cid)
                            if ci2 > 0:
                                cargo_ids.append(ci2)
                        except (TypeError, ValueError):
                            pass

        if not cargo_ids:
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error": "Нет cargo_ids для печати ШК грузоместа.",
                },
            )

        # supply_id_ozon — это posting_number, который фронт передаёт в URL.
        # Если вдруг row есть — предпочитаем posting_number из БД.
        supply_id_ozon: int | None = None
        if row:
            pn = (getattr(row, "posting_number", None) or "").strip()
            if pn.isdigit():
                supply_id_ozon = int(pn)
        if not supply_id_ozon:
            if supply_id_str.isdigit():
                supply_id_ozon = int(supply_id_str)
            else:
                supply_id_ozon = supply_id
        client = OzonAPIClient()
        cargoes_with_status = [{"cargo_id": cid, "status": "IN_PROGRESS", "file_guid": None, "file_url": None} for cid in cargo_ids]
        for cargo_index, one_cargo_id in enumerate(cargo_ids):
            create_resp = None
            create_rate_limit_retries = 0
            # Держим UX отзывчивым: короткий backoff вместо "зависания" запроса на минуты.
            create_max_rate_limit_retries = 3
            while True:
                create_resp = await client.cargoes_label_create(supply_id_ozon, [one_cargo_id])
                if not create_resp.get("_error"):
                    break
                if create_resp.get("status_code") == 429 and create_rate_limit_retries < create_max_rate_limit_retries:
                    create_rate_limit_retries += 1
                    wait_sec = 2 + create_rate_limit_retries * 2
                    logger.info(
                        "cargo-labels/request: create 429 rate limit, retry {}/{} in {} sec cargo_id={}",
                        create_rate_limit_retries,
                        create_max_rate_limit_retries,
                        wait_sec,
                        one_cargo_id,
                    )
                    await asyncio.sleep(wait_sec)
                    continue
                err = create_resp.get("_error") or "Ошибка создания этикеток"
                if create_resp.get("status_code") == 429:
                    err = "Слишком много запросов к Ozon. Попробуйте через минуту."
                cargoes_with_status[cargo_index]["status"] = "FAILED"
                return JSONResponse(
                    status_code=502 if create_resp.get("status_code") != 400 else 400,
                    content={"ok": False, "error": err, "cargoes": cargoes_with_status, "ozon_response": create_resp.get("ozon_response")},
                )
            operation_id = (
                create_resp.get("operation_id")
                or (create_resp.get("result") or {}).get("operation_id")
                or create_resp.get("result")
            )
            if not operation_id:
                logger.warning("cargoes_label_create: no operation_id in response, keys=%s", list(create_resp.keys()))
                cargoes_with_status[cargo_index]["status"] = "FAILED"
                return JSONResponse(status_code=502, content={"ok": False, "error": "Ozon не вернул operation_id", "cargoes": cargoes_with_status})
            operation_id = str(operation_id).strip()
            logger.info("cargo-labels/request: create ok cargo_index={} cargo_id={} operation_id={}", cargo_index, one_cargo_id, operation_id[:36])
            await asyncio.sleep(5)
            rate_limit_retries = 0
            max_rate_limit_retries = 3
            file_guid_for_cargo = None
            file_url_for_cargo = None
            for attempt in range(1, 61):
                if attempt > 1:
                    await asyncio.sleep(8)
                get_resp = await client.cargoes_label_get(operation_id)
                if get_resp.get("_error"):
                    if get_resp.get("status_code") == 429 and rate_limit_retries < max_rate_limit_retries:
                        rate_limit_retries += 1
                        wait_sec = 2 + rate_limit_retries * 2
                        logger.info("cargo-labels/request: get 429 rate limit, retry {}/{} in {} sec", rate_limit_retries, max_rate_limit_retries, wait_sec)
                        await asyncio.sleep(wait_sec)
                        continue
                    err = get_resp.get("_error") or "Ошибка получения статуса этикеток"
                    ozon = get_resp.get("ozon_response")
                    if isinstance(ozon, dict) and ozon.get("message") and ozon.get("message") not in str(err):
                        err = err + ": " + str(ozon.get("message"))
                    if get_resp.get("status_code") == 429:
                        err = "Слишком много запросов к Ozon. Попробуйте через минуту."
                    logger.warning("cargo-labels/request: get failed operation_id={} err={}", operation_id[:20], err)
                    cargoes_with_status[cargo_index]["status"] = "FAILED"
                    return JSONResponse(status_code=502, content={"ok": False, "error": err, "cargoes": cargoes_with_status, "ozon_response": ozon})
                rate_limit_retries = 0
                result = get_resp.get("result") if isinstance(get_resp.get("result"), dict) else {}
                status = (get_resp.get("status") or result.get("status") or result.get("state") or "").upper()
                if status in ("FAILED", "ERROR"):
                    cargoes_with_status[cargo_index]["status"] = status
                    err_msg = get_resp.get("error") or result.get("error") or status
                    return JSONResponse(status_code=502, content={"ok": False, "error": "Создание этикеток: " + str(err_msg), "cargoes": cargoes_with_status})
                if status == "SUCCESS":
                    file_guid_for_cargo = (result.get("file_guid") or "").strip() if isinstance(result, dict) else ""
                    file_url_for_cargo = (result.get("file_url") or "").strip() if isinstance(result, dict) else ""
                    file_guid_for_cargo = str(file_guid_for_cargo) if file_guid_for_cargo else None
                    file_url_for_cargo = str(file_url_for_cargo) if file_url_for_cargo else None
                    break
            if file_guid_for_cargo:
                cargoes_with_status[cargo_index]["status"] = "SUCCESS"
                cargoes_with_status[cargo_index]["file_guid"] = file_guid_for_cargo
                cargoes_with_status[cargo_index]["file_url"] = file_url_for_cargo
                pdf_bytes, pdf_err = await client.cargoes_label_file(file_guid_for_cargo)
                if (not pdf_bytes) and file_url_for_cargo:
                    try:
                        import httpx

                        async with httpx.AsyncClient(timeout=60.0) as http:
                            resp = await http.get(file_url_for_cargo)
                            if resp.status_code == 200 and resp.content:
                                pdf_bytes = resp.content
                                pdf_err = None
                                logger.info("cargo-labels: file_url fallback ok cargo_id={}", one_cargo_id)
                            else:
                                logger.warning(
                                    "cargo-labels: file_url fallback failed cargo_id={} status_code={}",
                                    one_cargo_id,
                                    resp.status_code,
                                )
                    except Exception as e:
                        logger.warning("cargo-labels: file_url fallback exception cargo_id={} err={}", one_cargo_id, e)
                if pdf_bytes:
                    try:
                        CARGO_LABELS_DIR.mkdir(parents=True, exist_ok=True)
                        # Важно: файл сохраняем под тем же supply_id, который передан в URL,
                        # чтобы печать могла работать без наличия записи в БД.
                        path = CARGO_LABELS_DIR / f"{supply_id}_{one_cargo_id}.pdf"
                        path.write_bytes(pdf_bytes)
                        logger.info("cargo-labels: сохранён {}", path.name)
                    except OSError as e:
                        logger.warning("cargo-labels: не удалось сохранить PDF {}: {}", one_cargo_id, e)
                else:
                    logger.warning("cargo-labels: не удалось загрузить PDF для cargo_id={}: {}", one_cargo_id, pdf_err)
            elif file_url_for_cargo:
                # Если file_guid не пришёл, но file_url есть — сохраним по file_url напрямую (редко, но бывает).
                cargoes_with_status[cargo_index]["status"] = "SUCCESS"
                cargoes_with_status[cargo_index]["file_url"] = file_url_for_cargo
                try:
                    import httpx

                    async with httpx.AsyncClient(timeout=60.0) as http:
                        resp = await http.get(file_url_for_cargo)
                        if resp.status_code == 200 and resp.content:
                            CARGO_LABELS_DIR.mkdir(parents=True, exist_ok=True)
                            path = CARGO_LABELS_DIR / f"{supply_id}_{one_cargo_id}.pdf"
                            path.write_bytes(resp.content)
                            logger.info("cargo-labels: сохранён from file_url {}", path.name)
                        else:
                            logger.warning(
                                "cargo-labels: file_url fetch failed cargo_id={} status_code={}",
                                one_cargo_id,
                                resp.status_code,
                            )
                except Exception as e:
                    logger.warning("cargo-labels: file_url fetch exception cargo_id={} err={}", one_cargo_id, e)
            else:
                cargoes_with_status[cargo_index]["status"] = "FAILED"
        _cleanup_old_cargo_labels()
        return JSONResponse(content={"ok": True, "cargoes": cargoes_with_status})
    except Exception as e:
        logger.warning("api_supplies_cargo_labels_request: %s", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.get("/api/supplies/{supply_id}/cargo-label-file")
async def api_supplies_cargo_label_file(
    supply_id: int,
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Скачать PDF с этикетками грузомест. Query: file_guid=... (из cargo-labels/request)."""
    file_guid = (request.query_params.get("file_guid") or "").strip()
    if not file_guid:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен file_guid"})
    try:
        supply_id_str = str(supply_id).strip()
        r = await db.execute(
            select(OzonSupply).where(
                or_(
                    OzonSupply.id == supply_id,
                    OzonSupply.posting_number == supply_id_str,
                    OzonSupply.ozon_supply_id == supply_id_str,
                )
            )
        )
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Поставка не найдена"})
        client = OzonAPIClient()
        pdf_bytes, err = await client.cargoes_label_file(file_guid)
        if err or not pdf_bytes:
            return JSONResponse(status_code=502, content={"ok": False, "error": err or "Не удалось загрузить PDF"})
        return Response(content=pdf_bytes, media_type="application/pdf", headers={"Content-Disposition": 'attachment; filename="cargo-labels.pdf"'})
    except Exception as e:
        logger.warning("api_supplies_cargo_label_file: %s", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


def _cargo_label_pdf_path(supply_id: int, cargo_id: int) -> Optional[Path]:
    """Путь к сохранённому PDF ШК грузоместа или None."""
    p = CARGO_LABELS_DIR / f"{supply_id}_{cargo_id}.pdf"
    return p if p.is_file() else None


@router.get("/supply-queue/cargo-label-pdf/{supply_id}/{cargo_id}")
async def supply_queue_cargo_label_pdf(
    supply_id: int,
    cargo_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Отдать сохранённый PDF ШК грузоместа (для встраивания в iframe)."""
    path = _cargo_label_pdf_path(supply_id, cargo_id)
    if not path:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Файл ШК грузоместа не найден. Сгенерируйте этикетки заново."})
    return FileResponse(
        path=str(path),
        media_type="application/pdf",
        filename=f"cargo-{cargo_id}.pdf",
        headers={"Content-Disposition": 'inline; filename="cargo-' + str(cargo_id) + '.pdf"'},
    )


@router.get("/supply-queue/print/cargo-label/{supply_id}/{cargo_id}", response_class=HTMLResponse)
async def supply_queue_print_cargo_label(
    supply_id: int,
    cargo_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Страница с PDF в iframe и автоматическим открытием окна печати."""
    if not _cargo_label_pdf_path(supply_id, cargo_id):
        return HTMLResponse(content="<html><body>Файл ШК грузоместа не найден. Сгенерируйте этикетки заново.</body></html>", status_code=404)
    pdf_url = f"/supply-queue/cargo-label-pdf/{supply_id}/{cargo_id}#toolbar=0&navpanes=0&scrollbar=0&view=FitH"
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Печать ШК грузоместа</title>
<style>
@media print {{
  @page {{ size: 120mm 75mm; margin: 0; }}
  html, body {{ margin: 0; padding: 0; width: 120mm; overflow: hidden; }}
  .cargo-copy {{ page-break-after: always; width: 120mm; height: 75mm; overflow: hidden; }}
  .cargo-copy:last-child {{ page-break-after: auto; }}
  .cargo-copy iframe {{ width: 120mm; height: 75mm; border: 0; }}
}}
</style></head>
<body style="margin:0;">
<div class="cargo-copy"><iframe src="{pdf_url}" style="width:120mm;height:75mm;border:0;"></iframe></div>
<div class="cargo-copy"><iframe src="{pdf_url}" style="width:120mm;height:75mm;border:0;"></iframe></div>
<script>
(function() {{
  var printed = false;
  var frames = Array.from(document.querySelectorAll("iframe"));
  var loaded = 0;
  function doPrint() {{
    if (printed) return;
    printed = true;
    window.print();
  }}
  frames.forEach(function(frame) {{
    frame.addEventListener("load", function() {{
      loaded += 1;
      if (loaded >= frames.length) {{
        setTimeout(doPrint, 500);
      }}
    }});
  }});
  // fail-safe: даже если load не пришел, не зависать бесконечно
  setTimeout(doPrint, 4500);
}})();
</script></body></html>"""
    return HTMLResponse(content=html)


@router.get("/supply-queue/print/supply/{supply_id}", response_class=HTMLResponse)
async def supply_queue_print_supply(
    request: Request,
    supply_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Страница для печати ШК поставки (по умолчанию 2 экземпляра). Параметр ?copies=N запоминается в localStorage на фронте."""
    from fastapi.responses import HTMLResponse as HTMLResp
    r = await db.execute(select(OzonSupply).where(OzonSupply.id == supply_id))
    supply = r.scalar_one_or_none()
    if not supply:
        return HTMLResp(content="<html><body>Поставка не найдена.</body></html>", status_code=404)
    try:
        copies = int(request.query_params.get("copies", 2))
        copies = max(1, min(copies, 10))
    except (TypeError, ValueError):
        copies = 2
    qr_data = _qr_data_url(getattr(supply, "posting_number", None) or supply.ozon_supply_id or str(supply.id))
    return templates.TemplateResponse(
        "supply_queue_print_supply.html",
        {"request": request, "supply": supply, "copies": copies, "qr_data_url": qr_data},
    )


@router.get("/supply-queue/print/sku/{supply_id}", response_class=HTMLResponse)
async def supply_queue_print_sku(
    request: Request,
    supply_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Страница для печати ШК SKU. Без параметров — по всей поставке.
    С параметром cargo_index (индекс грузоместа 0,1,2...) — товары этого грузоместа (данные из БД).
    Либо с параметром items (JSON) — для обратной совместимости.
    """
    from fastapi.responses import HTMLResponse as HTMLResp

    async def _build_print_composition_from_cargo_items(
        cargo_items_raw: list,
        full_composition_raw: list,
    ) -> tuple[list, dict]:
        """Собрать composition для шаблона печати по items грузоместа."""
        cargo_items = cargo_items_raw if isinstance(cargo_items_raw, list) else []
        full_composition = full_composition_raw if isinstance(full_composition_raw, list) else []

        sku_to_full = {
            str(c.get("sku")): c
            for c in full_composition
            if isinstance(c, dict) and c.get("sku") is not None
        }
        article_to_full = {}
        for c in full_composition:
            if not isinstance(c, dict):
                continue
            art = str(c.get("article") or c.get("offer_id") or c.get("barcode") or "").strip()
            if art:
                article_to_full[art] = c

        normalized_rows: list[dict] = []
        article_keys: list[str] = []
        for it in cargo_items:
            if not isinstance(it, dict):
                continue
            try:
                qty = int(it.get("quantity") or it.get("quant") or 0)
            except (TypeError, ValueError):
                qty = 0
            if qty <= 0:
                continue
            sku_val = it.get("sku")
            offer_id = str(it.get("offer_id") or it.get("article") or "").strip()
            barcode = str(it.get("barcode") or "").strip()
            normalized_rows.append(
                {
                    "sku_raw": sku_val,
                    "qty": qty,
                    "offer_id": offer_id,
                    "barcode": barcode,
                }
            )
            if offer_id:
                article_keys.append(offer_id)
            if barcode:
                article_keys.append(barcode)

        products_by_article: dict[str, Product] = {}
        if article_keys:
            try:
                r_pa = await db.execute(
                    select(Product).where(Product.article.in_(list(dict.fromkeys(article_keys))[:500]))
                )
                for p in r_pa.scalars().all():
                    art = str(getattr(p, "article", "") or "").strip()
                    if art:
                        products_by_article[art] = p
            except Exception:
                products_by_article = {}

        composition: list[dict] = []
        product_ids: set[int] = set()
        for row in normalized_rows:
            sku_raw = row["sku_raw"]
            qty = row["qty"]
            offer_id = row["offer_id"]
            barcode = row["barcode"]
            sku_int = 0
            if sku_raw is not None:
                try:
                    sku_int = int(sku_raw)
                except (TypeError, ValueError):
                    sku_int = 0

            full = None
            if sku_int > 0:
                full = sku_to_full.get(str(sku_int))
            if not full and offer_id:
                full = article_to_full.get(offer_id)
            if not full and barcode:
                full = article_to_full.get(barcode)

            product = None
            if full and full.get("product_id"):
                try:
                    pid = int(full.get("product_id"))
                    if pid > 0:
                        product_ids.add(pid)
                except (TypeError, ValueError):
                    pid = None
            else:
                pid = None

            if not pid:
                product = products_by_article.get(offer_id) or products_by_article.get(barcode)
                if product:
                    pid = int(product.id)
                    product_ids.add(pid)

            composition.append(
                {
                    "product_id": pid,
                    "product_name": (
                        (full.get("product_name") if isinstance(full, dict) else None)
                        or (getattr(product, "name", None) if product else None)
                        or "—"
                    ),
                    "sku": sku_int if sku_int > 0 else (offer_id or barcode or "—"),
                    "quantity": qty,
                }
            )

        products = {}
        if product_ids:
            r_pr = await db.execute(select(Product).where(Product.id.in_(list(product_ids))))
            products = {p.id: p for p in r_pr.scalars().all()}
        return composition, products

    supply_id_str = str(supply_id).strip()
    r = await db.execute(
        select(OzonSupply).where(
            or_(
                OzonSupply.id == supply_id,
                OzonSupply.posting_number == supply_id_str,
                OzonSupply.ozon_supply_id == supply_id_str,
            )
        )
    )
    supply = r.scalar_one_or_none()
    cargo_index_param = request.query_params.get("cargo_index")
    items_param = request.query_params.get("items")

    # Если поставка не найдена в БД — не падаем, а берём грузоместа из Ozon по posting_number.
    if not supply:
        try:
            from types import SimpleNamespace
            client = OzonAPIClient()
            cargo_get_resp = await client.get_cargoes_by_supply_ids([supply_id_str])
            if cargo_get_resp.get("_error"):
                return HTMLResp(
                    content=f"<html><body>Ошибка получения грузомест из Ozon: {cargo_get_resp.get('_error')}</body></html>",
                    status_code=502,
                )
            cargoes = _parse_cargoes_from_get_response(cargo_get_resp, None)
            if cargo_index_param is not None:
                try:
                    cargo_index = int(cargo_index_param)
                except (TypeError, ValueError):
                    cargo_index = -1
                cargo_items = cargoes[cargo_index].get("items", []) if 0 <= cargo_index < len(cargoes) else []
            elif items_param is not None:
                # items param legacy: ожидаем JSON массив {sku, quantity}
                try:
                    cargo_items = _json.loads(items_param)
                except Exception:
                    cargo_items = []
                if isinstance(cargo_items, list):
                    # Нормализуем в формат {sku, quantity}
                    cargo_items_norm = []
                    for it in cargo_items:
                        if not isinstance(it, dict):
                            continue
                        sku = it.get("sku")
                        qty = it.get("quantity")
                        if sku is None:
                            continue
                        try:
                            sku_i = int(sku)
                            qty_i = int(qty or 1)
                        except (TypeError, ValueError):
                            continue
                        if sku_i > 0 and qty_i > 0:
                            cargo_items_norm.append({"sku": sku_i, "quantity": qty_i})
                    cargo_items = cargo_items_norm
                else:
                    cargo_items = []
            else:
                cargo_items = []
                for c in cargoes:
                    if isinstance(c, dict):
                        cargo_items.extend(c.get("items", []) or [])
            composition, products = await _build_print_composition_from_cargo_items(cargo_items, [])

            dummy_supply = SimpleNamespace(ozon_supply_id=supply_id_str, id=supply_id)
            return templates.TemplateResponse(
                "supply_queue_print_sku.html",
                {"request": request, "supply": dummy_supply, "composition": composition, "products": products},
            )
        except Exception:
            return HTMLResp(content="<html><body>Поставка не найдена.</body></html>", status_code=404)

    # Если поставка нашлась — работаем по данным из БД (существующая логика).
    full_composition = supply.composition if isinstance(supply.composition, list) else []
    composition = full_composition

    if cargo_index_param is not None:
        try:
            cargo_index = int(cargo_index_param)
        except (TypeError, ValueError):
            cargo_index = -1
        if cargo_index >= 0:
            raw = getattr(supply, "cargo_places_data", None)
            data = raw if raw is not None else []
            if isinstance(data, bytes):
                try:
                    data = data.decode("utf-8", errors="replace")
                except Exception:
                    data = "[]"
            if isinstance(data, str) and (data or "").strip():
                try:
                    data = _json.loads(data)
                except Exception:
                    data = []
            if not isinstance(data, list):
                data = []
            if cargo_index < len(data):
                place = data[cargo_index]
                if isinstance(place, dict):
                    place_items = place.get("items") or (place.get("value") or {}).get("items") or []
                    if place_items:
                        composition, products = await _build_print_composition_from_cargo_items(place_items, full_composition)
            else:
                composition = []
        else:
            composition = []
    else:
        if items_param is not None:
            try:
                cargo_items = _json.loads(items_param)
            except Exception:
                cargo_items = []
            if not isinstance(cargo_items, list):
                cargo_items = []
            if cargo_items:
                sku_to_full = {str(c.get("sku")): c for c in full_composition if isinstance(c, dict) and c.get("sku") is not None}
                composition = []
                for it in cargo_items:
                    if not isinstance(it, dict):
                        continue
                    sku = it.get("sku")
                    if sku is None:
                        continue
                    try:
                        sku_int = int(sku)
                    except (TypeError, ValueError):
                        sku_int = sku
                    qty = int(it.get("quantity") or 1)
                    if qty < 1:
                        continue
                    full = sku_to_full.get(str(sku)) or sku_to_full.get(str(sku_int))
                    if full:
                        composition.append({
                            "product_id": full.get("product_id"),
                            "product_name": full.get("product_name"),
                            "sku": sku_int,
                            "quantity": qty,
                        })
                    else:
                        composition.append({"product_id": None, "product_name": "—", "sku": sku_int, "quantity": qty})
            else:
                composition = []
        else:
            composition = full_composition
    product_ids = [c.get("product_id") for c in composition if c.get("product_id")]
    products = {}
    if product_ids:
        r_pr = await db.execute(select(Product).where(Product.id.in_(product_ids)))
        products = {p.id: p for p in r_pr.scalars().all()}
    return templates.TemplateResponse(
        "supply_queue_print_sku.html",
        {"request": request, "supply": supply, "composition": composition, "products": products},
    )


@router.get("/supply-queue/print/sku-item/{sku}", response_class=HTMLResponse)
async def supply_queue_print_sku_item(
    request: Request,
    sku: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Печать ШК конкретного SKU без обращений в Ozon:
    берём Product по Product.ozon_sku (справочник админки) и печатаем его ozon_barcode_filename.
    """
    from fastapi.responses import HTMLResponse as HTMLResp
    from types import SimpleNamespace
    try:
        qty = int(request.query_params.get("qty", 1))
    except (TypeError, ValueError):
        qty = 1
    qty = max(1, min(qty, 500))
    if sku <= 0:
        return HTMLResp(content="<html><body>Некорректный SKU.</body></html>", status_code=400)

    r = await db.execute(select(Product).where(Product.ozon_sku == sku))
    product = r.scalar_one_or_none()
    if not product:
        return HTMLResp(content="<html><body>SKU не найден в справочнике товаров.</body></html>", status_code=404)

    composition = [
        {
            "product_id": product.id,
            "product_name": product.name or "—",
            "sku": int(sku),
            "quantity": qty,
        }
    ]
    products = {product.id: product}
    supply_stub = SimpleNamespace(ozon_supply_id="", id=0)
    return templates.TemplateResponse(
        "supply_queue_print_sku.html",
        {"request": request, "supply": supply_stub, "composition": composition, "products": products},
    )


async def _fetch_wb_stocks_by_article() -> list:
    """Остатки WB по артикулам: все артикулы из каталога + остатки по складам."""
    try:
        wb_client = WildberriesAPIClient()
        # Все артикулы из карточек (каталог)
        all_article_to_nmid, nmid_to_name_cards = await wb_client.get_all_articles_from_cards()
        
        # Остатки по складам из отчёта
        url = f"{wb_client.BASE_URL}/api/v1/supplier/stocks"
        date_from = "2019-01-01T00:00:00+03:00"
        by_article = {}  # article -> {"warehouses": {warehouse_name: {"stock": qty}}}
        wb_article_to_nmid = {}
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                params = {"dateFrom": date_from}
                response = await client.get(url, params=params, headers=wb_client.headers)
                response.raise_for_status()
                rows = response.json()
                if not isinstance(rows, list):
                    rows = []
                for row in rows:
                    try:
                        art = (
                            row.get("supplierArticle")
                            or row.get("supplier_article")
                            or ""
                        )
                        if not art:
                            continue
                        art = str(art).strip()
                        key = " ".join(art.split())
                        
                        # Получаем склад
                        warehouse_name = (
                            row.get("warehouseName")
                            or row.get("warehouse_name")
                            or row.get("warehouse")
                            or "Не указан"
                        )
                        warehouse_name = str(warehouse_name).strip()
                        
                        if key not in by_article:
                            by_article[key] = {"warehouses": {}}
                        
                        qty = 0
                        if row.get("quantity") is not None:
                            qty = int(row.get("quantity", 0) or 0)
                        if qty < 0:
                            qty = 0
                        
                        if warehouse_name not in by_article[key]["warehouses"]:
                            by_article[key]["warehouses"][warehouse_name] = {"stock": 0}
                        by_article[key]["warehouses"][warehouse_name]["stock"] += qty
                        
                        nmid = row.get("nmId") or row.get("nmID") or row.get("nm_id")
                        if nmid is not None and key not in wb_article_to_nmid:
                            wb_article_to_nmid[key] = int(nmid)
                    except (TypeError, ValueError, KeyError):
                        continue
                if not rows:
                    break
                date_from = (rows[-1].get("lastChangeDate") or "").strip()
                if not date_from:
                    break
        
        # Все артикулы из каталога добавляем в by_article (чтобы отображать и с нулями)
        for art, nmid in all_article_to_nmid.items():
            if art not in by_article:
                by_article[art] = {"warehouses": {}}
            if art not in wb_article_to_nmid:
                wb_article_to_nmid[art] = nmid
        
        # Наименования: из карточек + по nmid из остатков для недостающих
        nmid_to_name = dict(nmid_to_name_cards) if nmid_to_name_cards else {}
        missing_nmids = set(wb_article_to_nmid.values()) - set(nmid_to_name.keys())
        if missing_nmids:
            from_map = await wb_client.get_product_names_by_nmids(list(missing_nmids))
            nmid_to_name.update(from_map)
        
        # Формируем результат: все артикулы, включая с нулевыми остатками
        articles = []
        for article in sorted(by_article.keys()):
            article_data = by_article[article]
            warehouses_list = []
            total_stock = 0
            
            for warehouse_name in sorted(article_data["warehouses"].keys()):
                stock = article_data["warehouses"][warehouse_name]["stock"]
                total_stock += stock
                warehouses_list.append({
                    "warehouse_name": warehouse_name,
                    "stock": stock,
                })
            
            nmid = wb_article_to_nmid.get(article)
            name = nmid_to_name.get(nmid, "—") if nmid is not None else "—"
            
            articles.append({
                "article": article,
                "name": name,
                "warehouses": warehouses_list,
                "total_stock": total_stock,
            })
        
        return articles
    except Exception as e:
        logger.warning(f"Остатки WB по артикулам: {e}")
        return []


def _norm_wh_for_ws_cluster(v: str) -> str:
    v = (v or "").replace("\xa0", " ").strip().lower()
    v = v.replace("_", " ")
    v = re.sub(r"\s+", " ", v)
    return v


def _warehouse_stocks_cluster_maps(cluster_list: list) -> Tuple[Dict[int, int], Dict[str, int]]:
    """id кластера/macrolocal → macrolocal; нормализованное имя склада → macrolocal."""
    resolve: Dict[int, int] = {}
    wh_norm_to_macrolocal: Dict[str, int] = {}
    for c in cluster_list or []:
        if not isinstance(c, dict):
            continue
        mid = c.get("macrolocal_cluster_id")
        try:
            mid_i = int(mid) if mid is not None else 0
        except (TypeError, ValueError):
            mid_i = 0
        if not mid_i:
            continue
        resolve[mid_i] = mid_i
        cid = c.get("id")
        if cid is not None:
            try:
                resolve[int(cid)] = mid_i
            except (TypeError, ValueError):
                pass
        for w in c.get("warehouses") or []:
            if not isinstance(w, dict):
                continue
            wn = _norm_wh_for_ws_cluster(w.get("name") or "")
            if wn:
                wh_norm_to_macrolocal[wn] = mid_i
    return resolve, wh_norm_to_macrolocal


def _normalize_supply_state_for_ws_corner(status: Optional[str]) -> str:
    """Код статуса заявки для сопоставления с WAREHOUSE_STOCKS_SQ_* (как в supply-order/get)."""
    s = (status or "").strip()
    if not s:
        return ""
    u = s.upper().replace("-", "_")
    for _ in range(6):
        old = u
        for prefix in ("ORDER_STATE_", "SUPPLY_ORDER_STATE_", "SUPPLY_ORDER_"):
            if u.startswith(prefix):
                u = u[len(prefix) :]
                break
        if u == old:
            break
    if u in SUPPLY_ORDER_STATE_LABELS_RU:
        return u
    sl = s.strip().lower()
    for code, label in SUPPLY_ORDER_STATE_LABELS_RU.items():
        if label and (label.lower() == sl or label.lower().replace("ё", "е") == sl.replace("ё", "е")):
            return code
    return u


def _norm_ws_cluster_name_for_match(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ").strip().lower())


def _cluster_name_to_macrolocal_from_ozon_table(ozon_by_article: list) -> Dict[str, int]:
    """Имя кластера (как в таблице остатков) → macrolocal_cluster_id."""
    out: Dict[str, int] = {}
    for item in ozon_by_article or []:
        if not isinstance(item, dict):
            continue
        for cl in item.get("clusters") or []:
            if not isinstance(cl, dict):
                continue
            name = (cl.get("name") or "").strip()
            mid = cl.get("macrolocal_cluster_id")
            if not name or mid is None:
                continue
            try:
                mi = int(mid)
            except (TypeError, ValueError):
                continue
            out[_norm_ws_cluster_name_for_match(name)] = mi
    return out


def _macrolocal_from_supply_queue_cluster_line(
    line: str,
    name_to_mid: Dict[str, int],
    resolve: Dict[int, int],
) -> int | None:
    """
    Поле storage_cluster_line из снимка очереди: название кластера или числовой id
    (до enrich_cluster_names мог быть str(macrolocal)).
    """
    s = (line or "").strip()
    if not s or s == "—":
        return None
    if s.isdigit():
        try:
            v = int(s)
            return int(resolve.get(v, v))
        except (TypeError, ValueError):
            return None
    key = _norm_ws_cluster_name_for_match(s)
    mid = name_to_mid.get(key)
    if mid is not None:
        return int(mid)
    best_len = 0
    best_mid: int | None = None
    for nk, ml in name_to_mid.items():
        if not nk or ml is None:
            continue
        if nk in key or key in nk:
            if len(nk) > best_len:
                best_len = len(nk)
                try:
                    best_mid = int(ml)
                except (TypeError, ValueError):
                    continue
    return best_mid


def _macrolocal_for_warehouse_stocks_corner_row(
    row: dict,
    name_to_mid: Dict[str, int],
    resolve: Dict[int, int],
) -> int | None:
    """Сначала id кластера из заявки (в снимке), иначе — по строке кластера в таблице."""
    ml = row.get("macrolocal_cluster_id")
    if ml is not None:
        try:
            mi = int(ml)
            if mi:
                return int(resolve.get(mi, mi))
        except (TypeError, ValueError):
            pass
    return _macrolocal_from_supply_queue_cluster_line(
        str(row.get("storage_cluster_line") or ""),
        name_to_mid,
        resolve,
    )


# Статусы для уголка справа сверху (коричневый) — SUPPLY_ORDER_STATE_LABELS_RU
WAREHOUSE_STOCKS_SQ_CORNER_STATUSES = frozenset({
    "DATA_FILLING",
    "READY_TO_SUPPLY",
})
# Статусы для уголка справа снизу (синий): «в пути» / приёмка
WAREHOUSE_STOCKS_SQ_TRANSIT_CORNER_STATUSES = frozenset({
    "ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
})


def _enrich_warehouse_stocks_supply_queue_corners(ozon_by_article: list, session: dict) -> None:
    """
    Уголки из снимка сессии «Очередь поставок» (тот же, что /supply-queue).
    Коричневый: DATA_FILLING / READY_TO_SUPPLY. Синий: принята на точке отгрузки / в пути / приёмка на складе.
    Состав: cargo_units → line_items (offer_id, quantity).
    """
    for item in ozon_by_article or []:
        if not isinstance(item, dict):
            continue
        item["supply_queue_corner_total"] = 0
        item["supply_queue_transit_total"] = 0
        for cl in item.get("clusters") or []:
            if isinstance(cl, dict):
                cl["supply_queue_corner_qty"] = 0
                cl["supply_queue_transit_qty"] = 0
    if not ozon_by_article:
        return
    resolve: Dict[int, int] = dict(_stocks_cache.get("macrolocal_resolve") or {})
    name_to_mid = _cluster_name_to_macrolocal_from_ozon_table(ozon_by_article)
    snap = _supply_queue_snapshot_from_session(session)
    if not isinstance(snap, dict):
        logger.info("warehouse_stocks: снимок очереди поставок в сессии отсутствует — уголки поставок 0")
        return
    rows = _supply_queue_rows_from_session(snap.get("rows") or [])
    cc = snap.get("composition_cache")
    if isinstance(cc, dict):
        _apply_composition_cache_to_rows(rows, cc)
    sq_qty_ready: Dict[Tuple[int, str], int] = defaultdict(int)
    sq_qty_transit: Dict[Tuple[int, str], int] = defaultdict(int)
    for row in rows:
        if not isinstance(row, dict):
            continue
        st = _normalize_supply_state_for_ws_corner(
            str(row.get("state_code") or row.get("state") or row.get("state_label_ru") or "")
        )
        if st in WAREHOUSE_STOCKS_SQ_CORNER_STATUSES:
            target = sq_qty_ready
        elif st in WAREHOUSE_STOCKS_SQ_TRANSIT_CORNER_STATUSES:
            target = sq_qty_transit
        else:
            continue
        mid_opt = _macrolocal_for_warehouse_stocks_corner_row(row, name_to_mid, resolve)
        if mid_opt is None:
            continue
        mid_i = int(mid_opt)
        for cu in row.get("cargo_units") or []:
            if not isinstance(cu, dict):
                continue
            for it in cu.get("line_items") or cu.get("items") or []:
                if not isinstance(it, dict):
                    continue
                offer = str(
                    it.get("offer_id") or it.get("offerId") or it.get("item_code") or it.get("article") or ""
                ).strip()
                if not offer and it.get("sku") is not None:
                    offer = str(it.get("sku")).strip()
                if not offer:
                    continue
                try:
                    qty = int(it.get("quantity") or 0)
                except (TypeError, ValueError):
                    qty = 0
                if qty <= 0:
                    continue
                target[(mid_i, offer)] += qty

    for item in ozon_by_article:
        oid = (item.get("sku") or "").strip()
        total_ready = 0
        total_tr = 0
        for cl in item.get("clusters") or []:
            mid = cl.get("macrolocal_cluster_id")
            try:
                mid_i = int(mid) if mid is not None else None
            except (TypeError, ValueError):
                mid_i = None
            if mid_i is None:
                cl["supply_queue_corner_qty"] = 0
                cl["supply_queue_transit_qty"] = 0
            else:
                q1 = int(sq_qty_ready.get((mid_i, oid), 0))
                q2 = int(sq_qty_transit.get((mid_i, oid), 0))
                cl["supply_queue_corner_qty"] = q1
                cl["supply_queue_transit_qty"] = q2
                total_ready += q1
                total_tr += q2
        item["supply_queue_corner_total"] = int(total_ready)
        item["supply_queue_transit_total"] = int(total_tr)


async def _fetch_ozon_products_table() -> tuple[list, datetime | None]:
    """
    Таблица товаров Ozon по артикулам с остатками по кластерам.
    Возвращает (by_article, last_updated), где by_article = [
      {"product_id", "name", "sku", "total_stock", "clusters": [{"name", "stock"}, ...]},
      ...
    ].
    """
    try:
        ozon_client = OzonAPIClient()
        products = await ozon_client.get_product_list()
        if not products:
            logger.warning("Остатки Ozon: get_product_list вернул пустой список")
            return [], datetime.now(MSK)
        offer_ids = [p["offer_id"] for p in products]
        names_map = await ozon_client.get_product_names(offer_ids)
        if not names_map:
            attributes = await ozon_client.get_products_info_attributes(offer_ids)
            names_map = {oid: (attributes.get(oid) or {}).get("name") or "—" for oid in offer_ids}
        cluster_list = await ozon_client.get_cluster_list()
        res_map, wh_map = _warehouse_stocks_cluster_maps(cluster_list)
        _stocks_cache["macrolocal_resolve"] = res_map
        _stocks_cache["warehouse_norm_to_macrolocal"] = wh_map
        ozon_pid_map: Dict[int, str] = {}
        for p in products:
            opid = p.get("product_id")
            ooid = (p.get("offer_id") or "").strip()
            if opid is not None and ooid:
                try:
                    ozon_pid_map[int(opid)] = ooid
                except (TypeError, ValueError):
                    pass
        _stocks_cache["ozon_product_id_to_offer"] = ozon_pid_map
        clusters = await ozon_client.get_stocks_by_cluster(cluster_list=cluster_list)
        # По каждому артикулу: total и список кластеров с остатками
        by_article = []
        for p in products:
            oid = p["offer_id"]
            pid = p["product_id"]
            name = names_map.get(oid) or "—"
            cluster_rows = []
            total = 0
            for c in clusters:
                cluster_name = c.get("name") or "—"
                row = next((r for r in c.get("rows") or [] if (r.get("article") or "").strip() == oid), None)
                cluster_qty = int(row.get("stock") or 0) if row else 0
                total += cluster_qty
                warehouses = []
                for wh in c.get("warehouses") or []:
                    wr = next((r for r in wh.get("rows") or [] if (r.get("article") or "").strip() == oid), None)
                    wh_qty = int(wr.get("stock") or 0) if wr else 0
                    warehouses.append({"name": wh.get("name") or "—", "stock": wh_qty})
                cluster_rows.append({
                    "name": cluster_name,
                    "stock": cluster_qty,
                    "warehouses": warehouses,
                    "macrolocal_cluster_id": c.get("macrolocal_cluster_id"),
                })
            cluster_rows.sort(key=lambda x: (x.get("name") or "").lower())
            by_article.append({
                "product_id": pid,
                "name": name,
                "sku": oid,
                "total_stock": total,
                "clusters": cluster_rows,
            })
        by_article.sort(key=lambda x: (x.get("sku") or "").lower())
        return by_article, datetime.now(MSK)
    except Exception as e:
        logger.warning("Остатки Ozon (таблица товаров): %s", e, exc_info=True)
        return [], None


async def _refresh_warehouse_stocks_cache_locked() -> tuple[list, datetime | None]:
    """
    Перезагрузка кэша остатков Ozon под lock.
    Второй параллельный вызов ждёт завершения первого и затем тоже выполняет запрос —
    раньше при занятом lock обновление молча пропускалось.
    """
    async with _warehouse_stocks_refresh_lock:
        ozon_by_article, last_updated = await _fetch_ozon_products_table()
        _stocks_cache["ozon_table"] = ozon_by_article or []
        _stocks_cache["last_updated_ozon_table"] = last_updated
        logger.info(
            "warehouse_stocks cache refreshed: rows={} last_updated={}",
            len(_stocks_cache.get("ozon_table") or []),
            last_updated,
        )
        return ozon_by_article or [], last_updated


async def _background_refresh_warehouse_stocks_cache() -> None:
    """Фоновое обновление кэша /warehouse-stocks (после открытия страницы с устаревшим кэшем)."""
    try:
        await _refresh_warehouse_stocks_cache_locked()
    except Exception as e:
        logger.warning("warehouse_stocks cache refresh failed: {}", e, exc_info=True)


def _normalize_supply_status_for_ui(status: Optional[str]) -> str:
    """
    Нормализация статуса для отображения в UI.
    Нужна, чтобы SUPPLY_STATUS_LABELS и фильтры совпадали с данными ЛК.
    """
    s = (status or "").strip()
    if not s:
        return "created"
    su = s.upper()
    if su.startswith("ORDER_STATE_"):
        # Убираем префикс в любом регистре.
        s = s[len("ORDER_STATE_") :]
    if s.lower() == "created":
        return "created"
    return s.strip().upper()


def _extract_supply_status_from_info_data(info_data: dict) -> str:
    """
    Универсально извлекает статус поставки из ответа v3/supply-order/get
    (включая bulk/одиночный и разные варианты вложенности).
    """
    if not isinstance(info_data, dict):
        return ""

    preferred_keys = (
        "order_state",
        "state",
        "status",
        "orderStatus",
        "supply_state",
        "supplyStatus",
        "state_name",
        "status_name",
    )

    def _candidate(v: object) -> str:
        s = _normalize_supply_status_for_ui(str(v or "").strip())
        if not s:
            return ""
        return s

    # Быстрый путь по ожидаемым структурам.
    try:
        result = info_data.get("result") or {}
        inner = result.get("result") if isinstance(result, dict) else result
        orders = []
        if isinstance(inner, dict):
            orders = inner.get("orders") or []
        if not orders and isinstance(result, dict):
            orders = result.get("orders") or []
        if not orders:
            orders = info_data.get("orders") or []
        if isinstance(orders, dict):
            orders = [orders]
        for o in orders or []:
            if not isinstance(o, dict):
                continue
            for k in preferred_keys:
                st = _candidate(o.get(k))
                if st:
                    return st
            supplies = o.get("supplies") or []
            if isinstance(supplies, dict):
                supplies = [supplies]
            for sup in supplies or []:
                if not isinstance(sup, dict):
                    continue
                for k in preferred_keys:
                    st = _candidate(sup.get(k))
                    if st:
                        return st
    except Exception:
        pass

    # Глубокий fallback: ищем в любом вложенном dict/list.
    stack: list[object] = [info_data]
    seen_ids: set[int] = set()
    while stack:
        node = stack.pop()
        nid = id(node)
        if nid in seen_ids:
            continue
        seen_ids.add(nid)
        if isinstance(node, dict):
            for k in preferred_keys:
                if k in node:
                    st = _candidate(node.get(k))
                    if st:
                        return st
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return ""


@router.get("/warehouse-stocks")
async def warehouse_stocks(
    user: User = Depends(verify_site_user),
):
    """Раздел перенесён: матрица остатков — в спойлере на странице «Очередь поставок»."""
    return RedirectResponse(url="/supply-queue", status_code=302)


@router.post("/api/warehouse-stocks/refresh")
async def api_warehouse_stocks_refresh(
    user: User = Depends(verify_site_user),
):
    """Обновляет кэш остатков Ozon в этом запросе; после ответа клиент может перезагрузить страницу."""
    try:
        rows, last_updated = await _refresh_warehouse_stocks_cache_locked()
        return JSONResponse(
            content={
                "ok": True,
                "last_updated": last_updated.isoformat() if last_updated and hasattr(last_updated, "isoformat") else None,
                "rows_count": len(rows) if isinstance(rows, list) else 0,
            }
        )
    except Exception as e:
        logger.warning("api/warehouse-stocks/refresh: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.get("/api/warehouse-stocks/refresh-status")
async def api_warehouse_stocks_refresh_status(
    user: User = Depends(verify_site_user),
):
    """Статус фонового обновления /warehouse-stocks (для спиннера в UI)."""
    last_updated = _stocks_cache.get("last_updated_ozon_table")
    rows = _stocks_cache.get("ozon_table") or []
    return JSONResponse(
        content={
            "ok": True,
            "refresh_in_progress": _warehouse_stocks_refresh_lock.locked(),
            "last_updated": last_updated.isoformat() if hasattr(last_updated, "isoformat") else None,
            "rows_count": len(rows) if isinstance(rows, list) else 0,
        }
    )


# ——— Склад (раздел: Материалы, Напечатанные детали, Сборка, Собранные изделия) ———


async def _printed_part_names_for_stock_log(db: AsyncSession, stock: PrintedPartStock) -> tuple[str, str]:
    part_res = await db.execute(select(Part).where(Part.id == stock.part_id))
    part = part_res.scalar_one_or_none()
    part_name = ((part.name or "").strip() if part else "") or "—"
    mat_name = "—"
    if stock.material_id:
        m_res = await db.execute(select(Material).where(Material.id == stock.material_id))
        m = m_res.scalar_one_or_none()
        if m:
            mat_name = _material_name_without_weight(m.name) or "—"
    return part_name, mat_name


async def _purge_printed_part_stock_log_older_than_3_months(db: AsyncSession) -> None:
    cutoff_date = datetime.now(MSK).date() - relativedelta(months=3)
    cutoff = datetime.combine(cutoff_date, datetime.min.time()).replace(tzinfo=MSK).astimezone(timezone.utc)
    await db.execute(delete(PrintedPartStockLog).where(PrintedPartStockLog.created_at < cutoff))


async def _log_printed_part_stock_change(
    db: AsyncSession,
    stock: PrintedPartStock,
    change_kind: str,
    qty: int,
) -> None:
    q = max(1, int(qty or 0))
    part_name, mat_name = await _printed_part_names_for_stock_log(db, stock)
    db.add(
        PrintedPartStockLog(
            printed_stock_id=stock.id,
            part_id=stock.part_id,
            material_id=stock.material_id,
            part_name=(part_name[:256] if len(part_name) > 256 else part_name),
            material_name=(mat_name[:256] if len(mat_name) > 256 else mat_name),
            change_kind=change_kind,
            quantity=q,
        )
    )
    await _purge_printed_part_stock_log_older_than_3_months(db)


@router.get("/warehouse", response_class=HTMLResponse)
async def warehouse_page(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Раздел «Склад»: вкладки Материалы (филамент + доп. материалы), Напечатанные детали, Сборка, Собранные изделия."""
    tab = request.query_params.get("tab") or "materials"
    if tab not in ("materials", "printed", "assembly", "assembled"):
        logger.warning("warehouse: неизвестный tab={!r}, подставляю materials", tab)
        tab = "materials"
    mat_sub = request.query_params.get("mat_sub") or "filament"
    if mat_sub not in ("filament", "extras", "written_off"):
        mat_sub = "filament"
    spools_list = []
    materials_for_spools = []
    printers_list = []
    printer_by_spool_id = {}
    color_hex_map = {}
    extra_materials_list = []  # справочник для выбора при оприходовании
    warehouse_extras_list = []  # оприходованные: [{extra_material, stock_row, quantity}, ...]
    warehouse_extra_ids_on_stock = []
    written_off_list = []  # списанные материалы (катушки и доп.)
    printed_parts_rows = []  # список складируемых напечатанных деталей для таблицы склада
    parts_for_printed = []
    materials_for_printed = []
    materials_for_printed_options = []
    color_hex_map = {}
    products_for_assembly = []
    assembly_batches_rows = []
    assembly_capacity_rows = []
    assembled_stock_rows = []
    assembled_log_rows = []
    products_for_assembled_tab = []
    if tab == "materials":
        try:
            result_c = await db.execute(select(Color))
            colors_list = result_c.scalars().all()
            color_hex_map = {c.name: (c.hex or "#000000") for c in colors_list}
            result_m = await db.execute(select(Material).order_by(Material.name, Material.color))
            materials_for_spools = [
                {"id": m.id, "name": m.name, "color": m.color or "", "hex": color_hex_map.get((m.color or "").strip(), "#888888")}
                for m in result_m.scalars().all()
            ]
            result_sp = await db.execute(
                select(Spool, Material)
                .select_from(Spool)
                .outerjoin(Material, Spool.material_id == Material.id)
                .order_by(Spool.id)
            )
            spools_list = []
            for s, m in result_sp.all():
                mat_hex = color_hex_map.get((m.color or "").strip(), "#888888") if m and (m.color or "").strip() else "#888888"
                rem = float(s.remaining_length_m or 0)
                icon = "reach" if rem >= 250 else ("midi" if rem >= 50 else "poor")
                plastic = (getattr(m, "plastic_type", None) or "").strip() if m else ""
                dataurl = _spool_svg_dataurl(mat_hex, icon, size=48, plastic_type=plastic or None)
                spools_list.append({"spool": s, "material": m, "spool_dataurl": dataurl})
            result_prn = await db.execute(select(Printer).order_by(Printer.number, Printer.name))
            printers_list = result_prn.scalars().all()
            for p in printers_list:
                sid = getattr(p, "current_spool_id", None)
                if sid is not None:
                    printer_by_spool_id[sid] = p
            # Дополнительные материалы: справочник и остатки на складе
            result_em = await db.execute(select(ExtraMaterial).order_by(ExtraMaterial.name))
            extra_materials_list = result_em.scalars().all()
            result_stock = await db.execute(
                select(WarehouseExtraStock, ExtraMaterial)
                .select_from(WarehouseExtraStock)
                .join(ExtraMaterial, WarehouseExtraStock.extra_material_id == ExtraMaterial.id)
                .where(WarehouseExtraStock.quantity > 0)
                .order_by(ExtraMaterial.name)
            )
            warehouse_extras_list = [
                {"extra_material": em, "stock": st, "quantity": st.quantity}
                for st, em in result_stock.all()
            ]
            warehouse_extra_ids_on_stock = [item["extra_material"].id for item in warehouse_extras_list]
            # Списанные материалы (все записи, сортировка по дате списания)
            result_wo = await db.execute(
                select(WrittenOffMaterial).order_by(WrittenOffMaterial.written_off_at.desc())
            )
            written_off_list = result_wo.scalars().all()
        except Exception as e:
            logger.warning("warehouse materials load: {}", e)
    elif tab == "printed":
        try:
            result_c = await db.execute(select(Color))
            colors_list = result_c.scalars().all()
            color_hex_map = {c.name: (c.hex or "#888888") for c in colors_list}

            result_parts = await db.execute(select(Part).order_by(Part.name))
            parts_for_printed = result_parts.scalars().all()

            result_mats = await db.execute(select(Material).order_by(Material.name, Material.color))
            materials_for_printed = result_mats.scalars().all()
            materials_for_printed_options = [
                {
                    "id": m.id,
                    "name": _material_name_without_weight(m.name),
                    "color": (m.color or "").strip(),
                }
                for m in materials_for_printed
            ]

            result_rows = await db.execute(
                select(PrintedPartStock, Part, Material)
                .select_from(PrintedPartStock)
                .join(Part, PrintedPartStock.part_id == Part.id)
                .outerjoin(Material, PrintedPartStock.material_id == Material.id)
                .order_by(Part.name, Material.name, Material.color)
            )
            for stock_row, part_obj, material_obj in result_rows.all():
                material_name = "—"
                if material_obj:
                    mat_name = _material_name_without_weight(material_obj.name)
                    material_name = mat_name or "—"
                printed_parts_rows.append({
                    "stock_id": stock_row.id,
                    "part": part_obj,
                    "material": material_obj,
                    "material_name": material_name,
                    "material_hex": color_hex_map.get((material_obj.color or "").strip(), "#888888") if material_obj else "#888888",
                    "quantity": int(stock_row.quantity or 0),
                })
        except Exception as e:
            logger.warning("warehouse printed load: {}", e)
    elif tab == "assembly":
        try:
            result_p = await db.execute(select(Product).order_by(Product.name, Product.article))
            products_for_assembly = result_p.scalars().all()
            stock_map: dict[tuple[int, int | None], int] = defaultdict(int)
            stk_all = await db.execute(select(PrintedPartStock))
            for st_row in stk_all.scalars().all():
                pk = (int(st_row.part_id), int(st_row.material_id) if st_row.material_id is not None else None)
                stock_map[pk] = int(st_row.quantity or 0)
            pp_all = await db.execute(select(ProductPart))
            parts_by_product: dict[int, list] = defaultdict(list)
            for pp in pp_all.scalars().all():
                parts_by_product[int(pp.product_id)].append(pp)
            for prod in products_for_assembly:
                pps = parts_by_product.get(int(prod.id), [])
                max_qty, no_bom = _assembly_max_buildable_from_stock_map(pps, stock_map)
                assembly_capacity_rows.append({"product": prod, "max_qty": max_qty, "no_bom": no_bom})
            assembly_capacity_rows.sort(
                key=lambda r: (
                    -(r["max_qty"] or 0),
                    (r["product"].article or "").strip(),
                    (r["product"].name or "").strip(),
                )
            )
            result_batches = await db.execute(
                select(WarehouseAssemblyBatch)
                .where(WarehouseAssemblyBatch.deleted_at.is_(None))
                .order_by(WarehouseAssemblyBatch.id.desc())
            )
            batches = result_batches.scalars().all()
            for batch in batches:
                items_res = await db.execute(
                    select(WarehouseAssemblyBatchItem, Product)
                    .join(Product, WarehouseAssemblyBatchItem.product_id == Product.id)
                    .where(WarehouseAssemblyBatchItem.batch_id == batch.id)
                    .order_by(Product.name)
                )
                items = []
                line_items = []
                for bi, prod in items_res.all():
                    items.append({"item": bi, "product": prod})
                    label = ((prod.article or "").strip() + " " if (prod.article or "").strip() else "") + (prod.name or "—")
                    line_items.append(
                        {
                            "name": label.strip(),
                            "article": (prod.article or "").strip(),
                            "qty": int(bi.quantity or 0),
                        }
                    )
                assembly_batches_rows.append({
                    "batch": batch,
                    "items": items,
                    "line_items": line_items,
                    "created_at_label": _format_dt_as_msk(getattr(batch, "created_at", None)) or "—",
                })
        except Exception as e:
            logger.warning("warehouse assembly load: {}", e)
    elif tab == "assembled":
        try:
            result_p = await db.execute(select(Product).order_by(Product.name, Product.article))
            products_for_assembled_tab = result_p.scalars().all()
            res_st = await db.execute(
                select(AssembledProductStock, Product)
                .join(Product, AssembledProductStock.product_id == Product.id)
                .order_by(Product.name, Product.article)
            )
            assembled_stock_rows = [{"stock": st, "product": p} for st, p in res_st.all()]
            res_log = await db.execute(
                select(AssembledProductStockLog).order_by(AssembledProductStockLog.id.desc()).limit(200)
            )
            for lg in res_log.scalars().all():
                dt = lg.created_at
                if dt is None:
                    dt = datetime.now(timezone.utc)
                if getattr(dt, "tzinfo", None) is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dl, tl = _assembled_log_datetime_parts(dt)
                assembled_log_rows.append(
                    {
                        "log": lg,
                        "date_label": dl,
                        "time_label": tl,
                        "action_label": ASSEMBLED_ACTION_LABELS_RU.get(lg.action_kind, lg.action_kind or ""),
                    }
                )
        except Exception as e:
            logger.warning("warehouse assembled load: {}", e)
    response = templates.TemplateResponse(
        "warehouse.html",
        {
            "request": request,
            "site_username": user.username,
            "initial_tab": tab,
            "mat_sub": mat_sub,
            "spools_list": spools_list,
            "materials_for_spools": materials_for_spools,
            "printers_list": printers_list,
            "printer_by_spool_id": printer_by_spool_id,
            "color_hex_map": color_hex_map,
            "extra_materials_list": extra_materials_list,
            "warehouse_extras_list": warehouse_extras_list,
            "warehouse_extra_ids_on_stock": warehouse_extra_ids_on_stock if tab == "materials" else [],
            "written_off_list": written_off_list if tab == "materials" else [],
            "printed_parts_rows": printed_parts_rows if tab == "printed" else [],
            "parts_for_printed": parts_for_printed if tab == "printed" else [],
            "materials_for_printed": materials_for_printed if tab == "printed" else [],
            "materials_for_printed_options": materials_for_printed_options if tab == "printed" else [],
            "products_for_assembly": products_for_assembly if tab == "assembly" else [],
            "assembly_batches_rows": assembly_batches_rows if tab == "assembly" else [],
            "assembly_capacity_rows": assembly_capacity_rows if tab == "assembly" else [],
            "assembled_stock_rows": assembled_stock_rows if tab == "assembled" else [],
            "assembled_log_rows": assembled_log_rows if tab == "assembled" else [],
            "products_for_assembled_tab": products_for_assembled_tab if tab == "assembled" else [],
        },
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


def _assembly_max_buildable_from_stock_map(
    pps: list[ProductPart],
    stock_map: dict[tuple[int, int | None], int],
) -> tuple[int, bool]:
    """Сколько полных изделий можно собрать из карты остатков напечатанных деталей. (max, no_bom_lines)."""
    if not pps:
        return 0, True
    limits: list[int] = []
    for pp in pps:
        per = int(pp.quantity or 0)
        if per <= 0:
            continue
        mid = int(pp.material_id) if pp.material_id is not None else None
        sk = int(stock_map.get((int(pp.part_id), mid), 0))
        limits.append(sk // per)
    if not limits:
        return 0, True
    return min(limits), False


async def _printed_part_stock_row_for_part_material(
    db: AsyncSession,
    part_id: int,
    material_id: int | None,
) -> PrintedPartStock | None:
    """Строка остатка напечатанной детали (part_id + material_id), если есть."""
    q_st = select(PrintedPartStock).where(PrintedPartStock.part_id == part_id)
    if material_id is not None:
        q_st = q_st.where(PrintedPartStock.material_id == material_id)
    else:
        q_st = q_st.where(PrintedPartStock.material_id.is_(None))
    r = await db.execute(q_st)
    return r.scalar_one_or_none()


async def _ensure_printed_part_stock_row(
    db: AsyncSession,
    part_id: int,
    material_id: int | None,
) -> PrintedPartStock:
    """Строка остатка: существующая или новая с нулевым количеством (для возврата после удаления партии)."""
    st = await _printed_part_stock_row_for_part_material(db, part_id, material_id)
    if st:
        return st
    st = PrintedPartStock(part_id=part_id, material_id=material_id, quantity=0)
    db.add(st)
    await db.flush()
    return st


async def _assembly_batch_aggregate_printed_needs(
    db: AsyncSession,
    lines: list[tuple[int, int]],
) -> tuple[dict[tuple[int, int | None], int] | None, str | None]:
    """По строкам (product_id, qty изделий) считает потребность в напечатанных деталях (part_id, material_id) -> шт."""
    needs: dict[tuple[int, int | None], int] = defaultdict(int)
    for product_id, prod_qty in lines:
        if prod_qty <= 0:
            continue
        res_pp = await db.execute(select(ProductPart).where(ProductPart.product_id == product_id))
        pps = res_pp.scalars().all()
        if not pps:
            return None, "no_bom"
        pos = False
        for pp in pps:
            per = int(pp.quantity or 0)
            if per <= 0:
                continue
            pos = True
            mid = int(pp.material_id) if pp.material_id is not None else None
            needs[(int(pp.part_id), mid)] += per * prod_qty
        if not pos:
            return None, "no_bom"
    return dict(needs), None


async def _assembly_return_printed_parts_for_lines(
    db: AsyncSession,
    lines: list[tuple[int, int]],
) -> str | None:
    """Вернуть напечатанные детали на склад по списку (product_id, количество изделий). Ошибка: no_bom."""
    if not lines:
        return None
    needs, err = await _assembly_batch_aggregate_printed_needs(db, lines)
    if err or needs is None:
        return err or "no_bom"
    for (part_id, mat_id), qty in needs.items():
        if qty <= 0:
            continue
        st = await _ensure_printed_part_stock_row(db, part_id, mat_id)
        st.quantity = int(st.quantity or 0) + qty
        await _log_printed_part_stock_change(db, st, "assembly_return", qty)
    return None


async def _ensure_assembled_product_stock_row(db: AsyncSession, product_id: int) -> AssembledProductStock:
    r = await db.execute(select(AssembledProductStock).where(AssembledProductStock.product_id == product_id))
    row = r.scalar_one_or_none()
    if row:
        return row
    row = AssembledProductStock(product_id=product_id, quantity=0)
    db.add(row)
    await db.flush()
    return row


async def _log_assembled_product_stock_entry(
    db: AsyncSession,
    *,
    product: Product,
    delta_qty: int,
    action_kind: str,
    assembly_batch_id: int | None = None,
) -> None:
    db.add(
        AssembledProductStockLog(
            product_id=product.id,
            product_label=_product_assembled_label(product)[:512],
            delta_qty=delta_qty,
            action_kind=action_kind,
            assembly_batch_id=assembly_batch_id,
        )
    )


async def _apply_warehouse_assembly_batch_completion_to_assembled(
    db: AsyncSession,
    batch: WarehouseAssemblyBatch,
) -> str | None:
    """Один раз при переводе партии в «Выполнена»: группировка по изделиям, приход на склад собранных."""
    if getattr(batch, "assembled_output_at", None) is not None:
        return None
    res_items = await db.execute(
        select(WarehouseAssemblyBatchItem).where(WarehouseAssemblyBatchItem.batch_id == batch.id)
    )
    items = res_items.scalars().all()
    sums: dict[int, int] = defaultdict(int)
    for it in items:
        sums[int(it.product_id)] += int(it.quantity or 0)
    if not sums:
        batch.assembled_output_at = datetime.now(timezone.utc)
        return None
    for pid in sums:
        if int(sums[pid] or 0) <= 0:
            continue
        pr = await db.execute(select(Product).where(Product.id == pid))
        if pr.scalar_one_or_none() is None:
            return "product_not_found"
    for pid, qty in sums.items():
        if qty <= 0:
            continue
        pr = await db.execute(select(Product).where(Product.id == pid))
        product = pr.scalar_one_or_none()
        row = await _ensure_assembled_product_stock_row(db, pid)
        row.quantity = int(row.quantity or 0) + qty
        await _log_assembled_product_stock_entry(
            db,
            product=product,
            delta_qty=qty,
            action_kind="assembly_complete",
            assembly_batch_id=batch.id,
        )
    batch.assembled_output_at = datetime.now(timezone.utc)
    logger.info(
        "warehouse assembly completion: batch internal_id={} display_no={} оприходованы собранные изделия",
        batch.id,
        getattr(batch, "display_batch_no", 0),
    )
    return None


@router.post("/warehouse/assembly/create")
async def warehouse_assembly_create(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    lines_json: str = Form("[]"),
    write_off_stock: Optional[str] = Form(None),
):
    """Создать партию сборки; при флаге write_off_stock списать напечатанные детали."""
    redirect_ok = "/warehouse?tab=assembly&success=batch_created"
    redirect_err = "/warehouse?tab=assembly&error={}"
    try:
        raw = _json.loads(lines_json or "[]")
    except Exception:
        return RedirectResponse(url=redirect_err.format("invalid_json"), status_code=303)
    if not isinstance(raw, list) or not raw:
        return RedirectResponse(url=redirect_err.format("no_lines"), status_code=303)
    lines: list[tuple[int, int]] = []
    for row in raw[:40]:
        if not isinstance(row, dict):
            continue
        try:
            pid = int(row.get("product_id") or 0)
            q = int(row.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if pid <= 0 or q <= 0:
            continue
        lines.append((pid, q))
    if not lines:
        return RedirectResponse(url=redirect_err.format("no_lines"), status_code=303)
    for pid, _ in lines:
        res = await db.execute(select(Product).where(Product.id == pid))
        if res.scalar_one_or_none() is None:
            return RedirectResponse(url=redirect_err.format("product_not_found"), status_code=303)
    write_off_enabled = str(write_off_stock or "").strip().lower() in {"1", "true", "on", "yes"}
    stocks_to_touch: list[tuple[PrintedPartStock, int]] = []
    if write_off_enabled:
        needs, err = await _assembly_batch_aggregate_printed_needs(db, lines)
        if err or needs is None:
            return RedirectResponse(url=redirect_err.format(err or "no_bom"), status_code=303)
        for (part_id, mat_id), need_qty in needs.items():
            if need_qty <= 0:
                continue
            st = await _printed_part_stock_row_for_part_material(db, part_id, mat_id)
            if not st or int(st.quantity or 0) < need_qty:
                return RedirectResponse(url=redirect_err.format("not_enough"), status_code=303)
            stocks_to_touch.append((st, need_qty))
    try:
        mx_row = await db.execute(
            select(func.coalesce(func.max(WarehouseAssemblyBatch.display_batch_no), 0))
        )
        next_display_no = int(mx_row.scalar_one() or 0) + 1
        batch = WarehouseAssemblyBatch(
            status="created",
            comment="",
            display_batch_no=next_display_no,
        )
        db.add(batch)
        await db.flush()
        for pid, q in lines:
            db.add(WarehouseAssemblyBatchItem(batch_id=batch.id, product_id=pid, quantity=q))
        if write_off_enabled:
            for st, need_qty in stocks_to_touch:
                st.quantity = max(0, int(st.quantity or 0) - need_qty)
                await _log_printed_part_stock_change(db, st, "assembly", need_qty)
        await db.commit()
        logger.info(
            "warehouse assembly: создана партия internal_id={} display_no={} строк={} write_off_stock={}",
            batch.id,
            batch.display_batch_no,
            len(lines),
            write_off_enabled,
        )
        return RedirectResponse(url=redirect_ok, status_code=303)
    except Exception as e:
        await db.rollback()
        logger.warning("warehouse assembly create failed: {}", e, exc_info=True)
        return RedirectResponse(url=redirect_err.format("failed"), status_code=303)


@router.post("/warehouse/assembly/batch/update")
async def warehouse_assembly_batch_update(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    batch_id: int = Form(...),
    status: Optional[str] = Form(None),
    comment: str = Form(""),
):
    """Обновить статус и/или комментарий партии сборки. При первом переводе в «Выполнена» — приход собранных изделий."""
    redirect_url = "/warehouse?tab=assembly&success=batch_updated"
    result = await db.execute(select(WarehouseAssemblyBatch).where(WarehouseAssemblyBatch.id == batch_id))
    batch = result.scalar_one_or_none()
    if not batch:
        return RedirectResponse(url="/warehouse?tab=assembly&error=batch_not_found", status_code=303)
    if getattr(batch, "deleted_at", None) is not None:
        return RedirectResponse(url="/warehouse?tab=assembly&error=batch_deleted", status_code=303)
    if status and status.strip() in ASSEMBLY_BATCH_STATUS_VALUES:
        new_status = status.strip()
    else:
        new_status = batch.status
    try:
        if (
            new_status == "completed"
            and getattr(batch, "assembled_output_at", None) is None
            and batch.deleted_at is None
        ):
            err = await _apply_warehouse_assembly_batch_completion_to_assembled(db, batch)
            if err:
                await db.rollback()
                return RedirectResponse(url="/warehouse?tab=assembly&error=complete_stock_failed", status_code=303)
        if status and status.strip() in ASSEMBLY_BATCH_STATUS_VALUES:
            batch.status = status.strip()
        batch.comment = (comment or "")[:512]
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.warning("warehouse assembly batch update failed: {}", e, exc_info=True)
        return RedirectResponse(url="/warehouse?tab=assembly&error=update_failed", status_code=303)
    return RedirectResponse(url=redirect_url, status_code=303)


@router.post("/warehouse/assembly/batch/delete")
async def warehouse_assembly_batch_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    batch_id: int = Form(...),
    return_parts_to_stock: Optional[str] = Form(None),
):
    """Удалить партию сборки; при флаге return_parts_to_stock вернуть напечатанные детали на склад."""
    redirect_ok = "/warehouse?tab=assembly&success=batch_deleted"
    redirect_err = "/warehouse?tab=assembly&error={}"
    result = await db.execute(select(WarehouseAssemblyBatch).where(WarehouseAssemblyBatch.id == batch_id))
    batch = result.scalar_one_or_none()
    if not batch:
        return RedirectResponse(url=redirect_err.format("batch_not_found"), status_code=303)
    if getattr(batch, "deleted_at", None) is not None:
        logger.info("warehouse assembly: повторное удаление партии id={} (уже помечена удалённой)", batch_id)
        return RedirectResponse(url=redirect_ok, status_code=303)
    items_res = await db.execute(
        select(WarehouseAssemblyBatchItem).where(WarehouseAssemblyBatchItem.batch_id == batch_id)
    )
    items = items_res.scalars().all()
    lines = [(int(it.product_id), int(it.quantity or 0)) for it in items if int(it.quantity or 0) > 0]
    return_parts_enabled = str(return_parts_to_stock or "").strip().lower() in {"1", "true", "on", "yes"}
    try:
        if not lines:
            batch.deleted_at = datetime.now(timezone.utc)
            await db.commit()
            logger.info("warehouse assembly: помечена удалённой пустая партия id={}", batch_id)
            return RedirectResponse(url=redirect_ok, status_code=303)
        if return_parts_enabled:
            ret_err = await _assembly_return_printed_parts_for_lines(db, lines)
            if ret_err:
                err_key = "delete_bom" if ret_err == "no_bom" else ret_err
                return RedirectResponse(url=redirect_err.format(err_key), status_code=303)
        batch.deleted_at = datetime.now(timezone.utc)
        await db.commit()
        logger.info(
            "warehouse assembly: партия id={} помечена удалённой, возврат деталей включён={}",
            batch_id,
            return_parts_enabled,
        )
        return RedirectResponse(url=redirect_ok, status_code=303)
    except Exception as e:
        await db.rollback()
        logger.warning("warehouse assembly batch delete failed: {}", e, exc_info=True)
        return RedirectResponse(url=redirect_err.format("delete_failed"), status_code=303)


@router.post("/warehouse/assembled/adjust")
async def warehouse_assembled_adjust(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    product_id: int = Form(...),
    action: str = Form(...),
    quantity: int = Form(1),
):
    """Списание / ручное добавление / брак по складу собранных изделий."""
    redirect_ok = "/warehouse?tab=assembled&success=assembled_updated"
    redirect_err = "/warehouse?tab=assembled&error={}"
    act = (action or "").strip()
    if act not in ASSEMBLED_ADJUST_ACTIONS:
        return RedirectResponse(url=redirect_err.format("bad_action"), status_code=303)
    qty = int(quantity or 0)
    if qty <= 0:
        return RedirectResponse(url=redirect_err.format("bad_qty"), status_code=303)
    try:
        pr = await db.execute(select(Product).where(Product.id == product_id))
        product = pr.scalar_one_or_none()
        if not product:
            return RedirectResponse(url=redirect_err.format("product_not_found"), status_code=303)
        row = await _ensure_assembled_product_stock_row(db, product_id)
        cur = int(row.quantity or 0)
        if act == "add":
            row.quantity = cur + qty
            await _log_assembled_product_stock_entry(
                db, product=product, delta_qty=qty, action_kind="manual_add"
            )
        else:
            if cur < qty:
                return RedirectResponse(url=redirect_err.format("not_enough"), status_code=303)
            row.quantity = cur - qty
            if act == "defect":
                db.add(
                    WarehouseDefectRecord(
                        item_type="product",
                        product_id=product_id,
                        display_name=_product_assembled_label(product)[:512],
                        quantity=qty,
                    )
                )
            await _log_assembled_product_stock_entry(
                db, product=product, delta_qty=-qty, action_kind=act
            )
        await db.commit()
        logger.info(
            "warehouse assembled: product_id={} action={} qty={} new_balance={}",
            product_id,
            act,
            qty,
            int(row.quantity or 0),
        )
        return RedirectResponse(url=redirect_ok, status_code=303)
    except Exception as e:
        await db.rollback()
        logger.warning("warehouse assembled adjust failed: {}", e, exc_info=True)
        return RedirectResponse(url=redirect_err.format("failed"), status_code=303)


@router.post("/warehouse/extras/save")
async def warehouse_extras_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    extra_material_id: int = Form(...),
    quantity: int = Form(0),
):
    """Оприходовать или изменить количество дополнительного материала на складе. При списании (quantity=0) запись попадает в «Списанные материалы»."""
    redirect_url = "/warehouse?tab=materials&mat_sub=extras"
    quantity = max(0, int(quantity))
    result = await db.execute(select(ExtraMaterial).where(ExtraMaterial.id == extra_material_id))
    em = result.scalar_one_or_none()
    if not em:
        return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
    result = await db.execute(
        select(WarehouseExtraStock).where(WarehouseExtraStock.extra_material_id == extra_material_id)
    )
    row = result.scalar_one_or_none()
    if row:
        if quantity == 0:
            # Перед удалением добавляем запись в списанные материалы
            db.add(WrittenOffMaterial(
                item_type="extra",
                extra_material_id=extra_material_id,
                display_name=em.name or "",
                quantity=row.quantity,
            ))
            await db.delete(row)
        else:
            row.quantity = quantity
    else:
        if quantity > 0:
            db.add(WarehouseExtraStock(extra_material_id=extra_material_id, quantity=quantity))
    await db.commit()
    return RedirectResponse(url=redirect_url + "&success=updated", status_code=303)


@router.post("/warehouse/written-off/return")
async def warehouse_written_off_return(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    written_off_id: int = Form(...),
):
    """Вернуть списанный материал на склад: доп. материал — в остаток; катушка — новая катушка с нулевым остатком."""
    redirect_url = "/warehouse?tab=materials&mat_sub=written_off"
    result = await db.execute(select(WrittenOffMaterial).where(WrittenOffMaterial.id == written_off_id))
    rec = result.scalar_one_or_none()
    if not rec:
        return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
    try:
        if rec.item_type == "extra" and rec.extra_material_id:
            stock = await db.execute(
                select(WarehouseExtraStock).where(WarehouseExtraStock.extra_material_id == rec.extra_material_id)
            )
            row = stock.scalar_one_or_none()
            if row:
                row.quantity += rec.quantity
            else:
                db.add(WarehouseExtraStock(extra_material_id=rec.extra_material_id, quantity=rec.quantity))
        elif rec.item_type == "spool" and rec.material_id:
            db.add(Spool(material_id=rec.material_id, remaining_length_m=0))
        await db.delete(rec)
        await db.commit()
        return RedirectResponse(url=redirect_url + "&success=returned", status_code=303)
    except Exception as e:
        logger.warning("written-off return: %s", e)
        return RedirectResponse(url=redirect_url + "&error=return_failed", status_code=303)


@router.post("/warehouse/written-off/delete")
async def warehouse_written_off_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    written_off_id: int = Form(...),
):
    """Удалить запись из списка списанных материалов."""
    redirect_url = "/warehouse?tab=materials&mat_sub=written_off"
    result = await db.execute(select(WrittenOffMaterial).where(WrittenOffMaterial.id == written_off_id))
    rec = result.scalar_one_or_none()
    if rec:
        await db.delete(rec)
        await db.commit()
    return RedirectResponse(url=redirect_url + "&success=deleted", status_code=303)


@router.post("/warehouse/spools/save")
async def warehouse_spool_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    spool_id: Optional[str] = Form(None),
    material_id: Optional[str] = Form(None),
    remaining_length_m: str = Form("0"),
):
    """Создать или обновить катушку (филамент на складе)."""
    try:
        rem = float((remaining_length_m or "0").replace(",", "."))
        rem = max(0.0, rem)
    except (TypeError, ValueError):
        rem = 0.0
    mat_id = None
    if material_id and material_id.strip():
        try:
            mat_id = int(material_id)
        except ValueError:
            pass
    redirect_url = "/warehouse?tab=materials"
    if spool_id and spool_id.strip():
        try:
            sid = int(spool_id)
        except ValueError:
            return RedirectResponse(url=redirect_url + "&error=invalid", status_code=303)
        result = await db.execute(select(Spool).where(Spool.id == sid))
        spool = result.scalar_one_or_none()
        if not spool:
            return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
        spool.material_id = mat_id
        spool.remaining_length_m = rem
        await db.commit()
        return RedirectResponse(url=redirect_url + "&success=updated", status_code=303)
    spool = Spool(material_id=mat_id, remaining_length_m=rem)
    db.add(spool)
    await db.commit()
    return RedirectResponse(url=redirect_url + "&success=created", status_code=303)


@router.post("/warehouse/spools/delete")
async def warehouse_spool_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    spool_id: str = Form(...),
):
    """Удалить катушку."""
    redirect_url = "/warehouse?tab=materials"
    try:
        sid = int(spool_id)
    except ValueError:
        return RedirectResponse(url=redirect_url + "&error=invalid", status_code=303)
    result = await db.execute(select(Spool).where(Spool.id == sid))
    spool = result.scalar_one_or_none()
    if not spool:
        return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
    await db.delete(spool)
    await db.commit()
    return RedirectResponse(url=redirect_url + "&success=deleted", status_code=303)


@router.get("/warehouse/spool/{spool_id:int}/qr")
async def warehouse_spool_qr(
    spool_id: int,
    user: User = Depends(verify_site_user),
):
    """PNG с QR-кодом катушки (SPOOL:id)."""
    import io
    import qrcode
    qr = qrcode.QRCode(version=1, box_size=6, border=2)
    qr.add_data(f"SPOOL:{spool_id}")
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


@router.get("/warehouse/printer/{printer_id:int}/qr")
async def warehouse_printer_qr(
    printer_id: int,
    user: User = Depends(verify_site_user),
):
    """PNG с QR-кодом принтера (PRINTER:id) для сканирования в карточке катушки."""
    import io
    import qrcode
    qr = qrcode.QRCode(version=1, box_size=6, border=2)
    qr.add_data(f"PRINTER:{printer_id}")
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


@router.get("/warehouse/spool/{spool_id:int}", response_class=HTMLResponse)
async def warehouse_spool_card(
    request: Request,
    spool_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Карточка катушки: сканер QR (принтер), действия установки/снятия, данные катушки."""
    result = await db.execute(
        select(Spool, Material)
        .select_from(Spool)
        .outerjoin(Material, Spool.material_id == Material.id)
        .where(Spool.id == spool_id)
    )
    row = result.one_or_none()
    if not row:
        return RedirectResponse(url="/warehouse?tab=materials&error=notfound", status_code=303)
    spool, material = row
    color_hex_map = {}
    try:
        result_c = await db.execute(select(Color))
        color_hex_map = {c.name: (c.hex or "#000000") for c in result_c.scalars().all()}
    except Exception:
        pass
    mat_hex = color_hex_map.get((material.color or "").strip(), "#888888") if material and (material.color or "").strip() else "#888888"
    current_printer = None
    try:
        result_prn = await db.execute(select(Printer).where(Printer.current_spool_id == spool_id))
        current_printer = result_prn.scalar_one_or_none()
    except Exception:
        pass
    printers_list = []
    try:
        result_all = await db.execute(select(Printer).order_by(Printer.number, Printer.name))
        printers_list = result_all.scalars().all()
    except Exception:
        pass
    # Иконка катушки 200x200 для карточки (reach/midi/poor по остатку, тип пластика из материала)
    rem = float(spool.remaining_length_m or 0)
    if rem >= 250:
        spool_icon = "reach"
    elif rem >= 50:
        spool_icon = "midi"
    else:
        spool_icon = "poor"
    plastic_type = (getattr(material, "plastic_type", None) or "").strip() if material else ""
    spool_dataurl = _spool_svg_dataurl(mat_hex, spool_icon, size=200, plastic_type=plastic_type or None) if mat_hex else None
    return templates.TemplateResponse(
        "warehouse_spool_card.html",
        {
            "request": request,
            "site_username": user.username,
            "spool": spool,
            "material": material,
            "material_hex": mat_hex,
            "current_printer": current_printer,
            "printers_list": printers_list,
            "spool_dataurl": spool_dataurl,
        },
    )


@router.post("/warehouse/spool/{spool_id:int}/install-on-printer")
async def warehouse_spool_install_on_printer(
    request: Request,
    spool_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    printer_id: int = Form(...),
):
    """Установить катушку на принтер (привязать принтер к катушке)."""
    result_s = await db.execute(select(Spool).where(Spool.id == spool_id))
    spool = result_s.scalar_one_or_none()
    if not spool:
        return RedirectResponse(url="/warehouse?tab=materials&error=notfound", status_code=303)
    result_p = await db.execute(select(Printer).where(Printer.id == printer_id))
    printer = result_p.scalar_one_or_none()
    if not printer:
        return RedirectResponse(url=f"/warehouse/spool/{spool_id}?error=printer_notfound", status_code=303)
    other = await db.execute(select(Printer).where(Printer.current_spool_id == spool_id, Printer.id != printer_id))
    if other.scalar_one_or_none():
        return RedirectResponse(url=f"/warehouse/spool/{spool_id}?error=spool_on_other", status_code=303)
    printer.current_spool_id = spool_id
    await db.commit()
    return RedirectResponse(url=f"/warehouse/spool/{spool_id}?success=installed", status_code=303)


@router.post("/warehouse/spool/{spool_id:int}/remove-from-printer")
async def warehouse_spool_remove_from_printer(
    request: Request,
    spool_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    printer_id: Optional[int] = Form(None),
):
    """Снять катушку с принтера. Остаток на катушке не меняется (меняется только при печати или ручном редактировании)."""
    result_s = await db.execute(select(Spool).where(Spool.id == spool_id))
    spool = result_s.scalar_one_or_none()
    if not spool:
        return RedirectResponse(url="/warehouse?tab=materials&error=notfound", status_code=303)
    if printer_id is not None:
        result_p = await db.execute(select(Printer).where(Printer.id == printer_id, Printer.current_spool_id == spool_id))
    else:
        result_p = await db.execute(select(Printer).where(Printer.current_spool_id == spool_id))
    printer = result_p.scalar_one_or_none()
    if printer:
        printer.current_spool_id = None
    await db.commit()
    return RedirectResponse(url=f"/warehouse/spool/{spool_id}?success=removed", status_code=303)


@router.post("/warehouse/spool/{spool_id:int}/write-off")
async def warehouse_spool_write_off(
    request: Request,
    spool_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
):
    """Списать катушку (закончен филамент): запись в «Списанные материалы», остаток 0, отвязка от принтера."""
    result = await db.execute(
        select(Spool, Material)
        .select_from(Spool)
        .outerjoin(Material, Spool.material_id == Material.id)
        .where(Spool.id == spool_id)
    )
    row = result.one_or_none()
    if not row:
        return RedirectResponse(url="/warehouse?tab=materials&mat_sub=written_off&error=notfound", status_code=303)
    spool, material = row
    display_name = (material.name or "Катушка") + " #" + str(spool_id)
    db.add(WrittenOffMaterial(
        item_type="spool",
        spool_id=spool_id,
        material_id=spool.material_id,
        display_name=display_name,
        quantity=1,
    ))
    spool.remaining_length_m = 0
    result_p = await db.execute(select(Printer).where(Printer.current_spool_id == spool_id))
    for printer in result_p.scalars().all():
        printer.current_spool_id = None
    await db.commit()
    return RedirectResponse(url="/warehouse?tab=materials&mat_sub=written_off&success=written_off", status_code=303)


@router.post("/warehouse/printed/add")
async def warehouse_printed_add(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    part_id: int = Form(...),
    material_id: int = Form(...),
):
    """Добавить деталь в список складируемых напечатанных деталей."""
    redirect_url = "/warehouse?tab=printed"
    result_part = await db.execute(select(Part).where(Part.id == part_id))
    part = result_part.scalar_one_or_none()
    result_material = await db.execute(select(Material).where(Material.id == material_id))
    material = result_material.scalar_one_or_none()
    if not part or not material:
        return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
    result_existing = await db.execute(
        select(PrintedPartStock).where(
            PrintedPartStock.part_id == part_id,
            PrintedPartStock.material_id == material_id,
        )
    )
    existing = result_existing.scalar_one_or_none()
    if not existing:
        db.add(PrintedPartStock(part_id=part_id, material_id=material_id, quantity=0))
        await db.commit()
    return RedirectResponse(url=redirect_url + "&success=added_to_stock", status_code=303)


@router.post("/warehouse/printed/adjust")
async def warehouse_printed_adjust(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    stock_id: int = Form(...),
    qty: int = Form(0),
    action: str = Form("add"),
):
    """Изменить остаток напечатанной детали: плюс или минус указанное количество."""
    redirect_url = "/warehouse?tab=printed"
    delta = max(0, int(qty or 0))
    if delta <= 0:
        return RedirectResponse(url=redirect_url + "&error=invalid_qty", status_code=303)
    result_stock = await db.execute(select(PrintedPartStock).where(PrintedPartStock.id == stock_id))
    stock = result_stock.scalar_one_or_none()
    if not stock:
        return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
    if action == "remove":
        stock.quantity = max(0, int(stock.quantity or 0) - delta)
    else:
        stock.quantity = int(stock.quantity or 0) + delta
    await _log_printed_part_stock_change(db, stock, "remove" if action == "remove" else "add", delta)
    await db.commit()
    return RedirectResponse(url=redirect_url + "&success=updated", status_code=303)


@router.get("/api/warehouse/printed/{stock_id:int}")
async def api_warehouse_printed_stock_info(
    stock_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Данные складируемой детали для модального окна после сканирования QR."""
    result = await db.execute(
        select(PrintedPartStock, Part, Material)
        .select_from(PrintedPartStock)
        .join(Part, PrintedPartStock.part_id == Part.id)
        .outerjoin(Material, PrintedPartStock.material_id == Material.id)
        .where(PrintedPartStock.id == stock_id)
    )
    row = result.one_or_none()
    if not row:
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    stock, part, material = row
    material_name = "—"
    if material:
        material_name = _material_name_without_weight(material.name) or "—"
    defect_total = 0
    try:
        defect_res = await db.execute(
            select(func.coalesce(func.sum(WarehouseDefectRecord.quantity), 0)).where(
                WarehouseDefectRecord.item_type == "part",
                WarehouseDefectRecord.part_id == stock.part_id,
                WarehouseDefectRecord.material_id == stock.material_id,
            )
        )
        defect_total = int(defect_res.scalar() or 0)
    except Exception:
        defect_total = 0
    return JSONResponse(
        content={
            "ok": True,
            "stock_id": stock.id,
            "part_name": (part.name or "").strip() or "—",
            "material_name": material_name,
            "quantity": int(stock.quantity or 0),
            "defect_quantity": defect_total,
        }
    )


@router.get("/api/warehouse/assembly/batch/{batch_id:int}")
async def api_warehouse_assembly_batch_info(
    batch_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Данные партии сборки для модального окна после сканирования QR."""
    result = await db.execute(
        select(WarehouseAssemblyBatch).where(
            WarehouseAssemblyBatch.id == batch_id,
            WarehouseAssemblyBatch.deleted_at.is_(None),
        )
    )
    batch = result.scalar_one_or_none()
    if not batch:
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    status_labels = {
        "created": "Создана",
        "in_progress": "В работе",
        "completed": "Выполнена",
    }
    options = [{"value": v, "label": status_labels.get(v, v)} for v in ASSEMBLY_BATCH_STATUS_VALUES]
    return JSONResponse(
        content={
            "ok": True,
            "batch_id": int(batch.id),
            "display_batch_no": int(batch.display_batch_no or 0),
            "status": str(batch.status or "created"),
            "status_label": status_labels.get(str(batch.status or ""), str(batch.status or "—")),
            "comment": str(batch.comment or ""),
            "status_options": options,
        }
    )


@router.post("/api/warehouse/assembly/batch/update-status")
async def api_warehouse_assembly_batch_update_status(
    batch_id: int = Form(...),
    status: str = Form(...),
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Смена статуса партии сборки из QR-сканера."""
    new_status = str(status or "").strip()
    if new_status not in ASSEMBLY_BATCH_STATUS_VALUES:
        return JSONResponse(status_code=400, content={"ok": False, "error": "bad_status"})
    result = await db.execute(
        select(WarehouseAssemblyBatch).where(
            WarehouseAssemblyBatch.id == batch_id,
            WarehouseAssemblyBatch.deleted_at.is_(None),
        )
    )
    batch = result.scalar_one_or_none()
    if not batch:
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    try:
        if (
            new_status == "completed"
            and getattr(batch, "assembled_output_at", None) is None
            and batch.deleted_at is None
        ):
            err = await _apply_warehouse_assembly_batch_completion_to_assembled(db, batch)
            if err:
                await db.rollback()
                return JSONResponse(status_code=400, content={"ok": False, "error": "complete_stock_failed"})
        batch.status = new_status
        await db.commit()
        status_labels = {
            "created": "Создана",
            "in_progress": "В работе",
            "completed": "Выполнена",
        }
        return JSONResponse(
            content={
                "ok": True,
                "status": new_status,
                "status_label": status_labels.get(new_status, new_status),
            }
        )
    except Exception as e:
        await db.rollback()
        logger.warning("api warehouse assembly batch update status failed: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": "failed"})


@router.post("/api/warehouse/assembly/batch/update-comment")
async def api_warehouse_assembly_batch_update_comment(
    batch_id: int = Form(...),
    comment: str = Form(""),
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Сохранение комментария партии сборки из QR-сканера."""
    result = await db.execute(
        select(WarehouseAssemblyBatch).where(
            WarehouseAssemblyBatch.id == batch_id,
            WarehouseAssemblyBatch.deleted_at.is_(None),
        )
    )
    batch = result.scalar_one_or_none()
    if not batch:
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    try:
        batch.comment = (comment or "")[:512]
        await db.commit()
        return JSONResponse(content={"ok": True, "comment": batch.comment})
    except Exception as e:
        await db.rollback()
        logger.warning("api warehouse assembly batch update comment failed: {}", e, exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": "failed"})


@router.post("/api/warehouse/printed/adjust")
async def api_warehouse_printed_adjust(
    stock_id: int = Form(...),
    qty: int = Form(1),
    action: str = Form("add"),
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Изменение остатка детали через модальное окно сканера."""
    delta = max(0, int(qty or 0))
    if delta <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_qty"})
    result_stock = await db.execute(select(PrintedPartStock).where(PrintedPartStock.id == stock_id))
    stock = result_stock.scalar_one_or_none()
    if not stock:
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    if action == "remove":
        stock.quantity = max(0, int(stock.quantity or 0) - delta)
    else:
        stock.quantity = int(stock.quantity or 0) + delta
    await _log_printed_part_stock_change(db, stock, "remove" if action == "remove" else "add", delta)
    await db.commit()
    defect_res = await db.execute(
        select(func.coalesce(func.sum(WarehouseDefectRecord.quantity), 0)).where(
            WarehouseDefectRecord.item_type == "part",
            WarehouseDefectRecord.part_id == stock.part_id,
            WarehouseDefectRecord.material_id == stock.material_id,
        )
    )
    defect_total = int(defect_res.scalar() or 0)
    return JSONResponse(content={"ok": True, "quantity": int(stock.quantity or 0), "defect_quantity": defect_total})


@router.post("/api/warehouse/printed/defect")
async def api_warehouse_printed_defect(
    stock_id: int = Form(...),
    defect_qty: int = Form(1),
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Добавить брак по детали через модальное окно сканера."""
    qty = max(0, int(defect_qty or 0))
    if qty <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_qty"})
    stock_res = await db.execute(select(PrintedPartStock).where(PrintedPartStock.id == stock_id))
    stock = stock_res.scalar_one_or_none()
    if not stock:
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    current_qty = int(stock.quantity or 0)
    if qty > current_qty:
        return JSONResponse(status_code=400, content={"ok": False, "error": "not_enough"})
    part_res = await db.execute(select(Part).where(Part.id == stock.part_id))
    part = part_res.scalar_one_or_none()
    if not part:
        return JSONResponse(status_code=404, content={"ok": False, "error": "notfound"})
    stock.quantity = max(0, current_qty - qty)
    db.add(
        WarehouseDefectRecord(
            item_type="part",
            printed_stock_id=stock.id,
            part_id=part.id,
            material_id=stock.material_id,
            display_name=part.name or "",
            quantity=qty,
        )
    )
    await _log_printed_part_stock_change(db, stock, "defect", qty)
    await db.commit()
    defect_res = await db.execute(
        select(func.coalesce(func.sum(WarehouseDefectRecord.quantity), 0)).where(
            WarehouseDefectRecord.item_type == "part",
            WarehouseDefectRecord.part_id == stock.part_id,
            WarehouseDefectRecord.material_id == stock.material_id,
        )
    )
    defect_total = int(defect_res.scalar() or 0)
    return JSONResponse(content={"ok": True, "quantity": int(stock.quantity or 0), "defect_quantity": defect_total})


@router.post("/warehouse/printed/defect")
async def warehouse_printed_defect(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    stock_id: int = Form(...),
    defect_qty: int = Form(0),
):
    """Переместить количество детали в брак и уменьшить складской остаток."""
    redirect_url = "/warehouse?tab=printed"
    qty = max(0, int(defect_qty or 0))
    if qty <= 0:
        return RedirectResponse(url=redirect_url + "&error=invalid_qty", status_code=303)
    result_stock = await db.execute(select(PrintedPartStock).where(PrintedPartStock.id == stock_id))
    stock = result_stock.scalar_one_or_none()
    if not stock:
        return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
    result_part = await db.execute(select(Part).where(Part.id == stock.part_id))
    part = result_part.scalar_one_or_none()
    if not part:
        return RedirectResponse(url=redirect_url + "&error=notfound", status_code=303)
    current_qty = int(stock.quantity or 0)
    if qty > current_qty:
        return RedirectResponse(url=redirect_url + "&error=not_enough", status_code=303)
    stock.quantity = max(0, current_qty - qty)
    db.add(
        WarehouseDefectRecord(
            item_type="part",
            printed_stock_id=stock.id,
            part_id=part.id,
            material_id=stock.material_id,
            display_name=part.name or "",
            quantity=qty,
        )
    )
    await _log_printed_part_stock_change(db, stock, "defect", qty)
    await db.commit()
    return RedirectResponse(url=redirect_url + "&success=defect_added", status_code=303)


@router.get("/warehouse/stock-log", response_class=HTMLResponse)
async def warehouse_stock_log_page(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
    f: str | None = None,
    kind: list[str] | None = Query(default=None),
    part_q: str | None = None,
    mat_q: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    time_from: str | None = None,
    time_to: str | None = None,
):
    """Журнал изменений остатков напечатанных деталей (до 3 мес.)."""
    await _purge_printed_part_stock_log_older_than_3_months(db)
    await db.commit()
    filter_applied = f == "1"
    kinds_sel = [k for k in (kind or []) if k in PRINTED_STOCK_LOG_KINDS]
    stmt = select(PrintedPartStockLog)
    conds = []
    if filter_applied:
        if kinds_sel:
            conds.append(PrintedPartStockLog.change_kind.in_(kinds_sel))
        else:
            conds.append(PrintedPartStockLog.id == -1)
    pq = (part_q or "").strip()
    if pq:
        conds.append(func.instr(func.lower(PrintedPartStockLog.part_name), pq.lower()) > 0)
    mq = (mat_q or "").strip()
    if mq:
        conds.append(func.instr(func.lower(PrintedPartStockLog.material_name), mq.lower()) > 0)
    if conds:
        stmt = stmt.where(and_(*conds))
    stmt = stmt.order_by(PrintedPartStockLog.created_at.desc())
    result = await db.execute(stmt)
    rows = list(result.scalars().all())
    df = _parse_stock_log_date_param(date_from)
    dt_d = _parse_stock_log_date_param(date_to)
    tf = _parse_stock_log_time_param(time_from)
    tt = _parse_stock_log_time_param(time_to)
    if df is not None or dt_d is not None or tf is not None or tt is not None:
        rows = [
            r
            for r in rows
            if _stock_log_row_matches_msk_datetime_filters(r.created_at, df, dt_d, tf, tt)
        ]
    for r in rows:
        setattr(r, "_log_date_msk", _format_stock_log_date_msk(r.created_at))
        setattr(r, "_log_time_msk", _format_stock_log_time_msk(r.created_at))
    return templates.TemplateResponse(
        "warehouse_stock_log.html",
        {
            "request": request,
            "site_username": user.username,
            "log_rows": rows,
            "filter_applied": filter_applied,
            "kinds_sel": kinds_sel,
            "part_q": part_q or "",
            "mat_q": mat_q or "",
            "date_from": date_from or "",
            "date_to": date_to or "",
            "time_from": time_from or "",
            "time_to": time_to or "",
        },
    )


@router.get("/warehouse/printed/{stock_id:int}/qr")
async def warehouse_printed_stock_qr(
    stock_id: int,
    user: User = Depends(verify_site_user),
):
    """PNG с QR-кодом складируемой напечатанной детали (PRINTED_STOCK:id)."""
    import io
    import qrcode
    qr = qrcode.QRCode(version=1, box_size=6, border=2)
    qr.add_data(f"Деталь/{stock_id}")
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


@router.get("/warehouse/assembly/batch/{batch_id:int}/qr")
async def warehouse_assembly_batch_qr(
    batch_id: int,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """PNG с QR-кодом партии сборки (AssemblyBatch/id)."""
    result_batch = await db.execute(
        select(WarehouseAssemblyBatch).where(
            WarehouseAssemblyBatch.id == batch_id,
            WarehouseAssemblyBatch.deleted_at.is_(None),
        )
    )
    batch = result_batch.scalar_one_or_none()
    if not batch:
        return Response(status_code=404)
    import io
    import qrcode
    qr = qrcode.QRCode(version=1, box_size=6, border=2)
    qr.add_data(f"ПартияСборки/{batch_id}")
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


@router.get("/warehouse/defects", response_class=HTMLResponse)
async def warehouse_defects_page(
    request: Request,
    user: User = Depends(verify_site_user),
    db: AsyncSession = Depends(get_db),
):
    """Журнал брака по деталям и изделиям."""
    result = await db.execute(select(WarehouseDefectRecord).order_by(WarehouseDefectRecord.created_at.desc()))
    all_rows = result.scalars().all()
    color_hex_map = {}
    try:
        result_c = await db.execute(select(Color))
        color_hex_map = {c.name: (c.hex or "#888888") for c in result_c.scalars().all()}
    except Exception:
        color_hex_map = {}
    material_ids = {int(r.material_id) for r in all_rows if getattr(r, "material_id", None)}
    material_by_id = {}
    if material_ids:
        result_m = await db.execute(select(Material).where(Material.id.in_(material_ids)))
        material_by_id = {m.id: m for m in result_m.scalars().all()}
    for r in all_rows:
        m = material_by_id.get(getattr(r, "material_id", None))
        if m:
            setattr(r, "_material_name", _material_name_without_weight(m.name) or "—")
            setattr(r, "_material_hex", color_hex_map.get((m.color or "").strip(), "#888888"))
        else:
            setattr(r, "_material_name", "—")
            setattr(r, "_material_hex", "#888888")
        setattr(r, "_created_at_msk", _format_dt_as_msk(getattr(r, "created_at", None)))
    part_rows = [r for r in all_rows if (r.item_type or "").strip() == "part"]
    product_rows = [r for r in all_rows if (r.item_type or "").strip() == "product"]
    return templates.TemplateResponse(
        "warehouse_defects.html",
        {
            "request": request,
            "site_username": user.username,
            "part_rows": part_rows,
            "product_rows": product_rows,
        },
    )


@router.post("/warehouse/defects/return")
async def warehouse_defects_return(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    defect_id: int = Form(...),
    return_qty: int = Form(1),
):
    """Вернуть позицию из брака обратно на соответствующий склад."""
    redirect_url = "/warehouse/defects"
    result = await db.execute(select(WarehouseDefectRecord).where(WarehouseDefectRecord.id == defect_id))
    rec = result.scalar_one_or_none()
    if not rec:
        return RedirectResponse(url=redirect_url + "?error=notfound", status_code=303)
    qty_total = max(0, int(rec.quantity or 0))
    qty = max(1, int(return_qty or 1))
    qty = min(qty, qty_total)
    if qty_total <= 0:
        await db.delete(rec)
        await db.commit()
        return RedirectResponse(url=redirect_url + "?success=returned", status_code=303)
    if rec.item_type == "part" and rec.part_id:
        # Основной сценарий: жесткая привязка брака к ID складируемой детали.
        stock_row = None
        if rec.printed_stock_id:
            stock_result = await db.execute(
                select(PrintedPartStock).where(PrintedPartStock.id == rec.printed_stock_id)
            )
            stock_row = stock_result.scalar_one_or_none()
        # Fallback только для старых записей, где еще не было printed_stock_id.
        if not stock_row:
            if rec.material_id is None:
                stock_result = await db.execute(
                    select(PrintedPartStock).where(
                        PrintedPartStock.part_id == rec.part_id,
                        PrintedPartStock.material_id.is_(None),
                    )
                )
            else:
                stock_result = await db.execute(
                    select(PrintedPartStock).where(
                        PrintedPartStock.part_id == rec.part_id,
                        PrintedPartStock.material_id == rec.material_id,
                    )
                )
            stock_rows = stock_result.scalars().all()
            if not stock_rows:
                stock_row = PrintedPartStock(part_id=rec.part_id, material_id=rec.material_id, quantity=0)
                db.add(stock_row)
                await db.flush()
            else:
                stock_row = stock_rows[0]
                if len(stock_rows) > 1:
                    for dup in stock_rows[1:]:
                        stock_row.quantity = int(stock_row.quantity or 0) + int(dup.quantity or 0)
                        await db.delete(dup)
        stock_row.quantity = int(stock_row.quantity or 0) + qty
        await _log_printed_part_stock_change(db, stock_row, "defect_return", qty)
    elif rec.item_type == "product" and rec.product_id:
        stock_result = await db.execute(select(AssembledProductStock).where(AssembledProductStock.product_id == rec.product_id))
        stock_row = stock_result.scalar_one_or_none()
        if not stock_row:
            stock_row = AssembledProductStock(product_id=rec.product_id, quantity=0)
            db.add(stock_row)
            await db.flush()
        stock_row.quantity = int(stock_row.quantity or 0) + qty
    rest = qty_total - qty
    if rest <= 0:
        await db.delete(rec)
    else:
        rec.quantity = rest
    await db.commit()
    return RedirectResponse(url=redirect_url + "?success=returned", status_code=303)


# ——— Упаковочные задания (заглушка) ———

@router.get("/packaging-tasks", response_class=HTMLResponse)
async def packaging_tasks_page(
    request: Request,
    user: User = Depends(verify_site_user),
):
    """Страница «Упаковочные задания» — заглушка."""
    return templates.TemplateResponse(
        "packaging_tasks.html",
        {"request": request, "site_username": user.username},
    )
