"""
Фоновое сканирование очереди поставок Ozon: по конфигу черновика перебираем кластеры,
создаём черновик на кластер, ждём 5 с, проверяем статус, запрашиваем таймслоты (макс. период по API — 28 дней).
Таблица и картинка — 21 колонка (день).
Лимиты Ozon на POST /v1/draft/crossdock/create: 2/мин, 50/час, 500/сутки — интервал между create из settings (дефолт 72 с).
"""
from __future__ import annotations

import asyncio
import copy
import io
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from loguru import logger
from PIL import Image, ImageDraw, ImageFont
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import SupplyDraftConfig, SupplyQueueResult, SupplyQueueScan
from app.db.database import AsyncSessionLocal
from app.config import settings
from app.modules.ozon.api_client import OzonAPIClient
from app.time_utils import now_msk

# Дефолтное тело черновика (стартовый набор пользователя)
DEFAULT_DRAFT_BODY = {
    "cluster_info": {
        "macrolocal_cluster_id": 4066,
        "items": [{"sku": 1572420324, "quantity": 100}],
    },
    "deletion_sku_mode": "PARTIAL",
    "delivery_info": {
        "drop_off_warehouse": {
            "warehouse_id": 1020005000295764,
            "warehouse_type": "CROSS_DOCK",
        },
        "seller_warehouse_id": 1020005008005660,
        "type": "DROPOFF",
    },
}

DAYS_COUNT = 21  # колонок в таблице (отображение и запрос к API; макс. по доке — 28)
DAYS_API_REQUEST = 21  # период запроса таймслотов (дней)
DRAFT_CREATE_INTERVAL_SEC = float(getattr(settings, "ozon_draft_create_min_interval_sec", 72.0))
AFTER_DRAFT_WAIT_SEC = 5
STATUS_POLL_INTERVAL_SEC = 1
STATUS_POLL_MAX_ATTEMPTS = 60
TIMESLOT_RATE_SEC = 6  # пауза перед запросом таймслотов (снижает 429)
TIMESLOT_429_RETRY_DELAY_SEC = 30  # при 429 ждём и повторяем (окно лимита Ozon может быть до минуты)
TIMESLOT_429_MAX_RETRIES = 4  # всего 5 попыток (0..4), при 429 — пауза 25 с между попытками


async def _get_draft_config(session: AsyncSession) -> Optional[dict]:
    """Получить тело черновика из БД (первая запись) или None."""
    r = await session.execute(select(SupplyDraftConfig).limit(1))
    row = r.scalar_one_or_none()
    if row and row.draft_body:
        return dict(row.draft_body)
    return None


async def _get_clusters() -> list[dict]:
    """Список кластеров Ozon для кроссдокинга."""
    client = OzonAPIClient()
    clusters = await client.get_cluster_list(cluster_type="CLUSTER_TYPE_OZON")
    return clusters or []


def _parse_dates_from_timeslot_response(data: dict) -> str:
    """Из ответа v2/draft/timeslot/info извлечь даты с доступными слотами (ДД.ММ через запятую) или 'нет дат'."""
    try:
        result = (data or {}).get("result") or {}
        drop_off = result.get("drop_off_warehouse_timeslots") or {}
        days = drop_off.get("days") or []
        dates = []
        for day in days:
            if not isinstance(day, dict):
                logger.debug("Supply scan _parse_dates: пропуск дня (не dict): %s", type(day))
                continue
            slots = day.get("timeslots") or []
            if slots and day.get("date_in_timezone"):
                d = day["date_in_timezone"]
                if len(d) >= 10:
                    dates.append(d[8:10] + "." + d[5:7])
                else:
                    dates.append(d)
        return ", ".join(dates) if dates else "нет дат"
    except Exception as e:
        logger.warning(
            "Supply scan _parse_dates_from_timeslot_response error: %s (%s), data_keys=%s result_keys=%s",
            type(e).__name__, e, list((data or {}).keys()), list(((data or {}).get("result") or {}).keys()),
        )
        return "нет дат"


def _parse_day_counts(data: dict, date_from_str: str) -> list[int]:
    """
    Из ответа v2/draft/timeslot/info — список из DAYS_COUNT чисел: количество слотов по дням от date_from.
    -1 не используется здесь (ошибки задаются при сохранении).
    """
    out: list[int] = []
    try:
        result = (data or {}).get("result") or {}
        drop_off = result.get("drop_off_warehouse_timeslots") or {}
        days_raw = drop_off.get("days") or []
        by_date: dict[str, int] = {}
        for day in days_raw:
            if not isinstance(day, dict):
                continue
            dt = day.get("date_in_timezone") or ""
            if len(dt) >= 10:
                key = dt[:10]
                by_date[key] = len(day.get("timeslots") or [])
        base = datetime.strptime(date_from_str, "%Y-%m-%d").date()
        for i in range(DAYS_COUNT):
            d = base + timedelta(days=i)
            key = d.strftime("%Y-%m-%d")
            out.append(by_date.get(key, 0))
    except Exception as e:
        logger.warning(
            "Supply scan _parse_day_counts error: %s (%s), date_from_str=%s data_keys=%s",
            type(e).__name__, e, date_from_str, list((data or {}).keys()),
        )
        out = [0] * DAYS_COUNT
    return out if len(out) == DAYS_COUNT else [0] * DAYS_COUNT


# Цвета ячеек для картинки (как в шаблоне)
_CELL_EMPTY = (232, 232, 232)
_CELL_RED = (201, 131, 64)  # оранжевый (1 слот)
_CELL_YELLOW = (223, 235, 52)
_CELL_GREEN = (52, 235, 140)
_CELL_ERROR = (255, 224, 224)


def _cell_color(count: int) -> tuple[int, int, int]:
    if count < 0:
        return _CELL_ERROR
    if count == 0:
        return _CELL_EMPTY
    if count == 1:
        return _CELL_RED
    if count <= 6:
        return _CELL_YELLOW
    return _CELL_GREEN


def _get_table_font(size: int = 10):
    """Шрифт с поддержкой кириллицы и цифр для картинки таблицы."""
    for path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/DejaVuSans.ttf",
    ):
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    try:
        return ImageFont.load_default()
    except Exception:
        return None


def _draw_rotated_text(img: Image.Image, text: str, center_xy: tuple[int, int], font, angle: int = -90):
    """Рисует текст на img повёрнутым на angle градусов (по умолчанию -90). center_xy — центр подписи."""
    cx, cy = center_xy
    if not text or not font:
        return
    tmp_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    try:
        bbox = tmp_draw.textbbox((0, 0), text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    except Exception:
        try:
            tw, th = tmp_draw.textsize(text, font=font)
        except Exception:
            return
    pad = 2
    tile = Image.new("RGBA", (tw + pad * 2, th + pad * 2), (255, 255, 255, 0))
    tdraw = ImageDraw.Draw(tile)
    tdraw.text((pad, pad), text, font=font, fill=(0, 0, 0, 255))
    rotated = tile.rotate(angle, expand=True)
    rw, rh = rotated.size
    paste_x = cx - rw // 2
    paste_y = cy - rh // 2
    if rotated.mode == "RGBA":
        img.paste(rotated, (paste_x, paste_y), rotated)
    else:
        img.paste(rotated, (paste_x, paste_y))


def _build_queue_table_image(
    results: list[tuple[int, str, str, list[int]]],
    date_from_str: str,
) -> bytes:
    """
    Рисует таблицу: колонка «Склад», DAYS_COUNT колонок по дням. Возвращает PNG bytes.
    """
    try:
        base = datetime.strptime(date_from_str, "%Y-%m-%d").date()
        dates = [base + timedelta(days=i) for i in range(DAYS_COUNT)]
    except Exception:
        dates = [None] * DAYS_COUNT

    try:
        cell_w = 20
        cell_h = 22
        name_col_w = 140
        header_h = 48
        row_h = max(cell_h, 20)
        pad = 2
        font = _get_table_font(10)

        n_rows = max(1, len(results))
        img_w = name_col_w + DAYS_COUNT * cell_w + pad * 2
        img_h = header_h + n_rows * row_h + pad * 2
        img = Image.new("RGB", (img_w, img_h), (255, 255, 255))
        draw = ImageDraw.Draw(img)

        # Заголовок: «Склад» + даты (dd.mm), даты повёрнуты на 90°
        draw.rectangle([0, 0, img_w, header_h], fill=(229, 229, 229), outline=(180, 180, 180))
        if font:
            draw.text((pad + 4, pad + 4), "Склад", fill=(0, 0, 0), font=font)
        for i, d in enumerate(dates):
            x = name_col_w + i * cell_w + pad
            draw.rectangle([x, 0, x + cell_w, header_h], fill=(229, 229, 229), outline=(180, 180, 180))
            if d is not None:
                label = d.strftime("%d.%m")
            else:
                label = str(i + 1)
            center_x = x + cell_w // 2
            center_y = header_h // 2
            _draw_rotated_text(img, label, (center_x, center_y), font, angle=90)

        for r_idx, row in enumerate(results):
            _, cluster_name, _, day_counts = row
            y0 = header_h + r_idx * row_h
            draw.rectangle([0, y0, name_col_w, y0 + row_h], fill=(245, 245, 245), outline=(200, 200, 200))
            name_short = (cluster_name[:18] + "...") if len(cluster_name) > 18 else cluster_name
            if font:
                draw.text((pad + 4, y0 + 2), name_short, fill=(0, 0, 0), font=font)
            counts = day_counts if len(day_counts) >= DAYS_COUNT else (list(day_counts) + [0] * (DAYS_COUNT - len(day_counts)))
            for i in range(DAYS_COUNT):
                c = counts[i] if i < len(counts) else 0
                if not isinstance(c, int):
                    try:
                        c = int(c)
                    except (TypeError, ValueError):
                        c = 0
                x = name_col_w + i * cell_w + pad
                color = _cell_color(c)
                draw.rectangle([x, y0, x + cell_w, y0 + row_h], fill=color, outline=(200, 200, 200))

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logger.warning("Supply scan: не удалось построить картинку таблицы: %s", e)
        return b""


async def run_supply_queue_scan() -> None:
    """
    Запуск парсинга: загружаем конфиг, для каждого кластера создаём черновик,
    ждём 5 с, опрашиваем статус до SUCCESS, запрашиваем таймслоты на 21 день,
    сохраняем результат в БД. В конце — отправка картинки таблицы в Telegram.
    Выполняется под общей блокировкой Ozon (не вместе с отслеживателем слотов).
    """
    from app.modules.ozon.runner import ozon_runner_lock

    async with ozon_runner_lock:
        await _run_supply_queue_scan_impl()


async def _run_supply_queue_scan_impl() -> None:
    """Внутренняя реализация парсинга (вызывается под ozon_runner_lock)."""
    async with AsyncSessionLocal() as session:
        body = await _get_draft_config(session)
        if not body:
            body = copy.deepcopy(DEFAULT_DRAFT_BODY)
            logger.info("Supply scan: конфиг из БД пуст, использован дефолтный набор черновика")

    clusters = await _get_clusters()
    if not clusters:
        logger.warning("Supply scan: список кластеров пуст, сканирование отменено")
        return

    # Период скана — от «сегодня» по МСК
    today = now_msk()
    today_str = today.strftime("%Y-%m-%d")
    to_date = today + timedelta(days=DAYS_API_REQUEST)
    to_str = to_date.strftime("%Y-%m-%d")

    client = OzonAPIClient()
    results: list[tuple[int, str, str, list[int]]] = []  # (cluster_id, cluster_name, dates_text, day_counts)

    for i, cluster in enumerate(clusters):
        cluster_id = cluster.get("macrolocal_cluster_id") or cluster.get("id")
        if cluster_id is None:
            continue
        cluster_name = cluster.get("name") or cluster.get("cluster_name") or f"Кластер {cluster_id}"

        payload = copy.deepcopy(body)
        if "cluster_info" not in payload:
            payload["cluster_info"] = {}
        payload["cluster_info"] = dict(payload["cluster_info"])
        payload["cluster_info"]["macrolocal_cluster_id"] = int(cluster_id)

        draft_created_at = time.monotonic()

        try:
            resp = await client.create_crossdock_draft_raw(payload)
            draft_id = resp.get("draft_id")
            if not draft_id or draft_id == 0 or (resp.get("errors")):
                results.append((int(cluster_id), cluster_name, "ошибка создания черновика", [-1] * DAYS_COUNT))
                await asyncio.sleep(max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at)))
                continue
        except httpx.HTTPStatusError as e:
            resp_text = (e.response.text or "")[:1500]
            logger.warning(
                "Supply scan cluster {}: create draft failed: HTTP {} | response: {}",
                cluster_id,
                e.response.status_code,
                resp_text or "(empty)",
            )
            results.append((int(cluster_id), cluster_name, "ошибка создания черновика", [-1] * DAYS_COUNT))
        except Exception as e:
            logger.warning("Supply scan cluster {}: create draft failed: {}", cluster_id, e)
            results.append((int(cluster_id), cluster_name, "ошибка создания черновика", [-1] * DAYS_COUNT))
            await asyncio.sleep(max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at)))
            continue

        await asyncio.sleep(AFTER_DRAFT_WAIT_SEC)

        # Опрос статуса до SUCCESS (или FAILED с «нет слотов на точке отгрузки»)
        success = False
        failed_no_slots = False  # FAILED + DROP_OFF_POINT_HAS_NO_TIMESLOTS → «нет дат»
        for _ in range(STATUS_POLL_MAX_ATTEMPTS):
            info = await client.get_draft_info(str(draft_id))
            if info.get("_error"):
                await asyncio.sleep(STATUS_POLL_INTERVAL_SEC)
                continue
            payload = info.get("result") or info.get("data") or info
            st = info.get("status") or info.get("state") or (payload.get("status") if isinstance(payload, dict) else None) or (payload.get("state") if isinstance(payload, dict) else None)
            if st == "SUCCESS":
                success = True
                break
            if st == "FAILED":
                err_list = info.get("errors") or (payload.get("errors") if isinstance(payload, dict) else None) or []
                for e in err_list:
                    msg = (e.get("error_message") or "") if isinstance(e, dict) else ""
                    if msg == "DROP_OFF_POINT_HAS_NO_TIMESLOTS":
                        failed_no_slots = True
                        logger.info(
                            "Supply scan cluster " + str(cluster_id) + " (" + str(cluster_name) + "): черновик FAILED — точка отгрузки без слотов, сохраняем «нет дат»"
                        )
                        break
                if failed_no_slots:
                    break
            await asyncio.sleep(STATUS_POLL_INTERVAL_SEC)

        if failed_no_slots:
            results.append((int(cluster_id), cluster_name, "нет дат", [0] * DAYS_COUNT))
            await asyncio.sleep(max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at)))
            continue
        if not success:
            results.append((int(cluster_id), cluster_name, "таймаут статуса", [-1] * DAYS_COUNT))
            await asyncio.sleep(max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at)))
            continue

        await asyncio.sleep(TIMESLOT_RATE_SEC)

        try:
            logger.info("Supply scan cluster " + str(cluster_id) + " (" + str(cluster_name) + "): запрос таймслотов draft_id=" + str(draft_id) + " date_from=" + str(today_str) + " date_to=" + str(to_str))
            ts_resp = None
            for retry in range(TIMESLOT_429_MAX_RETRIES + 1):
                ts_resp = await client.get_draft_timeslots(
                    draft_id=int(draft_id),
                    date_from=today_str,
                    date_to=to_str,
                    supply_type="CROSSDOCK",
                    selected_cluster_warehouses=[{"macrolocal_cluster_id": int(cluster_id)}],
                )
                if not ts_resp.get("_error"):
                    break
                if ts_resp.get("status_code") == 429 and retry < TIMESLOT_429_MAX_RETRIES:
                    logger.info("Supply scan cluster " + str(cluster_id) + ": 429 rate limit, пауза " + str(TIMESLOT_429_RETRY_DELAY_SEC) + " с, повтор " + str(retry + 2))
                    await asyncio.sleep(TIMESLOT_429_RETRY_DELAY_SEC)
                    continue
                break
            if ts_resp.get("_error"):
                status_code = ts_resp.get("status_code")
                if status_code == 404:
                    logger.info(
                        "Supply scan cluster " + str(cluster_id) + " (" + str(cluster_name) + "): Ozon 404 — нет слотов по кластеру, сохраняем «нет дат»"
                    )
                    results.append((int(cluster_id), cluster_name, "нет дат", [0] * DAYS_COUNT))
                else:
                    logger.warning(
                        "Supply scan cluster " + str(cluster_id) + ": таймслоты ошибка _error=" + str(ts_resp.get("_error")) + " status_code=" + str(status_code) + " ozon_preview=" + str(ts_resp.get("ozon_response", ""))[:400],
                    )
                    results.append((int(cluster_id), cluster_name, "ошибка таймслотов", [-1] * DAYS_COUNT))
            else:
                dates_text = _parse_dates_from_timeslot_response(ts_resp)
                day_counts = _parse_day_counts(ts_resp, today_str)
                logger.info("Supply scan cluster " + str(cluster_id) + ": таймслоты успех dates_text=" + str(dates_text) + " day_counts=" + str(day_counts))
                results.append((int(cluster_id), cluster_name, dates_text, day_counts))
        except Exception as e:
            logger.warning("Supply scan cluster " + str(cluster_id) + ": timeslots exception " + type(e).__name__ + " (" + str(e) + ")", exc_info=True)
            results.append((int(cluster_id), cluster_name, "ошибка таймслотов", [-1] * DAYS_COUNT))

        await asyncio.sleep(max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at)))

    # Сохраняем результаты в БД.
    # Важно: создаём scan только сейчас, чтобы не оставлять "пустые" сканы при рестарте/прерывании процесса.
    if not results:
        logger.warning("Supply scan: results пустой, scan не будет сохранён (чтобы не затирать последнюю таблицу на сайте)")
        return

    async with AsyncSessionLocal() as session:
        scan = SupplyQueueScan()
        session.add(scan)
        await session.flush()  # получаем scan.id до commit
        scan_id = scan.id
        for cluster_id, cluster_name, dates_text, day_counts in results:
            session.add(
                SupplyQueueResult(
                    scan_id=scan_id,
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    dates_text=dates_text,
                    day_counts=day_counts,
                )
            )
        await session.commit()

    # Отправка в Telegram — картинка таблицы
    try:
        from app.telegram.bot import send_report_photo

        scan_time_str = now_msk().strftime("%d.%m.%Y %H:%M")
        caption = f"Очередь поставок (скан {scan_time_str} МСК)"
        results_sorted = sorted(results, key=lambda r: (r[1] or "").lower())
        photo_bytes = _build_queue_table_image(results_sorted, today_str)
        if photo_bytes:
            await send_report_photo(photo=photo_bytes, caption=caption)
        else:
            logger.warning("Supply scan: картинка таблицы пуста, в Telegram не отправлено")
    except Exception as e:
        logger.warning("Supply scan: отправка в Telegram не удалась: %s", e)

    logger.info("Supply scan завершён: scan_id={}, кластеров={}", scan_id, len(results))
