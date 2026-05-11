"""
Отслеживатель свободных таймслотов: по конфигу проверяет выбранные кластеры в заданном периоде (7/14/21 дней),
при появлении слотов отправляет уведомление в Telegram. Остальные параметры черновика берутся из основного парсера.
Результат выводится картинкой (как в основном парсере).
"""
from __future__ import annotations

import asyncio
import copy
import io
import time
from datetime import datetime, timedelta
from typing import Optional

from loguru import logger
from PIL import Image, ImageDraw, ImageFont
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import SlotsTrackerConfig, SupplyDraftConfig
from app.db.database import AsyncSessionLocal
from app.config import settings
from app.modules.ozon.api_client import OzonAPIClient
from app.time_utils import MSK, now_msk

# Если конфиг парсера пуст — используем дефолт (как в supply_scan), чтобы отслеживатель мог запуститься
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

DRAFT_CREATE_INTERVAL_SEC = float(getattr(settings, "ozon_draft_create_min_interval_sec", 72.0))
AFTER_DRAFT_WAIT_SEC = 5
STATUS_POLL_INTERVAL_SEC = 1
STATUS_POLL_MAX_ATTEMPTS = 60
TIMESLOT_RATE_SEC = 4
TIMESLOT_429_RETRY_DELAY_SEC = 20
TIMESLOT_429_MAX_RETRIES = 2


async def _get_tracker_config(session: AsyncSession) -> Optional[SlotsTrackerConfig]:
    """Получить первый включённый конфиг отслеживателя."""
    r = await session.execute(
        select(SlotsTrackerConfig).where(SlotsTrackerConfig.enabled == 1).limit(1)
    )
    return r.scalar_one_or_none()


async def _get_draft_config(session: AsyncSession) -> Optional[dict]:
    """Тело черновика из основного парсера (delivery_info и прочее)."""
    r = await session.execute(select(SupplyDraftConfig).limit(1))
    row = r.scalar_one_or_none()
    if row and row.draft_body:
        return dict(row.draft_body)
    return None


def _parse_dates_and_counts_in_period(
    data: dict, date_from_str: str, period_days: int
) -> tuple[str, list[int]]:
    """
    Из ответа v2/draft/timeslot/info извлечь даты с слотами и список количеств слотов по дням
    в пределах period_days. Возвращает (dates_text, day_counts).
    """
    dates: list[str] = []
    out: list[int] = []
    try:
        result = (data or {}).get("result") or (data or {}).get("data") or {}
        drop_off = result.get("drop_off_warehouse_timeslots") or {}
        days_raw = drop_off.get("days") or []
        if not days_raw and isinstance(result, dict):
            # альтернативная структура ответа
            for key in ("warehouse_timeslots", "timeslots", "days"):
                cand = result.get(key)
                if isinstance(cand, list):
                    days_raw = cand
                    break
        if not days_raw:
            logger.debug(
                "Slots tracker _parse_dates: пустой days, ключи data=%s result=%s",
                list((data or {}).keys()),
                list(result.keys()) if isinstance(result, dict) else [],
            )
        by_date: dict[str, int] = {}
        for day in days_raw:
            if not isinstance(day, dict):
                continue
            dt = day.get("date_in_timezone") or ""
            if len(dt) >= 10:
                key = dt[:10]
                cnt = len(day.get("timeslots") or [])
                by_date[key] = cnt
        base = datetime.strptime(date_from_str, "%Y-%m-%d").date()
        for i in range(period_days):
            d = base + timedelta(days=i)
            key = d.strftime("%Y-%m-%d")
            cnt = by_date.get(key, 0)
            out.append(cnt)
            if cnt > 0:
                dates.append(d.strftime("%d.%m"))
    except Exception as e:
        logger.warning(
            "Slots tracker _parse_dates_and_counts error: %s (%s)",
            type(e).__name__, e,
        )
        out = [0] * period_days
    dates_text = ", ".join(dates) if dates else "нет дат"
    return dates_text, out[:period_days]


def _has_slots_in_period(day_counts: list[int]) -> bool:
    """Есть ли хотя бы один день с доступными слотами в периоде."""
    return any(c > 0 for c in day_counts)


# Цвета ячеек для картинки (как в основном парсере)
_CELL_EMPTY = (232, 232, 232)
_CELL_RED = (201, 131, 64)
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
    paste_x, paste_y = cx - rw // 2, cy - rh // 2
    if rotated.mode == "RGBA":
        img.paste(rotated, (paste_x, paste_y), rotated)
    else:
        img.paste(rotated, (paste_x, paste_y))


def _build_tracker_table_image(
    results: list[tuple[int, str, str, list[int]]],
    date_from_str: str,
    period_days: int,
) -> bytes:
    """Таблица: колонка «Склад», period_days колонок по дням. Возвращает PNG bytes."""
    try:
        base = datetime.strptime(date_from_str, "%Y-%m-%d").date()
        dates = [base + timedelta(days=i) for i in range(period_days)]
    except Exception:
        dates = [None] * period_days
    try:
        cell_w = 20
        cell_h = 22
        name_col_w = 140
        header_h = 48
        row_h = max(cell_h, 20)
        pad = 2
        font = _get_table_font(10)
        n_rows = max(1, len(results))
        img_w = name_col_w + period_days * cell_w + pad * 2
        img_h = header_h + n_rows * row_h + pad * 2
        img = Image.new("RGB", (img_w, img_h), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        draw.rectangle([0, 0, img_w, header_h], fill=(229, 229, 229), outline=(180, 180, 180))
        if font:
            draw.text((pad + 4, pad + 4), "Склад", fill=(0, 0, 0), font=font)
        for i, d in enumerate(dates):
            x = name_col_w + i * cell_w + pad
            draw.rectangle([x, 0, x + cell_w, header_h], fill=(229, 229, 229), outline=(180, 180, 180))
            label = d.strftime("%d.%m") if d is not None else str(i + 1)
            _draw_rotated_text(img, label, (x + cell_w // 2, header_h // 2), font, angle=90)
        for r_idx, row in enumerate(results):
            _, cluster_name, _, day_counts = row
            y0 = header_h + r_idx * row_h
            draw.rectangle([0, y0, name_col_w, y0 + row_h], fill=(245, 245, 245), outline=(200, 200, 200))
            name_short = (cluster_name[:18] + "...") if len(cluster_name) > 18 else cluster_name
            if font:
                draw.text((pad + 4, y0 + 2), name_short, fill=(0, 0, 0), font=font)
            counts = list(day_counts) if len(day_counts) >= period_days else list(day_counts) + [0] * (period_days - len(day_counts))
            for i in range(period_days):
                c = int(counts[i]) if i < len(counts) and counts[i] is not None else 0
                if i < len(counts) and isinstance(counts[i], int) and counts[i] < 0:
                    c = -1
                x = name_col_w + i * cell_w + pad
                draw.rectangle([x, y0, x + cell_w, y0 + row_h], fill=_cell_color(c), outline=(200, 200, 200))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logger.warning("Slots tracker: не удалось построить картинку таблицы: %s", e)
        return b""


async def run_slots_tracker() -> None:
    """
    Запуск отслеживателя: загружаем конфиг, для каждого выбранного кластера создаём черновик
    (состав — по 50 шт каждого SKU, delivery_info и прочее из основного парсера), запрашиваем
    таймслоты на period_days. Уведомление в Telegram отправляется только при появлении слотов
    в искомом периоде.
    """
    logger.info("Slots tracker: запуск")
    async with AsyncSessionLocal() as session:
        config = await _get_tracker_config(session)
        if not config:
            logger.info("Slots tracker: конфиг отключён или отсутствует (enabled=1), пропуск")
            return
        cluster_ids = list(config.cluster_ids or [])
        if not cluster_ids:
            logger.warning("Slots tracker: список кластеров пуст, пропуск")
            return
        period_days = int(config.period_days) if config.period_days else 7
        if period_days not in (7, 14, 21):
            period_days = 7
        items = list(config.items or [])
        if not items:
            logger.warning("Slots tracker: товарный состав пуст, пропуск")
            return

        base_body = await _get_draft_config(session)
        if not base_body or not isinstance(base_body, dict):
            base_body = copy.deepcopy(DEFAULT_DRAFT_BODY)
            logger.info(
                "Slots tracker: конфиг черновика парсера пуст, использован дефолт. "
                "Сохраните конфиг в «Тестирование API» для своих складов."
            )

    clusters = await _get_clusters()
    logger.info("Slots tracker: конфиг загружен, кластеров к проверке=%s, период=%s дн", len(cluster_ids), period_days)
    cluster_by_id: dict[int, str] = {}
    for c in clusters or []:
        cid = c.get("macrolocal_cluster_id") or c.get("id")
        if cid is not None:
            cluster_by_id[int(cid)] = (
                c.get("name") or c.get("cluster_name") or f"Кластер {cid}"
            )

    # Период запроса слотов — от «сегодня» по МСК
    today = now_msk()
    today_str = today.strftime("%Y-%m-%d")
    to_date = today + timedelta(days=period_days)
    to_str = to_date.strftime("%Y-%m-%d")

    client = OzonAPIClient()
    found: list[tuple[int, str, str]] = []  # кластеры со слотами
    results: list[tuple[int, str, str]] = []  # по каждому кластеру (для логов)
    image_rows: list[tuple[int, str, str, list[int]]] = []  # (cluster_id, name, dates_text, day_counts) для картинки

    for cluster_id in cluster_ids:
        cid = int(cluster_id) if cluster_id is not None else None
        if cid is None:
            continue
        cluster_name = cluster_by_id.get(cid, f"Кластер {cid}")

        payload = copy.deepcopy(base_body)
        if "cluster_info" not in payload:
            payload["cluster_info"] = {}
        payload["cluster_info"] = dict(payload["cluster_info"])
        payload["cluster_info"]["macrolocal_cluster_id"] = cid
        payload["cluster_info"]["items"] = [dict(it) for it in items]

        draft_created_at = time.monotonic()

        try:
            resp = await client.create_crossdock_draft_raw(payload)
            draft_id = resp.get("draft_id")
            if (draft_id is None or draft_id == 0) and isinstance(resp.get("result"), dict):
                draft_id = resp.get("result").get("draft_id")
            if not draft_id or draft_id == 0 or resp.get("errors"):
                logger.debug(
                    "Slots tracker cluster %s: ошибка создания черновика",
                    cid,
                )
                results.append((cid, cluster_name, "ошибка создания черновика"))
                image_rows.append((cid, cluster_name, "ошибка", [-1] * period_days))
                await asyncio.sleep(
                    max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at))
                )
                continue
        except Exception as e:
            logger.warning("Slots tracker cluster %s: create draft failed: %s", cid, e)
            results.append((cid, cluster_name, "ошибка: " + str(e)[:80]))
            image_rows.append((cid, cluster_name, "ошибка", [-1] * period_days))
            await asyncio.sleep(
                max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at))
            )
            continue

        await asyncio.sleep(AFTER_DRAFT_WAIT_SEC)

        success = False
        failed_no_slots = False
        for _ in range(STATUS_POLL_MAX_ATTEMPTS):
            info = await client.get_draft_info(str(draft_id))
            if info.get("_error"):
                await asyncio.sleep(STATUS_POLL_INTERVAL_SEC)
                continue
            payload_res = info.get("result") or info.get("data") or info
            st = (
                info.get("status")
                or info.get("state")
                or (payload_res.get("status") if isinstance(payload_res, dict) else None)
                or (payload_res.get("state") if isinstance(payload_res, dict) else None)
            )
            st = (st or "").strip().upper()
            if st == "SUCCESS":
                success = True
                break
            if st == "FAILED":
                err_list = (
                    info.get("errors")
                    or (payload_res.get("errors") if isinstance(payload_res, dict) else None)
                    or []
                )
                for e in err_list:
                    msg = (e.get("error_message") or "") if isinstance(e, dict) else ""
                    if msg == "DROP_OFF_POINT_HAS_NO_TIMESLOTS":
                        failed_no_slots = True
                        break
                if failed_no_slots:
                    break
            await asyncio.sleep(STATUS_POLL_INTERVAL_SEC)

        if failed_no_slots or not success:
            results.append((cid, cluster_name, "нет слотов (черновик не готов)"))
            image_rows.append((cid, cluster_name, "нет слотов", [0] * period_days))
            await asyncio.sleep(
                max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at))
            )
            continue

        await asyncio.sleep(TIMESLOT_RATE_SEC)

        try:
            ts_resp = None
            for retry in range(TIMESLOT_429_MAX_RETRIES + 1):
                ts_resp = await client.get_draft_timeslots(
                    draft_id=int(draft_id),
                    date_from=today_str,
                    date_to=to_str,
                    supply_type="CROSSDOCK",
                    selected_cluster_warehouses=[{"macrolocal_cluster_id": cid}],
                )
                if not ts_resp.get("_error"):
                    break
                if (
                    ts_resp.get("status_code") == 429
                    and retry < TIMESLOT_429_MAX_RETRIES
                ):
                    await asyncio.sleep(TIMESLOT_429_RETRY_DELAY_SEC)
                    continue
                break
            if ts_resp.get("_error"):
                results.append((cid, cluster_name, "ошибка запроса таймслотов"))
                image_rows.append((cid, cluster_name, "ошибка", [-1] * period_days))
                await asyncio.sleep(
                    max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at))
                )
                continue
            dates_text, day_counts = _parse_dates_and_counts_in_period(
                ts_resp, today_str, period_days
            )
            results.append((cid, cluster_name, dates_text))
            image_rows.append((cid, cluster_name, dates_text, day_counts))
            if _has_slots_in_period(day_counts):
                found.append((cid, cluster_name, dates_text))
                logger.info(
                    "Slots tracker: слоты в периоде — кластер %s (%s): %s",
                    cid, cluster_name, dates_text,
                )
        except Exception as e:
            logger.warning(
                "Slots tracker cluster %s: timeslots exception %s",
                cid, e, exc_info=True,
            )
            results.append((cid, cluster_name, "ошибка: " + str(e)[:80]))
            image_rows.append((cid, cluster_name, "ошибка", [-1] * period_days))

        await asyncio.sleep(
            max(0, DRAFT_CREATE_INTERVAL_SEC - (time.monotonic() - draft_created_at))
        )

    # Отправляем результаты ТОЛЬКО по кластерам, где есть слоты:
    # по одному сообщению (картинка с одной строкой) на каждый кластер.
    logger.info(
        "Slots tracker: проверено кластеров=%s, со слотами=%s, результаты по кластерам: %s",
        len(cluster_ids),
        len(found),
        [(r[0], r[1], r[2]) for r in results],
    )
    if not found:
        logger.info("Slots tracker: слоты в выбранном периоде не найдены — уведомления не отправляем")
        return

    try:
        from app.telegram.bot import send_report_photo, send_report_message

        time_str = now_msk().strftime("%d.%m.%Y %H:%M")

        # Для каждого кластера со слотами ищем его строку в image_rows
        for cid, name, dates_text in found:
            row = None
            for r in image_rows:
                if r[0] == cid:
                    row = r
                    break
            if not row:
                # На всякий случай — если нет строки для картинки, шлём текст
                text = f"Отслеживатель — {time_str} МСК\n{name} (id {cid}): {dates_text}"
                await send_report_message(text)
                continue
            caption = f"Отслеживатель — {name} (id {cid}), {time_str} МСК"
            photo_bytes = _build_tracker_table_image([row], today_str, period_days)
            if photo_bytes:
                await send_report_photo(photo=photo_bytes, caption=caption)
                logger.info(
                    "Slots tracker: картинка в Telegram отправлена для кластера %s (%s)",
                    cid,
                    name,
                )
            else:
                text = f"Отслеживатель — {time_str} МСК\n{name} (id {cid}): {dates_text}"
                await send_report_message(text)
                logger.warning("Slots tracker: картинка не построена, отправлен текст для кластера %s", cid)
    except Exception as e:
        logger.warning("Slots tracker: отправка в Telegram не удалась: %s", e)


async def run_slots_tracker_safe() -> None:
    """
    Запуск отслеживателя под общей блокировкой Ozon (не вместе с парсером).
    Если блокировка занята (выполняется парсер) — запуск откладывается на TRACKER_DEFER_SECONDS.
    После выполнения обновляет last_run_at.
    """
    from app.modules.ozon.runner import ozon_runner_lock, TRACKER_DEFER_SECONDS

    if ozon_runner_lock.locked():
        logger.info(
            "Отслеживатель слотов: парсер или другая задача Ozon занята, откладываем запуск на %s с",
            TRACKER_DEFER_SECONDS,
        )
        asyncio.create_task(_deferred_slots_tracker())
        return
    async with ozon_runner_lock:
        await run_slots_tracker()
        async with AsyncSessionLocal() as session:
            r = await session.execute(
                select(SlotsTrackerConfig).where(SlotsTrackerConfig.enabled == 1).limit(1)
            )
            row = r.scalar_one_or_none()
            if row:
                row.last_run_at = now_msk()
                await session.commit()


async def _deferred_slots_tracker() -> None:
    """Отложенный запуск отслеживателя после паузы (когда освободится блокировка)."""
    from app.modules.ozon.runner import TRACKER_DEFER_SECONDS

    await asyncio.sleep(TRACKER_DEFER_SECONDS)
    await run_slots_tracker_safe()


async def run_slots_tracker_if_due() -> None:
    """
    Проверяет, пора ли запускать отслеживатель (по frequency_hours и last_run_at).
    Если да — вызывает run_slots_tracker_safe (с разведением по времени с парсером).
    Вызывается планировщиком раз в час. Сравнение по секундам, допуск 1 мин.
    """
    async with AsyncSessionLocal() as session:
        config = await _get_tracker_config(session)
        if not config:
            logger.info("Slots tracker (по расписанию): конфиг отключён или отсутствует, пропуск")
            return
        now = now_msk()
        last = config.last_run_at
        if last is not None and last.tzinfo is None:
            last = last.replace(tzinfo=MSK)
        frequency_hours = max(1, int(config.frequency_hours) if config.frequency_hours else 1)
        required_seconds = frequency_hours * 3600
        # Допуск 1 минута: запускаем, если прошло хотя бы (N часов − 1 мин), чтобы не пропустить из‑за сдвига времени
        threshold_seconds = max(0, required_seconds - 60)
        if last:
            delta_seconds = (now - last).total_seconds()
            if delta_seconds < threshold_seconds:
                logger.info(
                    "Slots tracker (по расписанию): ещё рано — прошло %.0f с (нужно >= %s с, частота %s ч), пропуск",
                    delta_seconds,
                    threshold_seconds,
                    frequency_hours,
                )
                return
        logger.info(
            "Slots tracker (по расписанию): запуск (последний запуск %s)",
            config.last_run_at.isoformat() if config.last_run_at else "никогда",
        )
    await run_slots_tracker_safe()


async def _get_clusters() -> list[dict]:
    """Список кластеров Ozon для кроссдокинга."""
    client = OzonAPIClient()
    clusters = await client.get_cluster_list(cluster_type="CLUSTER_TYPE_OZON")
    return clusters or []
