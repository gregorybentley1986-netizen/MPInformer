"""
Модуль формирования и отправки аналитических отчетов
"""
import asyncio
from calendar import monthrange
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict

from dateutil import tz, parser as dt_parser
from loguru import logger
from app.modules.ozon.api_client import OzonAPIClient
from app.modules.wildberries.api_client import WildberriesAPIClient
from app.telegram.bot import send_report_with_logos

# Часовой пояс для отчётов: московское время (МСК, UTC+3)
MSK = tz.gettz("Europe/Moscow")


def _get(obj: dict, *keys: str):
    """Взять значение по первому существующему ключу (snake_case / camelCase)."""
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return None


def _parse_wb_order_date(order: dict) -> Optional[datetime]:
    """Дата создания заказа WB (поле date). С суффиксом Z — UTC, иначе МСК по доке."""
    raw = _get(order, "date", "Date")
    if not raw:
        return None
    try:
        if isinstance(raw, datetime):
            dt = raw
        else:
            s = str(raw).strip()
            # Z или +00:00 — UTC; иначе МСК
            if s.upper().endswith("Z") or "+00:00" in s:
                s_clean = s.replace("Z", "").replace("z", "").replace("+00:00", "")[:19]
                dt = datetime.strptime(s_clean, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            else:
                s = s[:19] if len(s) > 19 else s
                dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=MSK)
        return dt.astimezone(MSK)
    except (ValueError, TypeError):
        return None


def _parse_wb_last_change_date(order: dict) -> Optional[datetime]:
    """lastChangeDate заказа WB в МСК."""
    raw = order.get("lastChangeDate")
    if not raw:
        return None
    try:
        s = str(raw).strip()
        if len(s) > 19:
            s = s[:19]
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        return dt.astimezone(MSK)
    except (ValueError, TypeError):
        return None


def _filter_wb_orders_by_date(
    orders: list,
    date_from: datetime,
    date_to: datetime,
) -> list:
    """Оставить только заказы с датой создания (date) в диапазоне [date_from, date_to] по МСК."""
    if not date_from.tzinfo:
        date_from = date_from.replace(tzinfo=MSK)
    else:
        date_from = date_from.astimezone(MSK)
    if not date_to.tzinfo:
        date_to = date_to.replace(tzinfo=MSK)
    else:
        date_to = date_to.astimezone(MSK)
    out = []
    for o in orders:
        dt = _parse_wb_order_date(o)
        if dt is not None and date_from <= dt <= date_to:
            out.append(o)
    return out


def _filter_wb_orders_by_last_change_date(
    orders: list,
    date_from: datetime,
    date_to: datetime,
) -> list:
    """Оставить только заказы с lastChangeDate в диапазоне [date_from, date_to] по МСК."""
    if not date_from.tzinfo:
        date_from = date_from.replace(tzinfo=MSK)
    else:
        date_from = date_from.astimezone(MSK)
    if not date_to.tzinfo:
        date_to = date_to.replace(tzinfo=MSK)
    else:
        date_to = date_to.astimezone(MSK)
    out = []
    for o in orders:
        dt = _parse_wb_last_change_date(o)
        if dt is not None and date_from <= dt <= date_to:
            out.append(o)
    return out


def _log_wb_orders_structure(
    raw_orders: list,
    filtered_orders: list,
    deduped_orders: list,
    date_from: datetime,
    date_to: datetime,
) -> None:
    """Логирует структуру формирования числа заказов WB."""
    df = date_from.astimezone(MSK) if date_from.tzinfo else date_from.replace(tzinfo=MSK)
    dt = date_to.astimezone(MSK) if date_to.tzinfo else date_to.replace(tzinfo=MSK)
    lines = [
        "━━━ WB заказы: структура формирования ━━━",
        f"Период фильтра (МСК): {df.strftime('%d.%m.%Y %H:%M:%S')} — {dt.strftime('%d.%m.%Y %H:%M:%S')}",
        "Фильтр: по дате создания заказа (date)",
        f"1. Сырых с API: {len(raw_orders)}",
        f"2. После фильтра по date: {len(filtered_orders)}",
        f"3. После дедупликации (odid/srid/gNumber): {len(deduped_orders)}",
        "Примеры date (первые 5):",
    ]
    for i, o in enumerate(raw_orders[:5]):
        raw_val = o.get("date", "—")
        parsed = _parse_wb_order_date(o)
        in_range = "✓" if (parsed and df <= parsed <= dt) else "✗"
        lines.append(f"   [{i+1}] raw={raw_val} → parsed={parsed} {in_range}")
    if len(raw_orders) > 5:
        lines.append(f"   ... и ещё {len(raw_orders) - 5}")
    lines.append(f"→ Итог в отчёте: {len(deduped_orders)} заказов")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("\n".join(lines))


def _dedupe_wb_orders(orders: list) -> list:
    """Убрать дубли заказов WB по odid/srid/gNumber (при пагинации одна строка может повториться)."""
    seen = set()
    out = []
    for o in orders:
        uid = o.get("odid") or o.get("srid") or o.get("gNumber")
        if uid is not None:
            if uid in seen:
                continue
            seen.add(uid)
        out.append(o)
    return out


def _ozon_article_key(product: dict) -> str:
    """Артикул продавца из товара Ozon: offer_id / offerId, иначе sku."""
    for key in ("offer_id", "offerId", "offer_id_str", "sku", "Sku"):
        val = product.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


async def get_today_sales_by_article() -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Продажи за сегодня (с 00:00 МСК до текущего момента): артикул -> количество.
    Возвращает (ozon_sold: dict, wb_sold: dict). Используется для главной страницы и отчётов.
    """
    now_msk = datetime.now(MSK)
    date_from_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    date_to_msk = now_msk
    date_from_utc = date_from_msk.astimezone(tz.UTC)
    date_to_utc = date_to_msk.astimezone(tz.UTC)
    date_from_ozon = date_from_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to_ozon = date_to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    ozon_sold: Dict[str, int] = {}
    wb_sold: Dict[str, int] = {}

    try:
        ozon_client = OzonAPIClient()
        ozon_orders_data = await ozon_client.get_orders(
            since=date_from_ozon, to=date_to_ozon, limit=1000
        )
        for order in ozon_orders_data or []:
            try:
                fin_data = _get(order, "financial_data", "financialData") or {}
                fin_products = _get(fin_data, "products", "Products") or []
                top_products = _get(order, "products", "Products") or []
                article_price_qty = []
                if fin_products and top_products and len(fin_products) == len(top_products):
                    for i, fin_p in enumerate(fin_products):
                        qty = int(_get(fin_p, "quantity", "Quantity") or 0) or 1
                        art = _ozon_article_key(top_products[i]) if i < len(top_products) else _ozon_article_key(fin_p)
                        if art:
                            article_price_qty.append((art, qty))
                elif fin_products:
                    for p in fin_products:
                        qty = int(_get(p, "quantity", "Quantity") or 0) or 1
                        art = _ozon_article_key(p)
                        if art:
                            article_price_qty.append((art, qty))
                if not article_price_qty and top_products:
                    for p in top_products:
                        qty = int(_get(p, "quantity", "Quantity") or 0) or 1
                        art = _ozon_article_key(p)
                        if art:
                            article_price_qty.append((art, qty))
                for art, qty in article_price_qty:
                    ozon_sold[art] = ozon_sold.get(art, 0) + qty
            except (KeyError, TypeError, ValueError):
                pass
    except Exception as e:
        logger.warning(f"get_today_sales_by_article Ozon: {e}")

    try:
        wb_client = WildberriesAPIClient()
        wb_orders_data = await wb_client.get_orders(
            date_from=date_from_msk, date_to=date_to_msk, limit=1000
        )
        filtered = _filter_wb_orders_by_date(wb_orders_data or [], date_from_msk, date_to_msk)
        wb_orders = _dedupe_wb_orders(filtered)
        for order in wb_orders:
            try:
                qty = 1
                for key in ("quantity", "totalQuantity", "count"):
                    if key in order and order[key] is not None:
                        try:
                            qty = int(order[key])
                            if qty < 1:
                                qty = 1
                            break
                        except (TypeError, ValueError):
                            pass
                art = (order.get("supplierArticle") or order.get("supplier_article") or "").strip()
                if art:
                    wb_sold[art] = wb_sold.get(art, 0) + qty
            except (KeyError, TypeError, ValueError):
                pass
    except Exception as e:
        logger.warning(f"get_today_sales_by_article WB: {e}")

    return ozon_sold, wb_sold


def _empty_sales_tuple() -> Tuple[
    Dict[str, int], Dict[str, int], Dict[str, int], Dict[str, int],
    Dict[str, int], Dict[str, int], Dict[str, int], Dict[str, int],
]:
    empty = {}
    return (empty, empty, empty, empty, empty, empty, empty, empty)


async def get_sales_for_main_page() -> Tuple[
    Dict[str, int], Dict[str, int], Dict[str, int], Dict[str, int],
    Dict[str, int], Dict[str, int], Dict[str, int], Dict[str, int],
]:
    """
    Продажи для главной страницы: сегодня, неделя, вчера, прошлая неделя.
    Возвращает (ozon_today, wb_today, ozon_week, wb_week,
                ozon_yesterday, wb_yesterday, ozon_prev_week, wb_prev_week).
    """
    try:
        return await _get_sales_for_main_page_impl()
    except Exception as e:
        logger.warning("get_sales_for_main_page: %s", e, exc_info=True)
        return _empty_sales_tuple()


async def _get_sales_for_main_page_impl() -> Tuple[
    Dict[str, int], Dict[str, int], Dict[str, int], Dict[str, int],
    Dict[str, int], Dict[str, int], Dict[str, int], Dict[str, int],
]:
    now_msk = datetime.now(MSK)
    date_today_start = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    date_yesterday_start = date_today_start - timedelta(days=1)
    date_week_start = date_today_start - timedelta(days=7)
    date_prev_week_start = date_week_start - timedelta(days=7)
    date_from_utc = date_prev_week_start.astimezone(tz.UTC)
    date_to_utc = now_msk.astimezone(tz.UTC)
    date_from_ozon = date_from_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to_ozon = date_to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    ozon_today: Dict[str, int] = {}
    ozon_week: Dict[str, int] = {}
    ozon_yesterday: Dict[str, int] = {}
    ozon_prev_week: Dict[str, int] = {}
    wb_today: Dict[str, int] = {}
    wb_week: Dict[str, int] = {}
    wb_yesterday: Dict[str, int] = {}
    wb_prev_week: Dict[str, int] = {}

    def _add_ozon_order_to_maps(
        order: dict,
        day_start: datetime,
        week_start: datetime,
        yesterday_start: datetime,
        prev_week_start: datetime,
    ) -> None:
        try:
            fin_data = _get(order, "financial_data", "financialData") or {}
            fin_products = _get(fin_data, "products", "Products") or []
            top_products = _get(order, "products", "Products") or []
            article_qty = []
            if fin_products and top_products and len(fin_products) == len(top_products):
                for i, fin_p in enumerate(fin_products):
                    qty = int(_get(fin_p, "quantity", "Quantity") or 0) or 1
                    art = _ozon_article_key(top_products[i]) if i < len(top_products) else _ozon_article_key(fin_p)
                    if art:
                        article_qty.append((art, qty))
            elif fin_products:
                for p in fin_products:
                    qty = int(_get(p, "quantity", "Quantity") or 0) or 1
                    art = _ozon_article_key(p)
                    if art:
                        article_qty.append((art, qty))
            if not article_qty and top_products:
                for p in top_products:
                    qty = int(_get(p, "quantity", "Quantity") or 0) or 1
                    art = _ozon_article_key(p)
                    if art:
                        article_qty.append((art, qty))
            if not article_qty:
                return
            # Дата заказа Ozon (FBO): in_process_at / inProcessAt, created_at / createdAt, shipment_date / shipmentDate
            order_dt = None
            for key in ("in_process_at", "inProcessAt", "shipment_date", "shipmentDate", "created_at", "createdAt"):
                raw = order.get(key)
                if raw:
                    try:
                        s = str(raw).strip()[:19].replace("Z", "")
                        order_dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=tz.UTC).astimezone(MSK)
                        break
                    except (ValueError, TypeError):
                        pass
            if order_dt is None:
                order_dt = date_week_start
            for art, qty in article_qty:
                if order_dt >= week_start:
                    ozon_week[art] = ozon_week.get(art, 0) + qty
                if order_dt >= prev_week_start and order_dt < week_start:
                    ozon_prev_week[art] = ozon_prev_week.get(art, 0) + qty
                if order_dt >= day_start:
                    ozon_today[art] = ozon_today.get(art, 0) + qty
                if order_dt >= yesterday_start and order_dt < day_start:
                    ozon_yesterday[art] = ozon_yesterday.get(art, 0) + qty
        except (KeyError, TypeError, ValueError, AttributeError):
            pass

    try:
        ozon_client = OzonAPIClient()
        # Как в отчёте Telegram: запрос с since/to (UTC). При 0 ответе api_client сам запрашивает без фильтра.
        ozon_orders_data = await ozon_client.get_orders(
            since=date_from_ozon, to=date_to_ozon, limit=1000
        )
        for order in ozon_orders_data or []:
            if not isinstance(order, dict):
                continue
            try:
                _add_ozon_order_to_maps(
                    order, date_today_start, date_week_start,
                    date_yesterday_start, date_prev_week_start,
                )
            except (KeyError, TypeError, ValueError, AttributeError):
                pass
    except Exception as e:
        logger.warning("get_sales_for_main_page Ozon: %s", e, exc_info=True)

    try:
        wb_client = WildberriesAPIClient()
        # Как в отчёте: date_from/date_to по lastChangeDate, затем фильтр по date (создание) в коде
        wb_orders_data = await wb_client.get_orders(
            date_from=date_prev_week_start, date_to=now_msk, limit=1000
        )
        filtered_week = _filter_wb_orders_by_date(wb_orders_data or [], date_week_start, now_msk)
        filtered_today = _filter_wb_orders_by_date(wb_orders_data or [], date_today_start, now_msk)
        filtered_yesterday = _filter_wb_orders_by_date(
            wb_orders_data or [], date_yesterday_start, date_today_start - timedelta(seconds=1)
        )
        filtered_prev_week = _filter_wb_orders_by_date(
            wb_orders_data or [], date_prev_week_start, date_week_start - timedelta(seconds=1)
        )
        wb_orders_week = _dedupe_wb_orders(filtered_week)
        wb_orders_today = _dedupe_wb_orders(filtered_today)
        wb_orders_yesterday = _dedupe_wb_orders(filtered_yesterday)
        wb_orders_prev_week = _dedupe_wb_orders(filtered_prev_week)
        for order in wb_orders_week:
            try:
                qty = int(_get(order, "quantity", "totalQuantity", "count", "Quantity") or 1)
                if qty < 1:
                    qty = 1
                art = (_get(order, "supplierArticle", "supplier_article") or "").strip()
                if art:
                    wb_week[art] = wb_week.get(art, 0) + qty
            except (KeyError, TypeError, ValueError):
                pass
        for order in wb_orders_today:
            try:
                qty = int(_get(order, "quantity", "totalQuantity", "count", "Quantity") or 1)
                if qty < 1:
                    qty = 1
                art = (_get(order, "supplierArticle", "supplier_article") or "").strip()
                if art:
                    wb_today[art] = wb_today.get(art, 0) + qty
            except (KeyError, TypeError, ValueError):
                pass
        for order in wb_orders_yesterday:
            try:
                qty = int(_get(order, "quantity", "totalQuantity", "count", "Quantity") or 1)
                if qty < 1:
                    qty = 1
                art = (_get(order, "supplierArticle", "supplier_article") or "").strip()
                if art:
                    wb_yesterday[art] = wb_yesterday.get(art, 0) + qty
            except (KeyError, TypeError, ValueError):
                pass
        for order in wb_orders_prev_week:
            try:
                qty = int(_get(order, "quantity", "totalQuantity", "count", "Quantity") or 1)
                if qty < 1:
                    qty = 1
                art = (_get(order, "supplierArticle", "supplier_article") or "").strip()
                if art:
                    wb_prev_week[art] = wb_prev_week.get(art, 0) + qty
            except (KeyError, TypeError, ValueError):
                pass
    except Exception as e:
        logger.warning("get_sales_for_main_page WB: %s", e, exc_info=True)

    return (
        ozon_today, wb_today, ozon_week, wb_week,
        ozon_yesterday, wb_yesterday, ozon_prev_week, wb_prev_week,
    )


async def get_daily_chart_data() -> list:
    """
    Разбивка по дням за последние 7 суток для графика: список из 7 элементов
    [{date_str, ozon_qty, ozon_sum, wb_qty, wb_sum}, ...], от старых к новым.
    """
    try:
        return await _get_daily_chart_data_impl()
    except Exception as e:
        logger.warning("get_daily_chart_data: %s", e, exc_info=True)
        return _empty_chart_data_7_days()


def _empty_chart_data_7_days() -> list:
    now_msk = datetime.now(MSK)
    date_today_start = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    return [
        {
            "date_str": (date_today_start - timedelta(days=6 - i)).strftime("%d.%m"),
            "ozon_qty": 0, "ozon_sum": 0.0, "wb_qty": 0, "wb_sum": 0.0,
        }
        for i in range(7)
    ]


async def _get_daily_chart_data_impl() -> list:
    now_msk = datetime.now(MSK)
    date_today_start = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    date_from = date_today_start - timedelta(days=6)
    date_to = now_msk
    date_from_utc = date_from.astimezone(tz.UTC)
    date_to_utc = date_to.astimezone(tz.UTC)
    date_from_ozon = date_from_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to_ozon = date_to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # day_key -> {ozon_qty, ozon_sum, wb_qty, wb_sum}
    by_day: Dict[str, Dict[str, float]] = {}
    for i in range(7):
        d = date_today_start - timedelta(days=6 - i)
        by_day[d.strftime("%Y-%m-%d")] = {"ozon_qty": 0, "ozon_sum": 0, "wb_qty": 0, "wb_sum": 0}

    try:
        ozon_client = OzonAPIClient()
        # Как в отчёте Telegram: запрос с since/to за 7 дней (UTC). При 0 ответе api_client запрашивает без фильтра.
        ozon_orders = await ozon_client.get_orders(since=date_from_ozon, to=date_to_ozon, limit=1000)
        for order in ozon_orders or []:
            if not isinstance(order, dict):
                continue
            try:
                fin_data = _get(order, "financial_data", "financialData") or {}
                fin_products = _get(fin_data, "products", "Products") or []
                top_products = _get(order, "products", "Products") or []
                order_qty, order_sum = 0, 0.0
                if fin_products and top_products and len(fin_products) == len(top_products):
                    for i, fin_p in enumerate(fin_products):
                        qty = int(_get(fin_p, "quantity", "Quantity") or 0) or 1
                        price = float(_get(fin_p, "price", "Price") or 0)
                        order_qty += qty
                        order_sum += price * qty
                elif fin_products:
                    for p in fin_products:
                        qty = int(_get(p, "quantity", "Quantity") or 0) or 1
                        price = float(_get(p, "price", "Price") or 0)
                        order_qty += qty
                        order_sum += price * qty
                if order_qty == 0 and top_products:
                    for p in top_products:
                        qty = int(_get(p, "quantity", "Quantity") or 0) or 1
                        price = float(_get(p, "price", "Price") or 0)
                        order_qty += qty
                        order_sum += price * qty
                if order_qty == 0:
                    continue
                order_dt = None
                for key in ("in_process_at", "inProcessAt", "shipment_date", "shipmentDate", "created_at", "createdAt"):
                    raw = order.get(key)
                    if raw:
                        try:
                            s = str(raw).strip()[:19].replace("Z", "")
                            order_dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=tz.UTC).astimezone(MSK)
                            break
                        except (ValueError, TypeError):
                            pass
                if order_dt is None:
                    continue
                day_key = order_dt.strftime("%Y-%m-%d")
                if day_key in by_day:
                    by_day[day_key]["ozon_qty"] += order_qty
                    by_day[day_key]["ozon_sum"] += order_sum
            except (KeyError, TypeError, ValueError, AttributeError):
                pass
    except Exception as e:
        logger.warning("get_daily_chart_data Ozon: %s", e, exc_info=True)

    try:
        wb_client = WildberriesAPIClient()
        # Как в отчёте: date_from/date_to (МСК), затем фильтр по date в коде
        wb_orders_data = await wb_client.get_orders(date_from=date_from, date_to=date_to, limit=1000)
        filtered = _filter_wb_orders_by_date(wb_orders_data or [], date_from, date_to)
        for order in _dedupe_wb_orders(filtered):
            try:
                qty = int(_get(order, "quantity", "totalQuantity", "count", "Quantity") or 1)
                if qty < 1:
                    qty = 1
                price = float(_get(order, "priceWithDisc", "totalPrice", "price", "Price") or 0)
                dt = _parse_wb_order_date(order)
                if dt is None:
                    continue
                day_key = dt.strftime("%Y-%m-%d")
                if day_key in by_day:
                    by_day[day_key]["wb_qty"] += qty
                    by_day[day_key]["wb_sum"] += price
            except (KeyError, TypeError, ValueError):
                pass
    except Exception as e:
        logger.warning("get_daily_chart_data WB: %s", e, exc_info=True)

    result = []
    for i in range(7):
        d = date_today_start - timedelta(days=6 - i)
        day_key = d.strftime("%Y-%m-%d")
        r = by_day.get(day_key, {})
        result.append({
            "date_str": d.strftime("%d.%m"),
            "ozon_qty": int(r.get("ozon_qty", 0) or 0),
            "ozon_sum": round(float(r.get("ozon_sum", 0) or 0), 2),
            "wb_qty": int(r.get("wb_qty", 0) or 0),
            "wb_sum": round(float(r.get("wb_sum", 0) or 0), 2),
        })
    return result


# Русские названия месяцев для таблицы выплат
_MONTH_NAMES = (
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
)

# Кэш выплат: (data, updated_at). Обновление только по кнопке или в 00:00.
_payout_cache: dict | None = None


def _ozon_op_amount(op: dict) -> float:
    """Сумма операции Ozon. amount > 0 — приход (выплата)."""
    val = op.get("amount") or op.get("Amount")
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def _ozon_op_is_payout(op: dict) -> bool:
    """Только переводы продавцу (выплаты), не начисления по продажам."""
    ot = (op.get("operation_type") or op.get("OperationType") or "").strip()
    otn = (op.get("operation_type_name") or op.get("OperationTypeName") or "").lower()
    # Известные типы выплат в API Ozon
    if ot in (
        "SellerTransferPayout", "SellerTransfer", "Transfer",
        "ClientTransfer", "MarketplaceTransfer", "SellerPayout", "Payout",
        "SellerTransferPayoutRequest",
    ):
        return True
    # По названию (рус/англ): перевод, выплата, перечисление, селлеру
    if any(
        x in otn
        for x in (
            "перевод", "выплата", "payout", "transfer",
            "перечисление", "перечисл", "селлеру", "seller",
        )
    ):
        return True
    return False


def _ozon_op_date(op: dict) -> datetime | None:
    """Дата операции Ozon в МСК."""
    raw = op.get("operation_date") or op.get("operationDate")
    if not raw:
        return None
    try:
        s = str(raw).strip()[:19].replace("Z", "+00:00")
        if "+" not in s and "Z" not in str(raw):
            s = s + "+00:00"
        dt = dt_parser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(MSK)
    except (ValueError, TypeError):
        return None


def _wb_row_is_payout(row: dict) -> bool:
    """Строка про перевод/выплату (в reportDetailByPeriod таких может не быть)."""
    doc = (row.get("doc_type_name") or row.get("docTypeName") or "").strip().lower()
    oper = (row.get("supplier_oper_name") or row.get("supplierOperName") or "").strip().lower()
    if "перевод" in doc or "выплата" in doc or "перевод" in oper or "выплата" in oper:
        return True
    if "transfer" in doc or "payout" in doc or "transfer" in oper or "payout" in oper:
        return True
    if "перечисление" in doc or "перечисл" in oper or "продавц" in oper:
        return True
    return False


def _wb_row_ppvz_for_pay(row: dict) -> float:
    """Сумма «К перечислению продавцу» из строки (начисление/удержание по операции)."""
    for key in ("ppvz_for_pay", "ppvzForPay", "for_pay", "forPay"):
        val = row.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _wb_row_retail(row: dict) -> float:
    """Сумма «Вайлдберриз реализовал товар» (что заплатил покупатель) — валовая реализация."""
    for key in (
        "retail_amount", "retailAmount", "sale_amount", "saleAmount",
        "realization_sum", "price_with_disc", "priceWithDisc",
    ):
        val = row.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _wb_row_amount(row: dict) -> float:
    """Сумма перевода/выплаты из строки WB. Для выплат — ищем поле с суммой перевода."""
    for key in (
        "payment_amount", "paymentAmount", "sum_sale", "sumSale",
        "for_pay", "forPay", "ppvz_for_pay", "ppvzForPay",
        "retail_amount", "retailAmount", "penalty", "additional_payment", "additionalPayment",
    ):
        val = row.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _wb_row_date(row: dict) -> datetime | None:
    """Дата операции WB в МСК (дата продажи/реализации)."""
    raw = _get(
        row,
        "rrd_date", "rrdDate",  # дата реализации
        "date_from", "dateFrom", "operation_dt", "operationDt",
        "date", "sale_dt", "saleDate", "realization_date",
    )
    if not raw:
        return None
    try:
        s = str(raw).strip()[:19].replace("Z", "")
        if "T" in s:
            dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        else:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        return dt.astimezone(MSK)
    except (ValueError, TypeError):
        return None


def _by_date_to_weekly_payouts(by_date: dict[str, float]) -> list[dict]:
    """Сгруппировать понедельно (пн–вс) и вернуть [{date, realized?, sum}, ...]. realized опционально."""
    return _by_date_to_weekly_payouts_with_realized(by_date, None)


def _by_date_to_weekly_payouts_with_realized(
    by_date_sum: dict[str, float],
    by_date_realized: dict[str, float] | None,
) -> list[dict]:
    """Сгруппировать понедельно; вернуть [{date: "dd.mm–dd.mm", realized: float|None, sum: float}, ...]."""
    from collections import defaultdict
    weekly_sum: dict[str, float] = defaultdict(float)
    weekly_realized: dict[str, float] = defaultdict(float)
    for date_str, s in by_date_sum.items():
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        mon = dt - timedelta(days=dt.weekday())
        week_key = mon.strftime("%Y-%m-%d")
        weekly_sum[week_key] += s
    if by_date_realized:
        for date_str, r in by_date_realized.items():
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            except (ValueError, TypeError):
                continue
            mon = dt - timedelta(days=dt.weekday())
            week_key = mon.strftime("%Y-%m-%d")
            weekly_realized[week_key] += r
    result = []
    for week_key in sorted(weekly_sum.keys()):
        mon = datetime.strptime(week_key, "%Y-%m-%d")
        sun = mon + timedelta(days=6)
        label = f"{mon.strftime('%d.%m')}–{sun.strftime('%d.%m')}"
        r = weekly_realized.get(week_key) or 0
        result.append({
            "date": label,
            "realized": r if by_date_realized else None,
            "sum": weekly_sum[week_key],
        })
    return result


def get_payout_cached() -> dict:
    """
    Вернуть данные выплат из кэша. Без вызова API.
    Если кэш пуст — возвращает пустую структуру.
    """
    global _payout_cache
    if _payout_cache is not None:
        return _payout_cache
    return {"ozon": [], "wb": []}


async def _fetch_and_build_payout_data() -> dict:
    """
    Загрузить выплаты из API и собрать по конкретным датам (группировка по дню).
    Возвращает {ozon: [{month_name, payouts: [{date: "dd.mm", sum}], total}], wb: [...]}.
    """
    from collections import defaultdict

    now = datetime.now(MSK)
    result = {"ozon": [], "wb": []}
    ozon_client = OzonAPIClient()
    wb_client = WildberriesAPIClient()

    for month_offset in (1, 0):  # предыдущий месяц, текущий
        y = now.year
        m = now.month - month_offset
        while m <= 0:
            m += 12
            y -= 1
        _, last_day = monthrange(y, m)
        month_name = f"{_MONTH_NAMES[m - 1]} {y}"
        date_month_start = datetime(y, m, 1, tzinfo=MSK)
        date_month_end = datetime(y, m, last_day, 23, 59, 59, tzinfo=MSK)

        # Ozon: заработок по транзакциям; реализовано (цена×кол-во) по заказам
        ozon_by_date: dict[str, float] = defaultdict(float)
        ozon_realized_by_date: dict[str, float] = defaultdict(float)
        try:
            date_from_utc = date_month_start.astimezone(timezone.utc)
            date_to_utc = date_month_end.astimezone(timezone.utc)
            ozon_ops = await ozon_client.get_transactions(date_from_utc, date_to_utc)
            if ozon_ops and len(ozon_ops) > 0:
                seen_ot = set()
                for op in ozon_ops[:30]:
                    ot = op.get("operation_type") or op.get("OperationType") or ""
                    otn = op.get("operation_type_name") or op.get("OperationTypeName") or ""
                    if (ot, otn) not in seen_ot:
                        seen_ot.add((ot, otn))
                logger.info("Ozon transactions: примеры operation_type / operation_type_name: %s", list(seen_ot)[:15])
            for op in ozon_ops or []:
                amt = _ozon_op_amount(op)
                if amt <= 0:
                    continue
                op_dt = _ozon_op_date(op)
                if op_dt is None:
                    continue
                key = op_dt.strftime("%Y-%m-%d")
                ozon_by_date[key] += amt
            # Реализовано: из заказов (цена × количество) по дате
            date_from_ozon = date_from_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            date_to_ozon = date_to_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            ozon_orders = await ozon_client.get_orders(since=date_from_ozon, to=date_to_ozon, limit=1000)
            for order in ozon_orders or []:
                if not isinstance(order, dict):
                    continue
                try:
                    fin_data = _get(order, "financial_data", "financialData") or {}
                    fin_products = _get(fin_data, "products", "Products") or []
                    top_products = _get(order, "products", "Products") or []
                    order_sum = 0.0
                    if fin_products and top_products and len(fin_products) == len(top_products):
                        for i, fin_p in enumerate(fin_products):
                            qty = int(_get(fin_p, "quantity", "Quantity") or 0) or 1
                            price = float(_get(fin_p, "price", "Price") or 0)
                            order_sum += price * qty
                    elif fin_products:
                        for p in fin_products:
                            order_sum += float(_get(p, "price", "Price") or 0) * (int(_get(p, "quantity", "Quantity") or 0) or 1)
                    if order_sum == 0 and top_products:
                        for p in top_products:
                            order_sum += float(_get(p, "price", "Price") or 0) * (int(_get(p, "quantity", "Quantity") or 0) or 1)
                    if order_sum == 0:
                        continue
                    order_dt = None
                    for key in ("in_process_at", "inProcessAt", "shipment_date", "shipmentDate", "created_at", "createdAt"):
                        raw = order.get(key)
                        if raw:
                            try:
                                s = str(raw).strip()[:19].replace("Z", "")
                                order_dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=tz.UTC).astimezone(MSK)
                                break
                            except (ValueError, TypeError):
                                pass
                    if order_dt is not None:
                        ozon_realized_by_date[order_dt.strftime("%Y-%m-%d")] += order_sum
                except (KeyError, TypeError, ValueError, AttributeError):
                    pass
        except Exception as e:
            logger.warning("get_payout_data Ozon: %s", e)

        payouts_ozon = _by_date_to_weekly_payouts_with_realized(
            dict(ozon_by_date), dict(ozon_realized_by_date) if ozon_realized_by_date and any(ozon_realized_by_date) else None
        )
        total_realized_ozon = sum(ozon_realized_by_date.values()) if ozon_realized_by_date and any(ozon_realized_by_date) else None
        result["ozon"].append({
            "month_name": month_name,
            "payouts": payouts_ozon,
            "total_realized": int(total_realized_ozon) if total_realized_ozon is not None else None,
            "total": int(sum(ozon_by_date.values())),
        })

        # WB — лимит 1 req/min: пауза перед вторым месяцем
        if month_offset == 0:
            await asyncio.sleep(62)
        wb_by_date: dict[str, float] = defaultdict(float)
        wb_retail_by_date: dict[str, float] = defaultdict(float)
        try:
            wb_rows = await wb_client.get_report_detail_by_period(date_month_start, date_month_end)
            seen_types = set()
            for row in wb_rows or []:
                doc = (row.get("doc_type_name") or row.get("docTypeName") or "")
                oper = (row.get("supplier_oper_name") or row.get("supplierOperName") or "")
                if (doc, oper) not in seen_types and len(seen_types) < 20:
                    seen_types.add((doc, oper))
                amt = _wb_row_ppvz_for_pay(row)
                retail = _wb_row_retail(row)
                row_dt = _wb_row_date(row)
                if row_dt is None:
                    row_dt = date_month_start
                key = row_dt.strftime("%Y-%m-%d")
                if amt != 0:
                    wb_by_date[key] += amt
                if retail != 0:
                    wb_retail_by_date[key] += retail
            if (wb_rows or []) and not wb_by_date:
                if len(seen_types) > 0:
                    logger.info("WB выплаты: «к перечислению» по дням не найдено. Примеры doc_type_name/supplier_oper_name: %s", list(seen_types)[:10])
                if wb_rows:
                    logger.info("WB reportDetailByPeriod: ключи первой строки (для отладки поля суммы): %s", list((wb_rows[0] or {}).keys()))
        except Exception as e:
            logger.warning("get_payout_data WB: %s", e)

        payouts_wb = _by_date_to_weekly_payouts_with_realized(
            dict(wb_by_date), dict(wb_retail_by_date) if wb_retail_by_date and any(wb_retail_by_date) else None
        )
        total_retail_wb = sum(wb_retail_by_date.values()) if wb_retail_by_date and any(wb_retail_by_date) else None
        result["wb"].append({
            "month_name": month_name,
            "payouts": payouts_wb,
            "total_realized": int(total_retail_wb) if total_retail_wb is not None else None,
            "total": int(sum(wb_by_date.values())),
        })

    return result


async def refresh_payout_cache() -> dict:
    """
    Принудительно обновить кэш выплат (вызов API).
    Вызывается по кнопке «Обновить» и в 00:00 по расписанию.
    """
    global _payout_cache
    _payout_cache = await _fetch_and_build_payout_data()
    logger.info("Кэш выплат обновлён")
    return _payout_cache


async def collect_and_send_report():
    """
    Собрать данные о заказах с 00:00 текущих суток (МСК) до текущего момента (МСК) и отправить отчёт в Telegram.
    Интервал считается по московскому времени; в API (Ozon, WB) передаётся UTC.
    """
    from app.config import settings
    
    logger.info("Начало сбора данных для отчета...")
    
    try:
        # Явно работаем по московскому времени (МСК, UTC+3)
        now_msk = datetime.now(MSK)
        date_from_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
        date_to_msk = now_msk
        
        logger.info(f"Период отчета (МСК): с 00:00 до {date_to_msk.strftime('%H:%M')} ({date_from_msk.strftime('%d.%m.%Y')} - {date_to_msk.strftime('%d.%m.%Y %H:%M')})")
        
        # В API передаём UTC (Ozon и WB принимают даты в UTC)
        date_from_utc = date_from_msk.astimezone(tz.UTC)
        date_to_utc = date_to_msk.astimezone(tz.UTC)
        date_from_ozon = date_from_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_to_ozon = date_to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.info(f"Ozon API запрос (UTC): since={date_from_ozon}, to={date_to_ozon}")
        
        ozon_client = OzonAPIClient()
        ozon_orders = []
        try:
            ozon_orders_data = await ozon_client.get_orders(
                since=date_from_ozon,
                to=date_to_ozon,
                limit=1000
            )
            ozon_orders = [order for order in ozon_orders_data]
            logger.info(f"Получено {len(ozon_orders)} заказов с Ozon")
        except Exception as e:
            logger.error(f"Ошибка при получении заказов Ozon: {e}")
        
        # WB: API возвращает по lastChangeDate; для отчёта фильтруем по дате создания (date).
        wb_client = WildberriesAPIClient()
        wb_orders = []
        try:
            wb_orders_data = await wb_client.get_orders(
                date_from=date_from_msk,
                date_to=date_to_msk,
                limit=1000
            )
            filtered = _filter_wb_orders_by_date(wb_orders_data, date_from_msk, date_to_msk)
            wb_orders = _dedupe_wb_orders(filtered)
            logger.info(
                "WB: период по дате создания (date) 00:00–сейчас МСК; сырых с API: {}, после фильтра: {}",
                len(wb_orders_data),
                len(wb_orders),
            )
            _log_wb_orders_structure(
                wb_orders_data,
                filtered,
                wb_orders,
                date_from_msk,
                date_to_msk,
            )
        except Exception as e:
            logger.error(f"Ошибка при получении заказов Wildberries: {e}")
        
        # Остатки: Ozon по кластерам (складам) и суммарно, WB по артикулам
        ozon_clusters = []
        ozon_stocks = {}
        wb_stocks = {}
        try:
            ozon_clusters = await ozon_client.get_stocks_by_cluster()
            for c in ozon_clusters:
                for row in c.get("rows") or []:
                    art = (row.get("article") or "").strip()
                    if art:
                        ozon_stocks[art] = ozon_stocks.get(art, 0) + int(row.get("stock") or 0)
        except Exception as e:
            logger.warning(f"Остатки Ozon не загружены: {e}")
        try:
            wb_stocks = await wb_client.get_stocks()
        except Exception as e:
            logger.warning(f"Остатки WB не загружены: {e}")
        
        # Формируем отчет по блокам (для отправки с логотипами маркетплейсов)
        report_parts = format_report(
            ozon_orders, wb_orders, date_from_msk, date_to_msk,
            ozon_stocks=ozon_stocks,
            wb_stocks=wb_stocks,
            ozon_clusters=ozon_clusters,
        )
        
        # Отправляем отчёт в Telegram (логотип + блок для каждого маркетплейса)
        await send_report_with_logos(report_parts)
        
        logger.info("Отчет успешно отправлен")
        
    except Exception as e:
        logger.error(f"Ошибка при формировании отчета: {e}")


def format_report(
    ozon_orders: list,
    wb_orders: list,
    date_from: datetime,
    date_to: datetime,
    ozon_stocks: dict = None,
    wb_stocks: dict = None,
    ozon_clusters: list = None,
) -> str:
    """
    Форматировать отчет в текстовый формат.

    Args:
        ozon_orders: Список заказов Ozon
        wb_orders: Список заказов Wildberries
        date_from: Дата начала периода
        date_to: Дата окончания периода
        ozon_stocks: Остатки Ozon по артикулу (offer_id -> суммарное кол-во)
        wb_stocks: Остатки WB по артикулу (supplierArticle -> кол-во)
        ozon_clusters: Остатки Ozon по складам [{"name": "Склад", "rows": [{"article", "stock"}, ...]}, ...]

    Returns:
        dict с ключами: header, ozon_section, wb_section, footer (для отправки с логотипами)
    """
    ozon_stocks = ozon_stocks or {}
    wb_stocks = wb_stocks or {}
    ozon_clusters = ozon_clusters or []

    def _norm_art(s: str) -> str:
        """Нормализация артикула для сопоставления (пробелы)."""
        return " ".join((s or "").strip().split())

    def _get_stock(stocks: dict, art: str, normalize: bool = False):
        """Остаток по артикулу; при normalize=True — и по нормализованному ключу (только для Ozon)."""
        v = stocks.get(art)
        if v is not None:
            return v
        if normalize:
            v = stocks.get(_norm_art(art))
        return v
    # Подсчитываем статистику по Ozon и разбивку по артикулам продавца (offer_id)
    ozon_orders_count = len(ozon_orders)
    ozon_units = 0
    ozon_amount = 0.0
    ozon_by_article = {}  # артикул -> {"qty": int, "amount": float}

    def _ozon_article_key(product: dict) -> str:
        """Артикул продавца: offer_id / offerId, иначе sku, иначе «Без артикула»."""
        for key in ("offer_id", "offerId", "offer_id_str", "sku"):
            val = product.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
        return "Без артикула"

    logger.debug(f"Обработка {ozon_orders_count} заказов Ozon для расчета суммы, единиц и разбивки по артикулам")

    for idx, order in enumerate(ozon_orders):
        try:
            amount = 0.0
            order_units = 0
            fin_data = _get(order, "financial_data", "financialData") or {}
            fin_products = _get(fin_data, "products", "Products") or []
            top_products = _get(order, "products", "Products") or []

            # В FBO financial_data.products часто без offer_id; артикул берём из order.products по тому же индексу
            article_price_qty = []
            if fin_products and top_products and len(fin_products) == len(top_products):
                for i, fin_p in enumerate(fin_products):
                    price = float(_get(fin_p, "price", "Price") or 0)
                    qty = float(_get(fin_p, "quantity", "Quantity") or 0)
                    art = _ozon_article_key(top_products[i]) if i < len(top_products) else _ozon_article_key(fin_p)
                    article_price_qty.append((art, price, int(qty) or 1))
            elif fin_products:
                for p in fin_products:
                    price = float(_get(p, "price", "Price") or 0)
                    qty = float(_get(p, "quantity", "Quantity") or 0)
                    art = _ozon_article_key(p)
                    article_price_qty.append((art, price, int(qty) or 1))
            if not article_price_qty and top_products:
                for p in top_products:
                    price = float(_get(p, "price", "Price") or 0)
                    qty = float(_get(p, "quantity", "Quantity") or 0)
                    art = _ozon_article_key(p)
                    article_price_qty.append((art, price, int(qty) or 1))
            if not article_price_qty:
                price = float(_get(order, "price", "Price") or 0)
                qty = int(_get(order, "quantity", "Quantity") or 1)
                art = _ozon_article_key(order)
                article_price_qty = [(art, price, qty)]

            for art, price, qty in article_price_qty:
                item_sum = price * qty
                order_units += qty
                amount += item_sum
                entry = ozon_by_article.setdefault(art, {"qty": 0, "amount": 0.0})
                entry["qty"] += qty
                entry["amount"] += item_sum

            if amount == 0 and _get(order, "commission_amount", "commissionAmount") is not None:
                try:
                    amount = float(_get(order, "commission_amount", "commissionAmount") or 0)
                    if amount > 0:
                        order_units = order_units or 1
                except (TypeError, ValueError):
                    pass
            if order_units == 0:
                order_units = 1
            ozon_units += order_units
            ozon_amount += amount

        except (KeyError, TypeError, AttributeError, ValueError) as e:
            logger.warning(f"Ошибка при обработке заказа Ozon #{idx+1}: {e}")
            ozon_units += 1
            continue
    
    # Подсчитываем статистику по Wildberries и разбивку по артикулам продавца (supplierArticle)
    wb_orders_count = len(wb_orders)
    wb_units = 0
    wb_amount = 0.0
    wb_by_article = {}  # артикул -> {"qty": int, "amount": float}

    logger.debug(f"Обработка {wb_orders_count} записей Wildberries для расчета суммы, единиц и разбивки по артикулам")

    for order in wb_orders:
        try:
            qty = int(_get(order, "quantity", "totalQuantity", "count", "Quantity") or 1)
            if qty < 1:
                qty = 1
            price = float(_get(order, "priceWithDisc", "totalPrice", "price", "Price") or 0)
            art = (_get(order, "supplierArticle", "supplier_article") or "").strip() or "Без артикула"
            wb_units += qty
            wb_amount += price
            entry = wb_by_article.setdefault(art, {"qty": 0, "amount": 0.0})
            entry["qty"] += qty
            entry["amount"] += price
        except (KeyError, TypeError, ValueError) as e:
            logger.debug(f"Ошибка при обработке заказа Wildberries: {e}")
            wb_units += 1
            continue
    
    # Общая статистика
    total_units = ozon_units + wb_units
    total_orders = ozon_orders_count + wb_orders_count
    total_amount = ozon_amount + wb_amount
    
    logger.info(f"Итоговая статистика: Ozon - {ozon_orders_count} заказов, {ozon_units} ед., {ozon_amount:.2f} ₽; WB - {wb_orders_count} заказов, {wb_units} ед., {wb_amount:.2f} ₽")
    
    # Остатки Ozon по складам: артикул -> { "склад_name": qty, ... }
    ozon_stock_by_warehouse: dict = {}
    for c in ozon_clusters:
        wh_name = (c.get("name") or "").strip() or "Склад"
        for row in c.get("rows") or []:
            art = (row.get("article") or "").strip()
            if not art:
                continue
            art_norm = _norm_art(art)
            if art_norm not in ozon_stock_by_warehouse:
                ozon_stock_by_warehouse[art_norm] = {}
            ozon_stock_by_warehouse[art_norm][wh_name] = ozon_stock_by_warehouse[art_norm].get(wh_name, 0) + int(row.get("stock") or 0)

    def _ozon_stock_str(article: str) -> str:
        """Строка остатков Ozon: только суммарное количество (без разбивки по складам)."""
        total = _get_stock(ozon_stocks, article, normalize=True)
        if total is not None:
            return f", остаток: {total} шт"
        by_wh = ozon_stock_by_warehouse.get(_norm_art(article)) or ozon_stock_by_warehouse.get(article)
        if by_wh:
            return ", остаток: " + str(sum(by_wh.values())) + " шт"
        return ", остаток: —"

    # Строки по артикулам Ozon (сортируем по артикулу), с остатком на складах
    ozon_article_lines = []
    for art in sorted(ozon_by_article.keys()):
        data = ozon_by_article[art]
        stock_str = _ozon_stock_str(art)
        ozon_article_lines.append(f"• {art} — {data['qty']} шт ({data['amount']:,.0f} ₽{stock_str})")
    ozon_articles_block = "\n".join(ozon_article_lines) if ozon_article_lines else "— нет данных"

    # Строки по артикулам WB (сортируем по артикулу), с остатком на складах
    wb_article_lines = []
    for art in sorted(wb_by_article.keys()):
        data = wb_by_article[art]
        stock = _get_stock(wb_stocks, art, normalize=True)
        stock_str = f", остаток: {stock} шт" if stock is not None else ", остаток: —"
        wb_article_lines.append(f"• {art} — {data['qty']} шт ({data['amount']:,.0f} ₽{stock_str})")
    wb_articles_block = "\n".join(wb_article_lines) if wb_article_lines else "— нет данных"

    header = f"""📊 АНАЛИТИЧЕСКИЙ ОТЧЕТ ПО ЗАКАЗАМ
Период (МСК): с 00:00 по {date_to.strftime('%H:%M')} ({date_to.strftime('%d.%m.%Y')})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

    ozon_section = f"""• Единиц (шт.): {ozon_units}
• Заказов: {ozon_orders_count}
• Сумма: {ozon_amount:,.2f} ₽

По артикулам продавца:
{ozon_articles_block}"""

    wb_section = f"""• Единиц (шт.): {wb_units}
• Заказов: {wb_orders_count}
• Сумма: {wb_amount:,.2f} ₽

По артикулам продавца:
{wb_articles_block}"""

    footer = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 ИТОГО
• Единиц (шт.): {total_units}
• Заказов: {total_orders}
• Общая сумма: {total_amount:,.2f} ₽

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⏰ Отчет сформирован: {date_to.strftime('%d.%m.%Y %H:%M:%S')}"""

    return {
        "header": header.strip(),
        "ozon_section": ozon_section.strip(),
        "wb_section": wb_section.strip(),
        "footer": footer.strip(),
    }
