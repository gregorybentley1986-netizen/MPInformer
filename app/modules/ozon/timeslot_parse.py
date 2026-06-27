"""Разбор ответов Ozon v2/draft/create/info и v2/draft/timeslot/info для парсера слотов."""
from __future__ import annotations

import re
from datetime import datetime, timedelta

from loguru import logger

_DATE_KEY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")


def extract_draft_id(resp: dict | None) -> int | None:
    """draft_id из ответа POST /v1/draft/crossdock/create (верхний уровень или result)."""
    if not isinstance(resp, dict):
        return None
    raw = resp.get("draft_id")
    if (raw is None or raw == 0) and isinstance(resp.get("result"), dict):
        raw = resp["result"].get("draft_id")
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return None
    return val if val > 0 else None


def _result_payload(data: dict | None) -> dict:
    if not isinstance(data, dict):
        return {}
    payload = data.get("result") or data.get("data")
    return payload if isinstance(payload, dict) else {}


def parse_draft_status(info: dict | None) -> str:
    """Статус расчёта черновика из ответа v2/draft/create/info."""
    if not isinstance(info, dict):
        return ""
    payload = _result_payload(info)
    for cand in (
        info.get("status"),
        info.get("state"),
        payload.get("status"),
        payload.get("state"),
    ):
        if cand is not None and str(cand).strip():
            return str(cand).strip().upper()
    return ""


def is_draft_status_ready(status: str) -> bool:
    st = (status or "").strip().upper()
    if not st:
        return False
    if st == "SUCCESS":
        return True
    if st.endswith("_SUCCESS"):
        return True
    if "SUCCESS" in st and "UNSUCCESS" not in st and st != "FAILED":
        return True
    return False


def is_draft_status_in_progress(status: str) -> bool:
    st = (status or "").strip().upper()
    if not st:
        return True
    if is_draft_status_ready(st):
        return False
    if st in ("FAILED", "ERROR", "CANCELLED", "CANCELED"):
        return False
    if "FAILED" in st or st.endswith("_ERROR"):
        return False
    return True


def _iter_draft_errors(info: dict) -> list[dict]:
    out: list[dict] = []
    payload = _result_payload(info)
    for src in (info, payload):
        if not isinstance(src, dict):
            continue
        errs = src.get("errors")
        if isinstance(errs, list):
            for e in errs:
                if isinstance(e, dict):
                    out.append(e)
    return out


def _error_tokens(err: dict) -> set[str]:
    tokens: set[str] = set()
    for key in ("error_message", "message"):
        val = err.get(key)
        if val is not None and str(val).strip():
            tokens.add(str(val).strip().upper())
    reasons = err.get("error_reasons")
    if isinstance(reasons, list):
        for r in reasons:
            if r is not None and str(r).strip():
                tokens.add(str(r).strip().upper())
    return tokens


def is_drop_off_point_has_no_timeslots(info: dict | None) -> bool:
    """Черновик FAILED: на точке отгрузки нет слотов (это не ошибка парсера)."""
    if not isinstance(info, dict):
        return False
    for err in _iter_draft_errors(info):
        if "DROP_OFF_POINT_HAS_NO_TIMESLOTS" in _error_tokens(err):
            return True
    return False


def timeslot_error_reason(data: dict | None) -> str:
    if not isinstance(data, dict):
        return ""
    raw = data.get("error_reason")
    if raw is not None and str(raw).strip() and str(raw).strip().upper() != "UNSPECIFIED":
        return str(raw).strip()
    return ""


def _day_date_key(day: dict) -> str:
    if not isinstance(day, dict):
        return ""
    for field in ("date_in_timezone", "date", "day"):
        raw = day.get(field)
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        m = _DATE_KEY_RE.match(text)
        if m:
            return m.group(1)
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return ""


def _count_timeslots(day: dict) -> int:
    if not isinstance(day, dict):
        return 0
    slots = day.get("timeslots") or day.get("slots") or []
    if not isinstance(slots, list):
        return 0
    n = 0
    for slot in slots:
        if not isinstance(slot, dict):
            continue
        if slot.get("from_in_timezone") or slot.get("to_in_timezone"):
            n += 1
    return n


def _days_from_block(block: object) -> list[dict]:
    if isinstance(block, dict):
        days = block.get("days")
        if isinstance(days, list):
            return [d for d in days if isinstance(d, dict)]
    if isinstance(block, list):
        return [d for d in block if isinstance(d, dict)]
    return []


def extract_timeslot_days(data: dict | None, macrolocal_cluster_id: int | None = None) -> list[dict]:
    """
    Список дней {date_in_timezone, timeslots} из ответа v2/draft/timeslot/info.
    Поддерживает drop_off_warehouse_timeslots и альтернативные вложенные структуры Ozon.
    """
    if not isinstance(data, dict):
        return []
    result = _result_payload(data) or data
    if not isinstance(result, dict):
        return []

    drop_off = result.get("drop_off_warehouse_timeslots")
    days = _days_from_block(drop_off)
    if days:
        return days
    if isinstance(drop_off, dict) and isinstance(drop_off.get("days"), list):
        logger.debug(
            "timeslot_parse: drop_off_warehouse_timeslots.days пуст (macrolocal_cluster_id={})",
            macrolocal_cluster_id,
        )

    # Иногда слоты лежат в массиве по кластерам/складам.
    for key in (
        "selected_cluster_warehouse_timeslots",
        "cluster_warehouse_timeslots",
        "storage_warehouse_timeslots",
        "warehouse_timeslots",
    ):
        blocks = result.get(key)
        if not isinstance(blocks, list):
            continue
        picked: list[dict] = []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            if macrolocal_cluster_id is not None:
                ml = block.get("macrolocal_cluster_id")
                try:
                    if ml is not None and int(ml) != int(macrolocal_cluster_id):
                        continue
                except (TypeError, ValueError):
                    pass
            picked.extend(_days_from_block(block))
        if picked:
            logger.debug(
                "timeslot_parse: days из result.{} count={} macrolocal_cluster_id={}",
                key,
                len(picked),
                macrolocal_cluster_id,
            )
            return picked

    for key in ("days", "timeslots"):
        cand = result.get(key)
        if isinstance(cand, list) and cand and isinstance(cand[0], dict):
            if "timeslots" in cand[0] or "date_in_timezone" in cand[0]:
                return [d for d in cand if isinstance(d, dict)]

    logger.debug(
        "timeslot_parse: days не найдены, keys data={} result={}",
        list(data.keys()),
        list(result.keys()) if isinstance(result, dict) else [],
    )
    return []


def parse_dates_text_from_days(days: list[dict]) -> str:
    dates: list[str] = []
    for day in days:
        cnt = _count_timeslots(day)
        if cnt <= 0:
            continue
        key = _day_date_key(day)
        if not key:
            continue
        try:
            dates.append(datetime.strptime(key, "%Y-%m-%d").strftime("%d.%m"))
        except ValueError:
            dates.append(key[8:10] + "." + key[5:7] if len(key) >= 10 else key)
    return ", ".join(dates) if dates else "нет дат"


def parse_day_counts_from_days(days: list[dict], date_from_str: str, days_count: int) -> list[int]:
    by_date: dict[str, int] = {}
    for day in days:
        key = _day_date_key(day)
        if key:
            by_date[key] = _count_timeslots(day)
    out: list[int] = []
    try:
        base = datetime.strptime(date_from_str, "%Y-%m-%d").date()
    except ValueError:
        return [0] * days_count
    for i in range(days_count):
        key = (base + timedelta(days=i)).strftime("%Y-%m-%d")
        out.append(by_date.get(key, 0))
    return out


def parse_timeslot_day_counts(
    data: dict | None,
    date_from_str: str,
    days_count: int,
    macrolocal_cluster_id: int | None = None,
) -> list[int]:
    days = extract_timeslot_days(data, macrolocal_cluster_id=macrolocal_cluster_id)
    return parse_day_counts_from_days(days, date_from_str, days_count)


def parse_timeslot_dates_text(
    data: dict | None,
    macrolocal_cluster_id: int | None = None,
) -> str:
    days = extract_timeslot_days(data, macrolocal_cluster_id=macrolocal_cluster_id)
    return parse_dates_text_from_days(days)


def cluster_macrolocal_id(cluster: dict) -> int | None:
    """
    macrolocal_cluster_id из POST /v1/cluster/list — для draft/crossdock/create и timeslot/info.
    Не путать с полем id того же ответа (id строки кластера для UI/БД).
    """
    if not isinstance(cluster, dict):
        return None
    raw = cluster.get("macrolocal_cluster_id")
    if raw is not None:
        try:
            val = int(raw)
            if val > 0:
                return val
        except (TypeError, ValueError):
            pass
    for lc in cluster.get("logistic_clusters") or []:
        if not isinstance(lc, dict):
            continue
        raw = lc.get("macrolocal_cluster_id")
        if raw is None:
            continue
        try:
            val = int(raw)
            if val > 0:
                return val
        except (TypeError, ValueError):
            continue
    raw = cluster.get("id")
    if raw is not None:
        try:
            val = int(raw)
            if val > 0:
                return val
        except (TypeError, ValueError):
            pass
    return None


def cluster_list_id(cluster: dict, macrolocal_cluster_id: int | None = None) -> int | None:
    """id кластера из POST /v1/cluster/list — для сохранения в БД (как в UI)."""
    if not isinstance(cluster, dict):
        return None
    raw = cluster.get("id")
    if raw is not None:
        try:
            val = int(raw)
            if val > 0:
                return val
        except (TypeError, ValueError):
            pass
    return macrolocal_cluster_id


def build_crossdock_draft_payload(config: dict | None, macrolocal_cluster_id: int) -> dict:
    """
    Нормализованное тело POST /v1/draft/crossdock/create — как create_fbs_crossdock_draft в api_client.
    Берёт items и delivery_info из конфига, подставляет macrolocal_cluster_id кластера скана.
    """
    cfg = config if isinstance(config, dict) else {}
    ci = cfg.get("cluster_info") if isinstance(cfg.get("cluster_info"), dict) else {}
    di = cfg.get("delivery_info") if isinstance(cfg.get("delivery_info"), dict) else {}
    items: list[dict] = []
    for it in ci.get("items") or []:
        if not isinstance(it, dict):
            continue
        try:
            sku = int(it.get("sku") or 0)
            qty = int(it.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if sku > 0 and qty > 0:
            items.append({"sku": sku, "quantity": qty})
    drop_id = di.get("drop_off_warehouse_id")
    drop_off = di.get("drop_off_warehouse") if isinstance(di.get("drop_off_warehouse"), dict) else {}
    if drop_id in (None, "", 0, "0") and drop_off:
        drop_id = drop_off.get("warehouse_id")
    wh_type = (di.get("warehouse_type") or drop_off.get("warehouse_type") or "CROSS_DOCK")
    wh_type = str(wh_type).strip() or "CROSS_DOCK"
    seller = di.get("seller_warehouse_id")
    delivery_type = str(di.get("type") or "DROPOFF").strip() or "DROPOFF"
    deletion = cfg.get("deletion_sku_mode") or "PARTIAL"
    if deletion not in ("PARTIAL", "FULL"):
        deletion = "PARTIAL"
    try:
        drop_i = int(drop_id or 0)
    except (TypeError, ValueError):
        drop_i = 0
    try:
        seller_i = int(seller or 0)
    except (TypeError, ValueError):
        seller_i = 0
    return {
        "cluster_info": {
            "macrolocal_cluster_id": int(macrolocal_cluster_id),
            "items": items,
        },
        "deletion_sku_mode": deletion,
        "delivery_info": {
            "drop_off_warehouse": {"warehouse_id": drop_i, "warehouse_type": wh_type},
            "seller_warehouse_id": seller_i,
            "type": delivery_type,
        },
    }


def _storage_warehouse_id_from_wh(wh: dict) -> int | None:
    if not isinstance(wh, dict):
        return None
    for key in ("storage_warehouse_id", "warehouse_id", "id"):
        raw = wh.get(key)
        if raw in (None, "", 0, "0"):
            continue
        try:
            val = int(raw)
            if val > 0:
                return val
        except (TypeError, ValueError):
            continue
    nested = wh.get("storage_warehouse")
    if isinstance(nested, dict):
        return _storage_warehouse_id_from_wh(nested)
    return None


def _cluster_for_macrolocal(clusters: list, macrolocal_cluster_id: int) -> dict | None:
    req = int(macrolocal_cluster_id)
    for cl in clusters:
        if not isinstance(cl, dict):
            continue
        try:
            if int(cl.get("macrolocal_cluster_id")) == req:
                return cl
        except (TypeError, ValueError):
            continue
    return None


def summarize_draft_info_clusters(draft_info: dict | None, macrolocal_cluster_id: int) -> str:
    """Краткая сводка clusters[] из v2/draft/create/info для логов."""
    if not isinstance(draft_info, dict):
        return "draft_info пуст"
    payload = _result_payload(draft_info)
    clusters = payload.get("clusters") or draft_info.get("clusters") or []
    if not isinstance(clusters, list) or not clusters:
        return "clusters[] пуст"
    req = int(macrolocal_cluster_id)
    parts: list[str] = []
    for cl in clusters:
        if not isinstance(cl, dict):
            continue
        try:
            ml = int(cl.get("macrolocal_cluster_id"))
        except (TypeError, ValueError):
            ml = 0
        wh_ids: list[int] = []
        for wh in cl.get("warehouses") or []:
            sw = _storage_warehouse_id_from_wh(wh) if isinstance(wh, dict) else None
            if sw:
                wh_ids.append(sw)
        mark = "*" if ml == req else ""
        parts.append(f"ml={ml}{mark} wh={wh_ids or '—'}")
    return "; ".join(parts) if parts else "clusters без macrolocal"


def timeslot_selected_wh_attempts(
    draft_info: dict | None,
    macrolocal_cluster_id: int,
) -> list[list[dict] | None]:
    """
    Варианты selected_cluster_warehouses для POST /v2/draft/timeslot/info (по порядку).
    None — не передавать поле (Ozon берёт scoring из черновика).
    """
    ml = int(macrolocal_cluster_id)
    attempts: list[list[dict] | None] = []
    seen: set[str] = set()

    def add(entry: list[dict] | None) -> None:
        key = repr(entry)
        if key in seen:
            return
        seen.add(key)
        attempts.append(entry)

    if isinstance(draft_info, dict):
        payload = _result_payload(draft_info)
        clusters = payload.get("clusters") or draft_info.get("clusters") or []
        if isinstance(clusters, list):
            cl = _cluster_for_macrolocal(clusters, ml)
            if cl:
                for wh in cl.get("warehouses") or []:
                    if not isinstance(wh, dict):
                        continue
                    sw = _storage_warehouse_id_from_wh(wh)
                    if sw:
                        add([{"macrolocal_cluster_id": ml, "storage_warehouse_id": sw}])

    add([{"macrolocal_cluster_id": ml}])
    add(None)
    return attempts


def build_selected_cluster_warehouses_from_draft(
    draft_info: dict | None,
    macrolocal_cluster_id: int,
) -> list[dict]:
    """Первый (предпочтительный) вариант selected_cluster_warehouses для timeslot/info."""
    for cand in timeslot_selected_wh_attempts(draft_info, macrolocal_cluster_id):
        if cand:
            return cand
    return [{"macrolocal_cluster_id": int(macrolocal_cluster_id)}]
