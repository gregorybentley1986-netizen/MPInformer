"""Выборка заданий очереди печати (диаграмма Ганта) на календарный день."""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta

from dateutil.parser import isoparse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Material, Part, PrintJob, PrintQueueItem, Printer, ShiftTask
from app.shift_planning.constants import SHIFT_TASK_TYPE_PRINT
from app.time_utils import MSK


def _parse_execution_time_minutes(s: str) -> int:
    if not s or not isinstance(s, str):
        return 0
    s = s.strip()
    total = 0
    for m in re.finditer(r"(\d+)\s*ч", s, re.IGNORECASE):
        total += int(m.group(1)) * 60
    for m in re.finditer(r"(\d+)\s*мин", s, re.IGNORECASE):
        total += int(m.group(1))
    return total


def _ensure_datetime_msk(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=MSK)
        return value.astimezone(MSK) if value.tzinfo != MSK else value
    try:
        dt = isoparse(str(value)) if isinstance(value, str) else value
    except Exception:
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=MSK)
    else:
        dt = dt.astimezone(MSK)
    return dt


def _day_bounds_msk(d: date) -> tuple[datetime, datetime]:
    start = datetime.combine(d, datetime.min.time(), tzinfo=MSK)
    return start, start + timedelta(days=1)


def _format_time_range(start: datetime | None, end: datetime | None) -> str:
    if not start:
        return "—"
    s = start.strftime("%H:%M")
    if end:
        return f"{s}–{end.strftime('%H:%M')}"
    return s


def _parts_summary_from_job(job: PrintJob, parts_by_id: dict[int, Part]) -> str:
    pqs = job.part_quantities
    if isinstance(pqs, str):
        try:
            pqs = json.loads(pqs)
        except Exception:
            pqs = []
    if not isinstance(pqs, list):
        return ""
    bits: list[str] = []
    for pq in pqs:
        if not isinstance(pq, dict):
            continue
        pid = pq.get("part_id")
        qty = int(pq.get("qty") or 0)
        if pid is None or qty <= 0:
            continue
        pname = parts_by_id.get(int(pid))
        label = (pname.name if pname else f"деталь #{pid}").strip()
        bits.append(f"{label} ×{qty}")
    return ", ".join(bits)


async def load_print_queue_for_day(
    db: AsyncSession,
    day: date,
    *,
    sheet_id: int | None = None,
) -> list[dict]:
    """
    Задания из очереди печати (диаграмма), у которых scheduled_start попадает в день day (МСК).
    Если sheet_id задан — помечает уже добавленные в этот лист пункты.
    """
    day_start, day_end = _day_bounds_msk(day)

    already_in_sheet: set[int] = set()
    if sheet_id is not None:
        r_exist = await db.execute(
            select(ShiftTask.print_queue_item_id).where(
                ShiftTask.sheet_id == sheet_id,
                ShiftTask.print_queue_item_id.isnot(None),
            )
        )
        already_in_sheet = {int(x) for x in r_exist.scalars().all() if x is not None}

    r = await db.execute(
        select(PrintQueueItem, PrintJob, Printer, Material)
        .join(PrintJob, PrintQueueItem.print_job_id == PrintJob.id)
        .join(Printer, PrintQueueItem.printer_id == Printer.id)
        .outerjoin(Material, PrintQueueItem.material_id == Material.id)
        .order_by(PrintQueueItem.scheduled_start.asc(), PrintQueueItem.sequence.asc())
    )
    rows = r.all()

    part_ids: set[int] = set()
    for _item, job, _pr, _mat in rows:
        pqs = job.part_quantities
        if isinstance(pqs, str):
            try:
                pqs = json.loads(pqs)
            except Exception:
                pqs = []
        if isinstance(pqs, list):
            for pq in pqs:
                if isinstance(pq, dict) and pq.get("part_id") is not None:
                    part_ids.add(int(pq["part_id"]))

    parts_by_id: dict[int, Part] = {}
    if part_ids:
        r_parts = await db.execute(select(Part).where(Part.id.in_(part_ids)))
        parts_by_id = {p.id: p for p in r_parts.scalars().all()}

    out: list[dict] = []
    for item, job, printer, material in rows:
        start = _ensure_datetime_msk(item.scheduled_start)
        if start is None or start < day_start or start >= day_end:
            continue
        dur = _parse_execution_time_minutes(job.execution_time or "")
        end = start + timedelta(minutes=dur) if dur else start
        mat_label = ""
        if material:
            mat_label = (material.name or "").strip()
            if (material.color or "").strip():
                col = material.color.strip()
                mat_label = f"{mat_label} ({col})" if mat_label else col
        parts_txt = _parts_summary_from_job(job, parts_by_id)
        pr_num = (printer.number or "").strip() if printer else ""
        pr_name = (printer.name or "").strip() if printer else ""
        printer_label = f"№{pr_num}" if pr_num else (pr_name or "—")
        if pr_num and pr_name:
            printer_label = f"№{pr_num} {pr_name}"

        qid = item.id
        out.append(
            {
                "queue_item_id": qid,
                "print_job_id": job.id,
                "job_name": job.name or "",
                "printer_label": printer_label,
                "time_label": _format_time_range(start, end),
                "material_label": mat_label or "—",
                "parts_summary": parts_txt,
                "duration_minutes": dur,
                "already_added": qid in already_in_sheet,
            }
        )
    return out


def build_shift_task_from_queue_row(row: dict) -> dict:
    """Поля для создания ShiftTask из строки load_print_queue_for_day."""
    title = (row.get("job_name") or "").strip() or "Печать"
    desc_lines = [
        f"Принтер: {row.get('printer_label') or '—'}",
        f"Время (план): {row.get('time_label') or '—'}",
    ]
    if row.get("material_label") and row["material_label"] != "—":
        desc_lines.append(f"Материал: {row['material_label']}")
    if row.get("parts_summary"):
        desc_lines.append(f"Состав задания: {row['parts_summary']}")
    return {
        "task_type": SHIFT_TASK_TYPE_PRINT,
        "title": title[:256],
        "description": "\n".join(desc_lines),
        "target_quantity": 1,
        "unit_label": "запуск",
        "print_queue_item_id": row.get("queue_item_id"),
    }
