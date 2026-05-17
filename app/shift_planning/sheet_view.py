"""Подготовка данных листа смены для компактного UI (печать по времени)."""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Color, Material, PrintJob, PrintQueueItem, Printer, ShiftTask, Spool
from app.shift_planning.constants import SHIFT_TASK_TYPE_PRINT
from app.shift_planning.print_queue_pick import _ensure_datetime_msk, _format_time_range, _parse_execution_time_minutes
from app.time_utils import MSK

_RE_PRINTER = re.compile(r"^Принтер:\s*(.+)$", re.MULTILINE)
_RE_TIME = re.compile(r"^Время \(план\):\s*(.+)$", re.MULTILINE)
_RE_MATERIAL = re.compile(r"^Материал:\s*(.+)$", re.MULTILINE)


def _parse_description_fallback(desc: str) -> dict:
    desc = (desc or "").strip()
    printer = ""
    time_label = ""
    material = ""
    if desc:
        if m := _RE_PRINTER.search(desc):
            printer = m.group(1).strip()
        if m := _RE_TIME.search(desc):
            time_label = m.group(1).strip()
        if m := _RE_MATERIAL.search(desc):
            material = m.group(1).strip()
    time_sort = 999999
    time_short = "—"
    if time_label and time_label != "—":
        part = time_label.split("–")[0].strip()
        time_short = part
        try:
            h, mi = part.split(":")
            time_sort = int(h) * 60 + int(mi)
        except ValueError:
            pass
    pr_num = printer
    if printer.startswith("№"):
        pr_num = printer.split()[0] if printer else "—"
    return {
        "time_label": time_short,
        "time_sort": time_sort,
        "printer_number": pr_num,
        "job_name": "",
        "material_label": material or "—",
    }


def _material_label(mat: Material | None) -> str:
    if not mat:
        return "—"
    label = (mat.name or "").strip()
    if (mat.color or "").strip():
        col = mat.color.strip()
        label = f"{label} ({col})" if label else col
    return label or "—"


def _material_display(mat: Material | None, color_hex_map: dict[str, str]) -> dict:
    if not mat:
        return {
            "color_hex": "#cccccc",
            "color_name": "",
            "plastic_type": "",
            "has_material": False,
        }
    color_name = (mat.color or "").strip()
    return {
        "color_hex": color_hex_map.get(color_name, "#888888") if color_name else "#888888",
        "color_name": color_name,
        "plastic_type": (mat.plastic_type or "").strip(),
        "has_material": True,
    }


def _printer_number(printer: Printer | None) -> str:
    if not printer:
        return "—"
    num = (printer.number or "").strip()
    return f"№{num}" if num else ((printer.name or "").strip() or "—")


def _printer_display(printer: Printer | None) -> tuple[str, str]:
    """Короткая подпись для ячейки и полный title."""
    if not printer:
        return "—", "—"
    num = (printer.number or "").strip()
    if num:
        return num, f"№{num}"
    name = (printer.name or "").strip()
    return (name[:4] if name else "—"), name or "—"


def _material_warning(
    required_material_id: int | None,
    printer: Printer | None,
    spools_by_id: dict[int, Spool],
    materials_by_id: dict[int, Material],
) -> str | None:
    if required_material_id is None:
        return None
    if not printer or not printer.current_spool_id:
        return "no_spool"
    spool = spools_by_id.get(int(printer.current_spool_id))
    if not spool or not spool.material_id:
        return "no_spool"
    if int(spool.material_id) != int(required_material_id):
        loaded = materials_by_id.get(int(spool.material_id))
        loaded_name = _material_label(loaded)
        return f"mismatch:{loaded_name}"
    return None


def material_warning_text(code: str | None) -> str:
    if not code:
        return ""
    if code == "no_spool":
        return "Материал не подключён к принтеру"
    if code.startswith("mismatch:"):
        name = code.split(":", 1)[1]
        return f"В принтере другой материал: {name}"
    return code


async def build_shift_sheet_view(db: AsyncSession, tasks: list[ShiftTask]) -> dict:
    """
    print_groups: [{time_label, rows: [view_row, ...]}]
    other_tasks: задания не print
    """
    print_tasks = [t for t in tasks if t.task_type == SHIFT_TASK_TYPE_PRINT]
    other_tasks = [t for t in tasks if t.task_type != SHIFT_TASK_TYPE_PRINT]

    qids = [t.print_queue_item_id for t in print_tasks if t.print_queue_item_id]
    queue_by_id: dict[int, PrintQueueItem] = {}
    job_by_id: dict[int, PrintJob] = {}
    printer_by_id: dict[int, Printer] = {}
    material_by_id: dict[int, Material] = {}
    color_hex_map: dict[str, str] = {}
    r_col = await db.execute(select(Color))
    color_hex_map = {c.name: (c.hex or "#888888") for c in r_col.scalars().all() if c.name}

    if qids:
        r = await db.execute(select(PrintQueueItem).where(PrintQueueItem.id.in_(qids)))
        items = list(r.scalars().all())
        queue_by_id = {i.id: i for i in items}
        job_ids = {i.print_job_id for i in items}
        printer_ids = {i.printer_id for i in items}
        mat_ids = {i.material_id for i in items if i.material_id}

        if job_ids:
            rj = await db.execute(select(PrintJob).where(PrintJob.id.in_(job_ids)))
            job_by_id = {j.id: j for j in rj.scalars().all()}
        if printer_ids:
            rp = await db.execute(select(Printer).where(Printer.id.in_(printer_ids)))
            printer_by_id = {p.id: p for p in rp.scalars().all()}
        if mat_ids:
            rm = await db.execute(select(Material).where(Material.id.in_(mat_ids)))
            material_by_id = {m.id: m for m in rm.scalars().all()}

    if printer_by_id:
        rp_all = await db.execute(select(Printer).where(Printer.id.in_(printer_by_id.keys())))
        printer_by_id = {p.id: p for p in rp_all.scalars().all()}

    spool_ids = {p.current_spool_id for p in printer_by_id.values() if p.current_spool_id}
    spools_by_id: dict[int, Spool] = {}
    if spool_ids:
        rs = await db.execute(select(Spool).where(Spool.id.in_(spool_ids)))
        spools_by_id = {s.id: s for s in rs.scalars().all()}
        extra_mat_ids = {s.material_id for s in spools_by_id.values() if s.material_id}
        extra_mat_ids -= set(material_by_id.keys())
        if extra_mat_ids:
            rm2 = await db.execute(select(Material).where(Material.id.in_(extra_mat_ids)))
            for m in rm2.scalars().all():
                material_by_id[m.id] = m

    rows_for_group: list[dict] = []
    for task in print_tasks:
        fb = _parse_description_fallback(task.description or "")
        job_name = (task.title or "").strip() or "—"
        time_label = fb["time_label"]
        time_sort = fb["time_sort"]
        printer_number = fb["printer_number"]
        printer_display = printer_number.lstrip("№") if printer_number.startswith("№") else printer_number
        printer_title = printer_number
        material_label = fb["material_label"]
        mat_display = _material_display(None, color_hex_map)
        material_warning = None
        qid = task.print_queue_item_id

        if qid and qid in queue_by_id:
            item = queue_by_id[qid]
            job = job_by_id.get(item.print_job_id)
            printer = printer_by_id.get(item.printer_id)
            mat = material_by_id.get(item.material_id) if item.material_id else None
            if job and (job.name or "").strip():
                job_name = (job.name or "").strip()
            printer_display, printer_title = _printer_display(printer)
            printer_number = _printer_number(printer)
            material_label = _material_label(mat)
            mat_display = _material_display(mat, color_hex_map)
            start = _ensure_datetime_msk(item.scheduled_start)
            dur = 0
            if job:
                dur = _parse_execution_time_minutes(job.execution_time or "")
            end = start + timedelta(minutes=dur) if start and dur else start
            if start:
                time_sort = start.hour * 60 + start.minute
                time_label = _format_time_range(start, end)
            material_warning = _material_warning(
                item.material_id,
                printer,
                spools_by_id,
                material_by_id,
            )
        else:
            material_warning = None

        comment = (task.worker_comment or "").strip()
        rows_for_group.append(
            {
                "task": task,
                "task_id": task.id,
                "status": task.status,
                "completion_percent": task.completion_percent,
                "has_comment": bool(comment),
                "worker_comment": comment,
                "time_label": time_label,
                "time_sort": time_sort,
                "printer_number": printer_number,
                "printer_display": printer_display,
                "printer_title": printer_title,
                "job_name": job_name,
                "material_label": material_label,
                "material_color_hex": mat_display["color_hex"],
                "material_color_name": mat_display["color_name"],
                "material_plastic_type": mat_display["plastic_type"],
                "material_has": mat_display["has_material"],
                "material_warning": material_warning,
                "material_warning_text": material_warning_text(material_warning),
                "attachments": list(task.attachments or []),
                "attachments_list": [
                    {
                        "url": f"/uploads/shift_tasks/{a.stored_filename}",
                        "name": (a.original_filename or a.stored_filename or "").strip(),
                    }
                    for a in (task.attachments or [])
                ],
            }
        )

    by_time: dict[int, list[dict]] = defaultdict(list)
    for row in rows_for_group:
        by_time[row["time_sort"]].append(row)

    print_groups: list[dict] = []
    for sort_key in sorted(by_time.keys()):
        group_rows = sorted(by_time[sort_key], key=lambda r: (r["printer_number"], r["job_name"]))
        time_label = group_rows[0]["time_label"] if group_rows else "—"
        if "–" in time_label:
            time_label = time_label.split("–")[0].strip()
        print_groups.append({"time_label": time_label, "rows": group_rows})

    return {"print_groups": print_groups, "other_tasks": other_tasks}
