"""Админка: планирование листов-заданий на смену."""
from __future__ import annotations

from datetime import date, datetime

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from pathlib import Path
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.admin.auth import verify_admin
from app.db.database import get_db
from app.db.models import ShiftSheet, ShiftTask, User
from app.shift_planning.print_queue_pick import (
    build_shift_task_from_queue_row,
    load_print_queue_for_day,
)
from app.shift_planning.sheet_view import build_shift_sheet_view, material_warning_text
from app.shift_planning.constants import (
    SHIFT_SHEET_STATUS_DRAFT,
    SHIFT_SHEET_STATUS_LABELS,
    SHIFT_SHEET_STATUS_PUBLISHED,
    SHIFT_TASK_TYPE_LABELS,
    SHIFT_TASK_TYPE_PRINT,
    SHIFT_TASK_STATUS_LABELS,
    USER_ROLE_OPERATOR,
)
from app.time_utils import MSK

router = APIRouter(prefix="/planning", tags=["admin-planning"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent.parent / "templates"))


async def _operators(db: AsyncSession) -> list[User]:
    r = await db.execute(
        select(User)
        .where(User.role == USER_ROLE_OPERATOR)
        .order_by(User.username.asc())
    )
    return list(r.scalars().all())


async def _load_sheet(db: AsyncSession, sheet_id: int) -> ShiftSheet | None:
    r = await db.execute(
        select(ShiftSheet)
        .options(
            selectinload(ShiftSheet.tasks).selectinload(ShiftTask.attachments),
            selectinload(ShiftSheet.assignee),
        )
        .where(ShiftSheet.id == sheet_id)
    )
    return r.scalar_one_or_none()


@router.get("", response_class=HTMLResponse)
async def planning_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Список листов-заданий."""
    r = await db.execute(
        select(ShiftSheet)
        .options(selectinload(ShiftSheet.assignee), selectinload(ShiftSheet.tasks))
        .order_by(ShiftSheet.shift_date.desc(), ShiftSheet.id.desc())
    )
    sheets = r.scalars().all()
    return templates.TemplateResponse(
        "admin/planning_list.html",
        {
            "request": request,
            "sheets": sheets,
            "status_labels": SHIFT_SHEET_STATUS_LABELS,
            "task_type_labels": SHIFT_TASK_TYPE_LABELS,
        },
    )


@router.get("/new", response_class=HTMLResponse)
async def planning_new_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    operators = await _operators(db)
    today = datetime.now(MSK).date()
    return templates.TemplateResponse(
        "admin/planning_new.html",
        {
            "request": request,
            "operators": operators,
            "default_date": today.isoformat(),
        },
    )


@router.post("/new")
async def planning_create(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    assignee_user_id: int = Form(...),
    shift_date: str = Form(...),
    manager_notes: str = Form(""),
):
    try:
        sd = datetime.strptime((shift_date or "").strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return RedirectResponse(url="/admin/planning/new?error=date", status_code=303)
    user = await db.get(User, assignee_user_id)
    if not user or (user.role or "staff") != USER_ROLE_OPERATOR:
        return RedirectResponse(url="/admin/planning/new?error=user", status_code=303)
    sheet = ShiftSheet(
        assignee_user_id=assignee_user_id,
        shift_date=sd,
        status=SHIFT_SHEET_STATUS_DRAFT,
        manager_notes=(manager_notes or "").strip()[:1024],
    )
    db.add(sheet)
    await db.commit()
    await db.refresh(sheet)
    return RedirectResponse(url=f"/admin/planning/{sheet.id}", status_code=303)


@router.get("/{sheet_id:int}", response_class=HTMLResponse)
async def planning_sheet_page(
    request: Request,
    sheet_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    sheet = await _load_sheet(db, sheet_id)
    if not sheet:
        return RedirectResponse(url="/admin/planning?error=notfound", status_code=303)
    print_queue_day = sheet.shift_date
    print_queue_rows = await load_print_queue_for_day(db, print_queue_day, sheet_id=sheet.id)
    sheet_view = await build_shift_sheet_view(db, list(sheet.tasks))
    today_msk = datetime.now(MSK).date()
    return templates.TemplateResponse(
        "admin/planning_sheet.html",
        {
            "request": request,
            "sheet": sheet,
            "status_labels": SHIFT_SHEET_STATUS_LABELS,
            "task_type_labels": SHIFT_TASK_TYPE_LABELS,
            "task_status_labels": SHIFT_TASK_STATUS_LABELS,
            "print_queue_rows": print_queue_rows,
            "print_queue_day": print_queue_day,
            "today_msk": today_msk,
            "print_groups": sheet_view["print_groups"],
            "other_tasks": sheet_view["other_tasks"],
            "material_warning_text": material_warning_text,
        },
    )


@router.post("/{sheet_id:int}/tasks/from-print-queue")
async def planning_tasks_from_print_queue(
    request: Request,
    sheet_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Добавить в лист выбранные задания из очереди печати (диаграмма) на дату смены."""
    sheet = await db.get(ShiftSheet, sheet_id)
    if not sheet:
        return RedirectResponse(url="/admin/planning?error=notfound", status_code=303)
    if sheet.status != SHIFT_SHEET_STATUS_DRAFT:
        return RedirectResponse(url=f"/admin/planning/{sheet_id}?error=locked", status_code=303)

    form = await request.form()
    raw_ids = form.getlist("queue_item_id")
    selected_ids: list[int] = []
    for v in raw_ids:
        try:
            selected_ids.append(int(v))
        except (TypeError, ValueError):
            continue
    if not selected_ids:
        return RedirectResponse(url=f"/admin/planning/{sheet_id}?error=no_print_pick", status_code=303)

    rows = await load_print_queue_for_day(db, sheet.shift_date, sheet_id=sheet.id)
    by_id = {r["queue_item_id"]: r for r in rows if not r.get("already_added")}

    max_q = await db.execute(
        select(func.coalesce(func.max(ShiftTask.sort_order), 0)).where(ShiftTask.sheet_id == sheet_id)
    )
    sort_order = int(max_q.scalar_one() or 0)
    added = 0
    for qid in selected_ids:
        row = by_id.get(qid)
        if not row:
            continue
        fields = build_shift_task_from_queue_row(row)
        sort_order += 1
        db.add(
            ShiftTask(
                sheet_id=sheet_id,
                sort_order=sort_order,
                task_type=fields["task_type"],
                title=fields["title"],
                description=fields["description"],
                target_quantity=fields["target_quantity"],
                unit_label=fields["unit_label"],
                print_queue_item_id=fields.get("print_queue_item_id"),
            )
        )
        added += 1
    await db.commit()
    if added == 0:
        return RedirectResponse(url=f"/admin/planning/{sheet_id}?error=print_pick_invalid", status_code=303)
    logger.info("В лист смены {} добавлено {} заданий из очереди печати", sheet_id, added)
    return RedirectResponse(url=f"/admin/planning/{sheet_id}?success=print_added", status_code=303)


@router.post("/{sheet_id:int}/task/add")
async def planning_task_add(
    sheet_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    task_type: str = Form("print"),
    title: str = Form(...),
    description: str = Form(""),
    target_quantity: int = Form(1),
    unit_label: str = Form("шт."),
):
    sheet = await db.get(ShiftSheet, sheet_id)
    if not sheet:
        return RedirectResponse(url="/admin/planning?error=notfound", status_code=303)
    if sheet.status != SHIFT_SHEET_STATUS_DRAFT:
        return RedirectResponse(url=f"/admin/planning/{sheet_id}?error=locked", status_code=303)
    tt = (task_type or "print").strip().lower()
    if tt not in SHIFT_TASK_TYPE_LABELS:
        tt = "print"
    qty = max(1, int(target_quantity or 1))
    max_q = await db.execute(
        select(func.coalesce(func.max(ShiftTask.sort_order), 0)).where(ShiftTask.sheet_id == sheet_id)
    )
    max_order = int(max_q.scalar_one() or 0)
    db.add(
        ShiftTask(
            sheet_id=sheet_id,
            sort_order=max_order + 1,
            task_type=tt,
            title=(title or "").strip()[:256] or "Задание",
            description=(description or "").strip(),
            target_quantity=qty,
            unit_label=(unit_label or "шт.").strip()[:32] or "шт.",
        )
    )
    await db.commit()
    return RedirectResponse(url=f"/admin/planning/{sheet_id}", status_code=303)


@router.post("/{sheet_id:int}/task/{task_id:int}/delete")
async def planning_task_delete(
    sheet_id: int,
    task_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    sheet = await db.get(ShiftSheet, sheet_id)
    if not sheet or sheet.status != SHIFT_SHEET_STATUS_DRAFT:
        return RedirectResponse(url=f"/admin/planning/{sheet_id}?error=locked", status_code=303)
    task = await db.get(ShiftTask, task_id)
    if task and task.sheet_id == sheet_id:
        await db.delete(task)
        await db.commit()
    return RedirectResponse(url=f"/admin/planning/{sheet_id}", status_code=303)


@router.post("/{sheet_id:int}/publish")
async def planning_publish(
    sheet_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    sheet = await _load_sheet(db, sheet_id)
    if not sheet:
        return RedirectResponse(url="/admin/planning?error=notfound", status_code=303)
    if not sheet.tasks:
        return RedirectResponse(url=f"/admin/planning/{sheet_id}?error=empty", status_code=303)
    sheet.status = SHIFT_SHEET_STATUS_PUBLISHED
    sheet.published_at = datetime.now(MSK)
    await db.commit()
    logger.info("Лист смены {} выдан оператору user_id={}", sheet_id, sheet.assignee_user_id)
    return RedirectResponse(url=f"/admin/planning/{sheet_id}?success=published", status_code=303)


@router.post("/{sheet_id:int}/close")
async def planning_close(
    sheet_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    sheet = await db.get(ShiftSheet, sheet_id)
    if not sheet:
        return RedirectResponse(url="/admin/planning?error=notfound", status_code=303)
    sheet.status = "closed"
    await db.commit()
    return RedirectResponse(url=f"/admin/planning/{sheet_id}?success=closed", status_code=303)


@router.post("/{sheet_id:int}/delete")
async def planning_delete(
    sheet_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    sheet = await db.get(ShiftSheet, sheet_id)
    if sheet:
        await db.delete(sheet)
        await db.commit()
    return RedirectResponse(url="/admin/planning?success=deleted", status_code=303)
