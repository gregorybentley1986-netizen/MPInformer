"""Страницы оператора: листы-задания на смену."""
from __future__ import annotations

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from pathlib import Path
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import verify_site_user
from app.db.database import get_db
from app.db.models import ShiftSheet, ShiftTask, ShiftTaskAttachment, User
from app.shift_planning.constants import (
    SHIFT_SHEET_STATUS_PUBLISHED,
    SHIFT_TASK_STATUS_COMPLETED,
    SHIFT_TASK_STATUS_FAILED,
    SHIFT_TASK_STATUS_LABELS,
    SHIFT_TASK_STATUS_PARTIAL,
    SHIFT_TASK_STATUS_PENDING,
    SHIFT_TASK_TYPE_LABELS,
    SHIFT_SHEET_STATUS_LABELS,
)
from app.shift_planning.helpers import (
    save_shift_task_attachments,
    shift_attachment_url,
    user_is_operator,
)
from app.shift_planning.sheet_view import build_shift_sheet_view, material_warning_text
from app.time_utils import MSK

router = APIRouter(tags=["shift"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent.parent / "templates"))


async def _load_operator_sheet(db: AsyncSession, sheet_id: int, user_id: int) -> ShiftSheet | None:
    r = await db.execute(
        select(ShiftSheet)
        .options(
            selectinload(ShiftSheet.tasks).selectinload(ShiftTask.attachments),
            selectinload(ShiftSheet.assignee),
        )
        .where(
            ShiftSheet.id == sheet_id,
            ShiftSheet.assignee_user_id == user_id,
            ShiftSheet.status.in_([SHIFT_SHEET_STATUS_PUBLISHED, "closed"]),
        )
    )
    return r.scalar_one_or_none()


@router.get("/my-shift", response_class=HTMLResponse)
async def my_shift_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
):
    """Список выданных листов-заданий оператора."""
    if not user_is_operator(user):
        return RedirectResponse(url="/", status_code=303)
    since = datetime.now(MSK).date() - timedelta(days=14)
    r = await db.execute(
        select(ShiftSheet)
        .where(
            ShiftSheet.assignee_user_id == user.id,
            ShiftSheet.status.in_([SHIFT_SHEET_STATUS_PUBLISHED, "closed"]),
            ShiftSheet.shift_date >= since,
        )
        .order_by(ShiftSheet.shift_date.desc(), ShiftSheet.id.desc())
    )
    sheets = r.scalars().all()
    return templates.TemplateResponse(
        "site/my_shift_list.html",
        {
            "request": request,
            "site_username": user.username,
            "sheets": sheets,
            "status_labels": SHIFT_SHEET_STATUS_LABELS,
        },
    )


@router.get("/my-shift/{sheet_id:int}", response_class=HTMLResponse)
async def my_shift_sheet(
    request: Request,
    sheet_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
):
    if not user_is_operator(user):
        return RedirectResponse(url="/", status_code=303)
    sheet = await _load_operator_sheet(db, sheet_id, user.id)
    if not sheet:
        return RedirectResponse(url="/my-shift?error=notfound", status_code=303)
    readonly = sheet.status == "closed"
    sheet_view = await build_shift_sheet_view(db, list(sheet.tasks))
    return templates.TemplateResponse(
        "site/my_shift_sheet.html",
        {
            "request": request,
            "site_username": user.username,
            "sheet": sheet,
            "readonly": readonly,
            "task_type_labels": SHIFT_TASK_TYPE_LABELS,
            "task_status_labels": SHIFT_TASK_STATUS_LABELS,
            "shift_attachment_url": shift_attachment_url,
            "print_groups": sheet_view["print_groups"],
            "other_tasks": sheet_view["other_tasks"],
            "material_warning_text": material_warning_text,
        },
    )


def _redirect_comment_required(
    sheet_id: int,
    task_id: int,
    *,
    report_status: str | None = None,
) -> RedirectResponse:
    q = f"error=comment&task={task_id}"
    if report_status:
        q += f"&status={report_status}"
    return RedirectResponse(url=f"/my-shift/{sheet_id}?{q}", status_code=303)


async def _add_task_attachments(
    db: AsyncSession,
    task_id: int,
    files: list,
) -> None:
    saved = await save_shift_task_attachments(files or [])
    for stored, orig in saved:
        db.add(
            ShiftTaskAttachment(
                task_id=task_id,
                stored_filename=stored,
                original_filename=orig[:256],
            )
        )


@router.post("/my-shift/task/{task_id:int}/comment")
async def my_shift_task_comment(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    worker_comment: str = Form(""),
    attachments: list[UploadFile] | None = File(None),
):
    if not user_is_operator(user):
        return RedirectResponse(url="/", status_code=303)
    r = await db.execute(
        select(ShiftTask)
        .options(selectinload(ShiftTask.sheet), selectinload(ShiftTask.attachments))
        .where(ShiftTask.id == task_id)
    )
    task = r.scalar_one_or_none()
    if not task or not task.sheet:
        return RedirectResponse(url="/my-shift?error=notfound", status_code=303)
    sheet = task.sheet
    if sheet.assignee_user_id != user.id or sheet.status != SHIFT_SHEET_STATUS_PUBLISHED:
        return RedirectResponse(url=f"/my-shift/{sheet.id}?error=locked", status_code=303)

    comment = (worker_comment or "").strip()
    if task.status in (SHIFT_TASK_STATUS_PARTIAL, SHIFT_TASK_STATUS_FAILED) and not comment:
        return _redirect_comment_required(sheet.id, task_id)

    task.worker_comment = comment[:2000]
    await _add_task_attachments(db, task.id, attachments or [])
    await db.commit()
    return RedirectResponse(url=f"/my-shift/{sheet.id}?success=1#task-{task_id}", status_code=303)


@router.post("/my-shift/task/{task_id:int}/report")
async def my_shift_task_report(
    request: Request,
    task_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(verify_site_user),
    report_status: str = Form(...),
    completion_percent: str = Form(""),
    worker_comment: str = Form(""),
    attachments: list[UploadFile] | None = File(None),
):
    if not user_is_operator(user):
        return RedirectResponse(url="/", status_code=303)
    r = await db.execute(
        select(ShiftTask)
        .options(selectinload(ShiftTask.sheet), selectinload(ShiftTask.attachments))
        .where(ShiftTask.id == task_id)
    )
    task = r.scalar_one_or_none()
    if not task or not task.sheet:
        return RedirectResponse(url="/my-shift?error=notfound", status_code=303)
    sheet = task.sheet
    if sheet.assignee_user_id != user.id or sheet.status != SHIFT_SHEET_STATUS_PUBLISHED:
        return RedirectResponse(url=f"/my-shift/{sheet.id}?error=locked", status_code=303)

    st = (report_status or "").strip().lower()
    comment = (worker_comment or "").strip()
    sheet_id = sheet.id

    if st == SHIFT_TASK_STATUS_COMPLETED:
        task.status = SHIFT_TASK_STATUS_COMPLETED
        task.completion_percent = 100
        if comment:
            task.worker_comment = comment
        task.completed_at = datetime.now(MSK)
    elif st == SHIFT_TASK_STATUS_PARTIAL:
        if not comment:
            return _redirect_comment_required(sheet_id, task_id, report_status=st)
        try:
            pct = int((completion_percent or "").strip())
        except ValueError:
            pct = -1
        if pct < 1 or pct > 99:
            return RedirectResponse(
                url=f"/my-shift/{sheet_id}?error=percent&task={task_id}",
                status_code=303,
            )
        task.status = SHIFT_TASK_STATUS_PARTIAL
        task.completion_percent = pct
        task.worker_comment = comment[:2000]
        task.completed_at = datetime.now(MSK)
    elif st == SHIFT_TASK_STATUS_FAILED:
        if not comment:
            return _redirect_comment_required(sheet_id, task_id, report_status=st)
        task.status = SHIFT_TASK_STATUS_FAILED
        task.completion_percent = None
        task.worker_comment = comment[:2000]
        task.completed_at = datetime.now(MSK)
    else:
        return RedirectResponse(url=f"/my-shift/{sheet_id}?error=status", status_code=303)

    await _add_task_attachments(db, task.id, attachments or [])
    await db.commit()
    logger.info(
        "Оператор {} отметил задание {} как {} (лист {})",
        user.username,
        task_id,
        task.status,
        sheet_id,
    )
    return RedirectResponse(url=f"/my-shift/{sheet_id}?success=1", status_code=303)
