"""
API маршруты для админ-панели
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from dateutil import tz as dateutil_tz

import secrets

from fastapi import APIRouter, Depends, File, Request, Form, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.datastructures import UploadFile as StarletteUploadFile
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.templating import Jinja2Templates
from jinja2 import TemplateNotFound
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import delete, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified
from typing import Optional

from app.admin.auth import verify_admin
from app.auth import hash_password
from app.config import settings
from app.time_utils import MSK
from app.db.database import get_db


def _datetime_to_msk_display(dt):
    """Форматирует datetime в строку по МСК для отображения пользователю."""
    if dt is None or not hasattr(dt, "strftime"):
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=dateutil_tz.UTC)
    if hasattr(dt, "astimezone"):
        return dt.astimezone(MSK).strftime("%d.%m.%Y %H:%M")
    return dt.strftime("%d.%m.%Y %H:%M")


from app.db.models import (
    User,
    Material,
    Color,
    Spool,
    Part,
    Product,
    ProductPart,
    ExtraMaterial,
    ProductExtraMaterial,
    ProductIndividualPackaging,
    IndividualPackaging,
    TransportPackaging,
    AssemblyOption,
    AssemblyOptionItem,
    Printer,
    PrintJob,
    SupplyDraftConfig,
    SlotsTrackerConfig,
    SupplyQueueScan,
    FinanceEntry,
)
from app.modules.notifications.scheduler import scheduler, stop_scheduler, start_scheduler
from app.telegram.bot import stop_bot
from app.site.routes import _spool_svg_dataurl


router = APIRouter(prefix="/admin", tags=["admin"])

# Путь к шаблонам (корень templates, внутри admin/ и site/)
templates_path = Path(__file__).parent.parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_path))
# Фильтр tojson для шаблонов (в стандартном Jinja2 может отсутствовать)
if "tojson" not in templates.env.filters:
    templates.env.filters["tojson"] = lambda x: json.dumps(x, ensure_ascii=False)

# Корень загрузок (STL деталей, фото)
_uploads_base = Path(__file__).parent.parent.parent / (getattr(settings, "uploads_dir", "uploads") or "uploads")
_temp_photo_subdirs = ("products", "extra_materials")

def _photo_ext(filename: str) -> str:
    ext = (Path(filename or "").suffix or ".jpg").lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
        return ".jpg"
    return ".jpeg" if ext == ".jpg" else ext


# Расширения для штрихкодов: растровые форматы + PDF
_BARCODE_EXTS = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif")


def _barcode_ext(filename: str) -> str:
    ext = (Path(filename or "").suffix or "").lower()
    if ext not in _BARCODE_EXTS:
        return ".pdf"
    return ".jpeg" if ext == ".jpg" else ext


class SettingsUpdate(BaseModel):
    """Модель для обновления настроек"""
    scheduler_interval_minutes: Optional[int] = None
    log_level: Optional[str] = None


def _check_admin_credentials(username: str, password: str) -> bool:
    """Проверка логина/пароля по .env."""
    u = (username or "").strip()
    p = (password or "").strip()
    correct_u = (getattr(settings, "admin_username", None) or "admin").strip()
    correct_p = (getattr(settings, "admin_password", None) or "admin123").strip()
    return secrets.compare_digest(u, correct_u) and secrets.compare_digest(p, correct_p)


def _session_get(request: Request, key: str, default=None):
    """Безопасное чтение из сессии (на случай отсутствия SessionMiddleware или ошибки cookie)."""
    try:
        session = getattr(request, "session", None)
        return session.get(key, default) if session is not None else default
    except Exception:
        return default


def _login_form_html(error: bool = False) -> str:
    """Минимальная форма входа (fallback, если шаблон admin/login.html не найден на сервере)."""
    err_block = (
        '<div style="background:#f8d7da;color:#721c24;padding:12px;border-radius:5px;margin-bottom:20px;">'
        "Неверный логин или пароль.</div>"
    ) if error else ""
    return f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Вход — MPInformer</title>
<style>body{{font-family:sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh;margin:0;display:flex;align-items:center;justify-content:center;padding:20px;box-sizing:border-box;}}
.card{{background:#fff;border-radius:10px;padding:40px;max-width:400px;width:100%;box-shadow:0 4px 6px rgba(0,0,0,.1);}}
.card h1{{color:#333;margin-bottom:10px;}}
.form-group{{margin-bottom:20px;}}
.form-group label{{display:block;font-weight:bold;color:#333;margin-bottom:8px;}}
.form-group input{{width:100%;padding:12px;border:2px solid #e0e0e0;border-radius:5px;font-size:16px;box-sizing:border-box;}}
.btn{{width:100%;padding:12px;background:#667eea;color:#fff;border:none;border-radius:5px;font-size:16px;cursor:pointer;margin-top:10px;}}
.btn:hover{{background:#5568d3;}}</style></head>
<body><div class="card"><h1>Вход в админку</h1><p style="color:#666;margin-bottom:24px;">MPInformer</p>{err_block}
<form method="POST" action="/admin/login"><div class="form-group"><label for="username">Логин</label><input type="text" id="username" name="username" required autocomplete="username"></div>
<div class="form-group"><label for="password">Пароль</label><input type="password" id="password" name="password" required autocomplete="current-password"></div>
<button type="submit" class="btn">Войти</button></form></div></body></html>"""


@router.get("/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    """Страница входа. Если уже авторизован — редирект в админку."""
    if _session_get(request, "admin_user"):
        return RedirectResponse(url="/admin/", status_code=303)
    error = request.query_params.get("error") == "1"
    try:
        return templates.TemplateResponse("admin/login.html", {
            "request": request,
            "error": error,
        })
    except TemplateNotFound:
        return HTMLResponse(content=_login_form_html(error=error))


@router.post("/login")
async def admin_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    """Проверка логина/пароля и создание сессии. Редирект в админку или обратно на вход."""
    if _check_admin_credentials(username, password):
        try:
            session = getattr(request, "session", None)
            if session is not None:
                session["admin_user"] = username.strip()
        except Exception as e:
            logger.warning(f"Не удалось записать сессию: {e}")
        return RedirectResponse(url="/admin/", status_code=303)
    logger.warning(f"Неудачная попытка входа в админку: {username}")
    return RedirectResponse(url="/admin/login?error=1", status_code=303)


@router.get("/logout")
async def admin_logout(request: Request):
    """Выход: сброс сессии и редирект на страницу входа. Без запроса логина/пароля."""
    try:
        session = getattr(request, "session", None)
        if session is not None:
            session.clear()
    except Exception:
        pass
    return RedirectResponse(url="/admin/login", status_code=303)


@router.get("/", response_class=HTMLResponse)
async def admin_dashboard(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Главная страница админ-панели"""
    # Статусы информеров (для вывода на дашборде)
    supply_status = {
        "configured": False,
        "last_scan_at": None,
    }
    slots_status = {
        "configured": False,
        "enabled": False,
        "last_run_at": None,
    }
    try:
        # Конфиг парсера очереди поставок
        r = await db.execute(select(SupplyDraftConfig).limit(1))
        draft_row = r.scalar_one_or_none()
        if draft_row:
            supply_status["configured"] = True
        # Последний запуск парсера
        r = await db.execute(
            select(SupplyQueueScan)
            .order_by(SupplyQueueScan.scanned_at.desc())
            .limit(1)
        )
        last_scan = r.scalar_one_or_none()
        if last_scan:
            supply_status["last_scan_at"] = last_scan.scanned_at
    except Exception:
        pass

    try:
        # Конфиг и статус отслеживателя слотов
        r = await db.execute(select(SlotsTrackerConfig).limit(1))
        tr_row = r.scalar_one_or_none()
        if tr_row:
            slots_status["configured"] = True
            slots_status["enabled"] = bool(tr_row.enabled)
            slots_status["last_run_at"] = tr_row.last_run_at
    except Exception:
        pass

    # Вывод времени только в МСК
    supply_status["last_scan_at_display"] = _datetime_to_msk_display(supply_status.get("last_scan_at"))
    slots_status["last_run_at_display"] = _datetime_to_msk_display(slots_status.get("last_run_at"))

    return templates.TemplateResponse("admin/dashboard.html", {
        "request": request,
        "settings": {
            "scheduler_interval_minutes": settings.scheduler_interval_minutes,
            "log_level": settings.log_level,
            "server_port": settings.server_port,
            "report_notification_times": getattr(settings, 'report_notification_times', '09:00'),
        },
        "scheduler_running": getattr(scheduler, "running", False) if scheduler else False,
        "informers_status": {
            "report": {
                "active": getattr(scheduler, "running", False) if scheduler else False,
                "times": getattr(settings, "report_notification_times", "09:00"),
            },
            "supply": supply_status,
            "slots": slots_status,
        },
    })


@router.get("/informers", response_class=HTMLResponse)
async def admin_informers(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Раздел информеров и парсеров — все настройки и кнопки запуска по вкладкам."""
    products_with_sku = []
    try:
        r = await db.execute(
            select(Product.id, Product.name, Product.ozon_sku)
            .where(Product.ozon_sku.isnot(None), Product.ozon_sku != 0)
            .order_by(Product.name)
        )
        for row in r.all():
            products_with_sku.append({"id": row.id, "name": row.name or "", "ozon_sku": row.ozon_sku})
    except Exception:
        pass
    tracker_config = None
    slots_status = {
        "configured": False,
        "enabled": False,
        "last_run_at": None,
    }
    supply_status = {
        "configured": False,
        "last_scan_at": None,
    }
    # Статус отслеживателя слотов
    try:
        r = await db.execute(select(SlotsTrackerConfig).limit(1))
        row = r.scalar_one_or_none()
        if row:
            tracker_config = {
                "cluster_ids": list(row.cluster_ids or []),
                "period_days": int(row.period_days) if row.period_days is not None else 7,
                "items": list(row.items or []),
                "frequency_hours": int(row.frequency_hours) if row.frequency_hours is not None else 4,
                "enabled": 1 if row.enabled else 0,
            }
            slots_status["configured"] = True
            slots_status["enabled"] = bool(row.enabled)
            slots_status["last_run_at"] = row.last_run_at
    except Exception:
        pass
    # Статус парсера очереди поставок
    try:
        r = await db.execute(select(SupplyDraftConfig).limit(1))
        draft_row = r.scalar_one_or_none()
        if draft_row:
            supply_status["configured"] = True
        r = await db.execute(
            select(SupplyQueueScan)
            .order_by(SupplyQueueScan.scanned_at.desc())
            .limit(1)
        )
        last_scan = r.scalar_one_or_none()
        if last_scan:
            supply_status["last_scan_at"] = last_scan.scanned_at
    except Exception:
        pass
    supply_status["last_scan_at_display"] = _datetime_to_msk_display(supply_status.get("last_scan_at"))
    slots_status["last_run_at_display"] = _datetime_to_msk_display(slots_status.get("last_run_at"))
    tab = request.query_params.get("tab") or "report"
    if tab not in ("report", "supply", "slots"):
        tab = "report"
    return templates.TemplateResponse("admin/informers.html", {
        "request": request,
        "tracker_products": products_with_sku,
        "tracker_config": tracker_config,
        "initial_tab": tab,
        "informers_status": {
            "report": {
                "active": getattr(scheduler, "running", False) if scheduler else False,
                "times": getattr(settings, "report_notification_times", "09:00"),
            },
            "supply": supply_status,
            "slots": slots_status,
        },
    })


@router.get("/informers/supply-scan-config")
async def informers_supply_scan_config_get(
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db),
):
    """Получить конфиг черновика для фонового парсера очереди поставок (supply_draft_config)."""
    try:
        r = await db.execute(select(SupplyDraftConfig).limit(1))
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(content={"ok": True, "draft_body": None})
        return JSONResponse(content={"ok": True, "draft_body": row.draft_body})
    except Exception as e:
        logger.exception("Informers supply-scan-config get: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/informers/supply-scan-config")
async def informers_supply_scan_config_save(
    request: Request,
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db),
):
    """Сохранить конфиг черновика для фонового парсера очереди поставок (supply_draft_config)."""
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"Неверный JSON: {e}"})
    draft_body = body.get("draft_body")
    if not draft_body or not isinstance(draft_body, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Нужен draft_body (объект для POST /v1/draft/crossdock/create)"})
    if "cluster_info" not in draft_body or "delivery_info" not in draft_body:
        return JSONResponse(status_code=400, content={"ok": False, "error": "draft_body должен содержать cluster_info и delivery_info"})
    try:
        r = await db.execute(select(SupplyDraftConfig).limit(1))
        row = r.scalar_one_or_none()
        if row:
            row.draft_body = draft_body
            flag_modified(row, "draft_body")
            await db.commit()
            logger.info("Supply draft config: обновлён запись id=%s", row.id)
            return JSONResponse(content={"ok": True, "message": "Конфиг обновлён"})
        new_row = SupplyDraftConfig(draft_body=draft_body)
        db.add(new_row)
        await db.commit()
        await db.refresh(new_row)
        logger.info("Supply draft config: создана запись id=%s", new_row.id)
        return JSONResponse(content={"ok": True, "message": "Конфиг сохранён"})
    except Exception as e:
        logger.exception("Informers supply-scan-config save: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.get("/informers/ozon/crossdock-draft-options")
async def informers_crossdock_draft_options(
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Варианты для формы отслеживателя: продукты с SKU Ozon и список кластеров."""
    try:
        result_pr = await db.execute(
            select(Product.id, Product.name, Product.ozon_sku).where(Product.ozon_sku.isnot(None), Product.ozon_sku != 0).order_by(Product.name)
        )
        rows = result_pr.all()
        products = [{"id": r.id, "name": r.name or "", "ozon_sku": r.ozon_sku} for r in rows]
        from app.modules.ozon.api_client import OzonAPIClient
        client = OzonAPIClient()
        clusters = await client.get_cluster_list(cluster_type="CLUSTER_TYPE_OZON")
        return JSONResponse(content={"ok": True, "products": products, "clusters": clusters})
    except Exception as e:
        logger.exception("Informers crossdock draft options: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "products": [], "clusters": []})


@router.get("/informers/slots-tracker-config")
async def informers_slots_tracker_config_get(
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db),
):
    """Получить конфиг отслеживателя слотов (для формы в разделе Информеры)."""
    try:
        r = await db.execute(select(SlotsTrackerConfig).limit(1))
        row = r.scalar_one_or_none()
        if not row:
            return JSONResponse(content={
                "ok": True,
                "config": {
                    "cluster_ids": [],
                    "period_days": 7,
                    "items": [],
                    "frequency_hours": 4,
                    "enabled": 1,
                },
            })
        return JSONResponse(content={
            "ok": True,
            "config": {
                "cluster_ids": list(row.cluster_ids or []),
                "period_days": int(row.period_days) if row.period_days is not None else 7,
                "items": list(row.items or []),
                "frequency_hours": int(row.frequency_hours) if row.frequency_hours is not None else 4,
                "enabled": 1 if row.enabled else 0,
            },
        })
    except Exception as e:
        logger.exception("Slots tracker config get: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/informers/slots-tracker-config")
async def informers_slots_tracker_config_save(
    request: Request,
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db),
):
    """Сохранить конфиг отслеживателя слотов."""
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"Неверный JSON: {e}"})
    cluster_ids = body.get("cluster_ids")
    if cluster_ids is not None and not isinstance(cluster_ids, list):
        cluster_ids = [int(x) for x in str(cluster_ids).replace(",", " ").split() if str(x).strip().isdigit()]
    if cluster_ids is None:
        cluster_ids = []
    else:
        cluster_ids = [int(x) for x in cluster_ids if x is not None and str(x).strip() != ""]
    period_days = body.get("period_days")
    if period_days is not None and period_days not in (7, 14, 21):
        period_days = 7
    if period_days is None:
        period_days = 7
    items = body.get("items")
    if items is None or not isinstance(items, list):
        items = []
    else:
        items = [
            {"sku": int(x.get("sku") or 0), "quantity": int(x.get("quantity") or 50)}
            for x in items
            if isinstance(x, dict) and (x.get("sku") or x.get("sku") == 0)
        ]
    frequency_hours = body.get("frequency_hours")
    if frequency_hours is not None:
        try:
            frequency_hours = max(1, min(168, int(frequency_hours)))
        except (TypeError, ValueError):
            frequency_hours = 4
    else:
        frequency_hours = 4
    enabled = body.get("enabled")
    if enabled is not None:
        enabled = 1 if enabled else 0
    else:
        enabled = 1
    try:
        r = await db.execute(select(SlotsTrackerConfig).limit(1))
        row = r.scalar_one_or_none()
        if row:
            row.cluster_ids = cluster_ids
            row.period_days = period_days
            row.items = items
            row.frequency_hours = frequency_hours
            row.enabled = enabled
            await db.commit()
            return JSONResponse(content={"ok": True, "message": "Конфиг отслеживателя обновлён"})
        new_row = SlotsTrackerConfig(
            cluster_ids=cluster_ids,
            period_days=period_days,
            items=items,
            frequency_hours=frequency_hours,
            enabled=enabled,
        )
        db.add(new_row)
        await db.commit()
        return JSONResponse(content={"ok": True, "message": "Конфиг отслеживателя сохранён"})
    except Exception as e:
        logger.exception("Slots tracker config save: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


# ---------- Пользователи сайта (CRUD) ----------

@router.get("/users", response_class=HTMLResponse)
async def admin_users_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Список пользователей сайта (логин-пароль)."""
    result = await db.execute(select(User).order_by(User.id))
    users = result.scalars().all()
    return templates.TemplateResponse("admin/users.html", {
        "request": request,
        "users": users,
    })


@router.get("/users/new", response_class=HTMLResponse)
async def admin_user_new_page(request: Request, username: str = Depends(verify_admin)):
    """Форма создания пользователя."""
    return templates.TemplateResponse("admin/user_form.html", {
        "request": request,
        "user": None,
        "is_edit": False,
    })


@router.post("/users/new")
async def admin_user_create(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    login: str = Form(..., alias="username"),
    password: str = Form(...),
):
    """Создание пользователя (логин + пароль)."""
    login = (login or "").strip()
    password = (password or "").strip()
    if not login or not password:
        return RedirectResponse(url="/admin/users/new?error=empty", status_code=303)
    existing = await db.execute(select(User).where(User.username == login))
    if existing.scalar_one_or_none():
        return RedirectResponse(url="/admin/users/new?error=exists", status_code=303)
    user = User(username=login, password_hash=hash_password(password))
    db.add(user)
    await db.commit()
    return RedirectResponse(url="/admin/users?success=created", status_code=303)


@router.get("/users/{user_id:int}/edit", response_class=HTMLResponse)
async def admin_user_edit_page(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Форма редактирования пользователя."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return RedirectResponse(url="/admin/users?error=notfound", status_code=303)
    return templates.TemplateResponse("admin/user_form.html", {
        "request": request,
        "user": user,
        "is_edit": True,
    })


@router.post("/users/{user_id:int}/edit")
async def admin_user_update(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    login: str = Form(..., alias="username"),
    new_password: str = Form(""),
):
    """Обновление логина и/или пароля. Пароль меняется только если указан."""
    login = (login or "").strip()
    new_password = (new_password or "").strip()
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return RedirectResponse(url="/admin/users?error=notfound", status_code=303)
    if not login:
        return RedirectResponse(url=f"/admin/users/{user_id}/edit?error=empty", status_code=303)
    # Проверка уникальности логина (кроме текущего пользователя)
    other = await db.execute(select(User).where(User.username == login, User.id != user_id))
    if other.scalar_one_or_none():
        return RedirectResponse(url=f"/admin/users/{user_id}/edit?error=exists", status_code=303)
    user.username = login
    if new_password:
        user.password_hash = hash_password(new_password)
    await db.commit()
    return RedirectResponse(url="/admin/users?success=updated", status_code=303)


@router.post("/users/{user_id:int}/delete")
async def admin_user_delete(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Удаление пользователя."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return RedirectResponse(url="/admin/users?error=notfound", status_code=303)
    await db.delete(user)
    await db.commit()
    return RedirectResponse(url="/admin/users?success=deleted", status_code=303)


# ——— Справочники (страница с вкладками) ———

REF_LIST = [
    {"id": "materials", "name": "Материалы"},
    {"id": "products", "name": "Изделия"},
    {"id": "packaging", "name": "Упаковка"},
    {"id": "printers", "name": "Принтеры"},
    {"id": "print_jobs", "name": "Файлы печати"},
]


@router.get("/reference/materials")
async def admin_reference_materials_redirect(request: Request, username: str = Depends(verify_admin)):
    """Редирект со старого URL на страницу справочников."""
    return RedirectResponse(url="/admin/reference?ref=materials", status_code=303)


@router.get("/reference", response_class=HTMLResponse)
async def admin_reference(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    ref: str = "materials",
    tab: str = "materials",
):
    """Страница справочников: ref=materials|products|packaging, tab=..."""
    if ref not in ("materials", "products", "packaging", "printers", "print_jobs"):
        ref = "materials"
    if ref == "materials" and tab not in ("materials", "colors", "extras"):
        tab = "materials"
    if ref == "products" and tab not in ("products", "parts"):
        tab = "products"
    if ref == "packaging" and tab not in ("individual", "transport", "assembly"):
        tab = "assembly"

    result_m = await db.execute(select(Material).order_by(Material.name, Material.color))
    materials = result_m.scalars().all()
    result_c = await db.execute(select(Color).order_by(Color.name))
    colors_list = result_c.scalars().all()
    color_hex_map = {c.name: (c.hex or "#000000") for c in colors_list}
    distinct_plastic_types = sorted({getattr(m, "plastic_type", "") or "" for m in materials if (getattr(m, "plastic_type", "") or "").strip()})
    distinct_names = sorted({m.name for m in materials if (m.name or "").strip()})
    reference_colors = [{"name": c.name, "hex": c.hex or "#000000"} for c in colors_list]
    options_for_js = json.dumps({
        "plastic_type": distinct_plastic_types,
        "names": distinct_names,
        "colors": reference_colors,
    }, ensure_ascii=False).replace("</script>", "<\\/script>").replace("</SCRIPT>", "<\\/SCRIPT>")

    parts_list = []
    products_list = []
    product_parts_map = {}  # product_id -> [(part, quantity, material_or_none), ...]
    product_part_ids = {}   # product_id -> [part_id, ...]
    product_part_qtys = {}   # product_id -> [qty, ...]
    product_part_material_ids = {}  # product_id -> [material_id or null, ...]
    extra_materials_list = []
    product_extra_ids = {}   # product_id -> [extra_id, ...]
    product_extra_qtys = {}  # product_id -> [qty, ...]
    individual_packaging_for_products = []  # [{id, name}, ...] для выбора индивидуальной упаковки у изделия
    product_individual_packaging = {}  # product_id -> [{"individual_packaging_id": id, "qty": n}, ...]
    if ref == "products":
        try:
            result_p = await db.execute(select(Part).order_by(Part.name))
            parts_list = result_p.scalars().all()
            result_pr = await db.execute(select(Product).order_by(Product.name))
            products_list = result_pr.scalars().all()
            try:
                result_em = await db.execute(select(ExtraMaterial).order_by(ExtraMaterial.name))
                extra_materials_list = result_em.scalars().all()
            except Exception:
                extra_materials_list = []
            try:
                result_ip = await db.execute(select(IndividualPackaging).order_by(IndividualPackaging.name))
                individual_packaging_for_products = [{"id": ip.id, "name": ip.name} for ip in result_ip.scalars().all()]
            except Exception:
                individual_packaging_for_products = []
            for prod in products_list:
                result_pp = await db.execute(
                    select(ProductPart, Part, Material)
                    .join(Part, ProductPart.part_id == Part.id)
                    .outerjoin(Material, ProductPart.material_id == Material.id)
                    .where(ProductPart.product_id == prod.id)
                )
                rows = result_pp.all()
                product_parts_map[prod.id] = [(p, pp.quantity, mat) for pp, p, mat in rows]
                product_part_ids[prod.id] = [p.id for pp, p, _ in rows]
                product_part_qtys[prod.id] = [pp.quantity for pp, p, _ in rows]
                product_part_material_ids[prod.id] = [pp.material_id if pp.material_id else None for pp, p, _ in rows]
                try:
                    result_pem = await db.execute(
                        select(ProductExtraMaterial).where(ProductExtraMaterial.product_id == prod.id)
                    )
                    pem_rows = result_pem.scalars().all()
                    product_extra_ids[prod.id] = [pem.extra_material_id for pem in pem_rows]
                    product_extra_qtys[prod.id] = [pem.quantity for pem in pem_rows]
                except Exception:
                    product_extra_ids[prod.id] = []
                    product_extra_qtys[prod.id] = []
                try:
                    result_pip = await db.execute(
                        select(ProductIndividualPackaging).where(ProductIndividualPackaging.product_id == prod.id)
                    )
                    product_individual_packaging[prod.id] = [
                        {"individual_packaging_id": row.individual_packaging_id, "qty": row.quantity}
                        for row in result_pip.scalars().all()
                    ]
                except Exception:
                    product_individual_packaging[prod.id] = []
        except Exception as e:
            err_msg = str(e).lower()
            run_migration = (
                "no such column" in err_msg or "stl_thumb" in err_msg or "material_id" in err_msg
                or "operational" in err_msg or "parts" in err_msg or "products" in err_msg
            )
            if run_migration:
                from sqlalchemy import text
                from app.db.database import engine
                try:
                    from app.db.migrations import ensure_part_stl_thumb, ensure_product_part_material_id
                    async with engine.begin() as conn:
                        await conn.run_sync(ensure_part_stl_thumb)
                        await conn.run_sync(ensure_product_part_material_id)
                except Exception as mig_err:
                    try:
                        async with engine.begin() as conn:
                            await conn.execute(text(
                                "ALTER TABLE parts ADD COLUMN stl_thumb_filename VARCHAR(512) NOT NULL DEFAULT ''"
                            ))
                    except Exception:
                        pass
                    try:
                        async with engine.begin() as conn:
                            await conn.execute(text("ALTER TABLE product_parts ADD COLUMN material_id INTEGER"))
                    except Exception:
                        pass
                return RedirectResponse(url=str(request.url), status_code=303)
            logger.exception("admin_reference products load failed: %s", e)
            parts_list = []
            products_list = []
            product_parts_map = {}
            product_part_ids = {}
            product_part_qtys = {}
            product_part_material_ids = {}
            product_extra_ids = {}
            product_extra_qtys = {}
            individual_packaging_for_products = []
            product_individual_packaging = {}

    if ref == "materials":
        try:
            result_em = await db.execute(select(ExtraMaterial).order_by(ExtraMaterial.name))
            extra_materials_list = result_em.scalars().all()
        except Exception:
            extra_materials_list = []

    individual_packaging_list = []
    transport_packaging_list = []
    assembly_options_list = []
    if ref == "packaging":
        try:
            result_ip = await db.execute(select(IndividualPackaging).order_by(IndividualPackaging.name))
            individual_packaging_list = result_ip.scalars().all()
            result_tp = await db.execute(select(TransportPackaging).order_by(TransportPackaging.name))
            transport_packaging_list = result_tp.scalars().all()
            result_ao = await db.execute(select(AssemblyOption).order_by(AssemblyOption.name))
            assembly_options = result_ao.scalars().all()
            transport_by_id = {t.id: t for t in transport_packaging_list}
            individual_by_id = {i.id: i for i in individual_packaging_list}
            for ao in assembly_options:
                result_items = await db.execute(
                    select(AssemblyOptionItem, IndividualPackaging)
                    .join(IndividualPackaging, AssemblyOptionItem.individual_packaging_id == IndividualPackaging.id)
                    .where(AssemblyOptionItem.assembly_option_id == ao.id)
                )
                item_rows = result_items.all()
                items = [(item_row[1], item_row[0].quantity) for item_row in item_rows]
                transport = transport_by_id.get(ao.transport_packaging_id) if ao.transport_packaging_id else None
                items_ids = [ind.id for ind, _ in items]
                items_qtys = [qty for _, qty in items]
                assembly_options_list.append({
                    "assembly": {"id": ao.id, "name": ao.name, "transport_packaging_id": ao.transport_packaging_id},
                    "transport": {"id": transport.id, "name": transport.name, "photo_filename": transport.photo_filename} if transport else None,
                    "items": [{"id": ind.id, "name": ind.name, "photo_filename": ind.photo_filename or "", "qty": qty} for ind, qty in items],
                    "items_ids": items_ids,
                    "items_qtys": items_qtys,
                    "items_ids_json": json.dumps(items_ids, ensure_ascii=False),
                    "items_qtys_json": json.dumps(items_qtys, ensure_ascii=False),
                })
        except Exception as e:
            logger.exception("packaging/assembly load failed: %s", e)
            individual_packaging_list = []
            transport_packaging_list = []
            assembly_options_list = []

    printers_list: list = []
    printer_spool_info_by_id: dict = {}
    if ref == "printers":
        try:
            result_prn = await db.execute(select(Printer).order_by(Printer.number, Printer.name))
            printers_list = result_prn.scalars().all()
            spool_ids = [getattr(p, "current_spool_id", None) for p in printers_list if getattr(p, "current_spool_id", None)]
            if spool_ids:
                result_sp = await db.execute(
                    select(Spool, Material)
                    .select_from(Spool)
                    .outerjoin(Material, Spool.material_id == Material.id)
                    .where(Spool.id.in_(spool_ids))
                )
                for s, m in result_sp.all():
                    mat_name = (m.name if m else "") or "—"
                    mat_hex = color_hex_map.get((m.color or "").strip(), "#888888") if m and (m.color or "").strip() else "#888888"
                    rem = float(s.remaining_length_m or 0)
                    icon = "reach" if rem >= 250 else ("midi" if rem >= 50 else "poor")
                    plastic = (getattr(m, "plastic_type", None) or "").strip() if m else ""
                    dataurl = _spool_svg_dataurl(mat_hex, icon, size=32, plastic_type=plastic or None)
                    printer_spool_info_by_id[s.id] = {
                        "material_name": mat_name,
                        "remaining_m": s.remaining_length_m,
                        "material_color_hex": mat_hex,
                        "spool_dataurl": dataurl,
                        "plastic_type": plastic or None,
                    }
        except Exception as e:
            logger.exception("printers load failed: %s", e)
            printers_list = []
            printer_spool_info_by_id = {}

    print_jobs_list: list = []
    print_jobs_display: list = []
    print_jobs_parts_list: list = []
    print_jobs_printers_list: list = []
    print_jobs_data_by_id: dict = {}
    if ref == "print_jobs":
        try:
            result_pj = await db.execute(select(PrintJob).order_by(PrintJob.name))
            print_jobs_list = result_pj.scalars().all()
            result_pp = await db.execute(select(Part).order_by(Part.name))
            print_jobs_parts_list = result_pp.scalars().all()
            result_prn2 = await db.execute(select(Printer).order_by(Printer.number, Printer.name))
            print_jobs_printers_list = result_prn2.scalars().all()
            part_name_by_id = {p.id: p.name for p in print_jobs_parts_list}
            printer_name_by_id = {pr.id: pr.name for pr in print_jobs_printers_list}
            printer_number_by_id = {pr.id: (pr.number or "") for pr in print_jobs_printers_list}
            for job in print_jobs_list:
                raw_pqs = job.part_quantities
                if isinstance(raw_pqs, list):
                    pqs = raw_pqs
                elif isinstance(raw_pqs, str):
                    try:
                        pqs = json.loads(raw_pqs) if raw_pqs.strip() else []
                    except (TypeError, ValueError):
                        pqs = []
                    if not isinstance(pqs, list):
                        pqs = []
                else:
                    pqs = []
                raw_pids = job.printer_ids
                if isinstance(raw_pids, list):
                    pids = raw_pids
                elif isinstance(raw_pids, str):
                    try:
                        pids = json.loads(raw_pids) if raw_pids.strip() else []
                    except (TypeError, ValueError):
                        pids = []
                    if not isinstance(pids, list):
                        pids = []
                part_lines = [
                    (part_name_by_id.get((pq.get("part_id") if isinstance(pq, dict) else None), "") or "?") + " × " + str(pq.get("qty", 1) if isinstance(pq, dict) else 1)
                    for pq in pqs
                ]
                printer_lines = [printer_number_by_id.get(pid, "") or "?" for pid in pids]
                print_jobs_display.append({
                    "job": job,
                    "part_quantities": pqs,
                    "printer_ids": pids,
                    "part_lines_visible": part_lines[:3],
                    "part_lines_spoiler": part_lines[3:],
                    "printer_lines_visible": printer_lines[:3],
                    "printer_lines_spoiler": printer_lines[3:],
                })
                print_jobs_data_by_id[str(job.id)] = {"part_quantities": pqs, "printer_ids": pids}
        except Exception as e:
            logger.exception("print_jobs load failed: %s", e)
            print_jobs_list = []
            print_jobs_display = []
            print_jobs_parts_list = []
            print_jobs_printers_list = []
            print_jobs_data_by_id = {}

    ref_toast_msg: Optional[str] = None
    ref_toast_type: Optional[str] = None
    success = request.query_params.get("success")
    err = request.query_params.get("error")
    if success or err:
        if ref == "products" and tab == "parts":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Деталь создана.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Деталь сохранена.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Деталь удалена.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование детали.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Деталь не найдена.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif ref == "materials" and tab == "extras":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Доп. материал создан.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Доп. материал сохранён.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Доп. материал удалён.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Доп. материал не найден.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif ref == "packaging" and tab == "individual":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Индивидуальная упаковка создана.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Индивидуальная упаковка сохранена.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Индивидуальная упаковка удалена.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Запись не найдена.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif ref == "packaging" and tab == "transport":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Транспортировочная упаковка создана.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Транспортировочная упаковка сохранена.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Транспортировочная упаковка удалена.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Запись не найдена.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif ref == "packaging" and tab == "assembly":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Сборочный вариант создан.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Сборочный вариант сохранён.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Сборочный вариант удалён.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование сборки.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Запись не найдена.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif ref == "printers":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Принтер добавлен.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Принтер сохранён.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Принтер удалён.", "success"
            elif success == "filament_set":
                ref_toast_msg, ref_toast_type = "Филамент привязан к принтеру.", "success"
            elif err == "filament_used":
                ref_toast_msg, ref_toast_type = "Эта катушка уже привязана к другому принтеру. Сначала отвяжите её там.", "error"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование принтера.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Принтер не найден.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif ref == "print_jobs":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Задание на печать создано.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Задание на печать сохранено.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Задание на печать удалено.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните название задания.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Задание не найдено.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif ref == "products" and tab == "products":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Изделие создано.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Изделие сохранено.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Изделие удалено.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните название изделия.", "error"
            elif err == "duplicate":
                ref_toast_msg, ref_toast_type = "Изделие с таким названием уже существует.", "error"
            elif err == "invalid":
                ref_toast_msg, ref_toast_type = "Неверный идентификатор изделия.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Изделие не найдено.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        elif tab == "colors":
            if success == "created":
                ref_toast_msg, ref_toast_type = "Цвет создан.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Цвет сохранён.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Цвет удалён.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование цвета.", "error"
            elif err == "exists":
                ref_toast_msg, ref_toast_type = "Цвет с таким названием уже есть.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Цвет не найден.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"
        else:
            if success == "created":
                ref_toast_msg, ref_toast_type = "Материал создан.", "success"
            elif success == "updated":
                ref_toast_msg, ref_toast_type = "Материал сохранён.", "success"
            elif success == "deleted":
                ref_toast_msg, ref_toast_type = "Материал удалён.", "success"
            elif err == "empty":
                ref_toast_msg, ref_toast_type = "Заполните наименование.", "error"
            elif err == "exists":
                ref_toast_msg, ref_toast_type = "Материал с таким наименованием и цветом уже есть.", "error"
            elif err == "notfound":
                ref_toast_msg, ref_toast_type = "Материал не найден.", "error"
            else:
                ref_toast_msg, ref_toast_type = "Ошибка операции.", "error"

    response = templates.TemplateResponse("admin/reference.html", {
        "request": request,
        "ref": ref,
        "tab": tab,
        "ref_list": REF_LIST,
        "materials": materials,
        "colors_list": colors_list,
        "distinct_plastic_types": distinct_plastic_types,
        "distinct_names": distinct_names,
        "reference_colors": reference_colors,
        "color_hex_map": color_hex_map,
        "options_for_js": options_for_js,
        "ref_toast_msg": ref_toast_msg,
        "ref_toast_type": ref_toast_type,
        "parts_list": parts_list,
        "products_list": products_list,
        "product_parts_map": product_parts_map,
        "product_part_ids": product_part_ids,
        "product_part_qtys": product_part_qtys,
        "product_part_material_ids": product_part_material_ids,
        "extra_materials_list": extra_materials_list if (ref == "products" or ref == "materials") else [],
        "product_extra_ids": product_extra_ids if ref == "products" else {},
        "product_extra_qtys": product_extra_qtys if ref == "products" else {},
        "individual_packaging_for_products": individual_packaging_for_products if ref == "products" else [],
        "product_individual_packaging": product_individual_packaging if ref == "products" else {},
        "individual_packaging_name_by_id": {ip["id"]: ip["name"] for ip in individual_packaging_for_products} if ref == "products" else {},
        "packaging_ids_json": json.dumps([ip["id"] for ip in individual_packaging_for_products]) if ref == "products" else "[]",
        "packaging_names_json": json.dumps([ip["name"] for ip in individual_packaging_for_products], ensure_ascii=False) if ref == "products" else "[]",
        "individual_packaging_list": individual_packaging_list if ref == "packaging" else [],
        "transport_packaging_list": transport_packaging_list if ref == "packaging" else [],
        "assembly_options_list": assembly_options_list if ref == "packaging" else [],
        "assembly_individual_ids_json": json.dumps([i.id for i in individual_packaging_list], ensure_ascii=False) if ref == "packaging" else "[]",
        "assembly_individual_names_json": json.dumps([i.name for i in individual_packaging_list], ensure_ascii=False) if ref == "packaging" else "[]",
        "materials_for_products_list": [
            {"id": m.id, "name": m.name, "color": m.color or "", "hex": color_hex_map.get((m.color or "").strip(), "#888888")}
            for m in (materials if ref == "products" else [])
        ],
        "printers_list": printers_list if ref == "printers" else [],
        "printer_spool_info_by_id": printer_spool_info_by_id if ref == "printers" else {},
        "print_jobs_list": print_jobs_list if ref == "print_jobs" else [],
        "print_jobs_display": print_jobs_display if ref == "print_jobs" else [],
        "print_jobs_parts_list": print_jobs_parts_list if ref == "print_jobs" else [],
        "print_jobs_printers_list": print_jobs_printers_list if ref == "print_jobs" else [],
        "print_job_part_name_by_id": {p.id: p.name for p in print_jobs_parts_list} if ref == "print_jobs" else {},
        "print_job_printer_name_by_id": {pr.id: pr.name for pr in print_jobs_printers_list} if ref == "print_jobs" else {},
        "print_jobs_parts_json": json.dumps([{"id": p.id, "name": p.name} for p in print_jobs_parts_list], ensure_ascii=False) if ref == "print_jobs" else "[]",
        "print_jobs_printers_json": json.dumps([{"id": pr.id, "name": pr.name, "number": pr.number or ""} for pr in print_jobs_printers_list], ensure_ascii=False) if ref == "print_jobs" else "[]",
        "print_jobs_data_by_id": print_jobs_data_by_id if ref == "print_jobs" else {},
    })
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@router.post("/reference/materials/save")
async def admin_material_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    material_id: Optional[str] = Form(None),
    plastic_type: str = Form(""),
    name: str = Form(""),
    color: str = Form(""),
    weight_grams: int = Form(1000),
):
    """Создать или обновить материал. Уникальность: name + color."""
    plastic_type = (plastic_type or "").strip()
    name = (name or "").strip()
    color = (color or "").strip()
    if not name:
        return RedirectResponse(url="/admin/reference?ref=materials&error=empty", status_code=303)
    try:
        weight_grams = max(1, int(weight_grams))
    except (TypeError, ValueError):
        weight_grams = 1000
    if material_id and material_id.strip():
        try:
            mid = int(material_id)
        except ValueError:
            return RedirectResponse(url="/admin/reference?ref=materials&error=invalid", status_code=303)
        result = await db.execute(select(Material).where(Material.id == mid))
        material = result.scalar_one_or_none()
        if not material:
            return RedirectResponse(url="/admin/reference?ref=materials&error=notfound", status_code=303)
        other = await db.execute(
            select(Material).where(
                Material.name == name,
                Material.color == color,
                Material.id != mid,
            )
        )
        if other.scalar_one_or_none():
            return RedirectResponse(url="/admin/reference?ref=materials&error=exists", status_code=303)
        material.plastic_type = plastic_type
        material.name = name
        material.color = color
        material.manufacturer = ""
        material.weight_grams = weight_grams
        await db.commit()
        return RedirectResponse(url="/admin/reference?ref=materials&success=updated", status_code=303)
    other = await db.execute(
        select(Material).where(
            Material.name == name,
            Material.color == color,
        )
    )
    if other.scalar_one_or_none():
        return RedirectResponse(url="/admin/reference?ref=materials&error=exists", status_code=303)
    material = Material(plastic_type=plastic_type, name=name, color=color, manufacturer="", weight_grams=weight_grams)
    db.add(material)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=materials&success=created", status_code=303)


@router.post("/reference/colors/save")
async def admin_color_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    color_id: Optional[str] = Form(None),
    name: str = Form(""),
    hex_value: str = Form("#000000"),
):
    """Создать или обновить цвет. name уникален."""
    name = (name or "").strip()
    hex_value = (hex_value or "#000000").strip()
    if not name:
        return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&error=empty", status_code=303)
    if not hex_value.startswith("#"):
        hex_value = "#" + hex_value
    if len(hex_value) != 7:
        hex_value = "#000000"
    if color_id and color_id.strip():
        try:
            cid = int(color_id)
        except ValueError:
            return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&error=invalid", status_code=303)
        result = await db.execute(select(Color).where(Color.id == cid))
        color = result.scalar_one_or_none()
        if not color:
            return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&error=notfound", status_code=303)
        other = await db.execute(select(Color).where(Color.name == name, Color.id != cid))
        if other.scalar_one_or_none():
            return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&error=exists", status_code=303)
        color.name = name
        color.hex = hex_value
        await db.commit()
        return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&success=updated", status_code=303)
    other = await db.execute(select(Color).where(Color.name == name))
    if other.scalar_one_or_none():
        return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&error=exists", status_code=303)
    color = Color(name=name, hex=hex_value)
    db.add(color)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&success=created", status_code=303)


@router.post("/reference/colors/delete")
async def admin_color_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    color_id: str = Form(...),
):
    """Удалить цвет."""
    try:
        cid = int(color_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&error=invalid", status_code=303)
    result = await db.execute(select(Color).where(Color.id == cid))
    color = result.scalar_one_or_none()
    if not color:
        return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&error=notfound", status_code=303)
    await db.delete(color)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=materials&tab=colors&success=deleted", status_code=303)


@router.post("/reference/materials/delete")
async def admin_material_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    material_id: str = Form(...),
):
    """Удалить материал."""
    try:
        mid = int(material_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=materials&error=invalid", status_code=303)
    result = await db.execute(select(Material).where(Material.id == mid))
    material = result.scalar_one_or_none()
    if not material:
        return RedirectResponse(url="/admin/reference?ref=materials&error=notfound", status_code=303)
    await db.delete(material)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=materials&success=deleted", status_code=303)


@router.get("/reference/printer/{printer_id:int}/qr")
async def admin_printer_qr(
    printer_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Вернуть PNG с QR-кодом принтера (кодирует номер принтера для сканирования в карточке катушки)."""
    import io
    import qrcode
    result = await db.execute(select(Printer).where(Printer.id == printer_id))
    printer = result.scalar_one_or_none()
    if not printer:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("Printer not found", status_code=404)
    # В код зашиваем PRINTER:id (id принтера) для однозначного определения при сканировании
    qr = qrcode.QRCode(version=1, box_size=6, border=2)
    qr.add_data(f"PRINTER:{printer_id}")
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


# ——— Изделия и детали (справочник) ———

def _safe_filename(name: str) -> str:
    """Оставить только безопасные символы для имени файла."""
    allowed = "._-"
    return "".join(c for c in name if c.isalnum() or c in allowed)


@router.get("/reference/product/{product_id:int}", response_class=HTMLResponse)
async def admin_product_card(
    request: Request,
    product_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Карточка изделия: название, состав (детали + материал с цветом), доп. материалы, фото."""
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    if not product:
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=notfound", status_code=303)
    result_pp = await db.execute(
        select(ProductPart, Part, Material)
        .join(Part, ProductPart.part_id == Part.id)
        .outerjoin(Material, ProductPart.material_id == Material.id)
        .where(ProductPart.product_id == product_id)
    )
    composition = [(pp.quantity, p, mat) for pp, p, mat in result_pp.all()]
    try:
        result_pem = await db.execute(
            select(ProductExtraMaterial, ExtraMaterial)
            .join(ExtraMaterial, ProductExtraMaterial.extra_material_id == ExtraMaterial.id)
            .where(ProductExtraMaterial.product_id == product_id)
        )
        composition_extras = [(pem.quantity, em) for pem, em in result_pem.all()]
    except Exception:
        composition_extras = []
    product_packaging = []
    try:
        result_pip = await db.execute(
            select(ProductIndividualPackaging, IndividualPackaging)
            .join(IndividualPackaging, ProductIndividualPackaging.individual_packaging_id == IndividualPackaging.id)
            .where(ProductIndividualPackaging.product_id == product_id)
        )
        product_packaging = [(row[0].quantity, row[1].name) for row in result_pip.all()]
    except Exception:
        product_packaging = []
    result_c = await db.execute(select(Color))
    colors_list = result_c.scalars().all()
    color_hex_map = {c.name: (c.hex or "#000000") for c in colors_list}
    return templates.TemplateResponse("admin/product_card.html", {
        "request": request,
        "product": product,
        "composition": composition,
        "composition_extras": composition_extras,
        "product_packaging": product_packaging,
        "color_hex_map": color_hex_map,
    })


@router.post("/reference/parts/save")
async def admin_part_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    part_id: Optional[str] = Form(None),
    name: str = Form(""),
    weight_grams: int = Form(0),
    stl_file: Optional[UploadFile] = File(None),
    stl_filename: Optional[str] = Form(None),
    stl_thumb_filename: Optional[str] = Form(None),
):
    """Создать или обновить деталь. STL и превью могут быть уже в репозитории (stl_filename, stl_thumb_filename)."""
    name = (name or "").strip()
    if not name:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=empty", status_code=303)
    try:
        weight_grams = max(0, int(weight_grams))
    except (TypeError, ValueError):
        weight_grams = 0

    stl_filename_val = (stl_filename or "").strip()
    if stl_file and stl_file.filename:
        ext = Path(stl_file.filename).suffix.lower()
        if ext != ".stl":
            return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=invalid", status_code=303)
        stl_dir = _uploads_base / "parts" / "stl"
        stl_dir.mkdir(parents=True, exist_ok=True)
        stl_filename_val = _safe_filename(Path(stl_file.filename).stem) + ext
        path = stl_dir / stl_filename_val
        content = await stl_file.read()
        path.write_bytes(content)

    if part_id and part_id.strip():
        try:
            pid = int(part_id)
        except ValueError:
            return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=invalid", status_code=303)
        result = await db.execute(select(Part).where(Part.id == pid))
        part = result.scalar_one_or_none()
        if not part:
            return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=notfound", status_code=303)
        part.name = name
        part.weight_grams = weight_grams
        if stl_filename_val:
            part.stl_filename = stl_filename_val
        await db.commit()
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&success=updated", status_code=303)

    stl_thumb_val = (stl_thumb_filename or "").strip()
    part = Part(name=name, weight_grams=weight_grams, stl_filename=stl_filename_val or "", stl_thumb_filename=stl_thumb_val or "")
    db.add(part)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=products&tab=parts&success=created", status_code=303)


@router.post("/reference/parts/delete")
async def admin_part_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    part_id: str = Form(...),
):
    try:
        pid = int(part_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=invalid", status_code=303)
    result = await db.execute(select(Part).where(Part.id == pid))
    part = result.scalar_one_or_none()
    if not part:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=notfound", status_code=303)
    await db.delete(part)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=products&tab=parts&success=deleted", status_code=303)


@router.post("/reference/printers/save")
async def admin_printer_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    printer_id: Optional[str] = Form(None),
    name: str = Form(""),
    number: str = Form(""),
    bed_size: str = Form(""),
    ip_address: str = Form(""),
):
    """Создать или обновить принтер."""
    name = (name or "").strip()
    number = (number or "").strip()
    bed_size = (bed_size or "").strip()
    ip_address = (ip_address or "").strip()
    if not name:
        return RedirectResponse(url="/admin/reference?ref=printers&error=empty", status_code=303)
    if printer_id and printer_id.strip():
        try:
            pid = int(printer_id)
        except ValueError:
            return RedirectResponse(url="/admin/reference?ref=printers&error=invalid", status_code=303)
        result = await db.execute(select(Printer).where(Printer.id == pid))
        printer = result.scalar_one_or_none()
        if not printer:
            return RedirectResponse(url="/admin/reference?ref=printers&error=notfound", status_code=303)
        printer.name = name
        printer.number = number
        printer.bed_size = bed_size
        printer.ip_address = ip_address
        await db.commit()
        return RedirectResponse(url="/admin/reference?ref=printers&success=updated", status_code=303)
    printer = Printer(name=name, number=number, bed_size=bed_size, ip_address=ip_address)
    db.add(printer)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=printers&success=created", status_code=303)


@router.post("/reference/printers/delete")
async def admin_printer_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    printer_id: str = Form(...),
):
    try:
        pid = int(printer_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=printers&error=invalid", status_code=303)
    result = await db.execute(select(Printer).where(Printer.id == pid))
    printer = result.scalar_one_or_none()
    if not printer:
        return RedirectResponse(url="/admin/reference?ref=printers&error=notfound", status_code=303)
    await db.delete(printer)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=printers&success=deleted", status_code=303)


@router.post("/reference/printers/set-filament")
async def admin_printer_set_filament(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    printer_id: str = Form(...),
    spool_id: str = Form(...),
    return_to: Optional[str] = Form(None),
):
    """Привязать катушку (филамент) к принтеру. spool_id может быть пустым — отвязать."""
    back_url = "/print-queue" if (return_to or "").strip() == "print_queue" else None
    def redirect_ref(path: str = ""):  # path like "&error=invalid"
        if back_url:
            return RedirectResponse(url=back_url + ("?" + path.lstrip("&") if path else ""), status_code=303)
        return RedirectResponse(url="/admin/reference?ref=printers" + (path if path.startswith("&") else "&" + path), status_code=303)
    try:
        pid = int(printer_id)
    except ValueError:
        return redirect_ref("&error=invalid")
    result = await db.execute(select(Printer).where(Printer.id == pid))
    printer = result.scalar_one_or_none()
    if not printer:
        return redirect_ref("&error=notfound")
    sid = None
    if spool_id and str(spool_id).strip():
        try:
            sid = int(spool_id)
        except ValueError:
            pass
        if sid is not None:
            result_s = await db.execute(select(Spool).where(Spool.id == sid))
            if not result_s.scalar_one_or_none():
                return redirect_ref("&error=notfound")
            other = await db.execute(select(Printer).where(Printer.current_spool_id == sid, Printer.id != pid))
            if other.scalar_one_or_none():
                return redirect_ref("&error=filament_used")
    printer.current_spool_id = sid
    await db.commit()
    return redirect_ref("&success=filament_set") if back_url else RedirectResponse(url="/admin/reference?ref=printers&success=filament_set", status_code=303)


def _parse_print_job_part_quantities(raw: str) -> list:
    """Валидация part_quantities: [{"part_id": int, "qty": int}, ...]."""
    try:
        data = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        try:
            part_id = int(item.get("part_id", 0))
            qty = int(item.get("qty", 1))
            if part_id > 0 and qty > 0:
                out.append({"part_id": part_id, "qty": qty})
        except (TypeError, ValueError):
            continue
    return out


def _parse_print_job_printer_ids(raw: str) -> list:
    """Валидация printer_ids: [int, ...]."""
    try:
        data = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    out = []
    for x in data:
        try:
            pid = int(x)
            if pid > 0:
                out.append(pid)
        except (TypeError, ValueError):
            continue
    return out


@router.post("/reference/print_jobs/save")
async def admin_print_job_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    job_id: Optional[str] = Form(None),
    name: str = Form(""),
    part_quantities_json: str = Form("[]"),
    printer_ids_json: str = Form("[]"),
    execution_time: str = Form(""),
    material_weight_grams: Optional[str] = Form("0"),
    gcode_filename: str = Form(""),
    gcode_thumb_filename: str = Form(""),
):
    name = (name or "").strip()
    if not name:
        return RedirectResponse(url="/admin/reference?ref=print_jobs&error=empty", status_code=303)
    part_quantities = _parse_print_job_part_quantities(part_quantities_json)
    printer_ids = _parse_print_job_printer_ids(printer_ids_json)
    try:
        weight = float(material_weight_grams or "0")
        if weight < 0:
            weight = 0
    except (TypeError, ValueError):
        weight = 0.0
    execution_time = (execution_time or "").strip()
    gcode_filename = (gcode_filename or "").strip()
    gcode_thumb_filename = (gcode_thumb_filename or "").strip()

    if job_id and job_id.strip():
        try:
            jid = int(job_id)
        except ValueError:
            return RedirectResponse(url="/admin/reference?ref=print_jobs&error=invalid", status_code=303)
        result = await db.execute(select(PrintJob).where(PrintJob.id == jid))
        job = result.scalar_one_or_none()
        if not job:
            return RedirectResponse(url="/admin/reference?ref=print_jobs&error=notfound", status_code=303)
        job.name = name
        job.part_quantities = part_quantities
        job.printer_ids = printer_ids
        job.execution_time = execution_time
        job.material_weight_grams = weight
        job.gcode_filename = gcode_filename
        job.gcode_thumb_filename = gcode_thumb_filename
        await db.commit()
        return RedirectResponse(url="/admin/reference?ref=print_jobs&success=updated", status_code=303)

    job = PrintJob(
        name=name,
        part_quantities=part_quantities,
        printer_ids=printer_ids,
        execution_time=execution_time,
        material_weight_grams=weight,
        gcode_filename=gcode_filename,
        gcode_thumb_filename=gcode_thumb_filename,
    )
    db.add(job)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=print_jobs&success=created", status_code=303)


@router.post("/reference/print_jobs/delete")
async def admin_print_job_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    job_id: str = Form(...),
):
    try:
        jid = int(job_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=print_jobs&error=invalid", status_code=303)
    result = await db.execute(select(PrintJob).where(PrintJob.id == jid))
    job = result.scalar_one_or_none()
    if not job:
        return RedirectResponse(url="/admin/reference?ref=print_jobs&error=notfound", status_code=303)
    gcode_dir = _uploads_base / "print_jobs" / "gcode"
    thumbs_dir = _uploads_base / "print_jobs" / "thumbs"
    if job.gcode_filename:
        old_path = gcode_dir / job.gcode_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    if job.gcode_thumb_filename:
        thumb_path = thumbs_dir / job.gcode_thumb_filename
        if thumb_path.exists():
            try:
                thumb_path.unlink()
            except Exception:
                pass
    await db.delete(job)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=print_jobs&success=deleted", status_code=303)


GCODE_UPLOAD_MAX_BYTES = 50 * 1024 * 1024


@router.post("/reference/print_jobs/gcode")
async def admin_print_job_gcode_upload(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    job_id: str = Form(""),
    gcode_file: UploadFile = File(...),
):
    """Загрузить gcode-файл для задания. Возвращает JSON с ok и gcode_filename."""
    if not gcode_file.filename or not gcode_file.filename.lower().endswith(".gcode"):
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    content = await gcode_file.read()
    if len(content) > GCODE_UPLOAD_MAX_BYTES:
        return JSONResponse({"ok": False, "error": "too_large"}, status_code=400)
    gcode_dir = _uploads_base / "print_jobs" / "gcode"
    gcode_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _safe_filename(Path(gcode_file.filename).name)
    if not safe_name.lower().endswith(".gcode"):
        safe_name = safe_name + ".gcode" if not safe_name.endswith(".gcode") else safe_name
    target = gcode_dir / safe_name
    try:
        target.write_bytes(content)
    except OSError as e:
        logger.exception("gcode write failed: %s", e)
        return JSONResponse({"ok": False, "error": "upload_failed"}, status_code=500)
    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JSONResponse({"ok": True, "gcode_filename": safe_name})
    job_id_val = (job_id or "").strip()
    if job_id_val:
        try:
            jid = int(job_id_val)
            result = await db.execute(select(PrintJob).where(PrintJob.id == jid))
            job = result.scalar_one_or_none()
            if job:
                job.gcode_filename = safe_name
                await db.commit()
        except (ValueError, Exception):
            pass
    return RedirectResponse(url="/admin/reference?ref=print_jobs&success=updated", status_code=303)


@router.post("/reference/print_jobs/gcode/upload-thumb")
async def admin_print_job_gcode_upload_thumb(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Принять превью (PNG) для файла печати. Лимит: 5 МБ."""
    form = await request.form()
    job_id_raw = form.get("job_id")
    job_id = (job_id_raw.strip() if isinstance(job_id_raw, str) and job_id_raw else None) or None
    gcode_filename_raw = form.get("gcode_filename")
    gcode_filename = (gcode_filename_raw.strip() if isinstance(gcode_filename_raw, str) and gcode_filename_raw else None) or None
    thumb = form.get("thumb")
    if not isinstance(thumb, (UploadFile, StarletteUploadFile)) or not thumb.filename:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    if not (thumb.content_type or "").startswith("image/") and not (thumb.filename or "").lower().endswith(".png"):
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    thumb_dir = _uploads_base / "print_jobs" / "thumbs"
    thumb_dir.mkdir(parents=True, exist_ok=True)
    try:
        content = await thumb.read()
        if len(content) > 5 * 1024 * 1024:
            return JSONResponse({"ok": False, "error": "too_large"}, status_code=400)
    except Exception:
        return JSONResponse({"ok": False, "error": "save"}, status_code=500)

    if job_id and job_id.strip():
        try:
            jid = int(job_id)
        except ValueError:
            return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
        result = await db.execute(select(PrintJob).where(PrintJob.id == jid))
        job = result.scalar_one_or_none()
        if not job:
            return JSONResponse({"ok": False, "error": "notfound"}, status_code=400)
        thumb_filename = f"job_{job.id}.png"
        try:
            (thumb_dir / thumb_filename).write_bytes(content)
        except Exception:
            return JSONResponse({"ok": False, "error": "save"}, status_code=500)
        job.gcode_thumb_filename = thumb_filename
        await db.commit()
        return JSONResponse({"ok": True, "job_id": job.id, "gcode_thumb_filename": thumb_filename})

    gcode_name = (gcode_filename or "").strip()
    if not gcode_name or not gcode_name.lower().endswith(".gcode"):
        return JSONResponse({"ok": False, "error": "gcode_filename_required"}, status_code=400)
    stem = _safe_filename(Path(gcode_name).stem)
    thumb_filename = f"thumb_{stem}.png"
    try:
        (thumb_dir / thumb_filename).write_bytes(content)
    except Exception:
        return JSONResponse({"ok": False, "error": "save"}, status_code=500)
    return JSONResponse({"ok": True, "gcode_thumb_filename": thumb_filename})


@router.get("/reference/parts/stl-exists")
async def admin_part_stl_exists(
    filename: str = "",
    username: str = Depends(verify_admin),
):
    """Проверить, есть ли в репозитории STL с таким именем. Возвращает exists и size (байт) существующего файла."""
    if not filename or not filename.lower().endswith(".stl"):
        return JSONResponse({"exists": False, "size": 0})
    stl_filename = _safe_filename(Path(filename).stem) + ".stl"
    path = _uploads_base / "parts" / "stl" / stl_filename
    if not path.exists():
        return JSONResponse({"exists": False, "size": 0})
    try:
        size = path.stat().st_size
    except OSError:
        size = 0
    return JSONResponse({"exists": True, "size": size, "filename": stl_filename})


# Лимит размера STL при загрузке (байт). По умолчанию Starlette — 1 МБ; для деталей разрешаем до 50 МБ.
STL_UPLOAD_MAX_BYTES = 50 * 1024 * 1024


@router.post("/reference/parts/stl")
async def admin_part_stl_upload(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Загрузить STL в репозиторий. part_id необязателен. Лимит файла: 50 МБ (см. STL_UPLOAD_MAX_BYTES)."""
    is_ajax = request.headers.get("x-requested-with") == "XMLHttpRequest"

    def _err_redirect(err_key: str):
        if is_ajax:
            return JSONResponse({"ok": False, "error": err_key}, status_code=400)
        return RedirectResponse(url=f"/admin/reference?ref=products&tab=parts&error={err_key}", status_code=303)

    try:
        form = getattr(request.state, "stl_form", None)
        if form is None:
            try:
                # Лимит 50 МБ подставляется глобальным патчем в app.main по пути /admin/reference/parts/stl
                form = await request.form()
            except StarletteHTTPException as e:
                detail = getattr(e, "detail", str(e))
                if e.status_code == 400 and detail and ("size" in str(detail).lower() or "max_part" in str(detail).lower() or "large" in str(detail).lower()):
                    if is_ajax:
                        return JSONResponse(
                            {"ok": False, "error": "too_large"},
                            status_code=413,
                            headers={"X-MPInformer-413": "app"},
                        )
                    return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=too_large", status_code=303)
                raise
            except Exception as e:
                if "max_part_size" in str(e).lower() or "too large" in str(e).lower() or "size" in str(e).lower():
                    if is_ajax:
                        return JSONResponse(
                            {"ok": False, "error": "too_large"},
                            status_code=413,
                            headers={"X-MPInformer-413": "app"},
                        )
                    return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=too_large", status_code=303)
                raise
        part_id_raw = form.get("part_id")
        part_id = (part_id_raw.strip() if isinstance(part_id_raw, str) and part_id_raw else None) or None
        stl_file = form.get("stl_file")
        # Starlette 0.48 может вернуть starlette.datastructures.UploadFile, а не fastapi.UploadFile
        if not isinstance(stl_file, (UploadFile, StarletteUploadFile)) or not (stl_file.filename and str(stl_file.filename).lower().endswith(".stl")):
            return _err_redirect("invalid")
        stl_dir = _uploads_base / "parts" / "stl"
        stl_dir.mkdir(parents=True, exist_ok=True)
        stl_filename = _safe_filename(Path(stl_file.filename).stem) + ".stl"
        path = stl_dir / stl_filename
        content = await stl_file.read()
        if len(content) > STL_UPLOAD_MAX_BYTES:
            if is_ajax:
                return JSONResponse(
                    {"ok": False, "error": "too_large"},
                    status_code=413,
                    headers={"X-MPInformer-413": "app"},
                )
            return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=too_large", status_code=303)
        upload_size = len(content)
        if path.exists() and request.headers.get("x-replace") != "1":
            try:
                existing_size = path.stat().st_size
            except OSError:
                existing_size = 0
            if is_ajax:
                return JSONResponse(
                    {"ok": False, "exists": True, "existing_size": existing_size, "upload_size": upload_size},
                    status_code=409,
                )
            return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=exists", status_code=303)
        path.write_bytes(content)
        updated_part_id = None
        if part_id and part_id.strip():
            try:
                pid = int(part_id)
            except ValueError:
                return _err_redirect("invalid")
            result = await db.execute(select(Part).where(Part.id == pid))
            part = result.scalar_one_or_none()
            if part:
                part.stl_filename = stl_filename
                await db.commit()
                updated_part_id = part.id

        if is_ajax:
            out = {"ok": True, "stl_filename": stl_filename}
            if updated_part_id is not None:
                out["part_id"] = updated_part_id
            return JSONResponse(out)
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&success=updated", status_code=303)
    except Exception as e:
        logger.exception("STL upload failed: %s", e)
        if is_ajax:
            return JSONResponse({"ok": False, "error": "upload_failed"}, status_code=500)
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=upload_failed", status_code=303)


@router.post("/reference/parts/stl/upload-thumb")
async def admin_part_stl_upload_thumb(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Принять превью (PNG) с клиента. Лимит: 5 МБ."""
    form = getattr(request.state, "stl_form", None)
    if form is None:
        form = await request.form(max_part_size=5 * 1024 * 1024)
    part_id_raw = form.get("part_id")
    part_id = (part_id_raw.strip() if isinstance(part_id_raw, str) and part_id_raw else None) or None
    stl_filename_raw = form.get("stl_filename")
    stl_filename = (stl_filename_raw.strip() if isinstance(stl_filename_raw, str) and stl_filename_raw else None) or None
    thumb = form.get("thumb")
    if not isinstance(thumb, (UploadFile, StarletteUploadFile)) or not thumb.filename:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    if not (thumb.content_type or "").startswith("image/") and not thumb.filename.lower().endswith(".png"):
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    thumb_dir = _uploads_base / "parts" / "thumbs"
    thumb_dir.mkdir(parents=True, exist_ok=True)
    try:
        content = await thumb.read()
        if len(content) > 5 * 1024 * 1024:
            return JSONResponse({"ok": False, "error": "too_large"}, status_code=400)
    except Exception:
        return JSONResponse({"ok": False, "error": "save"}, status_code=500)

    if part_id and part_id.strip():
        try:
            pid = int(part_id)
        except ValueError:
            return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
        result = await db.execute(select(Part).where(Part.id == pid))
        part = result.scalar_one_or_none()
        if not part:
            return JSONResponse({"ok": False, "error": "notfound"}, status_code=400)
        thumb_filename = f"part_{part.id}.png"
        try:
            (thumb_dir / thumb_filename).write_bytes(content)
        except Exception:
            return JSONResponse({"ok": False, "error": "save"}, status_code=500)
        part.stl_thumb_filename = thumb_filename
        await db.commit()
        return JSONResponse({"ok": True, "part_id": part.id, "stl_thumb_filename": thumb_filename})

    stl_name = (stl_filename or "").strip()
    if not stl_name or not stl_name.lower().endswith(".stl"):
        return JSONResponse({"ok": False, "error": "stl_filename_required"}, status_code=400)
    stem = _safe_filename(Path(stl_name).stem)
    thumb_filename = f"thumb_{stem}.png"
    try:
        (thumb_dir / thumb_filename).write_bytes(content)
    except Exception:
        return JSONResponse({"ok": False, "error": "save"}, status_code=500)
    return JSONResponse({"ok": True, "stl_thumb_filename": thumb_filename})


@router.post("/reference/parts/stl/delete")
async def admin_part_stl_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    part_id: str = Form(...),
):
    """Удалить STL и превью у детали."""
    try:
        pid = int(part_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=invalid", status_code=303)
    result = await db.execute(select(Part).where(Part.id == pid))
    part = result.scalar_one_or_none()
    if not part:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=notfound", status_code=303)
    for subdir, name in [("stl", part.stl_filename), ("thumbs", part.stl_thumb_filename)]:
        if name:
            p = _uploads_base / "parts" / subdir / name
            if p.exists():
                try:
                    p.unlink()
                except Exception:
                    pass
    part.stl_filename = ""
    part.stl_thumb_filename = ""
    await db.commit()
    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JSONResponse({"ok": True})
    return RedirectResponse(url="/admin/reference?ref=products&tab=parts&success=updated", status_code=303)


@router.get("/reference/parts/{part_id:int}/stl-thumb-status")
async def admin_part_stl_thumb_status(
    part_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Статус превью STL для AJAX-опроса (возвращает имя файла превью или null)."""
    result = await db.execute(select(Part).where(Part.id == part_id))
    part = result.scalar_one_or_none()
    if not part:
        return JSONResponse({"stl_thumb_filename": None}, status_code=404)
    return JSONResponse({"stl_thumb_filename": part.stl_thumb_filename})


@router.post("/reference/parts/photo")
async def admin_part_photo_upload(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    part_id: str = Form(...),
    photo: UploadFile = File(...),
):
    try:
        pid = int(part_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=invalid", status_code=303)
    result = await db.execute(select(Part).where(Part.id == pid))
    part = result.scalar_one_or_none()
    if not part:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=notfound", status_code=303)
    ext = (Path(photo.filename or "").suffix or ".jpg").lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
        ext = ".jpg"
    photo_dir = _uploads_base / "parts" / "photos"
    photo_dir.mkdir(parents=True, exist_ok=True)
    new_name = f"part_{pid}{ext}"
    path = photo_dir / new_name
    content = await photo.read()
    path.write_bytes(content)
    part.photo_filename = new_name
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=products&tab=parts&success=updated", status_code=303)


@router.post("/reference/parts/photo/delete")
async def admin_part_photo_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    part_id: str = Form(...),
):
    try:
        pid = int(part_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=invalid", status_code=303)
    result = await db.execute(select(Part).where(Part.id == pid))
    part = result.scalar_one_or_none()
    if not part:
        return RedirectResponse(url="/admin/reference?ref=products&tab=parts&error=notfound", status_code=303)
    if part.photo_filename:
        old_path = _uploads_base / "parts" / "photos" / part.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    part.photo_filename = ""
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=products&tab=parts&success=updated", status_code=303)


@router.post("/reference/products/save")
async def admin_product_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    product_id: Optional[str] = Form(None),
    name: str = Form(""),
    article: str = Form(""),
    ozon_sku: Optional[str] = Form(None),
    wildberries_sku: Optional[str] = Form(None),
    part_quantities: str = Form(""),  # JSON: [{"part_id":1,"qty":2,"material_id":...}, ...]
    extra_quantities: str = Form(""),  # JSON: [{"extra_id":1,"qty":2}, ...]
    individual_packaging_items: str = Form("[]"),  # JSON: [{"individual_packaging_id": 1, "qty": 2}, ...]
    pending_photo: Optional[str] = Form(None),  # temp filename from uploads/temp/products/
):
    name = (name or "").strip()
    article = (article or "").strip()
    ozon_sku_val = None
    if ozon_sku is not None and str(ozon_sku or "").strip():
        try:
            ozon_sku_val = int(str(ozon_sku).strip())
        except ValueError:
            ozon_sku_val = None
    wildberries_sku_val = None
    if wildberries_sku is not None and str(wildberries_sku or "").strip():
        try:
            wildberries_sku_val = int(str(wildberries_sku).strip())
        except ValueError:
            wildberries_sku_val = None
    if not name:
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=empty", status_code=303)
    # Проверка уникальности названия (без учёта регистра, в Python — SQLite LOWER не поддерживает кириллицу)
    name_lower = name.lower()
    result = await db.execute(select(Product.id, Product.name))
    for row in result.all():
        if row.name and row.name.lower() == name_lower:
            if product_id and product_id.strip():
                try:
                    if int(product_id) == row.id:
                        continue
                except ValueError:
                    pass
            return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=duplicate", status_code=303)
    try:
        pq = json.loads(part_quantities or "[]")
    except json.JSONDecodeError:
        pq = []
    items = []
    for x in pq:
        if not isinstance(x, dict) or not x.get("part_id"):
            continue
        try:
            part_id = int(x["part_id"])
            qty = int(x.get("qty", 1))
            material_id = x.get("material_id")
            if material_id is not None and material_id != "":
                material_id = int(material_id)
            else:
                material_id = None
            items.append((part_id, qty, material_id))
        except (TypeError, ValueError):
            continue
    try:
        eq = json.loads(extra_quantities or "[]")
    except json.JSONDecodeError:
        eq = []
    extra_items = []
    for x in eq:
        if not isinstance(x, dict) or not x.get("extra_id"):
            continue
        try:
            extra_id = int(x["extra_id"])
            qty = int(x.get("qty", 1))
            if qty < 1:
                continue
            extra_items.append((extra_id, qty))
        except (TypeError, ValueError):
            continue

    if product_id and product_id.strip():
        try:
            prid = int(product_id)
        except ValueError:
            return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=invalid", status_code=303)
        result = await db.execute(select(Product).where(Product.id == prid))
        product = result.scalar_one_or_none()
        if not product:
            return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=notfound", status_code=303)
        product.name = name
        product.article = article
        product.ozon_sku = ozon_sku_val
        product.wildberries_sku = wildberries_sku_val
        await db.execute(delete(ProductPart).where(ProductPart.product_id == prid))
        await db.execute(delete(ProductExtraMaterial).where(ProductExtraMaterial.product_id == prid))
        await db.execute(delete(ProductIndividualPackaging).where(ProductIndividualPackaging.product_id == prid))
        try:
            pip_items = json.loads(individual_packaging_items or "[]")
            for item in pip_items:
                if not isinstance(item, dict):
                    continue
                ip_id = item.get("individual_packaging_id") or item.get("id")
                if ip_id is None:
                    continue
                try:
                    ip_id = int(ip_id)
                    qty = int(item.get("qty", item.get("quantity", 1)))
                except (TypeError, ValueError):
                    continue
                if ip_id > 0 and qty >= 1:
                    db.add(ProductIndividualPackaging(product_id=prid, individual_packaging_id=ip_id, quantity=qty))
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
        for part_id, qty, mat_id in items:
            if qty < 1:
                continue
            db.add(ProductPart(product_id=prid, part_id=part_id, material_id=mat_id, quantity=qty))
        for extra_id, qty in extra_items:
            db.add(ProductExtraMaterial(product_id=prid, extra_material_id=extra_id, quantity=qty))
        await db.commit()
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&success=updated", status_code=303)

    product = Product(name=name, article=article, ozon_sku=ozon_sku_val, wildberries_sku=wildberries_sku_val)
    db.add(product)
    await db.flush()
    try:
        pip_items = json.loads(individual_packaging_items or "[]")
        for item in pip_items:
            if not isinstance(item, dict):
                continue
            ip_id = item.get("individual_packaging_id") or item.get("id")
            if ip_id is None:
                continue
            try:
                ip_id = int(ip_id)
                qty = int(item.get("qty", item.get("quantity", 1)))
            except (TypeError, ValueError):
                continue
            if ip_id > 0 and qty >= 1:
                db.add(ProductIndividualPackaging(product_id=product.id, individual_packaging_id=ip_id, quantity=qty))
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    for part_id, qty, mat_id in items:
        if qty < 1:
            continue
        db.add(ProductPart(product_id=product.id, part_id=part_id, material_id=mat_id, quantity=qty))
    for extra_id, qty in extra_items:
        db.add(ProductExtraMaterial(product_id=product.id, extra_material_id=extra_id, quantity=qty))
    if pending_photo and (pending_photo := (pending_photo or "").strip()):
        temp_path = _uploads_base / "temp" / "products" / pending_photo
        if temp_path.exists() and temp_path.suffix.lower() in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
            photo_dir = _uploads_base / "products" / "photos"
            photo_dir.mkdir(parents=True, exist_ok=True)
            new_name = f"product_{product.id}{temp_path.suffix}"
            dest = photo_dir / new_name
            try:
                dest.write_bytes(temp_path.read_bytes())
                temp_path.unlink()
            except Exception:
                pass
            else:
                product.photo_filename = new_name
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=products&tab=products&success=created", status_code=303)


@router.post("/reference/products/delete")
async def admin_product_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    product_id: str = Form(...),
):
    try:
        prid = int(product_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=invalid", status_code=303)
    result = await db.execute(select(Product).where(Product.id == prid))
    product = result.scalar_one_or_none()
    if not product:
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=notfound", status_code=303)
    await db.delete(product)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=products&tab=products&success=deleted", status_code=303)


@router.post("/reference/products/photo")
async def admin_product_photo_upload(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    product_id: Optional[str] = Form(None),
    photo: UploadFile = File(...),
):
    """Загрузка фото изделия. product_id пустой или 0 — во временный файл (превью до сохранения объекта)."""
    if not isinstance(photo, (UploadFile, StarletteUploadFile)) or not (photo.filename and getattr(photo, "file", None)):
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    ext = _photo_ext(photo.filename or "")
    content = await photo.read()
    prid = None
    if product_id and product_id.strip():
        try:
            prid = int(product_id)
        except ValueError:
            pass
    if prid is not None:
        result = await db.execute(select(Product).where(Product.id == prid))
        product = result.scalar_one_or_none()
        if product:
            photo_dir = _uploads_base / "products" / "photos"
            photo_dir.mkdir(parents=True, exist_ok=True)
            new_name = f"product_{prid}{ext}"
            path = photo_dir / new_name
            path.write_bytes(content)
            product.photo_filename = new_name
            await db.commit()
            return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/products/photos/{new_name}", "temp": False})
    temp_dir = _uploads_base / "temp" / "products"
    temp_dir.mkdir(parents=True, exist_ok=True)
    new_name = f"pending_{secrets.token_hex(8)}{ext}"
    path = temp_dir / new_name
    path.write_bytes(content)
    return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/temp/products/{new_name}", "temp": True})


@router.post("/reference/products/photo/delete")
async def admin_product_photo_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    product_id: str = Form(...),
):
    try:
        prid = int(product_id)
    except ValueError:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    result = await db.execute(select(Product).where(Product.id == prid))
    product = result.scalar_one_or_none()
    if not product:
        return JSONResponse({"ok": False, "error": "notfound"}, status_code=404)
    if product.photo_filename:
        old_path = _uploads_base / "products" / "photos" / product.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    product.photo_filename = ""
    await db.commit()
    return JSONResponse({"ok": True})


async def _product_barcode_upload(
    db: AsyncSession,
    product_id: int,
    file: "UploadFile",
    marketplace: str,
) -> tuple[bool, str, str]:
    """Загрузить файл штрихкода для изделия. marketplace: 'ozon' | 'wildberries'. Возвращает (ok, filename_or_error, url)."""
    if marketplace not in ("ozon", "wildberries"):
        return False, "invalid", ""
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    if not product:
        return False, "notfound", ""
    if not isinstance(file, (UploadFile, StarletteUploadFile)) or not (file.filename and getattr(file, "file", None)):
        return False, "invalid", ""
    ext = _barcode_ext(file.filename or "")
    content = await file.read()
    if not content:
        return False, "empty", ""
    subdir = "ozon" if marketplace == "ozon" else "wildberries"
    barcode_dir = _uploads_base / "products" / "barcodes" / subdir
    barcode_dir.mkdir(parents=True, exist_ok=True)
    new_name = f"product_{product_id}{ext}"
    path = barcode_dir / new_name
    path.write_bytes(content)
    attr = "ozon_barcode_filename" if marketplace == "ozon" else "wildberries_barcode_filename"
    old_name = getattr(product, attr, "") or ""
    if old_name and old_name != new_name:
        old_path = barcode_dir / old_name
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    setattr(product, attr, new_name)
    await db.commit()
    url = f"/uploads/products/barcodes/{subdir}/{new_name}"
    return True, new_name, url


@router.post("/reference/product/{product_id:int}/barcode/ozon")
async def admin_product_barcode_ozon_upload(
    request: Request,
    product_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    file: UploadFile = File(...),
):
    """Загрузка штрихкода Озон для изделия (PDF или растровый формат)."""
    ok, fn_or_err, url = await _product_barcode_upload(db, product_id, file, "ozon")
    if ok:
        return RedirectResponse(url=f"/admin/reference/product/{product_id}?barcode=ozon_ok", status_code=303)
    if fn_or_err == "notfound":
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=notfound", status_code=303)
    return RedirectResponse(url=f"/admin/reference/product/{product_id}?barcode_error=invalid", status_code=303)


@router.post("/reference/product/{product_id:int}/barcode/wildberries")
async def admin_product_barcode_wildberries_upload(
    request: Request,
    product_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    file: UploadFile = File(...),
):
    """Загрузка штрихкода Wildberries для изделия (PDF или растровый формат)."""
    ok, fn_or_err, url = await _product_barcode_upload(db, product_id, file, "wildberries")
    if ok:
        return RedirectResponse(url=f"/admin/reference/product/{product_id}?barcode=wb_ok", status_code=303)
    if fn_or_err == "notfound":
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=notfound", status_code=303)
    return RedirectResponse(url=f"/admin/reference/product/{product_id}?barcode_error=invalid", status_code=303)


@router.post("/reference/product/{product_id:int}/barcode/ozon/delete")
async def admin_product_barcode_ozon_delete(
    request: Request,
    product_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    if not product:
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=notfound", status_code=303)
    if product.ozon_barcode_filename:
        barcode_dir = _uploads_base / "products" / "barcodes" / "ozon"
        old_path = barcode_dir / product.ozon_barcode_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
        product.ozon_barcode_filename = ""
        await db.commit()
    return RedirectResponse(url=f"/admin/reference/product/{product_id}?barcode=ozon_deleted", status_code=303)


@router.post("/reference/product/{product_id:int}/barcode/wildberries/delete")
async def admin_product_barcode_wildberries_delete(
    request: Request,
    product_id: int,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    if not product:
        return RedirectResponse(url="/admin/reference?ref=products&tab=products&error=notfound", status_code=303)
    if product.wildberries_barcode_filename:
        barcode_dir = _uploads_base / "products" / "barcodes" / "wildberries"
        old_path = barcode_dir / product.wildberries_barcode_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
        product.wildberries_barcode_filename = ""
        await db.commit()
    return RedirectResponse(url=f"/admin/reference/product/{product_id}?barcode=wb_deleted", status_code=303)


@router.post("/reference/products/photo/delete-temp")
async def admin_product_photo_delete_temp(
    request: Request,
    username: str = Depends(verify_admin),
    temp_filename: str = Form(...),
):
    """Удалить временное фото (загруженное до сохранения изделия)."""
    if not temp_filename or ".." in temp_filename or "/" in temp_filename or "\\" in temp_filename:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    path = _uploads_base / "temp" / "products" / temp_filename
    if path.exists():
        try:
            path.unlink()
        except Exception:
            return JSONResponse({"ok": False, "error": "delete_failed"}, status_code=500)
    return JSONResponse({"ok": True})


@router.post("/reference/extras/save")
async def admin_extra_material_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    extra_id: Optional[str] = Form(None),
    name: str = Form(""),
    pending_photo: Optional[str] = Form(None),
):
    """Создать или обновить дополнительный материал (наименование). Фото — отдельным запросом."""
    name = (name or "").strip()
    if not name:
        return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&error=empty", status_code=303)
    if extra_id and extra_id.strip():
        try:
            eid = int(extra_id)
        except ValueError:
            return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&error=invalid", status_code=303)
        result = await db.execute(select(ExtraMaterial).where(ExtraMaterial.id == eid))
        extra = result.scalar_one_or_none()
        if not extra:
            return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&error=notfound", status_code=303)
        extra.name = name
        await db.commit()
        return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&success=updated", status_code=303)
    extra = ExtraMaterial(name=name)
    db.add(extra)
    await db.flush()
    if pending_photo and (pending_photo := (pending_photo or "").strip()):
        temp_path = _uploads_base / "temp" / "extra_materials" / pending_photo
        if temp_path.exists() and temp_path.suffix.lower() in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
            photo_dir = _uploads_base / "extra_materials"
            photo_dir.mkdir(parents=True, exist_ok=True)
            new_name = f"extra_{extra.id}{temp_path.suffix}"
            dest = photo_dir / new_name
            try:
                dest.write_bytes(temp_path.read_bytes())
                temp_path.unlink()
            except Exception:
                pass
            else:
                extra.photo_filename = new_name
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&success=created", status_code=303)


@router.post("/reference/extras/delete")
async def admin_extra_material_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    extra_id: str = Form(...),
):
    try:
        eid = int(extra_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&error=invalid", status_code=303)
    result = await db.execute(select(ExtraMaterial).where(ExtraMaterial.id == eid))
    extra = result.scalar_one_or_none()
    if not extra:
        return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&error=notfound", status_code=303)
    if extra.photo_filename:
        old_path = _uploads_base / "extra_materials" / extra.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    await db.delete(extra)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=materials&tab=extras&success=deleted", status_code=303)


@router.post("/reference/extras/photo")
async def admin_extra_material_photo_upload(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    extra_id: Optional[str] = Form(None),
    photo: UploadFile = File(...),
):
    """Загрузка фото доп. материала. extra_id пустой или 0 — во временный файл."""
    if not isinstance(photo, (UploadFile, StarletteUploadFile)) or not (photo.filename and getattr(photo, "file", None)):
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    ext = _photo_ext(photo.filename or "")
    content = await photo.read()
    eid = None
    if extra_id and extra_id.strip():
        try:
            eid = int(extra_id)
        except ValueError:
            pass
    if eid is not None:
        result = await db.execute(select(ExtraMaterial).where(ExtraMaterial.id == eid))
        extra = result.scalar_one_or_none()
        if extra:
            photo_dir = _uploads_base / "extra_materials"
            photo_dir.mkdir(parents=True, exist_ok=True)
            new_name = f"extra_{eid}{ext}"
            path = photo_dir / new_name
            if extra.photo_filename and extra.photo_filename != new_name:
                old_path = photo_dir / extra.photo_filename
                if old_path.exists():
                    try:
                        old_path.unlink()
                    except Exception:
                        pass
            path.write_bytes(content)
            extra.photo_filename = new_name
            await db.commit()
            return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/extra_materials/{new_name}", "temp": False})
    temp_dir = _uploads_base / "temp" / "extra_materials"
    temp_dir.mkdir(parents=True, exist_ok=True)
    new_name = f"pending_{secrets.token_hex(8)}{ext}"
    path = temp_dir / new_name
    path.write_bytes(content)
    return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/temp/extra_materials/{new_name}", "temp": True})


@router.post("/reference/extras/photo/delete")
async def admin_extra_material_photo_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    extra_id: str = Form(...),
):
    try:
        eid = int(extra_id)
    except ValueError:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    result = await db.execute(select(ExtraMaterial).where(ExtraMaterial.id == eid))
    extra = result.scalar_one_or_none()
    if not extra:
        return JSONResponse({"ok": False, "error": "notfound"}, status_code=404)
    if extra.photo_filename:
        old_path = _uploads_base / "extra_materials" / extra.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    extra.photo_filename = ""
    await db.commit()
    return JSONResponse({"ok": True})


@router.post("/reference/extras/photo/delete-temp")
async def admin_extra_material_photo_delete_temp(
    request: Request,
    username: str = Depends(verify_admin),
    temp_filename: str = Form(...),
):
    """Удалить временное фото доп. материала."""
    if not temp_filename or ".." in temp_filename or "/" in temp_filename or "\\" in temp_filename:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    path = _uploads_base / "temp" / "extra_materials" / temp_filename
    if path.exists():
        try:
            path.unlink()
        except Exception:
            return JSONResponse({"ok": False, "error": "delete_failed"}, status_code=500)
    return JSONResponse({"ok": True})


# ——— Индивидуальная упаковка ———

async def _packaging_save_common(
    db: AsyncSession,
    model_class,
    item_id: Optional[str],
    name: str,
    length_mm: int,
    width_mm: int,
    height_mm: int,
    pending_photo: Optional[str],
    temp_subdir: str,
    upload_subdir: str,
    redirect_base: str,
):
    """Общая логика сохранения упаковки (инд. или трансп.)."""
    name = (name or "").strip()
    if not name:
        return RedirectResponse(url=f"{redirect_base}&error=empty", status_code=303)
    length_mm = max(0, int(length_mm)) if length_mm is not None else 0
    width_mm = max(0, int(width_mm)) if width_mm is not None else 0
    height_mm = max(0, int(height_mm)) if height_mm is not None else 0
    if item_id and item_id.strip():
        try:
            iid = int(item_id)
        except ValueError:
            return RedirectResponse(url=f"{redirect_base}&error=invalid", status_code=303)
        result = await db.execute(select(model_class).where(model_class.id == iid))
        item = result.scalar_one_or_none()
        if not item:
            return RedirectResponse(url=f"{redirect_base}&error=notfound", status_code=303)
        item.name = name
        item.length_mm = length_mm
        item.width_mm = width_mm
        item.height_mm = height_mm
        await db.commit()
        return RedirectResponse(url=f"{redirect_base}&success=updated", status_code=303)
    item = model_class(name=name, length_mm=length_mm, width_mm=width_mm, height_mm=height_mm)
    db.add(item)
    await db.flush()
    if pending_photo and (pending_photo := (pending_photo or "").strip()):
        temp_path = _uploads_base / "temp" / temp_subdir / pending_photo
        if temp_path.exists() and temp_path.suffix.lower() in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
            photo_dir = _uploads_base / upload_subdir
            photo_dir.mkdir(parents=True, exist_ok=True)
            new_name = f"{temp_subdir}_{item.id}{temp_path.suffix}"
            dest = photo_dir / new_name
            try:
                dest.write_bytes(temp_path.read_bytes())
                temp_path.unlink()
            except Exception:
                pass
            else:
                item.photo_filename = new_name
    await db.commit()
    return RedirectResponse(url=f"{redirect_base}&success=created", status_code=303)


@router.post("/reference/packaging/individual/save")
async def admin_individual_packaging_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: Optional[str] = Form(None),
    name: str = Form(""),
    length_mm: int = Form(0),
    width_mm: int = Form(0),
    height_mm: int = Form(0),
    pending_photo: Optional[str] = Form(None),
):
    redirect_base = "/admin/reference?ref=packaging&tab=individual"
    return await _packaging_save_common(
        db, IndividualPackaging, item_id, name, length_mm, width_mm, height_mm,
        pending_photo, "individual_packaging", "individual_packaging", redirect_base,
    )


@router.post("/reference/packaging/individual/delete")
async def admin_individual_packaging_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: str = Form(...),
):
    try:
        iid = int(item_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=packaging&tab=individual&error=invalid", status_code=303)
    result = await db.execute(select(IndividualPackaging).where(IndividualPackaging.id == iid))
    item = result.scalar_one_or_none()
    if not item:
        return RedirectResponse(url="/admin/reference?ref=packaging&tab=individual&error=notfound", status_code=303)
    if item.photo_filename:
        old_path = _uploads_base / "individual_packaging" / item.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    await db.delete(item)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=packaging&tab=individual&success=deleted", status_code=303)


@router.post("/reference/packaging/individual/photo")
async def admin_individual_packaging_photo_upload(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: Optional[str] = Form(None),
    photo: UploadFile = File(...),
):
    if not isinstance(photo, (UploadFile, StarletteUploadFile)) or not (photo.filename and getattr(photo, "file", None)):
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    ext = _photo_ext(photo.filename or "")
    content = await photo.read()
    eid = None
    if item_id and item_id.strip():
        try:
            eid = int(item_id)
        except ValueError:
            pass
    if eid is not None:
        result = await db.execute(select(IndividualPackaging).where(IndividualPackaging.id == eid))
        item = result.scalar_one_or_none()
        if item:
            photo_dir = _uploads_base / "individual_packaging"
            photo_dir.mkdir(parents=True, exist_ok=True)
            new_name = f"individual_packaging_{eid}{ext}"
            path = photo_dir / new_name
            if item.photo_filename and item.photo_filename != new_name:
                old_path = photo_dir / item.photo_filename
                if old_path.exists():
                    try:
                        old_path.unlink()
                    except Exception:
                        pass
            path.write_bytes(content)
            item.photo_filename = new_name
            await db.commit()
            return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/individual_packaging/{new_name}", "temp": False})
    temp_dir = _uploads_base / "temp" / "individual_packaging"
    temp_dir.mkdir(parents=True, exist_ok=True)
    new_name = f"pending_{secrets.token_hex(8)}{ext}"
    path = temp_dir / new_name
    path.write_bytes(content)
    return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/temp/individual_packaging/{new_name}", "temp": True})


@router.post("/reference/packaging/individual/photo/delete")
async def admin_individual_packaging_photo_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: str = Form(...),
):
    try:
        iid = int(item_id)
    except ValueError:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    result = await db.execute(select(IndividualPackaging).where(IndividualPackaging.id == iid))
    item = result.scalar_one_or_none()
    if not item:
        return JSONResponse({"ok": False, "error": "notfound"}, status_code=404)
    if item.photo_filename:
        old_path = _uploads_base / "individual_packaging" / item.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    item.photo_filename = ""
    await db.commit()
    return JSONResponse({"ok": True})


@router.post("/reference/packaging/individual/photo/delete-temp")
async def admin_individual_packaging_photo_delete_temp(
    request: Request,
    username: str = Depends(verify_admin),
    temp_filename: str = Form(...),
):
    if not temp_filename or ".." in temp_filename or "/" in temp_filename or "\\" in temp_filename:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    path = _uploads_base / "temp" / "individual_packaging" / temp_filename
    if path.exists():
        try:
            path.unlink()
        except Exception:
            return JSONResponse({"ok": False, "error": "delete_failed"}, status_code=500)
    return JSONResponse({"ok": True})


# ——— Транспортировочная упаковка ———

@router.post("/reference/packaging/transport/save")
async def admin_transport_packaging_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: Optional[str] = Form(None),
    name: str = Form(""),
    length_mm: int = Form(0),
    width_mm: int = Form(0),
    height_mm: int = Form(0),
    pending_photo: Optional[str] = Form(None),
):
    redirect_base = "/admin/reference?ref=packaging&tab=transport"
    return await _packaging_save_common(
        db, TransportPackaging, item_id, name, length_mm, width_mm, height_mm,
        pending_photo, "transport_packaging", "transport_packaging", redirect_base,
    )


@router.post("/reference/packaging/transport/delete")
async def admin_transport_packaging_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: str = Form(...),
):
    try:
        iid = int(item_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=packaging&tab=transport&error=invalid", status_code=303)
    result = await db.execute(select(TransportPackaging).where(TransportPackaging.id == iid))
    item = result.scalar_one_or_none()
    if not item:
        return RedirectResponse(url="/admin/reference?ref=packaging&tab=transport&error=notfound", status_code=303)
    if item.photo_filename:
        old_path = _uploads_base / "transport_packaging" / item.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    await db.delete(item)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=packaging&tab=transport&success=deleted", status_code=303)


@router.post("/reference/packaging/transport/photo")
async def admin_transport_packaging_photo_upload(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: Optional[str] = Form(None),
    photo: UploadFile = File(...),
):
    if not isinstance(photo, (UploadFile, StarletteUploadFile)) or not (photo.filename and getattr(photo, "file", None)):
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    ext = _photo_ext(photo.filename or "")
    content = await photo.read()
    eid = None
    if item_id and item_id.strip():
        try:
            eid = int(item_id)
        except ValueError:
            pass
    if eid is not None:
        result = await db.execute(select(TransportPackaging).where(TransportPackaging.id == eid))
        item = result.scalar_one_or_none()
        if item:
            photo_dir = _uploads_base / "transport_packaging"
            photo_dir.mkdir(parents=True, exist_ok=True)
            new_name = f"transport_packaging_{eid}{ext}"
            path = photo_dir / new_name
            if item.photo_filename and item.photo_filename != new_name:
                old_path = photo_dir / item.photo_filename
                if old_path.exists():
                    try:
                        old_path.unlink()
                    except Exception:
                        pass
            path.write_bytes(content)
            item.photo_filename = new_name
            await db.commit()
            return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/transport_packaging/{new_name}", "temp": False})
    temp_dir = _uploads_base / "temp" / "transport_packaging"
    temp_dir.mkdir(parents=True, exist_ok=True)
    new_name = f"pending_{secrets.token_hex(8)}{ext}"
    path = temp_dir / new_name
    path.write_bytes(content)
    return JSONResponse({"ok": True, "filename": new_name, "url": f"/uploads/temp/transport_packaging/{new_name}", "temp": True})


@router.post("/reference/packaging/transport/photo/delete")
async def admin_transport_packaging_photo_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    item_id: str = Form(...),
):
    try:
        iid = int(item_id)
    except ValueError:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    result = await db.execute(select(TransportPackaging).where(TransportPackaging.id == iid))
    item = result.scalar_one_or_none()
    if not item:
        return JSONResponse({"ok": False, "error": "notfound"}, status_code=404)
    if item.photo_filename:
        old_path = _uploads_base / "transport_packaging" / item.photo_filename
        if old_path.exists():
            try:
                old_path.unlink()
            except Exception:
                pass
    item.photo_filename = ""
    await db.commit()
    return JSONResponse({"ok": True})


@router.post("/reference/packaging/transport/photo/delete-temp")
async def admin_transport_packaging_photo_delete_temp(
    request: Request,
    username: str = Depends(verify_admin),
    temp_filename: str = Form(...),
):
    if not temp_filename or ".." in temp_filename or "/" in temp_filename or "\\" in temp_filename:
        return JSONResponse({"ok": False, "error": "invalid"}, status_code=400)
    path = _uploads_base / "temp" / "transport_packaging" / temp_filename
    if path.exists():
        try:
            path.unlink()
        except Exception:
            return JSONResponse({"ok": False, "error": "delete_failed"}, status_code=500)
    return JSONResponse({"ok": True})


# ——— Сборочные варианты ———

@router.post("/reference/packaging/assembly/save")
async def admin_assembly_option_save(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    assembly_id: Optional[str] = Form(None),
    name: str = Form(""),
    transport_packaging_id: Optional[str] = Form(None),
    items_json: str = Form("[]"),
):
    """Создать или обновить сборочный вариант. items_json: [{"individual_packaging_id": 1, "qty": 2}, ...]"""
    redirect_base = "/admin/reference?ref=packaging&tab=assembly"
    name = (name or "").strip()
    if not name:
        return RedirectResponse(url=f"{redirect_base}&error=empty", status_code=303)
    try:
        items = json.loads(items_json or "[]")
    except json.JSONDecodeError:
        items = []
    item_tuples = []
    for x in items:
        if not isinstance(x, dict):
            continue
        try:
            ind_id = int(x.get("individual_packaging_id") or 0)
            qty = int(x.get("qty") or 1)
            if ind_id > 0 and qty > 0:
                item_tuples.append((ind_id, qty))
        except (TypeError, ValueError):
            continue
    transport_id = None
    if transport_packaging_id and transport_packaging_id.strip():
        try:
            transport_id = int(transport_packaging_id)
        except ValueError:
            pass
    if assembly_id and assembly_id.strip():
        try:
            aid = int(assembly_id)
        except ValueError:
            return RedirectResponse(url=f"{redirect_base}&error=invalid", status_code=303)
        result = await db.execute(select(AssemblyOption).where(AssemblyOption.id == aid))
        assembly = result.scalar_one_or_none()
        if not assembly:
            return RedirectResponse(url=f"{redirect_base}&error=notfound", status_code=303)
        assembly.name = name
        assembly.transport_packaging_id = transport_id
        await db.execute(delete(AssemblyOptionItem).where(AssemblyOptionItem.assembly_option_id == aid))
        for ind_id, qty in item_tuples:
            db.add(AssemblyOptionItem(assembly_option_id=aid, individual_packaging_id=ind_id, quantity=qty))
        await db.commit()
        return RedirectResponse(url=f"{redirect_base}&success=updated", status_code=303)
    assembly = AssemblyOption(name=name, transport_packaging_id=transport_id)
    db.add(assembly)
    await db.flush()
    for ind_id, qty in item_tuples:
        db.add(AssemblyOptionItem(assembly_option_id=assembly.id, individual_packaging_id=ind_id, quantity=qty))
    await db.commit()
    return RedirectResponse(url=f"{redirect_base}&success=created", status_code=303)


@router.post("/reference/packaging/assembly/delete")
async def admin_assembly_option_delete(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
    assembly_id: str = Form(...),
):
    try:
        aid = int(assembly_id)
    except ValueError:
        return RedirectResponse(url="/admin/reference?ref=packaging&tab=assembly&error=invalid", status_code=303)
    result = await db.execute(select(AssemblyOption).where(AssemblyOption.id == aid))
    assembly = result.scalar_one_or_none()
    if not assembly:
        return RedirectResponse(url="/admin/reference?ref=packaging&tab=assembly&error=notfound", status_code=303)
    await db.delete(assembly)
    await db.commit()
    return RedirectResponse(url="/admin/reference?ref=packaging&tab=assembly&success=deleted", status_code=303)


@router.get("/settings", response_class=HTMLResponse)
async def admin_settings(request: Request, username: str = Depends(verify_admin)):
    """Страница настроек"""
    return templates.TemplateResponse("admin/settings.html", {
        "request": request,
        "settings": {
            "scheduler_interval_minutes": settings.scheduler_interval_minutes,
            "log_level": settings.log_level,
            "server_port": settings.server_port,
            "report_notification_times": getattr(settings, 'report_notification_times', '09:00'),
        }
    })


def _get_log_file_path() -> Path:
    """Путь к файлу логов (тот же, что в app.main)."""
    root = Path(__file__).resolve().parent.parent.parent
    log_file = getattr(settings, "log_file", "").strip() or "logs/mpinformer.log"
    return (root / log_file).resolve()


def _read_log_tail(max_lines: int = 2000) -> tuple[str, bool, Path | None, float | None]:
    """
    Прочитать последние max_lines строк из файла логов.
    Возвращает (текст, успех, путь_к_файлу, mtime или None).
    """
    path = _get_log_file_path()
    if not path.exists():
        return "Файл логов пока не создан. Записи появятся после перезапуска приложения с настроенным log_file.", False, path, None
    try:
        mtime = path.stat().st_mtime
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail = lines[-max_lines:] if len(lines) > max_lines else lines
        return "".join(tail), True, path, mtime
    except Exception as e:
        return f"Ошибка чтения логов: {e}", False, path, None


@router.get("/logs", response_class=HTMLResponse)
async def admin_logs(request: Request, username: str = Depends(verify_admin)):
    """Страница просмотра логов. Обновление только по кнопке «Обновить логи» (перезагрузка страницы)."""
    log_content, ok, log_path, log_mtime = _read_log_tail()
    msk = dateutil_tz.gettz("Europe/Moscow")
    log_mtime_str = datetime.fromtimestamp(log_mtime, tz=msk).strftime("%d.%m.%Y %H:%M:%S") if log_mtime else None
    return templates.TemplateResponse("admin/logs.html", {
        "request": request,
        "log_content": log_content,
        "log_ok": ok,
        "log_path": str(log_path),
        "log_mtime_str": log_mtime_str,
    })




@router.get("/api-check", response_class=HTMLResponse)
async def admin_api_check_page(request: Request, username: str = Depends(verify_admin)):
    """Страница ручной проверки API (заготовка под логи запроса/ответа и блоки запросов)."""
    return templates.TemplateResponse("admin/api_check.html", {
        "request": request,
        "request_blocks": [],
    })


@router.post("/api-check/posting-fbo-get", response_class=JSONResponse)
async def admin_api_check_posting_fbo_get(request: Request, username: str = Depends(verify_admin)):
    """POST /v2/posting/fbo/get — информация об отправлении (FBO)."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    posting_number = str(body.get("posting_number") or "").strip()
    if not posting_number:
        return JSONResponse(status_code=400, content={"ok": False, "error": "posting_number обязателен"})

    translit = bool(body.get("translit", True))
    with_block = body.get("with")
    if not isinstance(with_block, dict):
        with_block = {}

    analytics_data = bool(with_block.get("analytics_data", True))
    financial_data = bool(with_block.get("financial_data", True))
    legal_info = bool(with_block.get("legal_info", False))

    from app.modules.ozon.api_client import OzonAPIClient

    client = OzonAPIClient()
    resp = await client.get_posting_fbo(
        posting_number=posting_number,
        translit=translit,
        analytics_data=analytics_data,
        financial_data=financial_data,
        legal_info=legal_info,
    )

    if resp is None or (isinstance(resp, dict) and resp.get("_error")):
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": (resp or {}).get("_error") if isinstance(resp, dict) else "request_failed",
                "ozon_response": resp,
            },
        )

    request_payload = {
        "posting_number": posting_number,
        "translit": translit,
        "with": {
            "analytics_data": analytics_data,
            "financial_data": financial_data,
            "legal_info": legal_info,
        },
    }

    return JSONResponse(content={"ok": True, "request": request_payload, "response": resp})


@router.post("/api-check/posting-fbo-list", response_class=JSONResponse)
async def admin_api_check_posting_fbo_list(request: Request, username: str = Depends(verify_admin)):
    """POST /v2/posting/fbo/list — список отправлений (FBO)."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    dir_val = (body.get("dir") or "ASC").strip().upper()
    since = body.get("since")
    to = body.get("to")
    status = body.get("status")

    # allow both shapes:
    # 1) flat: {since,to,status,limit,offset,...}
    # 2) nested filter: {filter:{since,to,status}, ...}
    filt = body.get("filter")
    if isinstance(filt, dict):
        if since is None:
            since = filt.get("since")
        if to is None:
            to = filt.get("to")
        if status is None:
            status = filt.get("status")

    limit = body.get("limit")
    offset = body.get("offset")
    try:
        limit = int(limit) if limit is not None else 5
    except Exception:
        limit = 5
    try:
        offset = int(offset) if offset is not None else 0
    except Exception:
        offset = 0

    translit = bool(body.get("translit", True))
    with_block = body.get("with")
    if not isinstance(with_block, dict):
        with_block = {}

    analytics_data = bool(with_block.get("analytics_data", True))
    financial_data = bool(with_block.get("financial_data", True))
    legal_info = bool(with_block.get("legal_info", False))

    from app.modules.ozon.api_client import OzonAPIClient

    client = OzonAPIClient()
    resp = await client.get_posting_fbo_list(
        dir=dir_val,
        since=since,
        to=to,
        status=status,
        limit=limit,
        offset=offset,
        translit=translit,
        analytics_data=analytics_data,
        financial_data=financial_data,
        legal_info=legal_info,
    )

    if resp is None or (isinstance(resp, dict) and resp.get("_error")):
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": (resp or {}).get("_error") if isinstance(resp, dict) else "request_failed",
                "ozon_response": resp,
            },
        )

    request_payload = {
        "dir": dir_val,
        "filter": {"since": since, "status": status, "to": to},
        "limit": limit,
        "offset": offset,
        "translit": translit,
        "with": {
            "analytics_data": analytics_data,
            "financial_data": financial_data,
            "legal_info": legal_info,
        },
    }
    return JSONResponse(content={"ok": True, "request": request_payload, "response": resp})


def _env_set(env_file: Path, key: str, value: str) -> bool:
    """Установить или обновить переменную в .env. Возвращает True если файл обновлён."""
    if not env_file.exists():
        return False
    content = env_file.read_text(encoding="utf-8")
    lines = content.split("\n")
    updated_lines = []
    found = False
    for line in lines:
        if line.strip().startswith(f"{key}="):
            updated_lines.append(f"{key}={value}")
            found = True
        else:
            updated_lines.append(line)
    if not found:
        updated_lines.append(f"{key}={value}")
    env_file.write_text("\n".join(updated_lines), encoding="utf-8")
    return True


@router.post("/settings/update")
async def update_settings(
    request: Request,
    scheduler_interval_minutes: int = Form(None),
    log_level: str = Form(None),
    report_notification_times: str = Form(None),
    username: str = Depends(verify_admin)
):
    """Обновление настроек"""
    try:
        updated = False
        env_file = Path(".env")

        if scheduler_interval_minutes is not None and scheduler_interval_minutes > 0:
            if _env_set(env_file, "SCHEDULER_INTERVAL_MINUTES", str(scheduler_interval_minutes)):
                settings.scheduler_interval_minutes = scheduler_interval_minutes
                updated = True
                logger.info(f"Интервал планировщика обновлен: {scheduler_interval_minutes} минут")

        if log_level and log_level.upper() in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            if _env_set(env_file, "LOG_LEVEL", log_level.upper()):
                settings.log_level = log_level.upper()
                updated = True
                logger.info(f"Уровень логирования обновлен: {log_level.upper()}")

        # Время уведомлений (несколько через запятую)
        if report_notification_times is not None:
            normalized = ",".join(t.strip() for t in report_notification_times.split(",") if t.strip())
            if normalized:
                if _env_set(env_file, "REPORT_NOTIFICATION_TIMES", normalized):
                    settings.report_notification_times = normalized
                    updated = True
                    logger.info(f"Время уведомлений обновлено: {normalized}")
                if scheduler and scheduler.running:
                    stop_scheduler()
                start_scheduler()

        if updated:
            return RedirectResponse(url="/admin/settings?success=1", status_code=303)
        return RedirectResponse(url="/admin/settings?error=1", status_code=303)
    except Exception as e:
        logger.error(f"Ошибка при обновлении настроек: {e}")
        return RedirectResponse(url="/admin/settings?error=1", status_code=303)


@router.get("/scheduler/restart")
async def restart_scheduler(username: str = Depends(verify_admin)):
    """Перезапуск планировщика"""
    try:
        if scheduler and scheduler.running:
            stop_scheduler()
        start_scheduler()
        logger.info("Планировщик перезапущен")
        return RedirectResponse(url="/admin?success=1", status_code=303)
    except Exception as e:
        logger.error(f"Ошибка при перезапуске планировщика: {e}")
        return RedirectResponse(url="/admin?error=1", status_code=303)


@router.get("/report/supply-scan")
async def manual_supply_scan(username: str = Depends(verify_admin)):
    """Запустить парсинг очереди поставок в фоне (то же, что в 07:00)."""
    try:
        from app.modules.ozon.supply_scan import run_supply_queue_scan
        asyncio.create_task(run_supply_queue_scan())
        return RedirectResponse(url="/admin?success=supply_scan", status_code=303)
    except Exception as e:
        logger.exception("Manual supply scan: %s", e)
        return RedirectResponse(url="/admin?error=supply_scan", status_code=303)


@router.get("/report/slots-tracker")
async def manual_slots_tracker(username: str = Depends(verify_admin)):
    """Принудительный запуск Отслеживателя слотов в фоне (с разведением по времени с парсером)."""
    try:
        from app.modules.ozon.slots_tracker import run_slots_tracker_safe
        asyncio.create_task(run_slots_tracker_safe())
        return RedirectResponse(url="/admin?success=slots_tracker", status_code=303)
    except Exception as e:
        logger.exception("Manual slots tracker: %s", e)
        return RedirectResponse(url="/admin?error=slots_tracker", status_code=303)


@router.get("/report/manual")
async def manual_report(username: str = Depends(verify_admin)):
    """Ручной запуск формирования отчета"""
    try:
        from app.modules.notifications.reporter import collect_and_send_report

        asyncio.create_task(collect_and_send_report())
        logger.info("Ручной отчет запущен администратором")
        return RedirectResponse(url="/admin?success=report", status_code=303)
    except Exception as e:
        logger.error(f"Ошибка при запуске ручного отчета: {e}")
        return RedirectResponse(url="/admin?error=report", status_code=303)


def _project_root() -> Path:
    """Корень проекта (каталог, где лежит main.py)."""
    return Path(__file__).resolve().parent.parent.parent


def _is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Проверяет, занят ли порт (используется другим процессом)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            result = s.connect_ex((host, port))
            return result == 0
    except Exception:
        return False


@router.get("/app/restart")
async def restart_app(username: str = Depends(verify_admin)):
    """
    Перезапуск приложения: запускается новый процесс main.py, текущий завершается.
    После нажатия страница может не успеть обновиться — приложение перезагрузится через 5–7 сек.
    """
    try:
        root = _project_root()
        main_py = root / "main.py"
        if not main_py.exists():
            logger.error("main.py не найден в корне проекта")
            return RedirectResponse(url="/admin?error=restart", status_code=303)

        if scheduler and scheduler.running:
            stop_scheduler()
            logger.info("Планировщик остановлен перед перезапуском")

        # Останавливаем Telegram бота в текущем процессе, чтобы не было конфликта
        # (два процесса не могут одновременно опрашивать getUpdates)
        await stop_bot()
        logger.info("Telegram бот остановлен перед перезапуском")
        await asyncio.sleep(1.0)  # даём Telegram API освободить сессию

        # Запускаем новый процесс (тот же Python, main.py, рабочая директория = корень проекта)
        # На Windows используем CREATE_NEW_PROCESS_GROUP для независимого процесса
        creationflags = 0
        if sys.platform == "win32":
            # CREATE_NEW_PROCESS_GROUP позволяет новому процессу быть независимым
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        
        # Логируем вывод нового процесса в файл для отладки
        log_file = root / "restart.log"
        pid_file = root / "restart_pid.txt"
        try:
            log_handle = open(log_file, "a", encoding="utf-8")
        except Exception:
            log_handle = subprocess.DEVNULL
        
        # Записываем PID текущего процесса в файл — новый процесс прочитает его и будет ждать завершения старого
        # (надёжнее, чем переменные окружения на Windows)
        old_pid = os.getpid()
        try:
            pid_file.write_text(str(old_pid), encoding="utf-8")
        except Exception:
            pass
        
        # Чтобы в restart.log русский текст писался нормально (UTF-8)
        new_env = os.environ.copy()
        new_env["PYTHONIOENCODING"] = "utf-8"
        new_env["PYTHONUTF8"] = "1"  # режим UTF-8 для всего процесса (Python 3.7+)
        # Запускаем новый процесс в фоне
        new_process = subprocess.Popen(
            [sys.executable, str(main_py)],
            cwd=str(root),
            env=new_env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,  # Логируем в файл для отладки
            stderr=subprocess.STDOUT,  # stderr тоже в stdout
            creationflags=creationflags,
        )
        logger.info(f"Запущен новый процесс приложения (PID: {new_process.pid}), логи: {log_file}")

        # Стратегия перезапуска:
        # 1. Отправить ответ клиенту (мгновенно)
        # 2. Запустить новый процесс
        # 3. Очень быстро завершить старый процесс (через 0.5 сек), чтобы освободить порт
        #    до того, как uvicorn в новом процессе попытается его занять
        # 4. Новый процесс займёт порт после освобождения
        async def _exit_after_response():
            port = settings.server_port
            
            # Минимальная задержка, чтобы HTTP ответ успел отправиться клиенту
            await asyncio.sleep(0.8)
            
            # Проверяем, что новый процесс запустился (не упал сразу при старте Python)
            if new_process.poll() is not None:
                logger.error(f"Новый процесс (PID: {new_process.pid}) завершился с кодом {new_process.returncode}")
                logger.error("Проверьте логи в restart.log для деталей")
                # Не завершаем старый процесс - пусть продолжает работать
                return
            
            logger.info(f"Новый процесс (PID: {new_process.pid}) запущен, завершение текущего процесса для освобождения порта {port}...")
            logger.info("После освобождения порта новый процесс автоматически займёт его")
            
            # Завершаем старый процесс СРАЗУ - это освободит порт
            # Uvicorn в новом процессе ещё не успел попытаться занять порт (он только инициализируется)
            # После освобождения порта новый процесс успешно займёт его
            os._exit(0)

        asyncio.create_task(_exit_after_response())
        return RedirectResponse(url="/admin?success=restart", status_code=303)
    except Exception as e:
        logger.error(f"Ошибка при перезапуске приложения: {e}", exc_info=True)
        return RedirectResponse(url="/admin?error=restart", status_code=303)


@router.get("/finance", response_class=HTMLResponse)
async def admin_finance(
    request: Request,
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Раздел «Финансы» — вкладка «Доходы-Расходы»."""
    tab = (request.query_params.get("tab") or "income-expense").strip()
    if tab != "income-expense":
        tab = "income-expense"
    res = await db.execute(select(FinanceEntry).order_by(FinanceEntry.created_at.desc(), FinanceEntry.id.desc()))
    entries = res.scalars().all()
    return templates.TemplateResponse(
        "admin/finance.html",
        {
            "request": request,
            "tab": tab,
            "entries": entries,
        },
    )


@router.post("/finance/entry/save")
async def admin_finance_entry_save(
    request: Request,
    operation_type: str = Form(...),
    amount: str = Form(...),
    comment: str = Form(""),
    entry_id: str = Form(""),
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Создать/обновить запись дохода/расхода."""
    op = (operation_type or "").strip().lower()
    if op not in ("income", "expense"):
        return RedirectResponse(url="/admin/finance?tab=income-expense&error=invalid_operation", status_code=303)
    amount_raw = (amount or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount_value = float(amount_raw)
    except (TypeError, ValueError):
        return RedirectResponse(url="/admin/finance?tab=income-expense&error=invalid_amount", status_code=303)
    if amount_value <= 0:
        return RedirectResponse(url="/admin/finance?tab=income-expense&error=invalid_amount", status_code=303)
    comment_value = (comment or "").strip()
    if len(comment_value) > 512:
        comment_value = comment_value[:512]

    if (entry_id or "").strip():
        try:
            eid = int(entry_id)
        except (TypeError, ValueError):
            return RedirectResponse(url="/admin/finance?tab=income-expense&error=notfound", status_code=303)
        row = await db.get(FinanceEntry, eid)
        if not row:
            return RedirectResponse(url="/admin/finance?tab=income-expense&error=notfound", status_code=303)
        row.operation_type = op
        row.amount = amount_value
        row.comment = comment_value
        await db.commit()
        return RedirectResponse(url="/admin/finance?tab=income-expense&success=updated", status_code=303)

    row = FinanceEntry(
        operation_type=op,
        amount=amount_value,
        comment=comment_value,
    )
    db.add(row)
    await db.commit()
    return RedirectResponse(url="/admin/finance?tab=income-expense&success=created", status_code=303)


@router.post("/finance/entry/delete")
async def admin_finance_entry_delete(
    request: Request,
    entry_id: int = Form(...),
    db: AsyncSession = Depends(get_db),
    username: str = Depends(verify_admin),
):
    """Удалить запись дохода/расхода."""
    row = await db.get(FinanceEntry, entry_id)
    if not row:
        return RedirectResponse(url="/admin/finance?tab=income-expense&error=notfound", status_code=303)
    await db.delete(row)
    await db.commit()
    return RedirectResponse(url="/admin/finance?tab=income-expense&success=deleted", status_code=303)
