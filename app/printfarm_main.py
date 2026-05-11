"""
Точка входа FastAPI-приложения PrintFarm (локальный сервер).

Идея разделения:
- MPInformer на VPS отвечает за маркетплейсы/поставки/телеграм.
- PrintFarm отвечает за планирование печати и склад/принтеры.

На первом этапе PrintFarm использует те же модули `app.site.routes` и
`app.admin.routes`, что и MPInformer (без физического дублирования файлов),
но ограничивает публичные URL allowlist-ом middleware — маркетплейсные разделы
(`/supply-queue`, `/api/supplies`, …) недоступны. Админка доступна под `/admin`
(справочники принтеров, материалов, изделий — нужны складу и печати).
"""

from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

from dateutil import tz as dateutil_tz
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from loguru import logger
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.staticfiles import StaticFiles

from app.admin.routes import router as admin_router
from app.config import settings
from app.site.routes import router as site_router

# --- Логирование (как в app/main.py) ---
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level=settings.log_level,
)

_MSK = dateutil_tz.gettz("Europe/Moscow")


def _log_filter_msk(record):
    """Переводит время в записи лога в МСК перед выводом в файл."""
    t = record["time"]
    if t.tzinfo is None:
        t = datetime.fromtimestamp(t.timestamp(), tz=dateutil_tz.UTC).astimezone(_MSK)
    else:
        t = t.astimezone(_MSK)
    record["time"] = t
    return True


LOG_FILE_MAX_LINES = 5000

# Запись в файл для просмотра в админке/журнале (опционально).
try:
    _log_file = getattr(settings, "log_file", "").strip()
    if _log_file:
        _project_root = Path(__file__).resolve().parent.parent
        _log_path = (_project_root / _log_file).resolve()
        _log_path.parent.mkdir(parents=True, exist_ok=True)

        _lines_written = [0]

        def _file_sink(message: str):
            with open(_log_path, "a", encoding="utf-8") as f:
                f.write(message + "\n")
            _lines_written[0] += 1
            if _lines_written[0] % 100 == 0:
                # Просто оставляем файл "как есть" — обрезка логов не критична для локального MVP.
                pass

        logger.add(
            _file_sink,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
            level=settings.log_level,
            filter=_log_filter_msk,
        )
except Exception as e:
    print(f"[PrintFarm] Логи в файл не пишем: {e}", flush=True)
    print(traceback.format_exc(), flush=True)


# --- Создание приложения ---
app = FastAPI(
    title="PrintFarm",
    description="Планирование печати и склад/принтеры (локальный сервер)",
    version="0.1.0",
)

# Сессии для логина на /login (логин по форме, авторизация по сессии).
_session_secret = getattr(settings, "session_secret_key", "printfarm-session-secret")
if not isinstance(_session_secret, str) or not _session_secret.strip():
    _session_secret = "printfarm-session-secret"
app.add_middleware(SessionMiddleware, secret_key=_session_secret)


# --- Патчим Request.form() для больших STL загрузок ---
# На PrintFarm может не использоваться админка, но патч безопасен.
_STL_FORM_PATHS = (
    "/admin/reference/parts/stl",
    "/admin/reference/parts/stl/upload-thumb",
    "/admin/reference/print_jobs/gcode/upload-thumb",
)
_original_request_form = StarletteRequest.form


async def _patched_request_form(self, max_files=1000, max_fields=1000, max_part_size=None):
    scope = getattr(self, "scope", None)
    path = (scope.get("path") or "").strip() if isinstance(scope, dict) else ""
    if not path.startswith("/"):
        path = "/" + path
    if path in _STL_FORM_PATHS:
        max_part_size = 5 * 1024 * 1024 if "upload-thumb" in path else 50 * 1024 * 1024
    elif max_part_size is None:
        max_part_size = 1024 * 1024

    try:
        result = await _original_request_form(self, max_files=max_files, max_fields=max_fields, max_part_size=max_part_size)
        return result
    except StarletteHTTPException as e:
        raise
    except TypeError:
        if path in _STL_FORM_PATHS:
            logger.warning("Starlette не поддерживает max_part_size. Установите starlette>=0.40.0")
        return await _original_request_form(self, max_files=max_files, max_fields=max_fields)


StarletteRequest.form = _patched_request_form


@app.middleware("http")
async def _stl_large_form_middleware(request: StarletteRequest, call_next):
    """Тот же проход, что в main.py: POST на большие формы админки."""
    if request.method != "POST" or request.url.path not in (
        "/admin/reference/parts/stl",
        "/admin/reference/parts/stl/upload-thumb",
        "/admin/reference/print_jobs/gcode/upload-thumb",
    ):
        return await call_next(request)
    return await call_next(request)


@app.middleware("http")
async def _printfarm_allowlist_middleware(request: StarletteRequest, call_next):
    """
    Ограничиваем доступ внутри PrintFarm, чтобы маркетплейсные эндпоинты
    (supply-queue, supplies, warehouse-stocks и т.п.) случайно не вызывались.
    """
    path = request.url.path or ""

    # Редирект корня на /print-queue регистрируется отдельным роутером (до маркетплейсной `/`).
    if path == "/":
        return await call_next(request)

    allowed_prefixes = (
        "/login",
        "/logout",
        "/print-queue",
        "/print-plan",
        "/api/print-queue",
        "/api/print-plan",
        "/warehouse",
        "/packaging-tasks",
        "/admin",
        "/health",
        "/uploads",
        "/static",
    )

    # Разрешаем только точку `/api` (информация), а не весь префикс `/api/*`,
    # чтобы не открывать маркетплейсные эндпоинты (`/api/supplies` и т.п.).
    if path == "/api":
        return await call_next(request)

    if any(path == p or path.startswith(p + "/") for p in allowed_prefixes):
        return await call_next(request)

    # Непринадлежащие PrintFarm пути скрываем как 404.
    return JSONResponse(status_code=404, content={"detail": "Not Found"})


# --- Роуты: главная ведёт на печать (до site_router, иначе перехватит маркетплейсная `/`). ---
_pf_bootstrap = APIRouter(tags=["printfarm"])


@_pf_bootstrap.get("/")
async def printfarm_root_redirect():
    return RedirectResponse(url="/print-queue", status_code=302)


app.include_router(_pf_bootstrap)
app.include_router(site_router)
app.include_router(admin_router)

# --- Статика/загрузки ---
_project_root = Path(__file__).resolve().parent.parent
_uploads_dir = _project_root / (getattr(settings, "uploads_dir", "uploads") or "uploads")
_static_dir = _project_root / "static"
if _uploads_dir.exists():
    app.mount("/uploads", StaticFiles(directory=str(_uploads_dir)), name="uploads")
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# --- Exceptions (как в app/main.py) ---
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    headers = getattr(exc, "headers", None)
    location = None
    if exc.status_code == 303 and headers and isinstance(headers, dict):
        location = headers.get("Location") or headers.get("location")
    if location:
        return RedirectResponse(url=str(location), status_code=303)
    try:
        detail = exc.detail if isinstance(exc.detail, (str, type(None))) else exc.detail
        return JSONResponse(status_code=exc.status_code, content={"detail": detail})
    except Exception:
        return JSONResponse(status_code=exc.status_code, content={"detail": "Error"})


@app.exception_handler(Exception)
async def log_unhandled_exception(request: Request, exc: Exception):
    if isinstance(exc, StarletteHTTPException):
        raise exc
    logger.error(
        "Unhandled exception: {} | path={}\n{}",
        exc,
        request.url.path,
        traceback.format_exc(),
    )
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error", "path": request.url.path})


# --- Health/API ---
@app.get("/api")
async def api_info():
    return {"message": "PrintFarm API", "version": "0.1.0", "status": "running"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# --- Startup: миграции/инициализация ---
@app.on_event("startup")
async def startup_event():
    """
    Инициализация при запуске PrintFarm.
    Миграции запускаем так же, как в MPInformer, чтобы гарантировать нужные таблицы.
    """
    from sqlalchemy import text

    from app.db.database import engine
    from app.db.models import Base
    from app.db.migrations import (
        ensure_part_stl_thumb,
        ensure_product_part_material_id,
        ensure_product_article,
        ensure_print_jobs_table,
        ensure_print_job_gcode_thumb,
        ensure_print_queue_items_table,
        ensure_print_queue_items_sequence,
        ensure_print_plans_table,
        ensure_print_plan_items_table,
        ensure_spools_table,
        ensure_printers_current_spool_id,
        ensure_product_barcode_columns,
        ensure_product_ozon_sku,
        ensure_product_wildberries_sku,
        ensure_packaging_tasks_tables,
        ensure_supply_queue_result_day_counts,
        ensure_ozon_supplies_table,
        ensure_warehouse_extra_stock_table,
        ensure_written_off_materials_table,
        ensure_printed_part_stock_table,
        ensure_assembled_product_stock_table,
        ensure_warehouse_assembly_batch_tables,
        ensure_assembled_product_stock_log_table,
        ensure_warehouse_defect_records_table,
        ensure_printed_part_stock_log_table,
    )

    def _ensure_material_plastic_type(conn):
        try:
            conn.execute(text("ALTER TABLE materials ADD COLUMN plastic_type VARCHAR(128) NOT NULL DEFAULT ''"))
        except Exception:
            pass

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # ensure_* функции идемпотентные (должны), чтобы PrintFarm можно было запускать безопасно.
        await conn.run_sync(_ensure_material_plastic_type)
        await conn.run_sync(ensure_part_stl_thumb)
        await conn.run_sync(ensure_product_part_material_id)
        await conn.run_sync(ensure_product_article)
        await conn.run_sync(ensure_print_jobs_table)
        await conn.run_sync(ensure_print_job_gcode_thumb)
        await conn.run_sync(ensure_print_queue_items_table)
        await conn.run_sync(ensure_print_queue_items_sequence)
        await conn.run_sync(ensure_print_plans_table)
        await conn.run_sync(ensure_print_plan_items_table)
        await conn.run_sync(ensure_spools_table)
        await conn.run_sync(ensure_printers_current_spool_id)
        await conn.run_sync(ensure_product_barcode_columns)
        await conn.run_sync(ensure_product_ozon_sku)
        await conn.run_sync(ensure_product_wildberries_sku)
        await conn.run_sync(ensure_packaging_tasks_tables)
        await conn.run_sync(ensure_supply_queue_result_day_counts)
        await conn.run_sync(ensure_ozon_supplies_table)
        await conn.run_sync(ensure_warehouse_extra_stock_table)
        await conn.run_sync(ensure_written_off_materials_table)
        await conn.run_sync(ensure_printed_part_stock_table)
        await conn.run_sync(ensure_assembled_product_stock_table)
        await conn.run_sync(ensure_warehouse_assembly_batch_tables)
        await conn.run_sync(ensure_assembled_product_stock_log_table)
        await conn.run_sync(ensure_warehouse_defect_records_table)
        await conn.run_sync(ensure_printed_part_stock_log_table)

    logger.info("[PrintFarm] Запуск...")
    logger.info("[PrintFarm] Миграции выполнены/проверены")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("[PrintFarm] Остановка...")

