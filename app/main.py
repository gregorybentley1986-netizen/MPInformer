"""
Главный файл приложения FastAPI
"""
import traceback
from datetime import datetime
from pathlib import Path

from dateutil import tz as dateutil_tz
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from loguru import logger
import os
import sys

from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.staticfiles import StaticFiles

# Ранний импорт: при ошибке здесь uvicorn выдаёт "app not found" — проверка на сервере: python -c "from app.main import app"
from app.config import settings
from app.modules.notifications.scheduler import start_scheduler
from app.telegram.bot import start_bot, stop_bot
from app.admin.routes import router as admin_router
from app.site.routes import router as site_router
from app.site.shift_routes import router as shift_router

# Корень проекта для пути к файлу логов и загрузок
_project_root = Path(__file__).resolve().parent.parent
_uploads_dir = _project_root / (getattr(settings, "uploads_dir", "uploads") or "uploads")
_static_dir = _project_root / "static"
_uploads_dir.mkdir(parents=True, exist_ok=True)

# Настройка логирования (в терминал — stderr, чтобы видеть логи при запуске python main.py)
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level=settings.log_level
)
# Часовой пояс для времени в файле логов (МСК)
_MSK = dateutil_tz.gettz("Europe/Moscow")


LOG_FILE_MAX_LINES = 5000


def _log_filter_msk(record):
    """Переводит время в записи лога в МСК перед выводом в файл."""
    t = record["time"]
    if t.tzinfo is None:
        t = datetime.fromtimestamp(t.timestamp(), tz=dateutil_tz.UTC).astimezone(_MSK)
    else:
        t = t.astimezone(_MSK)
    record["time"] = t
    return True


def _trim_log_file(path: Path, max_lines: int) -> None:
    """Оставить в файле только последние max_lines строк."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        if len(lines) <= max_lines:
            return
        path.write_text("\n".join(lines[-max_lines:]) + "\n", encoding="utf-8")
    except Exception:
        pass


# Запись в файл для просмотра в админке (не более 5000 строк, старые удаляются)
try:
    _log_file = getattr(settings, "log_file", "").strip()
    if _log_file:
        _log_path = (_project_root / _log_file).resolve()
        _log_path.parent.mkdir(parents=True, exist_ok=True)

        _lines_written = [0]

        def _file_sink(message):
            with open(_log_path, "a", encoding="utf-8") as f:
                f.write(message + "\n")
            _lines_written[0] += 1
            if _lines_written[0] % 100 == 0:
                _trim_log_file(_log_path, LOG_FILE_MAX_LINES)

        logger.add(
            _file_sink,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
            level=settings.log_level,
            filter=_log_filter_msk,
        )
except Exception as e:
    import traceback as _tb
    print(f"[MPInformer] Логи в файл не пишем: {e}", flush=True)
    print(_tb.format_exc(), flush=True)

app = FastAPI(
    title="MPInformer",
    description="Приложение для работы с данными маркетплейсов",
    version="0.1.0"
)

# Сессии для админки (логин по форме, не Basic Auth)
_session_secret = getattr(settings, "session_secret_key", "mpinformer-session-secret")
if not isinstance(_session_secret, str) or not _session_secret.strip():
    _session_secret = "mpinformer-session-secret"
app.add_middleware(SessionMiddleware, secret_key=_session_secret)


# Для путей загрузки STL подставляем лимит 50 МБ (Starlette по умолчанию 1 МБ).
# Маршрут получает новый Request(scope, receive, send), а не наш wrapper — патчим все Request.
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
        # Всегда задаём лимит для этих путей (обход 1 МБ в Starlette)
        max_part_size = 5 * 1024 * 1024 if "upload-thumb" in path else 50 * 1024 * 1024
    elif max_part_size is None:
        max_part_size = 1024 * 1024  # дефолт Starlette 1 МБ для остальных путей
    try:
        result = await _original_request_form(self, max_files=max_files, max_fields=max_fields, max_part_size=max_part_size)
        return result
    except StarletteHTTPException as e:
        raise
    except TypeError:
        # Starlette < 0.40 не принимает max_part_size — вызываем без него (лимит останется 1 МБ)
        if path in _STL_FORM_PATHS:
            logger.warning("Starlette не поддерживает max_part_size. Установите starlette>=0.40.0 для загрузки STL > 1 МБ")
        return await _original_request_form(self, max_files=max_files, max_fields=max_fields)
    except Exception:
        raise

StarletteRequest.form = _patched_request_form


class _LargeFormRequest(StarletteRequest):
    """Дублируем лимит на случай, если маршрут получит этот request."""
    _form_max = 50 * 1024 * 1024

    async def form(self, max_files=1000, max_fields=1000, max_part_size=None):
        size = max_part_size if max_part_size is not None else self._form_max
        return await _original_request_form(self, max_files=max_files, max_fields=max_fields, max_part_size=size)


@app.middleware("http")
async def _stl_large_form_middleware(request: StarletteRequest, call_next):
    # Для STL не подменяем request: лимит 50 МБ задаётся патчем StarletteRequest.form(),
    # и исключения парсера должны ловиться в патче (try/except там). Раньше подменяли на
    # _LargeFormRequest — тогда исключения шли мимо патча и 400 не логировались.
    if request.method != "POST" or request.url.path not in (
        "/admin/reference/parts/stl",
        "/admin/reference/parts/stl/upload-thumb",
        "/admin/reference/print_jobs/gcode/upload-thumb",
    ):
        return await call_next(request)
    return await call_next(request)


# Публичные страницы сервиса (главная, очередь печати, очередь поставок)
app.include_router(site_router)
app.include_router(shift_router)
# Админ-панель
app.include_router(admin_router)
# Загрузки (STL деталей, фото деталей и изделий) — по /uploads/...
app.mount("/uploads", StaticFiles(directory=str(_uploads_dir)), name="uploads")
# Статика сайта (иконки, картинки) — по /static/...
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Редирект при 303 (неавторизованный доступ к сайту)."""
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
    """Логируем любую необработанную ошибку (в journalctl будет трейсбек)."""
    if isinstance(exc, HTTPException):
        raise exc
    logger.error(
        "Unhandled exception: {} | path={}\n{}",
        exc,
        request.url.path,
        traceback.format_exc(),
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "path": request.url.path},
    )


def _ensure_material_plastic_type(conn):
    """Добавить колонку plastic_type в materials, если её нет (миграция)."""
    from sqlalchemy import text
    try:
        conn.execute(text("ALTER TABLE materials ADD COLUMN plastic_type VARCHAR(128) NOT NULL DEFAULT ''"))
    except Exception:
        pass


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске приложения"""
    from app.db.database import engine
    from app.db.models import Base
    from app.db.migrations import (
    ensure_part_stl_thumb, ensure_product_part_material_id, ensure_product_article,
    ensure_print_jobs_table, ensure_print_job_gcode_thumb, ensure_print_queue_items_table,
    ensure_print_queue_items_sequence, ensure_print_plans_table, ensure_print_plan_items_table,
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
    ensure_finance_schema,
    ensure_user_role,
    ensure_shift_planning_tables,
)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
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
        await conn.run_sync(ensure_finance_schema)
        await conn.run_sync(ensure_user_role)
        await conn.run_sync(ensure_shift_planning_tables)
    logger.info("Запуск MPInformer...")
    logger.info(f"Время уведомлений: {getattr(settings, 'report_notification_times', '09:00')}")
    
    # Запуск планировщика задач
    start_scheduler()
    logger.info("Планировщик задач запущен")
    
    # Запуск Telegram бота (при перезапуске из админки — пауза, чтобы старый процесс успел освободить getUpdates)
    if os.environ.pop("MPINFORMER_DELAY_TELEGRAM", None):
        import asyncio
        await asyncio.sleep(15.0)
        logger.info("Пауза перед запуском Telegram бота после перезапуска завершена")
    try:
        await start_bot()
    except Exception as e:
        logger.error(f"Не удалось запустить Telegram бота: {e}")
        logger.warning("Приложение продолжит работу без Telegram бота")


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке приложения"""
    logger.info("Остановка MPInformer...")
    
    # Остановка Telegram бота
    await stop_bot()


@app.get("/api")
async def api_info():
    """Информация об API"""
    return {
        "message": "MPInformer API",
        "version": "0.1.0",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """Проверка здоровья приложения"""
    return {"status": "healthy"}
