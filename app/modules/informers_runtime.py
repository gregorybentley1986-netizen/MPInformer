"""
Управление фоновыми информерами: трекинг задач, остановка и вкл/выкл расписания.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Coroutine
from typing import Any

from loguru import logger

REPORT = "report"
SUPPLY = "supply"
SLOTS = "slots"
KINDS = (REPORT, SUPPLY, SLOTS)

_tasks: dict[str, asyncio.Task] = {}
_stop_events: dict[str, asyncio.Event] = {}
_active_count: dict[str, int] = {k: 0 for k in KINDS}
# Расписание (cron): включено по умолчанию. Стоп выключает; «Включить» — снова включает.
_schedule_enabled: dict[str, bool] = {k: True for k in KINDS}


def _event(kind: str) -> asyncio.Event:
    ev = _stop_events.get(kind)
    if ev is None:
        ev = asyncio.Event()
        _stop_events[kind] = ev
    return ev


def is_running(kind: str) -> bool:
    if _active_count.get(kind, 0) > 0:
        return True
    t = _tasks.get(kind)
    return t is not None and not t.done()


def is_schedule_enabled(kind: str) -> bool:
    return bool(_schedule_enabled.get(kind, True))


def set_schedule_enabled(kind: str, enabled: bool) -> None:
    if kind not in KINDS:
        return
    _schedule_enabled[kind] = bool(enabled)
    logger.info("Informer {}: расписание {}", kind, "включено" if enabled else "выключено")
    try:
        from app.modules.notifications.scheduler import apply_informer_schedule_state

        apply_informer_schedule_state(kind, bool(enabled))
    except Exception as e:
        logger.warning("Informer {}: не удалось обновить job планировщика: {}", kind, e)


def is_stop_requested(kind: str) -> bool:
    return _event(kind).is_set()


def clear_stop(kind: str) -> None:
    _event(kind).clear()


def _register_task(kind: str, task: asyncio.Task | None) -> None:
    if task is None:
        return
    _tasks[kind] = task


def _unregister_task(kind: str, task: asyncio.Task | None) -> None:
    if task is not None and _tasks.get(kind) is task:
        _tasks.pop(kind, None)


def request_stop(kind: str) -> dict[str, Any]:
    """Запросить остановку текущего запуска и выключить расписание."""
    if kind not in KINDS:
        return {"ok": False, "error": "unknown_kind", "was_running": False, "was_enabled": False}

    was_running = is_running(kind)
    was_enabled = is_schedule_enabled(kind)

    _event(kind).set()
    set_schedule_enabled(kind, False)

    t = _tasks.get(kind)
    cancelled = False
    if t is not None and not t.done():
        t.cancel()
        cancelled = True
        logger.info("Informer {}: остановка (cancel task id={})", kind, id(t))
    else:
        logger.info(
            "Informer {}: остановка (флаг stop, running={}, schedule_was={})",
            kind,
            was_running,
            was_enabled,
        )

    return {
        "ok": True,
        "kind": kind,
        "was_running": was_running,
        "was_enabled": was_enabled,
        "cancelled": cancelled,
    }


async def wait_until_stopped(kind: str, timeout: float = 5.0) -> bool:
    """Дождаться завершения задачи после request_stop. True — уже не running."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, timeout)
    while is_running(kind):
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(0.1)
    return True


async def sleep_or_stop(kind: str, seconds: float) -> bool:
    """Сон с прерыванием по stop. True — нужно остановиться."""
    if is_stop_requested(kind):
        return True
    if seconds <= 0:
        return is_stop_requested(kind)
    try:
        await asyncio.wait_for(_event(kind).wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return is_stop_requested(kind)
    except asyncio.CancelledError:
        raise


@asynccontextmanager
async def running_scope(kind: str) -> AsyncIterator[None]:
    """Пометить информер как выполняющийся и зарегистрировать текущую задачу для cancel()."""
    task = asyncio.current_task()
    _register_task(kind, task)
    _active_count[kind] = _active_count.get(kind, 0) + 1
    try:
        yield
    finally:
        _active_count[kind] = max(0, _active_count.get(kind, 0) - 1)
        _unregister_task(kind, task)


def spawn(kind: str, coro: Coroutine[Any, Any, Any]) -> asyncio.Task | None:
    """
    Запустить информер в фоне. Если уже выполняется — coro закрывается, возвращает None.
    """
    if kind not in KINDS:
        coro.close()
        return None
    if is_running(kind):
        logger.warning("Informer {}: уже выполняется, повторный запуск пропущен", kind)
        coro.close()
        return None

    clear_stop(kind)

    async def _runner() -> None:
        try:
            await coro
        except asyncio.CancelledError:
            logger.info("Informer {}: задача отменена", kind)
            raise
        except Exception:
            logger.exception("Informer {}: ошибка выполнения", kind)
            raise
        finally:
            _unregister_task(kind, asyncio.current_task())

    task = asyncio.create_task(_runner(), name=f"informer:{kind}")
    _register_task(kind, task)
    return task


def status_snapshot() -> dict[str, dict[str, bool]]:
    return {
        kind: {
            "running": is_running(kind),
            "stop_requested": is_stop_requested(kind),
            "schedule_enabled": is_schedule_enabled(kind),
        }
        for kind in KINDS
    }
