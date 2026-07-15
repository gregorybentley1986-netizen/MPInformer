"""
Управление фоновыми информерами: трекинг задач и кооперативная остановка.
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


def is_stop_requested(kind: str) -> bool:
    return _event(kind).is_set()


def clear_stop(kind: str) -> None:
    _event(kind).clear()


def request_stop(kind: str) -> dict[str, Any]:
    """Запросить остановку информера. Возвращает статус до запроса."""
    if kind not in KINDS:
        return {"ok": False, "error": "unknown_kind", "was_running": False}
    was_running = is_running(kind)
    _event(kind).set()
    t = _tasks.get(kind)
    if t is not None and not t.done():
        t.cancel()
        logger.info("Informer {}: запрошена остановка (cancel task)", kind)
    else:
        logger.info("Informer {}: запрошена остановка (флаг)", kind)
    return {"ok": True, "kind": kind, "was_running": was_running}


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


@asynccontextmanager
async def running_scope(kind: str) -> AsyncIterator[None]:
    """Пометить информер как выполняющийся (в т.ч. для джобов планировщика)."""
    _active_count[kind] = _active_count.get(kind, 0) + 1
    try:
        yield
    finally:
        _active_count[kind] = max(0, _active_count.get(kind, 0) - 1)


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
            if _tasks.get(kind) is asyncio.current_task():
                _tasks.pop(kind, None)

    task = asyncio.create_task(_runner(), name=f"informer:{kind}")
    _tasks[kind] = task
    return task


def status_snapshot() -> dict[str, dict[str, bool]]:
    return {
        kind: {
            "running": is_running(kind),
            "stop_requested": is_stop_requested(kind),
        }
        for kind in KINDS
    }
