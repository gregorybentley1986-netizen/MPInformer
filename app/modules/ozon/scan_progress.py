"""In-memory прогресс supply scan (тот же процесс uvicorn)."""

from __future__ import annotations

_supply_scan_progress: dict | None = None


def get_supply_scan_progress() -> dict | None:
    if not _supply_scan_progress:
        return None
    return dict(_supply_scan_progress)


def set_supply_scan_progress(*, scan_id: int, total: int, done: int) -> None:
    global _supply_scan_progress
    _supply_scan_progress = {
        "scan_id": int(scan_id),
        "total": int(total),
        "done": int(done),
    }


def clear_supply_scan_progress() -> None:
    global _supply_scan_progress
    _supply_scan_progress = None
