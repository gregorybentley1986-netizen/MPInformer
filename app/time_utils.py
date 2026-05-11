"""
Единый часовой пояс MPI: МСК (Europe/Moscow).
Все расписания, периоды и отображение времени — только в МСК.
"""
from __future__ import annotations

from datetime import datetime
from dateutil import tz as dateutil_tz

# Московское время (UTC+3) — рабочий пояс всего приложения
MSK = dateutil_tz.gettz("Europe/Moscow")


def now_msk() -> datetime:
    """Текущее время в МСК."""
    return datetime.now(MSK)
