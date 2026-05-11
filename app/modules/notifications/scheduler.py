"""
Планировщик задач: отправка отчётов, парсеры, отслеживатель.
Все задания выполняются по московскому времени (МСК, Europe/Moscow).
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from app.config import settings
from app.modules.notifications.reporter import collect_and_send_report, refresh_payout_cache
from app.modules.ozon.supply_scan import run_supply_queue_scan
from app.modules.ozon.slots_tracker import run_slots_tracker_if_due


scheduler = AsyncIOScheduler()
JOB_ID_PREFIX = "report_at_"


def _parse_notification_times(value: str) -> list[tuple[int, int]]:
    """Парсит строку времён '09:00,14:00,18:00' в список (hour, minute)."""
    result = []
    if not value or not value.strip():
        return result
    for part in value.split(","):
        part = part.strip()
        if ":" in part:
            a, b = part.split(":", 1)
            try:
                h, m = int(a.strip()) % 24, int(b.strip()) % 60
                result.append((h, m))
            except ValueError:
                continue
    return result


def start_scheduler():
    """Запустить планировщик: по одному cron-заданию на каждое время уведомления.

    Времена из settings.report_notification_times трактуются как локальное время МСК
    и напрямую используются в CronTrigger с таймзоной Europe/Moscow.
    """
    if scheduler.running:
        logger.warning("Планировщик уже запущен")
        return

    times_str = getattr(settings, "report_notification_times", None) or "09:00"
    times = _parse_notification_times(times_str)

    if not times:
        logger.warning("Не задано ни одного времени уведомления. Добавлена задача на 09:00.")
        times = [(9, 0)]

    for i, (hour, minute) in enumerate(times):
        job_id = f"{JOB_ID_PREFIX}{hour:02d}_{minute:02d}_{i}"
        scheduler.add_job(
            collect_and_send_report,
            trigger=CronTrigger(hour=hour, minute=minute, timezone="Europe/Moscow"),
            id=job_id,
            name=f"Отчет в {hour:02d}:{minute:02d}",
            replace_existing=True,
        )
        logger.info(f"Добавлено уведомление: каждый день в {hour:02d}:{minute:02d}")

    scheduler.add_job(
        refresh_payout_cache,
        trigger=CronTrigger(hour=0, minute=0, timezone="Europe/Moscow"),
        id="payout_cache_daily",
        name="Обновление кэша выплат в 00:00 МСК",
        replace_existing=True,
    )
    logger.info("Добавлено автообновление кэша выплат: каждый день в 00:00")

    scheduler.add_job(
        run_supply_queue_scan,
        trigger=CronTrigger(hour=7, minute=0, timezone="Europe/Moscow"),
        id="supply_queue_scan_07",
        name="Парсинг очереди поставок в 07:00 МСК",
        replace_existing=True,
    )
    logger.info("Добавлен парсинг очереди поставок: каждый день в 07:00 МСК")

    scheduler.add_job(
        run_slots_tracker_if_due,
        trigger=CronTrigger(minute=0, timezone="Europe/Moscow"),  # каждый час в :00 МСК
        id="slots_tracker_hourly",
        name="Отслеживатель слотов (проверка раз в час МСК)",
        replace_existing=True,
    )
    logger.info("Добавлен отслеживатель слотов: проверка каждый час в :00 МСК по конфигу")

    scheduler.start()
    logger.info(f"Планировщик запущен. Уведомлений в день: {len(times)}")


def stop_scheduler():
    """Остановить планировщик задач."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Планировщик остановлен")
