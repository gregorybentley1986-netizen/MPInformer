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
SUPPLY_JOB_ID = "supply_queue_scan_07"
SLOTS_JOB_ID = "slots_tracker_hourly"


async def _scheduled_report() -> None:
    from app.modules.informers_runtime import REPORT, is_schedule_enabled

    if not is_schedule_enabled(REPORT):
        logger.info("Отчёт по расписанию пропущен: информер выключен")
        return
    await collect_and_send_report()


async def _scheduled_supply_scan() -> None:
    from app.modules.informers_runtime import SUPPLY, is_schedule_enabled

    if not is_schedule_enabled(SUPPLY):
        logger.info("Скан поставок по расписанию пропущен: информер выключен")
        return
    await run_supply_queue_scan()


async def _scheduled_slots_tracker() -> None:
    from app.modules.informers_runtime import SLOTS, is_schedule_enabled

    if not is_schedule_enabled(SLOTS):
        logger.info("Отслеживатель слотов по расписанию пропущен: информер выключен")
        return
    await run_slots_tracker_if_due()


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


def _job_ids_for_informer(kind: str) -> list[str]:
    if kind == "report":
        if not scheduler.running:
            return []
        return [j.id for j in scheduler.get_jobs() if str(j.id).startswith(JOB_ID_PREFIX)]
    if kind == "supply":
        return [SUPPLY_JOB_ID]
    if kind == "slots":
        return [SLOTS_JOB_ID]
    return []


def apply_informer_schedule_state(kind: str, enabled: bool) -> None:
    """Пауза/возобновление cron-заданий конкретного информера."""
    if not scheduler.running:
        return
    for job_id in _job_ids_for_informer(kind):
        job = scheduler.get_job(job_id)
        if job is None:
            continue
        try:
            if enabled:
                job.resume()
                logger.info("Планировщик: job {} возобновлён", job_id)
            else:
                job.pause()
                logger.info("Планировщик: job {} на паузе", job_id)
        except Exception as e:
            logger.warning(
                "Планировщик: не удалось {} job {}: {}",
                "resume" if enabled else "pause",
                job_id,
                e,
            )


def is_informer_job_active(kind: str) -> bool:
    """Есть ли у информера активное (не на паузе) cron-задание."""
    if not scheduler.running:
        return False
    for job_id in _job_ids_for_informer(kind):
        job = scheduler.get_job(job_id)
        if job is None:
            continue
        # next_run_time is None when paused
        if job.next_run_time is not None:
            return True
    return False


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
            _scheduled_report,
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
        _scheduled_supply_scan,
        trigger=CronTrigger(hour=7, minute=0, timezone="Europe/Moscow"),
        id=SUPPLY_JOB_ID,
        name="Парсинг очереди поставок в 07:00 МСК",
        replace_existing=True,
    )
    logger.info("Добавлен парсинг очереди поставок: каждый день в 07:00 МСК")

    scheduler.add_job(
        _scheduled_slots_tracker,
        trigger=CronTrigger(minute=0, timezone="Europe/Moscow"),  # каждый час в :00 МСК
        id=SLOTS_JOB_ID,
        name="Отслеживатель слотов (проверка раз в час МСК)",
        replace_existing=True,
    )
    logger.info("Добавлен отслеживатель слотов: проверка каждый час в :00 МСК по конфигу")

    scheduler.start()

    # Применить текущие флаги вкл/выкл (рестарт планировщика без рестарта процесса)
    try:
        from app.modules.informers_runtime import REPORT, SLOTS, SUPPLY, is_schedule_enabled

        for kind in (REPORT, SUPPLY, SLOTS):
            if not is_schedule_enabled(kind):
                apply_informer_schedule_state(kind, False)
    except Exception as e:
        logger.warning("Не удалось применить флаги расписания информеров: {}", e)

    logger.info(f"Планировщик запущен. Уведомлений в день: {len(times)}")


def stop_scheduler():
    """Остановить планировщик задач."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Планировщик остановлен")
