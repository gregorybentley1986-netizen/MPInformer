"""
Telegram бот для отправки отчетов и обработки команд
"""
import os
import sys
from pathlib import Path

from telegram import Bot
from telegram.error import Conflict
from telegram.ext import Application
from loguru import logger
from app.config import settings
from app.telegram.handlers import setup_bot_handlers


bot = Bot(token=settings.telegram_bot_token)
bot_application = None

# Lock-файл: только один процесс может держать getUpdates (избегаем Conflict)
_LOCK_DIR = Path(os.environ.get("MPINFORMER_LOCK_DIR", os.path.join(os.path.expanduser("~"), ".mpinformer")))
_LOCK_FILE = _LOCK_DIR / "telegram_polling.lock"
_lock_file_handle = None


def _process_alive(pid: int) -> bool:
    """Проверка, жив ли процесс с указанным PID."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            SYNCHRONIZE = 0x100000
            h = kernel32.OpenProcess(SYNCHRONIZE, 0, pid)
            if h:
                kernel32.CloseHandle(h)
            return bool(h)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _acquire_polling_lock() -> bool:
    """Захватить блокировку «единственный процесс с polling». Возвращает True, если удалось."""
    global _lock_file_handle
    try:
        _LOCK_DIR.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    pid = os.getpid()
    for attempt in range(2):
        try:
            _lock_file_handle = open(_LOCK_FILE, "x")
            _lock_file_handle.write(str(pid))
            _lock_file_handle.flush()
            return True
        except FileExistsError:
            try:
                with open(_LOCK_FILE) as f:
                    other_pid = int(f.read().strip() or "0")
            except (ValueError, OSError):
                other_pid = 0
            if _process_alive(other_pid):
                logger.warning(
                    "Telegram polling не запущен: уже работает в другом процессе (PID %s). "
                    "Закройте тот процесс или подождите его завершения.",
                    other_pid,
                )
                return False
            try:
                _LOCK_FILE.unlink()
            except OSError:
                pass
    return False


def _release_polling_lock() -> None:
    """Освободить блокировку."""
    global _lock_file_handle
    if _lock_file_handle is not None:
        try:
            _lock_file_handle.close()
        except Exception:
            pass
        _lock_file_handle = None
    try:
        if _LOCK_FILE.exists():
            _LOCK_FILE.unlink()
    except OSError:
        pass


# В отчёте перед блоками маркетплейсов — только текст и кружки: синий Ozon, фиолетовый WB
EMOJI_OZON = "🔵"   # синий кружок
EMOJI_WB = "🟣"     # фиолетовый кружок


# Лимит длины одного сообщения Telegram (символов)
TELEGRAM_MESSAGE_MAX_LENGTH = 4096


async def send_report_with_logos(parts: dict):
    """
    Отправить отчёт в Telegram одним сообщением: шапка, блок Ozon (🔵), блок WB (🟣), итог.
    parts: dict с ключами header, ozon_section, wb_section, footer
    """
    chat_id = settings.telegram_chat_id
    if not chat_id or chat_id == "your_telegram_chat_id":
        logger.warning("TELEGRAM_CHAT_ID не настроен. Отчёт не будет отправлен.")
        return
    try:
        full_text = (
            parts["header"]
            + "\n\n"
            + f"{EMOJI_OZON} Ozon\n\n"
            + parts["ozon_section"]
            + "\n\n"
            + f"{EMOJI_WB} Wildberries\n\n"
            + parts["wb_section"]
            + "\n\n"
            + parts["footer"]
        )
        if len(full_text) <= TELEGRAM_MESSAGE_MAX_LENGTH:
            await bot.send_message(chat_id=chat_id, text=full_text, parse_mode="HTML")
        else:
            for i in range(0, len(full_text), TELEGRAM_MESSAGE_MAX_LENGTH):
                chunk = full_text[i : i + TELEGRAM_MESSAGE_MAX_LENGTH]
                await bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML")
        logger.info("Отчёт успешно отправлен в Telegram")
    except Exception as e:
        error_msg = str(e)
        if "bots can't send messages to bots" in error_msg:
            logger.error("TELEGRAM_CHAT_ID указывает на бота. Используйте ID пользователя или группы.")
        else:
            logger.error(f"Ошибка при отправке отчёта в Telegram: {e}")


async def send_report_message(text: str):
    """
    Отправить один текстовый отчёт в Telegram (для обратной совместимости).
    """
    if not settings.telegram_chat_id or settings.telegram_chat_id == "your_telegram_chat_id":
        logger.warning("TELEGRAM_CHAT_ID не настроен. Сообщение не будет отправлено.")
        return
    try:
        await bot.send_message(
            chat_id=settings.telegram_chat_id,
            text=text,
            parse_mode="HTML"
        )
        logger.info("Сообщение успешно отправлено в Telegram")
    except Exception as e:
        error_msg = str(e)
        if "bots can't send messages to bots" in error_msg:
            logger.error("TELEGRAM_CHAT_ID указывает на бота. Используйте ID пользователя или группы.")
        else:
            logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")


async def send_report_photo(photo: bytes, caption: str = ""):
    """
    Отправить фото в Telegram (например, картинку таблицы очереди поставок).
    """
    if not settings.telegram_chat_id or settings.telegram_chat_id == "your_telegram_chat_id":
        logger.warning("TELEGRAM_CHAT_ID не настроен. Фото не будет отправлено.")
        return
    if not photo:
        logger.warning("send_report_photo: пустое изображение, пропуск.")
        return
    try:
        await bot.send_photo(
            chat_id=settings.telegram_chat_id,
            photo=photo,
            caption=caption[:1024] if caption else None,
        )
        logger.info("Фото успешно отправлено в Telegram")
    except Exception as e:
        logger.error("Ошибка при отправке фото в Telegram: %s", e)


async def start_bot():
    """Запустить Telegram бота для обработки команд"""
    global bot_application

    # Проверяем, что токен заполнен
    if not settings.telegram_bot_token or settings.telegram_bot_token == "your_telegram_bot_token":
        logger.warning("Telegram токен не настроен. Бот не будет запущен.")
        return
    
    # Только один процесс на машине может держать getUpdates (lock-файл)
    if not _acquire_polling_lock():
        return
    
    try:
        # Проверка: если другой экземпляр уже держит getUpdates (тот же токен на VPS и локально или два процесса) — не запускаем polling
        try:
            await bot.get_updates(limit=0, timeout=1)
        except Conflict:
            _release_polling_lock()
            logger.warning(
                "Telegram: другой экземпляр бота уже запущен с этим токеном (Conflict). "
                "Остановите второй процесс (VPS или локальный) или подождите — бот не будет опрашивать обновления, отправка отчётов в Telegram остаётся доступной."
            )
            return

        # Создаем приложение бота
        bot_application = Application.builder().token(settings.telegram_bot_token).build()

        # Настраиваем обработчики команд
        setup_bot_handlers(bot_application)
        
        # Запускаем бота (в PTB v21+ нельзя подменять get_updates у ExtBot)
        await bot_application.initialize()
        await bot_application.start()
        await bot_application.updater.start_polling(
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True
        )
        
        logger.info("Telegram бот запущен (polling, один экземпляр по lock-файлу)")
        
    except Conflict:
        _release_polling_lock()
        logger.warning(
            "Telegram Conflict: уже запущен другой экземпляр с этим токеном. "
            "Бот не опрашивает обновления; отправка отчётов в чат по-прежнему работает."
        )
    except Exception as e:
        _release_polling_lock()
        logger.error(f"Ошибка при запуске Telegram бота: {e}")
        logger.error("Проверьте правильность токена в файле .env")
        # Не поднимаем исключение, чтобы приложение продолжало работать


async def stop_bot():
    """Остановить Telegram бота"""
    global bot_application
    
    if bot_application:
        try:
            updater = getattr(bot_application, "updater", None)
            if updater and getattr(updater, "running", False):
                await updater.stop()
            await bot_application.stop()
            await bot_application.shutdown()
            logger.info("Telegram бот остановлен")
        except Exception as e:
            logger.error(f"Ошибка при остановке Telegram бота: {e}")
        finally:
            _release_polling_lock()
