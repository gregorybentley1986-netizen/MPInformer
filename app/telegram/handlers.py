"""
Обработчики команд Telegram бота
"""
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from loguru import logger
from app.config import settings


# Создаем клавиатуру с кнопками
def get_keyboard():
    """Создать клавиатуру с кнопками"""
    keyboard = [
        [
            KeyboardButton("📊 Получить отчет"),
            KeyboardButton("✅ Статус")
        ],
        [
            KeyboardButton("ℹ️ Помощь")
        ]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот MPInformer.\n\n"
        "Я отправляю аналитические отчеты по заказам с маркетплейсов Ozon и Wildberries.\n\n"
        "Используйте кнопки ниже для быстрого доступа к функциям.\n\n"
        "Доступные команды:\n"
        "/start - Показать это сообщение\n"
        "/status - Статус системы\n"
        "/report - Получить отчет вручную",
        reply_markup=get_keyboard()
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status"""
    await update.message.reply_text(
        "✅ Система работает\n"
        f"Интервал проверки: {settings.scheduler_interval_minutes} минут",
        reply_markup=get_keyboard()
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /report"""
    # Lazy import для избежания циклического импорта
    from app.modules.notifications.reporter import collect_and_send_report
    
    await update.message.reply_text("⏳ Формирую отчет...", reply_markup=get_keyboard())
    try:
        await collect_and_send_report()
        await update.message.reply_text("✅ Отчет отправлен!", reply_markup=get_keyboard())
    except Exception as e:
        logger.error(f"Ошибка при формировании отчета: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при формировании отчета: {e}",
            reply_markup=get_keyboard()
        )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help или кнопки Помощь"""
    await update.message.reply_text(
        "ℹ️ Помощь по использованию бота:\n\n"
        "📊 Получить отчет - сформировать и отправить аналитический отчет по заказам\n"
        "✅ Статус - показать текущий статус системы\n"
        "ℹ️ Помощь - показать это сообщение\n\n"
        "Отчеты также отправляются автоматически каждые "
        f"{settings.scheduler_interval_minutes} минут.",
        reply_markup=get_keyboard()
    )


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений (нажатие на кнопки)"""
    text = (update.message.text or "").strip()
    logger.info("Бот получил текст (кнопка?): %r", text[:80])
    # Сопоставление по смыслу: кнопки могут приходить с разным форматированием эмодзи
    if "Получить отчет" in text or "отчет" in text.lower():
        try:
            await report_command(update, context)
        except Exception as e:
            logger.exception("Ошибка при обработке кнопки «Получить отчет»")
            await update.message.reply_text(
                f"❌ Ошибка: {e}. Попробуйте команду /report.",
                reply_markup=get_keyboard()
            )
        return
    if "Статус" in text:
        try:
            await status_command(update, context)
        except Exception as e:
            logger.exception("Ошибка при обработке кнопки «Статус»")
            await update.message.reply_text(
                f"❌ Ошибка: {e}. Попробуйте команду /status.",
                reply_markup=get_keyboard()
            )
        return
    if "Помощь" in text:
        try:
            await help_command(update, context)
        except Exception as e:
            logger.exception("Ошибка при обработке кнопки «Помощь»")
            await update.message.reply_text(
                f"❌ Ошибка: {e}. Попробуйте команду /help.",
                reply_markup=get_keyboard()
            )
        return
    # Не распознано — показываем помощь
    logger.debug("Текст от пользователя не распознан как кнопка: %r", text[:100])
    await update.message.reply_text(
        "Не понимаю эту команду. Используйте кнопки или команды:\n"
        "/start, /status, /report, /help",
        reply_markup=get_keyboard()
    )


def setup_bot_handlers(application: Application):
    """Настроить обработчики команд бота"""
    # Обработчики команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(CommandHandler("help", help_command))
    
    # Обработчик текстовых сообщений (для кнопок)
    # Важно: этот обработчик должен быть последним, чтобы не перехватывать команды
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
