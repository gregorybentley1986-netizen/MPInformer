"""
Конфигурация приложения
"""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Корень проекта (рядом с .env), чтобы .env загружался при любом рабочем каталоге
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Настройки приложения"""

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Ozon API
    ozon_client_id: str
    ozon_api_key: str
    # Пауза между запросами к api-seller.ozon.ru (сек), см. OzonAPIClient._ozon_request
    ozon_request_min_interval_sec: float = 0.25
    
    # Wildberries API
    wb_api_key: str
    
    # Telegram Bot
    telegram_bot_token: str
    telegram_chat_id: str
    
    # Scheduler
    scheduler_interval_minutes: int = 60
    
    # Report Settings (все времена — МСК, Europe/Moscow)
    # Время уведомлений: через запятую, МСК, например "09:00,14:00,18:00"
    report_notification_times: str = "09:00"
    
    # Database
    database_url: str = "sqlite:///./mpinformer.db"
    
    # Logging
    log_level: str = "INFO"
    # Файл логов (относительно корня проекта); если пусто — в файл не пишем
    log_file: str = "logs/mpinformer.log"
    
    # Server
    server_port: int = 8001  # Порт по умолчанию (можно изменить через переменную окружения)

    # Очередь поставок Ozon: ограничение параллельных POST supply-order/bundle и пауза после каждого (сек)
    supply_queue_bundle_delay_sec: float = 0.15
    supply_queue_bundle_max_concurrent: int = 3
    
    # Админка (логин и пароль из .env, авторизация по сессии)
    admin_username: str = "admin"
    admin_password: str = "admin123"  # Обязательно смените в .env на VPS
    # Секрет для подписи сессий (укажите в .env на VPS)
    session_secret_key: str = "mpinformer-session-secret-change-in-env"


settings = Settings()
