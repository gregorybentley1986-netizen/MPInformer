"""
Скрипт инициализации базы данных
"""
import asyncio
from app.db.database import engine, Base
from loguru import logger


async def init_db():
    """Создать все таблицы в базе данных"""
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("База данных успешно инициализирована")
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(init_db())
