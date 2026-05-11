# MPInformer

Веб-приложение для работы с данными от маркетплейсов (Ozon и Wildberries) через API. Модуль оповещений получает данные о заказах и отправляет аналитические отчеты через Telegram бот.

## Технологический стек

### Backend Framework
- **FastAPI** - современный, быстрый веб-фреймворк для Python с поддержкой асинхронности
- **Uvicorn** - ASGI сервер для запуска FastAPI приложения

### Работа с API маркетплейсов
- **httpx** - асинхронный HTTP клиент для работы с API Ozon и Wildberries
- **requests** - синхронный HTTP клиент (резервный вариант)

### Telegram Bot
- **python-telegram-bot** - библиотека для работы с Telegram Bot API

### Планировщик задач
- **APScheduler** - планировщик для периодических запросов к API маркетплейсов

### База данных
- **SQLite** - легковесная БД для локальной разработки (легко мигрировать на PostgreSQL для VPS)

### Конфигурация и переменные окружения
- **python-dotenv** - загрузка переменных окружения из .env файла
- **pydantic** - валидация настроек (встроен в FastAPI)

### Дополнительно
- **python-dateutil** - работа с датами и временем
- **loguru** - удобное логирование

## Установка

### Требования
- Python 3.10 или выше
- pip (менеджер пакетов Python)

### Шаги установки

1. **Клонируйте репозиторий или перейдите в директорию проекта:**
   ```bash
   cd "C:\Users\esox-\Documents\Cursor Project\MPInformer"
   ```

2. **Создайте виртуальное окружение (рекомендуется):**
   ```bash
   python -m venv venv
   ```

3. **Активируйте виртуальное окружение:**
   
   **Windows (PowerShell):**
   ```powershell
   .\venv\Scripts\Activate.ps1
   ```
   
   **Windows (CMD):**
   ```cmd
   venv\Scripts\activate.bat
   ```

4. **Установите зависимости:**
   ```bash
   pip install -r requirements.txt
   ```

5. **Создайте файл `.env` на основе `.env.example`:**
   ```bash
   copy .env.example .env
   ```
   
   Заполните необходимые переменные окружения:
   - Токены API для Ozon и Wildberries
   - Токен Telegram бота
   - ID чата для отправки отчетов

6. **Инициализируйте базу данных:**
   ```bash
   python -m app.db.init_db
   ```

7. **Запустите приложение:**
   ```bash
   python main.py
   ```
   
   Или через uvicorn напрямую:
   ```bash
   uvicorn app.main:app --reload
   ```

## Структура проекта

```
MPInformer/
├── app/
│   ├── __init__.py
│   ├── main.py                 # Точка входа приложения
│   ├── config.py               # Конфигурация приложения
│   ├── db/
│   │   ├── __init__.py
│   │   ├── database.py         # Подключение к БД
│   │   └── models.py           # Модели данных
│   ├── modules/
│   │   ├── __init__.py
│   │   ├── notifications/      # Модуль оповещений
│   │   │   ├── __init__.py
│   │   │   ├── scheduler.py    # Планировщик задач
│   │   │   └── reporter.py     # Формирование отчетов
│   │   ├── ozon/
│   │   │   ├── __init__.py
│   │   │   ├── api_client.py   # Клиент для работы с Ozon API
│   │   │   └── models.py       # Модели данных Ozon
│   │   └── wildberries/
│   │       ├── __init__.py
│   │       ├── api_client.py   # Клиент для работы с Wildberries API
│   │       └── models.py       # Модели данных Wildberries
│   └── telegram/
│       ├── __init__.py
│       ├── bot.py              # Telegram бот
│       └── handlers.py         # Обработчики команд бота
├── .env                         # Переменные окружения (не коммитится)
├── .env.example                 # Пример файла с переменными окружения
├── .gitignore
├── requirements.txt
├── README.md
└── main.py                      # Альтернативная точка входа
```

## Конфигурация

Все настройки хранятся в файле `.env`. Пример конфигурации в `.env.example`.

### Необходимые переменные:

- `OZON_CLIENT_ID` - Client ID для Ozon API
- `OZON_API_KEY` - API ключ для Ozon
- `WB_API_KEY` - API ключ для Wildberries
- `TELEGRAM_BOT_TOKEN` - Токен Telegram бота
- `TELEGRAM_CHAT_ID` - ID чата для отправки отчетов
- `SCHEDULER_INTERVAL_MINUTES` - Интервал проверки заказов (по умолчанию 60)

## Использование

После запуска приложение будет:
1. Периодически запрашивать данные о заказах с Ozon и Wildberries
2. Формировать аналитические отчеты
3. Отправлять отчеты в указанный Telegram чат

### Telegram бот

Бот поддерживает следующие команды:
- `/start` - Приветствие и информация о боте
- `/status` - Текущий статус системы
- `/report` - Получить отчет вручную

## Разработка

### Добавление нового маркетплейса

1. Создайте модуль в `app/modules/<marketplace_name>/`
2. Реализуйте `api_client.py` с методами для работы с API
3. Добавьте модели данных в `models.py`
4. Интегрируйте в модуль оповещений

## Миграция на VPS

При переносе на VPS рекомендуется:
1. Использовать PostgreSQL вместо SQLite
2. Настроить systemd сервис для автозапуска
3. Использовать nginx как reverse proxy
4. Настроить SSL сертификаты
5. Использовать supervisor или systemd для управления процессами

## Лицензия

[Укажите лицензию]
