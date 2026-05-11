# Инструкция по установке MPInformer

## Шаг 1: Проверка Python

Убедитесь, что у вас установлен Python 3.10 или выше:

```bash
python --version
```

Если Python не установлен, скачайте его с [python.org](https://www.python.org/downloads/)

## Шаг 2: Создание виртуального окружения

Перейдите в директорию проекта и создайте виртуальное окружение:

```powershell
cd "C:\Users\esox-\Documents\Cursor Project\MPInformer"
python -m venv venv
```

## Шаг 3: Активация виртуального окружения

**Windows PowerShell:**
```powershell
.\venv\Scripts\Activate.ps1
```

Если возникает ошибка выполнения скриптов, выполните:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Windows CMD:**
```cmd
venv\Scripts\activate.bat
```

## Шаг 4: Установка зависимостей

```bash
pip install -r requirements.txt
```

## Шаг 5: Настройка переменных окружения

1. Скопируйте файл `.env.example` в `.env`:
   ```bash
   copy .env.example .env
   ```

2. Откройте файл `.env` и заполните необходимые значения:

### Получение токенов API

#### Ozon API:
1. Зайдите в личный кабинет продавца Ozon
2. Перейдите в раздел "Настройки" → "API"
3. Создайте новое приложение или используйте существующее
4. Скопируйте `Client-Id` и `API-Key`

#### Wildberries API:
1. Зайдите в личный кабинет продавца Wildberries
2. Перейдите в раздел "Настройки" → "Доступ к API"
3. Создайте новый токен или используйте существующий
4. Скопируйте токен

#### Telegram Bot:
1. Найдите [@BotFather](https://t.me/botfather) в Telegram
2. Отправьте команду `/newbot` и следуйте инструкциям
3. Скопируйте токен бота
4. Чтобы узнать ID чата:
   - Добавьте бота [@userinfobot](https://t.me/userinfobot) в ваш чат
   - Отправьте команду `/start`
   - Скопируйте `Id` (это и есть chat_id)

### Пример заполненного .env файла:

```env
OZON_CLIENT_ID=123456
OZON_API_KEY=your_ozon_api_key_here
WB_API_KEY=your_wildberries_api_key_here
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=123456789
SCHEDULER_INTERVAL_MINUTES=60
LOG_LEVEL=INFO
```

## Шаг 6: Инициализация базы данных

```bash
python -m app.db.init_db
```

## Шаг 7: Запуск приложения

```bash
python main.py
```

Или через uvicorn напрямую:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Приложение будет доступно по адресу: http://localhost:8000

## Проверка работы

1. Откройте браузер и перейдите на http://localhost:8000
2. Должна отобразиться страница с информацией об API
3. Проверьте логи в консоли - должны быть сообщения о запуске планировщика
4. Отправьте команду `/start` вашему Telegram боту

## Возможные проблемы

### Ошибка "ModuleNotFoundError"
Убедитесь, что виртуальное окружение активировано и все зависимости установлены:
```bash
pip install -r requirements.txt
```

### Ошибка подключения к API маркетплейсов
- Проверьте правильность токенов в файле `.env`
- Убедитесь, что токены не истекли
- Проверьте интернет-соединение

### Ошибка отправки сообщений в Telegram
- Проверьте правильность токена бота
- Убедитесь, что бот добавлен в чат
- Проверьте правильность `TELEGRAM_CHAT_ID`

## Остановка приложения

Нажмите `Ctrl+C` в терминале, где запущено приложение.
