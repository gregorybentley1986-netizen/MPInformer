# Развёртывание MPInformer на VPS

Краткая инструкция по переносу приложения на виртуальный сервер (Linux).
Тест автодеплоя: служебная строка для проверки bat-сценария.

## 1. Требования к VPS

- **ОС:** Ubuntu 22.04 / Debian 12 (или другой Linux с Python 3.10+).
- **Память:** минимум 512 MB RAM.
- **Сеть:** открытый порт для приложения (по умолчанию 8001) или для nginx (80/443).

## 2. Подготовка сервера

Подключитесь по SSH и установите Python и зависимости:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git
```

## 3. Размещение проекта

**Вариант А: клонирование из Git**

```bash
cd /opt   # или домашний каталог
sudo git clone <URL_вашего_репозитория> MPInformer
cd MPInformer
```

**Вариант Б: копирование с компьютера**

- Архивируйте папку проекта (без `venv` и без `.env`).
- Перенесите архив на VPS (scp, rsync, SFTP).
- Распакуйте, например в `/opt/MPInformer`.

```bash
# На VPS после копирования
cd /opt/MPInformer
```

## 4. Виртуальное окружение и зависимости

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 5. Конфигурация (.env)

Создайте файл `.env` в корне проекта (рядом с `main.py`):

```bash
cp .env.example .env
nano .env   # или vim
```

Заполните переменные (без кавычек, если значение не содержит пробелов):

- `OZON_CLIENT_ID`, `OZON_API_KEY` — ключи Ozon Seller API.
- `WB_API_KEY` — ключ WB (категория «Статистика»).
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — бот и чат для отчётов.
- При необходимости: `REPORT_NOTIFICATION_TIMES`, `SCHEDULER_INTERVAL_MINUTES`, `SERVER_PORT`, `LOG_LEVEL`.

Сохраните файл. Проверьте права (чтобы только владелец читал):

```bash
chmod 600 .env
```

## 6. Проверка запуска

Запустите приложение вручную:

```bash
source venv/bin/activate
python main.py
```

В логах не должно быть ошибок. Проверьте в браузере: `http://IP_СЕРВЕРА:8001` (или другой порт из `.env`). Остановите процесс: `Ctrl+C`.

## 7. Запуск как сервис (systemd)

Чтобы приложение работало после перезагрузки и автоматически перезапускалось при сбоях:

```bash
sudo nano /etc/systemd/system/mpinformer.service
```

Вставьте (путь `/opt/MPInformer` замените на свой):

```ini
[Unit]
Description=MPInformer - отчёты Ozon/WB в Telegram
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/MPInformer
Environment="PATH=/opt/MPInformer/venv/bin"
ExecStart=/opt/MPInformer/venv/bin/python /opt/MPInformer/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Если проект лежит в домашней папке пользователя `ubuntu`, укажите `User=ubuntu` и путь вида `/home/ubuntu/MPInformer`.

Включите и запустите сервис:

```bash
sudo systemctl daemon-reload
sudo systemctl enable mpinformer
sudo systemctl start mpinformer
sudo systemctl status mpinformer
```

Просмотр логов:

```bash
sudo journalctl -u mpinformer -f
```

## 8. Фаервол (опционально)

Если используете ufw:

```bash
sudo ufw allow 8001/tcp
sudo ufw allow 22/tcp
sudo ufw enable
```

## 9. Nginx как обратный прокси (опционально)

Чтобы открывать приложение по 80 порту или по домену с HTTPS:

```bash
sudo apt install nginx certbot python3-certbot-nginx
sudo nano /etc/nginx/sites-available/MPInformer
```

Пример конфига (замените `your-domain.com` и порт при необходимости):

```nginx
server {
    listen 80;
    server_name your-domain.com;
    location / {
        proxy_pass http://127.0.0.1:8001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/MPInformer /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
# HTTPS: sudo certbot --nginx -d your-domain.com
```

## 10. Важно

- Файл **`.env`** не должен попадать в Git (он уже в `.gitignore`). На VPS создаётся вручную.
- **База данных** по умолчанию — SQLite (`mpinformer.db` в каталоге проекта). При смене каталога или пользователя путь к БД задаётся через `DATABASE_URL` в `.env`.
- Код в `main.py` содержит логику перезапуска под Windows (ожидание освобождения порта). На Linux она не мешает: проверки по `win32` просто не выполняются.

После выполнения шагов 1–7 приложение будет работать на VPS и отправлять отчёты по расписанию в Telegram.

## 11. Git: выгрузка с ПК и приём обновлений на VPS

Схема: **ваш ПК → `git push` → GitHub → VPS `git fetch` / `git pull`**. На VPS обычно достаточно **только чтения** из GitHub (отдельный deploy key). Пушить код с VPS в GitHub не обязательно.

### 11.0 Уже развёрнуто на VPS: три связи и ключи

Ниже — только то, что нужно «прописать», если код на сервере уже стоит и работает. Замените `USER/REPO`, IP и пути на свои (каталог проекта на VPS должен совпадать с `WorkingDirectory` в `mpinformer.service`, часто `/opt/MPInformer`).

| Связь | Зачем | Что сделать |
|--------|--------|-------------|
| **1. ПК → GitHub** | Пушить код с компьютера | SSH-ключ **личного** аккаунта GitHub **или** HTTPS + токен. |
| **2. VPS → GitHub** | На сервере выполнять `git fetch` / `git pull` | **Deploy key** в настройках **этого** репозитория (рекомендуется) **или** HTTPS + PAT на VPS. |
| **3. ПК → VPS** | Запускать деплой по SSH с Windows (`push-and-deploy.bat`) | Публичный ключ ПК в `authorized_keys` пользователя на VPS (часто `root`). |

**1. ПК → GitHub (SSH)**

На **Windows** (PowerShell):

```powershell
# если ключа ещё нет:
 
Get-Content $env:USERPROFILE\.ssh\id_ed25519_github.pub
```

Скопируйте строку `.pub` в GitHub: **Settings → SSH and GPG keys → New SSH key**.

В папке проекта на ПК проверьте remote (подставьте свой репозиторий):

```bash
git remote -v
# при необходимости:
# git remote set-url origin git@github.com:USER/REPO.git
```

Проверка с ПК:

```bash
ssh -T git@github.com
```

При нескольких ключах добавьте в `C:\Users\<вы>\.ssh\config`:

```text
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_github
  IdentitiesOnly yes
```

**2. VPS → GitHub (deploy key)**

На **VPS** (под тем пользователем, от которого будет `git fetch` — часто `root`):

```bash
ssh-keygen -t ed25519 -f ~/.ssh/mpinformer_github_read -N ""
cat ~/.ssh/mpinformer_github_read.pub
```

В GitHub: **репозиторий → Settings → Deploy keys → Add deploy key** — вставьте **публичный** ключ, галочку **Allow write access** не включайте (достаточно чтения для `pull`/`fetch`).

На VPS в `~/.ssh/config` (для того же пользователя):

```text
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/mpinformer_github_read
  IdentitiesOnly yes
```

В каталоге приложения:

```bash
cd /opt/MPInformer   # ваш путь
git remote set-url origin git@github.com:USER/REPO.git
ssh -T git@github.com
git fetch origin
git branch -vv
```

Если `git fetch` без ошибок — связь VPS ↔ GitHub готова.

**3. ПК → VPS (SSH для деплоя)**

На **ПК** возьмите или создайте ключ, которым вы уже заходите на VPS, **или** отдельный только для деплоя:

```powershell
ssh-keygen -t ed25519 -C "vps-deploy" -f $env:USERPROFILE\.ssh\id_ed25519_vps
Get-Content $env:USERPROFILE\.ssh\id_ed25519_vps.pub
```

На **VPS** добавьте эту `.pub` строку в `~/.ssh/authorized_keys` того пользователя, под которым заходите по SSH (например `/root/.ssh/authorized_keys`), права: каталог `~/.ssh` **700**, файл **600**.

Проверка с ПК:

```bash
ssh -i ~/.ssh/id_ed25519_vps root@ВАШ_IP
# или если в config задан Host:
ssh my-vps
```

Для [scripts/push-and-deploy.bat](./scripts/push-and-deploy.bat):

```bat
set MPINFORMER_SSH_TARGET=root@ВАШ_IP
```

Если используете нестандартный ключ, в `C:\Users\<вы>\.ssh\config`:

```text
Host my-vps
  HostName ВАШ_IP
  User root
  IdentityFile ~/.ssh/id_ed25519_vps
```

и тогда `set MPINFORMER_SSH_TARGET=my-vps`.

**Краткая проверка цепочки**

1. С ПК: `git push origin main` — коммит появляется на GitHub.  
2. На VPS: `cd /opt/MPInformer && git fetch origin && git log -1 origin/main` — тот же коммит.  
3. С ПК: `ssh root@ВАШ_IP` (или ваш `Host`) — вход без пароля.

### 11.1 На компьютере разработчика

1. Клонируйте репозиторий (один раз):

   ```bash
   git clone git@github.com:USER/MPInformer.git
   cd MPInformer
   ```

   Либо по HTTPS: `git clone https://github.com/USER/MPInformer.git` (потом Git Credential Manager или PAT).

2. Проверьте ветку по умолчанию (в проекте ожидается **`main`**):

   ```bash
   git branch --show-current
   git remote -v
   ```

3. Выгрузка изменений на GitHub:

   ```bash
   git add -A
   git commit -m "описание изменений"
   git push origin main
   ```

4. **SSH к GitHub с ПК:** убедитесь, что ключ добавлен в GitHub (Settings → SSH keys) и при необходимости задан `~/.ssh/config`:

   ```text
   Host github.com
     HostName github.com
     User git
     IdentityFile ~/.ssh/id_ed25519_github
   ```

### 11.2 Репозиторий на GitHub

- Репозиторий может быть **приватным** — тогда на VPS нужна аутентификация к GitHub (deploy key или HTTPS + токен).
- Ветка **`main`** должна существовать на GitHub (`git push -u origin main` с ПК при первом пуше).

### 11.3 На VPS: клон и обновление кода

**Первый раз — клон в каталог, совпадающий с systemd** (часто `/opt/MPInformer`):

```bash
sudo mkdir -p /opt
# если каталог уже есть и не нужен — удалите вручную только осознанно:
sudo git clone git@github.com:USER/MPInformer.git /opt/MPInformer
cd /opt/MPInformer
git checkout main
# если дальше правите код не от root:
# sudo chown -R "$USER:$USER" /opt/MPInformer
```

Если репозиторий **уже скопирован без `.git`**, проще один раз заново `git clone` в нужный путь (или `git init` + `remote add` + fetch — дольше).

**Ручное обновление кода на сервере:**

```bash
cd /opt/MPInformer
git fetch origin
git checkout main
git reset --hard origin/main
```

После обновления кода: при необходимости `source venv/bin/activate && pip install -r requirements.txt`, затем `sudo systemctl restart mpinformer`.

**Первичный клон одной командой (на сервере можно скопировать только этот файл):** [scripts/vps-git-setup.sh](./scripts/vps-git-setup.sh) — см. комментарий в начале файла.

**Автоматизация с вашего ПК (Windows):** скрипт [scripts/push-and-deploy.bat](./scripts/push-and-deploy.bat) делает `git push` с ПК и по SSH выполняет на VPS `git fetch` / синхронизацию с `origin/main` и перезапуск сервиса. Задайте `MPINFORMER_SSH_TARGET=root@IP_ВАШЕГО_VPS` (или передайте хост вторым аргументом).

### 11.4 Deploy key на VPS (только `git pull`, без доступа к остальным репозиториям)

На **VPS**:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/mpinformer_deploy -N ""
cat ~/.ssh/mpinformer_deploy.pub
```

В GitHub: **репозиторий → Settings → Deploy keys → Add deploy key** — вставьте **публичный** ключ, включите **Allow write access** только если серверу действительно нужен push (обычно **выкл**).

Настройте `origin` на SSH-URL и ключ:

```bash
cd /opt/MPInformer
git remote set-url origin git@github.com:USER/MPInformer.git
```

Создайте `~/.ssh/config` на VPS:

```text
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/mpinformer_deploy
  IdentitiesOnly yes
```

Проверка:

```bash
ssh -T git@github.com
git fetch origin
```

### 11.5 HTTPS на VPS (альтернатива)

```bash
cd /opt/MPInformer
git remote set-url origin https://github.com/USER/MPInformer.git
```

Для приватного репозитория GitHub больше не принимает пароль аккаунта; нужен **Personal Access Token** (classic: scope `repo`) как пароль при запросе, либо кэш учётных данных (`git config --global credential.helper store` — хранит в открытом виде, используйте только если осознанно).

### 11.6 Полезные проверки

```bash
cd /opt/MPInformer && git status && git remote -v && git log -1 --oneline
```

Убедитесь, что `WorkingDirectory` в unit-файле `mpinformer` совпадает с этим путём.

## 12. Рабочий цикл: обновление кода с ПК на VPS

После настройки ключей (раздел 11.0) обычный выпуск выглядит так.

### 12.1 На ПК (разработка)

```powershell
cd "C:\Users\esox-\Documents\Cursor Project\MPInformer"
git status
git add -A
git commit -m "кратко: что изменили"
git push origin main
```

Убедитесь, что в коммит не попали секреты (`.env` в `.gitignore`).

### 12.2 На VPS (вручную)

Подставьте свой путь, если не `/opt/MPInformer`. Если для GitHub на сервере не настроен `~/.ssh/config`, используйте переменную `GIT_SSH_COMMAND` (путь к deploy key).

```bash
export GIT_SSH_COMMAND='ssh -i /root/.ssh/mpinformer_github_read -o IdentitiesOnly=yes'
cd /opt/MPInformer
git fetch origin
git checkout main
git pull --ff-only origin main
unset GIT_SSH_COMMAND
```

Зависимости (если менялся `requirements.txt`):

```bash
cd /opt/MPInformer
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

Перезапуск приложения:

```bash
sudo systemctl restart mpinformer
sudo systemctl status mpinformer --no-pager -l
```

Проверка с сервера (порт возьмите из unit-файла `mpinformer` или из `.env`; в логах выше был пример **8000**):

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/health
```

В **nginx** в `proxy_pass` должен быть тот же порт, на котором слушает uvicorn.

### 12.3 С ПК одной командой (Windows)

Скрипт [scripts/push-and-deploy.bat](./scripts/push-and-deploy.bat): пуш на GitHub + по SSH синхронизация репозитория на VPS + перезапуск `mpinformer` + ожидание `/health`.

Перед первым запуском задайте хост (пример):

```bat
set MPINFORMER_SSH_TARGET=root@ВАШ_IP
```

Запуск из корня репозитория на ПК:

```bat
scripts\push-and-deploy.bat
```

При другом пути на сервере: `set DEPLOY_REMOTE_PATH=...`, при другом сервисе/health: `DEPLOY_SERVICE`, `DEPLOY_HEALTH_URL` (см. комментарии в начале `.bat`).

### 12.4 После первого `git clone` на сервере

- Файл **`.env`** на VPS не приезжает из Git — держите копию на сервере и при необходимости восстанавливайте после переустановки каталога.
- Каталог **`uploads`**: в актуальных версиях приложение создаёт его при старте; если сервис падает с `Directory '.../uploads' does not exist`, выполните один раз: `mkdir -p /opt/MPInformer/uploads` и перезапустите сервис (или обновите код с ПК и снова `git pull`).

## Устранение неполадок

**Ошибка в journalctl:** `Error loading ASGI app. Attribute "app" not found in module "app.main"`

Обычно это значит, что при импорте `app.main` до создания объекта `app` происходит исключение (или на сервере старая версия файла). Чтобы увидеть **реальную** ошибку, на сервере выполните:

```bash
cd /opt/MPInformer
/opt/MPInformer/venv/bin/python -c "from app.main import app; print('OK')"
```

Если команда упадёт — в выводе будет трейсбек (ImportError, Missing dependency, ошибка в коде и т.д.). Убедитесь также, что в `/opt/MPInformer/app/main.py` есть строка `app = FastAPI(...)` и что на сервер залита актуальная версия проекта (git pull или копирование файлов).

**Если команда выводит OK, а сервис всё равно падает с "app not found":** в unit-файле укажите полный путь к `main.py` в `ExecStart`:
`ExecStart=/opt/MPInformer/venv/bin/python /opt/MPInformer/main.py`
и проверьте, что задано `WorkingDirectory=/opt/MPInformer`. Затем выполните `sudo systemctl daemon-reload` и `sudo systemctl restart mpinformer`.

**`RuntimeError: Directory '.../uploads' does not exist`:** на сервере `mkdir -p /opt/MPInformer/uploads` (или ваш корень проекта) и `sudo systemctl restart mpinformer`; в новых версиях кода каталог создаётся при старте (см. раздел 12.4).

**Пустой репозиторий на GitHub после `git clone`:** с ПК нужен хотя бы один `git commit` и `git push origin main`, иначе на VPS не будет ветки `origin/main` и файлов.

**`ssh-keygen ... option requires an argument -- N`:** для ключа без пароля указывайте `-N ""` (пустая строка в кавычках).
