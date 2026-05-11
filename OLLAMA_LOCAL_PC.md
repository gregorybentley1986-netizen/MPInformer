# Ollama на локальном компьютере (ИИ-агент) + сервер (приложение MPInformer)

Схема: **локальный ПК** запускает Ollama (ИИ), **сервер** — приложение MPInformer. Сервер обращается к ИИ по сети (через проброс порта или туннель).

Так можно использовать свою модель на своём железе без установки Ollama на VPS.

---

## 1. Установка Ollama на локальный компьютер

1. Скачайте и установите Ollama: [ollama.com](https://ollama.com).
2. Запустите Ollama (в системе обычно появляется иконка в трее).
3. Скачайте модель (в терминале или через интерфейс):

   ```bash
   ollama run llama3.2
   ```

   При первом запуске модель скачается. Для плана печати достаточно одной модели (например `llama3.2`, `phi3`, `mistral`).

4. Проверка API локально:

   ```bash
   curl http://127.0.0.1:11434/v1/chat/completions ^
     -H "Content-Type: application/json" ^
     -d "{\"model\":\"llama3.2\",\"messages\":[{\"role\":\"user\",\"content\":\"Hi\"}],\"stream\":false}"
   ```

   (На Linux/macOS замените `^` на `\` и кавычки при необходимости.)

---

## 2. Доступ к Ollama с сервера (проброс порта)

Сервер (VPS с MPInformer) должен иметь возможность отправлять HTTP-запросы на ваш ПК. Варианты:

### Вариант A: ngrok

**Внимание:** при работе из РФ ngrok может блокироваться или возвращать 403. В таком случае используйте **Вариант A2: Cloudflare Tunnel** ниже.

1. Зарегистрируйтесь на [ngrok.com](https://ngrok.com), установите ngrok.
2. На **локальном ПК** (где запущена Ollama) в терминале:

   ```bash
   ngrok http 11434
   ```

3. В выводе появится URL вида `https://xxxx-xx-xx-xx-xx.ngrok-free.app`. Это и есть адрес вашего ИИ для сервера.
4. В **.env на сервере** (каталог с MPInformer, например `/opt/MPInformer/`):

   ```env
   LLM_DISTRIBUTION_URL=https://xxxx-xx-xx-xx-xx.ngrok-free.app/v1/chat/completions
   LLM_MODEL=llama3.2
   ```

   Ключ `LLM_API_KEY` для Ollama не нужен (оставьте пустым или не указывайте).

5. Пока ngrok запущен на ПК, сервер сможет обращаться к Ollama. После перезапуска ПК или ngrok URL может измениться (на бесплатном плане) — тогда обновите `LLM_DISTRIBUTION_URL` в .env и перезапустите приложение.

**Если приложение выдаёт «Ошибка ИИ 403»:** возможны две причины: (1) ngrok блокирует запрос — в коде уже добавлены заголовки обхода; (2) Ollama отклоняет запрос из‑за CORS (заголовок `Origin`). На **локальном ПК** нужно задать `OLLAMA_ORIGINS=*` и запускать Ollama так, чтобы она точно получила эту переменную.

- **Windows (Ollama как приложение):** переменная из «Переменные среды» не всегда подхватывается приложением до перезагрузки ПК. Надёжный способ — запускать Ollama из скрипта с переменной:
  1. Полностью закрыть Ollama (правый клик по иконке в трее → Quit).
  2. Создать на рабочем столе файл `start_ollama.bat` с содержимым:
     ```bat
     @echo off
     set OLLAMA_ORIGINS=*
     start "" "%LOCALAPPDATA%\Programs\Ollama\ollama app.exe"
     ```
     (Если Ollama установлена в другую папку, укажите полный путь к `ollama app.exe` — например `C:\Users\ИМЯ\AppData\Local\Programs\Ollama\ollama app.exe`.)
  3. Дальше запускать Ollama двойным кликом по `start_ollama.bat`, а не по ярлыку Ollama.
- **Windows (через PowerShell):** закрыть Ollama, затем в PowerShell: `$env:OLLAMA_ORIGINS="*"; & "$env:LOCALAPPDATA\Programs\Ollama\ollama app.exe"`.
- **macOS/Linux:** перед запуском `ollama serve` выполнить `export OLLAMA_ORIGINS="*"` или в systemd-сервисе добавить `Environment="OLLAMA_ORIGINS=*"` и перезапустить сервис.

### Вариант A2: Cloudflare Tunnel (рекомендуется при блокировке ngrok, в т.ч. в РФ)

Cloudflare Tunnel (cloudflared) обычно доступен из РФ и не возвращает 403 для серверных запросов. Бесплатно.

1. **На локальном ПК** скачайте cloudflared:
   - Windows: [developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation](https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation) — скачать exe или через PowerShell: `winget install cloudflare.cloudflared` (или вручную с GitHub releases).
   - Или без регистрации — быстрый туннель: скачайте `cloudflared` с [github.com/cloudflare/cloudflared/releases](https://github.com/cloudflare/cloudflared/releases), распакуйте.

2. **Запуск туннеля без аккаунта Cloudflare** (временный URL на сессию):
   ```bash
   cloudflared tunnel --url http://127.0.0.1:11434
   ```
   В выводе появится строка вида `https://xxxx-xxxx-xx.xx.trycloudflare.com` — это и есть URL для `.env`.

   **Если в логе ошибки «timeout», «QUIC», «no recent network activity»:** в некоторых сетях (в т.ч. в РФ) QUIC блокируется. Запустите туннель с протоколом HTTP/2:
   ```bash
   cloudflared tunnel --protocol http2 --url http://127.0.0.1:11434
   ```
   Окно с cloudflared должно оставаться открытым; при закрытии туннель перестанет работать.

3. **В .env на сервере** (каталог с MPInformer):
   ```env
   LLM_DISTRIBUTION_URL=https://ВАШ_TRYCLOUDFLARE_URL/v1/chat/completions
   LLM_MODEL=llama3.2
   ```
   Подставьте URL из шага 2 (без слэша в конце, путь `/v1/chat/completions` допишется в коде). После перезапуска cloudflared URL изменится — обновите `.env` и перезапустите приложение.

4. **С аккаунтом Cloudflare** можно получить постоянный домен: зарегистрируйтесь на [dash.cloudflare.com](https://dash.cloudflare.com), установите cloudflared, выполните `cloudflared tunnel login`, затем создайте туннель и привяжите его к `http://localhost:11434`. В документации Cloudflare: «Connect applications with Cloudflare Tunnel».

5. Заголовки обхода (как у ngrok) для Cloudflare не нужны — приложение отправляет обычный запрос.

### Вариант A3: Tailscale (VPN между сервером и ПК, без облачного туннеля)

**Tailscale** — бесплатная VPN: сервер и ПК оказываются в одной приватной сети и видят друг друга по внутренним IP (100.x.x.x). Не нужен домен, не нужен ngrok/Cloudflare; трафик идёт напрямую между машинами через зашифрованный канал. Часто доступен из РФ.

1. **Регистрация:** зайдите на [tailscale.com](https://tailscale.com), создайте аккаунт (можно через Google/GitHub).

2. **На локальном ПК (Windows):**
   - Скачайте установщик: [tailscale.com/download/windows](https://tailscale.com/download/windows).
   - Установите, запустите Tailscale и войдите в тот же аккаунт.
   - Узнайте Tailscale-IP ПК: в трее Tailscale → правый клик → «Copy Tailscale IP» или в PowerShell: `tailscale ip -4`. Адрес будет вида `100.64.x.x` или `100.x.x.x`.

3. **На сервере (Linux, где крутится MPInformer):**
   ```bash
   curl -fsSL https://tailscale.com/install.sh | sh
   sudo tailscale up
   ```
   Появится ссылка для входа — откройте в браузере и привяжите сервер к тому же аккаунту Tailscale. После этого на сервере выполните `tailscale ip -4` у другого узла или в админке Tailscale посмотрите IP обоих машин.

4. **Ollama на ПК** должна слушать все интерфейсы (в т.ч. Tailscale). В `start_ollama.bat` добавьте перед запуском:
   ```bat
   set OLLAMA_HOST=0.0.0.0
   set OLLAMA_ORIGINS=*
   ```
   Перезапустите Ollama.

5. **В .env на сервере** укажите Tailscale-IP вашего ПК (тот, что 100.x.x.x):
   ```env
   LLM_DISTRIBUTION_URL=http://100.64.Х.Х:11434/v1/chat/completions
   LLM_MODEL=llama3.2
   ```
   Подставьте реальный IP ПК из шага 2. Перезапустите приложение: `sudo systemctl restart mpinformer`.

6. **Проверка с сервера:** `curl http://100.64.Х.Х:11434/api/tags` — должен вернуть список моделей Ollama.

Плюсы: не зависит от Cloudflare/ngrok, один раз настроил — IP в tailnet стабильный (можно задать имя машины в админке Tailscale). Минус: на обеих машинах должен быть запущен Tailscale.

### Вариант A4: Tuna (tuna.am)

**[Tuna](https://tuna.am)** — российская платформа туннелей: доступ из интернета к локальным приложениям без белого IP и проброса портов. Подходит для схемы «ПК с Ollama — сервер MPInformer», работает из РФ, оплата в рублях.

1. **Регистрация:** [tuna.am](https://tuna.am) → «Войти» / «Попробовать бесплатно». Создайте аккаунт.

2. **Установка CLI на ПК (Windows):**
   - Через winget: `winget install --id yuccastream.tuna`
   - Или скачайте установщик: [releases.tuna.am](https://releases.tuna.am/tuna/latest/tuna_x64.msi)  
   Документация: [tuna.am/docs/guides/install/install-cli](https://tuna.am/docs/guides/install/install-cli/).

3. **Вход в аккаунт** (если потребуется): выполните в консоли команду входа по инструкции в личном кабинете Tuna или в документации.

4. **Запуск туннеля к Ollama** на ПК (Ollama должна быть уже запущена на порту 11434):
   ```bash
   tuna http localhost:11434
   ```
   В ответ появится публичная ссылка (например `https://xxxx.tuna.am` или динамический поддомен). Окно с запущенной командой не закрывайте — туннель активен пока работает процесс.

5. **В .env на сервере** укажите полученный URL (с путём до API чата):
   ```env
   LLM_DISTRIBUTION_URL=https://ВАШ_АДРЕС_ТУННЕЛЯ_TUNA/v1/chat/completions
   LLM_MODEL=llama3.2
   ```
   Подставьте адрес из вывода команды `tuna http localhost:11434`. Перезапустите приложение: `sudo systemctl restart mpinformer`.

6. **Ollama на ПК:** запускайте с `OLLAMA_ORIGINS=*` (например через `start_ollama.bat`), чтобы запросы с Tuna не получали 403 из‑за CORS.

**Тарифы:** бесплатный тариф «Новичок» — 1 HTTP‑туннель, до 30 минут работы за сессию (подходит для проверки). Для постоянной работы без ограничения по времени — тариф «Профи» (поддомены в зоне tuna.am, свой домен, без лимита по времени). Подробности: [tuna.am](https://tuna.am) → Тарифы.

**Альтернатива без консоли:** приложение [Tuna Desktop](https://tuna.am) для Windows/macOS/Linux — управление туннелями через графический интерфейс.

### Вариант B: Прямой доступ (белый IP или VPN без Tailscale)

Если у вашего ПК есть белый IP или вы поднимаете VPN между сервером и ПК:

- На ПК Ollama должна слушать все интерфейсы. Запустите Ollama с переменной окружения:
  - Windows (PowerShell): `$env:OLLAMA_HOST="0.0.0.0"; ollama serve`
  - Linux/macOS: `OLLAMA_HOST=0.0.0.0 ollama serve`
- В .env на сервере укажите:
  - `LLM_DISTRIBUTION_URL=http://IP_ВАШЕГО_ПК:11434/v1/chat/completions`
  - `LLM_MODEL=llama3.2`

### Вариант C: Один сервер (Ollama и MPInformer на одной машине)

Если и Ollama, и MPInformer стоят на одном сервере, см. [OLLAMA_SERVER.md](OLLAMA_SERVER.md): установите Ollama на сервер и укажите в .env `LLM_DISTRIBUTION_URL=http://127.0.0.1:11434/v1/chat/completions`.

---

## 3. Настройка .env на сервере (итог)

Минимально для схемы «ИИ на ПК — приложение на сервере»:

```env
# ИИ: URL до chat completions (Ollama на ПК через Tuna, Cloudflare, Tailscale, ngrok или другой туннель)
LLM_DISTRIBUTION_URL=https://ВАШ_АДРЕС_ТУННЕЛЯ/v1/chat/completions
LLM_MODEL=llama3.2
# Ключ для Ollama не нужен
# LLM_API_KEY=
```

Перезапуск приложения после изменения .env:

```bash
sudo systemctl restart mpinformer
```

---

## 4. Что делает приложение с ИИ

ИИ используется **при переносе плана в задачи** («Перенести план в задачи» на странице «План печати»): приложение запрашивает у модели распределение заданий по принтерам с учётом равномерной загрузки и группировки по материалу; по этой подсказке задачи расставляются на таймлайне (принтер и время). План (список изделий и количество) пользователь формирует вручную; ИИ только распределяет уже полученные задачи по принтерам и времени.

Оба сценария используют один и тот же endpoint (OpenAI-совместимый chat completions), поэтому подойдут и Ollama на ПК, и облачные API (OpenAI, Groq и т.д.) — см. [EXTERNAL_AI.md](EXTERNAL_AI.md).

---

## 5. Проверка

1. На ПК: Ollama запущена, ngrok (или туннель) указывает на порт 11434.
2. На сервере: в .env заданы `LLM_DISTRIBUTION_URL` и `LLM_MODEL`, приложение перезапущено.
3. В браузере откройте «План печати», заполните план (изделия и количество) и нажмите **«Перенести план в задачи»**. Если ИИ настроен, под полем появится подсказка «При переносе ИИ распределит задачи по принтерам и таймлайну»; после переноса задания окажутся расставлены по принтерам и времени с учётом подсказки модели.

Если кнопка неактивна или появляется ошибка — проверьте, что `LLM_DISTRIBUTION_URL` доступен с сервера (например, `curl -X POST "URL" -H "Content-Type: application/json" -d '{"model":"llama3.2","messages":[{"role":"user","content":"Hi"}]}'`).
