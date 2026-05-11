# Развёртывание Ollama на сервере

Ollama даёт бесплатный локальный ИИ с OpenAI-совместимым API. Ниже — установка на Linux (Ubuntu/Debian или аналог).

## 1. Установка Ollama

На сервере по SSH:

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

Проверка:

```bash
ollama --version
```

## 2. Запуск как сервиса (systemd)

Ollama после установки обычно уже регистрируется как сервис. Проверить и запустить:

```bash
sudo systemctl status ollama
sudo systemctl enable ollama   # автозапуск при перезагрузке
sudo systemctl start ollama
```

Если сервиса нет, создайте его вручную:

```bash
sudo tee /etc/systemd/system/ollama.service << 'EOF'
[Unit]
Description=Ollama
After=network-online.target

[Service]
ExecStart=/usr/local/bin/ollama serve
Restart=always
RestartSec=3
Environment="OLLAMA_HOST=0.0.0.0"
User=root

[Install]
WantedBy=default.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ollama
sudo systemctl start ollama
```

Важно: `OLLAMA_HOST=0.0.0.0` — чтобы API слушал все интерфейсы (доступ с других машин). Для только localhost оставьте по умолчанию (без этой строки).

## 3. Скачать модель

На сервере:

```bash
ollama run llama3.2
```

При первом запуске модель скачается. После можно выйти (Ctrl+D или команда `/bye`), Ollama продолжит работать в фоне. Для API достаточно одной загруженной модели:

```bash
ollama list
```

Рекомендуемые модели по размеру: `llama3.2` (3B, быстрая), `llama3.2:3b`, `phi3`, `mistral`, `llama3.1:8b` (тяжелее).

## 4. Доступ с другого хоста (если MPInformer на другом сервере)

Если приложение (MPInformer) и Ollama на **одном сервере** — в `.env` укажите:

```env
LLM_DISTRIBUTION_URL=http://127.0.0.1:11434/v1/chat/completions
LLM_MODEL=llama3.2
```

Если MPInformer на **другой машине**:

1. В `ollama.service` должна быть строка `Environment="OLLAMA_HOST=0.0.0.0"` (см. выше).
2. Откройте порт 11434 на сервере с Ollama:

   ```bash
   sudo ufw allow 11434/tcp
   sudo ufw reload
   ```

3. В `.env` на сервере с MPInformer укажите IP или домен сервера с Ollama:

   ```env
   LLM_DISTRIBUTION_URL=http://IP_ОЛЛАМЫ:11434/v1/chat/completions
   LLM_MODEL=llama3.2
   ```

`LLM_API_KEY` для Ollama не нужен.

## 5. Проверка API

С сервера с Ollama:

```bash
curl http://127.0.0.1:11434/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"llama3.2","messages":[{"role":"user","content":"Hi"}],"stream":false}'
```

Должен вернуться JSON с ответом модели.

## 6. Ресурсы сервера

- **ОЗУ:** минимум 4–6 GB для моделей 3B, 8+ GB для 7–8B.
- **Диск:** 2–5 GB на одну модель в зависимости от размера.
- **CPU:** работает и на CPU, но быстрее с GPU (NVIDIA через CUDA).

При нехватке памяти используйте маленькую модель, например `llama3.2:3b` или `phi3:mini`.
