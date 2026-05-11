# Внешний ИИ для расстановки печати

Расстановка принтеров при «Перенести план в задачи» может вызывать **внешний** OpenAI-совместимый API (облако). Ollama на сервере не нужна.

---

## 1. Удаление Ollama с сервера (освобождение места)

Выполните по SSH:

```bash
sudo systemctl stop ollama
sudo systemctl disable ollama
sudo apt remove -y ollama
sudo rm -rf /usr/share/ollama /usr/local/lib/ollama
```

Проверка места: `df -h /`

---

## 2. Настройка внешнего ИИ в MPInformer

В `.env` на сервере (каталог с приложением, например `/opt/MPInformer/`) укажите:

### Вариант A: OpenAI

Зарегистрируйтесь на [platform.openai.com](https://platform.openai.com), создайте API-ключ.

```env
LLM_DISTRIBUTION_URL=https://api.openai.com/v1/chat/completions
LLM_API_KEY=sk-ваш-ключ
LLM_MODEL=gpt-4o-mini
```

### Вариант B: Groq (быстро, есть бесплатный тир)

Зарегистрируйтесь на [console.groq.com](https://console.groq.com), создайте API-ключ.

```env
LLM_DISTRIBUTION_URL=https://api.groq.com/openai/v1/chat/completions
LLM_API_KEY=gsk_ваш-ключ
LLM_MODEL=llama-3.1-8b-instant
```

### Вариант C: Другой OpenAI-совместимый сервис

Укажите `LLM_DISTRIBUTION_URL` (endpoint чат-комплишн), `LLM_API_KEY` (если нужен) и `LLM_MODEL` по документации сервиса.

---

## 3. Перезапуск приложения

```bash
sudo systemctl restart mpinformer
```

После этого при нажатии «Перенести план в задачи» запрос уходит во внешний API; ответ используется для выбора принтеров по задачам.
