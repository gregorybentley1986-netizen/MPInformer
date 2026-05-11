# Справочник ключей API для заказов и сумм

Используемые в коде ключи для получения списка заказов и расчёта суммы. Сверьте с актуальной документацией маркетплейсов.

---

## Ozon

### Запрос заказов
- **Метод и URL:** `POST https://api-seller.ozon.ru/v3/posting/fbs/list`
- **Заголовки:** `Client-Id`, `Api-Key`, `Content-Type: application/json`
- **Тело запроса (payload):**
  - `dir` — направление сортировки (`"ASC"`)
  - `limit` — макс. количество (у нас 1000)
  - `offset` — смещение (0)
  - `translit` — транслитерация (true)
  - `with.analytics_data` — true
  - `with.financial_data` — true
  - **Фильтр по датам:** `filter.since`, `filter.to` — строки в формате **ISO 8601 UTC** (`YYYY-MM-DDTHH:MM:SSZ`)

### Ответ — откуда берём список заказов
- **Путь в ответе:** `response["result"]["postings"]`
- То есть: заказы = массив по ключу **`result.postings`**

### Расчёт суммы по одному заказу (в порядке приоритета)
Код перебирает варианты, пока не получит сумму > 0:

| № | Источник | Ключи |
|---|----------|--------|
| 1 | Финансовые данные по товарам | `order["financial_data"]["products"]` — для каждого элемента: **`price`** × **`quantity`** |
| 2 | Товары в заказе | `order["products"]` — для каждого: **`price`** × **`quantity`** |
| 3 | Поля в корне заказа | **`price`** × **`quantity`** (quantity по умолчанию 1) |
| 4 | Альтернативные поля | По одному значению: **`commission_amount`**, **`total_discount_value`**, **`price`**, **`amount`** |

Если ни один вариант не дал сумму, в лог пишутся все ключи верхнего уровня заказа: `list(order.keys())`.

---

## Wildberries

### Запрос заказов
- **Метод и URL:** `GET https://statistics-api.wildberries.ru/api/v1/supplier/orders`
- **Заголовки:** `Authorization: <WB_API_KEY>`, `Content-Type: application/json`
- **Query-параметры:**
  - **`dateFrom`** — начало периода (строка `YYYY-MM-DDTHH:MM:SSZ`; в коде передаётся локальная дата, приведённая к такому формату **без** перевода в UTC — см. ниже)
  - **`dateTo`** — конец периода (аналогично)
  - **`limit`** — макс. количество (1000)

**Важно:** для WB в reporter передаётся `date_from`/`date_to` в локальном времени, но в API уходит `.strftime("%Y-%m-%dT%H:%M:%SZ")` — то есть локальное время с суффиксом Z (как у Ozon до правки). При необходимости для WB тоже стоит конвертировать даты в UTC.

### Ответ — откуда берём список заказов
- **Ответ API:** массив объектов (список заказов). Код: `response.json()` и проверка `isinstance(orders, list)`.

### Расчёт суммы по одному заказу
- **Используется цена со скидкой:** **`priceWithDisc`** (если нет — **`totalPrice`**).
- Код: `order.get("priceWithDisc", 0) or order.get("totalPrice", 0)` — складываются суммы по заказам.

---

## Файлы в проекте

- **Ozon:** запрос и разбор ответа — `app/modules/ozon/api_client.py` (метод `get_orders`, возврат `data["result"].get("postings", [])`). Расчёт суммы по заказу — `app/modules/notifications/reporter.py` (функция `format_report`, цикл по `ozon_orders`).
- **Wildberries:** запрос — `app/modules/wildberries/api_client.py` (метод `get_orders`, URL и params). Расчёт суммы — `app/modules/notifications/reporter.py` (цикл по `wb_orders`, ключ `totalPrice`).

После проверки документации можно поменять ключи в этих местах или добавить альтернативные поля (например, для WB — `convertedPrice` или `priceWithDisc` с комментарием из доки).
