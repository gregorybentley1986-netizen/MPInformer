"""
Клиент для работы с Ozon API
"""
import json
import asyncio
import threading
import time
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Callable, Awaitable, Any
from collections import defaultdict
from loguru import logger
from app.config import settings

_OZON_THROTTLE_LOCK_GUARD = threading.Lock()
_ozon_throttle_async_lock: Optional[asyncio.Lock] = None
_ozon_throttle_next_at: float = 0.0
_ozon_last_request_started_at: Optional[float] = None
_ozon_manual_supply_priority_until: float = 0.0


def _ensure_ozon_throttle_lock() -> asyncio.Lock:
    global _ozon_throttle_async_lock
    with _OZON_THROTTLE_LOCK_GUARD:
        if _ozon_throttle_async_lock is None:
            _ozon_throttle_async_lock = asyncio.Lock()
        return _ozon_throttle_async_lock


def _ozon_path_for_log(url) -> str:
    """Короткий путь для лога (без хоста), из полного URL или относительного."""
    if url is None:
        return "/?"
    s = str(url).strip()
    if not s:
        return "/?"
    marker = "api-seller.ozon.ru"
    if marker in s:
        rest = s.split(marker, 1)[-1]
        if not rest.startswith("/"):
            rest = "/" + rest.lstrip("/")
        return rest.split("?", 1)[0][:160] or "/"
    if s.startswith("/"):
        return s.split("?", 1)[0][:160]
    return s[:160]


def _ozon_op_label(http_method: str, args: tuple, kwargs: dict, explicit: Optional[str]) -> str:
    if explicit:
        return explicit.strip()[:220]
    url = args[0] if len(args) > 0 else kwargs.get("url")
    path = _ozon_path_for_log(url)
    return f"{http_method} {path}".strip()[:220]


def _is_supply_queue_priority_op(op: str) -> bool:
    return (op or "").strip().startswith("supply_queue:")


def activate_manual_supply_priority(seconds: Optional[float] = None) -> None:
    """Включить окно приоритета ручной поставки: фоновые Ozon-запросы ждут."""
    global _ozon_manual_supply_priority_until
    hold = float(seconds) if seconds is not None else float(getattr(settings, "ozon_manual_supply_priority_window_sec", 180.0))
    hold = max(0.0, hold)
    now = time.monotonic()
    with _OZON_THROTTLE_LOCK_GUARD:
        _ozon_manual_supply_priority_until = max(_ozon_manual_supply_priority_until, now + hold)


class ThrottledOzonHttp:
    """
    Обёртка: каждый вызов post/get создаёт отдельный httpx-запрос и проходит через глобальный троттл
    (все экземпляры OzonAPIClient в одном процессе делят один замок).
    """

    __slots__ = ("_api", "_timeout")

    def __init__(self, api: "OzonAPIClient", timeout: float):
        self._api = api
        self._timeout = timeout

    async def post(self, *args, **kwargs):
        op_override = kwargs.pop("_ozon_op", None)
        op = _ozon_op_label("POST", args, kwargs, op_override)

        async def execute(client: httpx.AsyncClient) -> httpx.Response:
            return await client.post(*args, **kwargs)

        return await self._api._ozon_request(self._timeout, execute, op_label=op)

    async def get(self, *args, **kwargs):
        op_override = kwargs.pop("_ozon_op", None)
        op = _ozon_op_label("GET", args, kwargs, op_override)

        async def execute(client: httpx.AsyncClient) -> httpx.Response:
            return await client.get(*args, **kwargs)

        return await self._api._ozon_request(self._timeout, execute, op_label=op)


def _ozon_response_log_line(response: httpx.Response, body_max: int = 900) -> str:
    """
    Одна строка для диагностики лимитов Ozon: статус, Retry-After, типичные rate-limit заголовки, фрагмент JSON-тела.
    """
    h = response.headers
    parts = [f"status={response.status_code}"]
    ra = h.get("retry-after")
    if ra:
        parts.append(f"Retry-After={ra!r}")
    for name in (
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
        "x-ozon-ratelimit-limit",
        "x-ozon-ratelimit-remaining",
        "ratelimit-limit",
        "ratelimit-remaining",
        "ratelimit-reset",
    ):
        v = h.get(name)
        if v:
            parts.append(f"{name}={v!r}")
    raw = (response.text or "")[:body_max].replace("\n", " ").strip()
    if raw:
        parts.append(f"body_preview={raw!r}")
    return " ".join(parts)


class OzonAPIClient:
    """Клиент для работы с Ozon Seller API"""
    
    BASE_URL = "https://api-seller.ozon.ru"
    
    def __init__(self):
        self.client_id = settings.ozon_client_id
        self.api_key = settings.ozon_api_key
        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }

    async def _ozon_request(
        self,
        timeout: float,
        execute: Callable[[httpx.AsyncClient], Awaitable[httpx.Response]],
        *,
        op_label: str = "",
    ) -> httpx.Response:
        """
        Глобальная очередь запросов к api-seller.ozon.ru: один активный HTTP-запрос на процесс;
        после ответа — пауза settings.ozon_request_min_interval_sec.
        """
        lock = _ensure_ozon_throttle_lock()
        interval = max(0.0, float(settings.ozon_request_min_interval_sec))
        global _ozon_throttle_next_at
        global _ozon_last_request_started_at
        op = (op_label or "UNKNOWN").strip()
        task = asyncio.current_task()
        task_name = task.get_name() if task else "-"
        if not _is_supply_queue_priority_op(op):
            waited_ms = 0.0
            while True:
                with _OZON_THROTTLE_LOCK_GUARD:
                    rem = _ozon_manual_supply_priority_until - time.monotonic()
                if rem <= 0:
                    break
                sleep_s = min(0.5, rem)
                await asyncio.sleep(sleep_s)
                waited_ms += sleep_s * 1000.0
            if waited_ms >= 200:
                logger.info(
                    "OzonRequest paused by manual supply priority: op={} wait_ms={:.0f} asyncio_task={!r}",
                    op,
                    waited_ms,
                    task_name,
                )
        async with lock:
            entered = time.monotonic()
            now = time.monotonic()
            wait_after_prev_end = max(0.0, _ozon_throttle_next_at - now)
            wait = wait_after_prev_end
            if wait > 0:
                await asyncio.sleep(wait)
            throttle_wait_ms = (time.monotonic() - entered) * 1000.0
            t_http = time.monotonic()
            _ozon_last_request_started_at = t_http
            try:
                async with httpx.AsyncClient(timeout=timeout) as raw:
                    response = await execute(raw)
            finally:
                _ozon_throttle_next_at = time.monotonic() + interval
            http_ms = (time.monotonic() - t_http) * 1000.0
        sc = response.status_code
        trace_all = bool(getattr(settings, "ozon_request_trace_all", False))
        contended = throttle_wait_ms >= 150.0 or sc >= 400
        detail_429 = ""
        if sc == 429:
            detail_429 = " | " + _ozon_response_log_line(response, body_max=400)
        msg = (
            "OzonRequest trace: op={} status={} throttle_wait_ms={:.0f} http_ms={:.0f} "
            "asyncio_task={!r} interval_sec={}{}"
        )
        if trace_all or contended or sc != 200:
            logger.info(
                msg,
                op,
                sc,
                throttle_wait_ms,
                http_ms,
                task_name,
                interval,
                detail_429,
            )
        else:
            logger.debug(
                msg,
                op,
                sc,
                throttle_wait_ms,
                http_ms,
                task_name,
                interval,
                detail_429,
            )
        return response

    @asynccontextmanager
    async def _ozon_http(self, timeout: float):
        """async with self._ozon_http(30.0) as client: await client.post(...) — каждый post/get через троттл."""
        yield ThrottledOzonHttp(self, timeout)

    async def get_orders(
        self,
        since: Optional[str] = None,
        to: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Получить список заказов
        
        Args:
            since: Дата начала периода (ISO 8601)
            to: Дата окончания периода (ISO 8601)
            status: Статус заказа
            limit: Максимальное количество заказов
        
        Returns:
            Список заказов
        """
        # FBO — заказы со складов Ozon (v2 API)
        url = f"{self.BASE_URL}/v2/posting/fbo/list"
        
        # Создаем фильтр только если есть параметры для фильтрации
        filter_dict = {}
        
        # Ozon API v2 FBO: фильтр по датам — since и to в формате ISO 8601 (UTC, с Z)
        if since:
            filter_dict["since"] = since
            filter_dict["to"] = to if to else datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if status:
            filter_dict["status"] = status
        
        # v2 FBO: limit обязателен, допустимый диапазон (0, 1000]
        limit = max(1, min(1000, limit))
        
        payload = {
            "limit": limit,
            "offset": 0,
            "translit": True,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }
        
        # Добавляем фильтр только если он не пустой
        if filter_dict:
            payload["filter"] = filter_dict
        
        logger.info(f"Ozon API (FBO): POST {url}, filter={filter_dict}")
        
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                
                result = data.get("result")
                if result is None:
                    logger.warning(f"Ozon API: ответ без result. Ключи ответа: {list(data.keys())}")
                    return []
                
                # v2 FBO: result — массив отгрузок напрямую или result.postings / result.Postings
                postings = result if isinstance(result, list) else (result.get("postings") or result.get("Postings") or [])
                logger.info(f"Ozon API (FBO): получено postings={len(postings)}")
                
                if len(postings) == 0 and filter_dict:
                    # Диагностика: пробуем без фильтра по датам (последние заказы)
                    logger.info("Ozon API (FBO): 0 заказов с фильтром по датам, пробуем запрос без фильтра...")
                    payload_no_filter = {
                        "limit": limit,
                        "offset": 0,
                        "translit": True,
                        "with": {"analytics_data": True, "financial_data": True},
                    }
                    resp2 = await client.post(url, json=payload_no_filter, headers=self.headers)
                    resp2.raise_for_status()
                    data2 = resp2.json()
                    result2 = data2.get("result")
                    postings2 = result2 if isinstance(result2, list) else (result2 or {}).get("postings") or (result2 or {}).get("Postings") or []
                    logger.info(f"Ozon API без фильтра: получено postings={len(postings2)}. Если > 0 — фильтр since/to не подходит или даты неверные.")
                    if postings2:
                        # Показать дату первого заказа для проверки
                        first = postings2[0]
                        for key in ("in_process_at", "inProcessAt", "created_at", "createdAt", "shipment_date", "shipmentDate"):
                            if key in first:
                                logger.info(f"Ozon API: у первого заказа без фильтра {key}={first.get(key)}")
                                break
                        return postings2
                
                return postings
                
        except httpx.HTTPStatusError as e:
            # Если ошибка связана с фильтром, попробуем запрос без фильтра по датам
            if e.response.status_code == 400 and since and "Filter" in str(e.response.text):
                logger.warning("Ошибка с фильтром по датам, пробуем запрос без фильтра...")
                # Пробуем запрос без фильтра по датам
                payload_no_date_filter = {
                    "limit": limit,
                    "offset": 0,
                    "translit": True,
                    "with": {
                        "analytics_data": True,
                        "financial_data": True
                    }
                }
                if status:
                    payload_no_date_filter["filter"] = {"status": status}
                
                try:
                    async with self._ozon_http(30.0) as client_retry:
                        response_retry = await client_retry.post(url, json=payload_no_date_filter, headers=self.headers)
                        response_retry.raise_for_status()
                        data_retry = response_retry.json()
                        res = data_retry.get("result")
                        if res is not None:
                            logger.info("Запрос без фильтра по датам успешен, но может вернуть больше данных")
                            return res if isinstance(res, list) else res.get("postings", [])
                except Exception as retry_error:
                    logger.error(f"Запрос без фильтра также не удался: {retry_error}")
            
            logger.error(f"Ошибка при получении заказов Ozon: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при работе с Ozon API: {e}")
            raise
    
    async def get_posting_fbo_list(
        self,
        dir: str = "ASC",
        since: Optional[str] = None,
        to: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 5,
        offset: int = 0,
        translit: bool = True,
        analytics_data: bool = True,
        financial_data: bool = True,
        legal_info: bool = False,
    ) -> Dict:
        """
        Список отправлений FBO.
        POST /v2/posting/fbo/list

        Возвращает исходный JSON Ozon (в основном: {"result": [...]}).
        """
        url = f"{self.BASE_URL}/v2/posting/fbo/list"

        payload: Dict = {
            "dir": (dir or "ASC").strip().upper(),
            "filter": {},
            "limit": max(1, min(1000, int(limit) if limit is not None else 5)),
            "offset": max(0, int(offset) if offset is not None else 0),
            "translit": bool(translit),
            "with": {
                "analytics_data": bool(analytics_data),
                "financial_data": bool(financial_data),
                "legal_info": bool(legal_info),
            },
        }

        if since:
            payload["filter"]["since"] = str(since)
        if to:
            payload["filter"]["to"] = str(to)
        if status is not None:
            payload["filter"]["status"] = str(status)

        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            preview = (e.response.text or "")[:800]
            logger.warning("Ozon v2/posting/fbo/list HTTP {}: {}", code, preview)
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": preview}
        except Exception as e:
            logger.warning("Ozon v2/posting/fbo/list error: {}", e, exc_info=True)
            return {"_error": "request_failed", "error": str(e)}
    
    async def get_order_details(self, posting_number: str) -> Optional[Dict]:
        """
        Получить детали заказа
        
        Args:
            posting_number: Номер отправления
        
        Returns:
            Детали заказа или None
        """
        url = f"{self.BASE_URL}/v3/posting/fbs/get"
        
        payload = {
            "posting_number": posting_number,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }
        
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                
                if data.get("result"):
                    return data["result"]
                return None
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Ошибка при получении деталей заказа Ozon: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при получении деталей заказа Ozon: {e}")
            return None

    async def get_posting_fbo(
        self,
        posting_number: str,
        translit: bool = True,
        analytics_data: bool = True,
        financial_data: bool = True,
        legal_info: bool = False,
    ) -> Optional[Dict]:
        """
        Информация об отправлении (FBO).
        POST /v2/posting/fbo/get
        Тело:
        {
            "posting_number": "...",
            "translit": true,
            "with": {
                "analytics_data": true,
                "financial_data": true,
                "legal_info": false
            }
        }
        """
        posting_number = (posting_number or "").strip()
        if not posting_number:
            return None

        url = f"{self.BASE_URL}/v2/posting/fbo/get"
        payload = {
            "posting_number": posting_number,
            "translit": bool(translit),
            "with": {
                "analytics_data": bool(analytics_data),
                "financial_data": bool(financial_data),
                "legal_info": bool(legal_info),
            },
        }

        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict) and data.get("result") is not None:
                    return data.get("result")
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            preview = (e.response.text or "")[:800]
            logger.warning("Ozon v2/posting/fbo/get HTTP {}: {}", code, preview)
            return {"_error": f"HTTP {code}", "ozon_response": preview}
        except Exception as e:
            logger.warning("Ozon v2/posting/fbo/get error: {}", e, exc_info=True)
            return {"_error": "request_failed", "error": str(e)}

    async def get_transactions(
        self,
        date_from: datetime,
        date_to: datetime,
        page_size: int = 1000,
    ) -> List[Dict]:
        """
        Получить транзакции (выплаты) за период.
        POST /v3/finance/transaction/list.
        Returns: список операций с amount, operation_date.
        """
        url = f"{self.BASE_URL}/v3/finance/transaction/list"
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=timezone.utc)
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=timezone.utc)
        from_iso = date_from.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        to_iso = date_to.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        all_ops: List[Dict] = []
        page = 1
        try:
            async with self._ozon_http(60.0) as client:
                while True:
                    payload = {
                        "filter": {"date": {"from": from_iso, "to": to_iso}},
                        "page": page,
                        "page_size": page_size,
                    }
                    response = await client.post(url, json=payload, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    result = data.get("result") or {}
                    ops = result.get("operations") or result.get("Operations") or []
                    if page == 1 and ops:
                        logger.info("Ozon transactions: первая запись: %s", json.dumps(ops[0], ensure_ascii=False, default=str)[:500])
                    all_ops.extend(ops)
                    page_count = result.get("page_count") or result.get("PageCount") or 1
                    if page >= page_count or not ops:
                        break
                    page += 1
        except httpx.HTTPStatusError as e:
            logger.warning("Ozon transactions: %s - %s", e.response.status_code, e.response.text[:300])
            return []
        except Exception as e:
            logger.warning("Ozon transactions: %s", e)
            return []
        return all_ops

    async def get_product_list(self) -> List[Dict]:
        """
        Список товаров: POST /v3/product/list с пагинацией по last_id.
        Возвращает список [{"product_id": int, "offer_id": str}, ...].
        """
        url = f"{self.BASE_URL}/v3/product/list"
        items: List[Dict] = []
        last_id = ""
        try:
            async with self._ozon_http(60.0) as client:
                while True:
                    # limit обязателен, значение в диапазоне (0, 1000]
                    payload = {"filter": {"visibility": "ALL"}, "limit": 1000}
                    if last_id:
                        payload["last_id"] = last_id
                    response = await client.post(url, json=payload, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    res = data.get("result")
                    if res is None:
                        logger.warning(
                            "Ozon v3/product/list: ответ без result, ключи=%s, body=%s",
                            list(data.keys()),
                            json.dumps(data, ensure_ascii=False)[:500],
                        )
                        break
                    if isinstance(res, list):
                        batch = res
                    else:
                        batch = res.get("items") or res.get("Items") or []
                    if not isinstance(batch, list):
                        batch = []
                    if not items and not batch:
                        logger.warning(
                            "Ozon v3/product/list: пустой items в result, result.keys=%s",
                            list(res.keys()) if isinstance(res, dict) else type(res),
                        )
                        break
                    for item in batch:
                        if not isinstance(item, dict):
                            continue
                        pid = item.get("product_id") or item.get("productId")
                        oid = item.get("offer_id") or item.get("offerId") or ""
                        if pid is not None and oid:
                            items.append({
                                "product_id": int(pid),
                                "offer_id": str(oid).strip(),
                            })
                    last_id = (res.get("last_id") or "").strip()
                    if not last_id or not batch:
                        break
            logger.info("Ozon v3/product/list: загружено товаров=%s" % len(items))
        except httpx.HTTPStatusError as e:
            body = (e.response.text or "")[:500]
            logger.warning("Ozon get_product_list HTTP %s: %s" % (e.response.status_code, body))
        except Exception as e:
            logger.warning("Ozon get_product_list: %s" % str(e), exc_info=True)
        return items

    async def get_products_info_attributes(self, offer_ids: List[str]) -> Dict[str, Dict]:
        """
        Атрибуты товаров (в т.ч. name) по offer_id: POST /v4/product/info/attributes.
        Возвращает словарь offer_id -> {"id": product_id, "name": str, "offer_id": str}.
        """
        if not offer_ids:
            return {}
        url = f"{self.BASE_URL}/v4/product/info/attributes"
        result: Dict[str, Dict] = {}
        batch_size = 1000
        try:
            async with self._ozon_http(60.0) as client:
                for i in range(0, len(offer_ids), batch_size):
                    batch = offer_ids[i : i + batch_size]
                    # v4/product/info/attributes: filter обязателен, limit в диапазоне (0, 1000]
                    payload = {"filter": {"visibility": "ALL", "offer_id": batch}, "limit": 1000}
                    response = await client.post(url, json=payload, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    res = data.get("result")
                    if isinstance(res, list):
                        items = res
                    elif isinstance(res, dict):
                        items = res.get("items") or res.get("Items") or []
                    else:
                        items = []
                    if not isinstance(items, list):
                        items = []
                    if items and not result:
                        logger.info(
                            "Ozon v4/product/info/attributes — ключи первого item: %s"
                            % (list(items[0].keys()) if isinstance(items[0], dict) else None,)
                        )
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        oid = (item.get("offer_id") or item.get("offerId") or "").strip()
                        if not oid:
                            continue
                        pid = item.get("id") or item.get("product_id") or item.get("productId")
                        name = (item.get("name") or "").strip()
                        if not name:
                            # Название в attributes: ищем атрибут с attribute_id 4180 (Название) или первый с value
                            for att in item.get("attributes") or []:
                                if not isinstance(att, dict):
                                    continue
                                aid = att.get("attribute_id") or att.get("id")
                                if aid == 4180:
                                    vals = att.get("values") or []
                                    if isinstance(vals, list) and vals:
                                        v = vals[0] if isinstance(vals[0], dict) else {}
                                        name = (v.get("value") or "").strip()
                                    break
                                if not name:
                                    vals = att.get("values") or []
                                    if isinstance(vals, list) and vals:
                                        v = vals[0] if isinstance(vals[0], dict) else {}
                                        name = (v.get("value") or "").strip()
                        result[oid] = {"id": pid, "name": name or "—", "offer_id": oid}
            logger.info("Ozon v4/product/info/attributes: загружено атрибутов=%s" % len(result))
        except httpx.HTTPStatusError as e:
            body = (e.response.text or "")[:500]
            logger.warning("Ozon get_products_info_attributes HTTP %s: %s" % (e.response.status_code, body))
        except Exception as e:
            logger.warning("Ozon get_products_info_attributes: %s" % str(e), exc_info=True)
        return result

    async def get_product_names(self, offer_ids: List[str]) -> Dict[str, str]:
        """
        Получить названия товаров по артикулам (offer_id).
        POST /v3/product/info/list. Параметр: offer_id или articles (массив до 100 за запрос).
        Возвращает словарь offer_id -> name.
        """
        if not offer_ids:
            return {}
        result_map: Dict[str, str] = {}
        # Документация: до 100 товаров за запрос (Get products informations)
        batch_size = 100
        try:
            async with self._ozon_http(60.0) as client:
                for i in range(0, len(offer_ids), batch_size):
                    batch = offer_ids[i : i + batch_size]
                    # Документация: offer_id или articles (Articles в док.)
                    url = f"{self.BASE_URL}/v3/product/info/list"
                    payload = {"offer_id": batch}
                    response = await client.post(url, json=payload, headers=self.headers)
                    if response.status_code == 400 and "offer_id" in (response.text or "").lower():
                        payload = {"articles": batch}
                        response = await client.post(url, json=payload, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    res = data.get("result")
                    if isinstance(res, list):
                        items = res
                    else:
                        items = (res or {}).get("items") or (res or {}).get("Items") or []
                    if not isinstance(items, list):
                        items = []
                    if items and not result_map:
                        logger.info(
                            "Ozon API /v3/product/info/list — ключи первого item: %s",
                            list(items[0].keys()) if isinstance(items[0], dict) else None,
                        )
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        oid = (item.get("offer_id") or item.get("offerId") or "").strip()
                        if not oid:
                            continue
                        # name или title (в зависимости от версии API)
                        name = (
                            (item.get("name") or item.get("title") or item.get("Name") or "")
                            .strip()
                        )
                        result_map[oid] = name or "—"
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ошибка при получении наименований Ozon: %s — %s",
                e.response.status_code,
                (e.response.text or "")[:400],
            )
        except Exception as e:
            logger.warning("Ошибка при получении наименований Ozon: %s", e, exc_info=True)
        return result_map

    def _parse_v1_cluster_list_response(self, data: object) -> List[Dict]:
        """
        Разбор JSON ответа POST /v1/cluster/list (clusters[]).
        Возвращает: [ {"id", "name", "warehouses", "macrolocal_cluster_id"?}, ... ].
        """
        if not isinstance(data, dict):
            return []
        items = data.get("clusters")
        if not isinstance(items, list):
            logger.info(
                "Ozon v1/cluster/list: ключи ответа={}, clusters не массив",
                list(data.keys()),
            )
            return []
        out: List[Dict] = []
        for c in items:
            if not isinstance(c, dict):
                continue
            cid = c.get("id")
            macrolocal = c.get("macrolocal_cluster_id")
            if macrolocal is None and c.get("logistic_clusters"):
                lc0 = (c["logistic_clusters"] or [None])[0]
                if isinstance(lc0, dict):
                    macrolocal = lc0.get("macrolocal_cluster_id")
            cname = (c.get("name") or "").strip() or str(cid or "")
            warehouses: List[Dict] = []
            for lc in c.get("logistic_clusters") or []:
                if not isinstance(lc, dict):
                    continue
                for w in lc.get("warehouses") or []:
                    if not isinstance(w, dict):
                        continue
                    wid = w.get("warehouse_id") or w.get("id")
                    wname = (w.get("name") or "").strip() or str(wid or "")
                    wtype = (w.get("type") or "").strip() or "DELIVERY_POINT"
                    warehouses.append({"id": str(wid) if wid is not None else "", "name": wname, "type": wtype})
            row: Dict = {
                "id": str(cid) if cid is not None else "",
                "name": cname,
                "warehouses": warehouses,
            }
            if macrolocal is not None:
                row["macrolocal_cluster_id"] = int(macrolocal) if isinstance(macrolocal, (int, float)) else macrolocal
            out.append(row)
        return out

    async def get_cluster_list(
        self,
        cluster_type: str = "CLUSTER_TYPE_OZON",
        cluster_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Список кластеров и складов: POST /v1/cluster/list.
        Тело: cluster_type (обязательно), cluster_ids (опционально).
        Ответ: clusters[] с id, name, logistic_clusters[].warehouses[] (warehouse_id, name, type).
        Возвращает: [ {"id": str, "name": str, "warehouses": [ {"id": str, "name": str}, ... ] }, ... ].
        """
        url = f"{self.BASE_URL}/v1/cluster/list"
        body: Dict = {"cluster_type": cluster_type}
        if cluster_ids:
            body["cluster_ids"] = cluster_ids
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:cluster_list",
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ozon v1/cluster/list HTTP {}: {}",
                e.response.status_code,
                (e.response.text or "")[:400],
            )
            return []
        except Exception as e:
            logger.warning("Ozon get_cluster_list: {}", e, exc_info=True)
            return []
        out = self._parse_v1_cluster_list_response(data)
        logger.info(
            "Ozon v1/cluster/list ({}): кластеров={}, всего складов={}",
            cluster_type,
            len(out),
            sum(len(cl.get("warehouses") or []) for cl in out),
        )
        return out

    async def get_cluster_list_for_supply(
        self,
        filter_by_supply_type: Optional[List[str]] = None,
        search: str = "",
        cluster_type: str = "CLUSTER_TYPE_OZON",
    ) -> List[Dict]:
        """
        POST /v1/cluster/list — тело: cluster_type, filter_by_supply_type, search (док. Ozon FBO).
        Без cluster_type Ozon отвечает 400 invalid cluster type.
        Ответ: тот же clusters[] → тот же нормализованный список, что у get_cluster_list.
        """
        url = f"{self.BASE_URL}/v1/cluster/list"
        body: Dict = {
            "cluster_type": cluster_type,
            "filter_by_supply_type": filter_by_supply_type or ["CREATE_TYPE_CROSSDOCK"],
            "search": (search or "").strip(),
        }
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:cluster_list_supply",
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ozon v1/cluster/list (supply filter) HTTP {}: {}",
                e.response.status_code,
                (e.response.text or "")[:400],
            )
            return []
        except Exception as e:
            logger.warning("Ozon get_cluster_list_for_supply: {}", e, exc_info=True)
            return []
        out = self._parse_v1_cluster_list_response(data)
        logger.info(
            "Ozon v1/cluster/list (filter_by_supply_type): кластеров={}, всего складов={}",
            len(out),
            sum(len(cl.get("warehouses") or []) for cl in out),
        )
        return out

    async def get_warehouse_list(self) -> Dict[str, str]:
        """
        Список складов: warehouse_id -> название (для отображения кластеров).
        POST /v1/warehouse/list — склады продавца (для остатков по кластерам).
        Другой метод: /v1/warehouse/fbo/list — склады/сортировочные центры FBO (другое назначение).
        При ошибке возвращаем пустой словарь. В лог выводится полный JSON первого склада без обрезки ключей.
        """
        url = f"{self.BASE_URL}/v1/warehouse/list"
        result: Dict[str, str] = {}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json={}, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                res = data.get("result")
                if isinstance(res, list):
                    items = res
                elif isinstance(res, dict):
                    items = res.get("warehouses") or res.get("items") or res.get("list") or res.get("data") or []
                    if not items and "result" in res:
                        items = res["result"] if isinstance(res["result"], list) else []
                else:
                    items = []
                if not isinstance(items, list):
                    items = []
                if not items and data:
                    items = data.get("warehouses") or data.get("items") or []
                    if isinstance(items, list):
                        pass
                    else:
                        items = []
                logger.info(
                    "Ozon warehouse/list: ключи data=%s, result type=%s, len(items)=%s",
                    list(data.keys()), type(res).__name__, len(items),
                )
                if not items and isinstance(res, dict):
                    logger.info("Ozon warehouse/list: ключи result=%s", list(res.keys()))
                if items and isinstance(items[0], dict):
                    logger.info(
                        "Ozon API /v1/warehouse/list — полный ответ для 1 склада (первый item): %s"
                        % (json.dumps(items[0], ensure_ascii=False, indent=2),)
                    )
                    logger.info("Ozon warehouse/list: ключи первого склада=%s" % list(items[0].keys()))
                for w in items:
                    if not isinstance(w, dict):
                        continue
                    wid = w.get("warehouse_id") or w.get("warehouseId") or w.get("id")
                    name = (
                        (w.get("name") or w.get("title") or w.get("warehouse_name") or "")
                        .strip()
                        or str(wid or "")
                    )
                    if wid is not None:
                        result[str(wid)] = name or str(wid)
                logger.info("Ozon warehouse/list: загружено складов=%s, id->name=%s" % (len(result), result))
        except Exception as e:
            logger.warning(f"Ошибка при получении списка складов Ozon: {e}")
        return result

    async def get_fbo_warehouse_list(self) -> Dict[str, str]:
        """
        Список складов FBO (города/сортировочные центры Ozon): warehouse_id -> название.
        POST /v1/warehouse/fbo/list — для подстановки названий по warehouse_id из отчёта остатков.
        Параметр search обязателен (мин. 4 символа). Пробуем несколько запросов и объединяем результат.
        """
        url = f"{self.BASE_URL}/v1/warehouse/fbo/list"
        result: Dict[str, str] = {}
        for search in ("Озон", "склад", "Москва", "Пушкин", "Казань", "Санкт", "Петербург", "Екатеринбург", "сортировочный", "ПВЗ"):
            try:
                async with self._ozon_http(15.0) as client:
                    response = await client.post(
                        url,
                        json={"search": search},
                        headers=self.headers,
                    )
                    response.raise_for_status()
                    data = response.json()
                    res = data.get("result")
                    if isinstance(res, dict):
                        items = (
                            res.get("warehouses")
                            or res.get("search")
                            or res.get("list")
                            or res.get("items")
                            or res.get("data")
                            or []
                        )
                    elif isinstance(res, list):
                        items = res
                    else:
                        items = data.get("warehouses") or data.get("search") or data.get("list") or []
                    # Документация: ответ может быть {"search": [...]} на верхнем уровне
                    if (not items or not isinstance(items, list)) and data.get("search") is not None:
                        items = data["search"] if isinstance(data["search"], list) else []
                    if not isinstance(items, list):
                        items = []
                    if not result and data:
                        logger.info(
                            "Ozon FBO warehouse/list (search=%s): ключи ответа=%s, result type=%s",
                            search,
                            list(data.keys()),
                            type(res).__name__,
                        )
                    for w in items:
                        if not isinstance(w, dict):
                            continue
                        wid = w.get("warehouse_id") or w.get("warehouseId") or w.get("id")
                        name = (
                            (w.get("name") or w.get("title") or w.get("warehouse_name") or w.get("warehouseName") or "")
                            .strip()
                            or str(wid or "")
                        )
                        if wid is not None and (name or str(wid)):
                            result[str(wid)] = name or str(wid)
                        elif name:
                            result[name] = name
            except Exception as e:
                logger.debug("Ozon FBO warehouse list (search=%s): %s", search, e)
        if result:
            logger.info("Ozon FBO warehouse/list: загружено складов=%s", len(result))
        return result

    async def search_fbo_shipment_points(
        self,
        search: str,
        filter_by_supply_type: Optional[List[str]] = None,
    ) -> Dict:
        """
        Поиск точек отгрузки для кросс-докинга и прямых поставок.
        POST /v1/warehouse/fbo/list — по доке Ozon: filter_by_supply_type и search (мин. 4 символа).
        filter_by_supply_type: CREATE_TYPE_CROSSDOCK | CREATE_TYPE_DIRECT.
        """
        url = f"{self.BASE_URL}/v1/warehouse/fbo/list"
        search = (search or "").strip()
        if len(search) < 4:
            return {"result": {}, "_error": "Параметр search должен быть не короче 4 символов"}
        body: Dict = {
            "filter_by_supply_type": filter_by_supply_type or ["CREATE_TYPE_CROSSDOCK"],
            "search": search,
        }
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.warning("Ozon v1/warehouse/fbo/list HTTP {}: {}", e.response.status_code, (e.response.text or "")[:400])
            raise
        except Exception as e:
            logger.warning("Ozon search_fbo_shipment_points: {}", e)
            raise

    async def get_stocks_from_analytics(self) -> List[Dict]:
        """
        Остатки по складам из аналитики: POST /v2/analytics/stock_on_warehouses.
        Возвращает список: [ {"name": "Название склада", "rows": [{"article", "name", "stock"}], ...} ],
        или пустой список при ошибке / пустом ответе.
        """
        url = f"{self.BASE_URL}/v2/analytics/stock_on_warehouses"
        rows_raw: List[Dict] = []
        try:
            async with self._ozon_http(60.0) as client:
                cursor = None
                while True:
                    payload: Dict = {"limit": 1000}
                    if cursor is not None:
                        payload["cursor"] = cursor
                    response = await client.post(url, json=payload, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    res = data.get("result") or data
                    chunk = []
                    if isinstance(res, list):
                        chunk = res
                    elif isinstance(res, dict):
                        chunk = (
                            res.get("rows")
                            or res.get("items")
                            or res.get("stock_on_warehouses")
                            or res.get("data")
                            or res.get("result")
                            or []
                        )
                    if not isinstance(chunk, list):
                        chunk = []
                    if not chunk and isinstance(data.get("result"), dict):
                        chunk = data["result"].get("stock_on_warehouses") or []
                    if not isinstance(chunk, list):
                        chunk = []
                    rows_raw.extend(chunk)
                    cursor = None
                    if isinstance(res, dict):
                        cursor = (res.get("cursor") or res.get("next_id") or res.get("last_id") or "").strip()
                    if not cursor or not chunk:
                        break
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ozon v2/analytics/stock_on_warehouses HTTP %s: %s"
                % (e.response.status_code, (e.response.text or "")[:400])
            )
            return []
        except Exception as e:
            logger.warning("Ozon get_stocks_from_analytics: %s" % str(e), exc_info=True)
            return []

        if not rows_raw:
            logger.info(
                "Ozon v2/analytics/stock_on_warehouses: ключи ответа=%s, строк=0"
                % list(data.keys())
            )
            return []

        # Сопоставление product_id -> offer_id для подстановки в таблицу
        products = await self.get_product_list()
        product_id_to_offer_id: Dict[str, str] = {}
        for p in products:
            pid = p.get("product_id")
            oid = (p.get("offer_id") or "").strip()
            if pid is not None and oid:
                product_id_to_offer_id[str(pid)] = oid

        first = rows_raw[0] if rows_raw else {}
        if isinstance(first, dict):
            logger.info(
                "Ozon v2/analytics/stock_on_warehouses: ключи первой строки=%s, полная строка=%s"
                % (list(first.keys()), json.dumps(first, ensure_ascii=False)[:600])
            )
        # Группировка: кластер -> склад -> offer_id -> qty (если в ответе есть cluster_id/cluster_name)
        # Иначе: один кластер "Склады" со всеми складами внутри
        by_cluster_wh: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        all_offer_ids: set = set()
        for r in rows_raw:
            if not isinstance(r, dict):
                continue
            cluster_id = r.get("cluster_id") or r.get("clusterId")
            cluster_name = (
                (r.get("cluster_name") or r.get("clusterName") or r.get("cluster") or "").strip()
            )
            cluster_key = cluster_name or (str(cluster_id) if cluster_id is not None else "__single__")
            if cluster_id is not None and not cluster_name:
                cluster_key = str(cluster_id)
            wh_id = r.get("warehouse_id") or r.get("warehouseId")
            wh_name = (
                (r.get("warehouse_name") or r.get("warehouseName") or r.get("name") or r.get("title") or "")
                .strip()
            )
            wh_key = wh_name or (str(wh_id) if wh_id is not None else "__default__")
            if wh_id is not None and not wh_name:
                wh_key = str(wh_id)
            article = (
                str(r.get("offer_id") or r.get("offerId") or r.get("item_code") or r.get("article") or "")
                .strip()
            )
            if not article:
                pid = r.get("product_id") or r.get("productId") or r.get("sku")
                if pid is not None:
                    article = product_id_to_offer_id.get(str(pid)) or ""
            if not article:
                continue
            qty = 0
            for key in (
                "free_to_sell",
                "item_count",
                "stock",
                "present",
                "quantity",
                "count",
                "amount",
                "free_to_sell_amount",
                "reserved",
                "qty",
                "total",
            ):
                val = r.get(key)
                if val is not None:
                    try:
                        qty = int(val)
                        break
                    except (TypeError, ValueError):
                        pass
            if qty == 0 and isinstance(r.get("item"), dict):
                for key in ("free_to_sell", "item_count", "stock", "present", "quantity", "count"):
                    val = r["item"].get(key)
                    if val is not None:
                        try:
                            qty = int(val)
                            break
                        except (TypeError, ValueError):
                            pass
            if qty == 0 and isinstance(r.get("stock"), dict):
                for key in ("free_to_sell", "present", "quantity", "count"):
                    val = r["stock"].get(key)
                    if val is not None:
                        try:
                            qty = int(val)
                            break
                        except (TypeError, ValueError):
                            pass
            by_cluster_wh[cluster_key][wh_key][article] += qty
            all_offer_ids.add(article)

        if not by_cluster_wh:
            return []

        warehouse_names = await self.get_warehouse_list()
        fbo_names = await self.get_fbo_warehouse_list()
        for wid, wname in fbo_names.items():
            if wid and wname and wid not in warehouse_names:
                warehouse_names[wid] = wname
        names_map = await self.get_product_names(list(all_offer_ids)) if all_offer_ids else {}

        def _place_name(key: str, is_cluster: bool) -> str:
            if key in ("__default__", "__single__"):
                return "Ozon" if is_cluster else "—"
            if key and not str(key).isdigit():
                return key
            return warehouse_names.get(key) or str(key)

        # Если кластер не задан в API — один кластер "Склады", внутри все склады
        if len(by_cluster_wh) == 1 and "__single__" in by_cluster_wh:
            by_warehouse_flat = by_cluster_wh["__single__"]
            # Добавляем все склады из справочников, чтобы вывести полный список (остаток 0 если нет в аналитике)
            for wid, wname in list(warehouse_names.items()) + list(fbo_names.items()):
                if wid and str(wid) not in by_warehouse_flat:
                    by_warehouse_flat[str(wid)] = defaultdict(int)
                if wname and str(wname).strip() and str(wname).strip() not in by_warehouse_flat:
                    by_warehouse_flat[str(wname).strip()] = defaultdict(int)
            by_cluster_wh = {"Склады": by_warehouse_flat}

        clusters: List[Dict] = []
        for cl_key in sorted(by_cluster_wh.keys(), key=lambda x: (x == "__single__", _place_name(x, True))):
            by_wh = by_cluster_wh[cl_key]
            cluster_display_name = _place_name(cl_key, True)
            warehouses: List[Dict] = []
            for wh_key in sorted(by_wh.keys(), key=lambda x: (x == "__default__", _place_name(x, False))):
                rows_data = by_wh[wh_key]
                wh_display_name = _place_name(wh_key, False)
                rows = []
                for article in sorted(all_offer_ids):
                    qty = rows_data.get(article, 0)
                    rows.append({"article": article, "name": names_map.get(article, "—"), "stock": qty})
                warehouses.append({"name": wh_display_name, "rows": rows})
            rows = []
            for article in sorted(all_offer_ids):
                total_cluster = sum(by_wh[wh_key].get(article, 0) for wh_key in by_wh)
                rows.append({"article": article, "name": names_map.get(article, "—"), "stock": total_cluster})
            clusters.append({
                "name": cluster_display_name,
                "rows": rows,
                "warehouses": warehouses,
            })
        logger.info(
            "Ozon v2/analytics/stock_on_warehouses: кластеров=%s, строк всего=%s"
            % (len(clusters), sum(len(c["rows"]) for c in clusters))
        )
        return clusters

    async def get_stocks_by_cluster(
        self,
        *,
        cluster_list: Optional[List[Dict]] = None,
    ) -> List[Dict]:
        """
        Остатки Ozon по кластерам и складам.
        Сначала получаем список кластеров (SupplyDraftAPI DraftClusterList: /v1/supply/draft/cluster/list),
        внутри каждого — список складов. Остатки (аналитика или v4) раскидываем по этим складам.
        Если список кластеров пустой — аналитика или v4/product/info/stocks без привязки к кластерам.
        """
        if cluster_list is None:
            cluster_list = await self.get_cluster_list()
        if cluster_list:
            stock_clusters = await self.get_stocks_from_analytics()
            if not stock_clusters:
                stock_clusters = await self._get_stocks_by_cluster_v4_fallback()
            by_warehouse: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
            all_offer_ids: set = set()
            for c in stock_clusters:
                for wh in c.get("warehouses") or []:
                    wh_key = wh.get("name") or wh.get("id") or ""
                    for row in wh.get("rows") or []:
                        a = (row.get("article") or "").strip()
                        if a:
                            by_warehouse[wh_key][a] = int(row.get("stock") or 0)
                            all_offer_ids.add(a)
            products = await self.get_product_list()
            for p in products:
                oid = (p.get("offer_id") or "").strip()
                if oid:
                    all_offer_ids.add(oid)
            names_map = await self.get_product_names(list(all_offer_ids)) if all_offer_ids else {}
            result: List[Dict] = []
            for cl in cluster_list:
                wh_list: List[Dict] = []
                cluster_total: Dict[str, int] = defaultdict(int)
                for wh in cl.get("warehouses") or []:
                    wh_id = wh.get("id") or ""
                    wh_name = (wh.get("name") or "").strip() or wh_id or "—"
                    stock_map = by_warehouse.get(wh_id) or by_warehouse.get(wh_name) or {}
                    rows = []
                    for a in sorted(all_offer_ids):
                        q = stock_map.get(a, 0)
                        cluster_total[a] += q
                        rows.append({"article": a, "name": names_map.get(a, "—"), "stock": q})
                    wh_list.append({"name": wh_name, "rows": rows})
                cluster_rows = [
                    {"article": a, "name": names_map.get(a, "—"), "stock": cluster_total.get(a, 0)}
                    for a in sorted(all_offer_ids)
                ]
                entry: Dict = {
                    "name": (cl.get("name") or "").strip() or "—",
                    "rows": cluster_rows,
                    "warehouses": wh_list,
                }
                mid = cl.get("macrolocal_cluster_id")
                if mid is not None:
                    try:
                        entry["macrolocal_cluster_id"] = int(mid)
                    except (TypeError, ValueError):
                        entry["macrolocal_cluster_id"] = mid
                result.append(entry)
            logger.info(
                "Ozon get_stocks_by_cluster (по списку кластеров): кластеров=%s, всего складов=%s"
                % (len(result), sum(len(r["warehouses"]) for r in result))
            )
            return result

        clusters = await self.get_stocks_from_analytics()
        if clusters:
            return clusters
        return await self._get_stocks_by_cluster_v4_fallback()

    async def _get_stocks_by_cluster_v4_fallback(self) -> List[Dict]:
        """Остатки из v4/product/info/stocks, формат как у get_stocks_from_analytics (clusters с warehouses)."""
        url = f"{self.BASE_URL}/v4/product/info/stocks"
        # warehouse_id -> { offer_id -> qty }
        by_warehouse: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        last_id = ""
        try:
            async with self._ozon_http(60.0) as client:
                while True:
                    payload = {
                        "filter": {"visibility": "ALL"},
                        "limit": 1000,
                        "Limit": 1000,
                    }
                    if last_id:
                        payload["last_id"] = last_id
                    response = await client.post(
                        f"{url}?limit=1000",
                        json=payload,
                        headers=self.headers,
                    )
                    response.raise_for_status()
                    data = response.json()
                    res = data.get("result") or {}
                    items = res.get("items") or data.get("items") or []
                    if not by_warehouse and data:
                        logger.info(
                            "Ozon stocks v4: ключи ответа=%s, result.keys=%s, len(items)=%s"
                            % (list(data.keys()), list(res.keys()) if res else [], len(items))
                        )
                    if not by_warehouse:
                        if not items:
                            logger.info("Ozon stocks v4: первая порция пуста (items=0). result=%s" % (list(res.keys()) if res else "нет"))
                        else:
                            first = items[0]
                            logger.info(
                                "Ozon API /v4/product/info/stocks — полный ответ для 1 товара (первый item): %s"
                                % (json.dumps(first, ensure_ascii=False, indent=2),)
                            )
                            logger.info(
                                "Ozon stocks v4: ключи первого item=%s"
                                % (list(first.keys()) if isinstance(first, dict) else type(first),)
                            )
                            if isinstance(first, dict) and first.get("stocks"):
                                logger.info(
                                    "Ozon stocks v4: первый item.stocks[0] ключи=%s"
                                    % (list(first["stocks"][0].keys()) if first["stocks"] else None,)
                                )
                    for item in items:
                        try:
                            offer_id = (
                                item.get("offer_id")
                                or item.get("offerId")
                                or (item.get("offer") or {}).get("offer_id")
                                or (item.get("offer") or {}).get("offerId")
                                or ""
                            )
                            if not offer_id:
                                continue
                            offer_id = str(offer_id).strip()
                            # Вариант 1: один item на строку — offer_id, warehouse_id, stock
                            wh_id = item.get("warehouse_id") or item.get("warehouseId")
                            qty = 0
                            if item.get("stock") is not None:
                                qty = int(item.get("stock", 0) or 0)
                            if wh_id is not None and qty >= 0:
                                by_warehouse[str(wh_id)][offer_id] += qty
                                continue
                            # Вариант 2: в item вложен массив stocks[] — present, warehouse_id/warehouse_ids, возможно warehouse_name
                            added = False
                            for s in item.get("stocks") or []:
                                present = int(s.get("present", 0) or 0)
                                if present <= 0:
                                    continue
                                # Название склада из ответа (город) — приоритет
                                wh_name = (
                                    (s.get("warehouse_name") or s.get("warehouseName") or s.get("name") or s.get("title") or "")
                                    .strip()
                                )
                                swh = s.get("warehouse_id") or s.get("warehouseId")
                                warehouse_ids = s.get("warehouse_ids") or s.get("warehouseIds") or []
                                if wh_name:
                                    by_warehouse[wh_name][offer_id] += present
                                    added = True
                                elif swh is not None:
                                    by_warehouse[str(swh)][offer_id] += present
                                    added = True
                                elif warehouse_ids:
                                    first_wh = next((w for w in warehouse_ids if w is not None), None)
                                    if first_wh is not None and len(warehouse_ids) == 1:
                                        by_warehouse[str(first_wh)][offer_id] += present
                                    else:
                                        by_warehouse["__default__"][offer_id] += present
                                    added = True
                                else:
                                    by_warehouse["__default__"][offer_id] += present
                                    added = True
                            if not added:
                                qty = qty or int(item.get("present", 0) or 0) or int(item.get("free_to_sell", 0) or 0)
                                if qty > 0:
                                    by_warehouse["__default__"][offer_id] += qty
                        except (TypeError, ValueError, KeyError):
                            continue
                    last_id = (res.get("last_id") or "").strip()
                    if not last_id or not items:
                        break
        except Exception as e:
            logger.warning(f"Ошибка при получении остатков Ozon по кластерам: {e}")
            return []

        by_wh_summary = {wh: len(rows) for wh, rows in by_warehouse.items()}
        logger.info(
            "Ozon stocks by_cluster: всего складов/кластеров=%s, распределение (wh_id -> кол-во артикулов)=%s"
            % (len(by_warehouse), by_wh_summary)
        )

        # Справочники названий складов загружаем до формирования кластеров (как для наименований товаров)
        warehouse_names = await self.get_warehouse_list()
        fbo_names = await self.get_fbo_warehouse_list()
        for wid, wname in fbo_names.items():
            if wid and wname and wid not in warehouse_names:
                warehouse_names[wid] = wname
        all_offer_ids = set()
        for wh_data in by_warehouse.values():
            all_offer_ids.update(wh_data.keys())
        names_map = await self.get_product_names(list(all_offer_ids)) if all_offer_ids else {}

        clusters: List[Dict] = []
        def _cluster_name(wh_key: str) -> str:
            if wh_key == "__default__":
                return "Ozon"
            # Если ключ уже похож на название (не число) — используем как есть
            if wh_key and not str(wh_key).isdigit():
                return wh_key
            return warehouse_names.get(wh_key) or str(wh_key)
        for wh_id in sorted(by_warehouse.keys(), key=lambda x: (x == "__default__", _cluster_name(x))):
            rows_data = by_warehouse[wh_id]
            name = _cluster_name(wh_id)
            rows = []
            for article in sorted(all_offer_ids):
                qty = rows_data.get(article, 0)
                rows.append({
                    "article": article,
                    "name": names_map.get(article, "—"),
                    "stock": qty,
                })
            # v4 не различает кластер/склад — один уровень; для единого формата: один склад в кластере
            clusters.append({"name": name, "rows": rows, "warehouses": [{"name": name, "rows": rows}]})
        logger.info(
            "Ozon stocks by_cluster: итого кластеров=%s, строк всего=%s"
            % (len(clusters), sum(len(c["rows"]) for c in clusters))
        )
        return clusters

    async def get_stocks(self) -> Dict[str, int]:
        """
        Получить общие остатки по артикулам продавца (offer_id).
        Возвращает словарь: offer_id -> суммарный остаток по всем складам.
        """
        clusters = await self.get_stocks_by_cluster()
        result: Dict[str, int] = {}
        for c in clusters:
            for row in c["rows"]:
                result[row["article"]] = result.get(row["article"], 0) + row["stock"]
        return result

    async def get_available_timeslots(
        self,
        cluster_id: int,
        date: str,
        supply_type: str = "CROSSDOCK",
    ) -> Dict:
        """
        Список доступных таймслотов и складов по кластеру и дате.
        Пробуем POST /v2/draft/timeslot/info (по доке Ozon). Эндпоинт может требовать draft_id —
        тогда возвращаем пустой результат; при 404/ошибке тоже пустой (фильтрация отключится).
        """
        url = f"{self.BASE_URL}/v2/draft/timeslot/info"
        body: Dict = {
            "date_from": date,
            "date_to": date,
        }
        if cluster_id:
            body["cluster_id"] = cluster_id
        if supply_type:
            body["supply_type"] = supply_type
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="catalog:draft_timeslot_info_by_cluster",
                )
                response.raise_for_status()
                data = response.json()
                result = data.get("result") or data
                timeslots = result.get("timeslots") or []
                warehouses = result.get("warehouses") or []
                logger.info(
                    "Ozon v2/draft/timeslot/info cluster_id={} date={}: timeslots={}, warehouses={}",
                    cluster_id, date, len(timeslots), len(warehouses),
                )
                return {
                    "timeslots": timeslots,
                    "warehouses": warehouses,
                    "cluster_id": result.get("cluster_id"),
                    "date": result.get("date"),
                }
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ozon v2/draft/timeslot/info HTTP {}: {}",
                e.response.status_code, (e.response.text or "")[:400],
            )
            return {"timeslots": [], "warehouses": []}
        except Exception as e:
            logger.warning("Ozon get_available_timeslots: {}", e, exc_info=True)
            return {"timeslots": [], "warehouses": []}

    async def get_draft_timeslots(
        self,
        draft_id: int,
        date_from: str,
        date_to: str,
        supply_type: str = "CROSSDOCK",
        selected_cluster_warehouses: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        Список доступных таймслотов для черновика.
        POST /v2/draft/timeslot/info — draft_id из /v2/draft/create/info.
        Параметры: date_from, date_to (YYYY-MM-DD), supply_type (CROSSDOCK, DIRECT, MULTI_CLUSTER),
        selected_cluster_warehouses — опционально [{ macrolocal_cluster_id, storage_warehouse_id? }].
        """
        url = f"{self.BASE_URL}/v2/draft/timeslot/info"
        body: Dict = {
            "date_from": date_from,
            "date_to": date_to,
            "draft_id": draft_id,
            "supply_type": supply_type,
        }
        if selected_cluster_warehouses is not None and len(selected_cluster_warehouses) > 0:
            body["selected_cluster_warehouses"] = selected_cluster_warehouses
        logger.info(
            "Ozon supply draft: POST /v2/draft/timeslot/info request draft_id={} date_from={} date_to={} supply_type={} selected_warehouses={}",
            draft_id,
            date_from,
            date_to,
            supply_type,
            len(selected_cluster_warehouses or []),
        )
        ts_pre = max(0.0, float(getattr(settings, "ozon_draft_timeslot_pre_delay_sec", 0.0) or 0.0))
        if ts_pre > 0:
            logger.info(
                "Ozon get_draft_timeslots: пауза {:.1f} с перед первым POST /v2/draft/timeslot/info (после цепочки draft)",
                ts_pre,
            )
            await asyncio.sleep(ts_pre)
        attempts: List[Dict] = []
        last_error: Optional[Dict] = None
        ts_http_timeout = float(
            getattr(settings, "ozon_draft_timeslot_http_timeout_sec", 600.0) or 600.0
        )
        for attempt_idx in range(1, 4):
            try:
                async with self._ozon_http(ts_http_timeout) as client:
                    response = await client.post(
                        url,
                        json=body,
                        headers=self.headers,
                        _ozon_op="supply_queue:draft_timeslot_info",
                    )
                raw_text = response.text or ""
                try:
                    data = response.json()
                except Exception as parse_err:
                    attempts.append({"attempt": attempt_idx, "status_code": response.status_code, "error": str(parse_err)})
                    last_error = {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw_text[:500], "attempts": attempts}
                    if attempt_idx < 3:
                        await asyncio.sleep(2)
                    continue
                attempts.append({"attempt": attempt_idx, "status_code": response.status_code})
                if response.status_code == 200:
                    result = data.get("result")
                    drop_off = (result or {}).get("drop_off_warehouse_timeslots") if isinstance(result, dict) else None
                    days = (drop_off.get("days") if isinstance(drop_off, dict) else None) or []
                    days_len = len(days) if isinstance(days, list) else 0
                    first_day_slots = len((days[0].get("timeslots") or [])) if days and len(days) > 0 and isinstance(days[0], dict) else 0
                    logger.info(
                        "Ozon supply draft: POST /v2/draft/timeslot/info success draft_id={} response_keys={} result_keys={} drop_off_keys={} days_count={} first_day_slots={}",
                        draft_id,
                        list(data.keys()),
                        list(result.keys()) if result else [],
                        list(drop_off.keys()) if drop_off else [],
                        days_len,
                        first_day_slots,
                    )
                    data["attempts"] = attempts
                    return data
                last_error = {
                    "_error": f"HTTP {response.status_code}",
                    "status_code": response.status_code,
                    "ozon_response": data if isinstance(data, dict) else raw_text[:500],
                    "attempts": attempts,
                }
            except Exception as e:
                attempts.append({"attempt": attempt_idx, "status_code": 0, "error": str(e)})
                last_error = {"_error": str(e), "status_code": 0, "ozon_response": None, "attempts": attempts}
            if attempt_idx < 3:
                await asyncio.sleep(2)
        return last_error or {
            "_error": "Не удалось получить таймслоты",
            "status_code": 0,
            "ozon_response": None,
            "attempts": attempts,
        }

    async def confirm_draft_supply(
        self,
        draft_id: int,
        from_in_timezone: str,
        to_in_timezone: str,
        selected_cluster_warehouses: List[Dict],
        supply_type: str = "CROSSDOCK",
    ) -> Dict:
        """
        Создать заявку на поставку по черновику. POST /v2/draft/supply/create (документация Ozon).
        Тело: draft_id, selected_cluster_warehouses [{ macrolocal_cluster_id [, storage_warehouse_id] }],
        timeslot { from_in_timezone, to_in_timezone }, supply_type (CROSSDOCK | DIRECT | MULTI_CLUSTER).
        storage_warehouse_id — только для типа DIRECT.
        При 429 — повторы с паузой (лимит per second у Ozon часто режет сразу после цепочки draft).
        """
        url = f"{self.BASE_URL}/v2/draft/supply/create"
        body = {
            "draft_id": draft_id,
            "selected_cluster_warehouses": selected_cluster_warehouses,
            "timeslot": {
                "from_in_timezone": from_in_timezone.strip(),
                "to_in_timezone": to_in_timezone.strip(),
            },
            "supply_type": supply_type.strip() or "CROSSDOCK",
        }
        pre_delay = max(0.0, float(getattr(settings, "ozon_confirm_supply_pre_delay_sec", 0.0) or 0.0))
        if pre_delay > 0:
            logger.info(
                "Ozon confirm_draft_supply: пауза {:.1f} с перед POST /v2/draft/supply/create (снижение 429 после цепочки draft)",
                pre_delay,
            )
            await asyncio.sleep(pre_delay)
        attempts: List[Dict] = []
        last_error: Optional[Dict] = None
        for attempt in range(3):
            try:
                async with self._ozon_http(30.0) as client:
                    response = await client.post(
                        url,
                        json=body,
                        headers=self.headers,
                        _ozon_op="supply_queue:confirm_draft_supply",
                    )
            except Exception as e:
                logger.warning("Ozon confirm_draft_supply: {}", e)
                attempts.append({"attempt": attempt + 1, "status_code": 0, "error": str(e)})
                last_error = {"_error": str(e), "status_code": 0, "ozon_response": None, "attempts": attempts}
                if attempt < 2:
                    await asyncio.sleep(2)
                continue

            raw_text = response.text or ""
            try:
                data = response.json()
            except Exception:
                data = {}
            attempts.append({"attempt": attempt + 1, "status_code": response.status_code})

            if response.status_code == 200:
                if isinstance(data, dict):
                    data["attempts"] = attempts
                return data
            code = response.status_code
            text = raw_text[:800]
            logger.warning("Ozon v2/draft/supply/create HTTP {} attempt={}/3: {}", code, attempt + 1, text[:200])
            last_error = {
                "_error": f"HTTP {code}",
                "status_code": code,
                "ozon_response": data if isinstance(data, dict) else text,
                "attempts": attempts,
            }
            if attempt < 2:
                await asyncio.sleep(2)

        return last_error or {
            "_error": "Не удалось создать заявку по черновику",
            "status_code": 0,
            "ozon_response": None,
            "attempts": attempts,
        }

    async def get_draft_supply_create_status(self, draft_id: int) -> Dict:
        """
        Получить статус создания заявки на поставку. POST /v2/draft/supply/create/status.
        Вызывать через несколько секунд после /v2/draft/supply/create. В ответе order_id — идентификатор заявки.
        """
        url = f"{self.BASE_URL}/v2/draft/supply/create/status"
        body = {"draft_id": draft_id}
        attempts: List[Dict] = []
        last_error: Optional[Dict] = None
        for attempt in range(3):
            try:
                async with self._ozon_http(15.0) as client:
                    response = await client.post(url, json=body, headers=self.headers)
            except Exception as e:
                logger.warning("Ozon get_draft_supply_create_status: {}", e)
                attempts.append({"attempt": attempt + 1, "status_code": 0, "error": str(e)})
                last_error = {"_error": str(e), "status_code": 0, "ozon_response": None, "attempts": attempts}
                if attempt < 2:
                    await asyncio.sleep(2)
                continue

            raw_text = response.text or ""
            try:
                data = response.json()
            except Exception:
                data = {}
            attempts.append({"attempt": attempt + 1, "status_code": response.status_code})
            if response.status_code == 200:
                if isinstance(data, dict):
                    data["attempts"] = attempts
                return data

            code = response.status_code
            text = raw_text[:500]
            logger.warning("Ozon v2/draft/supply/create/status HTTP {} attempt={}/3: {}", code, attempt + 1, text[:200])
            last_error = {"_error": f"HTTP {code}", "status_code": code, "ozon_response": data if isinstance(data, dict) else text, "attempts": attempts}
            if attempt < 2:
                await asyncio.sleep(2)

        return last_error or {"_error": "Не удалось получить статус создания заявки", "status_code": 0, "ozon_response": None, "attempts": attempts}

    async def cancel_supply_order(self, order_id: int) -> Dict:
        """
        Отменить заявку на поставку. POST /v1/supply-order/cancel.
        Тело: {"order_id": int}. В ответе operation_id — для проверки статуса отмены.
        """
        url = f"{self.BASE_URL}/v1/supply-order/cancel"
        body = {"order_id": order_id}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v1/supply-order/cancel HTTP {}: {}", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon cancel_supply_order: {}", e)
            raise

    async def get_supply_info(self, order_id: str) -> Dict:
        """
        Информация о заявке на поставку. POST /v3/supply-order/get.
        order_id — идентификатор заявки (8 цифр из create/status). В ответе result.items[].supply_id — 13 цифр (Идентификатор поставки в системе Ozon).
        """
        url = f"{self.BASE_URL}/v3/supply-order/get"
        try:
            order_id_int = int(order_id)
        except (TypeError, ValueError):
            return {"_error": "order_id must be numeric"}
        body = {"order_ids": [str(order_id_int)]}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:supply_order_get",
                )
                response.raise_for_status()
                data = response.json()
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v3/supply-order/get HTTP {}: {}", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon get_supply_info: {}", e)
            raise

    async def get_supply_info_many(self, order_ids: List[str]) -> Dict:
        """
        Информация о заявках на поставку пачкой. POST /v3/supply-order/get.
        По документации принимает до 50 order_ids за запрос.
        """
        url = f"{self.BASE_URL}/v3/supply-order/get"
        ids: List[int] = []
        for oid in order_ids or []:
            s = str(oid).strip()
            if not s:
                continue
            try:
                oi = int(s)
            except (TypeError, ValueError):
                continue
            ids.append(oi)
        ids = list(dict.fromkeys(ids))[:50]
        if not ids:
            return {"_error": "order_ids is empty"}
        body = {"order_ids": ids}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:supply_order_get_bulk",
                )
                response.raise_for_status()
                data = response.json()
                return data if isinstance(data, dict) else {"_error": "Invalid response shape"}
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v3/supply-order/get bulk HTTP {}: {}", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon get_supply_info_many: {}", e)
            raise

    async def list_supply_orders(
        self,
        states: Optional[List[str]] = None,
        last_id: Optional[str] = None,
        limit: int = 100,
        sort_by: str = "ORDER_CREATION",
        sort_dir: str = "DESC",
    ) -> Dict:
        """
        Список заявок на поставку. POST /v3/supply-order/list.

        Пример payload:
        {
          "filter": { "states": ["COMPLETED"] },
          "last_id": "null",
          "limit": 1,
          "sort_by": "ORDER_CREATION",
          "sort_dir": "DESC"
        }

        Возвращает dict с полями:
        - order_ids: список 8-значных идентификаторов
        - last_id: строка для пагинации
        """
        url = f"{self.BASE_URL}/v3/supply-order/list"

        payload: Dict = {
            "filter": {},
            # Ozon ожидает именно JSON `null`, а не строку "null".
            "last_id": None if last_id in (None, "", "null") else str(last_id),
            "limit": max(1, min(100, int(limit) if limit is not None else 100)),
            "sort_by": str(sort_by or "ORDER_CREATION").strip(),
            "sort_dir": str(sort_dir or "DESC").strip().upper(),
        }

        if states:
            payload["filter"]["states"] = [str(s).strip() for s in states if str(s).strip()]

        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers=self.headers,
                    _ozon_op="supply_queue:supply_order_list",
                )
                response.raise_for_status()
                data = response.json()
                return data if isinstance(data, dict) else {"_error": "Invalid response shape"}
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:800]
            logger.warning("Ozon v3/supply-order/list HTTP {}: {}", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon v3/supply-order/list error: {}", e, exc_info=True)
            return {"_error": "request_failed", "error": str(e)}

    async def get_supply_info_with_log(self, order_id: str) -> Dict:
        """
        То же, что get_supply_info, но возвращает структуру для лога: request (url, body), response (status, body)
        и извлечённый bundle_id из orders[].supplies[].bundle_id.
        Тело запроса: order_ids — массив чисел, как в доке Озон.
        """
        url = f"{self.BASE_URL}/v3/supply-order/get"
        try:
            order_id_int = int(order_id)
        except (TypeError, ValueError):
            return {
                "_error": "order_id must be numeric",
                "request": {"method": "POST", "url": url, "body": None},
                "response_status": None,
                "response_body": None,
                "bundle_id": None,
            }
        body = {"order_ids": [order_id_int]}
        request_info = {"method": "POST", "url": url, "body": body}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:supply_order_get_with_log",
                )
                try:
                    response_body = response.json()
                except Exception:
                    response_body = {"_raw": (response.text or "")[:2000]}
                bundle_id = None
                supply_id = None
                if response.status_code == 200 and isinstance(response_body, dict):
                    orders = response_body.get("result", {}).get("orders") or response_body.get("orders") or []
                    if isinstance(orders, list) and orders:
                        first_order = orders[0] if isinstance(orders[0], dict) else {}
                        supplies = first_order.get("supplies") or []
                        if isinstance(supplies, list) and supplies and isinstance(supplies[0], dict):
                            bundle_id = supplies[0].get("bundle_id")
                            supply_id = supplies[0].get("supply_id")
                return {
                    "request": request_info,
                    "response_status": response.status_code,
                    "response_body": response_body,
                    "bundle_id": bundle_id,
                    "supply_id": supply_id,
                    "_error": None if response.status_code == 200 else f"HTTP {response.status_code}",
                }
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            try:
                err_body = e.response.json()
            except Exception:
                err_body = (e.response.text or "")[:2000]
            return {
                "request": request_info,
                "response_status": code,
                "response_body": err_body,
                "bundle_id": None,
                "supply_id": None,
                "_error": f"HTTP {code}",
            }
        except Exception as e:
            logger.warning("Ozon get_supply_info_with_log: {}", e)
            return {
                "request": request_info,
                "response_status": None,
                "response_body": {"_error": str(e)},
                "bundle_id": None,
                "supply_id": None,
                "_error": str(e),
            }

    async def get_supply_order_bundle(
        self,
        bundle_ids: List[str],
        *,
        is_asc: bool = True,
        item_tags_calculation: Optional[Dict[str, Any]] = None,
        last_id: str = "",
        limit: int = 100,
        query: str = "",
        sort_field: str = "NAME",
    ) -> Dict:
        """
        Состав поставки/заявки по bundle_id. POST /v1/supply-order/bundle.
        Тело: bundle_ids (1–100), limit 1–100, is_asc, sort_field, query, last_id (пагинация),
        item_tags_calculation (dropoff_warehouse_id, storage_warehouse_ids до 10).
        """
        if not bundle_ids:
            return {}
        url = f"{self.BASE_URL}/v1/supply-order/bundle"
        ids = [str(b).strip() for b in bundle_ids if str(b).strip()][:100]
        if not ids:
            return {}
        lim = max(1, min(100, int(limit)))
        body: Dict[str, Any] = {
            "bundle_ids": ids,
            "is_asc": is_asc,
            "limit": lim,
            "query": query if query else "",
            "sort_field": sort_field,
        }
        if last_id:
            body["last_id"] = last_id
        if item_tags_calculation is not None:
            body["item_tags_calculation"] = item_tags_calculation
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:supply_order_bundle",
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v1/supply-order/bundle HTTP {}: {}", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon get_supply_order_bundle: {}", e)
            raise

    @staticmethod
    def _unwrap_supply_order_bundle_page(resp: Dict[str, Any]) -> tuple[list, Optional[bool], Optional[str]]:
        """items / has_next / last_id — на верхнем уровне или в result (как в других методах Ozon)."""
        if not isinstance(resp, dict):
            return [], None, None
        items = resp.get("items")
        has_next = resp.get("has_next")
        last_id = resp.get("last_id")
        r = resp.get("result")
        if isinstance(r, dict):
            if items is None:
                items = r.get("items")
            if has_next is None:
                has_next = r.get("has_next")
            if last_id is None:
                lid = r.get("last_id")
                last_id = lid
        if not isinstance(items, list):
            items = []
        ls = str(last_id).strip() if last_id is not None else None
        if has_next is None:
            has_next = False
        elif not isinstance(has_next, bool):
            has_next = bool(has_next)
        return items, has_next, ls or None

    async def get_supply_order_bundle_items_all_pages(
        self,
        bundle_id: str,
        item_tags_calculation: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Все позиции состава по одному bundle_id (цикл last_id / has_next). POST /v1/supply-order/bundle.
        Сначала с item_tags_calculation (если передан), при пустом ответе — повтор без него.
        """
        bid = str(bundle_id).strip()
        if not bid:
            return []

        tags_try: list[Optional[Dict[str, Any]]] = [item_tags_calculation]
        if item_tags_calculation is not None:
            tags_try.append(None)

        out: List[Dict[str, Any]] = []
        for tags in tags_try:
            out = []
            last_id = ""
            for _ in range(50):
                resp = await self.get_supply_order_bundle(
                    [bid],
                    item_tags_calculation=tags,
                    last_id=last_id,
                    limit=100,
                    is_asc=True,
                    sort_field="NAME",
                    query="",
                )
                if resp.get("_error"):
                    logger.warning("Ozon supply-order/bundle page: bundle_id={} tags={} err={}", bid, tags is not None, resp.get("_error"))
                    break
                raw, has_next, nxt = self._unwrap_supply_order_bundle_page(resp)
                for it in raw:
                    if not isinstance(it, dict):
                        continue
                    qty = it.get("quantity")
                    if qty is None:
                        qty = it.get("quant")
                    try:
                        qn = int(qty or 0)
                    except (TypeError, ValueError):
                        qn = 0
                    out.append(
                        {
                            "name": str(it.get("name") or ""),
                            "offer_id": str(it.get("offer_id") or ""),
                            "sku": it.get("sku"),
                            "quantity": qn,
                            "icon_path": str(it.get("icon_path") or ""),
                        }
                    )
                if not has_next:
                    break
                if not nxt or str(nxt).strip() == "":
                    break
                if str(nxt).strip() == str(last_id).strip() and last_id:
                    break
                last_id = str(nxt).strip()
            if out:
                break
        if not out:
            logger.info(
                "Ozon supply-order/bundle состав: bundle_id={} позиций=0 (все попытки, tags={})",
                bid,
                "да" if item_tags_calculation is not None else "нет",
            )
        else:
            logger.info(
                "Ozon supply-order/bundle состав: bundle_id={} позиций={}",
                bid,
                len(out),
            )
        return out

    async def get_supply_order_bundle_with_log(self, bundle_ids: List[str]) -> Dict:
        """
        То же, что get_supply_order_bundle, но возвращает request/response для лога и извлечённые
        items (sku, quantity), total_count из ответа.
        """
        if not bundle_ids:
            return {
                "request": {"method": "POST", "url": f"{self.BASE_URL}/v1/supply-order/bundle", "body": None},
                "response_status": None,
                "response_body": None,
                "items": [],
                "total_count": None,
                "_error": "bundle_ids пустой",
            }
        url = f"{self.BASE_URL}/v1/supply-order/bundle"
        ids = [str(b).strip() for b in bundle_ids if str(b).strip()][:100]
        if not ids:
            return {
                "request": {"method": "POST", "url": url, "body": None},
                "response_status": None,
                "response_body": None,
                "items": [],
                "total_count": None,
                "_error": "bundle_ids пустой после нормализации",
            }
        body = {"bundle_ids": ids, "limit": 100}
        request_info = {"method": "POST", "url": url, "body": body}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:supply_order_bundle_with_log",
                )
                try:
                    response_body = response.json()
                except Exception:
                    response_body = {"_raw": (response.text or "")[:2000]}
                items = []
                total_count = None
                if response.status_code == 200 and isinstance(response_body, dict):
                    total_count = response_body.get("total_count")
                    raw_items = response_body.get("items") or []
                    for it in raw_items if isinstance(raw_items, list) else []:
                        if not isinstance(it, dict):
                            continue
                        sku = it.get("sku")
                        qty = it.get("quantity") if it.get("quantity") is not None else it.get("quant")
                        items.append({"sku": sku, "quantity": qty})
                return {
                    "request": request_info,
                    "response_status": response.status_code,
                    "response_body": response_body,
                    "items": items,
                    "total_count": total_count,
                    "_error": None if response.status_code == 200 else f"HTTP {response.status_code}",
                }
        except httpx.HTTPStatusError as e:
            try:
                err_body = e.response.json()
            except Exception:
                err_body = (e.response.text or "")[:2000]
            return {
                "request": request_info,
                "response_status": e.response.status_code,
                "response_body": err_body,
                "items": [],
                "total_count": None,
                "_error": f"HTTP {e.response.status_code}",
            }
        except Exception as e:
            logger.warning("Ozon get_supply_order_bundle_with_log: {}", e)
            return {
                "request": request_info,
                "response_status": None,
                "response_body": {"_error": str(e)},
                "items": [],
                "total_count": None,
                "_error": str(e),
            }

    async def get_supply_order_cancel_status(self, operation_id: str) -> Dict:
        """
        Статус отмены заявки на поставку. POST /v1/supply-order/cancel/status.
        Тело: {"operation_id": str}. status: SUCCESS — отменена, IN_PROGRESS — в процессе, ERROR — ошибка.
        """
        url = f"{self.BASE_URL}/v1/supply-order/cancel/status"
        body = {"operation_id": operation_id}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v1/supply-order/cancel/status HTTP {}: {}", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon get_supply_order_cancel_status: {}", e)
            raise

    async def supply_order_content_update(
        self, order_id: int, supply_id: int, items: List[Dict]
    ) -> Dict:
        """
        Редактирование товарного состава заявки. POST /v1/supply-order/content/update.
        Полностью заменяет состав. items: [{"sku": int, "quantity": int}, ...], макс. 5000.
        order_id — идентификатор заявки (8 цифр), supply_id — идентификатор поставки (13 цифр).
        Возвращает operation_id при успехе или _error при ошибке.
        """
        url = f"{self.BASE_URL}/v1/supply-order/content/update"
        payload = []
        for i in items:
            if not i:
                continue
            qty = int(i.get("quantity") or i.get("quant") or 0)
            if qty <= 0:
                continue
            sku_val = int(i.get("sku", 0))
            if sku_val <= 0:
                continue
            # API валидирует SupplyOrderContentUpdateRequest_Item.Quant (value must be greater than 0)
            payload.append({"sku": sku_val, "quant": qty, "quantity": qty})
        if not payload:
            return {"_error": "EMPTY_CONTENT", "message": "Нет товаров с количеством > 0"}
        body = {"order_id": order_id, "supply_id": supply_id, "items": payload}
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                data = response.json() if response.content else {}
                if response.status_code != 200:
                    logger.warning(
                        "Ozon v1/supply-order/content/update HTTP {}: {}",
                        response.status_code,
                        (response.text or "")[:300],
                    )
                    return {
                        "_error": f"HTTP {response.status_code}",
                        "status_code": response.status_code,
                        "ozon_response": data,
                        "errors": data.get("errors") or [],
                    }
                if data.get("errors"):
                    return {"_error": "API_ERROR", "errors": data.get("errors"), "ozon_response": data}
                return {"operation_id": data.get("operation_id"), **data}
        except Exception as e:
            logger.warning("Ozon supply_order_content_update: {}", e)
            raise

    async def supply_order_content_update_status(self, operation_id: str) -> Dict:
        """
        Статус редактирования состава. POST /v1/supply-order/content/update/status.
        Тело: {"operation_id": str}. status: SUCCESS, IN_PROGRESS, ERROR.
        """
        url = f"{self.BASE_URL}/v1/supply-order/content/update/status"
        body = {"operation_id": operation_id}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning(
                "Ozon v1/supply-order/content/update/status HTTP {}: {}", code, text[:200]
            )
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon supply_order_content_update_status: {}", e)
            raise

    async def supply_order_content_update_validation(
        self, supply_id: int, new_bundle_id: str
    ) -> Dict:
        """
        Проверка нового товарного состава перед/после правок.
        POST /v1/supply-order/content/update/validation.
        Тело: {"supply_id": int, "new_bundle_id": str}.
        """
        url = f"{self.BASE_URL}/v1/supply-order/content/update/validation"
        body = {
            "supply_id": int(supply_id),
            "new_bundle_id": str(new_bundle_id or "").strip(),
        }
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning(
                "Ozon v1/supply-order/content/update/validation HTTP {}: {}",
                code,
                text[:200],
            )
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon supply_order_content_update_validation: {}", e)
            raise

    async def get_supply_delivery_methods(self) -> List[Dict]:
        """
        Список методов доставки (точек отгрузки) для заявок на поставку.
        GET /v1/supply/delivery-methods — может понадобиться delivery_method_id для создания черновика.
        Возвращает список [{ "id": int, "name": str, ... }, ...] или [] при ошибке.
        """
        url = f"{self.BASE_URL}/v1/supply/delivery-methods"
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                result = data.get("result") or data.get("delivery_methods") or data
                if isinstance(result, list):
                    return result
                items = result.get("delivery_methods") or result.get("methods") or result.get("items") or []
                return items if isinstance(items, list) else []
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ozon v1/supply/delivery-methods HTTP {}: {}",
                e.response.status_code, (e.response.text or "")[:300],
            )
            return []
        except Exception as e:
            logger.warning("Ozon get_supply_delivery_methods: {}", e)
            return []

    async def get_shipment_points(
        self,
        filter_by_api_type: Optional[List[str]] = None,
        query: Optional[str] = None,
        match: Optional[str] = None,
    ) -> Dict:
        """
        Поиск точек для отгрузки поставки (кросс-докинг и прямые поставки).
        POST /v1/warehouse/shipment/points (документация Ozon).
        filter_by_api_type: WAREHOUSE_TYPE_CROSSDOCK | WAREHOUSE_TYPE_DELIVERY_POINT |
          WAREHOUSE_TYPE_FULL_FILLMENT | WAREHOUSE_TYPE_SORTING_CENTER.
        query — поиск по части названия склада, match — по полному названию.
        """
        body: Dict = {}
        if filter_by_api_type:
            body["filter_by_api_type"] = filter_by_api_type
        if query is not None and str(query).strip():
            body["query"] = str(query).strip()
        if match is not None and str(match).strip():
            body["match"] = str(match).strip()
        if not body:
            body["filter_by_api_type"] = ["WAREHOUSE_TYPE_CROSSDOCK"]
        for path in ("/v1/warehouse/shipment/points", "/v2/warehouse/shipment/points"):
            url = f"{self.BASE_URL}{path}"
            try:
                async with self._ozon_http(30.0) as client:
                    response = await client.post(url, json=body, headers=self.headers)
                    response.raise_for_status()
                    return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning("Ozon {} HTTP 404, пробуем другой путь", path)
                    continue
                logger.warning(
                    "Ozon {} HTTP {}: {}",
                    path, e.response.status_code, (e.response.text or "")[:400],
                )
                raise
        # Оба пути вернули 404 — возвращаем пустой результат вместо исключения, чтобы кнопка в админке не падала
        logger.warning("Ozon get_shipment_points: v1 и v2 вернули 404, возвращаем заглушку")
        return {"addresses": [], "_note": "Эндпоинт /v1/warehouse/shipment/points вернул 404. Возможно, доступен в другом регионе API или переименован. Проверьте docs.ozon.ru."}

    async def get_supply_warehouses(self) -> List[Dict]:
        """
        Список складов продавца для поставок (seller_warehouse_id).
        GET /v1/supply/warehouses — склады, с которых продавец отгружает в Ozon.
        """
        url = f"{self.BASE_URL}/v1/supply/warehouses"
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                result = data.get("result") or data
                items = result.get("warehouses") or result.get("items") or (result if isinstance(result, list) else [])
                return items if isinstance(items, list) else []
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ozon v1/supply/warehouses HTTP {}: {}",
                e.response.status_code, (e.response.text or "")[:300],
            )
            return []
        except Exception as e:
            logger.warning("Ozon get_supply_warehouses: {}", e)
            return []

    async def create_crossdock_supply_draft(
        self,
        macrolocal_cluster_id: int,
        items: List[Dict],
        delivery_info: Dict,
        deletion_sku_mode: str = "PARTIAL",
    ) -> Dict:
        """
        Создать черновик заявки на поставку кросс-докингом.
        POST /v1/draft/crossdock/create

        Лимиты: 2 запроса/мин, 50/час, 500/день. Черновик доступен 30 минут.

        Args:
            macrolocal_cluster_id: ID макролокального кластера.
            items: Список позиций [{"sku": int, "quantity": int}, ...].
            delivery_info: Информация о доставке:
                {
                    "drop_off_warehouse": {"warehouse_id": int, "warehouse_type": "DELIVERY_POINT"},
                    "seller_warehouse_id": int,
                    "type": "DROPOFF"
                }
            deletion_sku_mode: "PARTIAL" или "FULL" (по умолчанию PARTIAL).

        Returns:
            Ответ API: может содержать clusters, errors, status и др.
        """
        url = f"{self.BASE_URL}/v1/draft/crossdock/create"
        payload = {
            "cluster_info": {
                "macrolocal_cluster_id": macrolocal_cluster_id,
                "items": [{"sku": int(it.get("sku", 0)), "quantity": int(it.get("quantity", 0))} for it in items],
            },
            "deletion_sku_mode": deletion_sku_mode if deletion_sku_mode in ("PARTIAL", "FULL") else "PARTIAL",
            "delivery_info": delivery_info,
        }
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Ozon API: лимит запросов на создание черновика поставки (429)")
            else:
                logger.warning("Ozon API create_crossdock_supply_draft: {} {}", e.response.status_code, (e.response.text or "")[:500])
            raise
        except Exception as e:
            logger.exception("Ozon API create_crossdock_supply_draft: %s", e)
            raise

    async def create_fbs_crossdock_draft(
        self,
        cluster_info: Dict,
        delivery_date: str,
        additional_cluster_id: Optional[int] = None,
        stock_type: str = "FIT",
    ) -> Dict:
        """
        Создать черновик заявки на поставку кросс-докингом (FBO).
        POST /v1/draft/crossdock/create — единственный используемый эндпоинт.
        Возвращает {"request": {method, url, body}, "data": ответ Ozon}, чтобы рядом с ответом показывать фактический запрос.

        cluster_info: из формы (items, crossdock_cluster_id, delivery_info с drop_off_warehouse_id, warehouse_type, seller_warehouse_id, type).
        delivery_date: дата поставки (YYYY-MM-DD).
        """
        url_draft = f"{self.BASE_URL}/v1/draft/crossdock/create"
        di = (cluster_info or {}).get("delivery_info") or {}
        drop_id = int(di.get("drop_off_warehouse_id") or 0)
        drop_type = (di.get("warehouse_type") or "DELIVERY_POINT").strip()
        macrolocal_cluster_id = (
            cluster_info.get("macrolocal_cluster_id")
            if isinstance(cluster_info, dict)
            else None
        )
        if macrolocal_cluster_id in (None, "", 0, "0"):
            # backward compatibility with older clients
            macrolocal_cluster_id = (cluster_info or {}).get("crossdock_cluster_id") if isinstance(cluster_info, dict) else None
        draft_payload = {
            "cluster_info": {
                "macrolocal_cluster_id": int(macrolocal_cluster_id or 0),
                "items": [{"sku": int(it.get("sku", 0)), "quantity": int(it.get("quantity", 0))} for it in (cluster_info.get("items") or [])],
            },
            "deletion_sku_mode": "PARTIAL",
            "delivery_info": {
                "drop_off_warehouse": {"warehouse_id": drop_id, "warehouse_type": drop_type},
                "seller_warehouse_id": int(di.get("seller_warehouse_id") or 0),
                "type": "DROPOFF",
            },
        }
        request_sent = {"method": "POST", "url": url_draft, "body": draft_payload}
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(
                    url_draft,
                    json=draft_payload,
                    headers=self.headers,
                    _ozon_op="supply_queue:draft_crossdock_create",
                )
                response.raise_for_status()
                data = response.json()
            draft_id = data.get("draft_id")
            if not draft_id and draft_id != 0:
                logger.info("Ozon draft/crossdock/create: ответ без draft_id, полный ответ: {}", data)
            elif draft_id == 0:
                logger.warning("Ozon draft/crossdock/create: draft_id=0 (черновик не создан), ответ: {}", data)
            else:
                logger.info("Ozon draft/crossdock/create: draft_id={}, keys={}", draft_id, list(data.keys()) if isinstance(data, dict) else None)
            return {"request": request_sent, "data": data}
        except httpx.HTTPStatusError as e:
            body_preview = (e.response.text or "")[:1500]
            if e.response.status_code == 429:
                logger.warning(
                    "Ozon supply draft: POST /v1/draft/crossdock/create HTTP 429 (лимит Ozon на создание черновика; в доке: 2/мин, 50/час, 500/сутки). {}",
                    _ozon_response_log_line(e.response),
                )
                return {
                    "request": request_sent,
                    "data": {},
                    "_error": "Превышен лимит Ozon (429) на создание черновика (дока: 2/мин, 50/час, 500/сутки). Подождите 1–2 минуты; между вызовами crossdock/create держите интервал не реже ~72 с (OZON_DRAFT_CREATE_MIN_INTERVAL_SEC).",
                    "status_code": 429,
                    "response_text": body_preview,
                }
            logger.warning(
                "Ozon supply draft: POST /v1/draft/crossdock/create HTTP {}: {}",
                e.response.status_code,
                _ozon_response_log_line(e.response),
            )
            return {
                "request": request_sent,
                "data": {},
                "_error": f"Ошибка Ozon при создании черновика (HTTP {e.response.status_code}).",
                "status_code": e.response.status_code,
                "response_text": body_preview,
            }
        except Exception as e:
            logger.warning("Ozon create_fbs_crossdock_draft: {}", e)
            raise

    async def create_crossdock_draft_raw(self, body: Dict) -> Dict:
        """
        Создать черновик кроссдокинга, отправив тело как есть.
        POST /v1/draft/crossdock/create. body — полное тело (cluster_info, deletion_sku_mode, delivery_info).
        Возвращает ответ Ozon (draft_id, errors, ...).
        """
        url = f"{self.BASE_URL}/v1/draft/crossdock/create"
        async with self._ozon_http(30.0) as client:
            response = await client.post(
                url,
                json=body,
                headers=self.headers,
                _ozon_op="slots_scan:draft_crossdock_create",
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                body_preview = (e.response.text or "")[:1000]
                logger.warning(
                    "Ozon v1/draft/crossdock/create HTTP {}: {}",
                    e.response.status_code,
                    body_preview or "(empty)",
                )
                raise
            return response.json()

    async def get_draft_info(self, draft_id: str) -> Dict:
        """
        Получить информацию о черновике поставки.
        POST /v2/draft/create/info — тело {"draft_id": int} (v2 ожидает draft_id > 0).
        При 4xx/499 не бросаем исключение, возвращаем dict с _error и телом ответа.
        """
        url = f"{self.BASE_URL}/v2/draft/create/info"
        draft_id = (draft_id or "").strip()
        if not draft_id:
            return {"_error": "draft_id обязателен", "ozon_response": None}
        try:
            draft_id_int = int(draft_id)
        except (TypeError, ValueError):
            return {"_error": "draft_id должен быть числом", "ozon_response": None, "draft_id": draft_id}
        if draft_id_int <= 0:
            return {"_error": "draft_id должен быть > 0", "ozon_response": None, "draft_id": draft_id_int}
        body = {"draft_id": draft_id_int}
        attempts: List[Dict] = []
        last_error: Optional[Dict] = None
        for attempt_idx in range(1, 4):
            try:
                async with self._ozon_http(15.0) as client:
                    response = await client.post(
                        url,
                        json=body,
                        headers=self.headers,
                        _ozon_op="supply_queue:draft_create_info",
                    )
                raw_text = (response.text or "")[:800]
                try:
                    data = response.json()
                except Exception:
                    data = {}
                attempts.append({"attempt": attempt_idx, "status_code": response.status_code})
                if response.status_code == 200:
                    if isinstance(data, dict):
                        data["attempts"] = attempts
                    return data
                last_error = {
                    "_error": f"HTTP {response.status_code}",
                    "status_code": response.status_code,
                    "ozon_response": data if isinstance(data, dict) and data else raw_text,
                    "draft_id": draft_id_int,
                    "attempts": attempts,
                }
            except Exception as e:
                attempts.append({"attempt": attempt_idx, "status_code": 0, "error": str(e)})
                last_error = {
                    "_error": str(e),
                    "status_code": 0,
                    "ozon_response": None,
                    "draft_id": draft_id_int,
                    "attempts": attempts,
                }
            if attempt_idx < 3:
                await asyncio.sleep(2)
        return last_error or {
            "_error": "Не удалось получить статус черновика",
            "status_code": 0,
            "ozon_response": None,
            "draft_id": draft_id_int,
            "attempts": attempts,
        }

    async def set_cargo_places(
        self,
        supply_id: int,
        cargoes: List[Dict],
        delete_current_version: bool = False,
    ) -> Dict:
        """
        Установка грузомест. POST /v1/cargoes/create.
        Документация: supply_id (int64), cargoes — массив [{ key (str), value: { items: [{ barcode, quantity, offer_id? }], type: "BOX"|"PALLET" } }].
        Не более 40 палет или 30 коробок.
        """
        url = f"{self.BASE_URL}/v1/cargoes/create"
        body: Dict = {
            "supply_id": supply_id,
            "cargoes": cargoes,
            "delete_current_version": delete_current_version,
        }
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                raw = (response.text or "").strip()
                if response.status_code in (200, 201, 204) and not raw:
                    return {}
                try:
                    data = response.json()
                except Exception as parse_err:
                    logger.warning(
                        "Ozon v1/cargoes/create JSON parse failed: "
                        + str(parse_err)
                        + " status="
                        + str(response.status_code)
                        + " body_preview="
                        + (raw[:400] if raw else "(empty)")
                    )
                    if response.status_code in (200, 201, 204):
                        return {}
                    return {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw[:500]}
                response.raise_for_status()
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:800]
            logger.warning("Ozon v1/cargoes/create HTTP %s: %s", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon set_cargo_places: %s", e)
            raise

    async def get_cargoes_create_info(self, operation_id: str) -> Dict:
        """
        Получить информацию по установке грузомест. POST /v2/cargoes/create/info.
        Тело: {"operation_id": "string"}. В result: status (SUCCESS, IN_PROGRESS, FAILED), cargoes.
        """
        url = f"{self.BASE_URL}/v2/cargoes/create/info"
        body = {"operation_id": str(operation_id).strip()}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                raw = (response.text or "").strip()
                try:
                    data = response.json()
                except Exception as parse_err:
                    logger.warning(
                        "Ozon v2/cargoes/create/info JSON parse failed: %s status=%s",
                        parse_err, response.status_code,
                    )
                    return {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw[:500]}
                response.raise_for_status()
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v2/cargoes/create/info HTTP %s: %s", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon get_cargoes_create_info: %s", e)
            raise

    async def get_cargoes(self, supply_id: int) -> Dict:
        """
        Получить информацию о грузоместах поставки. POST /v1/cargoes/get.
        Тело: {"supply_ids": [int64]}. supply_id — идентификатор поставки (13 цифр, posting_number).
        """
        url = f"{self.BASE_URL}/v1/cargoes/get"
        body = {"supply_ids": [int(supply_id)]}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:cargoes_get",
                )
                raw = (response.text or "").strip()
                try:
                    data = response.json()
                except Exception as parse_err:
                    logger.warning(
                        "Ozon v1/cargoes/get JSON parse failed: %s status=%s",
                        parse_err, response.status_code,
                    )
                    return {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw[:500]}
                response.raise_for_status()
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v1/cargoes/get HTTP %s: %s", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon get_cargoes: %s", e)
            raise

    async def get_cargoes_by_supply_ids(self, supply_ids: List[str]) -> Dict:
        """
        POST /v1/cargoes/get — массив supply_ids (строки, док. Ozon), до 100 id за запрос.
        Возвращает { "supply": [ { "supply_id", "cargoes": [...] }, ... ], "_error": None }.
        """
        ids = [str(s).strip() for s in supply_ids if s and str(s).strip()]
        if not ids:
            return {"supply": [], "_error": None}
        url = f"{self.BASE_URL}/v1/cargoes/get"
        body = {"supply_ids": ids}
        try:
            async with self._ozon_http(45.0) as client:
                response = await client.post(
                    url,
                    json=body,
                    headers=self.headers,
                    _ozon_op="supply_queue:cargoes_get_batch",
                )
                try:
                    data = response.json()
                except Exception as e:
                    logger.warning("Ozon v1/cargoes/get batch: не JSON: {}", e)
                    return {
                        "_error": "invalid_json",
                        "supply": [],
                        "status_code": response.status_code,
                    }
                response.raise_for_status()
                if not isinstance(data, dict):
                    return {"_error": "bad_response", "supply": []}
                supply_list = data.get("supply")
                if supply_list is None and isinstance(data.get("result"), dict):
                    supply_list = data["result"].get("supply")
                if not isinstance(supply_list, list):
                    supply_list = []
                return {"supply": supply_list, "_error": None}
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v1/cargoes/get batch HTTP {}: {}", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {
                "_error": f"HTTP {code}",
                "status_code": code,
                "supply": [],
                "ozon_response": err_body,
            }
        except Exception as e:
            logger.warning("Ozon get_cargoes_by_supply_ids: {}", e, exc_info=True)
            return {"_error": str(e), "supply": []}

    async def get_cargoes_with_log(self, supply_ids: List[str]) -> Dict:
        """
        Получить информацию о грузоместах. POST /v1/cargoes/get.
        Тело: {"supply_ids": ["string"]}. Возвращает request/response для лога и извлечённый supply (массив с cargoes).
        """
        if not supply_ids:
            return {
                "request": {"method": "POST", "url": f"{self.BASE_URL}/v1/cargoes/get", "body": None},
                "response_status": None,
                "response_body": None,
                "supply": [],
                "_error": "supply_ids пустой",
            }
        url = f"{self.BASE_URL}/v1/cargoes/get"
        ids = [str(s).strip() for s in supply_ids if str(s).strip()]
        if not ids:
            return {
                "request": {"method": "POST", "url": url, "body": None},
                "response_status": None,
                "response_body": None,
                "supply": [],
                "_error": "supply_ids пустой после нормализации",
            }
        body = {"supply_ids": ids}
        request_info = {"method": "POST", "url": url, "body": body}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                try:
                    response_body = response.json()
                except Exception:
                    response_body = {"_raw": (response.text or "")[:2000]}
                supply_list = []
                if response.status_code == 200 and isinstance(response_body, dict):
                    supply_list = response_body.get("supply") or response_body.get("result", {}).get("supply") or []
                    if not isinstance(supply_list, list):
                        supply_list = []
                return {
                    "request": request_info,
                    "response_status": response.status_code,
                    "response_body": response_body,
                    "supply": supply_list,
                    "_error": None if response.status_code == 200 else f"HTTP {response.status_code}",
                }
        except httpx.HTTPStatusError as e:
            try:
                err_body = e.response.json()
            except Exception:
                err_body = (e.response.text or "")[:2000]
            return {
                "request": request_info,
                "response_status": e.response.status_code,
                "response_body": err_body,
                "supply": [],
                "_error": f"HTTP {e.response.status_code}",
            }
        except Exception as e:
            logger.warning("Ozon get_cargoes_with_log: {}", e)
            return {
                "request": request_info,
                "response_status": None,
                "response_body": {"_error": str(e)},
                "supply": [],
                "_error": str(e),
            }

    async def delete_cargoes(self, supply_id: int, cargo_ids: List[int]) -> Dict:
        """
        Удаление грузомест. POST /v1/cargoes/delete.

        Тело: {"supply_id": int64, "cargo_ids": [int64, ...]}.
        """
        url = f"{self.BASE_URL}/v1/cargoes/delete"
        body: Dict = {
            "supply_id": int(supply_id),
            "cargo_ids": [int(cid) for cid in cargo_ids],
        }
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                raw = (response.text or "").strip()
                if response.status_code in (200, 201, 204) and not raw:
                    return {}
                try:
                    data = response.json()
                except Exception as parse_err:
                    logger.warning(
                        "Ozon v1/cargoes/delete JSON parse failed: "
                        + str(parse_err)
                        + " status="
                        + str(response.status_code)
                        + " body_preview="
                        + (raw[:400] if raw else "(empty)")
                    )
                    if response.status_code in (200, 201, 204):
                        return {}
                    return {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw[:500]}
                response.raise_for_status()
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:800]
            logger.warning("Ozon v1/cargoes/delete HTTP %s: %s", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon delete_cargoes: %s", e)
            raise

    async def get_cargoes_delete_status(self, operation_id: str) -> Dict:
        """
        Получить статус удаления грузомест. POST /v1/cargoes/delete/status.

        Тело: {"operation_id": "string"}.
        """
        url = f"{self.BASE_URL}/v1/cargoes/delete/status"
        body = {"operation_id": str(operation_id).strip()}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                raw = (response.text or "").strip()
                try:
                    data = response.json()
                except Exception as parse_err:
                    logger.warning(
                        "Ozon v1/cargoes/delete/status JSON parse failed: %s status=%s",
                        parse_err, response.status_code,
                    )
                    return {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw[:500]}
                response.raise_for_status()
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v1/cargoes/delete/status HTTP %s: %s", code, text[:200])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            return {"_error": f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon get_cargoes_delete_status: %s", e)
            raise

    @staticmethod
    def _parse_ozon_error_message(data: Optional[Dict], raw: str) -> str:
        """Извлечь читаемое сообщение об ошибке из ответа Ozon."""
        if isinstance(data, dict):
            if data.get("message"):
                return str(data["message"]).strip()
            if data.get("error"):
                return str(data["error"]).strip()
            errs = data.get("errors")
            if isinstance(errs, list) and errs:
                parts = []
                for e in errs[:3]:
                    if isinstance(e, dict) and (e.get("message") or e.get("code")):
                        parts.append(e.get("message") or e.get("code"))
                    elif isinstance(e, str):
                        parts.append(e)
                if parts:
                    return "; ".join(str(p) for p in parts)
            if data.get("code"):
                return str(data["code"]).strip()
        if isinstance(raw, str) and len(raw) < 300 and raw.strip():
            return raw.strip()
        return ""

    async def cargoes_label_create(self, supply_id: int, cargo_ids: List[int]) -> Dict:
        """
        Создать задание на формирование этикеток грузомест. POST /v1/cargoes-label/create.
        Тело: supply_id (идентификатор поставки в Ozon), cargo_ids — массив cargo_id из /v1/cargoes/get.
        Ответ: operation_id для проверки статуса через cargoes_label_get.
        Ошибки: 400 (неверный запрос), 429 (лимит), 5xx — возвращаем _error и ozon_response.
        """
        url = f"{self.BASE_URL}/v1/cargoes-label/create"
        try:
            sid = int(supply_id)
        except (TypeError, ValueError):
            return {"_error": "supply_id должен быть числом", "status_code": 0, "ozon_response": None}
        cargoes_list = [{"cargo_id": int(cid)} for cid in cargo_ids if cid is not None]
        if not cargoes_list:
            return {"_error": "cargoes не может быть пустым", "status_code": 0, "ozon_response": None}
        body = {
            "cargoes": cargoes_list,
            "supply_id": sid,
        }
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                raw = (response.text or "").strip()
                try:
                    data = response.json()
                except Exception as parse_err:
                    logger.warning(
                        "Ozon v1/cargoes-label/create JSON parse failed: %s status=%s body=%s",
                        parse_err, response.status_code, raw[:300],
                    )
                    return {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw[:500]}
                if response.status_code != 200:
                    err_msg = self._parse_ozon_error_message(data, raw)
                    logger.warning("Ozon v1/cargoes-label/create HTTP {}: {}", response.status_code, err_msg or raw[:200])
                    return {"_error": err_msg or f"HTTP {response.status_code}", "status_code": response.status_code, "ozon_response": data if isinstance(data, dict) else raw}
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            err_msg = self._parse_ozon_error_message(err_body if isinstance(err_body, dict) else None, text)
            logger.warning("Ozon v1/cargoes-label/create HTTP {}: {}", code, err_msg or text)
            return {"_error": err_msg or f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon cargoes_label_create: %s", e)
            raise

    async def cargoes_label_get(self, operation_id: str) -> Dict:
        """
        Получить статус создания этикеток и идентификатор файла. POST /v1/cargoes-label/get.
        Тело: {"operation_id": "string"}.
        """
        url = f"{self.BASE_URL}/v1/cargoes-label/get"
        body = {"operation_id": str(operation_id).strip()}
        try:
            async with self._ozon_http(15.0) as client:
                response = await client.post(url, json=body, headers=self.headers)
                raw = (response.text or "").strip()
                try:
                    data = response.json()
                except Exception as parse_err:
                    logger.warning(
                        "Ozon v1/cargoes-label/get JSON parse failed: {} status={}",
                        parse_err, response.status_code,
                    )
                    return {"_error": "Invalid JSON", "status_code": response.status_code, "ozon_response": raw[:500]}
                response.raise_for_status()
                return data
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            text = (e.response.text or "")[:500]
            logger.warning("Ozon v1/cargoes-label/get HTTP {}: {}", code, text[:300])
            try:
                err_body = e.response.json()
            except Exception:
                err_body = text
            err_msg = self._parse_ozon_error_message(err_body if isinstance(err_body, dict) else None, text)
            return {"_error": err_msg or f"HTTP {code}", "status_code": code, "ozon_response": err_body}
        except Exception as e:
            logger.warning("Ozon cargoes_label_get: {}", e)
            raise

    async def cargoes_label_file(self, file_guid: str) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Получить PDF с этикетками грузовых мест. GET /v1/cargoes-label/file/{file_guid}.
        Возвращает (bytes PDF или None, ошибка или None).
        """
        url = f"{self.BASE_URL}/v1/cargoes-label/file/{file_guid}"
        try:
            async with self._ozon_http(30.0) as client:
                response = await client.get(url, headers=self.headers)
                if response.status_code != 200:
                    text = (response.text or "")[:500]
                    logger.warning("Ozon v1/cargoes-label/file HTTP %s: %s", response.status_code, text[:200])
                    return None, f"HTTP {response.status_code}"
                return response.content, None
        except Exception as e:
            logger.warning("Ozon cargoes_label_file: %s", e)
            return None, str(e)
