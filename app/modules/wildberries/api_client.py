"""
Клиент для работы с Wildberries API.
Даты в API передаются в московском времени (МСК, UTC+3).
"""
import json
import httpx
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from dateutil import tz as dateutil_tz
from loguru import logger
from app.config import settings

MSK = dateutil_tz.gettz("Europe/Moscow")


class WildberriesAPIClient:
    """Клиент для работы с Wildberries API"""
    
    BASE_URL = "https://statistics-api.wildberries.ru"
    CONTENT_BASE_URL = "https://content-api.wildberries.ru"
    
    def __init__(self):
        self.api_key = settings.wb_api_key
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
    
    async def get_orders(
        self,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[Dict]:
        """
        Получить список заказов. Параметры dateFrom/dateTo в API фильтруют по lastChangeDate
        (дата последнего изменения заказа), а не по дате создания. Поле date в ответе — дата
        создания. Для статистики «продано за период» фильтруйте результат по полю date в коде.
        """
        url = f"{self.BASE_URL}/api/v1/supplier/orders"
        
        # По умолчанию берем заказы за последние 7 дней
        if not date_from:
            date_from = datetime.now(MSK) - timedelta(days=7)
        if not date_to:
            date_to = datetime.now(MSK)
        # WB API принимает даты в московском времени (МСК)
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=MSK)
        else:
            date_from = date_from.astimezone(MSK)
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=MSK)
        else:
            date_to = date_to.astimezone(MSK)
        # Формат для WB: дата без таймзоны (интерпретируется как МСК по документации)
        def _msk_iso(dt: datetime) -> str:
            if dt.tzinfo:
                dt = dt.astimezone(MSK)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        # dateTo + 1 мин: API может трактовать dateTo как строго меньше
        date_to_ceiling = date_to + timedelta(minutes=1)
        all_orders: List[Dict] = []
        max_pages = 100
        # Без flag — API фильтрует по lastChangeDate (дата изменения заказа). flag=1 может давать пустой результат.
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                for page_num in range(max_pages):
                    params = {
                        "dateFrom": _msk_iso(date_from),
                        "dateTo": _msk_iso(date_to_ceiling),
                        "limit": limit,
                    }
                    response = await client.get(url, params=params, headers=self.headers)
                    response.raise_for_status()
                    orders = response.json()
                    # Лог структуры ответа API (первая порция)
                    if page_num == 0 and isinstance(orders, list) and orders:
                        first = orders[0]
                        keys = list(first.keys()) if isinstance(first, dict) else []
                        logger.info("WB orders API: ключи первой записи: {}", keys)
                        sample = json.dumps(first, ensure_ascii=False, indent=2, default=str)
                        logger.info("WB orders API: первая запись (образец):\n{}", sample)
                    if not isinstance(orders, list) or not orders:
                        if page_num == 0 and len(all_orders) == 0:
                            logger.warning(
                                "WB orders: пустой ответ. Параметры: dateFrom={}, dateTo={}. "
                                "Проверьте: 1) период (00:00–сейчас МСК), 2) задержку обновления Statistics API (до 1–2 ч)",
                                params["dateFrom"],
                                params["dateTo"],
                            )
                        break
                    all_orders.extend(orders)
                    logger.debug(f"WB orders: страница {page_num + 1}, получено {len(orders)} строк, всего {len(all_orders)}")
                    if len(orders) < limit:
                        break
                    next_from = orders[-1].get("lastChangeDate") or orders[-1].get("date")
                    if not next_from:
                        break
                    try:
                        s = str(next_from).strip()
                        if "T" in s:
                            s = s[:19] if len(s) > 19 else s
                            date_from = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=MSK)
                        else:
                            break
                    except (ValueError, TypeError):
                        break
                logger.info("WB orders: всего получено {} записей", len(all_orders))
                # По каждому заказу: артикул, время заказа, куда направляется
                for o in all_orders:
                    art = (o.get("supplierArticle") or o.get("supplier_article") or "").strip() or "—"
                    dt = (o.get("date") or o.get("lastChangeDate") or "").strip() or "—"
                    oblast = (o.get("oblast") or "").strip()
                    region = (o.get("region") or "").strip()
                    address = (o.get("address") or "").strip()
                    direction = ", ".join(x for x in (oblast, region) if x) or address or "—"
                    logger.info("WB заказ: артикул={}, время={}, направление={}", art, dt, direction)
                return all_orders
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.error("Ошибка авторизации Wildberries: токен API недействителен или отозван")
                logger.error("Проверьте правильность WB_API_KEY в файле .env")
                logger.error("Получите новый токен в личном кабинете Wildberries: Настройки → Доступ к API")
            else:
                logger.error(f"Ошибка при получении заказов Wildberries: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при работе с Wildberries API: {e}")
            raise
    
    async def get_sales(
        self,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """
        Получить список продаж
        
        Args:
            date_from: Дата начала периода
            date_to: Дата окончания периода
            limit: Максимальное количество продаж
        
        Returns:
            Список продаж
        """
        url = f"{self.BASE_URL}/api/v1/supplier/sales"
        
        if not date_from:
            date_from = datetime.now(MSK) - timedelta(days=7)
        if not date_to:
            date_to = datetime.now(MSK)
        
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dateTo": date_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": limit
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params, headers=self.headers)
                response.raise_for_status()
                sales = response.json()
                
                if isinstance(sales, list):
                    return sales
                return []
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Ошибка при получении продаж Wildberries: {e.response.status_code}")
            return []
        except Exception as e:
            logger.error(f"Неожиданная ошибка при получении продаж Wildberries: {e}")
            return []

    async def get_report_detail_by_period(
        self,
        date_from: datetime,
        date_to: datetime,
    ) -> List[Dict]:
        """
        Детализация отчёта о реализации за период (выплаты/финансы).
        GET /api/v5/supplier/reportDetailByPeriod.
        Лимит: 1 запрос в минуту, макс. 30 дней за запрос.
        """
        url = f"{self.BASE_URL}/api/v5/supplier/reportDetailByPeriod"
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=MSK)
        else:
            date_from = date_from.astimezone(MSK)
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=MSK)
        else:
            date_to = date_to.astimezone(MSK)

        def _msk_iso(dt: datetime) -> str:
            return dt.strftime("%Y-%m-%dT%H:%M:%S")

        params = {"dateFrom": _msk_iso(date_from), "dateTo": _msk_iso(date_to)}
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.get(url, params=params, headers=self.headers)
                response.raise_for_status()
                rows = response.json()
                if isinstance(rows, list) and rows:
                    logger.info("WB reportDetailByPeriod: получено %s записей, первая: %s", len(rows), json.dumps(rows[0], ensure_ascii=False, default=str)[:400])
                return rows if isinstance(rows, list) else []
        except httpx.HTTPStatusError as e:
            logger.warning("WB reportDetailByPeriod: %s - %s", e.response.status_code, (e.response.text or "")[:300])
            return []
        except Exception as e:
            logger.warning("WB reportDetailByPeriod: %s", e)
            return []

    async def get_product_names_by_nmids(self, nmids: List[int]) -> Dict[int, str]:
        """
        Получить названия товаров по nmId через Content API.
        POST /content/v2/get/cards/list с пагинацией, затем отбор по nmids.
        Возвращает словарь nmId -> name.
        """
        if not nmids:
            return {}
        nmid_set = set(nmids)
        result_map: Dict[int, str] = {}
        url = f"{self.CONTENT_BASE_URL}/content/v2/get/cards/list"
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                cursor = None
                while True:
                    payload = {"limit": 1000}
                    if cursor is not None:
                        payload["cursor"] = cursor
                    response = await client.post(url, json=payload, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    cursor = data.get("cursor")
                    cards = data.get("cards") or []
                    for card in cards:
                        nmid = card.get("nmID") or card.get("nmId") or card.get("nm_id")
                        if nmid is None:
                            continue
                        nmid = int(nmid)
                        if nmid not in nmid_set:
                            continue
                        name = (
                            (card.get("title") or card.get("name") or "").strip()
                            or "—"
                        )
                        result_map[nmid] = name
                    if not cursor or not cards:
                        break
            logger.info(f"WB Content API: загружены наименования для {len(result_map)} из {len(nmids)} nmId")
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Ошибка WB Content API (наименования): %s — %s",
                e.response.status_code,
                (e.response.text or "")[:300],
            )
        except Exception as e:
            logger.warning(f"Ошибка при получении наименований WB: {e}")
        return result_map

    async def get_all_articles_from_cards(self) -> Tuple[Dict[str, int], Dict[int, str]]:
        """
        Получить все артикулы (vendorCode) из каталога карточек WB.
        Возвращает (article -> nmId, nmId -> name) для всех карточек.
        """
        article_to_nmid: Dict[str, int] = {}
        nmid_to_name: Dict[int, str] = {}
        url = f"{self.CONTENT_BASE_URL}/content/v2/get/cards/list"
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                cursor = None
                while True:
                    payload = {"limit": 1000}
                    if cursor is not None:
                        payload["cursor"] = cursor
                    response = await client.post(url, json=payload, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    cursor = data.get("cursor")
                    cards = data.get("cards") or []
                    for card in cards:
                        nmid = card.get("nmID") or card.get("nmId") or card.get("nm_id")
                        if nmid is None:
                            continue
                        nmid = int(nmid)
                        art = (
                            (card.get("vendorCode") or card.get("vendor_code") or "")
                            .strip()
                        )
                        if not art:
                            continue
                        key = " ".join(art.split())
                        if key not in article_to_nmid:
                            article_to_nmid[key] = nmid
                        name = (
                            (card.get("title") or card.get("name") or "").strip()
                            or "—"
                        )
                        nmid_to_name[nmid] = name
                    if not cursor or not cards:
                        break
            logger.info(
                f"WB Content API: загружено артикулов из карточек: {len(article_to_nmid)}"
            )
        except Exception as e:
            logger.warning(f"Ошибка при получении списка артикулов WB: {e}")
        return article_to_nmid, nmid_to_name

    async def get_stocks_with_nmids(self) -> Tuple[Dict[str, int], Dict[str, int]]:
        """
        Остатки по артикулам + маппинг артикул -> nmId (для последующей подстановки наименований).
        Возвращает (article -> qty, article -> nmId).
        """
        url = f"{self.BASE_URL}/api/v1/supplier/stocks"
        date_from = "2019-01-01T00:00:00+03:00"
        result_qty: Dict[str, int] = {}
        result_nmid: Dict[str, int] = {}
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                while True:
                    params = {"dateFrom": date_from}
                    response = await client.get(url, params=params, headers=self.headers)
                    response.raise_for_status()
                    rows = response.json()
                    if not isinstance(rows, list):
                        rows = []
                    for row in rows:
                        try:
                            art = (
                                row.get("supplierArticle")
                                or row.get("supplier_article")
                                or ""
                            )
                            if not art:
                                continue
                            art = str(art).strip()
                            key = " ".join(art.split())
                            qty = 0
                            if row.get("quantity") is not None:
                                qty = int(row.get("quantity", 0) or 0)
                            if qty < 0:
                                qty = 0
                            result_qty[key] = result_qty.get(key, 0) + qty
                            nmid = row.get("nmId") or row.get("nmID") or row.get("nm_id")
                            if nmid is not None and key not in result_nmid:
                                result_nmid[key] = int(nmid)
                        except (TypeError, ValueError, KeyError):
                            continue
                    if not rows:
                        break
                    date_from = (rows[-1].get("lastChangeDate") or "").strip()
                    if not date_from:
                        break
        except Exception as e:
            logger.warning(f"Ошибка при получении остатков WB (with nmids): {e}")
        return result_qty, result_nmid

    async def get_stocks(self) -> Dict[str, int]:
        """
        Получить общие остатки по артикулам продавца (supplierArticle).
        По тому же принципу, что Ozon: пагинация до конца, разбор полей с fallback, сумма по артикулу.
        Сумма по всем складам. Возвращает словарь: артикул -> суммарный остаток.
        """
        qty_dict, _ = await self.get_stocks_with_nmids()
        logger.info(f"WB API: загружены остатки по {len(qty_dict)} артикулам")
        return qty_dict
