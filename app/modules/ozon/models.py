"""
Модели данных Ozon
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class OzonOrder(BaseModel):
    """Модель заказа Ozon"""
    posting_number: str
    order_id: int
    order_number: str
    status: str
    delivery_method: Optional[dict] = None
    created_at: datetime
    in_process_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    products: list = []
    analytics_data: Optional[dict] = None
    financial_data: Optional[dict] = None
    
    @property
    def amount(self) -> float:
        """Получить сумму заказа"""
        if self.financial_data and "products" in self.financial_data:
            total = sum(
                p.get("price", 0) * p.get("quantity", 0)
                for p in self.financial_data["products"]
            )
            return total
        return 0.0
    
    @classmethod
    def from_api_response(cls, data: dict) -> "OzonOrder":
        """Создать объект из ответа API"""
        return cls(**data)
