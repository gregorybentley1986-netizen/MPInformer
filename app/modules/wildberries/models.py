"""
Модели данных Wildberries
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class WildberriesOrder(BaseModel):
    """Модель заказа Wildberries"""
    date: datetime
    lastChangeDate: datetime
    supplierArticle: str
    techSize: str
    barcode: str
    totalPrice: float
    discountPercent: int
    warehouseName: str
    oblast: str
    incomeID: int
    odid: int
    nmId: int
    subject: str
    category: str
    brand: str
    isCancel: bool
    cancel_dt: Optional[datetime] = None
    gNumber: str
    sticker: str
    
    @property
    def order_id(self) -> str:
        """Получить ID заказа"""
        return str(self.odid)
    
    @property
    def amount(self) -> float:
        """Получить сумму заказа"""
        return self.totalPrice
    
    @property
    def status(self) -> str:
        """Получить статус заказа"""
        return "cancelled" if self.isCancel else "active"
    
    @classmethod
    def from_api_response(cls, data: dict) -> "WildberriesOrder":
        """Создать объект из ответа API"""
        return cls(**data)
