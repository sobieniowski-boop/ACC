from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class OrderLineOut(BaseModel):
    sku: Optional[str] = None
    asin: Optional[str] = None
    title: Optional[str] = None
    title_pl: Optional[str] = None
    quantity: int
    item_price: Optional[float] = None
    currency: str = "PLN"
    purchase_price_pln: Optional[float] = None
    cogs_pln: Optional[float] = None
    fba_fee_pln: Optional[float] = None
    referral_fee_pln: Optional[float] = None
    model_config = {"from_attributes": True}


class ProfitOrderOut(BaseModel):
    id: str
    amazon_order_id: str
    marketplace_id: str
    marketplace_code: Optional[str] = None
    purchase_date: datetime
    status: str
    fulfillment_channel: str
    order_total: Optional[float] = None
    currency: str = "PLN"
    revenue_pln: Optional[float] = None
    cogs_pln: Optional[float] = None
    amazon_fees_pln: Optional[float] = None
    ads_cost_pln: Optional[float] = None
    logistics_pln: Optional[float] = None
    contribution_margin_pln: Optional[float] = None
    cm_percent: Optional[float] = None
    lines: list[OrderLineOut] = []
    model_config = {"from_attributes": True}


class ProfitOrderListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[ProfitOrderOut]


class ProfitSummaryBySkuItem(BaseModel):
    sku: str
    asin: Optional[str] = None
    title: Optional[str] = None
    units: int
    revenue_pln: float
    cogs_pln: float
    amazon_fees_pln: float
    logistics_pln: float = 0
    contribution_margin_pln: float
    cm_percent: float
    orders: int


class ProfitSummaryBySkuResponse(BaseModel):
    date_from: date
    date_to: date
    marketplace_id: Optional[str] = None
    total_skus: int
    items: list[ProfitSummaryBySkuItem]
