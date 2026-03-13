"""Pydantic schemas for the Finance Profitability module."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Overview / KPI
# ---------------------------------------------------------------------------

class ProfitabilityKPI(BaseModel):
    total_revenue_pln: float = 0
    total_cm1_pln: float = 0
    total_cm2_pln: float = 0
    total_profit_pln: float = 0
    profit_tier: str = "cm1_cm2_np"
    total_margin_pct: float = 0
    cm1_margin_pct: float = 0
    total_orders: int = 0
    total_units: int = 0
    total_ad_spend_pln: float = 0
    ad_spend_share_pct: float = 0
    tacos_pct: float = 0
    total_refund_pln: float = 0
    return_rate_pct: float = 0


class SkuRankItem(BaseModel):
    sku: str
    asin: Optional[str] = None
    marketplace_id: str
    marketplace_code: Optional[str] = None
    revenue_pln: float = 0
    profit_pln: float = 0
    margin_pct: Optional[float] = None
    units: int = 0
    acos_pct: Optional[float] = None
    return_rate_pct: Optional[float] = None


class LossOrderItem(BaseModel):
    amazon_order_id: str
    marketplace_id: str
    marketplace_code: Optional[str] = None
    purchase_date: datetime
    sku: Optional[str] = None
    revenue_pln: float = 0
    profit_pln: float = 0
    margin_pct: Optional[float] = None


class DataFreshnessInfo(BaseModel):
    rollup_recomputed_at: Optional[datetime] = None
    cache_age_seconds: Optional[float] = None
    rollup_covers: Optional[dict] = None
    data_source: Optional[str] = None  # "rollup" or "live"


class ProfitabilityOverviewResponse(BaseModel):
    kpi: ProfitabilityKPI
    best_skus: list[SkuRankItem]
    worst_skus: list[SkuRankItem]
    loss_orders: list[LossOrderItem]
    warnings: list[str] = []
    data_freshness: Optional[DataFreshnessInfo] = None


# ---------------------------------------------------------------------------
# Orders table
# ---------------------------------------------------------------------------

class ProfitabilityOrderItem(BaseModel):
    amazon_order_id: str
    marketplace_id: str
    marketplace_code: Optional[str] = None
    purchase_date: datetime
    sku: Optional[str] = None
    asin: Optional[str] = None
    sku_count: int = 1
    all_skus: Optional[str] = None
    revenue_pln: float = 0
    amazon_fees_pln: float = 0
    fba_fees_pln: float = 0
    logistics_pln: float = 0
    cogs_pln: float = 0
    ad_cost_pln: float = 0
    refund_pln: float = 0
    profit_pln: float = 0
    margin_pct: Optional[float] = None


class ProfitabilityOrdersResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[ProfitabilityOrderItem]


# ---------------------------------------------------------------------------
# Products / SKU table
# ---------------------------------------------------------------------------

class ProfitabilityProductItem(BaseModel):
    sku: str
    asin: Optional[str] = None
    marketplace_id: str
    marketplace_code: Optional[str] = None
    units: int = 0
    orders: int = 0
    revenue_pln: float = 0
    cogs_pln: float = 0
    amazon_fees_pln: float = 0
    logistics_pln: float = 0
    ad_spend_pln: float = 0
    refund_pln: float = 0
    cm1_pln: float = 0
    cm2_pln: float = 0
    profit_pln: float = 0
    margin_pct: Optional[float] = None
    acos_pct: Optional[float] = None
    return_rate_pct: Optional[float] = None


class ProfitabilityProductsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[ProfitabilityProductItem]


# ---------------------------------------------------------------------------
# Marketplace rollup
# ---------------------------------------------------------------------------

class MarketplaceProfitabilityItem(BaseModel):
    marketplace_id: str
    marketplace_code: Optional[str] = None
    total_orders: int = 0
    total_units: int = 0
    unique_skus: int = 0
    revenue_pln: float = 0
    cm1_pln: float = 0
    cm2_pln: float = 0
    profit_pln: float = 0
    margin_pct: Optional[float] = None
    ad_spend_pln: float = 0
    acos_pct: Optional[float] = None
    return_rate_pct: Optional[float] = None


class MarketplaceProfitabilityResponse(BaseModel):
    items: list[MarketplaceProfitabilityItem]


# ---------------------------------------------------------------------------
# Price simulator
# ---------------------------------------------------------------------------

class PriceSimulatorRequest(BaseModel):
    sale_price: float
    purchase_cost: float
    shipping_cost: float = 0
    amazon_fee_pct: float = 15.0   # referral fee %
    fba_fee: float = 0
    ad_cost: float = 0
    currency: str = "EUR"
    fx_rate: Optional[float] = None  # to PLN, auto-resolved if None


class PriceSimulatorResult(BaseModel):
    sale_price: float
    purchase_cost: float
    shipping_cost: float
    amazon_fee: float
    fba_fee: float
    ad_cost: float
    total_cost: float
    profit: float
    margin_pct: float
    breakeven_price: float
    currency: str
    fx_rate: float


# ---------------------------------------------------------------------------
# Rollup job result
# ---------------------------------------------------------------------------

class RollupJobResult(BaseModel):
    sku_rows_upserted: int = 0
    marketplace_rows_upserted: int = 0
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    elapsed_seconds: float = 0
    recomputed_at: Optional[str] = None
