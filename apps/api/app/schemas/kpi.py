"""KPI / Executive Dashboard schemas."""
from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel


class KPISummaryRequest(BaseModel):
    date_from: date
    date_to: date
    marketplace_id: Optional[str] = None  # None = all marketplaces


class MarketplaceKPI(BaseModel):
    marketplace_id: str
    marketplace_code: str
    revenue_pln: float
    orders: int
    units: int
    cm1_pln: float
    cm1_percent: float
    cm2_pln: float = 0
    cm2_percent: float = 0
    overhead_pln: float = 0
    net_profit_pln: float = 0
    net_profit_percent: float = 0
    acos: Optional[float] = None
    ads_spend_pln: float
    avg_order_value_pln: float
    courier_cost_pln: float = 0
    return_rate_pct: Optional[float] = None
    refund_units: int = 0
    refund_pln: float = 0


class KPISummaryResponse(BaseModel):
    date_from: date
    date_to: date
    last_sync: Optional[str] = None

    # Aggregate totals
    total_revenue_pln: float
    total_orders: int
    total_units: int
    total_cm1_pln: float
    total_cm1_percent: float
    total_cm2_pln: float = 0
    total_cm2_percent: float = 0
    total_overhead_pln: float = 0
    total_net_profit_pln: float = 0
    total_net_profit_percent: float = 0
    total_ads_spend_pln: float
    total_acos: Optional[float] = None
    total_tacos: Optional[float] = None
    avg_order_value_pln: float
    total_courier_cost_pln: float = 0
    total_return_rate_pct: Optional[float] = None
    total_refund_pln: float = 0
    total_refund_units: int = 0
    fbm_logistics_by_mkt: list[dict] = []
    fbm_coverage_pct: Optional[float] = None
    fbm_billing_pct: Optional[float] = None

    # FBA / FBM breakdown
    fba_orders: int = 0
    fbm_orders: int = 0
    fba_units: int = 0
    fbm_units: int = 0
    fba_units_per_order: Optional[float] = None
    fbm_units_per_order: Optional[float] = None

    # Delta vs prior period (percentage)
    revenue_delta_pct: Optional[float] = None
    orders_delta_pct: Optional[float] = None
    cm1_delta_pct: Optional[float] = None

    # Breakdown by marketplace
    by_marketplace: list[MarketplaceKPI] = []

    # Top 5 alerts (summary)
    active_alerts_count: int = 0
    critical_alerts_count: int = 0


class RevenueChartPoint(BaseModel):
    date: date
    revenue_pln: float
    cm1_pln: float
    orders: int


class RevenueChartResponse(BaseModel):
    date_from: date
    date_to: date
    marketplace_id: Optional[str] = None
    points: list[RevenueChartPoint]


class TrendChartPoint(BaseModel):
    date: date
    revenue_pln: float = 0
    cm1_pln: float = 0
    cm2_pln: float = 0
    profit_pln: float = 0
    orders: int = 0
    units: int = 0
    ad_spend_pln: float = 0


class TrendChartResponse(BaseModel):
    date_from: date
    date_to: date
    marketplace_id: Optional[str] = None
    points: list[TrendChartPoint]
