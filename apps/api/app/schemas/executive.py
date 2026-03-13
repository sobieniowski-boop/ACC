"""Pydantic schemas for Executive Command Center."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


# --- Health Score ---
class HealthScore(BaseModel):
    period_date: date
    revenue_score: float
    profit_score: float
    demand_score: float
    inventory_score: float
    operations_score: float
    overall_score: float


class HealthScoreLabel(BaseModel):
    score: float
    label: str  # excellent / healthy / watchlist / risk / critical
    color: str  # green / blue / yellow / orange / red


# --- Daily Metrics ---
class ExecDailyMetric(BaseModel):
    period_date: date
    marketplace_id: str
    marketplace_code: Optional[str] = None
    revenue_pln: float
    cm1_pln: float = 0
    cm2_pln: float = 0
    profit_pln: float
    margin_pct: float
    units: int
    orders: int
    ad_spend_pln: float
    acos_pct: Optional[float] = None
    return_rate_pct: Optional[float] = None
    refund_pln: float
    cogs_pln: float
    sessions: Optional[int] = None
    cvr_pct: Optional[float] = None
    stockout_skus: int = 0
    suppressed_skus: int = 0


# --- KPI summary ---
class ExecKPI(BaseModel):
    revenue_pln: float
    cm1_pln: float = 0
    cm2_pln: float = 0
    profit_pln: float
    margin_pct: float
    orders: int
    units: int
    ad_spend_pln: float
    acos_pct: Optional[float] = None
    return_rate_pct: Optional[float] = None
    revenue_growth_pct: Optional[float] = None
    profit_growth_pct: Optional[float] = None


# --- Opportunity / Risk ---
class Opportunity(BaseModel):
    id: int
    opp_type: str
    category: str
    priority: str
    marketplace_id: Optional[str] = None
    marketplace_code: Optional[str] = None
    sku: Optional[str] = None
    title: str
    description: Optional[str] = None
    impact_estimate: Optional[float] = None
    confidence: Optional[float] = None
    is_active: bool = True
    created_at: Optional[datetime] = None


# --- Overview response ---
class ExecOverviewResponse(BaseModel):
    kpi: ExecKPI
    kpi_prev: Optional[ExecKPI] = None
    health: Optional[HealthScore] = None
    health_label: Optional[HealthScoreLabel] = None
    risks: list[Opportunity]
    growth: list[Opportunity]
    best_skus: list[dict]
    worst_skus: list[dict]


# --- Product row ---
class ExecProductItem(BaseModel):
    sku: str
    asin: Optional[str] = None
    marketplace_id: str
    marketplace_code: Optional[str] = None
    revenue_pln: float
    cm1_pln: float = 0
    cm2_pln: float = 0
    profit_pln: float
    margin_pct: Optional[float] = None
    units: int
    sessions: Optional[int] = None
    cvr_pct: Optional[float] = None
    return_rate_pct: Optional[float] = None
    acos_pct: Optional[float] = None
    inventory_risk: Optional[str] = None  # ok / low / critical / stockout


class ExecProductsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[ExecProductItem]


# --- Marketplace row ---
class ExecMarketplaceItem(BaseModel):
    marketplace_id: str
    marketplace_code: Optional[str] = None
    revenue_pln: float
    cm1_pln: float = 0
    cm2_pln: float = 0
    profit_pln: float
    margin_pct: Optional[float] = None
    orders: int
    units: int
    sessions: Optional[int] = None
    cvr_pct: Optional[float] = None
    acos_pct: Optional[float] = None
    return_rate_pct: Optional[float] = None
    health_score: Optional[float] = None


class ExecMarketplacesResponse(BaseModel):
    items: list[ExecMarketplaceItem]


# --- Recompute result ---
class ExecRecomputeResult(BaseModel):
    metrics_rows: int
    health_computed: bool
    opportunities_found: int
    risks_found: int
