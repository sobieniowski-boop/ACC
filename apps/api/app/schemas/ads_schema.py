"""Pydantic schemas — Ads / PPC module."""
from __future__ import annotations

from datetime import date
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class CampaignOut(BaseModel):
    campaign_id: str
    marketplace_id: str
    marketplace_code: str
    campaign_name: str
    ad_type: str = "SP"          # SP | SB | SD
    state: str = "ENABLED"       # ENABLED | PAUSED | ARCHIVED
    targeting_type: str = ""     # AUTO | MANUAL | ""
    daily_budget: Optional[float] = None
    currency: str = "EUR"
    is_active: bool = True       # derived from state == ENABLED


class CampaignDayOut(BaseModel):
    id: int
    campaign_id: str
    report_date: date
    impressions: int
    clicks: int
    spend: float
    sales_7d: float
    orders_7d: int
    acos: Optional[float] = None       # spend/sales
    roas: Optional[float] = None       # sales/spend
    cpc: Optional[float] = None        # spend/clicks
    ctr: Optional[float] = None        # clicks/impressions
    spend_pln: Optional[float] = None
    sales_pln: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class AdsSummaryResponse(BaseModel):
    period_days: int
    total_spend_pln: float
    total_sales_pln: float
    total_orders: int
    total_impressions: int
    total_clicks: int
    avg_acos: float
    avg_roas: float
    avg_cpc: float
    avg_ctr: float


class AdsChartPoint(BaseModel):
    report_date: str
    spend_pln: float
    sales_pln: float
    acos: float
    roas: float
    orders: int


class AdsChartResponse(BaseModel):
    points: list[AdsChartPoint]
    campaign_id: Optional[str] = None
    marketplace_id: Optional[str] = None


class TopCampaignRow(BaseModel):
    campaign_id: str
    campaign_name: str
    marketplace_code: str
    total_spend_pln: float
    total_sales_pln: float
    avg_acos: float
    avg_roas: float
    orders: int
    efficiency_score: float     # composite 0-100


class AdsListResponse(BaseModel):
    items: list[TopCampaignRow]
    total: int
    summary: AdsSummaryResponse


class BudgetRecommendation(BaseModel):
    campaign_id: str
    campaign_name: str
    current_daily_budget: float
    recommended_daily_budget: float
    expected_sales_uplift_pln: float
    confidence: float
    reason: str
