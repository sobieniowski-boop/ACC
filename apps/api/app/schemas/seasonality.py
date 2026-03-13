"""Pydantic schemas for Seasonality & Demand Intelligence module."""
from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────

class SeasonalityClass(str, Enum):
    EVERGREEN = "EVERGREEN"
    MILD_SEASONAL = "MILD_SEASONAL"
    STRONG_SEASONAL = "STRONG_SEASONAL"
    PEAK_SEASONAL = "PEAK_SEASONAL"
    EVENT_DRIVEN = "EVENT_DRIVEN"
    IRREGULAR = "IRREGULAR"


class OpportunityType(str, Enum):
    PREPARE_STOCK = "PREPARE_STOCK"
    PREPARE_CONTENT = "PREPARE_CONTENT"
    PREPARE_ADS = "PREPARE_ADS"
    PREPARE_PRICING = "PREPARE_PRICING"
    MARKET_EXPANSION_PREP = "MARKET_EXPANSION_PREP"
    FAMILY_EXPANSION_PREP = "FAMILY_EXPANSION_PREP"
    BUNDLE_PREP = "BUNDLE_PREP"
    PROFIT_PROTECTION = "PROFIT_PROTECTION"
    LIQUIDATE_POST_SEASON = "LIQUIDATE_POST_SEASON"


class OpportunityStatus(str, Enum):
    NEW = "new"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    COMPLETED = "completed"


# ── Monthly Metrics ──────────────────────────────────────────────────

class MonthlyMetric(BaseModel):
    id: int
    marketplace: str
    entity_type: str
    entity_id: str
    year: int
    month: int
    sessions: float | None = None
    page_views: float | None = None
    clicks: float | None = None
    impressions: float | None = None
    purchases: float | None = None
    units: float | None = None
    orders: float | None = None
    revenue: float | None = None
    profit_cm1: float | None = None
    profit_cm2: float | None = None
    profit_np: float | None = None
    unit_session_pct: float | None = None
    ad_spend: float | None = None
    refunds: float | None = None
    stockout_days: int | None = None
    suppression_days: int | None = None


# ── Seasonality Profile ─────────────────────────────────────────────

class SeasonalityProfile(BaseModel):
    id: int
    marketplace: str
    entity_type: str
    entity_id: str
    seasonality_class: str
    demand_strength_score: float
    sales_strength_score: float
    profit_strength_score: float
    evergreen_score: float
    volatility_score: float
    seasonality_confidence_score: float
    peak_months: list[int] = Field(default_factory=list)
    ramp_months: list[int] = Field(default_factory=list)
    decay_months: list[int] = Field(default_factory=list)
    season_length_months: int | None = None
    demand_vs_sales_gap: float | None = None
    sales_vs_profit_gap: float | None = None
    updated_at: datetime | None = None


# ── Index Cache ──────────────────────────────────────────────────────

class MonthIndex(BaseModel):
    month: int
    demand_index: float | None = None
    sales_index: float | None = None
    profit_index: float | None = None


# ── Opportunity ──────────────────────────────────────────────────────

class SeasonalityOpportunity(BaseModel):
    id: int
    marketplace: str
    entity_type: str
    entity_id: str
    opportunity_type: str
    title: str
    description: str
    priority_score: float
    confidence_score: float
    estimated_revenue_uplift: float | None = None
    estimated_profit_uplift: float | None = None
    recommended_start_date: date | None = None
    status: str
    source_signals: dict | None = None
    created_at: datetime | None = None


# ── Cluster ──────────────────────────────────────────────────────────

class ClusterCreate(BaseModel):
    cluster_name: str
    description: str | None = None
    rules_json: dict | None = None
    members: list[ClusterMemberInput] = Field(default_factory=list)


class ClusterMemberInput(BaseModel):
    sku: str | None = None
    asin: str | None = None
    product_type: str | None = None
    category: str | None = None


# Fix forward ref
ClusterCreate.model_rebuild()


class Cluster(BaseModel):
    id: int
    cluster_name: str
    description: str | None = None
    rules_json: dict | None = None
    members_count: int = 0
    created_by: str | None = None
    created_at: datetime | None = None


class ClusterDetail(Cluster):
    members: list[ClusterMemberInput] = Field(default_factory=list)
    seasonality_class: str | None = None
    peak_months: list[int] = Field(default_factory=list)
    confidence: float | None = None


# ── Response Models ──────────────────────────────────────────────────

class OverviewKPI(BaseModel):
    seasonal_categories: int = 0
    evergreen_categories: int = 0
    strongest_upcoming_season: dict | None = None
    highest_demand_ramp: dict | None = None
    biggest_execution_gap: dict | None = None
    biggest_profit_opportunity: dict | None = None


class OverviewResponse(BaseModel):
    kpi: OverviewKPI
    marketplace_heatmap: list[dict] = Field(default_factory=list)
    class_distribution: dict = Field(default_factory=dict)
    upcoming_opportunities: list[dict] = Field(default_factory=list)
    peak_calendar: list[dict] = Field(default_factory=list)


class MapRow(BaseModel):
    entity_type: str
    entity_id: str
    marketplace: str
    indices: list[MonthIndex]
    seasonality_class: str
    peak_months: list[int] = Field(default_factory=list)
    strength_score: float = 0
    confidence_score: float = 0
    evergreen_score: float = 0
    volatility_score: float = 0


class MapResponse(BaseModel):
    items: list[MapRow]
    total: int = 0
    page: int = 1
    page_size: int = 50


class EntitiesResponse(BaseModel):
    items: list[SeasonalityProfile]
    total: int = 0
    page: int = 1
    page_size: int = 50


class EntityDetailResponse(BaseModel):
    profile: SeasonalityProfile
    monthly_metrics: list[MonthlyMetric] = Field(default_factory=list)
    indices: list[MonthIndex] = Field(default_factory=list)
    demand_vs_execution_gap: dict = Field(default_factory=dict)
    marketplace_comparison: list[dict] = Field(default_factory=list)


class OpportunitiesResponse(BaseModel):
    items: list[SeasonalityOpportunity]
    total: int = 0
    page: int = 1
    page_size: int = 50


class SettingsResponse(BaseModel):
    settings: dict[str, str]
