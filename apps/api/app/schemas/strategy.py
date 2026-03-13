"""Strategy / Growth Engine — Pydantic schemas."""
from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, Field


# ── Enums ───────────────────────────────────────────────────────────
class OpportunityType(str, Enum):
    PRICE_INCREASE = "PRICE_INCREASE"
    PRICE_DECREASE = "PRICE_DECREASE"
    ADS_SCALE_UP = "ADS_SCALE_UP"
    ADS_CUT_WASTE = "ADS_CUT_WASTE"
    CONTENT_FIX = "CONTENT_FIX"
    CONTENT_EXPANSION = "CONTENT_EXPANSION"
    STOCK_REPLENISH = "STOCK_REPLENISH"
    STOCK_PROTECTION = "STOCK_PROTECTION"
    BUNDLE_CREATE = "BUNDLE_CREATE"
    VARIANT_EXPANSION = "VARIANT_EXPANSION"
    MARKETPLACE_EXPANSION = "MARKETPLACE_EXPANSION"
    FAMILY_REPAIR = "FAMILY_REPAIR"
    RETURN_REDUCTION = "RETURN_REDUCTION"
    SUPPRESSION_FIX = "SUPPRESSION_FIX"
    FBA_MIGRATION = "FBA_MIGRATION"
    FBM_FALLBACK = "FBM_FALLBACK"
    LIQUIDATE_OR_PROMO = "LIQUIDATE_OR_PROMO"
    COST_RENEGOTIATION = "COST_RENEGOTIATION"
    CATEGORY_WINNER_SCALE = "CATEGORY_WINNER_SCALE"
    LOW_POTENTIAL_DEPRIORITIZE = "LOW_POTENTIAL_DEPRIORITIZE"


class RootCause(str, Enum):
    traffic_problem = "traffic_problem"
    conversion_problem = "conversion_problem"
    pricing_problem = "pricing_problem"
    availability_problem = "availability_problem"
    inventory_problem = "inventory_problem"
    family_structure_problem = "family_structure_problem"
    content_problem = "content_problem"
    advertising_problem = "advertising_problem"
    returns_problem = "returns_problem"
    cost_problem = "cost_problem"
    ops_problem = "ops_problem"
    expansion_gap = "expansion_gap"


class OpportunityStatus(str, Enum):
    new = "new"
    in_review = "in_review"
    accepted = "accepted"
    rejected = "rejected"
    completed = "completed"


class PriorityLabel(str, Enum):
    do_now = "do_now"
    this_week = "this_week"
    this_month = "this_month"
    backlog = "backlog"
    low = "low"


class ExperimentStatus(str, Enum):
    planned = "planned"
    running = "running"
    completed = "completed"
    cancelled = "cancelled"


# ── Opportunity ─────────────────────────────────────────────────────
class GrowthOpportunity(BaseModel):
    id: int
    opportunity_type: str
    marketplace_id: Optional[str] = None
    marketplace_code: Optional[str] = None
    sku: Optional[str] = None
    asin: Optional[str] = None
    parent_asin: Optional[str] = None
    family_id: Optional[int] = None
    title: str
    description: Optional[str] = None
    root_cause: Optional[str] = None
    recommendation: Optional[str] = None
    priority_score: float = 0
    priority_label: Optional[str] = None
    confidence_score: float = 0
    estimated_revenue_uplift: Optional[float] = None
    estimated_profit_uplift: Optional[float] = None
    estimated_margin_uplift: Optional[float] = None
    estimated_units_uplift: Optional[int] = None
    effort_score: Optional[float] = None
    owner_role: Optional[str] = None
    blocker_json: Optional[Any] = None
    source_signals_json: Optional[Any] = None
    status: str = "new"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class OpportunityLogEntry(BaseModel):
    id: int
    opportunity_id: int
    action: str
    actor: Optional[str] = None
    note: Optional[str] = None
    created_at: Optional[datetime] = None


# ── Overview ────────────────────────────────────────────────────────
class OverviewKPI(BaseModel):
    total_revenue_uplift: float = 0
    total_profit_uplift: float = 0
    total_opportunities: int = 0
    do_now_count: int = 0
    this_week_count: int = 0
    this_month_count: int = 0
    blocked_count: int = 0
    completed_30d: int = 0
    completed_impact_30d: float = 0


class TypeBreakdown(BaseModel):
    opportunity_type: str
    count: int = 0
    revenue_uplift: float = 0
    profit_uplift: float = 0


class MarketBreakdown(BaseModel):
    marketplace_id: str
    marketplace_code: Optional[str] = None
    count: int = 0
    revenue_uplift: float = 0
    profit_uplift: float = 0


class OwnerBreakdown(BaseModel):
    owner_role: str
    count: int = 0


class StrategyOverviewResponse(BaseModel):
    kpi: OverviewKPI
    by_type: List[TypeBreakdown] = []
    by_market: List[MarketBreakdown] = []
    by_owner: List[OwnerBreakdown] = []
    top_priorities: List[GrowthOpportunity] = []
    do_now: List[GrowthOpportunity] = []
    this_week: List[GrowthOpportunity] = []
    blocked: List[GrowthOpportunity] = []


# ── Opportunity list (paginated) ────────────────────────────────────
class OpportunityListResponse(BaseModel):
    items: List[GrowthOpportunity] = []
    total: int = 0
    pages: int = 0


# ── Opportunity detail ──────────────────────────────────────────────
class OpportunityDetailResponse(BaseModel):
    opportunity: GrowthOpportunity
    timeline: List[OpportunityLogEntry] = []


# ── Status change ───────────────────────────────────────────────────
class StatusChangeRequest(BaseModel):
    note: Optional[str] = None


class StatusChangeResponse(BaseModel):
    id: int
    status: str
    updated_at: datetime


# ── Marketplace expansion ──────────────────────────────────────────
class MarketExpansionItem(BaseModel):
    family_id: Optional[int] = None
    parent_asin: Optional[str] = None
    sku: Optional[str] = None
    source_marketplace: str
    target_marketplace: str
    source_revenue: float = 0
    source_profit: float = 0
    readiness_score: float = 0
    readiness_label: str = "not_viable"   # launch_ready / needs_content / needs_family_fix / needs_inventory / not_viable
    missing_components: Optional[List[str]] = None
    estimated_revenue_uplift: float = 0
    estimated_profit_uplift: float = 0
    confidence: float = 0


class MarketExpansionResponse(BaseModel):
    items: List[MarketExpansionItem] = []
    total: int = 0


# ── Bundle candidates ──────────────────────────────────────────────
class BundleCandidate(BaseModel):
    id: Optional[int] = None
    sku_a: str
    sku_b: Optional[str] = None
    proposed_bundle_sku: Optional[str] = None
    marketplace_id: Optional[str] = None
    est_margin: Optional[float] = None
    est_profit_uplift: Optional[float] = None
    confidence: float = 0
    blocker: Optional[str] = None
    action: Optional[str] = None


class BundleCandidateResponse(BaseModel):
    bundles: List[BundleCandidate] = []
    variant_gaps: List[dict] = []


# ── Experiments ────────────────────────────────────────────────────
class Experiment(BaseModel):
    id: int
    opportunity_id: Optional[int] = None
    experiment_type: str
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    asin: Optional[str] = None
    hypothesis: str
    owner: Optional[str] = None
    status: str = "planned"
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    success_metric: Optional[str] = None
    baseline_value: Optional[float] = None
    result_value: Optional[float] = None
    result_summary: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ExperimentCreate(BaseModel):
    opportunity_id: Optional[int] = None
    experiment_type: str
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    asin: Optional[str] = None
    hypothesis: str
    owner: Optional[str] = None
    success_metric: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class ExperimentListResponse(BaseModel):
    items: List[Experiment] = []
    total: int = 0


# ── Playbooks ──────────────────────────────────────────────────────
class PlaybookStep(BaseModel):
    seq: int
    action: str
    owner_role: Optional[str] = None
    details: Optional[str] = None


class Playbook(BaseModel):
    id: str
    name: str
    description: str
    trigger_condition: str
    opportunity_types: List[str] = []
    steps: List[PlaybookStep] = []
    metrics_to_monitor: List[str] = []
    expected_time_to_impact: Optional[str] = None


class PlaybookListResponse(BaseModel):
    playbooks: List[Playbook] = []


# ── Run jobs ───────────────────────────────────────────────────────
class JobRunRequest(BaseModel):
    job_type: str = Field(..., description="Job identifier, e.g. 'detect_all'")
    days_back: int = 30


class JobRunResponse(BaseModel):
    opportunities_found: int = 0
    elapsed_sec: float = 0
    details: Optional[dict] = None
