from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class InventoryCoverageItem(BaseModel):
    key: str
    label: str
    pct: float
    status: str
    note: str | None = None


class InventoryOverviewMetric(BaseModel):
    label: str
    value: float | int
    unit: str | None = None
    delta_pct: float | None = None
    status: str = "ok"


class InventoryDecisionItem(BaseModel):
    sku: str
    asin: str | None = None
    marketplace_id: str
    marketplace_code: str
    title_preferred: str | None = None
    brand: str | None = None
    category: str | None = None
    product_type: str | None = None
    fulfillment_badge: str = "FBA"
    listing_status: str = "unknown"
    suppression_reason: str | None = None
    local_parent_asin: str | None = None
    local_theme: str | None = None
    family_health: str = "unknown"
    global_family_status: str = "missing"
    fba_on_hand: int = 0
    fba_available: int = 0
    inbound: int = 0
    reserved: int = 0
    fbm_on_hand: int = 0
    velocity_7d_units: float = 0.0
    velocity_30d_units: float = 0.0
    days_cover: float | None = None
    stockout_risk_badge: str = "ok"
    overstock_risk_badge: str = "ok"
    stranded_units: int = 0
    stranded_value_pln: float = 0.0
    aged_90_plus_units: int = 0
    aged_90_plus_value_pln: float = 0.0
    sessions_7d: int | None = None
    sessions_30d: int | None = None
    page_views_7d: int | None = None
    page_views_30d: int | None = None
    orders_7d: int = 0
    units_ordered_7d: int = 0
    unit_session_pct_7d: float | None = None
    unit_session_pct_30d: float | None = None
    sessions_delta_pct: float | None = None
    cvr_delta_pct: float | None = None
    demand_vs_supply_badge: str = "traffic_missing"
    traffic_coverage_flag: bool = True
    inventory_freshness: date | None = None
    last_change_at: datetime | None = None
    notes_indicator: bool = False
    internal_sku: str | None = None
    ean: str | None = None
    parent_asin: str | None = None


class InventoryFamilySummary(BaseModel):
    marketplace_code: str
    parent_asin: str
    children_count: int
    theme: str | None = None
    coverage_vs_de_pct: float | None = None
    missing_children: int = 0
    extra_children: int = 0
    conflicts_count: int = 0
    missing_required_attrs_count: int = 0
    confidence_avg: float | None = None
    status: str = "needs_review"
    updated_at: datetime | None = None


class InventoryOverviewResponse(BaseModel):
    metrics: list[InventoryOverviewMetric]
    coverage: list[InventoryCoverageItem]
    top_high_demand_low_supply: list[InventoryDecisionItem]
    top_cvr_crash: list[InventoryDecisionItem]
    top_suppressed_high_sessions: list[InventoryDecisionItem]
    recently_changed_families: list[InventoryFamilySummary]
    generated_at: datetime


class InventoryAllResponse(BaseModel):
    items: list[InventoryDecisionItem]
    total: int
    snapshot_date: date | None = None
    coverage: list[InventoryCoverageItem]


class InventorySkuTimelinePoint(BaseModel):
    date: date
    sessions: int | None = None
    page_views: int | None = None
    units: int = 0
    orders: int = 0
    revenue: float = 0.0
    unit_session_pct: float | None = None
    on_hand: int | None = None
    available: int | None = None
    inbound: int | None = None


class InventorySkuDetailResponse(BaseModel):
    item: InventoryDecisionItem
    inventory_timeline: list[InventorySkuTimelinePoint] = Field(default_factory=list)
    traffic_timeline: list[InventorySkuTimelinePoint] = Field(default_factory=list)
    family_context: dict[str, Any] = Field(default_factory=dict)
    issues: list[str] = Field(default_factory=list)
    change_history: list[dict[str, Any]] = Field(default_factory=list)
    coverage: list[InventoryCoverageItem] = Field(default_factory=list)


class InventoryFamilyListResponse(BaseModel):
    items: list[InventoryFamilySummary]
    total: int


class InventoryFamilyChildItem(BaseModel):
    child_asin: str | None = None
    child_sku: str | None = None
    master_key: str | None = None
    key_type: str | None = None
    variant_attributes: dict[str, Any] = Field(default_factory=dict)
    current_parent_asin: str | None = None
    proposed_parent_asin: str | None = None
    match_type: str | None = None
    confidence: float | None = None
    warnings: list[str] = Field(default_factory=list)


class InventoryFamilyDetailResponse(BaseModel):
    marketplace_code: str
    parent_asin: str
    theme: str | None = None
    status: str
    current_children: list[InventoryFamilyChildItem] = Field(default_factory=list)
    proposed_children: list[InventoryFamilyChildItem] = Field(default_factory=list)
    coverage_vs_de_pct: float | None = None
    issues: list[str] = Field(default_factory=list)


class InventoryDraftItem(BaseModel):
    id: str
    draft_type: str
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    affected_parent_asin: str | None = None
    affected_sku: str | None = None
    validation_status: str
    approval_status: str
    apply_status: str
    created_by: str | None = None
    created_at: datetime
    approved_by: str | None = None
    approved_at: datetime | None = None
    apply_started_at: datetime | None = None
    applied_at: datetime | None = None
    rolled_back_at: datetime | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)
    snapshot_before_json: dict[str, Any] = Field(default_factory=dict)
    snapshot_after_json: dict[str, Any] = Field(default_factory=dict)
    validation_errors: list[str] = Field(default_factory=list)


class InventoryDraftCreate(BaseModel):
    draft_type: str
    marketplace_id: str | None = None
    affected_parent_asin: str | None = None
    affected_sku: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)
    snapshot_before_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None


class InventoryDraftListResponse(BaseModel):
    items: list[InventoryDraftItem]
    total: int


class InventoryDraftActionResponse(BaseModel):
    draft: InventoryDraftItem
    events: list[dict[str, Any]] = Field(default_factory=list)


class InventoryJobItem(BaseModel):
    id: str
    job_type: str
    status: str
    progress_pct: int
    progress_message: str | None = None
    records_processed: int | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime | None = None
    duration_seconds: float | None = None


class InventoryJobListResponse(BaseModel):
    items: list[InventoryJobItem]
    total: int
    latest_by_type: list[InventoryJobItem] = Field(default_factory=list)


class InventorySettingsResponse(BaseModel):
    thresholds: dict[str, Any] = Field(default_factory=dict)
    theme_requirements: dict[str, Any] = Field(default_factory=dict)
    apply_safety: dict[str, Any] = Field(default_factory=dict)
    traffic_schedule: dict[str, Any] = Field(default_factory=dict)
    saved_views_enabled: bool = True
    updated_at: datetime | None = None


class InventorySettingsUpdate(BaseModel):
    thresholds: dict[str, Any] | None = None
    theme_requirements: dict[str, Any] | None = None
    apply_safety: dict[str, Any] | None = None
    traffic_schedule: dict[str, Any] | None = None
    saved_views_enabled: bool | None = None
