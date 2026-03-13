from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


# ==========================================================================
#  FBA Fee Audit — anomaly detection schemas
# ==========================================================================

class FbaFeeAnomalyPeriod(BaseModel):
    week_start: str
    week_end: str | None = None
    order_count: int
    avg_fee: float
    min_fee: float | None = None
    max_fee: float | None = None


class FbaFeeAnomaly(BaseModel):
    sku: str
    asin: str | None = None
    title: str | None = None
    internal_sku: str | None = None
    parent_asin: str | None = None
    currency: str = "EUR"
    current_period: FbaFeeAnomalyPeriod
    previous_period: FbaFeeAnomalyPeriod
    fee_ratio: float
    estimated_overcharge: float = 0
    severity: str = "medium"  # medium | high | critical
    recommendation: str | None = None


class FbaFeeAnomalyResponse(BaseModel):
    anomalies: list[FbaFeeAnomaly]
    total_anomalies: int
    total_estimated_overcharge_eur: float
    scan_period: dict[str, str | None] = Field(default_factory=dict)


class FbaFeeCharge(BaseModel):
    date: str
    order_id: str | None = None
    charge_type: str | None = None
    fee: float


class FbaFeeDailySummary(BaseModel):
    date: str
    order_count: int
    avg_fee: float
    min_fee: float
    max_fee: float


class FbaFeeStatistics(BaseModel):
    total_orders: int
    mean_fee: float
    median_fee: float
    p25_fee: float
    p75_fee: float
    iqr: float
    upper_fence: float
    min_fee: float
    max_fee: float
    anomalous_orders: int


class FbaFeeAnomalyPeriodDetail(BaseModel):
    start_date: str
    end_date: str | None = None
    days: int
    avg_anomaly_fee: float
    normal_fee: float
    overcharge_per_unit: float
    ongoing: bool = False


class FbaFeeTimelineResponse(BaseModel):
    sku: str
    asin: str | None = None
    title: str | None = None
    internal_sku: str | None = None
    currency: str = "EUR"
    charges: list[FbaFeeCharge] = Field(default_factory=list)
    daily_summary: list[FbaFeeDailySummary] = Field(default_factory=list)
    statistics: FbaFeeStatistics | None = None
    anomaly_periods: list[FbaFeeAnomalyPeriodDetail] = Field(default_factory=list)


class FbaOverchargedOrder(BaseModel):
    order_id: str
    date: str
    actual_fee: float
    expected_fee: float
    overcharge: float


class FbaSkuOvercharge(BaseModel):
    sku: str
    asin: str | None = None
    title: str | None = None
    internal_sku: str | None = None
    currency: str = "EUR"
    total_charges: int
    median_fee: float
    threshold: float
    overcharged_order_count: int
    estimated_overcharge: float
    estimated_overcharge_eur: float = 0
    overcharged_orders: list[FbaOverchargedOrder] = Field(default_factory=list)
    severity: str = "medium"


class FbaOverchargeSummaryResponse(BaseModel):
    items: list[FbaSkuOvercharge]
    total_skus_affected: int
    total_affected_orders: int
    total_estimated_overcharge_eur: float
    overcharge_by_currency: dict[str, float] = Field(default_factory=dict)
    scan_date: str
    filters: dict[str, Any] = Field(default_factory=dict)


class FbaFeeReferenceItem(BaseModel):
    sku: str
    marketplace_id: str | None = None
    size_tier: str | None = None
    expected_fee_eur: float
    avg_actual_fee: float
    min_actual_fee: float
    max_actual_fee: float
    total_orders: int
    fee_delta: float
    delta_pct: float


class FbaFeeReferenceResponse(BaseModel):
    available: bool = False
    message: str | None = None
    items: list[FbaFeeReferenceItem] = Field(default_factory=list)
    total: int = 0


# ==========================================================================
#  Original FBA Ops schemas below
# ==========================================================================

class FbaOverviewMetric(BaseModel):
    label: str
    value: float | int
    unit: str | None = None
    trend: float | None = None
    status: str = "ok"


class FbaRiskItem(BaseModel):
    sku: str
    asin: str | None = None
    internal_sku: str | None = None
    ean: str | None = None
    parent_asin: str | None = None
    title_preferred: str | None = None
    marketplace_id: str
    marketplace_code: str
    brand: str | None = None
    category: str | None = None
    on_hand: int
    inbound: int
    reserved: int
    units_available: int
    velocity_7d: float
    velocity_30d: float
    days_cover: float | None = None
    target_days: int = 45
    stockout_risk: str = "ok"
    overstock_risk: str = "ok"
    aged_90_plus_units: int = 0
    aged_90_plus_value_pln: float = 0.0
    stranded_units: int = 0
    stranded_value_pln: float = 0.0
    last_restock_date: date | None = None
    next_inbound_eta: date | None = None


class FbaInboundShipmentItem(BaseModel):
    shipment_id: str
    shipment_name: str | None = None
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    from_warehouse: str | None = None
    status: str
    created_at: datetime | None = None
    last_update_at: datetime | None = None
    units_planned: int = 0
    units_received: int = 0
    variance_units: int = 0
    first_receive_at: datetime | None = None
    closed_at: datetime | None = None
    days_in_status: int = 0
    problems: list[str] = Field(default_factory=list)


class FbaInboundShipmentListResponse(BaseModel):
    items: list[FbaInboundShipmentItem]
    total: int
    by_status: dict[str, int]


class FbaInboundShipmentLineItem(BaseModel):
    sku: str
    asin: str | None = None
    internal_sku: str | None = None
    ean: str | None = None
    parent_asin: str | None = None
    title_preferred: str | None = None
    qty_planned: int = 0
    qty_received: int = 0
    variance_units: int = 0
    payload_json: dict[str, Any] = Field(default_factory=dict)


class FbaInboundShipmentDetailResponse(BaseModel):
    shipment: FbaInboundShipmentItem
    lines: list[FbaInboundShipmentLineItem] = Field(default_factory=list)


class FbaCaseEventItem(BaseModel):
    id: str
    case_id: str
    event_type: str
    event_at: datetime
    actor: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)


class FbaCaseTimelineResponse(BaseModel):
    case: FbaCaseItem
    events: list[FbaCaseEventItem] = Field(default_factory=list)


class FbaAgedItem(BaseModel):
    sku: str
    asin: str | None = None
    internal_sku: str | None = None
    ean: str | None = None
    title_preferred: str | None = None
    marketplace_id: str
    marketplace_code: str
    aged_90_plus_units: int
    aged_90_plus_value_pln: float
    storage_fee_impact_estimate_pln: float = 0.0
    recommended_action: str


class FbaStrandedItem(BaseModel):
    sku: str
    asin: str | None = None
    internal_sku: str | None = None
    ean: str | None = None
    title_preferred: str | None = None
    marketplace_id: str
    marketplace_code: str
    stranded_units: int
    stranded_value_pln: float
    reason: str | None = None
    recommended_action: str


class FbaOverviewResponse(BaseModel):
    metrics: list[FbaOverviewMetric]
    top_stockout_risks: list[FbaRiskItem]
    top_aged_value_skus: list[FbaAgedItem]
    inbound_delays: list[FbaInboundShipmentItem]
    snapshot_date: date | None = None


class FbaReportDiagnosticItem(BaseModel):
    report_type: str
    fetch_mode: str
    request_status: str | None = None
    selected_status: str | None = None
    fallback_source: str | None = None
    detail_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class FbaMarketplaceDiagnosticItem(BaseModel):
    marketplace_id: str
    marketplace_code: str
    planning: FbaReportDiagnosticItem | None = None
    stranded: FbaReportDiagnosticItem | None = None
    inventory_api: FbaReportDiagnosticItem | None = None


class FbaReportDiagnosticsResponse(BaseModel):
    generated_at: datetime
    items: list[FbaMarketplaceDiagnosticItem]


class FbaInventoryListResponse(BaseModel):
    items: list[FbaRiskItem]
    total: int
    snapshot_date: date | None = None


class FbaInventoryTimelinePoint(BaseModel):
    date: date
    on_hand: int = 0
    inbound: int = 0
    reserved: int = 0
    units_sold: int = 0


class FbaInventoryDetailResponse(BaseModel):
    item: FbaRiskItem
    inventory_timeline: list[FbaInventoryTimelinePoint]
    sales_timeline: list[FbaInventoryTimelinePoint]
    notes: list[dict[str, Any]] = Field(default_factory=list)


class FbaReplenishmentSuggestion(BaseModel):
    sku: str
    asin: str | None = None
    title_preferred: str | None = None
    marketplace_id: str
    marketplace_code: str
    brand: str | None = None
    category: str | None = None
    current_days_cover: float | None = None
    target_days_cover: int
    lead_time_days: int
    safety_stock_days: int
    suggested_qty: int
    suggested_ship_week: date
    urgency: str
    exceptions: list[str] = Field(default_factory=list)


class FbaReplenishmentResponse(BaseModel):
    items: list[FbaReplenishmentSuggestion]
    total: int


class FbaKpiComponent(BaseModel):
    key: str
    label: str
    unit: str
    direction: str
    weight: float
    actual: float | None = None
    alarm: float | None = None
    target: float | None = None
    good: float | None = None
    factor: float = 0.0
    score_contribution: float = 0.0
    data_ready: bool = False
    note: str | None = None


class FbaShipmentPlanItem(BaseModel):
    id: str
    quarter: str
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    shipment_id: str | None = None
    plan_week_start: date
    planned_ship_date: date | None = None
    planned_units: int = 0
    actual_ship_date: date | None = None
    actual_units: int | None = None
    tolerance_pct: float = 0.10
    status: str
    owner: str | None = None
    notes_json: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime


class FbaShipmentPlanCreate(BaseModel):
    quarter: str
    marketplace_id: str | None = None
    shipment_id: str | None = None
    plan_week_start: date
    planned_ship_date: date | None = None
    planned_units: int = 0
    actual_ship_date: date | None = None
    actual_units: int | None = None
    tolerance_pct: float = 0.10
    status: str = "planned"
    owner: str | None = None
    notes_json: dict[str, Any] = Field(default_factory=dict)


class FbaShipmentPlanUpdate(BaseModel):
    shipment_id: str | None = None
    plan_week_start: date | None = None
    planned_ship_date: date | None = None
    planned_units: int | None = None
    actual_ship_date: date | None = None
    actual_units: int | None = None
    tolerance_pct: float | None = None
    status: str | None = None
    owner: str | None = None
    notes_json: dict[str, Any] | None = None


class FbaShipmentPlanListResponse(BaseModel):
    total: int
    items: list[FbaShipmentPlanItem]


class FbaCaseItem(BaseModel):
    id: str
    case_type: str
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    sku: str | None = None
    detected_date: date
    close_date: date | None = None
    owner: str | None = None
    status: str
    root_cause: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class FbaCaseCreate(BaseModel):
    case_type: str
    marketplace_id: str | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    sku: str | None = None
    detected_date: date
    close_date: date | None = None
    owner: str | None = None
    status: str = "open"
    root_cause: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)


class FbaCaseUpdate(BaseModel):
    close_date: date | None = None
    owner: str | None = None
    status: str | None = None
    root_cause: str | None = None
    payload_json: dict[str, Any] | None = None


class FbaCaseCommentCreate(BaseModel):
    comment: str
    author: str | None = None


class FbaCaseCommentUpdate(BaseModel):
    comment: str
    author: str | None = None


class FbaCaseListResponse(BaseModel):
    total: int
    items: list[FbaCaseItem]


class FbaLaunchItem(BaseModel):
    id: str
    quarter: str
    launch_type: str
    sku: str | None = None
    bundle_id: str | None = None
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    planned_go_live_date: date | None = None
    actual_go_live_date: date | None = None
    live_stable_at: date | None = None
    incident_free: bool = True
    vine_eligible: bool = False
    vine_eligible_at: date | None = None
    vine_submitted_at: date | None = None
    owner: str | None = None
    status: str
    payload_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class FbaLaunchCreate(BaseModel):
    quarter: str
    launch_type: str = "new_sku"
    sku: str | None = None
    bundle_id: str | None = None
    marketplace_id: str | None = None
    planned_go_live_date: date | None = None
    actual_go_live_date: date | None = None
    live_stable_at: date | None = None
    incident_free: bool = True
    vine_eligible: bool = False
    vine_eligible_at: date | None = None
    vine_submitted_at: date | None = None
    owner: str | None = None
    status: str = "planned"
    payload_json: dict[str, Any] = Field(default_factory=dict)


class FbaLaunchUpdate(BaseModel):
    actual_go_live_date: date | None = None
    live_stable_at: date | None = None
    incident_free: bool | None = None
    vine_eligible: bool | None = None
    vine_eligible_at: date | None = None
    vine_submitted_at: date | None = None
    owner: str | None = None
    status: str | None = None
    payload_json: dict[str, Any] | None = None


class FbaLaunchListResponse(BaseModel):
    total: int
    items: list[FbaLaunchItem]


class FbaInitiativeItem(BaseModel):
    id: str
    quarter: str
    initiative_type: str
    title: str
    sku: str | None = None
    bundle_id: str | None = None
    owner: str | None = None
    status: str
    planned: bool = True
    approved: bool = True
    live_stable_at: date | None = None
    created_at: datetime
    updated_at: datetime


class FbaInitiativeCreate(BaseModel):
    quarter: str
    initiative_type: str
    title: str
    sku: str | None = None
    bundle_id: str | None = None
    owner: str | None = None
    status: str = "planned"
    planned: bool = True
    approved: bool = True
    live_stable_at: date | None = None


class FbaInitiativeUpdate(BaseModel):
    title: str | None = None
    owner: str | None = None
    status: str | None = None
    planned: bool | None = None
    approved: bool | None = None
    live_stable_at: date | None = None


class FbaInitiativeListResponse(BaseModel):
    total: int
    items: list[FbaInitiativeItem]


class FbaKpiScorecardResponse(BaseModel):
    quarter: str
    data_ready: bool = False
    score: float = 0.0
    score_pct_of_target: float = 0.0
    safety_gate_passed: bool = True
    explanation: str
    kpis: dict[str, float] = Field(default_factory=dict)
    factors: dict[str, float] = Field(default_factory=dict)
    weights: dict[str, float] = Field(default_factory=dict)
    components: list[FbaKpiComponent] = Field(default_factory=list)
    missing_inputs: list[str] = Field(default_factory=list)
