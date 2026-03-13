"""Pydantic schemas for Profit Engine v2 (CM1/CM2/NP)."""
from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Product Profit Table
# ---------------------------------------------------------------------------

class ProductProfitItem(BaseModel):
    entity_type: str = "sku"
    group_key: Optional[str] = None
    sku: str
    sample_sku: Optional[str] = None
    asin: Optional[str] = None
    parent_asin: Optional[str] = None
    marketplace_id: str
    marketplace_code: str
    title: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    internal_sku: Optional[str] = None
    fulfillment_channel: str = ""
    # Sales
    units: int = 0
    order_count: int = 0
    sku_count: int = 0
    child_count: int = 0
    revenue_pln: float = 0
    shipping_charge_pln: float = 0
    # Unit economics
    cogs_per_unit: float = 0
    fees_per_unit: float = 0
    revenue_per_unit: float = 0
    # Cost totals
    cogs_pln: float = 0
    amazon_fees_pln: float = 0
    fba_fee_pln: float = 0
    referral_fee_pln: float = 0
    logistics_pln: float = 0
    # CM1
    cm1_profit: float = 0
    cm1_percent: float = 0
    # CM2 components
    ads_cost_pln: float = 0
    returns_net_pln: float = 0
    refund_gross_pln: float = 0
    return_handling_pln: float = 0
    fba_storage_fee_pln: float = 0
    fba_aged_fee_pln: float = 0
    fba_removal_fee_pln: float = 0
    fba_liquidation_fee_pln: float = 0
    refund_finance_pln: float = 0
    shipping_surcharge_pln: float = 0
    fba_inbound_fee_pln: float = 0
    promo_cost_pln: float = 0
    warehouse_loss_pln: float = 0
    amazon_other_fee_pln: float = 0
    cm2_profit: float = 0
    cm2_percent: float = 0
    # NP components
    overhead_allocated_pln: float = 0
    overhead_allocation_method: str = "none"
    overhead_confidence_pct: float = 0
    np_profit: float = 0
    np_percent: float = 0
    # Data quality
    cogs_coverage_pct: float = 0
    fees_coverage_pct: float = 0
    confidence_score: float = 0
    loss_orders_pct: float = 0
    return_rate: Optional[float] = None
    tacos: Optional[float] = None
    days_of_cover: Optional[float] = None
    shipping_match_pct: Optional[float] = None
    finance_match_pct: Optional[float] = None
    # Import flag
    is_import: bool = False
    # Refund impact (Shipped orders with refunds — our cost)
    refund_orders: int = 0
    refund_units: int = 0
    refund_cost_pln: float = 0
    # Return tracker — COGS classification from acc_return_item
    return_cogs_recovered_pln: float = 0   # sellable returns → WZ reversal
    return_cogs_write_off_pln: float = 0   # damaged/lost returns → loss
    return_cogs_pending_pln: float = 0     # awaiting physical return
    cm1_adjusted: float = 0                # CM1 + cogs_recovered (true P&L)


class ProductProfitSummary(BaseModel):
    total_revenue_pln: float = 0
    total_cogs_pln: float = 0
    total_fees_pln: float = 0
    total_cm1_pln: float = 0
    total_cm1_pct: float = 0
    total_ads_cost_pln: float = 0
    total_logistics_pln: float = 0
    total_cm2_pln: float = 0
    total_cm2_pct: float = 0
    total_np_pln: float = 0
    total_np_pct: float = 0
    total_returns_net_pln: float = 0
    total_refund_gross_pln: float = 0
    total_return_handling_pln: float = 0
    total_fba_storage_fee_pln: float = 0
    total_fba_aged_fee_pln: float = 0
    total_fba_removal_fee_pln: float = 0
    total_fba_liquidation_fee_pln: float = 0
    total_overhead_allocated_pln: float = 0
    overhead_allocation_method: str = "none"
    overhead_confidence_pct: float = 0
    total_units: int = 0
    avg_confidence: float = 0
    # Refund info — Shipped+refund orders (included in profit as cost)
    refund_shipped_orders: int = 0
    refund_shipped_units: int = 0
    refund_shipped_cost_pln: float = 0
    # Refund info — Return status orders (excluded from Shipped filter)
    refund_orders_excluded: int = 0
    refund_full_count: int = 0
    refund_partial_count: int = 0
    refund_total_pln: float = 0
    # Return tracker — aggregate COGS impact
    total_return_cogs_recovered_pln: float = 0
    total_return_cogs_write_off_pln: float = 0
    total_return_cogs_pending_pln: float = 0


class ProductProfitTableResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    summary: ProductProfitSummary
    items: list[ProductProfitItem]
    warnings: list[str] = []


# ---------------------------------------------------------------------------
# Product What-if Table (open offers simulation)
# ---------------------------------------------------------------------------

class ProductWhatIfItem(BaseModel):
    entity_type: str = "offer"
    group_key: Optional[str] = None
    sku: str
    sample_sku: Optional[str] = None
    asin: Optional[str] = None
    parent_asin: Optional[str] = None
    marketplace_id: str
    marketplace_code: str
    title: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    internal_sku: Optional[str] = None
    fulfillment_channel: str = ""
    offer_status: str = ""
    offer_currency: str = ""
    offer_price: float = 0
    offer_price_pln: float = 0
    scenario_qty: int = 1
    offer_count: int = 1
    sku_count: int = 1
    child_count: int = 1
    suggested_pack_qty: int = 1
    packages_count: int = 1
    plan_logistics_pln: float = 0
    observed_logistics_pln: float = 0
    decision_logistics_pln: float = 0
    logistics_gap_pct: Optional[float] = None
    logistics_decision_rule: str = "missing"
    logistics_plan_source: str = "missing"
    logistics_observed_source: str = "missing"
    logistics_observed_samples: int = 0
    execution_drift: bool = False
    estimated_shipping_charge_pln: float = 0
    estimated_logistics_pln: float = 0
    estimated_ads_pln: float = 0
    estimated_returns_net_pln: float = 0
    estimated_fba_storage_fee_pln: float = 0
    estimated_fba_aged_fee_pln: float = 0
    estimated_fba_removal_fee_pln: float = 0
    estimated_fba_liquidation_fee_pln: float = 0
    overhead_allocated_pln: float = 0
    overhead_allocation_method: str = "none"
    overhead_confidence_pct: float = 0
    cogs_per_unit_pln: float = 0
    fba_fee_per_unit_pln: float = 0
    referral_fee_per_unit_pln: float = 0
    revenue_pln: float = 0
    cogs_pln: float = 0
    amazon_fees_pln: float = 0
    cm1_profit: float = 0
    cm1_percent: float = 0
    cm2_profit: float = 0
    cm2_percent: float = 0
    np_profit: float = 0
    np_percent: float = 0
    history_orders: int = 0
    history_units: int = 0
    single_order_samples: int = 0
    confidence_score: float = 0
    cogs_source: str = "missing"
    fba_fee_source: str = "missing"
    referral_fee_source: str = "missing"
    logistics_source: str = "missing"
    shipping_charge_source: str = "missing"
    shipping_charge_mode: str = "missing"
    pack_suggestion_source: str = "default"
    flags: list[str] = []


class ProductWhatIfSummary(BaseModel):
    summary_scope: str = "page"
    total_revenue_pln: float = 0
    total_cogs_pln: float = 0
    total_fees_pln: float = 0
    total_logistics_pln: float = 0
    total_shipping_charge_pln: float = 0
    total_ads_pln: float = 0
    total_returns_net_pln: float = 0
    total_fba_storage_fee_pln: float = 0
    total_fba_aged_fee_pln: float = 0
    total_fba_removal_fee_pln: float = 0
    total_fba_liquidation_fee_pln: float = 0
    total_overhead_allocated_pln: float = 0
    total_cm1_pln: float = 0
    total_cm2_pln: float = 0
    total_np_pln: float = 0
    total_cm1_pct: float = 0
    total_cm2_pct: float = 0
    total_np_pct: float = 0
    total_offers: int = 0
    avg_confidence: float = 0


class ProductWhatIfResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    scenario_qty: int
    include_shipping_charge: bool
    summary: ProductWhatIfSummary
    items: list[ProductWhatIfItem]


# ---------------------------------------------------------------------------
# Product Drilldown
# ---------------------------------------------------------------------------

class DrilldownItem(BaseModel):
    amazon_order_id: str
    marketplace_id: str
    marketplace_code: str
    purchase_date: str
    fulfillment_channel: str = ""
    sku: Optional[str] = None
    asin: Optional[str] = None
    title: Optional[str] = None
    qty: int = 0
    currency: str = ""
    fx_rate: float = 1.0
    # Waterfall
    item_price: float = 0
    item_tax: float = 0
    promo_discount: float = 0
    revenue_pln: float = 0
    shipping_charge_pln: float = 0
    cogs_pln: float = 0
    fba_fee_pln: float = 0
    referral_fee_pln: float = 0
    amazon_fees_pln: float = 0
    logistics_pln: float = 0
    cm1_profit: float = 0
    cm1_percent: float = 0
    # Meta
    purchase_price_pln: float = 0
    price_source: Optional[str] = None
    cost_source: str = "Missing"
    # Refund info
    is_refund: bool = False
    refund_type: Optional[str] = None
    refund_amount_pln: Optional[float] = None


class DrilldownSummary(BaseModel):
    revenue_pln: float = 0
    shipping_charge_pln: float = 0
    cogs_pln: float = 0
    fees_pln: float = 0
    logistics_pln: float = 0
    cm1_pln: float = 0
    cm1_pct: float = 0
    units: int = 0


class ProductDrilldownResponse(BaseModel):
    sku: str
    total: int
    page: int
    page_size: int
    pages: int
    summary: DrilldownSummary
    items: list[DrilldownItem]


# ---------------------------------------------------------------------------
# Loss Orders
# ---------------------------------------------------------------------------

class LossOrderItem(BaseModel):
    amazon_order_id: str
    marketplace_id: str
    marketplace_code: str
    purchase_date: str
    fulfillment_channel: str = ""
    sku: Optional[str] = None
    asin: Optional[str] = None
    title: Optional[str] = None
    product_title: Optional[str] = None
    qty: int = 0
    currency: str = ""
    revenue_pln: float = 0
    shipping_charge_pln: float = 0
    cogs_pln: float = 0
    amazon_fees_pln: float = 0
    logistics_pln: float = 0
    cm1_profit: float = 0
    cm1_percent: float = 0
    primary_loss_driver: str = ""
    driver_amount: float = 0


class LossOrdersResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    total_loss_pln: float = 0
    items: list[LossOrderItem]


# ---------------------------------------------------------------------------
# Fee Breakdown (Granular P&L)
# ---------------------------------------------------------------------------

class FeeBreakdownLine(BaseModel):
    line_type: str  # "revenue", "cost", "subtotal", "section_header"
    charge_type: str
    category: str
    description: str
    profit_layer: str
    profit_bucket: Optional[str] = None
    amount_pln: float = 0
    txn_count: int = 0
    pct_of_revenue: float = 0
    source: str = ""  # "orders" | "finance" | ""


class FeeBreakdownSummary(BaseModel):
    revenue_pln: float = 0
    cogs_pln: float = 0
    cm1_pln: float = 0
    cm2_pln: float = 0
    np_pln: float = 0
    units: int = 0


class FeeBreakdownResponse(BaseModel):
    date_from: str
    date_to: str
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    total_lines: int = 0
    summary: FeeBreakdownSummary
    lines: list[FeeBreakdownLine]


# ---------------------------------------------------------------------------
# Data Quality
# ---------------------------------------------------------------------------

class DataQualityOverview(BaseModel):
    total_order_lines: int = 0
    distinct_orders: int = 0
    distinct_skus: int = 0
    cogs_coverage_pct: float = 0
    purchase_price_coverage_pct: float = 0
    fba_fee_coverage_pct: float = 0
    referral_fee_coverage_pct: float = 0
    product_mapping_pct: float = 0
    finance_match_pct: float = 0
    fx_rate_coverage: str = ""


class MissingCOGSHardSuggestion(BaseModel):
    suggested_internal_sku: Optional[str] = None
    suggested_price_pln: Optional[float] = None
    source_type: str
    source_label: str
    note: Optional[str] = None
    is_hard_source: bool = True


class MissingCOGSAICandidate(BaseModel):
    matched_internal_sku: str
    matched_title: Optional[str] = None
    confidence: float = 0
    reasoning: Optional[str] = None
    hard_price_pln: Optional[float] = None
    hard_price_source: Optional[str] = None


class MissingCOGSItem(BaseModel):
    sku: str
    asin: Optional[str] = None
    internal_sku: Optional[str] = None
    units: int = 0
    revenue_orig: float = 0
    line_count: int = 0
    current_price_pln: Optional[float] = None
    current_price_source: Optional[str] = None
    ean: Optional[str] = None
    hard_suggestion: Optional[MissingCOGSHardSuggestion] = None
    ai_candidate: Optional[MissingCOGSAICandidate] = None


class MarketplaceCoverage(BaseModel):
    marketplace_id: str
    marketplace_code: str
    total_lines: int = 0
    cogs_coverage_pct: float = 0
    fees_coverage_pct: float = 0
    fba_fee_coverage_pct: float = 0


class DataQualityPeriod(BaseModel):
    date_from: str
    date_to: str


class DataQualityResponse(BaseModel):
    period: DataQualityPeriod
    overview: DataQualityOverview
    missing_cogs_top: list[MissingCOGSItem] = []
    by_marketplace: list[MarketplaceCoverage] = []


class FeeGapReasonItem(BaseModel):
    marketplace_id: str
    marketplace_code: str
    gap_type: str
    gap_reason: str
    missing_lines: int = 0
    missing_orders: int = 0


class FeeGapOrderItem(BaseModel):
    marketplace_id: str
    marketplace_code: str
    gap_type: str
    gap_reason: str
    amazon_order_id: str
    purchase_date: str
    fulfillment_channel: str = ""
    sample_sku: Optional[str] = None
    sample_asin: Optional[str] = None
    missing_lines: int = 0
    finance_rows: int = 0
    order_fee_rows: int = 0
    fee_rows_without_sku: int = 0
    charge_types: list[str] = []
    ownership_bucket: str = ""


class FeeGapWatchItem(BaseModel):
    id: str
    gap_type: str
    gap_reason: str
    marketplace_id: str
    marketplace_code: str
    amazon_order_id: str
    sample_sku: Optional[str] = None
    sample_asin: Optional[str] = None
    fulfillment_channel: str = ""
    status: str
    first_seen_at: str
    last_seen_at: str
    last_checked_at: Optional[str] = None
    resolved_at: Optional[str] = None
    last_amazon_event_count: int = 0
    last_note: Optional[str] = None


class FeeGapDiagnosticsResponse(BaseModel):
    period: DataQualityPeriod
    overview: DataQualityOverview
    reasons: list[FeeGapReasonItem] = []
    de_finance_exists_no_fba_charge: list[FeeGapOrderItem] = []
    likely_amazon_missing: list[FeeGapOrderItem] = []
    likely_internal_fixable: list[FeeGapOrderItem] = []


class FeeGapWatchSeedResponse(BaseModel):
    period: DataQualityPeriod
    inserted: int = 0
    updated: int = 0
    open_total: int = 0


class FeeGapRecheckResponse(BaseModel):
    checked: int = 0
    resolved: int = 0
    amazon_events_available: int = 0
    still_missing: int = 0
    items: list[FeeGapWatchItem] = []


# ---------------------------------------------------------------------------
# Purchase Price Upsert (manual entry from Data Quality)
# ---------------------------------------------------------------------------

class PurchasePriceUpsertRequest(BaseModel):
    internal_sku: str
    netto_price_pln: float


class PurchasePriceUpsertResponse(BaseModel):
    internal_sku: str
    netto_price_pln: float
    status: str  # 'created' | 'updated'


class MapAndPriceRequest(BaseModel):
    sku: str
    internal_sku: str
    netto_price_pln: float


class MapAndPriceResponse(BaseModel):
    sku: str
    internal_sku: str
    netto_price_pln: float
    mapped_products: int
    price_status: str  # 'created' | 'updated'


# ---------------------------------------------------------------------------
# Profit KPIs (Executive)
# ---------------------------------------------------------------------------

class ProfitKPIMetrics(BaseModel):
    revenue_pln: float = 0
    cogs_pln: float = 0
    fees_pln: float = 0
    logistics_pln: float = 0
    cm1_pln: float = 0
    cm1_pct: float = 0
    units: int = 0
    orders: int = 0
    cogs_coverage_pct: float = 0


class ProfitKPIDeltas(BaseModel):
    revenue_delta_pct: Optional[float] = None
    cm1_delta_pct: Optional[float] = None
    units_delta_pct: Optional[float] = None
    orders_delta_pct: Optional[float] = None


class ProfitKPIResponse(BaseModel):
    current: ProfitKPIMetrics
    previous: ProfitKPIMetrics
    deltas: ProfitKPIDeltas


# ---------------------------------------------------------------------------
# Product Tasks (Pricing / Content / Watchlist)
# ---------------------------------------------------------------------------

class ProductTaskCreate(BaseModel):
    task_type: str  # pricing | content | watchlist
    sku: str
    marketplace_id: Optional[str] = None
    title: Optional[str] = None
    note: Optional[str] = None
    owner: Optional[str] = None
    source_page: str = "product_profit"
    payload_json: Optional[str] = None


class ProductTaskItem(BaseModel):
    id: str
    task_type: str
    sku: str
    marketplace_id: Optional[str] = None
    status: str
    title: Optional[str] = None
    note: Optional[str] = None
    owner: Optional[str] = None
    source_page: Optional[str] = None
    created_at: str


class ProductTaskListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[ProductTaskItem]


class ProductTaskUpdate(BaseModel):
    status: Optional[str] = None  # open | investigating | resolved
    owner: Optional[str] = None
    title: Optional[str] = None
    note: Optional[str] = None


class ProductTaskCommentCreate(BaseModel):
    comment: str
    author: Optional[str] = None


class ProductTaskCommentItem(BaseModel):
    id: int
    task_id: str
    comment: str
    author: Optional[str] = None
    created_at: str


class TaskOwnerRuleCreate(BaseModel):
    owner: str
    priority: int = 100
    task_type: Optional[str] = None
    marketplace_id: Optional[str] = None
    brand: Optional[str] = None
    is_active: bool = True


class TaskOwnerRuleItem(BaseModel):
    id: int
    owner: str
    priority: int
    task_type: Optional[str] = None
    marketplace_id: Optional[str] = None
    brand: Optional[str] = None
    is_active: bool
    created_at: str


# ---------------------------------------------------------------------------
# AI Product Match Suggestions
# ---------------------------------------------------------------------------

class AIMatchRunResponse(BaseModel):
    status: str = "ok"
    unmapped_count: int = 0
    batches_processed: int = 0
    gpt_results: int = 0
    suggestions_saved: int = 0
    errors_count: int = 0
    error_code: Optional[str] = None
    error_summary: Optional[str] = None
    message: str = ""


class BOMComponent(BaseModel):
    internal_sku: Optional[str] = None
    name: Optional[str] = None
    qty: int = 1
    unit_price_pln: Optional[float] = None


class AIMatchSuggestionItem(BaseModel):
    id: int
    unmapped_sku: str
    unmapped_asin: Optional[str] = None
    unmapped_title: Optional[str] = None
    matched_internal_sku: Optional[str] = None
    matched_title: Optional[str] = None
    matched_sku: Optional[str] = None
    confidence: float = 0
    reasoning: Optional[str] = None
    quantity_in_bundle: int = 1
    unit_price_pln: Optional[float] = None
    total_price_pln: Optional[float] = None
    bom: list[BOMComponent] = []
    status: str = "pending"
    created_at: str = ""


class AIMatchSuggestionsResponse(BaseModel):
    total: int = 0
    page: int = 1
    page_size: int = 50
    pages: int = 0
    items: list[AIMatchSuggestionItem] = []


class AIMatchActionResponse(BaseModel):
    id: int
    status: str
    unmapped_sku: str
    matched_internal_sku: Optional[str] = None
    mapped_products: Optional[int] = None
    price_status: Optional[str] = None
