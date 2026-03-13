from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class FinanceImportRequest(BaseModel):
    days_back: int = 30
    marketplace_id: str | None = None


class FinanceAccountItem(BaseModel):
    account_code: str
    name: str
    account_type: str
    parent_code: str | None = None
    is_active: bool = True


class FinanceTaxCodeItem(BaseModel):
    code: str
    vat_rate: float
    oss_flag: bool = False
    country: str | None = None
    description: str | None = None
    is_active: bool = True


class FinanceLedgerEntryItem(BaseModel):
    id: str
    entry_date: date
    source: str
    source_ref: str
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    settlement_id: str | None = None
    financial_event_group_id: str | None = None
    amazon_order_id: str | None = None
    transaction_type: str | None = None
    charge_type: str | None = None
    currency: str
    amount: float
    fx_rate: float
    amount_base: float
    base_currency: str
    account_code: str
    tax_code: str | None = None
    country: str | None = None
    sku: str | None = None
    asin: str | None = None
    description: str | None = None
    tags_json: dict[str, Any] = Field(default_factory=dict)
    reversed_entry_id: str | None = None


class FinanceLedgerListResponse(BaseModel):
    items: list[FinanceLedgerEntryItem]
    total: int


class FinanceManualLedgerCreate(BaseModel):
    entry_date: date
    source_ref: str | None = None
    marketplace_id: str | None = None
    settlement_id: str | None = None
    financial_event_group_id: str | None = None
    amazon_order_id: str | None = None
    transaction_type: str | None = None
    charge_type: str | None = None
    currency: str = "PLN"
    amount: float
    fx_rate: float = 1.0
    amount_base: float | None = None
    account_code: str
    tax_code: str | None = None
    country: str | None = None
    sku: str | None = None
    asin: str | None = None
    description: str | None = None
    tags_json: dict[str, Any] = Field(default_factory=dict)


class FinanceCreateOut(BaseModel):
    id: str
    reversed_entry_id: str | None = None


class FinancePayoutReconciliationItem(BaseModel):
    settlement_id: str
    financial_event_group_id: str | None = None
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    currency: str
    total_amount: float
    total_amount_base: float
    transaction_count: int
    posted_from: date | None = None
    posted_to: date | None = None
    id: str | None = None
    status: str
    bank_line_id: str | None = None
    matched_amount: float | None = None
    diff_amount: float | None = None
    notes: str | None = None
    bank_date: date | None = None
    bank_amount: float | None = None
    bank_currency: str | None = None
    reference: str | None = None


class FinancePayoutReconciliationListResponse(BaseModel):
    items: list[FinancePayoutReconciliationItem]
    total: int


class FinanceAutoMatchOut(BaseModel):
    matched: int
    settlements: int


class FinanceBankImportOut(BaseModel):
    filename: str
    inserted: int
    skipped: int


class FinanceSyncDiagnosticItem(BaseModel):
    financial_event_group_id: str
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    processing_status: str | None = None
    fund_transfer_status: str | None = None
    group_start: datetime | None = None
    group_end: datetime | None = None
    first_posted_at: datetime | None = None
    last_posted_at: datetime | None = None
    last_row_count: int = 0
    payload_signature: str | None = None
    event_type_counts_json: dict[str, int] = Field(default_factory=dict)
    last_synced_at: datetime | None = None
    open_refresh_after: datetime | None = None
    open_age_hours: float = 0
    cost_score: float = 0
    sync_state: str


class FinanceSyncDiagnosticsResponse(BaseModel):
    latest_watermark_from: datetime | None = None
    tracked_open_groups: int = 0
    items: list[FinanceSyncDiagnosticItem]


class FinanceCompletenessMarketplaceItem(BaseModel):
    marketplace_id: str
    marketplace_code: str
    order_days: int = 0
    finance_days: int = 0
    day_coverage_pct: float = 0
    orders_total: int = 0
    orders_with_finance: int = 0
    order_coverage_pct: float = 0
    status: str
    note: str | None = None


class FinanceCompletenessResponse(BaseModel):
    date_from: date
    date_to: date
    overall_status: str
    partial: bool = False
    note: str | None = None
    marketplaces: list[FinanceCompletenessMarketplaceItem]


class FinanceCoverageBreakdownItem(BaseModel):
    key: str
    orders_total: int = 0
    orders_with_finance: int = 0
    coverage_pct: float = 0


class FinanceGapDiagnosticsMarketplaceItem(BaseModel):
    marketplace_id: str
    marketplace_code: str
    tracked_groups: int = 0
    groups_with_rows: int = 0
    imported_rows: int = 0
    imported_orders: int = 0
    unmapped_rows: int = 0
    missing_order_rows: int = 0
    missing_order_distinct_orders: int = 0
    event_type_counts: dict[str, int] = Field(default_factory=dict)
    first_group_start: datetime | None = None
    last_group_end: datetime | None = None
    order_days: int = 0
    finance_days: int = 0
    day_coverage_pct: float = 0
    order_coverage_pct: float = 0
    imported_transaction_type_counts: dict[str, int] = Field(default_factory=dict)
    by_age_bucket: list[FinanceCoverageBreakdownItem] = Field(default_factory=list)
    by_fulfillment_channel: list[FinanceCoverageBreakdownItem] = Field(default_factory=list)
    missing_order_age_bucket_counts: dict[str, int] = Field(default_factory=dict)
    missing_order_transaction_type_counts: dict[str, int] = Field(default_factory=dict)
    missing_order_likely_cause: str | None = None
    likely_gap_driver: str | None = None
    gap_reason: str
    note: str | None = None


class FinanceGapDiagnosticsResponse(BaseModel):
    date_from: date
    date_to: date
    note: str
    marketplaces: list[FinanceGapDiagnosticsMarketplaceItem]


class FinanceRevenueIntegrityResponse(BaseModel):
    date_from: date
    date_to: date
    total_orders: int = 0
    active_orders: int = 0
    canceled_orders: int = 0
    missing_revenue_total: int = 0
    missing_revenue_active: int = 0
    missing_revenue_shipped: int = 0
    missing_revenue_unshipped: int = 0
    missing_order_total_total: int = 0
    missing_order_total_active: int = 0
    missing_order_total_shipped: int = 0
    missing_order_total_unshipped: int = 0
    shipped_missing_revenue_zero_line_headers: int = 0
    unshipped_missing_revenue_zero_line_headers: int = 0
    missing_revenue_by_status: dict[str, int] = Field(default_factory=dict)
    missing_order_total_by_status: dict[str, int] = Field(default_factory=dict)
    note: str


class FinanceDashboardSectionItem(BaseModel):
    key: str
    label: str
    status: str
    note: str | None = None


class FinanceDashboardJobItem(BaseModel):
    id: str
    job_type: str
    status: str
    progress_pct: float = 0
    progress_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    records_processed: int | None = None


class FinanceOrderSyncHealthItem(BaseModel):
    marketplace_id: str | None = None
    marketplace_code: str | None = None
    status: str
    gap_minutes: float | None = None
    note: str | None = None


class FinanceOrderSyncHealthResponse(BaseModel):
    ok: bool = False
    status: str = "unknown"
    error: str | None = None
    items: list[FinanceOrderSyncHealthItem] = Field(default_factory=list)


class FinanceDashboardResponse(BaseModel):
    date_from: date | None = None
    date_to: date | None = None
    revenue_base: float = 0
    fees_base: float = 0
    vat_base: float = 0
    profit_proxy: float = 0
    unmatched_payouts: int = 0
    ledger_rows: int = 0
    settlement_rows: int = 0
    payout_rows: int = 0
    bank_line_rows: int = 0
    completeness_status: str = "unknown"
    partial: bool = False
    note: str | None = None
    sections: list[FinanceDashboardSectionItem] = Field(default_factory=list)
    recent_jobs: list[FinanceDashboardJobItem] = Field(default_factory=list)
    completeness: FinanceCompletenessResponse | None = None
    gap_diagnostics: FinanceGapDiagnosticsResponse | None = None
    order_revenue_integrity: FinanceRevenueIntegrityResponse | None = None
    sync_diagnostics: FinanceSyncDiagnosticsResponse | None = None
    payout_reconciliation: FinancePayoutReconciliationListResponse | None = None
    order_sync: FinanceOrderSyncHealthResponse | None = None
