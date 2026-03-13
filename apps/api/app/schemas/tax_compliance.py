"""Pydantic schemas for the Tax Compliance module."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


# ── Shared ──────────────────────────────────────────────────────────

class PaginatedResponse(BaseModel):
    total: int = 0
    page: int = 1
    page_size: int = 50
    items: list = []


# ── VAT Classification ──────────────────────────────────────────────

class VatEventOut(BaseModel):
    id: Optional[int] = None
    order_id: Optional[str] = None
    order_line_id: Optional[str] = None
    marketplace: Optional[str] = None
    event_type: Optional[str] = None
    event_date: Optional[str] = None
    sku: Optional[str] = None
    asin: Optional[str] = None
    seller_country: Optional[str] = None
    warehouse_country: Optional[str] = None
    destination_country: Optional[str] = None
    consumption_country: Optional[str] = None
    vat_classification: Optional[str] = None
    tax_rate: Optional[float] = None
    amount_net: Optional[float] = None
    amount_vat: Optional[float] = None
    amount_gross: Optional[float] = None
    currency: Optional[str] = None
    amount_eur: Optional[float] = None
    evidence_status: Optional[str] = None
    confidence_score: Optional[float] = None
    is_manual_override: Optional[bool] = None
    reviewed_by: Optional[str] = None
    created_at: Optional[str] = None


class ClassifyRequest(BaseModel):
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    reprocess: bool = False


class OverrideClassificationRequest(BaseModel):
    new_classification: str
    reviewer: Optional[str] = None


# ── OSS ─────────────────────────────────────────────────────────────

class OssReturnLineOut(BaseModel):
    id: Optional[int] = None
    consumption_country: Optional[str] = None
    vat_rate: Optional[float] = None
    taxable_amount_eur: Optional[float] = None
    vat_amount_eur: Optional[float] = None
    order_count: Optional[int] = None
    correction_flag: Optional[int] = None


class OssReturnPeriodOut(BaseModel):
    id: Optional[int] = None
    year: Optional[int] = None
    quarter: Optional[int] = None
    period_ref: Optional[str] = None
    status: Optional[str] = None
    total_taxable_eur: Optional[float] = None
    total_vat_eur: Optional[float] = None
    country_count: Optional[int] = None
    transaction_count: Optional[int] = None
    filed_at: Optional[str] = None
    created_at: Optional[str] = None
    lines: list[OssReturnLineOut] = []


class BuildOssPeriodRequest(BaseModel):
    year: Optional[int] = None
    quarter: Optional[int] = None


# ── Evidence ────────────────────────────────────────────────────────

class EvidenceRecordOut(BaseModel):
    id: Optional[int] = None
    order_id: Optional[str] = None
    marketplace: Optional[str] = None
    evidence_status: Optional[str] = None
    proof_transport: Optional[int] = None
    proof_delivery: Optional[int] = None
    proof_order: Optional[int] = None
    proof_payment: Optional[int] = None
    proofs_collected: Optional[int] = None
    proofs_required: Optional[int] = None
    tracking_number: Optional[str] = None
    carrier: Optional[str] = None
    created_at: Optional[str] = None


class EvidenceSummaryOut(BaseModel):
    total: int = 0
    complete: int = 0
    partial: int = 0
    missing: int = 0
    completeness_pct: float = 0
    suspended_count: int = 0


# ── FBA Movements ───────────────────────────────────────────────────

class FbaMovementOut(BaseModel):
    id: Optional[int] = None
    movement_ref: Optional[str] = None
    movement_date: Optional[str] = None
    sku: Optional[str] = None
    asin: Optional[str] = None
    from_country: Optional[str] = None
    to_country: Optional[str] = None
    quantity: Optional[int] = None
    vat_treatment: Optional[str] = None
    matching_pair_status: Optional[str] = None


# ── Local VAT ───────────────────────────────────────────────────────

class LocalVatOut(BaseModel):
    id: Optional[int] = None
    order_id: Optional[str] = None
    warehouse_country: Optional[str] = None
    event_date: Optional[str] = None
    sku: Optional[str] = None
    amount_net: Optional[float] = None
    amount_vat: Optional[float] = None
    tax_rate: Optional[float] = None
    currency: Optional[str] = None
    filing_status: Optional[str] = None


class LocalVatSummaryItem(BaseModel):
    country: Optional[str] = None
    total_net: float = 0
    total_vat: float = 0
    transaction_count: int = 0
    currency: Optional[str] = None


# ── Amazon Clearing ─────────────────────────────────────────────────

class ReconciliationOut(BaseModel):
    id: Optional[int] = None
    settlement_id: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    gross_sales: Optional[float] = None
    vat_oss: Optional[float] = None
    vat_local: Optional[float] = None
    amazon_fees: Optional[float] = None
    refunds: Optional[float] = None
    ads: Optional[float] = None
    payout_net: Optional[float] = None
    computed_net: Optional[float] = None
    difference_amount: Optional[float] = None
    status: Optional[str] = None


class RunReconciliationRequest(BaseModel):
    days_back: int = 60


# ── Filing Readiness ────────────────────────────────────────────────

class FilingReadinessOut(BaseModel):
    snapshot: Optional[dict] = None
    blockers: list = []


# ── Audit Archive ───────────────────────────────────────────────────

class GenerateAuditPackRequest(BaseModel):
    period_type: str = "quarter"
    period_ref: Optional[str] = None


# ── Compliance Issues ───────────────────────────────────────────────

class ComplianceIssueOut(BaseModel):
    id: Optional[int] = None
    issue_type: Optional[str] = None
    severity: Optional[str] = None
    source_ref: Optional[str] = None
    country: Optional[str] = None
    marketplace: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[str] = None


class AssignIssueRequest(BaseModel):
    owner: str


class ResolveIssueRequest(BaseModel):
    resolver: Optional[str] = None


# ── VAT Rates ───────────────────────────────────────────────────────

class UpsertVatRateRequest(BaseModel):
    country: str
    rate_type: str = "standard"
    rate: float
    valid_from: Optional[date] = None


# ── ECB Rates ───────────────────────────────────────────────────────

class SyncEcbRatesRequest(BaseModel):
    days_back: int = 30


# ── Tax Overview ────────────────────────────────────────────────────

class TaxOverviewOut(BaseModel):
    classification_summary: Optional[dict] = None
    oss_summary: Optional[dict] = None
    local_vat_summary: Optional[dict] = None
    evidence_summary: Optional[dict] = None
    movements_summary: Optional[dict] = None
    reconciliation_summary: Optional[dict] = None
    filing_readiness: Optional[dict] = None
    open_issues: int = 0
    p1_issues: int = 0
