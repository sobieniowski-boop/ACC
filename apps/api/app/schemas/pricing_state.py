"""Pydantic schemas for Pricing State (snapshots, rules, recommendations)."""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

class PricingSnapshotOut(BaseModel):
    id: int
    seller_sku: str
    asin: Optional[str] = None
    marketplace_id: str
    our_price: Optional[float] = None
    our_currency: str = "EUR"
    fulfillment_channel: Optional[str] = None
    buybox_price: Optional[float] = None
    buybox_landed_price: Optional[float] = None
    has_buybox: bool = False
    is_featured_merchant: bool = False
    buybox_seller_id: Optional[str] = None
    lowest_price_new: Optional[float] = None
    num_offers_new: Optional[int] = None
    num_offers_used: Optional[int] = None
    bsr_rank: Optional[int] = None
    bsr_category: Optional[str] = None
    price_vs_buybox_pct: Optional[float] = None
    source: str = ""
    observed_at: Optional[str] = None


class SnapshotHistoryResponse(BaseModel):
    seller_sku: str
    marketplace_id: str
    count: int
    snapshots: list[PricingSnapshotOut]


class BuyBoxOverviewItem(BaseModel):
    seller_sku: str
    asin: Optional[str] = None
    marketplace_id: str
    our_price: Optional[float] = None
    our_currency: str = "EUR"
    fulfillment_channel: Optional[str] = None
    buybox_price: Optional[float] = None
    has_buybox: bool = False
    is_featured_merchant: bool = False
    lowest_price_new: Optional[float] = None
    num_offers_new: Optional[int] = None
    price_vs_buybox_pct: Optional[float] = None
    source: str = ""
    observed_at: Optional[str] = None


class BuyBoxOverviewResponse(BaseModel):
    count: int
    items: list[BuyBoxOverviewItem]


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

class PricingRuleCreate(BaseModel):
    seller_sku: Optional[str] = None
    marketplace_id: Optional[str] = None
    rule_type: str = Field(..., description="min_margin | max_deviation | floor_price | ceiling_price")
    min_margin_pct: Optional[float] = Field(None, ge=0, le=100)
    max_price_deviation_pct: Optional[float] = Field(None, ge=0, le=200)
    floor_price: Optional[float] = Field(None, gt=0)
    ceiling_price: Optional[float] = Field(None, gt=0)
    target_margin_pct: Optional[float] = Field(None, ge=0, le=100)
    strategy: str = "monitor"
    is_active: bool = True
    priority: int = Field(100, ge=1, le=9999)


class PricingRuleOut(BaseModel):
    id: int
    seller_sku: Optional[str] = None
    marketplace_id: Optional[str] = None
    rule_type: str
    min_margin_pct: Optional[float] = None
    max_price_deviation_pct: Optional[float] = None
    floor_price: Optional[float] = None
    ceiling_price: Optional[float] = None
    target_margin_pct: Optional[float] = None
    strategy: str = "monitor"
    is_active: bool = True
    priority: int = 100
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PricingRuleListResponse(BaseModel):
    count: int
    rules: list[PricingRuleOut]


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

class PricingRecommendationOut(BaseModel):
    id: int
    seller_sku: str
    asin: Optional[str] = None
    marketplace_id: str
    current_price: Optional[float] = None
    recommended_price: Optional[float] = None
    buybox_price: Optional[float] = None
    price_delta: Optional[float] = None
    price_delta_pct: Optional[float] = None
    reason_code: str
    reason_text: Optional[str] = None
    confidence: Optional[float] = None
    rule_id: Optional[int] = None
    snapshot_id: Optional[int] = None
    status: str = "pending"
    created_at: Optional[str] = None
    expires_at: Optional[str] = None


class RecommendationListResponse(BaseModel):
    count: int
    recommendations: list[PricingRecommendationOut]


class RecommendationDecision(BaseModel):
    decision: str = Field(..., description="accepted | dismissed")
    decided_by: str = "user"


# ---------------------------------------------------------------------------
# Capture / Evaluate
# ---------------------------------------------------------------------------

class CaptureResult(BaseModel):
    marketplace_id: str
    snapshots: int = 0
    status: str = "ok"


class EvalResult(BaseModel):
    evaluated: int = 0
    recommendations_created: int = 0
    no_rules: bool = False


class CaptureAllResult(BaseModel):
    marketplaces: int = 0
    snapshots: int = 0
    errors: int = 0


class EvalAllResult(BaseModel):
    marketplaces: int = 0
    evaluated: int = 0
    recommendations: int = 0
    expired: int = 0
