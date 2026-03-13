"""Pydantic schemas — Pricing module."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class OfferPriceOut(BaseModel):
    """Single offer price snapshot."""
    id: UUID
    marketplace_id: str
    marketplace_code: str
    sku: str
    asin: str
    current_price: float
    currency: str = "EUR"
    buybox_price: Optional[float] = None
    has_buybox: bool
    status: str = "Active"
    fulfillment_channel: str = "FBA"
    our_share_pct: Optional[float] = None     # % of Buy Box 30-day avg
    fba_fee: Optional[float] = None
    referral_fee_rate: Optional[float] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PriceUpdateRequest(BaseModel):
    """Bulk price update payload."""
    sku: str
    marketplace_id: str
    new_price: float = Field(..., gt=0, description="New listing price in marketplace currency")
    reason: Optional[str] = None


class PriceUpdateResponse(BaseModel):
    sku: str
    marketplace_id: str
    old_price: float
    new_price: float
    status: str   # "queued" | "ok" | "error"
    message: Optional[str] = None


class PriceRuleOut(BaseModel):
    """Auto-pricing rule."""
    id: UUID
    sku: str
    marketplace_id: str
    strategy: str         # "buybox_win" | "min_margin" | "fixed"
    target_margin_pct: Optional[float] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    is_active: bool
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PriceRuleCreate(BaseModel):
    sku: str
    marketplace_id: str
    strategy: str = "min_margin"
    target_margin_pct: Optional[float] = Field(default=15.0, ge=0, le=100)
    min_price: Optional[float] = Field(default=None, gt=0)
    max_price: Optional[float] = Field(default=None, gt=0)


class PricingListResponse(BaseModel):
    items: list[OfferPriceOut]
    total: int
    page: int
    page_size: int


class BuyBoxStatsOut(BaseModel):
    """Buy Box win stats per marketplace."""
    marketplace_id: str
    marketplace_code: str
    total_active_offers: int
    active_offers: int = 0
    inactive_offers: int = 0
    fba_offers: int = 0
    fbm_offers: int = 0
    buybox_wins: int
    buybox_win_pct: float
    avg_price_gap: Optional[float] = None    # our price - buybox price
    last_sync: Optional[datetime] = None
