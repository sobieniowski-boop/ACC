from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TaxonomyPredictionItem(BaseModel):
    id: str
    marketplace_id: str | None = None
    sku: str | None = None
    asin: str | None = None
    ean: str | None = None
    suggested_brand: str | None = None
    suggested_category: str | None = None
    suggested_product_type: str | None = None
    confidence: float
    source: str
    status: str
    reason: str | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None


class TaxonomyPredictionListResponse(BaseModel):
    items: list[TaxonomyPredictionItem]
    total: int


class TaxonomyRefreshResponse(BaseModel):
    status: str
    candidates: int
    generated: int
    source_counts: dict[str, int] = Field(default_factory=dict)
    auto_applied: int


class TaxonomyReviewResponse(BaseModel):
    status: str
    prediction_id: str
    action: str
    new_status: str
