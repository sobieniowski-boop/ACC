"""Pydantic schemas — AI Recommendations module."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict


class AIRecommendationOut(BaseModel):
    id: int
    rec_type: str               # pricing | reorder | listing | ad_budget | risk
    title: str
    summary: str
    action_items: list[str]
    confidence_score: float     # 0-1
    model_used: str
    status: str                 # new | accepted | dismissed
    sku: Optional[str] = None
    marketplace_id: Optional[str] = None
    expected_impact_pln: Optional[float] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AIRecommendationListResponse(BaseModel):
    items: list[AIRecommendationOut]
    total: int
    new_count: int


class AIGenerateRequest(BaseModel):
    rec_type: str               # pricing | reorder | listing | ad_budget | risk
    sku: Optional[str] = None
    marketplace_id: Optional[str] = None
    context: Optional[dict[str, Any]] = None    # extra context for GPT


class AIStatusUpdate(BaseModel):
    status: str    # accepted | dismissed


class AIInsightSummary(BaseModel):
    """High-level AI dashboard card."""
    total_recommendations: int
    new_count: int
    accepted_count: int
    dismissed_count: int
    total_expected_impact_pln: float
    top_rec: Optional[AIRecommendationOut] = None
    last_generated_at: Optional[datetime] = None
