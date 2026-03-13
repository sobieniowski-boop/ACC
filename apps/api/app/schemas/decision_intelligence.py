"""Pydantic schemas for Decision Intelligence / Feedback Loop module."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────
class ExecutionStatus(str, Enum):
    monitoring = "monitoring"
    evaluated = "evaluated"
    expired = "expired"


class SuccessLabel(str, Enum):
    overperformed = "overperformed"    # >= 1.2
    on_target = "on_target"            # 0.8 - 1.2
    partial_success = "partial_success" # 0.4 - 0.8
    failure = "failure"                # < 0.4


# ── Execution ────────────────────────────────────────────────────
class ExecutionCreate(BaseModel):
    opportunity_id: int
    entity_type: Optional[str] = None      # sku, asin, family
    entity_id: Optional[str] = None
    action_type: str                       # price_change, content_update, ads_adjustment
    executed_by: Optional[str] = None
    monitoring_days: int = 14              # how long to monitor


class Execution(BaseModel):
    id: int
    opportunity_id: int
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    action_type: str
    executed_by: Optional[str] = None
    executed_at: Optional[str] = None
    baseline_metrics: Optional[Dict[str, Any]] = None
    expected_metrics: Optional[Dict[str, Any]] = None
    monitoring_start: Optional[str] = None
    monitoring_end: Optional[str] = None
    status: str = "monitoring"
    # joined fields
    opportunity_type: Optional[str] = None
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    title: Optional[str] = None


# ── Outcome ──────────────────────────────────────────────────────
class Outcome(BaseModel):
    id: int
    execution_id: int
    monitoring_days: int
    actual_metrics: Optional[Dict[str, Any]] = None
    expected_metrics: Optional[Dict[str, Any]] = None
    delta: Optional[Dict[str, Any]] = None
    success_score: Optional[float] = None
    success_label: Optional[str] = None
    impact_score: Optional[float] = None
    confidence_adjustment: Optional[float] = None
    evaluated_at: Optional[str] = None


class OutcomeDetail(BaseModel):
    execution: Execution
    outcomes: List[Outcome] = []
    opportunity_type: Optional[str] = None
    opportunity_title: Optional[str] = None


# ── Learning ─────────────────────────────────────────────────────
class LearningEntry(BaseModel):
    opportunity_type: str
    sample_size: int = 0
    avg_expected_profit: Optional[float] = None
    avg_actual_profit: Optional[float] = None
    prediction_accuracy: Optional[float] = None
    avg_success_score: Optional[float] = None
    confidence_adjustment: Optional[float] = None
    win_rate: Optional[float] = None
    avg_roi: Optional[float] = None
    last_updated: Optional[str] = None


class ModelAdjustment(BaseModel):
    opportunity_type: str
    impact_weight_adjustment: float = 0
    confidence_weight_adjustment: float = 0
    priority_weight_adjustment: float = 0
    reason: Optional[str] = None
    updated_at: Optional[str] = None


# ── Responses ────────────────────────────────────────────────────
class OutcomesListResponse(BaseModel):
    items: List[Dict[str, Any]]
    total: int
    pages: int


class LearningDashboardResponse(BaseModel):
    learning: List[LearningEntry]
    adjustments: List[ModelAdjustment]
    summary: Dict[str, Any]


class WeeklyReportResponse(BaseModel):
    period_start: str
    period_end: str
    top_performing: List[Dict[str, Any]]
    worst_performing: List[Dict[str, Any]]
    prediction_accuracy: float
    total_evaluated: int
    total_success: int
    insights: List[str]
