from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class PlanLineOut(BaseModel):
    id: int
    plan_id: int
    marketplace_id: str
    marketplace_code: str
    target_revenue_pln: float
    target_orders: int
    target_acos_pct: float
    target_cm_pct: float
    budget_ads_pln: float
    actual_revenue_pln: Optional[float] = None
    actual_orders: Optional[int] = None
    actual_acos_pct: Optional[float] = None
    actual_cm_pct: Optional[float] = None
    revenue_attainment_pct: Optional[float] = None
    model_config = {"from_attributes": True}


class PlanMonthOut(BaseModel):
    id: int
    year: int
    month: int
    month_label: str
    status: str
    total_target_revenue_pln: float
    total_target_budget_ads_pln: float
    total_actual_revenue_pln: Optional[float] = None
    revenue_attainment_pct: Optional[float] = None
    lines: list[PlanLineOut] = []
    created_by: Optional[str] = None
    created_at: datetime
    model_config = {"from_attributes": True}


class PlanLineCreate(BaseModel):
    marketplace_id: str
    target_revenue_pln: float = Field(..., gt=0)
    target_orders: int = Field(..., gt=0)
    target_acos_pct: float = Field(default=10.0, ge=0, le=100)
    target_cm_pct: float = Field(default=20.0, ge=-100, le=100)
    budget_ads_pln: float = Field(default=0.0, ge=0)


class PlanMonthCreate(BaseModel):
    year: int = Field(..., ge=2024, le=2035)
    month: int = Field(..., ge=1, le=12)
    lines: list[PlanLineCreate]


class PlanStatusUpdate(BaseModel):
    status: str = Field(..., pattern="^(draft|approved|locked)$")


class PlanVsActualRow(BaseModel):
    month_label: str
    target_revenue_pln: float
    actual_revenue_pln: float
    revenue_attainment_pct: float
    target_cm_pct: float
    actual_cm_pct: float
    target_acos_pct: float
    actual_acos_pct: float


class PlanVsActualResponse(BaseModel):
    rows: list[PlanVsActualRow]
    ytd_target_revenue_pln: float
    ytd_actual_revenue_pln: float
    ytd_attainment_pct: float
