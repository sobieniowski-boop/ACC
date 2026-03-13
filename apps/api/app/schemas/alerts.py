from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class AlertRuleCreate(BaseModel):
    name: str
    description: Optional[str] = None
    rule_type: str
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    category: Optional[str] = None
    threshold_value: Optional[float] = None
    threshold_operator: Optional[str] = None
    severity: str = "warning"
    is_active: bool = True


class AlertRuleOut(AlertRuleCreate):
    id: str
    created_by: Optional[str] = None
    created_at: datetime
    model_config = {"from_attributes": True}


class AlertOut(BaseModel):
    id: str
    rule_id: str
    rule_type: Optional[str] = None
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    title: str
    detail: Optional[str] = None
    detail_json: dict[str, Any] = Field(default_factory=dict)
    context_json: dict[str, Any] = Field(default_factory=dict)
    severity: str
    current_value: Optional[float] = None
    is_read: bool
    is_resolved: bool
    triggered_at: datetime
    model_config = {"from_attributes": True}


class AlertListResponse(BaseModel):
    total: int
    unread: int
    critical_count: int
    items: list[AlertOut]
