from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class JobRunRequest(BaseModel):
    job_type: str
    marketplace_id: Optional[str] = None
    params: Optional[dict] = None


class JobRunOut(BaseModel):
    id: str
    celery_task_id: Optional[str] = None
    job_type: str
    marketplace_id: Optional[str] = None
    trigger_source: str
    status: str
    progress_pct: int
    progress_message: Optional[str] = None
    records_processed: Optional[int] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 0
    next_retry_at: Optional[datetime] = None
    last_error_code: Optional[str] = None
    last_error_kind: Optional[str] = None
    retry_policy: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    created_at: datetime
    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    total: int
    items: list[JobRunOut]
