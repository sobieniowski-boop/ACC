"""JobRun model — tracks background Celery tasks."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class JobRun(Base):
    __tablename__ = "acc_job_run"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    celery_task_id: Mapped[str | None] = mapped_column(String(100), unique=True, nullable=True, index=True)

    job_type: Mapped[str] = mapped_column(String(100), index=True)
    # e.g. sync_orders, sync_finances, sync_inventory, sync_pricing, generate_ai_report

    marketplace_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    triggered_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_user.id"), nullable=True)
    trigger_source: Mapped[str] = mapped_column(String(50), default="manual")  # manual, schedule, webhook

    # Status
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    # pending, running, success, failure, revoked

    progress_pct: Mapped[int] = mapped_column(default=0)
    progress_message: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Result
    records_processed: Mapped[int | None] = mapped_column(nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_summary: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

    def __repr__(self) -> str:
        return f"<JobRun {self.job_type} status={self.status}>"
