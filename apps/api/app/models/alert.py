"""Alert and AlertRule models."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class AlertRule(Base):
    __tablename__ = "acc_alert_rule"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Trigger config
    rule_type: Mapped[str] = mapped_column(String(100))
    # e.g. buybox_lost, stock_low, price_drop, acos_high, cm_negative, returns_spike
    marketplace_id: Mapped[str | None] = mapped_column(ForeignKey("acc_marketplace.id"), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True)
    category: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Threshold
    threshold_value: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    threshold_operator: Mapped[str | None] = mapped_column(String(10), nullable=True)  # lt, gt, eq, lte, gte

    severity: Mapped[str] = mapped_column(String(20), default="warning")  # info, warning, critical
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_user.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    alerts: Mapped[list["Alert"]] = relationship("Alert", back_populates="rule", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<AlertRule {self.name} type={self.rule_type}>"


class Alert(Base):
    __tablename__ = "acc_alert"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    rule_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("acc_alert_rule.id"), index=True)

    marketplace_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True)

    title: Mapped[str] = mapped_column(String(500))
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str] = mapped_column(String(20), default="warning")
    current_value: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)

    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    is_resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_user.id"), nullable=True)

    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

    rule: Mapped["AlertRule"] = relationship("AlertRule", back_populates="alerts")

    def __repr__(self) -> str:
        return f"<Alert {self.title[:60]} sev={self.severity}>"
