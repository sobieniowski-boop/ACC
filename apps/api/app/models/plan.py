"""PlanMonth and PlanLine models — budgeting/planning module."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class PlanMonth(Base):
    __tablename__ = "acc_plan_month"
    __table_args__ = (
        UniqueConstraint("year", "month", "marketplace_id", name="uq_acc_plan_month_mkt"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    marketplace_id: Mapped[str] = mapped_column(ForeignKey("acc_marketplace.id"), index=True)

    year: Mapped[int] = mapped_column()
    month: Mapped[int] = mapped_column()
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Aggregate targets (PLN)
    target_revenue_pln: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    target_orders: Mapped[int | None] = mapped_column(nullable=True)
    target_acos: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)
    target_cm_percent: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)
    budget_ads_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="draft")  # draft / approved / locked

    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_user.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    marketplace: Mapped["Marketplace"] = relationship("Marketplace", back_populates="plans")  # noqa: F821
    lines: Mapped[list["PlanLine"]] = relationship("PlanLine", back_populates="plan_month", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<PlanMonth {self.year}-{self.month:02d} mkt={self.marketplace_id}>"


class PlanLine(Base):
    """Per-SKU / per-category plan targets."""
    __tablename__ = "acc_plan_line"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    plan_month_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("acc_plan_month.id"), index=True)
    product_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_product.id"), nullable=True, index=True)

    sku: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    category: Mapped[str | None] = mapped_column(String(200), nullable=True)

    target_units: Mapped[int | None] = mapped_column(nullable=True)
    target_revenue_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    target_price: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    target_ads_spend_pln: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)

    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    plan_month: Mapped["PlanMonth"] = relationship("PlanMonth", back_populates="lines")
    product: Mapped["Product | None"] = relationship("Product")  # noqa: F821

    def __repr__(self) -> str:
        return f"<PlanLine {self.sku or self.category}>"
