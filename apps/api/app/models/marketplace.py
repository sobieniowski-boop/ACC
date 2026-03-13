"""Marketplace model."""
from __future__ import annotations

from sqlalchemy import Boolean, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Marketplace(Base):
    __tablename__ = "acc_marketplace"

    id: Mapped[str] = mapped_column(String(20), primary_key=True)  # Amazon marketplace ID
    code: Mapped[str] = mapped_column(String(5), unique=True, index=True)  # DE, PL, GB …
    name: Mapped[str] = mapped_column(String(100))
    currency: Mapped[str] = mapped_column(String(5))
    timezone: Mapped[str] = mapped_column(String(50))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Relationships
    offers: Mapped[list["Offer"]] = relationship("Offer", back_populates="marketplace")  # noqa: F821
    orders: Mapped[list["AccOrder"]] = relationship("AccOrder", back_populates="marketplace")  # noqa: F821
    snapshots: Mapped[list["InventorySnapshot"]] = relationship("InventorySnapshot", back_populates="marketplace")  # noqa: F821
    campaigns: Mapped[list["AdsCampaign"]] = relationship("AdsCampaign", back_populates="marketplace")  # noqa: F821
    plans: Mapped[list["PlanMonth"]] = relationship("PlanMonth", back_populates="marketplace")  # noqa: F821

    def __repr__(self) -> str:
        return f"<Marketplace {self.code} ({self.id})>"
