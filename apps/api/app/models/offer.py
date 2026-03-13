"""Offer model — current listing state per SKU per marketplace."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Offer(Base):
    __tablename__ = "acc_offer"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    product_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("acc_product.id"), index=True)
    marketplace_id: Mapped[str] = mapped_column(ForeignKey("acc_marketplace.id"), index=True)

    sku: Mapped[str] = mapped_column(String(100), index=True)
    asin: Mapped[str | None] = mapped_column(String(20), index=True, nullable=True)
    fnsku: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Pricing
    price: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(5), default="EUR")
    buybox_price: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    has_buybox: Mapped[bool] = mapped_column(Boolean, default=False)
    is_featured_merchant: Mapped[bool] = mapped_column(Boolean, default=False)

    # Listing state
    fulfillment_channel: Mapped[str] = mapped_column(String(20), default="FBA")  # FBA / FBM
    status: Mapped[str] = mapped_column(String(50), default="Active")  # Active/Inactive/Blocked

    # BSR
    bsr_rank: Mapped[int | None] = mapped_column(nullable=True)
    bsr_category: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # FBA fees snapshot
    fba_fee: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    referral_fee_rate: Mapped[float | None] = mapped_column(Numeric(5, 4), nullable=True)

    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    product: Mapped["Product"] = relationship("Product", back_populates="offers")  # noqa: F821
    marketplace: Mapped["Marketplace"] = relationship("Marketplace", back_populates="offers")  # noqa: F821

    def __repr__(self) -> str:
        return f"<Offer {self.sku}@{self.marketplace_id} price={self.price}>"
