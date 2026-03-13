"""Product and ProductVariation models."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Product(Base):
    """Master product (parent ASIN or standalone)."""
    __tablename__ = "acc_product"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    asin: Mapped[str | None] = mapped_column(String(20), unique=True, index=True, nullable=True)
    ean: Mapped[str | None] = mapped_column(String(20), index=True, nullable=True)
    sku: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)
    brand: Mapped[str | None] = mapped_column(String(100), nullable=True)
    category: Mapped[str | None] = mapped_column(String(200), nullable=True)
    subcategory: Mapped[str | None] = mapped_column(String(200), nullable=True)
    title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    image_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_parent: Mapped[bool] = mapped_column(default=False)
    parent_asin: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)

    # Cost data from MSSQL
    netto_purchase_price_pln: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    vat_rate: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True, default=23.0)

    # Product mapping — internal SKU, K-number, Ergonode PIM
    internal_sku: Mapped[str | None] = mapped_column(String(20), index=True, nullable=True)
    k_number: Mapped[str | None] = mapped_column(String(20), index=True, nullable=True)
    ergonode_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    mapping_source: Mapped[str | None] = mapped_column(String(20), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    offers: Mapped[list["Offer"]] = relationship("Offer", back_populates="product")  # noqa: F821
    order_lines: Mapped[list["OrderLine"]] = relationship("OrderLine", back_populates="product")  # noqa: F821
    inventory_snapshots: Mapped[list["InventorySnapshot"]] = relationship("InventorySnapshot", back_populates="product")  # noqa: F821

    def __repr__(self) -> str:
        return f"<Product {self.sku or self.asin}>"