"""InventorySnapshot model."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Date, ForeignKey, Index, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class InventorySnapshot(Base):
    __tablename__ = "acc_inventory_snapshot"
    __table_args__ = (
        Index(
            "UQ_inventory_snap_dedup",
            "product_id", "marketplace_id", "snapshot_date",
            unique=True,
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    product_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("acc_product.id"), index=True)
    marketplace_id: Mapped[str] = mapped_column(ForeignKey("acc_marketplace.id"), index=True)

    snapshot_date: Mapped[datetime] = mapped_column(Date, index=True)  # type: ignore[assignment]
    sku: Mapped[str] = mapped_column(String(100), index=True)
    fnsku: Mapped[str | None] = mapped_column(String(20), nullable=True)
    asin: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # FBA stock
    qty_fulfillable: Mapped[int] = mapped_column(default=0)
    qty_reserved: Mapped[int] = mapped_column(default=0)
    qty_inbound: Mapped[int] = mapped_column(default=0)
    qty_unfulfillable: Mapped[int] = mapped_column(default=0)

    # DOI (Days of Inventory)
    avg_daily_sales_7d: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    doi: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)

    # Value
    inventory_value_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    product: Mapped["Product"] = relationship("Product", back_populates="inventory_snapshots")  # noqa: F821
    marketplace: Mapped["Marketplace"] = relationship("Marketplace", back_populates="snapshots")  # noqa: F821

    def __repr__(self) -> str:
        return f"<Inventory {self.sku}@{self.marketplace_id} qty={self.qty_fulfillable}>"
