"""AccOrder and OrderLine models."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class AccOrder(Base):
    """Amazon order header. Renamed from 'order' to avoid SQL keyword clash."""
    __tablename__ = "acc_order"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    amazon_order_id: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    marketplace_id: Mapped[str] = mapped_column(ForeignKey("acc_marketplace.id"), index=True)

    status: Mapped[str] = mapped_column(String(50), index=True)  # Shipped, Cancelled…
    fulfillment_channel: Mapped[str] = mapped_column(String(20), default="FBA")
    sales_channel: Mapped[str | None] = mapped_column(String(100), nullable=True)

    purchase_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_update_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ship_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Financials (in marketplace currency)
    order_total: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(5), default="EUR")

    # Profit fields (calculated by profit_service)
    revenue_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    vat_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    cogs_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    amazon_fees_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    shipping_surcharge_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    promo_order_fee_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    refund_commission_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    ads_cost_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    logistics_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    contribution_margin_pln: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    cm_percent: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)

    # Buyer info (minimal — no PII storage)
    buyer_country: Mapped[str | None] = mapped_column(String(5), nullable=True)
    ship_country: Mapped[str | None] = mapped_column(String(5), nullable=True)

    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    marketplace: Mapped["Marketplace"] = relationship("Marketplace", back_populates="orders")  # noqa: F821
    lines: Mapped[list["OrderLine"]] = relationship("OrderLine", back_populates="order", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<AccOrder {self.amazon_order_id} status={self.status}>"


class OrderLine(Base):
    __tablename__ = "acc_order_line"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("acc_order.id"), index=True)
    product_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_product.id"), nullable=True, index=True)

    amazon_order_item_id: Mapped[str] = mapped_column(String(50))
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    asin: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    title: Mapped[str | None] = mapped_column(String(500), nullable=True)

    quantity_ordered: Mapped[int] = mapped_column(default=1)
    quantity_shipped: Mapped[int] = mapped_column(default=0)

    item_price: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    item_tax: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    promotion_discount: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(5), default="EUR")

    # Profit fields
    cogs_pln: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    fba_fee_pln: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    referral_fee_pln: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)

    # Per-line purchase price (netto DIS, populated by sync_purchase_prices)
    purchase_price_pln: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    price_source: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Relationships
    order: Mapped["AccOrder"] = relationship("AccOrder", back_populates="lines")
    product: Mapped["Product | None"] = relationship("Product", back_populates="order_lines")  # noqa: F821

    def __repr__(self) -> str:
        return f"<OrderLine {self.sku} qty={self.quantity_ordered}>"
