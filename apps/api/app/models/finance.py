"""FinanceTransaction model — Amazon settlement events."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class FinanceTransaction(Base):
    __tablename__ = "acc_finance_transaction"
    __table_args__ = (
        Index(
            "UQ_finance_tx_dedup",
            "posted_date", "marketplace_id", "amazon_order_id",
            "sku", "charge_type", "amount", "currency",
            unique=True,
            mssql_where="amazon_order_id IS NOT NULL AND sku IS NOT NULL",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    marketplace_id: Mapped[str | None] = mapped_column(
        ForeignKey("acc_marketplace.id"),
        index=True,
        nullable=True,
    )

    # Amazon identifiers
    transaction_type: Mapped[str] = mapped_column(String(100), index=True)
    # e.g. Order, Refund, ServiceFee, FBAInventoryReimbursement
    amazon_order_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    shipment_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)

    # Timing
    posted_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    settlement_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    financial_event_group_id: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)

    # Amounts (in marketplace currency)
    amount: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(5))
    charge_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # e.g. Principal, Tax, ShippingCharge, FBAPerUnitFulfillmentFee, Commission

    # PLN equivalent
    amount_pln: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    exchange_rate: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)

    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"<FinanceTx {self.transaction_type} {self.amount} {self.currency}>"
