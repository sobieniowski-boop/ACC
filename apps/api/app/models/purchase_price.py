"""Purchase price history model."""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Date, DateTime, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class PurchasePrice(Base):
    """Netto purchase price history per internal SKU.

    Tracks price changes over time with validity ranges.
    Sources: 'holding' (FIFO from ERP) or 'xlsx' (official price list).
    """
    __tablename__ = "acc_purchase_price"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    internal_sku: Mapped[str] = mapped_column(String(20), index=True)
    netto_price_pln: Mapped[float] = mapped_column(Numeric(12, 4))
    valid_from: Mapped[date] = mapped_column(Date)
    valid_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    source: Mapped[str] = mapped_column(String(20))  # 'holding' or 'xlsx'
    source_document: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<PurchasePrice {self.internal_sku} {self.netto_price_pln} [{self.source}]>"
