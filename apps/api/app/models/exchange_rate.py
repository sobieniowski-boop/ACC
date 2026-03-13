"""ExchangeRate model — daily FX rates to PLN."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Date, DateTime, Numeric, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ExchangeRate(Base):
    __tablename__ = "acc_exchange_rate"
    __table_args__ = (
        UniqueConstraint("rate_date", "currency", name="uq_acc_rate_date_currency"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    rate_date: Mapped[datetime] = mapped_column(Date, index=True)  # type: ignore[assignment]
    currency: Mapped[str] = mapped_column(String(5), index=True)  # EUR, GBP, SEK…
    rate_to_pln: Mapped[float] = mapped_column(Numeric(10, 6))    # 1 currency = N PLN
    source: Mapped[str] = mapped_column(String(50), default="NBP")  # NBP or ECB

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"<ExchangeRate {self.currency}/{self.rate_date} = {self.rate_to_pln} PLN>"
