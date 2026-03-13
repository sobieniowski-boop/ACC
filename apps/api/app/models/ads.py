"""AdsCampaign and AdsCampaignDay models."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class AdsCampaign(Base):
    __tablename__ = "acc_ads_campaign"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    marketplace_id: Mapped[str] = mapped_column(ForeignKey("acc_marketplace.id"), index=True)

    campaign_id: Mapped[str] = mapped_column(String(50), index=True)
    campaign_name: Mapped[str] = mapped_column(String(500))
    campaign_type: Mapped[str] = mapped_column(String(50))  # SP, SB, SD
    targeting_type: Mapped[str | None] = mapped_column(String(50), nullable=True)  # AUTO, MANUAL
    state: Mapped[str] = mapped_column(String(20))  # enabled, paused, archived
    daily_budget: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(5), default="EUR")

    start_date: Mapped[datetime | None] = mapped_column(Date, nullable=True)  # type: ignore[assignment]
    end_date: Mapped[datetime | None] = mapped_column(Date, nullable=True)  # type: ignore[assignment]

    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    marketplace: Mapped["Marketplace"] = relationship("Marketplace", back_populates="campaigns")  # noqa: F821
    daily_stats: Mapped[list["AdsCampaignDay"]] = relationship("AdsCampaignDay", back_populates="campaign", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<AdsCampaign {self.campaign_name[:40]}>"


class AdsCampaignDay(Base):
    """Daily performance snapshot for a campaign."""
    __tablename__ = "acc_ads_campaign_day"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    campaign_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("acc_ads_campaign.id"), index=True)
    report_date: Mapped[datetime] = mapped_column(Date, index=True)  # type: ignore[assignment]

    impressions: Mapped[int] = mapped_column(default=0)
    clicks: Mapped[int] = mapped_column(default=0)
    spend: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    sales_7d: Mapped[float] = mapped_column(Numeric(12, 4), default=0)
    orders_7d: Mapped[int] = mapped_column(default=0)
    units_7d: Mapped[int] = mapped_column(default=0)
    acos: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)
    roas: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)

    # PLN equivalent
    spend_pln: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    sales_pln: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)

    campaign: Mapped["AdsCampaign"] = relationship("AdsCampaign", back_populates="daily_stats")

    def __repr__(self) -> str:
        return f"<AdsCampaignDay {self.report_date} spend={self.spend}>"
