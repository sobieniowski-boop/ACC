"""AIRecommendation model."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class AIRecommendation(Base):
    __tablename__ = "acc_ai_recommendation"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    marketplace_id: Mapped[str | None] = mapped_column(ForeignKey("acc_marketplace.id"), nullable=True, index=True)
    product_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_product.id"), nullable=True, index=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Context
    recommendation_type: Mapped[str] = mapped_column(String(100), index=True)
    # e.g. pricing, reorder, listing_optimization, ad_budget, risk_flag

    # Content
    title: Mapped[str] = mapped_column(String(500))
    summary: Mapped[str] = mapped_column(Text)
    action_items: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON string of action list
    confidence_score: Mapped[float | None] = mapped_column(Numeric(5, 4), nullable=True)

    # Model metadata
    model_used: Mapped[str] = mapped_column(String(100), default="gpt-4o")
    prompt_tokens: Mapped[int | None] = mapped_column(nullable=True)
    completion_tokens: Mapped[int | None] = mapped_column(nullable=True)

    # User interaction
    status: Mapped[str] = mapped_column(String(30), default="new")  # new, accepted, dismissed, implemented
    user_feedback: Mapped[str | None] = mapped_column(Text, nullable=True)
    acted_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("acc_user.id"), nullable=True)
    acted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

    def __repr__(self) -> str:
        return f"<AIRecommendation {self.recommendation_type} {self.title[:50]}>"
