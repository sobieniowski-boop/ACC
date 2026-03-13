"""API routes — AI Recommendations module."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user, require_role, Role
from app.models.user import User
from app.models.ai import AIRecommendation
from app.schemas.ai_rec import (
    AIRecommendationOut, AIRecommendationListResponse,
    AIGenerateRequest, AIStatusUpdate, AIInsightSummary,
)

router = APIRouter(prefix="/ai", tags=["ai"])


def _to_out(rec: AIRecommendation) -> AIRecommendationOut:
    return AIRecommendationOut(
        id=rec.id,
        rec_type=rec.rec_type,
        title=rec.title,
        summary=rec.summary,
        action_items=rec.action_items or [],
        confidence_score=float(rec.confidence_score or 0),
        model_used=rec.model_used or "gpt-5.2",
        status=rec.status,
        sku=rec.sku,
        marketplace_id=rec.marketplace_id,
        expected_impact_pln=float(rec.expected_impact_pln) if rec.expected_impact_pln else None,
        created_at=rec.created_at,
    )


@router.get("/recommendations", response_model=AIRecommendationListResponse)
async def list_recommendations(
    rec_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    marketplace_id: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = select(AIRecommendation).order_by(AIRecommendation.created_at.desc())
    if rec_type:
        q = q.where(AIRecommendation.rec_type == rec_type)
    if status:
        q = q.where(AIRecommendation.status == status)
    if marketplace_id:
        q = q.where(AIRecommendation.marketplace_id == marketplace_id)
    if sku:
        q = q.where(AIRecommendation.sku.ilike(f"%{sku}%"))

    recs = (await db.execute(q)).scalars().all()
    new_count = sum(1 for r in recs if r.status == "new")

    return AIRecommendationListResponse(
        items=[_to_out(r) for r in recs],
        total=len(recs),
        new_count=new_count,
    )


@router.get("/summary", response_model=AIInsightSummary)
async def ai_summary(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    recs = (
        await db.execute(
            select(AIRecommendation).order_by(AIRecommendation.created_at.desc())
        )
    ).scalars().all()

    new_recs = [r for r in recs if r.status == "new"]
    accepted = [r for r in recs if r.status == "accepted"]
    dismissed = [r for r in recs if r.status == "dismissed"]
    total_impact = sum(
        float(r.expected_impact_pln or 0) for r in accepted
    )
    top_rec = new_recs[0] if new_recs else (recs[0] if recs else None)
    last_ts = recs[0].created_at if recs else None

    return AIInsightSummary(
        total_recommendations=len(recs),
        new_count=len(new_recs),
        accepted_count=len(accepted),
        dismissed_count=len(dismissed),
        total_expected_impact_pln=round(total_impact, 2),
        top_rec=_to_out(top_rec) if top_rec else None,
        last_generated_at=last_ts,
    )


@router.post("/generate", response_model=AIRecommendationOut, status_code=201)
async def generate_recommendation(
    payload: AIGenerateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role(Role.DIRECTOR)),
):
    """Trigger GPT-5.2 to generate a new recommendation."""
    from app.services.ai_service import generate_recommendation as gen_rec
    try:
        rec = await gen_rec(
            db=db,
            rec_type=payload.rec_type,
            sku=payload.sku,
            marketplace_id=payload.marketplace_id,
            extra_context=payload.context or {},
        )
    except Exception as exc:
        raise HTTPException(500, f"AI generation failed: {exc}") from exc
    return _to_out(rec)


@router.patch("/recommendations/{rec_id}", response_model=AIRecommendationOut)
async def update_status(
    rec_id: int,
    payload: AIStatusUpdate,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    rec = (
        await db.execute(select(AIRecommendation).where(AIRecommendation.id == rec_id))
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Recommendation not found")
    rec.status = payload.status
    await db.commit()
    await db.refresh(rec)
    return _to_out(rec)
