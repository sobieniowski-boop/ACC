"""OpenAI GPT-5.2 integration — AI recommendations engine."""
from __future__ import annotations

import json
from typing import Any, Optional

import structlog
from openai import AsyncOpenAI
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.ai import AIRecommendation

log = structlog.get_logger(__name__)

_client: Optional[AsyncOpenAI] = None


def get_openai() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    return _client


SYSTEM_PROMPT = """
You are an expert Amazon e-commerce analyst for a Polish seller.
You analyze performance data and provide concise, actionable recommendations in Polish or English.
Always respond with valid JSON matching the requested schema.
"""


async def generate_recommendation(
    db: AsyncSession,
    recommendation_type: str,
    context_data: dict,
    marketplace_id: Optional[str] = None,
    product_id: Optional[str] = None,
    sku: Optional[str] = None,
) -> AIRecommendation:
    """Generate an AI recommendation using GPT-5.2."""
    prompt = _build_prompt(recommendation_type, context_data)

    client = get_openai()
    resp = await client.chat.completions.create(
        model=settings.OPENAI_MODEL,
        max_tokens=settings.OPENAI_MAX_TOKENS,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
    )

    content = resp.choices[0].message.content or "{}"
    parsed: dict = json.loads(content)
    usage = resp.usage

    rec = AIRecommendation(
        recommendation_type=recommendation_type,
        marketplace_id=marketplace_id,
        sku=sku,
        title=parsed.get("title", "Recommendation"),
        summary=parsed.get("summary", ""),
        action_items=json.dumps(parsed.get("action_items", [])),
        confidence_score=parsed.get("confidence_score"),
        model_used=settings.OPENAI_MODEL,
        prompt_tokens=usage.prompt_tokens if usage else None,
        completion_tokens=usage.completion_tokens if usage else None,
    )
    if product_id:
        rec.product_id = product_id  # type: ignore[assignment]

    db.add(rec)
    await db.commit()
    await db.refresh(rec)

    log.info(
        "ai.recommendation_generated",
        type=recommendation_type,
        sku=sku,
        tokens=usage.total_tokens if usage else None,
    )
    return rec


def _build_prompt(rec_type: str, data: dict) -> str:
    prompts = {
        "pricing": (
            "Analyze the following pricing and competition data for an Amazon listing. "
            "Identify whether the current price is optimal and suggest adjustments. "
            "Return JSON with keys: title, summary, action_items (list of strings), confidence_score (0-1). "
            f"Data: {json.dumps(data, ensure_ascii=False, default=str)}"
        ),
        "reorder": (
            "Analyze the following inventory and sales velocity data. "
            "Recommend reorder quantities and timing to avoid stockouts while minimizing storage fees. "
            "Return JSON with keys: title, summary, action_items (list of strings), confidence_score (0-1). "
            f"Data: {json.dumps(data, ensure_ascii=False, default=str)}"
        ),
        "listing_optimization": (
            "Review the following Amazon listing data. "
            "Suggest specific improvements for title, bullets, and keywords to improve ranking. "
            "Return JSON with keys: title, summary, action_items (list of strings), confidence_score (0-1). "
            f"Data: {json.dumps(data, ensure_ascii=False, default=str)}"
        ),
        "ad_budget": (
            "Analyze the following PPC campaign performance. "
            "Recommend budget allocations, bid adjustments, and keyword changes to improve TACoS. "
            "Return JSON with keys: title, summary, action_items (list of strings), confidence_score (0-1). "
            f"Data: {json.dumps(data, ensure_ascii=False, default=str)}"
        ),
        "risk_flag": (
            "Review the following account/listing risk indicators. "
            "Identify potential issues and recommend preventive actions. "
            "Return JSON with keys: title, summary, action_items (list of strings), confidence_score (0-1). "
            f"Data: {json.dumps(data, ensure_ascii=False, default=str)}"
        ),
    }
    return prompts.get(rec_type, (
        f"Analyze the following data and provide recommendations. "
        f"Return JSON with keys: title, summary, action_items, confidence_score. "
        f"Data: {json.dumps(data, ensure_ascii=False, default=str)}"
    ))
