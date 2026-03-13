"""Pricing State API — snapshots, rules, recommendations.

Endpoints for:
  • Browsing pricing snapshot history (per SKU or Buy Box overview)
  • Managing pricing guardrail rules (CRUD)
  • Viewing and deciding on pricing recommendations
  • Triggering snapshot capture and rule evaluation
  • Self-test: end-to-end pipeline validation
"""
from __future__ import annotations

import uuid
from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from app.schemas.pricing_state import (
    BuyBoxOverviewResponse,
    CaptureAllResult,
    CaptureResult,
    EvalAllResult,
    EvalResult,
    PricingRecommendationOut,
    PricingRuleCreate,
    PricingRuleListResponse,
    PricingRuleOut,
    RecommendationDecision,
    RecommendationListResponse,
    SnapshotHistoryResponse,
)
from app.services import pricing_state, pricing_rules

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/pricing-state", tags=["pricing-state"])


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

@router.get("/snapshots/{seller_sku}", response_model=SnapshotHistoryResponse)
def snapshot_history(
    seller_sku: str,
    marketplace_id: str = Query(...),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get pricing history for a specific SKU in a marketplace."""
    snaps = pricing_state.get_snapshot_history(seller_sku, marketplace_id, limit=limit)
    return SnapshotHistoryResponse(
        seller_sku=seller_sku,
        marketplace_id=marketplace_id,
        count=len(snaps),
        snapshots=snaps,
    )


@router.get("/snapshots/{seller_sku}/latest")
def latest_snapshot(
    seller_sku: str,
    marketplace_id: str = Query(...),
):
    """Get the most recent pricing snapshot for a SKU."""
    snap = pricing_state.get_latest_snapshot(seller_sku, marketplace_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {seller_sku} in {marketplace_id}")
    return snap


@router.get("/buybox-overview", response_model=BuyBoxOverviewResponse)
def buybox_overview(
    marketplace_id: Optional[str] = Query(None),
):
    """Latest pricing snapshot per SKU with Buy Box status. Dashboard-ready."""
    items = pricing_state.get_buybox_overview(marketplace_id)
    return BuyBoxOverviewResponse(count=len(items), items=items)


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

@router.get("/rules", response_model=PricingRuleListResponse)
def list_pricing_rules(
    seller_sku: Optional[str] = Query(None),
    marketplace_id: Optional[str] = Query(None),
    active_only: bool = Query(True),
):
    """List pricing guardrail rules."""
    rules = pricing_rules.list_rules(
        seller_sku=seller_sku,
        marketplace_id=marketplace_id,
        active_only=active_only,
    )
    return PricingRuleListResponse(count=len(rules), rules=rules)


@router.post("/rules", response_model=PricingRuleOut)
def create_pricing_rule(body: PricingRuleCreate):
    """Create or update a pricing rule (upsert by sku+marketplace+type)."""
    valid_types = {"min_margin", "max_deviation", "floor_price", "ceiling_price"}
    if body.rule_type not in valid_types:
        raise HTTPException(400, f"rule_type must be one of {valid_types}")

    result = pricing_rules.upsert_rule(
        rule_type=body.rule_type,
        seller_sku=body.seller_sku,
        marketplace_id=body.marketplace_id,
        min_margin_pct=body.min_margin_pct,
        max_price_deviation_pct=body.max_price_deviation_pct,
        floor_price=body.floor_price,
        ceiling_price=body.ceiling_price,
        target_margin_pct=body.target_margin_pct,
        strategy=body.strategy,
        is_active=body.is_active,
        priority=body.priority,
    )

    # Fetch back the created/updated rule
    rules = pricing_rules.list_rules(
        seller_sku=body.seller_sku,
        marketplace_id=body.marketplace_id,
        active_only=False,
    )
    match = next((r for r in rules if r["rule_type"] == body.rule_type), None)
    if not match:
        return result
    return match


@router.delete("/rules/{rule_id}")
def delete_pricing_rule(rule_id: int):
    """Delete a pricing rule."""
    ok = pricing_rules.delete_rule(rule_id)
    if not ok:
        raise HTTPException(404, f"Rule {rule_id} not found")
    return {"deleted": True, "rule_id": rule_id}


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

@router.get("/recommendations", response_model=RecommendationListResponse)
def list_recommendations(
    marketplace_id: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    """Get pending pricing recommendations for review."""
    recs = pricing_rules.get_pending_recommendations(
        marketplace_id=marketplace_id, limit=limit,
    )
    return RecommendationListResponse(count=len(recs), recommendations=recs)


@router.post("/recommendations/{rec_id}/decide")
def decide_on_recommendation(rec_id: int, body: RecommendationDecision):
    """Accept or dismiss a pricing recommendation."""
    ok = pricing_rules.decide_recommendation(rec_id, body.decision, body.decided_by)
    if not ok:
        raise HTTPException(404, f"Recommendation {rec_id} not found or not pending")
    return {"id": rec_id, "status": body.decision}


# ---------------------------------------------------------------------------
# Capture & Evaluate
# ---------------------------------------------------------------------------

@router.post("/capture", response_model=CaptureResult)
async def capture_snapshots(
    marketplace_id: str = Query(...),
    asin_limit: int = Query(500, ge=1, le=5000),
):
    """Trigger pricing snapshot capture from SP-API for a marketplace."""
    result = await pricing_state.capture_pricing_snapshots(
        marketplace_id, asin_limit=asin_limit,
    )
    return result


@router.post("/capture/all", response_model=CaptureAllResult)
async def capture_all():
    """Capture pricing snapshots for all marketplaces."""
    return await pricing_state.capture_all_marketplaces()


@router.post("/evaluate", response_model=EvalResult)
def evaluate_rules(
    marketplace_id: str = Query(...),
):
    """Run pricing rule evaluation for a marketplace."""
    return pricing_rules.evaluate_rules_for_marketplace(marketplace_id)


@router.post("/evaluate/all", response_model=EvalAllResult)
def evaluate_all():
    """Run pricing rule evaluation for all marketplaces."""
    return pricing_rules.evaluate_all_marketplaces()


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

@router.post("/self-test")
def self_test():
    """End-to-end pipeline validation: snapshot → rule → evaluate → recommendation.

    Inserts synthetic data, runs evaluation, checks recommendation,
    then cleans up.  Returns pass/fail for each stage.
    """
    run_id = uuid.uuid4().hex[:8]
    sku = f"_SELFTEST_{run_id}"
    asin = f"B0ST{run_id[:4].upper()}"
    mkt = "A1PA6795UKMFR9"  # DE
    our_price = 9.99
    floor = 15.00  # above our_price → violation

    snap_id: int | None = None
    rule_id: int | None = None
    result = {
        "snapshot_created": False,
        "rule_applied": False,
        "recommendation_created": False,
    }

    try:
        # 1. Snapshot
        snap_id = pricing_state.record_snapshot(
            sku, mkt,
            asin=asin,
            our_price=our_price,
            buybox_price=14.50,
            has_buybox=False,
            source="self_test",
        )
        result["snapshot_created"] = snap_id is not None and snap_id > 0

        # 2. Rule
        pricing_rules.upsert_rule(
            "floor_price",
            seller_sku=sku,
            marketplace_id=mkt,
            floor_price=floor,
            strategy="monitor",
            is_active=True,
            priority=1,
        )
        # look up rule id for cleanup
        from app.core.db_connection import connect_acc
        conn = connect_acc(autocommit=False, timeout=30)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM dbo.acc_pricing_rule WITH (NOLOCK) "
                "WHERE seller_sku = ? AND marketplace_id = ? AND rule_type = 'floor_price'",
                (sku, mkt),
            )
            row = cur.fetchone()
            rule_id = int(row[0]) if row else None
        finally:
            conn.close()
        result["rule_applied"] = rule_id is not None

        # 3. Evaluate
        pricing_rules.evaluate_rules_for_marketplace(mkt)

        # 4. Check recommendation
        recs = pricing_rules.get_pending_recommendations(marketplace_id=mkt, limit=1000)
        matching = [r for r in recs if r["seller_sku"] == sku]
        if matching:
            rec = matching[0]
            result["recommendation_created"] = (
                rec["status"] == "pending"
                and rec.get("confidence", 0) > 0
                and rec["recommended_price"] != rec["current_price"]
            )

    except Exception as exc:
        log.error("pricing_state.self_test.error", error=str(exc))
    finally:
        # Cleanup
        try:
            conn = connect_acc(autocommit=False, timeout=30)
            try:
                cur = conn.cursor()
                cur.execute("SET LOCK_TIMEOUT 30000")
                cur.execute(
                    "DELETE FROM dbo.acc_pricing_recommendation WHERE seller_sku = ? AND marketplace_id = ?",
                    (sku, mkt),
                )
                if rule_id:
                    cur.execute("DELETE FROM dbo.acc_pricing_rule WHERE id = ?", (rule_id,))
                if snap_id:
                    cur.execute("DELETE FROM dbo.acc_pricing_snapshot WHERE id = ?", (snap_id,))
                conn.commit()
            finally:
                conn.close()
        except Exception as cleanup_exc:
            log.warning("pricing_state.self_test.cleanup_error", error=str(cleanup_exc))

    return result
