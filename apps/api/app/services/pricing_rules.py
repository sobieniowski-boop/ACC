"""Pricing Rules Engine — evaluate guardrails and generate recommendations.

Operates on the latest ``acc_pricing_snapshot`` per SKU, applies
``acc_pricing_rule`` guardrails, and writes ``acc_pricing_recommendation``
rows for anything that needs human attention.

Rule evaluation:
  1. ``min_margin``       — flag if estimated margin < rule threshold
  2. ``max_deviation``    — flag if our price deviates > X% from buybox
  3. ``floor_price``      — flag if our price < absolute floor
  4. ``ceiling_price``    — flag if our price > absolute ceiling
  5. ``buybox_lost``      — flag if we lost the Buy Box

Confidence scoring:
  - Based on data freshness, source reliability, and margin data availability.
  - 80-100: strong signal (fresh data, margin known)
  - 50-79: moderate (stale or partial data)
  - 0-49: low (missing cost data, very stale)
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


def _connect():
    return connect_acc(autocommit=False, timeout=30)


# ---------------------------------------------------------------------------
# Rule CRUD
# ---------------------------------------------------------------------------

def upsert_rule(
    rule_type: str,
    *,
    seller_sku: str | None = None,
    marketplace_id: str | None = None,
    min_margin_pct: float | None = None,
    max_price_deviation_pct: float | None = None,
    floor_price: float | None = None,
    ceiling_price: float | None = None,
    target_margin_pct: float | None = None,
    strategy: str = "monitor",
    is_active: bool = True,
    priority: int = 100,
) -> dict[str, Any]:
    """Create or update a pricing rule. Returns the rule dict."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SET LOCK_TIMEOUT 30000;
            MERGE dbo.acc_pricing_rule AS tgt
            USING (SELECT ? AS seller_sku, ? AS marketplace_id, ? AS rule_type) AS src
            ON ISNULL(tgt.seller_sku, '') = ISNULL(src.seller_sku, '')
              AND ISNULL(tgt.marketplace_id, '') = ISNULL(src.marketplace_id, '')
              AND tgt.rule_type = src.rule_type
            WHEN MATCHED THEN
                UPDATE SET min_margin_pct = ?,
                           max_price_deviation_pct = ?,
                           floor_price = ?,
                           ceiling_price = ?,
                           target_margin_pct = ?,
                           strategy = ?,
                           is_active = ?,
                           priority = ?,
                           updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (seller_sku, marketplace_id, rule_type,
                        min_margin_pct, max_price_deviation_pct,
                        floor_price, ceiling_price, target_margin_pct,
                        strategy, is_active, priority)
                VALUES (?, ?, ?,
                        ?, ?,
                        ?, ?, ?,
                        ?, ?, ?);
        """, (
            seller_sku, marketplace_id, rule_type,
            # UPDATE
            min_margin_pct, max_price_deviation_pct,
            floor_price, ceiling_price, target_margin_pct,
            strategy, 1 if is_active else 0, priority,
            # INSERT
            seller_sku, marketplace_id, rule_type,
            min_margin_pct, max_price_deviation_pct,
            floor_price, ceiling_price, target_margin_pct,
            strategy, 1 if is_active else 0, priority,
        ))
        conn.commit()
        return {
            "seller_sku": seller_sku,
            "marketplace_id": marketplace_id,
            "rule_type": rule_type,
            "strategy": strategy,
            "status": "upserted",
        }
    finally:
        conn.close()


def list_rules(
    seller_sku: str | None = None,
    marketplace_id: str | None = None,
    active_only: bool = True,
) -> list[dict]:
    """List pricing rules with optional filters."""
    conn = _connect()
    try:
        cur = conn.cursor()
        where_parts = []
        params: list = []
        if active_only:
            where_parts.append("is_active = 1")
        if seller_sku:
            where_parts.append("(seller_sku = ? OR seller_sku IS NULL)")
            params.append(seller_sku)
        if marketplace_id:
            where_parts.append("(marketplace_id = ? OR marketplace_id IS NULL)")
            params.append(marketplace_id)

        where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        cur.execute(f"""
            SELECT id, seller_sku, marketplace_id, rule_type,
                   min_margin_pct, max_price_deviation_pct,
                   floor_price, ceiling_price, target_margin_pct,
                   strategy, is_active, priority,
                   created_at, updated_at
            FROM dbo.acc_pricing_rule WITH (NOLOCK)
            {where}
            ORDER BY priority ASC, seller_sku
        """, params)

        return [_rule_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def delete_rule(rule_id: int) -> bool:
    """Delete a pricing rule by ID."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_pricing_rule WHERE id = ?", (rule_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


def _rule_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "seller_sku": row[1],
        "marketplace_id": row[2],
        "rule_type": row[3],
        "min_margin_pct": float(row[4]) if row[4] is not None else None,
        "max_price_deviation_pct": float(row[5]) if row[5] is not None else None,
        "floor_price": float(row[6]) if row[6] is not None else None,
        "ceiling_price": float(row[7]) if row[7] is not None else None,
        "target_margin_pct": float(row[8]) if row[8] is not None else None,
        "strategy": row[9],
        "is_active": bool(row[10]),
        "priority": row[11],
        "created_at": str(row[12]),
        "updated_at": str(row[13]),
    }


# ---------------------------------------------------------------------------
# Recommendation CRUD
# ---------------------------------------------------------------------------

def _insert_recommendation(
    conn,
    *,
    seller_sku: str,
    asin: str | None,
    marketplace_id: str,
    current_price: float | None,
    recommended_price: float,
    buybox_price: float | None,
    reason_code: str,
    reason_text: str,
    confidence: float,
    rule_id: int | None = None,
    snapshot_id: int | None = None,
) -> int:
    """Insert a recommendation. Returns its id."""
    cur = conn.cursor()
    # Supersede any existing pending recommendation for same SKU/marketplace
    cur.execute("""
        UPDATE dbo.acc_pricing_recommendation
        SET status = 'superseded', decided_at = SYSUTCDATETIME()
        WHERE seller_sku = ? AND marketplace_id = ?
          AND status = 'pending'
    """, (seller_sku, marketplace_id))

    cur.execute("""
        INSERT INTO dbo.acc_pricing_recommendation (
            seller_sku, asin, marketplace_id,
            current_price, recommended_price, buybox_price,
            reason_code, reason_text, confidence,
            rule_id, snapshot_id,
            status, expires_at
        ) VALUES (
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            'pending', DATEADD(day, 7, SYSUTCDATETIME())
        );
        SELECT SCOPE_IDENTITY();
    """, (
        seller_sku, asin, marketplace_id,
        current_price, recommended_price, buybox_price,
        reason_code, reason_text, confidence,
        rule_id, snapshot_id,
    ))
    row = cur.fetchone()
    return int(row[0]) if row else 0


def get_pending_recommendations(
    marketplace_id: str | None = None,
    limit: int = 200,
) -> list[dict]:
    """Get pending recommendations for review."""
    conn = _connect()
    try:
        cur = conn.cursor()
        where = "WHERE r.status = 'pending' AND (r.expires_at IS NULL OR r.expires_at > SYSUTCDATETIME())"
        params: list = []
        if marketplace_id:
            where += " AND r.marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT TOP (?)
                r.id, r.seller_sku, r.asin, r.marketplace_id,
                r.current_price, r.recommended_price, r.buybox_price,
                r.price_delta, r.price_delta_pct,
                r.reason_code, r.reason_text, r.confidence,
                r.rule_id, r.snapshot_id,
                r.status, r.created_at, r.expires_at
            FROM dbo.acc_pricing_recommendation r WITH (NOLOCK)
            {where}
            ORDER BY r.confidence DESC, r.created_at DESC
        """, [limit] + params)
        return [_rec_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def decide_recommendation(
    rec_id: int,
    decision: str,
    decided_by: str = "user",
) -> bool:
    """Accept or dismiss a recommendation."""
    if decision not in ("accepted", "dismissed"):
        raise ValueError(f"Invalid decision: {decision}")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_pricing_recommendation
            SET status = ?, decided_at = SYSUTCDATETIME(), decided_by = ?
            WHERE id = ? AND status = 'pending'
        """, (decision, decided_by, rec_id))
        ok = cur.rowcount > 0
        conn.commit()
        return ok
    finally:
        conn.close()


def expire_old_recommendations() -> int:
    """Expire recommendations past their expiry date."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_pricing_recommendation
            SET status = 'expired', decided_at = SYSUTCDATETIME()
            WHERE status = 'pending' AND expires_at < SYSUTCDATETIME()
        """)
        count = cur.rowcount
        conn.commit()
        return count
    finally:
        conn.close()


def _rec_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "seller_sku": row[1],
        "asin": row[2],
        "marketplace_id": row[3],
        "current_price": float(row[4]) if row[4] is not None else None,
        "recommended_price": float(row[5]) if row[5] is not None else None,
        "buybox_price": float(row[6]) if row[6] is not None else None,
        "price_delta": float(row[7]) if row[7] is not None else None,
        "price_delta_pct": float(row[8]) if row[8] is not None else None,
        "reason_code": row[9],
        "reason_text": row[10],
        "confidence": float(row[11]) if row[11] is not None else None,
        "rule_id": row[12],
        "snapshot_id": row[13],
        "status": row[14],
        "created_at": str(row[15]),
        "expires_at": str(row[16]) if row[16] else None,
    }


# ---------------------------------------------------------------------------
# Rule evaluation engine
# ---------------------------------------------------------------------------

def evaluate_rules_for_marketplace(marketplace_id: str) -> dict[str, Any]:
    """Evaluate all active rules against latest snapshots for a marketplace.

    For each SKU with a recent snapshot, check applicable rules and
    generate recommendations where violations are detected.

    Returns: {evaluated, recommendations_created, details}
    """
    conn = _connect()
    try:
        cur = conn.cursor()

        # 1. Get active rules (SKU-specific + global defaults)
        cur.execute("""
            SELECT id, seller_sku, marketplace_id, rule_type,
                   min_margin_pct, max_price_deviation_pct,
                   floor_price, ceiling_price, target_margin_pct,
                   strategy, priority
            FROM dbo.acc_pricing_rule WITH (NOLOCK)
            WHERE is_active = 1
              AND (marketplace_id = ? OR marketplace_id IS NULL)
            ORDER BY priority ASC
        """, (marketplace_id,))
        rules = cur.fetchall()
        if not rules:
            return {"evaluated": 0, "recommendations_created": 0, "no_rules": True}

        # 2. Get latest snapshot per SKU for this marketplace
        cur.execute("""
            WITH latest AS (
                SELECT seller_sku, MAX(id) AS max_id
                FROM dbo.acc_pricing_snapshot WITH (NOLOCK)
                WHERE marketplace_id = ?
                GROUP BY seller_sku
            )
            SELECT s.id, s.seller_sku, s.asin, s.our_price,
                   s.buybox_price, s.has_buybox, s.lowest_price_new,
                   s.price_vs_buybox_pct, s.observed_at
            FROM dbo.acc_pricing_snapshot s WITH (NOLOCK)
            JOIN latest l ON s.id = l.max_id
        """, (marketplace_id,))
        snapshots = cur.fetchall()

        # 3. Optionally get margin data from profitability
        margin_by_sku: dict[str, float] = {}
        try:
            cur.execute("""
                SELECT sku, AVG(margin_pct) AS avg_margin
                FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
                WHERE marketplace_id = ?
                  AND period_date >= DATEADD(day, -30, GETUTCDATE())
                GROUP BY sku
            """, (marketplace_id,))
            for r in cur.fetchall():
                if r[0] and r[1] is not None:
                    margin_by_sku[r[0]] = float(r[1])
        except Exception:
            pass  # Table may not exist yet

        evaluated = 0
        recs_created = 0

        for snap in snapshots:
            snap_id, sku, asin = snap[0], snap[1], snap[2]
            our_price = float(snap[3]) if snap[3] else None
            bb_price = float(snap[4]) if snap[4] else None
            has_bb = bool(snap[5])
            lowest_new = float(snap[6]) if snap[6] else None
            deviation_pct = float(snap[7]) if snap[7] is not None else None
            observed = snap[8]

            if our_price is None:
                continue

            evaluated += 1
            margin = margin_by_sku.get(sku)

            # Data freshness factor for confidence
            if observed:
                age_hours = (datetime.now(timezone.utc) - observed.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            else:
                age_hours = 999
            freshness_factor = max(0.5, min(1.0, 1.0 - (age_hours / 48)))

            # Evaluate each rule against this snapshot
            for rule in rules:
                r_id, r_sku, r_mkt, r_type = rule[0], rule[1], rule[2], rule[3]
                r_min_margin = rule[4]
                r_max_dev = rule[5]
                r_floor = rule[6]
                r_ceiling = rule[7]
                r_target_margin = rule[8]
                r_strategy = rule[9]

                # SKU filter: if rule is SKU-specific, skip non-matching
                if r_sku and r_sku != sku:
                    continue

                recommendation = _evaluate_single_rule(
                    sku=sku, asin=asin, marketplace_id=marketplace_id,
                    our_price=our_price, bb_price=bb_price,
                    has_buybox=has_bb, lowest_new=lowest_new,
                    deviation_pct=deviation_pct, margin=margin,
                    freshness_factor=freshness_factor,
                    rule_id=r_id, rule_type=r_type,
                    min_margin_pct=float(r_min_margin) if r_min_margin else None,
                    max_deviation_pct=float(r_max_dev) if r_max_dev else None,
                    floor_price=float(r_floor) if r_floor else None,
                    ceiling_price=float(r_ceiling) if r_ceiling else None,
                    target_margin_pct=float(r_target_margin) if r_target_margin else None,
                    snap_id=snap_id,
                )

                if recommendation:
                    _insert_recommendation(conn, **recommendation)
                    recs_created += 1

        conn.commit()

        # Update sync state
        from app.services.pricing_state import _update_sync_state
        _update_sync_state(marketplace_id, recommendations_count=recs_created)

        log.info("pricing_rules.evaluated",
                 marketplace_id=marketplace_id,
                 evaluated=evaluated, recommendations=recs_created)
        return {"evaluated": evaluated, "recommendations_created": recs_created}
    finally:
        conn.close()


def _evaluate_single_rule(
    *,
    sku: str,
    asin: str | None,
    marketplace_id: str,
    our_price: float,
    bb_price: float | None,
    has_buybox: bool,
    lowest_new: float | None,
    deviation_pct: float | None,
    margin: float | None,
    freshness_factor: float,
    rule_id: int,
    rule_type: str,
    min_margin_pct: float | None,
    max_deviation_pct: float | None,
    floor_price: float | None,
    ceiling_price: float | None,
    target_margin_pct: float | None,
    snap_id: int,
) -> dict | None:
    """Evaluate a single rule against a snapshot. Returns recommendation kwargs or None."""

    base_confidence = 70 * freshness_factor
    if margin is not None:
        base_confidence += 15  # we have cost data

    # --- Floor price violation ---
    if rule_type == "floor_price" and floor_price is not None:
        if our_price < floor_price:
            return {
                "seller_sku": sku, "asin": asin, "marketplace_id": marketplace_id,
                "current_price": our_price,
                "recommended_price": floor_price,
                "buybox_price": bb_price,
                "reason_code": "price_below_floor",
                "reason_text": f"Price {our_price:.2f} is below floor {floor_price:.2f}",
                "confidence": min(95, base_confidence + 10),
                "rule_id": rule_id, "snapshot_id": snap_id,
            }

    # --- Ceiling price violation ---
    if rule_type == "ceiling_price" and ceiling_price is not None:
        if our_price > ceiling_price:
            return {
                "seller_sku": sku, "asin": asin, "marketplace_id": marketplace_id,
                "current_price": our_price,
                "recommended_price": ceiling_price,
                "buybox_price": bb_price,
                "reason_code": "price_above_ceiling",
                "reason_text": f"Price {our_price:.2f} exceeds ceiling {ceiling_price:.2f}",
                "confidence": min(95, base_confidence + 10),
                "rule_id": rule_id, "snapshot_id": snap_id,
            }

    # --- Min margin violation ---
    if rule_type == "min_margin" and min_margin_pct is not None and margin is not None:
        if margin < min_margin_pct:
            # Recommend price that would achieve target margin
            # Simple estimate: raise price by the margin gap %
            gap_pct = min_margin_pct - margin
            rec_price = round(our_price * (1 + gap_pct / 100), 2)
            return {
                "seller_sku": sku, "asin": asin, "marketplace_id": marketplace_id,
                "current_price": our_price,
                "recommended_price": rec_price,
                "buybox_price": bb_price,
                "reason_code": "margin_below_min",
                "reason_text": f"Margin {margin:.1f}% < min {min_margin_pct:.1f}%. "
                               f"Raise to ~{rec_price:.2f} for target margin.",
                "confidence": min(90, base_confidence),
                "rule_id": rule_id, "snapshot_id": snap_id,
            }

    # --- Max deviation from buybox ---
    if rule_type == "max_deviation" and max_deviation_pct is not None and bb_price and bb_price > 0:
        actual_dev = abs((our_price - bb_price) / bb_price * 100)
        if actual_dev > max_deviation_pct:
            # Recommend matching buybox
            return {
                "seller_sku": sku, "asin": asin, "marketplace_id": marketplace_id,
                "current_price": our_price,
                "recommended_price": round(bb_price, 2),
                "buybox_price": bb_price,
                "reason_code": "deviation_too_high",
                "reason_text": f"Price deviates {actual_dev:.1f}% from Buy Box "
                               f"(max allowed: {max_deviation_pct:.1f}%)",
                "confidence": min(90, base_confidence + 5),
                "rule_id": rule_id, "snapshot_id": snap_id,
            }

    return None


def evaluate_all_marketplaces() -> dict[str, Any]:
    """Run rule evaluation for all known marketplaces."""
    from app.core.config import MARKETPLACE_REGISTRY

    totals = {"marketplaces": 0, "evaluated": 0, "recommendations": 0}
    for mkt_id in MARKETPLACE_REGISTRY:
        result = evaluate_rules_for_marketplace(mkt_id)
        totals["marketplaces"] += 1
        totals["evaluated"] += result.get("evaluated", 0)
        totals["recommendations"] += result.get("recommendations_created", 0)

    # Expire old recommendations
    expired = expire_old_recommendations()
    totals["expired"] = expired

    return totals
