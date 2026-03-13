"""Repricing Decision Engine — strategy-driven dynamic pricing.

Sprint 15 – Core engine: strategy algorithms, guardrail enforcement,
             execution proposals, approval workflow.
Sprint 16 – Auto-execution via SP-API Feeds, bulk operations,
             repricing analytics, threshold auto-approval.

Strategy types:
  buybox_match         — match the Buy Box price within margin limits
  competitive_undercut — undercut lowest competitor by configurable %
  margin_target        — price to achieve target margin %
  velocity_based       — adjust price based on sales velocity trends

Guardrails (per-strategy):
  min_price / max_price           — absolute price bounds
  min_margin_pct                  — margin floor (blocks price drops that erode margin)
  max_daily_change_pct            — caps price swing per computation cycle

Tables:
  ``acc_repricing_strategy``   — strategy definitions (CRUD)
  ``acc_repricing_execution``  — execution proposals + audit trail
  ``acc_repricing_analytics``  — daily aggregated execution metrics
"""
from __future__ import annotations

import json
import math
import asyncio
from datetime import date, datetime, timezone
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


# ── Defaults ────────────────────────────────────────────────────────────

DEFAULT_UNDERCUT_PCT = 1.0        # 1% below lowest competitor
DEFAULT_TARGET_MARGIN_PCT = 15.0  # 15% target margin
DEFAULT_MAX_DAILY_CHANGE_PCT = 10.0
DEFAULT_AMAZON_FEE_PCT = 15.0     # referral fee default

VALID_STRATEGY_TYPES = {"buybox_match", "competitive_undercut", "margin_target", "velocity_based"}
VALID_STATUSES = {"proposed", "approved", "rejected", "executed", "failed", "expired"}

# Auto-approval threshold: changes ≤ this % auto-approve when requires_approval=False
AUTO_APPROVE_MAX_CHANGE_PCT = 5.0


# ── Schema DDL ──────────────────────────────────────────────────────────

_REPRICING_SCHEMA: list[str] = [
    """
    IF OBJECT_ID('dbo.acc_repricing_strategy', 'U') IS NULL
    CREATE TABLE dbo.acc_repricing_strategy (
        id                    INT            IDENTITY(1,1) PRIMARY KEY,
        seller_sku            NVARCHAR(100)  NULL,
        marketplace_id        VARCHAR(20)    NULL,
        strategy_type         VARCHAR(30)    NOT NULL,
        is_active             BIT            NOT NULL DEFAULT 1,
        parameters            NVARCHAR(MAX)  NULL,
        min_price             DECIMAL(12,2)  NULL,
        max_price             DECIMAL(12,2)  NULL,
        min_margin_pct        DECIMAL(6,2)   NULL,
        max_daily_change_pct  DECIMAL(6,2)   NULL DEFAULT 10.0,
        requires_approval     BIT            NOT NULL DEFAULT 1,
        priority              INT            NOT NULL DEFAULT 100,
        created_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_reprice_strat_sku_mkt_type
            UNIQUE (seller_sku, marketplace_id, strategy_type)
    )
    """,
    """
    IF OBJECT_ID('dbo.acc_repricing_execution', 'U') IS NULL
    CREATE TABLE dbo.acc_repricing_execution (
        id                    BIGINT         IDENTITY(1,1) PRIMARY KEY,
        seller_sku            NVARCHAR(100)  NOT NULL,
        asin                  VARCHAR(20)    NULL,
        marketplace_id        VARCHAR(20)    NOT NULL,
        strategy_id           INT            NULL,
        strategy_type         VARCHAR(30)    NOT NULL,
        current_price         DECIMAL(12,2)  NULL,
        target_price          DECIMAL(12,2)  NOT NULL,
        final_price           DECIMAL(12,2)  NULL,
        price_change          AS (target_price - current_price) PERSISTED,
        price_change_pct      AS CASE
            WHEN current_price > 0
            THEN CAST(((target_price - current_price) / current_price * 100) AS DECIMAL(8,2))
            ELSE NULL END PERSISTED,
        estimated_margin_pct  DECIMAL(6,2)   NULL,
        buybox_price          DECIMAL(12,2)  NULL,
        competitor_lowest     DECIMAL(12,2)  NULL,
        reason_code           VARCHAR(50)    NOT NULL,
        reason_text           NVARCHAR(500)  NULL,
        guardrail_applied     NVARCHAR(200)  NULL,
        status                VARCHAR(20)    NOT NULL DEFAULT 'proposed',
        approved_by           NVARCHAR(100)  NULL,
        approved_at           DATETIME2      NULL,
        executed_at           DATETIME2      NULL,
        error_message         NVARCHAR(500)  NULL,
        created_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        expires_at            DATETIME2      NULL
    )
    """,
    """
    IF OBJECT_ID('dbo.acc_repricing_analytics', 'U') IS NULL
    CREATE TABLE dbo.acc_repricing_analytics (
        id                    INT            IDENTITY(1,1) PRIMARY KEY,
        analytics_date        DATE           NOT NULL,
        marketplace_id        VARCHAR(20)    NULL,
        proposals_created     INT            NOT NULL DEFAULT 0,
        proposals_approved    INT            NOT NULL DEFAULT 0,
        proposals_rejected    INT            NOT NULL DEFAULT 0,
        proposals_expired     INT            NOT NULL DEFAULT 0,
        executions_submitted  INT            NOT NULL DEFAULT 0,
        executions_succeeded  INT            NOT NULL DEFAULT 0,
        executions_failed     INT            NOT NULL DEFAULT 0,
        auto_approved_count   INT            NOT NULL DEFAULT 0,
        avg_price_change_pct  DECIMAL(8,2)   NULL,
        avg_margin_after      DECIMAL(6,2)   NULL,
        total_revenue_impact  DECIMAL(14,2)  NULL,
        created_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_reprice_analytics_date_mkt
            UNIQUE (analytics_date, marketplace_id)
    )
    """,
    # Sprint 16 — add columns to execution table
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_repricing_execution')
          AND name = 'feed_id'
    )
    ALTER TABLE dbo.acc_repricing_execution
    ADD feed_id NVARCHAR(100) NULL
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_repricing_execution')
          AND name = 'auto_approved'
    )
    ALTER TABLE dbo.acc_repricing_execution
    ADD auto_approved BIT NOT NULL DEFAULT 0
    """,
]


def ensure_repricing_schema() -> None:
    """Create repricing tables (idempotent)."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        for ddl in _REPRICING_SCHEMA:
            cur.execute(ddl)
        conn.commit()
        log.info("repricing_engine.schema_ensured")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Pure computation — strategy algorithms
# ═══════════════════════════════════════════════════════════════════════════

def compute_buybox_match_price(
    our_price: float,
    buybox_price: float | None,
    *,
    min_price: float | None = None,
    max_price: float | None = None,
) -> float | None:
    """Compute price to match the Buy Box winner.

    Returns target price, or None if no change needed.
    """
    if buybox_price is None or buybox_price <= 0:
        return None
    target = buybox_price
    target = _apply_price_bounds(target, min_price, max_price)
    if abs(target - our_price) < 0.01:
        return None
    return round(target, 2)


def compute_competitive_undercut_price(
    our_price: float,
    competitor_lowest: float | None,
    *,
    undercut_pct: float = DEFAULT_UNDERCUT_PCT,
    min_price: float | None = None,
    max_price: float | None = None,
) -> float | None:
    """Undercut lowest competitor by undercut_pct%.

    Returns target price, or None if no change needed.
    """
    if competitor_lowest is None or competitor_lowest <= 0:
        return None
    target = competitor_lowest * (1 - undercut_pct / 100.0)
    target = max(0.01, target)
    target = _apply_price_bounds(target, min_price, max_price)
    if abs(target - our_price) < 0.01:
        return None
    return round(target, 2)


def compute_margin_target_price(
    purchase_cost: float,
    *,
    target_margin_pct: float = DEFAULT_TARGET_MARGIN_PCT,
    amazon_fee_pct: float = DEFAULT_AMAZON_FEE_PCT,
    fba_fee: float = 0,
    shipping_cost: float = 0,
    ad_cost: float = 0,
    min_price: float | None = None,
    max_price: float | None = None,
) -> float | None:
    """Compute price that achieves target margin after all costs.

    margin = (price - total_cost) / price * 100
    price * (1 - amazon_fee_pct/100 - target_margin_pct/100) = fixed_costs
    price = fixed_costs / (1 - amazon_fee_pct/100 - target_margin_pct/100)
    """
    fixed_costs = purchase_cost + shipping_cost + fba_fee + ad_cost
    divisor = 1 - (amazon_fee_pct / 100.0) - (target_margin_pct / 100.0)
    if divisor <= 0:
        return None
    target = fixed_costs / divisor
    if target <= 0:
        return None
    target = _apply_price_bounds(target, min_price, max_price)
    return round(target, 2)


def compute_velocity_based_price(
    our_price: float,
    velocity_7d: float,
    velocity_30d: float,
    *,
    price_up_pct: float = 3.0,
    price_down_pct: float = 5.0,
    min_price: float | None = None,
    max_price: float | None = None,
) -> float | None:
    """Adjust price based on velocity trend.

    If demand is accelerating (7d >> 30d) → raise price (capture margin).
    If demand is decelerating (7d << 30d) → lower price (protect rank).
    """
    if velocity_30d <= 0:
        return None
    change_pct = ((velocity_7d - velocity_30d) / velocity_30d) * 100.0
    if change_pct > 25.0:
        # Demand accelerating → raise price
        target = our_price * (1 + price_up_pct / 100.0)
    elif change_pct < -25.0:
        # Demand decelerating → lower price
        target = our_price * (1 - price_down_pct / 100.0)
    else:
        return None  # stable, no change
    target = _apply_price_bounds(target, min_price, max_price)
    if abs(target - our_price) < 0.01:
        return None
    return round(target, 2)


def _apply_price_bounds(
    price: float,
    min_price: float | None,
    max_price: float | None,
) -> float:
    """Clip price to [min_price, max_price] bounds."""
    if min_price is not None and price < min_price:
        price = min_price
    if max_price is not None and price > max_price:
        price = max_price
    return price


def enforce_margin_guardrail(
    target_price: float,
    purchase_cost: float,
    *,
    amazon_fee_pct: float = DEFAULT_AMAZON_FEE_PCT,
    fba_fee: float = 0,
    shipping_cost: float = 0,
    ad_cost: float = 0,
    min_margin_pct: float | None = None,
) -> tuple[float, str | None]:
    """Enforce minimum margin guardrail.

    Returns (adjusted_price, guardrail_note).
    If the target price violates min_margin, compute the floor price and return it.
    """
    if min_margin_pct is None:
        return target_price, None
    amazon_fee = target_price * (amazon_fee_pct / 100.0)
    total_cost = purchase_cost + shipping_cost + amazon_fee + fba_fee + ad_cost
    margin = ((target_price - total_cost) / target_price * 100.0) if target_price > 0 else 0
    if margin >= min_margin_pct:
        return target_price, None
    # Compute minimum price for required margin
    fixed_costs = purchase_cost + shipping_cost + fba_fee + ad_cost
    divisor = 1 - (amazon_fee_pct / 100.0) - (min_margin_pct / 100.0)
    if divisor <= 0:
        return target_price, "margin_guardrail_impossible"
    floor_price = fixed_costs / divisor
    return round(floor_price, 2), "margin_floor_applied"


def enforce_daily_change_guardrail(
    current_price: float,
    target_price: float,
    max_daily_change_pct: float = DEFAULT_MAX_DAILY_CHANGE_PCT,
) -> tuple[float, str | None]:
    """Limit price change to max_daily_change_pct in a single cycle."""
    if current_price <= 0:
        return target_price, None
    change_pct = abs((target_price - current_price) / current_price * 100.0)
    if change_pct <= max_daily_change_pct:
        return target_price, None
    # Clamp to max daily change
    if target_price > current_price:
        clamped = current_price * (1 + max_daily_change_pct / 100.0)
    else:
        clamped = current_price * (1 - max_daily_change_pct / 100.0)
    return round(clamped, 2), f"daily_change_capped_{max_daily_change_pct}pct"


def estimate_margin(
    price: float,
    purchase_cost: float,
    *,
    amazon_fee_pct: float = DEFAULT_AMAZON_FEE_PCT,
    fba_fee: float = 0,
    shipping_cost: float = 0,
    ad_cost: float = 0,
) -> float:
    """Estimate margin % at a given price point."""
    if price <= 0:
        return 0.0
    amazon_fee = price * (amazon_fee_pct / 100.0)
    total_cost = purchase_cost + shipping_cost + amazon_fee + fba_fee + ad_cost
    return round((price - total_cost) / price * 100.0, 2)


# ═══════════════════════════════════════════════════════════════════════════
#  Strategy CRUD
# ═══════════════════════════════════════════════════════════════════════════

def upsert_strategy(
    strategy_type: str,
    *,
    seller_sku: str | None = None,
    marketplace_id: str | None = None,
    parameters: dict | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    min_margin_pct: float | None = None,
    max_daily_change_pct: float | None = None,
    requires_approval: bool = True,
    is_active: bool = True,
    priority: int = 100,
) -> dict[str, Any]:
    """Create or update a repricing strategy."""
    if strategy_type not in VALID_STRATEGY_TYPES:
        raise ValueError(f"Invalid strategy_type: {strategy_type}")
    params_json = json.dumps(parameters) if parameters else None
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            MERGE dbo.acc_repricing_strategy AS tgt
            USING (SELECT ? AS seller_sku, ? AS marketplace_id, ? AS strategy_type) AS src
            ON ISNULL(tgt.seller_sku, '') = ISNULL(src.seller_sku, '')
              AND ISNULL(tgt.marketplace_id, '') = ISNULL(src.marketplace_id, '')
              AND tgt.strategy_type = src.strategy_type
            WHEN MATCHED THEN
                UPDATE SET parameters = ?, min_price = ?, max_price = ?,
                           min_margin_pct = ?, max_daily_change_pct = ?,
                           requires_approval = ?, is_active = ?, priority = ?,
                           updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (seller_sku, marketplace_id, strategy_type,
                        parameters, min_price, max_price,
                        min_margin_pct, max_daily_change_pct,
                        requires_approval, is_active, priority)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            seller_sku, marketplace_id, strategy_type,
            # UPDATE
            params_json, min_price, max_price,
            min_margin_pct, max_daily_change_pct,
            1 if requires_approval else 0, 1 if is_active else 0, priority,
            # INSERT
            seller_sku, marketplace_id, strategy_type,
            params_json, min_price, max_price,
            min_margin_pct, max_daily_change_pct,
            1 if requires_approval else 0, 1 if is_active else 0, priority,
        ))
        conn.commit()
        return {
            "seller_sku": seller_sku,
            "marketplace_id": marketplace_id,
            "strategy_type": strategy_type,
            "status": "upserted",
        }
    finally:
        conn.close()


def list_strategies(
    marketplace_id: str | None = None,
    *,
    active_only: bool = True,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    """List repricing strategies with pagination."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if active_only:
            where.append("is_active = 1")
        if marketplace_id:
            where.append("(marketplace_id = ? OR marketplace_id IS NULL)")
            params.append(marketplace_id)
        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_repricing_strategy WITH (NOLOCK) WHERE {where_sql}",
            params,
        )
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT id, seller_sku, marketplace_id, strategy_type,
                   is_active, parameters, min_price, max_price,
                   min_margin_pct, max_daily_change_pct, requires_approval,
                   priority, created_at, updated_at
            FROM dbo.acc_repricing_strategy WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY priority ASC, seller_sku
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, params + [offset, limit])
        items = [_strategy_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_strategy(strategy_id: int) -> dict | None:
    """Get a single strategy by ID."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, seller_sku, marketplace_id, strategy_type,
                   is_active, parameters, min_price, max_price,
                   min_margin_pct, max_daily_change_pct, requires_approval,
                   priority, created_at, updated_at
            FROM dbo.acc_repricing_strategy WITH (NOLOCK)
            WHERE id = ?
        """, (strategy_id,))
        row = cur.fetchone()
        return _strategy_row_to_dict(row) if row else None
    finally:
        conn.close()


def delete_strategy(strategy_id: int) -> bool:
    """Delete a strategy by ID."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_repricing_strategy WHERE id = ?", (strategy_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


def _strategy_row_to_dict(row) -> dict:
    params_raw = row[5]
    try:
        params = json.loads(params_raw) if params_raw else {}
    except (json.JSONDecodeError, TypeError):
        params = {}
    return {
        "id": row[0],
        "seller_sku": row[1],
        "marketplace_id": row[2],
        "strategy_type": row[3],
        "is_active": bool(row[4]),
        "parameters": params,
        "min_price": float(row[6]) if row[6] is not None else None,
        "max_price": float(row[7]) if row[7] is not None else None,
        "min_margin_pct": float(row[8]) if row[8] is not None else None,
        "max_daily_change_pct": float(row[9]) if row[9] is not None else None,
        "requires_approval": bool(row[10]),
        "priority": row[11],
        "created_at": str(row[12]),
        "updated_at": str(row[13]),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Strategy computation pipeline
# ═══════════════════════════════════════════════════════════════════════════

def _fv(v: Any) -> float | None:
    """Safe float coercion."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def compute_repricing_proposals(
    marketplace_id: str | None = None,
    *,
    target_date: date | None = None,
) -> int:
    """Run the repricing pipeline: evaluate all active strategies, enforce
    guardrails, and create execution proposals.

    Returns count of proposals created.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        # 1. Load active strategies
        strategy_where = "is_active = 1"
        strategy_params: list[Any] = []
        if marketplace_id:
            strategy_where += " AND (marketplace_id = ? OR marketplace_id IS NULL)"
            strategy_params.append(marketplace_id)

        cur.execute(f"""
            SELECT id, seller_sku, marketplace_id, strategy_type,
                   parameters, min_price, max_price, min_margin_pct,
                   max_daily_change_pct, requires_approval, priority
            FROM dbo.acc_repricing_strategy WITH (NOLOCK)
            WHERE {strategy_where}
            ORDER BY priority ASC
        """, strategy_params)
        strategies = cur.fetchall()
        if not strategies:
            log.info("repricing_engine.no_strategies")
            return 0

        # 2. Load latest pricing snapshots per SKU
        snap_where = "1=1"
        snap_params: list[Any] = []
        if marketplace_id:
            snap_where = "marketplace_id = ?"
            snap_params = [marketplace_id]

        cur.execute(f"""
            WITH latest AS (
                SELECT seller_sku, marketplace_id, MAX(id) AS max_id
                FROM dbo.acc_pricing_snapshot WITH (NOLOCK)
                WHERE {snap_where}
                GROUP BY seller_sku, marketplace_id
            )
            SELECT s.seller_sku, s.asin, s.marketplace_id,
                   s.our_price, s.buybox_price, s.has_buybox,
                   s.lowest_price_new
            FROM dbo.acc_pricing_snapshot s WITH (NOLOCK)
            JOIN latest l ON s.id = l.max_id
        """, snap_params)
        snapshots: dict[tuple[str, str], dict] = {}
        for r in cur.fetchall():
            key = (r[0], r[2])  # (seller_sku, marketplace_id)
            snapshots[key] = {
                "seller_sku": r[0], "asin": r[1], "marketplace_id": r[2],
                "our_price": _fv(r[3]), "buybox_price": _fv(r[4]),
                "has_buybox": bool(r[5]), "lowest_price_new": _fv(r[6]),
            }

        # 3. Load profitability data (margin, purchase cost)
        margin_data: dict[tuple[str, str], dict] = {}
        try:
            cur.execute(f"""
                SELECT sku, marketplace_id,
                       AVG(margin_pct) AS avg_margin,
                       AVG(purchase_cost_pln) AS avg_purchase_cost,
                       AVG(amazon_fee_pln) AS avg_amazon_fee,
                       AVG(fba_fee_pln) AS avg_fba_fee,
                       AVG(ISNULL(revenue_pln, 0) / NULLIF(units, 0)) AS avg_revenue_per_unit
                FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
                WHERE period_date >= DATEADD(day, -60, GETUTCDATE())
                GROUP BY sku, marketplace_id
            """)
            for r in cur.fetchall():
                if r[0]:
                    key = (r[0], r[1])
                    margin_data[key] = {
                        "avg_margin": _fv(r[2]) or 0,
                        "purchase_cost": _fv(r[3]) or 0,
                        "amazon_fee": _fv(r[4]) or 0,
                        "fba_fee": _fv(r[5]) or 0,
                        "avg_revenue_per_unit": _fv(r[6]) or 0,
                    }
        except Exception:
            pass  # Table may not exist

        # 4. Load velocity data from inventory risk scores
        velocity_data: dict[tuple[str, str], dict] = {}
        try:
            cur.execute(f"""
                WITH latest AS (
                    SELECT seller_sku, marketplace_id, MAX(score_date) AS max_date
                    FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
                    WHERE {snap_where}
                    GROUP BY seller_sku, marketplace_id
                )
                SELECT s.seller_sku, s.marketplace_id,
                       s.velocity_7d, s.velocity_30d
                FROM dbo.acc_inventory_risk_score s WITH (NOLOCK)
                JOIN latest l ON s.seller_sku = l.seller_sku
                    AND s.marketplace_id = l.marketplace_id
                    AND s.score_date = l.max_date
            """, snap_params)
            for r in cur.fetchall():
                velocity_data[(r[0], r[1])] = {
                    "velocity_7d": _fv(r[2]) or 0,
                    "velocity_30d": _fv(r[3]) or 0,
                }
        except Exception:
            pass  # Table may not exist

        # 5. Expire any old proposed executions
        cur.execute("""
            UPDATE dbo.acc_repricing_execution
            SET status = 'expired'
            WHERE status = 'proposed'
              AND expires_at < SYSUTCDATETIME()
        """)

        # 6. Evaluate each strategy against matching SKUs
        proposals = 0
        for strat in strategies:
            s_id, s_sku, s_mkt, s_type = strat[0], strat[1], strat[2], strat[3]
            s_params_raw = strat[4]
            s_min_price = _fv(strat[5])
            s_max_price = _fv(strat[6])
            s_min_margin = _fv(strat[7])
            s_max_daily_change = _fv(strat[8]) or DEFAULT_MAX_DAILY_CHANGE_PCT

            try:
                s_params = json.loads(s_params_raw) if s_params_raw else {}
            except (json.JSONDecodeError, TypeError):
                s_params = {}

            # Determine which SKUs this strategy applies to
            matched_snaps = []
            if s_sku:
                # SKU-specific strategy
                key = (s_sku, s_mkt or marketplace_id or "")
                if key in snapshots:
                    matched_snaps.append(snapshots[key])
                # Also try with no marketplace filter
                if not matched_snaps and s_mkt is None:
                    for k, v in snapshots.items():
                        if k[0] == s_sku:
                            matched_snaps.append(v)
            else:
                # Global strategy — applies to all SKUs in marketplace
                for k, snap in snapshots.items():
                    if s_mkt and k[1] != s_mkt:
                        continue
                    matched_snaps.append(snap)

            for snap in matched_snaps:
                sku = snap["seller_sku"]
                mkt = snap["marketplace_id"]
                our_price = snap.get("our_price")
                if not our_price or our_price <= 0:
                    continue

                target = _compute_strategy_target(
                    strategy_type=s_type,
                    our_price=our_price,
                    buybox_price=snap.get("buybox_price"),
                    lowest_price=snap.get("lowest_price_new"),
                    margin_info=margin_data.get((sku, mkt), {}),
                    velocity_info=velocity_data.get((sku, mkt), {}),
                    params=s_params,
                    min_price=s_min_price,
                    max_price=s_max_price,
                )
                if target is None:
                    continue

                # Apply guardrails
                guardrails_applied: list[str] = []

                # Margin guardrail
                cost_info = margin_data.get((sku, mkt), {})
                purchase_cost = cost_info.get("purchase_cost", 0)
                if purchase_cost > 0 and s_min_margin is not None:
                    target, note = enforce_margin_guardrail(
                        target, purchase_cost, min_margin_pct=s_min_margin,
                    )
                    if note:
                        guardrails_applied.append(note)

                # Daily change guardrail
                target, note = enforce_daily_change_guardrail(
                    our_price, target, s_max_daily_change,
                )
                if note:
                    guardrails_applied.append(note)

                # Skip if no meaningful change after guardrails
                if abs(target - our_price) < 0.01:
                    continue

                # Estimate margin at target price
                est_margin = None
                if purchase_cost > 0:
                    est_margin = estimate_margin(target, purchase_cost)

                # Build reason
                reason_code, reason_text = _build_reason(
                    s_type, our_price, target, snap, s_params,
                )

                # Supersede existing proposed for same SKU/marketplace
                cur.execute("""
                    UPDATE dbo.acc_repricing_execution
                    SET status = 'expired'
                    WHERE seller_sku = ? AND marketplace_id = ?
                      AND status = 'proposed'
                """, (sku, mkt))

                # Insert proposal
                cur.execute("""
                    INSERT INTO dbo.acc_repricing_execution (
                        seller_sku, asin, marketplace_id,
                        strategy_id, strategy_type,
                        current_price, target_price,
                        estimated_margin_pct,
                        buybox_price, competitor_lowest,
                        reason_code, reason_text, guardrail_applied,
                        status, expires_at
                    ) VALUES (
                        ?, ?, ?,
                        ?, ?,
                        ?, ?,
                        ?,
                        ?, ?,
                        ?, ?, ?,
                        'proposed', DATEADD(day, 3, SYSUTCDATETIME())
                    )
                """, (
                    sku, snap.get("asin"), mkt,
                    s_id, s_type,
                    our_price, target,
                    est_margin,
                    snap.get("buybox_price"), snap.get("lowest_price_new"),
                    reason_code, reason_text,
                    "; ".join(guardrails_applied) if guardrails_applied else None,
                ))
                proposals += 1

        conn.commit()
        log.info("repricing_engine.proposals_created", count=proposals)
        return proposals
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _compute_strategy_target(
    *,
    strategy_type: str,
    our_price: float,
    buybox_price: float | None,
    lowest_price: float | None,
    margin_info: dict,
    velocity_info: dict,
    params: dict,
    min_price: float | None,
    max_price: float | None,
) -> float | None:
    """Dispatch to the correct strategy algorithm."""
    if strategy_type == "buybox_match":
        return compute_buybox_match_price(
            our_price, buybox_price,
            min_price=min_price, max_price=max_price,
        )
    if strategy_type == "competitive_undercut":
        undercut = params.get("undercut_pct", DEFAULT_UNDERCUT_PCT)
        return compute_competitive_undercut_price(
            our_price, lowest_price,
            undercut_pct=undercut,
            min_price=min_price, max_price=max_price,
        )
    if strategy_type == "margin_target":
        purchase_cost = margin_info.get("purchase_cost", 0)
        if purchase_cost <= 0:
            return None
        target_margin = params.get("target_margin_pct", DEFAULT_TARGET_MARGIN_PCT)
        return compute_margin_target_price(
            purchase_cost,
            target_margin_pct=target_margin,
            min_price=min_price, max_price=max_price,
        )
    if strategy_type == "velocity_based":
        v7 = velocity_info.get("velocity_7d", 0)
        v30 = velocity_info.get("velocity_30d", 0)
        if v30 <= 0:
            return None
        return compute_velocity_based_price(
            our_price, v7, v30,
            price_up_pct=params.get("price_up_pct", 3.0),
            price_down_pct=params.get("price_down_pct", 5.0),
            min_price=min_price, max_price=max_price,
        )
    return None


def _build_reason(
    strategy_type: str,
    our_price: float,
    target: float,
    snap: dict,
    params: dict,
) -> tuple[str, str]:
    """Build reason_code and reason_text for a proposal."""
    direction = "increase" if target > our_price else "decrease"
    delta = abs(target - our_price)
    pct = (delta / our_price * 100) if our_price > 0 else 0

    if strategy_type == "buybox_match":
        return "buybox_match", f"Match Buy Box {snap.get('buybox_price', '?'):.2f} ({direction} {pct:.1f}%)"
    if strategy_type == "competitive_undercut":
        return "competitive_undercut", f"Undercut lowest {snap.get('lowest_price_new', '?')} by {params.get('undercut_pct', 1)}% ({direction} {pct:.1f}%)"
    if strategy_type == "margin_target":
        return "margin_target", f"Target {params.get('target_margin_pct', 15)}% margin → {target:.2f} ({direction} {pct:.1f}%)"
    if strategy_type == "velocity_based":
        return "velocity_adjustment", f"Velocity-driven {direction} {pct:.1f}%"
    return "unknown", f"Price {direction} by {pct:.1f}%"


# ═══════════════════════════════════════════════════════════════════════════
#  Execution proposal management
# ═══════════════════════════════════════════════════════════════════════════

def get_execution_proposals(
    marketplace_id: str | None = None,
    *,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """Get paginated execution proposals."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if marketplace_id:
            where.append("marketplace_id = ?")
            params.append(marketplace_id)
        if status:
            where.append("status = ?")
            params.append(status)
        else:
            where.append("status IN ('proposed', 'approved')")
        where_sql = " AND ".join(where)

        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_repricing_execution WITH (NOLOCK) WHERE {where_sql}",
            params,
        )
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT id, seller_sku, asin, marketplace_id,
                   strategy_id, strategy_type,
                   current_price, target_price, final_price,
                   price_change, price_change_pct,
                   estimated_margin_pct,
                   buybox_price, competitor_lowest,
                   reason_code, reason_text, guardrail_applied,
                   status, approved_by, approved_at,
                   executed_at, error_message,
                   created_at, expires_at,
                   feed_id, auto_approved
            FROM dbo.acc_repricing_execution WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY created_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, params + [offset, limit])
        items = [_exec_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def approve_execution(execution_id: int, approved_by: str = "operator") -> bool:
    """Approve a proposed repricing execution."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_repricing_execution
            SET status = 'approved',
                approved_by = ?,
                approved_at = SYSUTCDATETIME()
            WHERE id = ? AND status = 'proposed'
        """, (approved_by, execution_id))
        ok = cur.rowcount > 0
        conn.commit()
        return ok
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reject_execution(execution_id: int) -> bool:
    """Reject a proposed repricing execution."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_repricing_execution
            SET status = 'rejected'
            WHERE id = ? AND status = 'proposed'
        """, (execution_id,))
        ok = cur.rowcount > 0
        conn.commit()
        return ok
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_repricing_dashboard(marketplace_id: str | None = None) -> dict[str, Any]:
    """Dashboard summary: strategy counts, proposal stats, recent activity."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        # Strategy counts
        cur.execute(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active,
                   COUNT(DISTINCT strategy_type) AS types
            FROM dbo.acc_repricing_strategy WITH (NOLOCK)
            WHERE 1=1 {mkt_filter.replace('AND', 'AND' if mkt_filter else '')}
        """, params)
        strat_row = cur.fetchone()

        # Proposal stats
        exec_params = list(params)
        cur.execute(f"""
            SELECT
                SUM(CASE WHEN status = 'proposed' THEN 1 ELSE 0 END) AS proposed,
                SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN status = 'executed' THEN 1 ELSE 0 END) AS executed,
                SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
                COUNT(*) AS total_executions,
                AVG(CASE WHEN status IN ('proposed','approved') THEN price_change_pct END) AS avg_proposed_change
            FROM dbo.acc_repricing_execution WITH (NOLOCK)
            WHERE created_at >= DATEADD(day, -30, SYSUTCDATETIME())
              {mkt_filter}
        """, exec_params)
        exec_row = cur.fetchone()

        return {
            "strategies_total": strat_row[0] if strat_row else 0,
            "strategies_active": strat_row[1] if strat_row else 0,
            "strategy_types": strat_row[2] if strat_row else 0,
            "proposed": exec_row[0] if exec_row else 0,
            "approved": exec_row[1] if exec_row else 0,
            "executed": exec_row[2] if exec_row else 0,
            "rejected": exec_row[3] if exec_row else 0,
            "total_executions_30d": exec_row[4] if exec_row else 0,
            "avg_proposed_change_pct": _fv(exec_row[5]) if exec_row else None,
        }
    finally:
        conn.close()


def get_execution_history(
    seller_sku: str,
    marketplace_id: str,
    *,
    days: int = 30,
    limit: int = 50,
) -> list[dict]:
    """Get execution history for a specific SKU."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP (?)
                   id, seller_sku, asin, marketplace_id,
                   strategy_id, strategy_type,
                   current_price, target_price, final_price,
                   price_change, price_change_pct,
                   estimated_margin_pct,
                   buybox_price, competitor_lowest,
                   reason_code, reason_text, guardrail_applied,
                   status, approved_by, approved_at,
                   executed_at, error_message,
                   created_at, expires_at,
                   feed_id, auto_approved
            FROM dbo.acc_repricing_execution WITH (NOLOCK)
            WHERE seller_sku = ? AND marketplace_id = ?
              AND created_at >= DATEADD(day, -?, SYSUTCDATETIME())
            ORDER BY created_at DESC
        """, (limit, seller_sku, marketplace_id, days))
        return [_exec_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _exec_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "seller_sku": row[1],
        "asin": row[2],
        "marketplace_id": row[3],
        "strategy_id": row[4],
        "strategy_type": row[5],
        "current_price": _fv(row[6]),
        "target_price": _fv(row[7]),
        "final_price": _fv(row[8]),
        "price_change": _fv(row[9]),
        "price_change_pct": _fv(row[10]),
        "estimated_margin_pct": _fv(row[11]),
        "buybox_price": _fv(row[12]),
        "competitor_lowest": _fv(row[13]),
        "reason_code": row[14],
        "reason_text": row[15],
        "guardrail_applied": row[16],
        "status": row[17],
        "approved_by": row[18],
        "approved_at": str(row[19]) if row[19] else None,
        "executed_at": str(row[20]) if row[20] else None,
        "error_message": row[21],
        "created_at": str(row[22]),
        "expires_at": str(row[23]) if row[23] else None,
        "feed_id": row[24] if len(row) > 24 else None,
        "auto_approved": bool(row[25]) if len(row) > 25 else False,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Auto-execution pipeline
# ═══════════════════════════════════════════════════════════════════════════

def auto_approve_proposals(marketplace_id: str | None = None) -> int:
    """Auto-approve proposals from strategies with requires_approval=False,
    if the price change is within AUTO_APPROVE_MAX_CHANGE_PCT.

    Returns count of auto-approved proposals.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "AND e.marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            UPDATE e
            SET e.status = 'approved',
                e.approved_by = 'auto',
                e.approved_at = SYSUTCDATETIME(),
                e.auto_approved = 1
            FROM dbo.acc_repricing_execution e
            JOIN dbo.acc_repricing_strategy s ON e.strategy_id = s.id
            WHERE e.status = 'proposed'
              AND s.requires_approval = 0
              AND ABS(e.price_change_pct) <= ?
              {mkt_filter}
        """, [AUTO_APPROVE_MAX_CHANGE_PCT] + params)
        count = cur.rowcount
        conn.commit()
        log.info("repricing_engine.auto_approved", count=count)
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_approved_prices(marketplace_id: str) -> dict[str, Any]:
    """Submit approved repricing proposals to Amazon via Feeds API.

    1. Reads all approved executions for the marketplace.
    2. Builds JSON_LISTINGS_FEED patches per SKU.
    3. Submits the feed via FeedsClient.
    4. Updates execution records with feed_id and status.

    Returns summary dict.
    """
    from app.core.config import settings, MARKETPLACE_REGISTRY

    mkt_info = MARKETPLACE_REGISTRY.get(marketplace_id, {})
    currency = mkt_info.get("currency", "EUR")

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, seller_sku, asin, target_price
            FROM dbo.acc_repricing_execution WITH (NOLOCK)
            WHERE marketplace_id = ?
              AND status = 'approved'
            ORDER BY id
        """, (marketplace_id,))
        rows = cur.fetchall()

        if not rows:
            return {"marketplace_id": marketplace_id, "submitted": 0, "feed_id": None}

        # Build feed messages
        messages: list[dict] = []
        exec_ids: list[int] = []
        for idx, r in enumerate(rows):
            exec_id, sku, asin, target_price = r[0], r[1], r[2], float(r[3])
            exec_ids.append(exec_id)
            messages.append({
                "messageId": idx + 1,
                "sku": sku,
                "operationType": "PATCH",
                "productType": "PRODUCT",
                "patches": [{
                    "op": "replace",
                    "path": "/attributes/purchasable_offer",
                    "value": [{
                        "currency": currency,
                        "our_price": [{
                            "schedule": [{
                                "value_with_tax": target_price,
                            }]
                        }]
                    }]
                }],
            })

        feed_payload = {
            "header": {
                "sellerId": settings.SP_API_SELLER_ID,
                "version": "2.0",
                "issueLocale": "en_US",
            },
            "messages": messages,
        }

        # Submit via SP-API Feeds
        feed_id = None
        feed_status = "unknown"
        error_msg = None
        try:
            feed_id, feed_status = _submit_price_feed(marketplace_id, feed_payload)
        except Exception as exc:
            error_msg = str(exc)[:400]
            log.error("repricing_engine.feed_submit_error", error=error_msg)

        # Update execution records
        now_status = "executed" if feed_status in ("DONE", "IN_QUEUE", "IN_PROGRESS") else "failed"
        for exec_id in exec_ids:
            target_p = next((float(r[3]) for r in rows if r[0] == exec_id), None)
            cur.execute("""
                UPDATE dbo.acc_repricing_execution
                SET status = ?,
                    final_price = ?,
                    feed_id = ?,
                    executed_at = SYSUTCDATETIME(),
                    error_message = ?
                WHERE id = ?
            """, (now_status, target_p, feed_id, error_msg, exec_id))

        conn.commit()
        log.info("repricing_engine.prices_submitted",
                 marketplace_id=marketplace_id,
                 count=len(exec_ids), feed_id=feed_id, status=feed_status)
        return {
            "marketplace_id": marketplace_id,
            "submitted": len(exec_ids),
            "feed_id": feed_id,
            "feed_status": feed_status,
            "error": error_msg,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _submit_price_feed(marketplace_id: str, feed_payload: dict) -> tuple[str | None, str]:
    """Submit JSON listings feed and poll for result.

    Returns (feed_id, processing_status).
    """
    from app.connectors.amazon_sp_api.feeds import FeedsClient

    async def _run() -> tuple[str | None, str]:
        client = FeedsClient(marketplace_id=marketplace_id)
        submitted = await client.submit_json_listings_feed(
            marketplace_ids=[marketplace_id],
            feed_payload=feed_payload,
        )
        fid = str(submitted.get("feedId") or "")
        if not fid:
            return None, "SUBMIT_FAILED"
        feed_state = await client.wait_for_feed(fid, poll_interval=15.0, max_wait=120.0)
        status = str(feed_state.get("processingStatus") or "UNKNOWN")
        return fid, status

    return asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Bulk approve / reject
# ═══════════════════════════════════════════════════════════════════════════

def bulk_approve_executions(
    execution_ids: list[int],
    approved_by: str = "operator",
) -> dict[str, Any]:
    """Approve multiple proposed executions at once."""
    if not execution_ids:
        return {"approved": 0, "skipped": 0}
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        approved = 0
        for eid in execution_ids:
            cur.execute("""
                UPDATE dbo.acc_repricing_execution
                SET status = 'approved',
                    approved_by = ?,
                    approved_at = SYSUTCDATETIME()
                WHERE id = ? AND status = 'proposed'
            """, (approved_by, eid))
            approved += cur.rowcount
        conn.commit()
        return {"approved": approved, "skipped": len(execution_ids) - approved}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bulk_reject_executions(execution_ids: list[int]) -> dict[str, Any]:
    """Reject multiple proposed executions at once."""
    if not execution_ids:
        return {"rejected": 0, "skipped": 0}
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        rejected = 0
        for eid in execution_ids:
            cur.execute("""
                UPDATE dbo.acc_repricing_execution
                SET status = 'rejected'
                WHERE id = ? AND status = 'proposed'
            """, (eid,))
            rejected += cur.rowcount
        conn.commit()
        return {"rejected": rejected, "skipped": len(execution_ids) - rejected}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Repricing analytics
# ═══════════════════════════════════════════════════════════════════════════

def compute_daily_analytics(
    target_date: date | None = None,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Compute and upsert daily repricing analytics from execution records.

    Aggregates: proposals created, approved, rejected, expired,
    executions submitted/succeeded/failed, auto-approved count,
    avg price change %, avg margin after.
    """
    d = target_date or date.today()
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [str(d), str(d)]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status IN ('approved','executed') THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
                SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) AS expired,
                SUM(CASE WHEN status = 'executed' THEN 1 ELSE 0 END) AS submitted,
                SUM(CASE WHEN status = 'executed' AND error_message IS NULL THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN auto_approved = 1 THEN 1 ELSE 0 END) AS auto_approved,
                AVG(price_change_pct) AS avg_change,
                AVG(estimated_margin_pct) AS avg_margin
            FROM dbo.acc_repricing_execution WITH (NOLOCK)
            WHERE CAST(created_at AS DATE) >= ?
              AND CAST(created_at AS DATE) <= ?
              {mkt_filter}
        """, params)
        row = cur.fetchone()
        if not row or (row[0] or 0) == 0:
            return {"date": str(d), "total": 0}

        # MERGE upsert analytics
        cur.execute(f"""
            MERGE dbo.acc_repricing_analytics AS tgt
            USING (SELECT ? AS analytics_date, ? AS marketplace_id) AS src
            ON tgt.analytics_date = src.analytics_date
              AND ISNULL(tgt.marketplace_id, '') = ISNULL(src.marketplace_id, '')
            WHEN MATCHED THEN
                UPDATE SET
                    proposals_created = ?,
                    proposals_approved = ?,
                    proposals_rejected = ?,
                    proposals_expired = ?,
                    executions_submitted = ?,
                    executions_succeeded = ?,
                    executions_failed = ?,
                    auto_approved_count = ?,
                    avg_price_change_pct = ?,
                    avg_margin_after = ?
            WHEN NOT MATCHED THEN
                INSERT (analytics_date, marketplace_id, proposals_created,
                        proposals_approved, proposals_rejected, proposals_expired,
                        executions_submitted, executions_succeeded, executions_failed,
                        auto_approved_count, avg_price_change_pct, avg_margin_after)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            str(d), marketplace_id,
            # UPDATE
            row[0] or 0, row[1] or 0, row[2] or 0, row[3] or 0,
            row[4] or 0, row[5] or 0, row[6] or 0, row[7] or 0,
            _fv(row[8]), _fv(row[9]),
            # INSERT
            str(d), marketplace_id,
            row[0] or 0, row[1] or 0, row[2] or 0, row[3] or 0,
            row[4] or 0, row[5] or 0, row[6] or 0, row[7] or 0,
            _fv(row[8]), _fv(row[9]),
        ))
        conn.commit()
        return {
            "date": str(d),
            "marketplace_id": marketplace_id,
            "proposals_created": row[0] or 0,
            "proposals_approved": row[1] or 0,
            "proposals_rejected": row[2] or 0,
            "proposals_expired": row[3] or 0,
            "executions_submitted": row[4] or 0,
            "executions_succeeded": row[5] or 0,
            "executions_failed": row[6] or 0,
            "auto_approved_count": row[7] or 0,
            "avg_price_change_pct": _fv(row[8]),
            "avg_margin_after": _fv(row[9]),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_analytics_trend(
    days: int = 30,
    marketplace_id: str | None = None,
) -> list[dict]:
    """Get daily analytics trend for charting."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [days]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT analytics_date, marketplace_id,
                   proposals_created, proposals_approved,
                   proposals_rejected, proposals_expired,
                   executions_submitted, executions_succeeded,
                   executions_failed, auto_approved_count,
                   avg_price_change_pct, avg_margin_after,
                   total_revenue_impact
            FROM dbo.acc_repricing_analytics WITH (NOLOCK)
            WHERE analytics_date >= DATEADD(day, -?, GETUTCDATE())
              {mkt_filter}
            ORDER BY analytics_date ASC
        """, params)
        result = []
        for r in cur.fetchall():
            result.append({
                "date": str(r[0]),
                "marketplace_id": r[1],
                "proposals_created": r[2] or 0,
                "proposals_approved": r[3] or 0,
                "proposals_rejected": r[4] or 0,
                "proposals_expired": r[5] or 0,
                "executions_submitted": r[6] or 0,
                "executions_succeeded": r[7] or 0,
                "executions_failed": r[8] or 0,
                "auto_approved_count": r[9] or 0,
                "avg_price_change_pct": _fv(r[10]),
                "avg_margin_after": _fv(r[11]),
                "total_revenue_impact": _fv(r[12]),
            })
        return result
    finally:
        conn.close()


def get_execution_summary_by_strategy(
    days: int = 30,
    marketplace_id: str | None = None,
) -> list[dict]:
    """Per-strategy execution summary for analytics."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [days]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT strategy_type,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'executed' THEN 1 ELSE 0 END) AS executed,
                   SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
                   SUM(CASE WHEN status = 'proposed' THEN 1 ELSE 0 END) AS pending,
                   AVG(price_change_pct) AS avg_change_pct,
                   AVG(estimated_margin_pct) AS avg_margin
            FROM dbo.acc_repricing_execution WITH (NOLOCK)
            WHERE created_at >= DATEADD(day, -?, GETUTCDATE())
              {mkt_filter}
            GROUP BY strategy_type
            ORDER BY total DESC
        """, params)
        return [
            {
                "strategy_type": r[0],
                "total": r[1] or 0,
                "executed": r[2] or 0,
                "rejected": r[3] or 0,
                "pending": r[4] or 0,
                "avg_change_pct": _fv(r[5]),
                "avg_margin": _fv(r[6]),
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()
