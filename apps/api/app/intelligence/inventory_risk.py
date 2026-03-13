"""Inventory Risk Engine — probabilistic stockout, overstock cost & aging risk.

Sprint 13 – S13.2 / S13.3 / S13.4
Sprint 14 – Replenishment plan, velocity trends, risk alerts.

Scoring model (0-100 composite risk score):
  - Stockout probability (7d)   40 pts  (scaled 0-1 → 0-40)
  - Overstock holding cost      30 pts  (scaled by cost percentile)
  - Aging write-off risk        30 pts  (scaled by aged-90+ share)

Risk tiers:
  - critical   score ≥ 70
  - high       score ≥ 50
  - medium     score ≥ 30
  - low        score < 30
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Schema DDL (idempotent) ─────────────────────────────────────────────

_SCHEMA_STATEMENTS: list[str] = [
    """
    IF OBJECT_ID('dbo.acc_inventory_risk_score', 'U') IS NULL
    CREATE TABLE dbo.acc_inventory_risk_score (
        id                     BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku             NVARCHAR(100) NOT NULL,
        asin                   NVARCHAR(20)  NULL,
        marketplace_id         VARCHAR(20)   NOT NULL,
        score_date             DATE          NOT NULL,
        stockout_prob_7d       DECIMAL(5,4)  NULL,
        stockout_prob_14d      DECIMAL(5,4)  NULL,
        stockout_prob_30d      DECIMAL(5,4)  NULL,
        days_cover             DECIMAL(10,1) NULL,
        velocity_7d            DECIMAL(10,2) NULL DEFAULT 0,
        velocity_30d           DECIMAL(10,2) NULL DEFAULT 0,
        velocity_cv            DECIMAL(6,3)  NULL,
        units_available        INT           NULL DEFAULT 0,
        overstock_holding_cost_pln DECIMAL(14,2) NULL DEFAULT 0,
        storage_fee_30d_pln    DECIMAL(14,2) NULL DEFAULT 0,
        capital_tie_up_pln     DECIMAL(14,2) NULL DEFAULT 0,
        excess_units           INT           NULL DEFAULT 0,
        excess_value_pln       DECIMAL(14,2) NULL DEFAULT 0,
        aging_risk_pln         DECIMAL(14,2) NULL DEFAULT 0,
        aged_90_plus_units     INT           NULL DEFAULT 0,
        aged_90_plus_value_pln DECIMAL(14,2) NULL DEFAULT 0,
        projected_aged_90_30d  INT           NULL DEFAULT 0,
        risk_tier              VARCHAR(20)   NOT NULL DEFAULT 'low',
        risk_score             SMALLINT      NOT NULL DEFAULT 0,
        computed_at            DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_irs_sku_mkt_date UNIQUE (seller_sku, marketplace_id, score_date)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_irs_date')
    CREATE INDEX ix_irs_date ON dbo.acc_inventory_risk_score (score_date DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_irs_tier')
    CREATE INDEX ix_irs_tier ON dbo.acc_inventory_risk_score (risk_tier, score_date DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_irs_mkt_date')
    CREATE INDEX ix_irs_mkt_date ON dbo.acc_inventory_risk_score (marketplace_id, score_date DESC)
    """,
]


def ensure_inventory_risk_schema() -> None:
    """Create inventory risk tables if they don't exist."""
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
    finally:
        conn.close()


# ── Constants ───────────────────────────────────────────────────────────

DEFAULT_LEAD_TIME_DAYS = 21
DEFAULT_TARGET_DAYS = 45
DEFAULT_SAFETY_STOCK_DAYS = 14
MONTHLY_STORAGE_FEE_PER_UNIT_EUR = 0.50
LONG_TERM_STORAGE_SURCHARGE_EUR = 6.90   # per unit >365 days
CAPITAL_COST_ANNUAL_RATE = 0.12           # 12% cost of capital
AGED_SURCHARGE_90_PLUS_PCT = 0.15         # estimated 15% value loss


# ── Stockout probability model ──────────────────────────────────────────

def compute_stockout_probability(
    units_available: int,
    velocity_mean: float,
    velocity_cv: float,
    horizon_days: int,
) -> float:
    """Estimate P(stockout) within *horizon_days* using normal approximation.

    Uses the coefficient of variation (CV) of daily sales to model demand
    uncertainty.  When CV is unavailable or zero, falls back to a
    deterministic days-cover check.

    Returns probability in [0.0, 1.0].
    """
    if velocity_mean <= 0:
        return 0.0 if units_available > 0 else 1.0
    if units_available <= 0:
        return 1.0

    expected_demand = velocity_mean * horizon_days
    if velocity_cv <= 0:
        # Deterministic: stockout if demand exceeds stock
        return 1.0 if expected_demand > units_available else 0.0

    demand_std = velocity_mean * velocity_cv * math.sqrt(horizon_days)
    if demand_std <= 0:
        return 1.0 if expected_demand > units_available else 0.0

    # z = (stock - expected_demand) / demand_std → P(stockout) = Φ(-z)
    z = (units_available - expected_demand) / demand_std
    # Approximate Φ using logistic function: Φ(x) ≈ 1/(1+exp(-1.7*x))
    prob = 1.0 / (1.0 + math.exp(min(1.7 * z, 500)))
    return max(0.0, min(1.0, prob))


def _compute_velocity_cv(daily_sales: list[float]) -> float:
    """Coefficient of variation of daily sales (std / mean)."""
    if len(daily_sales) < 3:
        return 0.0
    mean = sum(daily_sales) / len(daily_sales)
    if mean <= 0:
        return 0.0
    variance = sum((x - mean) ** 2 for x in daily_sales) / len(daily_sales)
    return math.sqrt(variance) / mean


# ── Overstock cost model ────────────────────────────────────────────────

def compute_overstock_cost(
    units_available: int,
    velocity_30d: float,
    unit_cost_pln: float,
    target_days: int = DEFAULT_TARGET_DAYS,
) -> dict[str, float]:
    """Estimate 30-day holding cost of excess inventory.

    Returns dict with storage_fee, capital_tie_up, total, excess_units,
    excess_value_pln.
    """
    target_stock = math.ceil(velocity_30d * target_days) if velocity_30d > 0 else 0
    excess = max(0, units_available - target_stock)
    excess_value = excess * max(unit_cost_pln, 0)

    # Storage fee for excess units (monthly rate)
    storage_fee = excess * MONTHLY_STORAGE_FEE_PER_UNIT_EUR * 4.30  # EUR→PLN approx
    # Capital opportunity cost (monthly)
    capital_cost = excess_value * (CAPITAL_COST_ANNUAL_RATE / 12.0)

    return {
        "storage_fee_30d_pln": round(storage_fee, 2),
        "capital_tie_up_pln": round(capital_cost, 2),
        "total_pln": round(storage_fee + capital_cost, 2),
        "excess_units": excess,
        "excess_value_pln": round(excess_value, 2),
    }


# ── Aging write-off risk ────────────────────────────────────────────────

def compute_aging_risk(
    aged_90_plus_units: int,
    unit_cost_pln: float,
    velocity_30d: float,
    total_on_hand: int,
) -> dict[str, float]:
    """Estimate aged inventory write-off risk.

    Projects how many current units will move into 90+ bucket within 30
    days based on slow-moving velocity.

    Returns dict with aged value, projected new aged units, risk PLN.
    """
    aged_value = aged_90_plus_units * max(unit_cost_pln, 0)

    # Project units aging into 90+ within next 30 days.
    # If velocity is very low relative to stock, more units age out.
    sellable_in_30d = min(int(velocity_30d * 30), total_on_hand)
    remaining_after_30d = max(0, total_on_hand - sellable_in_30d)
    # Units currently in 61-90 bucket will age into 90+ → approximate
    projected_new_aged = max(0, remaining_after_30d - aged_90_plus_units)

    # Risk = current aged value + projected new aged value loss
    projected_risk = projected_new_aged * max(unit_cost_pln, 0) * AGED_SURCHARGE_90_PLUS_PCT
    total_risk = aged_value * AGED_SURCHARGE_90_PLUS_PCT + projected_risk

    return {
        "aged_90_plus_value_pln": round(aged_value, 2),
        "projected_aged_90_30d": projected_new_aged,
        "aging_risk_pln": round(total_risk, 2),
    }


# ── Composite risk score ────────────────────────────────────────────────

def compute_composite_risk_score(
    stockout_prob_7d: float,
    overstock_cost_pln: float,
    aging_risk_pln: float,
    *,
    overstock_p90: float = 500.0,
    aging_p90: float = 200.0,
) -> tuple[int, str]:
    """Compute 0-100 composite risk score and tier.

    Weights:
      - Stockout probability (7d):  40 pts
      - Overstock holding cost:     30 pts (normalized by p90 threshold)
      - Aging write-off risk:       30 pts (normalized by p90 threshold)
    """
    stockout_pts = min(40.0, stockout_prob_7d * 40.0)
    overstock_norm = min(1.0, overstock_cost_pln / max(overstock_p90, 1.0))
    overstock_pts = overstock_norm * 30.0
    aging_norm = min(1.0, aging_risk_pln / max(aging_p90, 1.0))
    aging_pts = aging_norm * 30.0

    score = int(round(stockout_pts + overstock_pts + aging_pts))
    score = max(0, min(100, score))

    if score >= 70:
        tier = "critical"
    elif score >= 50:
        tier = "high"
    elif score >= 30:
        tier = "medium"
    else:
        tier = "low"

    return score, tier


# ── Daily computation pipeline ──────────────────────────────────────────

def compute_daily_risk_scores(
    target_date: date | None = None,
    marketplace_id: str | None = None,
) -> int:
    """Compute and persist risk scores for all active SKUs.

    Returns count of rows upserted.
    """
    if target_date is None:
        target_date = date.today()

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        # Load FBA config defaults
        defaults = _load_fba_config(cur)
        target_days = _safe_int(defaults.get("target_days"), DEFAULT_TARGET_DAYS)

        # Load inventory + velocity data
        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "AND inv.marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(
            f"""
            WITH daily_sales AS (
                SELECT
                    o.marketplace_id,
                    ISNULL(ol.sku, p.sku) AS sku,
                    CAST(o.purchase_date AS DATE) AS sale_date,
                    SUM(CASE
                        WHEN ISNULL(ol.quantity_shipped, 0) > 0
                        THEN ol.quantity_shipped ELSE ol.quantity_ordered
                    END) AS units_sold
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
                WHERE o.status = 'Shipped'
                  AND CAST(o.purchase_date AS DATE) >= DATEADD(DAY, -90, CAST(GETUTCDATE() AS DATE))
                GROUP BY o.marketplace_id, ISNULL(ol.sku, p.sku), CAST(o.purchase_date AS DATE)
            ),
            velocity AS (
                SELECT
                    marketplace_id, sku,
                    SUM(CASE WHEN sale_date >= DATEADD(DAY, -7, CAST(GETUTCDATE() AS DATE))
                        THEN units_sold ELSE 0 END) / 7.0 AS velocity_7d,
                    SUM(CASE WHEN sale_date >= DATEADD(DAY, -30, CAST(GETUTCDATE() AS DATE))
                        THEN units_sold ELSE 0 END) / 30.0 AS velocity_30d,
                    STDEV(CASE WHEN sale_date >= DATEADD(DAY, -30, CAST(GETUTCDATE() AS DATE))
                        THEN CAST(units_sold AS FLOAT) END) AS daily_std_30d,
                    AVG(CASE WHEN sale_date >= DATEADD(DAY, -30, CAST(GETUTCDATE() AS DATE))
                        THEN CAST(units_sold AS FLOAT) END) AS daily_avg_30d
                FROM daily_sales
                GROUP BY marketplace_id, sku
            )
            SELECT
                inv.marketplace_id,
                inv.sku,
                inv.asin,
                ISNULL(inv.on_hand, 0) AS on_hand,
                ISNULL(inv.reserved, 0) AS reserved,
                ISNULL(inv.aged_90_plus, 0) AS aged_90_plus,
                ISNULL(inv.excess_units, 0) AS excess_units,
                ISNULL(v.velocity_7d, 0) AS velocity_7d,
                ISNULL(v.velocity_30d, 0) AS velocity_30d,
                ISNULL(v.daily_std_30d, 0) AS daily_std,
                ISNULL(v.daily_avg_30d, 0) AS daily_avg,
                ISNULL(p.netto_purchase_price_pln, 0) AS unit_cost_pln
            FROM dbo.acc_fba_inventory_snapshot inv WITH (NOLOCK)
            LEFT JOIN velocity v
                ON v.marketplace_id = inv.marketplace_id AND v.sku = inv.sku
            LEFT JOIN dbo.acc_product p WITH (NOLOCK)
                ON p.sku = inv.sku
            WHERE inv.snapshot_date = (
                SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
            )
            {mkt_filter}
            """,
            params,
        )

        rows = _fetchall(cur)
        if not rows:
            conn.rollback()
            return 0

        # Compute per-SKU risk scores
        upserted = 0
        for row in rows:
            sku = row[1] or ""
            asin = row[2]
            mkt = row[0] or ""
            on_hand = _safe_int_val(row[3])
            reserved = _safe_int_val(row[4])
            aged_90_plus = _safe_int_val(row[5])
            excess = _safe_int_val(row[6])
            vel_7d = _safe_float_val(row[7])
            vel_30d = _safe_float_val(row[8])
            daily_std = _safe_float_val(row[9])
            daily_avg = _safe_float_val(row[10])
            unit_cost = _safe_float_val(row[11])

            units_avail = max(on_hand - reserved, 0)
            vel_cv = (daily_std / daily_avg) if daily_avg > 0 else 0.0

            # Stockout probability
            p7 = compute_stockout_probability(units_avail, vel_30d, vel_cv, 7)
            p14 = compute_stockout_probability(units_avail, vel_30d, vel_cv, 14)
            p30 = compute_stockout_probability(units_avail, vel_30d, vel_cv, 30)
            days_cover = round(units_avail / vel_30d, 1) if vel_30d > 0 else None

            # Overstock cost
            ov = compute_overstock_cost(units_avail, vel_30d, unit_cost, target_days)

            # Aging risk
            ag = compute_aging_risk(aged_90_plus, unit_cost, vel_30d, on_hand)

            # Composite
            score, tier = compute_composite_risk_score(
                p7, ov["total_pln"], ag["aging_risk_pln"],
            )

            # MERGE upsert
            cur.execute(
                """
                MERGE dbo.acc_inventory_risk_score AS tgt
                USING (SELECT %s AS seller_sku, %s AS marketplace_id, %s AS score_date) AS src
                ON tgt.seller_sku = src.seller_sku
                   AND tgt.marketplace_id = src.marketplace_id
                   AND tgt.score_date = src.score_date
                WHEN MATCHED THEN UPDATE SET
                    asin = %s,
                    stockout_prob_7d = %s, stockout_prob_14d = %s, stockout_prob_30d = %s,
                    days_cover = %s,
                    velocity_7d = %s, velocity_30d = %s, velocity_cv = %s,
                    units_available = %s,
                    overstock_holding_cost_pln = %s,
                    storage_fee_30d_pln = %s, capital_tie_up_pln = %s,
                    excess_units = %s, excess_value_pln = %s,
                    aging_risk_pln = %s, aged_90_plus_units = %s,
                    aged_90_plus_value_pln = %s, projected_aged_90_30d = %s,
                    risk_tier = %s, risk_score = %s,
                    computed_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN INSERT (
                    seller_sku, asin, marketplace_id, score_date,
                    stockout_prob_7d, stockout_prob_14d, stockout_prob_30d,
                    days_cover, velocity_7d, velocity_30d, velocity_cv,
                    units_available,
                    overstock_holding_cost_pln, storage_fee_30d_pln, capital_tie_up_pln,
                    excess_units, excess_value_pln,
                    aging_risk_pln, aged_90_plus_units, aged_90_plus_value_pln,
                    projected_aged_90_30d, risk_tier, risk_score
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                );
                """,
                (
                    # USING key
                    sku, mkt, target_date,
                    # UPDATE values
                    asin,
                    round(p7, 4), round(p14, 4), round(p30, 4),
                    days_cover,
                    round(vel_7d, 2), round(vel_30d, 2), round(vel_cv, 3),
                    units_avail,
                    ov["total_pln"], ov["storage_fee_30d_pln"], ov["capital_tie_up_pln"],
                    ov["excess_units"], ov["excess_value_pln"],
                    ag["aging_risk_pln"], aged_90_plus,
                    ag["aged_90_plus_value_pln"], ag["projected_aged_90_30d"],
                    tier, score,
                    # INSERT values
                    sku, asin, mkt, target_date,
                    round(p7, 4), round(p14, 4), round(p30, 4),
                    days_cover, round(vel_7d, 2), round(vel_30d, 2), round(vel_cv, 3),
                    units_avail,
                    ov["total_pln"], ov["storage_fee_30d_pln"], ov["capital_tie_up_pln"],
                    ov["excess_units"], ov["excess_value_pln"],
                    ag["aging_risk_pln"], aged_90_plus,
                    ag["aged_90_plus_value_pln"], ag["projected_aged_90_30d"],
                    tier, score,
                ),
            )
            upserted += 1

        conn.commit()
        log.info("inventory_risk.compute_complete",
                 date=str(target_date), upserted=upserted)
        return upserted

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Query functions for API ─────────────────────────────────────────────

def get_risk_dashboard(
    marketplace_id: str | None = None,
    *,
    days: int = 1,
) -> dict[str, Any]:
    """Aggregated inventory risk dashboard."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [days]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total_skus,
                SUM(CASE WHEN risk_tier = 'critical' THEN 1 ELSE 0 END) AS critical_count,
                SUM(CASE WHEN risk_tier = 'high' THEN 1 ELSE 0 END) AS high_count,
                SUM(CASE WHEN risk_tier = 'medium' THEN 1 ELSE 0 END) AS medium_count,
                SUM(CASE WHEN risk_tier = 'low' THEN 1 ELSE 0 END) AS low_count,
                AVG(stockout_prob_7d) AS avg_stockout_prob_7d,
                SUM(overstock_holding_cost_pln) AS total_holding_cost_pln,
                SUM(aging_risk_pln) AS total_aging_risk_pln,
                SUM(excess_value_pln) AS total_excess_value_pln,
                AVG(CAST(risk_score AS FLOAT)) AS avg_risk_score
            FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            WHERE score_date >= DATEADD(DAY, -%s, CAST(GETUTCDATE() AS DATE))
              AND score_date = (
                  SELECT MAX(score_date) FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
              )
              {mkt_filter}
            """,
            params,
        )
        r = cur.fetchone()
        if not r or not r[0]:
            return {
                "total_skus": 0, "critical": 0, "high": 0, "medium": 0, "low": 0,
                "avg_stockout_prob_7d": None, "total_holding_cost_pln": 0,
                "total_aging_risk_pln": 0, "total_excess_value_pln": 0,
                "avg_risk_score": None,
            }
        return {
            "total_skus": r[0],
            "critical": r[1], "high": r[2], "medium": r[3], "low": r[4],
            "avg_stockout_prob_7d": round(float(r[5]), 4) if r[5] is not None else None,
            "total_holding_cost_pln": round(float(r[6]), 2) if r[6] else 0,
            "total_aging_risk_pln": round(float(r[7]), 2) if r[7] else 0,
            "total_excess_value_pln": round(float(r[8]), 2) if r[8] else 0,
            "avg_risk_score": round(float(r[9]), 1) if r[9] is not None else None,
        }
    finally:
        conn.close()


def get_risk_scores(
    marketplace_id: str | None = None,
    *,
    risk_tier: str | None = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "risk_score",
    sort_dir: str = "desc",
) -> dict[str, Any]:
    """Paginated risk score list for the most recent score_date."""
    allowed_sort = {"risk_score", "stockout_prob_7d", "overstock_holding_cost_pln",
                    "aging_risk_pln", "days_cover", "seller_sku"}
    if sort_by not in allowed_sort:
        sort_by = "risk_score"
    if sort_dir.lower() not in ("asc", "desc"):
        sort_dir = "desc"

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where = [
            "score_date = (SELECT MAX(score_date) FROM dbo.acc_inventory_risk_score WITH (NOLOCK))",
        ]
        params: list[Any] = []
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)
        if risk_tier:
            where.append("risk_tier = %s")
            params.append(risk_tier)

        where_sql = " AND ".join(where)

        # Count
        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_inventory_risk_score WITH (NOLOCK) WHERE {where_sql}",
            params,
        )
        total = cur.fetchone()[0] or 0

        # Data
        cur.execute(
            f"""
            SELECT seller_sku, asin, marketplace_id, score_date,
                   stockout_prob_7d, stockout_prob_14d, stockout_prob_30d,
                   days_cover, velocity_7d, velocity_30d, velocity_cv,
                   units_available,
                   overstock_holding_cost_pln, storage_fee_30d_pln, capital_tie_up_pln,
                   excess_units, excess_value_pln,
                   aging_risk_pln, aged_90_plus_units, aged_90_plus_value_pln,
                   projected_aged_90_30d, risk_tier, risk_score
            FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY {sort_by} {sort_dir}
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
            """,
            params + [offset, limit],
        )
        items = []
        for r in cur.fetchall():
            items.append({
                "seller_sku": r[0],
                "asin": r[1],
                "marketplace_id": r[2],
                "score_date": r[3].isoformat() if r[3] else None,
                "stockout_prob_7d": _fv(r[4]),
                "stockout_prob_14d": _fv(r[5]),
                "stockout_prob_30d": _fv(r[6]),
                "days_cover": _fv(r[7]),
                "velocity_7d": _fv(r[8]),
                "velocity_30d": _fv(r[9]),
                "velocity_cv": _fv(r[10]),
                "units_available": r[11],
                "overstock_holding_cost_pln": _fv(r[12]),
                "storage_fee_30d_pln": _fv(r[13]),
                "capital_tie_up_pln": _fv(r[14]),
                "excess_units": r[15],
                "excess_value_pln": _fv(r[16]),
                "aging_risk_pln": _fv(r[17]),
                "aged_90_plus_units": r[18],
                "aged_90_plus_value_pln": _fv(r[19]),
                "projected_aged_90_30d": r[20],
                "risk_tier": r[21],
                "risk_score": r[22],
            })
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_risk_history(
    seller_sku: str,
    marketplace_id: str,
    *,
    days: int = 30,
) -> list[dict]:
    """Daily risk score history for a specific SKU."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT score_date, risk_score, risk_tier,
                   stockout_prob_7d, overstock_holding_cost_pln, aging_risk_pln,
                   days_cover, units_available, velocity_30d
            FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            WHERE seller_sku = %s AND marketplace_id = %s
              AND score_date >= DATEADD(DAY, -%s, CAST(GETUTCDATE() AS DATE))
            ORDER BY score_date ASC
            """,
            (seller_sku, marketplace_id, days),
        )
        return [
            {
                "date": r[0].isoformat() if r[0] else None,
                "risk_score": r[1],
                "risk_tier": r[2],
                "stockout_prob_7d": _fv(r[3]),
                "overstock_holding_cost_pln": _fv(r[4]),
                "aging_risk_pln": _fv(r[5]),
                "days_cover": _fv(r[6]),
                "units_available": r[7],
                "velocity_30d": _fv(r[8]),
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def get_stockout_watchlist(
    marketplace_id: str | None = None,
    *,
    threshold: float = 0.3,
    limit: int = 50,
) -> list[dict]:
    """SKUs with highest P(stockout 7d), above threshold."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [threshold, limit]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(
            f"""
            SELECT TOP (%s)
                seller_sku, asin, marketplace_id,
                stockout_prob_7d, stockout_prob_14d, stockout_prob_30d,
                days_cover, velocity_30d, units_available
            FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            WHERE score_date = (
                SELECT MAX(score_date) FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            )
              AND stockout_prob_7d >= %s
              {mkt_filter}
            ORDER BY stockout_prob_7d DESC
            """,
            [limit, threshold] + ([marketplace_id] if marketplace_id else []),
        )
        return [
            {
                "seller_sku": r[0], "asin": r[1], "marketplace_id": r[2],
                "stockout_prob_7d": _fv(r[3]), "stockout_prob_14d": _fv(r[4]),
                "stockout_prob_30d": _fv(r[5]),
                "days_cover": _fv(r[6]), "velocity_30d": _fv(r[7]),
                "units_available": r[8],
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def get_overstock_report(
    marketplace_id: str | None = None,
    *,
    min_cost_pln: float = 0,
    limit: int = 50,
) -> list[dict]:
    """SKUs with highest overstock holding cost."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [min_cost_pln, limit]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(
            f"""
            SELECT TOP (%s)
                seller_sku, asin, marketplace_id,
                overstock_holding_cost_pln, storage_fee_30d_pln, capital_tie_up_pln,
                excess_units, excess_value_pln, days_cover, velocity_30d
            FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            WHERE score_date = (
                SELECT MAX(score_date) FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            )
              AND overstock_holding_cost_pln >= %s
              {mkt_filter}
            ORDER BY overstock_holding_cost_pln DESC
            """,
            [limit, min_cost_pln] + ([marketplace_id] if marketplace_id else []),
        )
        return [
            {
                "seller_sku": r[0], "asin": r[1], "marketplace_id": r[2],
                "overstock_holding_cost_pln": _fv(r[3]),
                "storage_fee_30d_pln": _fv(r[4]),
                "capital_tie_up_pln": _fv(r[5]),
                "excess_units": r[6], "excess_value_pln": _fv(r[7]),
                "days_cover": _fv(r[8]), "velocity_30d": _fv(r[9]),
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


# ── Helpers ─────────────────────────────────────────────────────────────

def _fetchall(cur: Any) -> list[tuple]:
    return cur.fetchall()


def _fv(val: Any) -> float | None:
    """Safe float value."""
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _safe_int_val(val: Any) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _safe_float_val(val: Any) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _load_fba_config(cur: Any) -> dict[str, Any]:
    """Load acc_fba_config key-value pairs."""
    try:
        cur.execute(
            "SELECT [key], value_json FROM dbo.acc_fba_config WITH (NOLOCK)"
        )
        result = {}
        for r in cur.fetchall():
            import json
            try:
                result[r[0]] = json.loads(r[1]) if r[1] else None
            except (json.JSONDecodeError, TypeError):
                result[r[0]] = r[1]
        return result
    except Exception:
        return {}


# ── Sprint 14 – Schema DDL (replenishment plan & risk alerts) ───────────

_REPLENISHMENT_SCHEMA: list[str] = [
    """
    IF OBJECT_ID('dbo.acc_replenishment_plan', 'U') IS NULL
    CREATE TABLE dbo.acc_replenishment_plan (
        id                     BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku             NVARCHAR(100) NOT NULL,
        asin                   NVARCHAR(20)  NULL,
        marketplace_id         VARCHAR(20)   NOT NULL,
        plan_date              DATE          NOT NULL,
        risk_score             SMALLINT      NOT NULL DEFAULT 0,
        risk_tier              VARCHAR(20)   NOT NULL DEFAULT 'low',
        stockout_prob_7d       DECIMAL(5,4)  NULL,
        days_cover             DECIMAL(10,1) NULL,
        velocity_7d            DECIMAL(10,2) NULL DEFAULT 0,
        velocity_30d           DECIMAL(10,2) NULL DEFAULT 0,
        velocity_trend         VARCHAR(20)   NULL DEFAULT 'stable',
        velocity_change_pct    DECIMAL(7,2)  NULL,
        suggested_reorder_qty  INT           NOT NULL DEFAULT 0,
        reorder_urgency        VARCHAR(20)   NOT NULL DEFAULT 'low',
        target_days_cover      INT           NOT NULL DEFAULT 45,
        lead_time_days         INT           NOT NULL DEFAULT 21,
        safety_stock_days      INT           NOT NULL DEFAULT 14,
        estimated_stockout_date DATE         NULL,
        overstock_holding_cost_pln DECIMAL(14,2) NULL DEFAULT 0,
        aging_risk_pln         DECIMAL(14,2) NULL DEFAULT 0,
        units_available        INT           NULL DEFAULT 0,
        is_acknowledged        BIT           NOT NULL DEFAULT 0,
        acknowledged_at        DATETIME2     NULL,
        acknowledged_by        NVARCHAR(100) NULL,
        computed_at            DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_rp_sku_mkt_date UNIQUE (seller_sku, marketplace_id, plan_date)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_rp_date')
    CREATE INDEX ix_rp_date ON dbo.acc_replenishment_plan (plan_date DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_rp_urgency')
    CREATE INDEX ix_rp_urgency ON dbo.acc_replenishment_plan (reorder_urgency, plan_date DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_rp_mkt_date')
    CREATE INDEX ix_rp_mkt_date ON dbo.acc_replenishment_plan (marketplace_id, plan_date DESC)
    """,
    """
    IF OBJECT_ID('dbo.acc_inventory_risk_alert', 'U') IS NULL
    CREATE TABLE dbo.acc_inventory_risk_alert (
        id                     BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku             NVARCHAR(100) NOT NULL,
        marketplace_id         VARCHAR(20)   NOT NULL,
        alert_type             VARCHAR(50)   NOT NULL,
        severity               VARCHAR(20)   NOT NULL DEFAULT 'warning',
        title                  NVARCHAR(200) NOT NULL,
        detail                 NVARCHAR(MAX) NULL,
        current_value          FLOAT         NULL,
        previous_value         FLOAT         NULL,
        threshold              FLOAT         NULL,
        risk_score             SMALLINT      NULL,
        risk_tier              VARCHAR(20)   NULL,
        is_resolved            BIT           NOT NULL DEFAULT 0,
        resolved_at            DATETIME2     NULL,
        triggered_at           DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ira_type_date')
    CREATE INDEX ix_ira_type_date ON dbo.acc_inventory_risk_alert (alert_type, triggered_at DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ira_sku_mkt')
    CREATE INDEX ix_ira_sku_mkt ON dbo.acc_inventory_risk_alert (seller_sku, marketplace_id)
    """,
]


def ensure_replenishment_schema() -> None:
    """Create replenishment plan + risk alert tables if they don't exist."""
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _REPLENISHMENT_SCHEMA:
            cur.execute(stmt)
    finally:
        conn.close()


# ── Velocity trend detection ────────────────────────────────────────────

def compute_velocity_trend(
    velocity_7d: float,
    velocity_30d: float,
) -> tuple[str, float]:
    """Detect velocity trend by comparing 7d vs 30d velocity.

    Returns (trend, change_pct):
      - accelerating  if 7d > 30d by >25%
      - decelerating  if 7d < 30d by >25%
      - stable        otherwise
    """
    if velocity_30d <= 0:
        if velocity_7d > 0:
            return "accelerating", 100.0
        return "stable", 0.0
    change_pct = ((velocity_7d - velocity_30d) / velocity_30d) * 100.0
    if change_pct > 25.0:
        return "accelerating", round(change_pct, 2)
    if change_pct < -25.0:
        return "decelerating", round(change_pct, 2)
    return "stable", round(change_pct, 2)


def compute_suggested_reorder_qty(
    units_available: int,
    velocity_30d: float,
    target_days: int = DEFAULT_TARGET_DAYS,
    safety_stock_days: int = DEFAULT_SAFETY_STOCK_DAYS,
    lead_time_days: int = DEFAULT_LEAD_TIME_DAYS,
) -> int:
    """Compute risk-informed suggested reorder quantity.

    Formula: max(0, ceil((target_days + safety_stock) * velocity - available))
    """
    if velocity_30d <= 0:
        return 0
    target_stock = math.ceil((target_days + safety_stock_days) * velocity_30d)
    qty = max(0, target_stock - units_available)
    return qty


def compute_reorder_urgency(
    days_cover: float | None,
    stockout_prob_7d: float,
    risk_tier: str,
    lead_time_days: int = DEFAULT_LEAD_TIME_DAYS,
) -> str:
    """Determine reorder urgency based on risk signals.

    Returns: critical, high, medium, low
    """
    dc = days_cover if days_cover is not None else 999.0
    if dc < 7 or stockout_prob_7d >= 0.7 or risk_tier == "critical":
        return "critical"
    if dc < lead_time_days or stockout_prob_7d >= 0.4 or risk_tier == "high":
        return "high"
    if dc < lead_time_days + 14 or stockout_prob_7d >= 0.2:
        return "medium"
    return "low"


def compute_estimated_stockout_date(
    units_available: int,
    velocity_30d: float,
    target_date: date | None = None,
) -> date | None:
    """Estimate when stock will run out at current velocity."""
    if velocity_30d <= 0 or units_available <= 0:
        return None
    days_until_stockout = int(units_available / velocity_30d)
    base = target_date or date.today()
    return base + timedelta(days=days_until_stockout)


# ── Replenishment plan pipeline ─────────────────────────────────────────

def compute_replenishment_plan(
    target_date: date | None = None,
    marketplace_id: str | None = None,
) -> int:
    """Generate risk-informed replenishment plan from latest risk scores.

    Reads from acc_inventory_risk_score, computes reorder qty + urgency +
    velocity trend, and persists to acc_replenishment_plan.

    Returns count of rows upserted.
    """
    if target_date is None:
        target_date = date.today()

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        defaults = _load_fba_config(cur)
        target_days = _safe_int(defaults.get("target_days"), DEFAULT_TARGET_DAYS)
        safety_days = _safe_int(defaults.get("safety_stock_days"), DEFAULT_SAFETY_STOCK_DAYS)
        lead_days = _safe_int(defaults.get("lead_time_days"), DEFAULT_LEAD_TIME_DAYS)

        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "AND marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(
            f"""
            SELECT seller_sku, asin, marketplace_id,
                   risk_score, risk_tier, stockout_prob_7d,
                   days_cover, velocity_7d, velocity_30d,
                   units_available, overstock_holding_cost_pln, aging_risk_pln
            FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            WHERE score_date = (
                SELECT MAX(score_date) FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            )
            {mkt_filter}
            """,
            params,
        )

        rows = _fetchall(cur)
        if not rows:
            conn.rollback()
            return 0

        upserted = 0
        for row in rows:
            sku = row[0] or ""
            asin = row[1]
            mkt = row[2] or ""
            score = _safe_int_val(row[3])
            tier = row[4] or "low"
            p7 = _safe_float_val(row[5])
            dc = _safe_float_val(row[6]) if row[6] is not None else None
            vel_7d = _safe_float_val(row[7])
            vel_30d = _safe_float_val(row[8])
            units = _safe_int_val(row[9])
            overstock_cost = _safe_float_val(row[10])
            aging_cost = _safe_float_val(row[11])

            trend, change_pct = compute_velocity_trend(vel_7d, vel_30d)
            reorder_qty = compute_suggested_reorder_qty(
                units, vel_30d, target_days, safety_days, lead_days,
            )
            urgency = compute_reorder_urgency(dc, p7, tier, lead_days)
            stockout_est = compute_estimated_stockout_date(units, vel_30d, target_date)

            # Skip SKUs that don't need reordering and have no risk
            if reorder_qty == 0 and urgency == "low":
                continue

            cur.execute(
                """
                MERGE dbo.acc_replenishment_plan AS tgt
                USING (SELECT %s AS seller_sku, %s AS marketplace_id, %s AS plan_date) AS src
                ON tgt.seller_sku = src.seller_sku
                   AND tgt.marketplace_id = src.marketplace_id
                   AND tgt.plan_date = src.plan_date
                WHEN MATCHED THEN UPDATE SET
                    asin = %s,
                    risk_score = %s, risk_tier = %s, stockout_prob_7d = %s,
                    days_cover = %s, velocity_7d = %s, velocity_30d = %s,
                    velocity_trend = %s, velocity_change_pct = %s,
                    suggested_reorder_qty = %s, reorder_urgency = %s,
                    target_days_cover = %s, lead_time_days = %s, safety_stock_days = %s,
                    estimated_stockout_date = %s,
                    overstock_holding_cost_pln = %s, aging_risk_pln = %s,
                    units_available = %s,
                    computed_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN INSERT (
                    seller_sku, asin, marketplace_id, plan_date,
                    risk_score, risk_tier, stockout_prob_7d,
                    days_cover, velocity_7d, velocity_30d,
                    velocity_trend, velocity_change_pct,
                    suggested_reorder_qty, reorder_urgency,
                    target_days_cover, lead_time_days, safety_stock_days,
                    estimated_stockout_date,
                    overstock_holding_cost_pln, aging_risk_pln,
                    units_available
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s,
                    %s
                );
                """,
                (
                    # USING key
                    sku, mkt, target_date,
                    # UPDATE values
                    asin,
                    score, tier, round(p7, 4),
                    dc, round(vel_7d, 2), round(vel_30d, 2),
                    trend, change_pct,
                    reorder_qty, urgency,
                    target_days, lead_days, safety_days,
                    stockout_est,
                    round(overstock_cost, 2), round(aging_cost, 2),
                    units,
                    # INSERT values
                    sku, asin, mkt, target_date,
                    score, tier, round(p7, 4),
                    dc, round(vel_7d, 2), round(vel_30d, 2),
                    trend, change_pct,
                    reorder_qty, urgency,
                    target_days, lead_days, safety_days,
                    stockout_est,
                    round(overstock_cost, 2), round(aging_cost, 2),
                    units,
                ),
            )
            upserted += 1

        conn.commit()
        log.info("replenishment_plan.compute_complete",
                 date=str(target_date), upserted=upserted)
        return upserted

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Risk alert generation ───────────────────────────────────────────────

_TIER_RANK = {"critical": 3, "high": 2, "medium": 1, "low": 0}


def generate_risk_alerts(
    marketplace_id: str | None = None,
) -> int:
    """Generate alerts for risk tier escalations and threshold breaches.

    Compares latest vs previous day's risk scores to detect:
      1. Tier escalation (e.g. medium → high)
      2. Stockout probability crossing 50% threshold
      3. Sudden velocity deceleration (>40% drop 7d vs 30d)

    Returns count of alerts created.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "AND curr.marketplace_id = %s"
            params.append(marketplace_id)

        # Compare latest two score dates
        cur.execute(
            f"""
            WITH ranked_dates AS (
                SELECT DISTINCT score_date,
                    ROW_NUMBER() OVER (ORDER BY score_date DESC) AS rn
                FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            ),
            curr_date AS (SELECT score_date FROM ranked_dates WHERE rn = 1),
            prev_date AS (SELECT score_date FROM ranked_dates WHERE rn = 2)
            SELECT
                curr.seller_sku, curr.marketplace_id,
                curr.risk_score, curr.risk_tier, curr.stockout_prob_7d,
                curr.velocity_7d, curr.velocity_30d,
                prev.risk_score AS prev_risk_score, prev.risk_tier AS prev_risk_tier,
                prev.stockout_prob_7d AS prev_stockout_prob_7d
            FROM dbo.acc_inventory_risk_score curr WITH (NOLOCK)
            CROSS JOIN curr_date cd
            LEFT JOIN prev_date pd ON 1=1
            LEFT JOIN dbo.acc_inventory_risk_score prev WITH (NOLOCK)
                ON prev.seller_sku = curr.seller_sku
                AND prev.marketplace_id = curr.marketplace_id
                AND prev.score_date = pd.score_date
            WHERE curr.score_date = cd.score_date
            {mkt_filter}
            """,
            params,
        )

        rows = _fetchall(cur)
        alerts_created = 0

        for row in rows:
            sku = row[0] or ""
            mkt = row[1] or ""
            curr_score = _safe_int_val(row[2])
            curr_tier = row[3] or "low"
            curr_p7 = _safe_float_val(row[4])
            vel_7d = _safe_float_val(row[5])
            vel_30d = _safe_float_val(row[6])
            prev_score = _safe_int_val(row[7]) if row[7] is not None else None
            prev_tier = row[8] or "low" if row[8] else None
            prev_p7 = _safe_float_val(row[9]) if row[9] is not None else None

            # Alert 1: Tier escalation
            if prev_tier is not None:
                curr_rank = _TIER_RANK.get(curr_tier, 0)
                prev_rank = _TIER_RANK.get(prev_tier, 0)
                if curr_rank > prev_rank and curr_tier in ("critical", "high"):
                    severity = "critical" if curr_tier == "critical" else "warning"
                    cur.execute(
                        """
                        INSERT INTO dbo.acc_inventory_risk_alert
                            (seller_sku, marketplace_id, alert_type, severity,
                             title, detail, current_value, previous_value,
                             threshold, risk_score, risk_tier)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            sku, mkt, "tier_escalation", severity,
                            f"Risk tier escalated: {prev_tier} → {curr_tier}",
                            f"SKU {sku} risk tier changed from {prev_tier} to {curr_tier} "
                            f"(score {prev_score} → {curr_score})",
                            float(curr_score), float(prev_score) if prev_score else None,
                            None, curr_score, curr_tier,
                        ),
                    )
                    alerts_created += 1

            # Alert 2: Stockout probability crossing 50%
            if curr_p7 >= 0.5 and (prev_p7 is None or prev_p7 < 0.5):
                cur.execute(
                    """
                    INSERT INTO dbo.acc_inventory_risk_alert
                        (seller_sku, marketplace_id, alert_type, severity,
                         title, detail, current_value, previous_value,
                         threshold, risk_score, risk_tier)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sku, mkt, "stockout_threshold", "critical",
                        f"Stockout probability crossed 50%: {curr_p7:.1%}",
                        f"SKU {sku} now has {curr_p7:.1%} probability of stocking out "
                        f"within 7 days (was {prev_p7:.1%} previously)"
                        if prev_p7 is not None
                        else f"SKU {sku} has {curr_p7:.1%} probability of stocking out within 7 days",
                        curr_p7, prev_p7,
                        0.5, curr_score, curr_tier,
                    ),
                )
                alerts_created += 1

            # Alert 3: Sudden velocity deceleration (>40% drop)
            if vel_30d > 0 and vel_7d < vel_30d * 0.6:
                change_pct = ((vel_7d - vel_30d) / vel_30d) * 100.0
                cur.execute(
                    """
                    INSERT INTO dbo.acc_inventory_risk_alert
                        (seller_sku, marketplace_id, alert_type, severity,
                         title, detail, current_value, previous_value,
                         threshold, risk_score, risk_tier)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sku, mkt, "velocity_drop", "warning",
                        f"Sales velocity dropped {abs(change_pct):.0f}%",
                        f"SKU {sku} 7d velocity ({vel_7d:.1f}/day) is {abs(change_pct):.0f}% "
                        f"below 30d average ({vel_30d:.1f}/day)",
                        vel_7d, vel_30d,
                        -40.0, curr_score, curr_tier,
                    ),
                )
                alerts_created += 1

        conn.commit()
        log.info("risk_alerts.generated", alerts_created=alerts_created)
        return alerts_created

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Sprint 14 query functions ───────────────────────────────────────────

def get_replenishment_plan(
    marketplace_id: str | None = None,
    *,
    urgency: str | None = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "reorder_urgency",
    sort_dir: str = "desc",
) -> dict[str, Any]:
    """Paginated risk-informed replenishment plan."""
    allowed_sort = {"reorder_urgency", "suggested_reorder_qty", "risk_score",
                    "days_cover", "stockout_prob_7d", "velocity_trend", "seller_sku"}
    if sort_by not in allowed_sort:
        sort_by = "reorder_urgency"
    if sort_dir.lower() not in ("asc", "desc"):
        sort_dir = "desc"

    # Map urgency for ordering: critical=4, high=3, medium=2, low=1
    order_col = sort_by
    if sort_by == "reorder_urgency":
        order_col = (
            "CASE reorder_urgency WHEN 'critical' THEN 4 WHEN 'high' THEN 3 "
            "WHEN 'medium' THEN 2 ELSE 1 END"
        )

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where = [
            "plan_date = (SELECT MAX(plan_date) FROM dbo.acc_replenishment_plan WITH (NOLOCK))",
        ]
        params: list[Any] = []
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)
        if urgency:
            where.append("reorder_urgency = %s")
            params.append(urgency)

        where_sql = " AND ".join(where)

        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_replenishment_plan WITH (NOLOCK) WHERE {where_sql}",
            params,
        )
        total = cur.fetchone()[0] or 0

        cur.execute(
            f"""
            SELECT seller_sku, asin, marketplace_id, plan_date,
                   risk_score, risk_tier, stockout_prob_7d,
                   days_cover, velocity_7d, velocity_30d,
                   velocity_trend, velocity_change_pct,
                   suggested_reorder_qty, reorder_urgency,
                   target_days_cover, lead_time_days, safety_stock_days,
                   estimated_stockout_date,
                   overstock_holding_cost_pln, aging_risk_pln,
                   units_available, is_acknowledged
            FROM dbo.acc_replenishment_plan WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY {order_col} {sort_dir}
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
            """,
            params + [offset, limit],
        )
        items = []
        for r in cur.fetchall():
            items.append({
                "seller_sku": r[0],
                "asin": r[1],
                "marketplace_id": r[2],
                "plan_date": r[3].isoformat() if r[3] else None,
                "risk_score": r[4],
                "risk_tier": r[5],
                "stockout_prob_7d": _fv(r[6]),
                "days_cover": _fv(r[7]),
                "velocity_7d": _fv(r[8]),
                "velocity_30d": _fv(r[9]),
                "velocity_trend": r[10],
                "velocity_change_pct": _fv(r[11]),
                "suggested_reorder_qty": r[12],
                "reorder_urgency": r[13],
                "target_days_cover": r[14],
                "lead_time_days": r[15],
                "safety_stock_days": r[16],
                "estimated_stockout_date": r[17].isoformat() if r[17] else None,
                "overstock_holding_cost_pln": _fv(r[18]),
                "aging_risk_pln": _fv(r[19]),
                "units_available": r[20],
                "is_acknowledged": bool(r[21]),
            })
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def acknowledge_replenishment(
    seller_sku: str,
    marketplace_id: str,
    *,
    acknowledged_by: str = "operator",
) -> bool:
    """Mark a replenishment suggestion as acknowledged."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_replenishment_plan
            SET is_acknowledged = 1,
                acknowledged_at = SYSUTCDATETIME(),
                acknowledged_by = %s
            WHERE seller_sku = %s AND marketplace_id = %s
              AND plan_date = (
                  SELECT MAX(plan_date) FROM dbo.acc_replenishment_plan WITH (NOLOCK)
              )
              AND is_acknowledged = 0
            """,
            (acknowledged_by, seller_sku, marketplace_id),
        )
        affected = cur.rowcount
        conn.commit()
        return affected > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_risk_alerts(
    marketplace_id: str | None = None,
    *,
    alert_type: str | None = None,
    include_resolved: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Paginated inventory risk alerts."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where = []
        params: list[Any] = []
        if not include_resolved:
            where.append("is_resolved = 0")
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)
        if alert_type:
            where.append("alert_type = %s")
            params.append(alert_type)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_inventory_risk_alert WITH (NOLOCK) WHERE {where_sql}",
            params,
        )
        total = cur.fetchone()[0] or 0

        cur.execute(
            f"""
            SELECT id, seller_sku, marketplace_id, alert_type, severity,
                   title, detail, current_value, previous_value,
                   threshold, risk_score, risk_tier,
                   is_resolved, resolved_at, triggered_at
            FROM dbo.acc_inventory_risk_alert WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY triggered_at DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
            """,
            params + [offset, limit],
        )
        items = []
        for r in cur.fetchall():
            items.append({
                "id": r[0],
                "seller_sku": r[1],
                "marketplace_id": r[2],
                "alert_type": r[3],
                "severity": r[4],
                "title": r[5],
                "detail": r[6],
                "current_value": _fv(r[7]),
                "previous_value": _fv(r[8]),
                "threshold": _fv(r[9]),
                "risk_score": r[10],
                "risk_tier": r[11],
                "is_resolved": bool(r[12]),
                "resolved_at": r[13].isoformat() if r[13] else None,
                "triggered_at": r[14].isoformat() if r[14] else None,
            })
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def resolve_risk_alert(alert_id: int) -> bool:
    """Mark a risk alert as resolved."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_inventory_risk_alert
            SET is_resolved = 1, resolved_at = SYSUTCDATETIME()
            WHERE id = %s AND is_resolved = 0
            """,
            (alert_id,),
        )
        affected = cur.rowcount
        conn.commit()
        return affected > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_velocity_trends(
    seller_sku: str,
    marketplace_id: str,
    *,
    days: int = 30,
) -> list[dict]:
    """Daily velocity + risk score trends for a specific SKU."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT score_date, velocity_7d, velocity_30d, velocity_cv,
                   risk_score, stockout_prob_7d, days_cover, units_available
            FROM dbo.acc_inventory_risk_score WITH (NOLOCK)
            WHERE seller_sku = %s AND marketplace_id = %s
              AND score_date >= DATEADD(DAY, -%s, CAST(GETUTCDATE() AS DATE))
            ORDER BY score_date ASC
            """,
            (seller_sku, marketplace_id, days),
        )
        return [
            {
                "date": r[0].isoformat() if r[0] else None,
                "velocity_7d": _fv(r[1]),
                "velocity_30d": _fv(r[2]),
                "velocity_cv": _fv(r[3]),
                "risk_score": r[4],
                "stockout_prob_7d": _fv(r[5]),
                "days_cover": _fv(r[6]),
                "units_available": r[7],
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()
