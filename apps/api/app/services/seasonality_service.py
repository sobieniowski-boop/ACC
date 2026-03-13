"""Seasonality & Demand Intelligence — core computation service.

Monthly aggregation, profile classification, index computation, and query helpers.
Heavy work runs as scheduler jobs, NOT per-request.

Data sources:
- acc_sku_profitability_rollup — own sales/profit (internal signal)
- acc_search_term_monthly — Amazon Brand Analytics search terms (market demand signal)

When search term data is available it is blended with internal sales to produce
a more accurate demand_index that reflects actual customer search behaviour
rather than only our own order history.
"""
from __future__ import annotations

import json
import math
import statistics
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import structlog

from app.core.config import MARKETPLACE_REGISTRY, RENEWED_SKU_SQL_FILTER
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Marketplace helpers (from central registry) ────────────────────
MKT_CODE = {mid: info["code"] for mid, info in MARKETPLACE_REGISTRY.items()}
ALL_MKT_IDS = list(MKT_CODE.keys())


def _f(v) -> float:
    if v is None:
        return 0.0
    return float(v)


def _rows(cur) -> list:
    rows = cur.fetchall()
    out = []
    for row in rows:
        conv = []
        for v in row:
            if isinstance(v, Decimal):
                conv.append(float(v))
            else:
                conv.append(v)
        out.append(tuple(conv))
    return out


# =====================================================================
# 1. MONTHLY AGGREGATION — build_monthly_seasonality_metrics
# =====================================================================

def build_monthly_metrics(*, months_back: int = 36) -> dict:
    """Aggregate acc_sku_profitability_rollup into seasonality_monthly_metrics.

    Uses SQL MERGE for bulk performance instead of row-by-row upserts.
    Returns summary of rows processed.
    """
    conn = connect_acc()
    cur = conn.cursor()
    cutoff = date.today() - timedelta(days=months_back * 31)

    log.info("seasonality.build_monthly.start", cutoff=str(cutoff))

    # Build a CASE expression mapping for marketplace codes
    case_parts = " ".join(
        f"WHEN '{mid}' THEN '{code}'" for mid, code in MKT_CODE.items()
    )
    mkt_case = f"CASE marketplace_id {case_parts} END"
    mkt_ids = ",".join(f"'{mid}'" for mid in MKT_CODE.keys())

    # MERGE SKU-level metrics in one SQL statement
    cur.execute(f"""
        MERGE seasonality_monthly_metrics AS tgt
        USING (
            SELECT {mkt_case} AS mkt,
                   sku,
                   YEAR(period_date) AS yr, MONTH(period_date) AS mo,
                   SUM(units_sold) AS units,
                   SUM(orders_count) AS orders,
                   SUM(revenue_pln) AS revenue,
                   SUM(profit_pln) AS cm1,
                   SUM(profit_pln - ad_spend_pln) AS cm2,
                   SUM(profit_pln - ad_spend_pln - storage_fee_pln - other_fees_pln) AS np,
                   SUM(ad_spend_pln) AS ad_spend,
                   SUM(refund_pln) AS refunds
            FROM acc_sku_profitability_rollup
            WHERE period_date >= '{cutoff.isoformat()}'
              AND marketplace_id IN ({mkt_ids})
              AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
            GROUP BY marketplace_id, sku, YEAR(period_date), MONTH(period_date)
        ) AS src
        ON tgt.marketplace = src.mkt
            AND tgt.entity_type = 'sku'
            AND tgt.entity_id = src.sku
            AND tgt.year = src.yr
            AND tgt.month = src.mo
        WHEN MATCHED THEN
            UPDATE SET units=src.units, orders=src.orders, revenue=src.revenue,
                       profit_cm1=src.cm1, profit_cm2=src.cm2, profit_np=src.np,
                       ad_spend=src.ad_spend, refunds=src.refunds,
                       created_at=SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (marketplace, entity_type, entity_id, year, month,
                    units, orders, revenue, profit_cm1, profit_cm2, profit_np,
                    ad_spend, refunds)
            VALUES (src.mkt, 'sku', src.sku, src.yr, src.mo,
                    src.units, src.orders, src.revenue,
                    src.cm1, src.cm2, src.np,
                    src.ad_spend, src.refunds);
    """)
    sku_rows = cur.rowcount
    conn.commit()
    log.info("seasonality.build_monthly.sku_merge_done", rows=sku_rows)

    # Also build category-level aggregates from SKU data
    cat_rows = _build_category_aggregates(cur, conn)
    conn.close()

    result = {"sku_rows": sku_rows, "category_rows": cat_rows}
    log.info("seasonality.build_monthly.done", **result)
    return result


def _build_category_aggregates(cur, conn) -> int:
    """Roll up SKU monthly metrics to category level via SQL MERGE."""
    cur.execute("""
        MERGE seasonality_monthly_metrics AS tgt
        USING (
            SELECT m.marketplace, p.category AS cat, m.year AS yr, m.month AS mo,
                   SUM(m.units) AS units, SUM(m.orders) AS orders,
                   SUM(m.revenue) AS revenue,
                   SUM(m.profit_cm1) AS cm1, SUM(m.profit_cm2) AS cm2,
                   SUM(m.profit_np) AS np,
                   SUM(m.ad_spend) AS ad_spend, SUM(m.refunds) AS refunds
            FROM seasonality_monthly_metrics m
            JOIN acc_product p ON p.sku = m.entity_id
            WHERE m.entity_type = 'sku'
              AND p.category IS NOT NULL AND p.category != ''
            GROUP BY m.marketplace, p.category, m.year, m.month
        ) AS src
        ON tgt.marketplace = src.marketplace
            AND tgt.entity_type = 'category'
            AND tgt.entity_id = src.cat
            AND tgt.year = src.yr
            AND tgt.month = src.mo
        WHEN MATCHED THEN
            UPDATE SET units=src.units, orders=src.orders, revenue=src.revenue,
                       profit_cm1=src.cm1, profit_cm2=src.cm2, profit_np=src.np,
                       ad_spend=src.ad_spend, refunds=src.refunds,
                       created_at=SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (marketplace, entity_type, entity_id, year, month,
                    units, orders, revenue, profit_cm1, profit_cm2, profit_np,
                    ad_spend, refunds)
            VALUES (src.marketplace, 'category', src.cat, src.yr, src.mo,
                    src.units, src.orders, src.revenue,
                    src.cm1, src.cm2, src.np,
                    src.ad_spend, src.refunds);
    """)
    n = cur.rowcount
    conn.commit()
    return n


# =====================================================================
# 2. SEASONALITY INDEX — compute month-level indices (1.0 = average)
# =====================================================================

def recompute_indices() -> dict:
    """For each entity: compute demand/sales/profit index per month (1-12).

    index = avg_metric_for_month / avg_metric_across_all_months.
    Uses SQL MERGE for bulk performance.

    When acc_search_term_monthly has data, also computes search_demand_index
    and blends it with the sales-based demand_index.
    """
    conn = connect_acc()
    cur = conn.cursor()

    # ── DDL: add search_demand_index column if missing ──────────────
    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'seasonality_index_cache'
              AND COLUMN_NAME = 'search_demand_index'
        )
        ALTER TABLE seasonality_index_cache
            ADD search_demand_index FLOAT NULL;
    """)
    conn.commit()

    # Delete existing index cache and rebuild via SQL
    cur.execute("DELETE FROM seasonality_index_cache")
    conn.commit()

    cur.execute("""
        INSERT INTO seasonality_index_cache
            (marketplace, entity_type, entity_id, month,
             demand_index, sales_index, profit_index)
        SELECT
            m.marketplace, m.entity_type, m.entity_id, m.month,
            CASE WHEN a.avg_demand > 0
                THEN ROUND(AVG(COALESCE(m.units, 0)) / a.avg_demand, 6)
                ELSE 1.0 END AS demand_index,
            CASE WHEN a.avg_sales > 0
                THEN ROUND(AVG(COALESCE(m.revenue, 0)) / a.avg_sales, 6)
                ELSE 1.0 END AS sales_index,
            CASE WHEN a.avg_profit != 0
                THEN ROUND(AVG(COALESCE(m.profit_cm2, 0)) / a.avg_profit, 6)
                ELSE 1.0 END AS profit_index
        FROM seasonality_monthly_metrics m
        CROSS APPLY (
            SELECT
                AVG(COALESCE(i.units, 0)) AS avg_demand,
                AVG(COALESCE(i.revenue, 0)) AS avg_sales,
                AVG(COALESCE(i.profit_cm2, 0)) AS avg_profit
            FROM seasonality_monthly_metrics i
            WHERE i.marketplace = m.marketplace
                AND i.entity_type = m.entity_type
                AND i.entity_id = m.entity_id
        ) a
        GROUP BY m.marketplace, m.entity_type, m.entity_id, m.month,
                 a.avg_demand, a.avg_sales, a.avg_profit
    """)
    processed = cur.rowcount
    conn.commit()

    # ── Blend search term demand (if data exists) ───────────────────
    search_blended = _blend_search_demand(cur, conn)

    conn.close()
    log.info("seasonality.indices.done", rows_inserted=processed,
             search_blended=search_blended)
    return {"rows_inserted": processed, "search_blended": search_blended}


# ── Search-term demand blending ──────────────────────────────────────

# Weight split: 60% search term demand (market signal), 40% own sales demand.
# When search term data is missing for an entity, demand_index stays unchanged.
SEARCH_DEMAND_WEIGHT = 0.6
SALES_DEMAND_WEIGHT = 0.4


def _blend_search_demand(cur, conn) -> int:
    """Compute search_demand_index from Brand Analytics and blend into demand_index.

    For each SKU in seasonality_index_cache:
    1. Look up ASINs via acc_product (sku → asin).
    2. Get monthly search frequency ranks from acc_search_term_monthly for those ASINs.
    3. Convert ranks → index (lower rank = higher demand): index = avg_rank_all_months / rank_this_month.
    4. Write search_demand_index to seasonality_index_cache.
    5. Update demand_index = SEARCH_DEMAND_WEIGHT * search_demand_index + SALES_DEMAND_WEIGHT * demand_index.

    Returns number of rows blended.
    """
    # Check if search term data exists
    cur.execute("""
        IF OBJECT_ID('acc_search_term_monthly', 'U') IS NOT NULL
            SELECT COUNT(*) FROM acc_search_term_monthly WITH (NOLOCK)
        ELSE
            SELECT 0
    """)
    row = cur.fetchone()
    if not row or int(row[0]) == 0:
        log.info("seasonality.blend_search.skipped_no_data")
        return 0

    # Marketplace ID → code mapping for join
    mkt_code_parts = " ".join(
        f"WHEN '{mid}' THEN '{code}'" for mid, code in MKT_CODE.items()
    )
    mkt_case = f"CASE stm.marketplace_id {mkt_code_parts} END"

    # Compute per-ASIN monthly search demand index and write to index cache.
    # Uses inverse rank as demand signal (lower rank → higher demand).
    # The index is normalized per entity: (avg_rank / rank_for_month).
    cur.execute(f"""
        ;WITH asin_search AS (
            -- Aggregate search term signals per ASIN per marketplace per month
            SELECT
                {mkt_case} AS marketplace,
                stm.asin,
                stm.month,
                -- Use inverse of avg frequency rank as demand measure:
                -- best (lowest) rank across all terms mentioning this ASIN
                MIN(stm.avg_frequency_rank) AS best_rank,
                SUM(stm.avg_click_share)    AS total_click_share,
                SUM(stm.weeks_seen)         AS total_weeks
            FROM acc_search_term_monthly stm WITH (NOLOCK)
            WHERE stm.avg_frequency_rank > 0
            GROUP BY stm.marketplace_id, stm.asin, stm.month
        ),
        asin_avg AS (
            -- Average rank across all months for each ASIN (baseline)
            SELECT marketplace, asin,
                   AVG(best_rank) AS avg_rank
            FROM asin_search
            GROUP BY marketplace, asin
            HAVING AVG(best_rank) > 0
        ),
        search_index AS (
            -- Search demand index: avg_rank / this_month_rank  (>1 = above avg demand)
            SELECT s.marketplace, s.asin, s.month,
                   ROUND(a.avg_rank / s.best_rank, 6) AS search_idx
            FROM asin_search s
            JOIN asin_avg a ON a.marketplace = s.marketplace AND a.asin = s.asin
            WHERE s.best_rank > 0
        )
        UPDATE ic
        SET ic.search_demand_index = si.search_idx,
            ic.demand_index = ROUND(
                {SEARCH_DEMAND_WEIGHT} * si.search_idx
                + {SALES_DEMAND_WEIGHT} * ic.demand_index,
            6)
        FROM seasonality_index_cache ic
        JOIN acc_product p WITH (NOLOCK) ON p.sku = ic.entity_id
        JOIN search_index si
            ON si.marketplace = ic.marketplace
            AND si.asin = p.asin
            AND si.month = ic.month
        WHERE ic.entity_type = 'sku'
    """)
    blended = cur.rowcount
    conn.commit()
    log.info("seasonality.blend_search.done", rows_blended=blended)
    return blended


# =====================================================================
# 3. PROFILE CLASSIFICATION — classify + score each entity
# =====================================================================

def recompute_profiles() -> dict:
    """Classify each entity's seasonality and compute all scores."""
    conn = connect_acc()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT marketplace, entity_type, entity_id
        FROM seasonality_index_cache
    """)
    entities = _rows(cur)
    processed = 0

    for mkt, etype, eid in entities:
        # Get indices
        cur.execute("""
            SELECT month, demand_index, sales_index, profit_index
            FROM seasonality_index_cache
            WHERE marketplace=? AND entity_type=? AND entity_id=?
            ORDER BY month
        """, (mkt, etype, eid))
        idx_rows = _rows(cur)

        if len(idx_rows) < 2:
            continue

        d_indices = [_f(r[1]) for r in idx_rows]
        s_indices = [_f(r[2]) for r in idx_rows]
        p_indices = [_f(r[3]) for r in idx_rows]
        months = [int(r[0]) for r in idx_rows]

        # Compute scores
        demand_strength = _strength_score(d_indices)
        sales_strength = _strength_score(s_indices)
        profit_strength = _strength_score(p_indices)

        evergreen = _evergreen_score(d_indices, s_indices)
        volatility = _volatility_score(d_indices, s_indices)

        # Count data coverage for confidence
        cur.execute("""
            SELECT COUNT(*) FROM seasonality_monthly_metrics
            WHERE marketplace=? AND entity_type=? AND entity_id=?
        """, (mkt, etype, eid))
        data_months = int(_rows(cur)[0][0])
        confidence = _confidence_score(data_months, d_indices)

        # Classify
        avg_strength = (demand_strength + sales_strength) / 2
        s_class = _classify(avg_strength, evergreen, volatility)

        # Peak detection
        peak_months = _detect_peaks(d_indices, months)
        ramp_months = _detect_ramp(d_indices, months, peak_months)
        decay_months = _detect_decay(d_indices, months, peak_months)
        season_length = len(peak_months) + len(ramp_months) + len(decay_months)

        # Gaps
        d_vs_s_gap = _gap_score(d_indices, s_indices)
        s_vs_p_gap = _gap_score(s_indices, p_indices)

        # Upsert profile
        cur.execute("""
            UPDATE seasonality_profile
            SET seasonality_class=?, demand_strength_score=?,
                sales_strength_score=?, profit_strength_score=?,
                evergreen_score=?, volatility_score=?,
                seasonality_confidence_score=?,
                peak_months_json=?, ramp_months_json=?, decay_months_json=?,
                season_length_months=?,
                demand_vs_sales_gap=?, sales_vs_profit_gap=?,
                updated_at=SYSUTCDATETIME()
            WHERE marketplace=? AND entity_type=? AND entity_id=?
        """, (s_class, demand_strength, sales_strength, profit_strength,
              evergreen, volatility, confidence,
              json.dumps(peak_months), json.dumps(ramp_months),
              json.dumps(decay_months), season_length,
              d_vs_s_gap, s_vs_p_gap,
              mkt, etype, eid))

        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO seasonality_profile
                    (marketplace, entity_type, entity_id, seasonality_class,
                     demand_strength_score, sales_strength_score,
                     profit_strength_score, evergreen_score, volatility_score,
                     seasonality_confidence_score,
                     peak_months_json, ramp_months_json, decay_months_json,
                     season_length_months, demand_vs_sales_gap, sales_vs_profit_gap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (mkt, etype, eid, s_class,
                  demand_strength, sales_strength, profit_strength,
                  evergreen, volatility, confidence,
                  json.dumps(peak_months), json.dumps(ramp_months),
                  json.dumps(decay_months), season_length,
                  d_vs_s_gap, s_vs_p_gap))

        processed += 1
        if processed % 500 == 0:
            conn.commit()

    conn.commit()
    conn.close()
    log.info("seasonality.profiles.done", entities_classified=processed)
    return {"entities_classified": processed}


# ── scoring helpers ──────────────────────────────────────────────────

def _strength_score(indices: list[float]) -> float:
    """0-100 score: how concentrated is volume in specific months."""
    if not indices or all(v == 0 for v in indices):
        return 0.0
    try:
        std = statistics.stdev(indices)
    except statistics.StatisticsError:
        return 0.0
    # Normalize: std of 0 → 0, std of 1.0+ → 100
    return round(min(std * 100, 100), 2)


def _evergreen_score(d_indices: list[float], s_indices: list[float]) -> float:
    """0-100: how flat is the distribution (100 = perfectly flat = evergreen)."""
    if not d_indices:
        return 50.0
    combined = [(d + s) / 2 for d, s in zip(d_indices, s_indices)]
    try:
        cv = statistics.stdev(combined) / statistics.mean(combined) if statistics.mean(combined) > 0 else 0
    except (statistics.StatisticsError, ZeroDivisionError):
        return 50.0
    # cv of 0 → 100 (perfect evergreen), cv of 1+ → 0
    return round(max(0, min(100, (1 - cv) * 100)), 2)


def _volatility_score(d_indices: list[float], s_indices: list[float]) -> float:
    """0-100: how erratic are month-to-month swings."""
    if len(d_indices) < 3:
        return 0.0
    deltas = []
    for i in range(1, len(d_indices)):
        deltas.append(abs(d_indices[i] - d_indices[i - 1]))
        deltas.append(abs(s_indices[i] - s_indices[i - 1]))
    avg_delta = statistics.mean(deltas) if deltas else 0
    return round(min(avg_delta * 50, 100), 2)


def _confidence_score(data_months: int, indices: list[float]) -> float:
    """0-100: confidence in the seasonality classification."""
    # More months → more confidence
    coverage = min(data_months / 24.0, 1.0) * 60  # up to 60 pts for 24+ months
    # Stability of pattern
    if len(indices) >= 12:
        stability = 40  # full year coverage
    elif len(indices) >= 6:
        stability = 20
    else:
        stability = 5
    return round(min(coverage + stability, 100), 2)


def _classify(strength: float, evergreen: float, volatility: float) -> str:
    """Assign seasonality class based on scores."""
    if evergreen >= 75 and strength < 25:
        return "EVERGREEN"
    if volatility > 70:
        return "IRREGULAR"
    if strength >= 80:
        return "PEAK_SEASONAL"
    if strength >= 55:
        return "STRONG_SEASONAL"
    if strength >= 25:
        return "MILD_SEASONAL"
    return "EVERGREEN"


def _detect_peaks(indices: list[float], months: list[int]) -> list[int]:
    """Return top 3 months by index value."""
    if not indices:
        return []
    paired = sorted(zip(months, indices), key=lambda x: x[1], reverse=True)
    return [m for m, _ in paired[:3]]


def _detect_ramp(indices: list[float], months: list[int],
                 peak_months: list[int]) -> list[int]:
    """Months just before peak where index is rising."""
    if not peak_months:
        return []
    ramp = []
    for pm in peak_months:
        prev = pm - 1 if pm > 1 else 12
        if prev in months and prev not in peak_months:
            ramp.append(prev)
    return sorted(set(ramp))


def _detect_decay(indices: list[float], months: list[int],
                  peak_months: list[int]) -> list[int]:
    """Months just after peak where index is declining."""
    if not peak_months:
        return []
    decay = []
    for pm in peak_months:
        nxt = pm + 1 if pm < 12 else 1
        if nxt in months and nxt not in peak_months:
            decay.append(nxt)
    return sorted(set(decay))


def _gap_score(a_indices: list[float], b_indices: list[float]) -> float:
    """Mean absolute difference between two index series."""
    if not a_indices or not b_indices:
        return 0.0
    diffs = [abs(a - b) for a, b in zip(a_indices, b_indices)]
    return round(statistics.mean(diffs), 4)


# =====================================================================
# 4. QUERY HELPERS — for API layer
# =====================================================================

def get_overview(*, marketplace: str | None = None) -> dict:
    """Overview dashboard data."""
    conn = connect_acc()
    cur = conn.cursor()

    where = "WHERE 1=1"
    params: list = []
    if marketplace:
        where += " AND marketplace = ?"
        params.append(marketplace)

    # Class distribution
    cur.execute(f"""
        SELECT seasonality_class, COUNT(*) cnt
        FROM seasonality_profile {where}
        GROUP BY seasonality_class
    """, params)
    dist = {r[0]: int(r[1]) for r in _rows(cur)}

    seasonal_count = sum(v for k, v in dist.items() if k != "EVERGREEN")
    evergreen_count = dist.get("EVERGREEN", 0)

    # Entity counts by type
    cur.execute(f"""
        SELECT entity_type, COUNT(*) FROM seasonality_profile {where} GROUP BY entity_type
    """, params)
    type_counts = {r[0]: int(r[1]) for r in _rows(cur)}

    # Upcoming peak (next 2 months)
    current_month = date.today().month
    next_months = [current_month + 1 if current_month < 12 else 1,
                   current_month + 2 if current_month < 11 else (current_month + 2 - 12)]

    cur.execute(f"""
        SELECT TOP 5 p.entity_type, p.entity_id, p.marketplace,
               p.demand_strength_score, p.peak_months_json,
               CASE WHEN p.entity_type = 'sku'
                    THEN (SELECT TOP 1 pr.title FROM acc_product pr WHERE pr.sku = p.entity_id)
                    ELSE p.entity_id END AS display_name
        FROM seasonality_profile p {where}
              AND p.seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
        ORDER BY p.demand_strength_score DESC
    """, params)
    upcoming = []
    for r in _rows(cur):
        peak_m = json.loads(r[4]) if r[4] else []
        if any(m in next_months for m in peak_m):
            upcoming.append({
                "entity_type": r[0], "entity_id": r[1],
                "marketplace": r[2], "strength": r[3],
                "peak_months": peak_m,
                "display_name": r[5],
            })

    # Biggest execution gaps
    cur.execute(f"""
        SELECT TOP 5 p.entity_type, p.entity_id, p.marketplace,
               p.demand_vs_sales_gap, p.sales_vs_profit_gap,
               CASE WHEN p.entity_type = 'sku'
                    THEN (SELECT TOP 1 pr.title FROM acc_product pr WHERE pr.sku = p.entity_id)
                    ELSE p.entity_id END AS display_name
        FROM seasonality_profile p {where}
        ORDER BY p.demand_vs_sales_gap DESC
    """, params)
    gaps = [{"entity_type": r[0], "entity_id": r[1], "marketplace": r[2],
             "demand_vs_sales_gap": r[3], "sales_vs_profit_gap": r[4],
             "display_name": r[5]}
            for r in _rows(cur)]

    # Marketplace heatmap: use CATEGORY-level data for meaningful signal
    cur.execute("""
        SELECT ic.marketplace, ic.month,
               AVG(ic.demand_index) d, AVG(ic.sales_index) s, AVG(ic.profit_index) p
        FROM seasonality_index_cache ic
        WHERE ic.entity_type = 'category'
        GROUP BY ic.marketplace, ic.month
        ORDER BY ic.marketplace, ic.month
    """)
    heatmap = [{"marketplace": r[0], "month": int(r[1]),
                "demand_index": r[2], "sales_index": r[3], "profit_index": r[4]}
               for r in _rows(cur)]

    # Peak calendar — with product titles for SKUs
    cur.execute(f"""
        SELECT TOP 20 p.entity_type, p.entity_id, p.marketplace, p.peak_months_json,
               p.demand_strength_score,
               CASE WHEN p.entity_type = 'sku'
                    THEN (SELECT TOP 1 pr.title FROM acc_product pr WHERE pr.sku = p.entity_id)
                    ELSE NULL END AS product_title
        FROM seasonality_profile p {where}
              AND p.seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
        ORDER BY p.demand_strength_score DESC
    """, params)
    calendar = [{"entity_type": r[0], "entity_id": r[1], "marketplace": r[2],
                 "peak_months": json.loads(r[3]) if r[3] else [],
                 "strength": r[4],
                 "product_title": r[5]}
                for r in _rows(cur)]

    # Top opportunities — with product titles
    cur.execute(f"""
        SELECT TOP 10 o.id, o.marketplace, o.entity_type, o.entity_id,
               o.opportunity_type, o.title, o.priority_score, o.estimated_revenue_uplift,
               o.recommended_start_date,
               CASE WHEN o.entity_type = 'sku'
                    THEN (SELECT TOP 1 pr.title FROM acc_product pr WHERE pr.sku = o.entity_id)
                    ELSE NULL END AS product_title
        FROM seasonality_opportunity o
        WHERE o.status = 'new'
              {"AND o.marketplace = ?" if marketplace else ""}
        ORDER BY o.priority_score DESC
    """, [marketplace] if marketplace else [])
    opps = [{"id": int(r[0]), "marketplace": r[1], "entity_type": r[2],
             "entity_id": r[3], "type": r[4], "title": r[5],
             "priority": r[6], "revenue_uplift": r[7],
             "start_date": str(r[8]) if r[8] else None,
             "product_title": r[9]}
            for r in _rows(cur)]

    # Search term coverage stats
    cur.execute("SELECT COUNT(*) FROM acc_search_term_monthly WITH (NOLOCK)")
    search_terms_count = int(_rows(cur)[0][0])
    cur.execute("""
        SELECT COUNT(*) FROM seasonality_index_cache
        WHERE search_demand_index IS NOT NULL AND search_demand_index != 0
    """)
    blended_count = int(_rows(cur)[0][0])

    conn.close()

    return {
        "kpi": {
            "total_entities": seasonal_count + evergreen_count,
            "sku_count": type_counts.get("sku", 0),
            "category_count": type_counts.get("category", 0),
            "seasonal_categories": seasonal_count,
            "evergreen_categories": evergreen_count,
            "strongest_upcoming_season": upcoming[0] if upcoming else None,
            "highest_demand_ramp": upcoming[1] if len(upcoming) > 1 else None,
            "biggest_execution_gap": gaps[0] if gaps else None,
            "biggest_profit_opportunity": None,
            "search_terms_count": search_terms_count,
            "search_blended_count": blended_count,
        },
        "marketplace_heatmap": heatmap,
        "class_distribution": dist,
        "upcoming_opportunities": opps,
        "peak_calendar": calendar,
    }


def get_map(*, entity_type: str = "sku", marketplace: str | None = None,
            seasonality_class: str | None = None,
            page: int = 1, page_size: int = 50) -> dict:
    """Heatmap matrix data."""
    conn = connect_acc()
    cur = conn.cursor()

    where = "WHERE p.entity_type = ?"
    params: list = [entity_type]
    if marketplace:
        where += " AND p.marketplace = ?"
        params.append(marketplace)
    if seasonality_class:
        where += " AND p.seasonality_class = ?"
        params.append(seasonality_class)

    # Count
    cur.execute(f"SELECT COUNT(*) FROM seasonality_profile p {where}", params)
    total = int(_rows(cur)[0][0])

    # Available filter values (so frontend knows what exists)
    cur.execute("SELECT DISTINCT entity_type FROM seasonality_profile ORDER BY entity_type")
    avail_entity_types = [r[0] for r in _rows(cur)]
    cur.execute("SELECT DISTINCT marketplace FROM seasonality_profile ORDER BY marketplace")
    avail_marketplaces = [r[0] for r in _rows(cur)]
    cur.execute("SELECT DISTINCT seasonality_class FROM seasonality_profile ORDER BY seasonality_class")
    avail_classes = [r[0] for r in _rows(cur)]

    # Page
    offset = (page - 1) * page_size
    cur.execute(f"""
        SELECT p.entity_type, p.entity_id, p.marketplace,
               p.seasonality_class, p.demand_strength_score,
               p.seasonality_confidence_score, p.evergreen_score,
               p.volatility_score, p.peak_months_json,
               CASE WHEN p.entity_type = 'sku'
                    THEN (SELECT TOP 1 pr.title FROM acc_product pr WHERE pr.sku = p.entity_id)
                    ELSE NULL END AS product_title
        FROM seasonality_profile p
        {where}
        ORDER BY p.demand_strength_score DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """, params + [offset, page_size])
    profile_rows = _rows(cur)

    items = []
    for r in profile_rows:
        etype, eid, mkt = r[0], r[1], r[2]
        # Fetch indices (include search_demand_index)
        cur.execute("""
            SELECT month, demand_index, sales_index, profit_index,
                   search_demand_index
            FROM seasonality_index_cache
            WHERE marketplace=? AND entity_type=? AND entity_id=?
            ORDER BY month
        """, (mkt, etype, eid))
        idx = [{"month": int(ir[0]), "demand_index": ir[1],
                "sales_index": ir[2], "profit_index": ir[3],
                "search_demand_index": ir[4]}
               for ir in _rows(cur)]

        items.append({
            "entity_type": etype, "entity_id": eid, "marketplace": mkt,
            "product_title": r[9],
            "indices": idx,
            "seasonality_class": r[3],
            "peak_months": json.loads(r[8]) if r[8] else [],
            "strength_score": r[4], "confidence_score": r[5],
            "evergreen_score": r[6], "volatility_score": r[7],
        })

    # Search term demand curves per marketplace per month
    mkt_code_parts = " ".join(
        f"WHEN '{mid}' THEN '{code}'" for mid, code in MKT_CODE.items()
    )
    mkt_case = f"CASE stm.marketplace_id {mkt_code_parts} END"
    cur.execute(f"""
        ;WITH monthly AS (
            SELECT {mkt_case} AS mkt, stm.month,
                   AVG(1.0 / NULLIF(stm.avg_frequency_rank, 0)) AS raw_demand,
                   COUNT(DISTINCT stm.search_term) AS terms_count
            FROM acc_search_term_monthly stm WITH (NOLOCK)
            WHERE stm.avg_frequency_rank > 0
            GROUP BY stm.marketplace_id, stm.month
        ),
        yearly_avg AS (
            SELECT mkt, AVG(raw_demand) AS avg_demand
            FROM monthly
            WHERE mkt IS NOT NULL
            GROUP BY mkt
        )
        SELECT m.mkt, m.month,
               ROUND(m.raw_demand / NULLIF(y.avg_demand, 0), 4) AS demand_index,
               m.terms_count
        FROM monthly m
        JOIN yearly_avg y ON y.mkt = m.mkt
        WHERE m.mkt IS NOT NULL
        ORDER BY m.mkt, m.month
    """)
    search_curves = [{"marketplace": r[0], "month": int(r[1]),
                      "demand_index": float(r[2]) if r[2] else 1.0,
                      "terms_count": int(r[3])}
                     for r in _rows(cur)]

    conn.close()
    return {
        "items": items, "total": total, "page": page, "page_size": page_size,
        "search_demand_curves": search_curves,
        "available_filters": {
            "entity_types": avail_entity_types,
            "marketplaces": avail_marketplaces,
            "classes": avail_classes,
        },
    }


def get_entities(*, entity_type: str | None = None,
                 marketplace: str | None = None,
                 seasonality_class: str | None = None,
                 sort: str = "demand_strength_score",
                 page: int = 1, page_size: int = 50) -> dict:
    """Paginated entity list with profiles."""
    conn = connect_acc()
    cur = conn.cursor()

    where_parts = ["1=1"]
    params: list = []
    if entity_type:
        where_parts.append("p.entity_type = ?")
        params.append(entity_type)
    if marketplace:
        where_parts.append("p.marketplace = ?")
        params.append(marketplace)
    if seasonality_class:
        where_parts.append("p.seasonality_class = ?")
        params.append(seasonality_class)

    where = " AND ".join(where_parts)

    # Validate sort column
    allowed_sorts = {
        "demand_strength_score", "sales_strength_score", "profit_strength_score",
        "evergreen_score", "volatility_score", "seasonality_confidence_score",
        "demand_vs_sales_gap", "entity_id",
    }
    if sort not in allowed_sorts:
        sort = "demand_strength_score"

    cur.execute(f"SELECT COUNT(*) FROM seasonality_profile p WHERE {where}", params)
    total = int(_rows(cur)[0][0])

    # Available filter values
    cur.execute("SELECT DISTINCT entity_type FROM seasonality_profile ORDER BY entity_type")
    avail_entity_types = [r[0] for r in _rows(cur)]
    cur.execute("SELECT DISTINCT marketplace FROM seasonality_profile ORDER BY marketplace")
    avail_marketplaces = [r[0] for r in _rows(cur)]
    cur.execute("SELECT DISTINCT seasonality_class FROM seasonality_profile ORDER BY seasonality_class")
    avail_classes = [r[0] for r in _rows(cur)]

    offset = (page - 1) * page_size
    cur.execute(f"""
        SELECT p.id, p.marketplace, p.entity_type, p.entity_id, p.seasonality_class,
               p.demand_strength_score, p.sales_strength_score, p.profit_strength_score,
               p.evergreen_score, p.volatility_score, p.seasonality_confidence_score,
               p.peak_months_json, p.ramp_months_json, p.decay_months_json,
               p.season_length_months, p.demand_vs_sales_gap, p.sales_vs_profit_gap,
               p.updated_at,
               CASE WHEN p.entity_type = 'sku'
                    THEN (SELECT TOP 1 pr.title FROM acc_product pr WHERE pr.sku = p.entity_id)
                    ELSE NULL END AS product_title
        FROM seasonality_profile p
        WHERE {where}
        ORDER BY p.{sort} DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """, params + [offset, page_size])

    items = []
    for r in _rows(cur):
        items.append({
            "id": int(r[0]), "marketplace": r[1], "entity_type": r[2],
            "entity_id": r[3], "seasonality_class": r[4],
            "demand_strength_score": r[5], "sales_strength_score": r[6],
            "profit_strength_score": r[7], "evergreen_score": r[8],
            "volatility_score": r[9], "seasonality_confidence_score": r[10],
            "peak_months": json.loads(r[11]) if r[11] else [],
            "ramp_months": json.loads(r[12]) if r[12] else [],
            "decay_months": json.loads(r[13]) if r[13] else [],
            "season_length_months": r[14],
            "demand_vs_sales_gap": r[15], "sales_vs_profit_gap": r[16],
            "updated_at": str(r[17]) if r[17] else None,
            "product_title": r[18],
        })

    conn.close()
    return {
        "items": items, "total": total, "page": page, "page_size": page_size,
        "available_filters": {
            "entity_types": avail_entity_types,
            "marketplaces": avail_marketplaces,
            "classes": avail_classes,
        },
    }


def get_entity_detail(entity_type: str, entity_id: str,
                      marketplace: str | None = None) -> dict:
    """Full detail for one entity across marketplaces."""
    conn = connect_acc()
    cur = conn.cursor()

    # Profile
    where = "entity_type=? AND entity_id=?"
    params: list = [entity_type, entity_id]
    if marketplace:
        where += " AND marketplace=?"
        params.append(marketplace)

    cur.execute(f"""
        SELECT id, marketplace, entity_type, entity_id, seasonality_class,
               demand_strength_score, sales_strength_score, profit_strength_score,
               evergreen_score, volatility_score, seasonality_confidence_score,
               peak_months_json, ramp_months_json, decay_months_json,
               season_length_months, demand_vs_sales_gap, sales_vs_profit_gap,
               updated_at
        FROM seasonality_profile
        WHERE {where}
    """, params)
    prof_rows = _rows(cur)
    if not prof_rows:
        conn.close()
        return None

    pr = prof_rows[0]
    profile = {
        "id": int(pr[0]), "marketplace": pr[1], "entity_type": pr[2],
        "entity_id": pr[3], "seasonality_class": pr[4],
        "demand_strength_score": pr[5], "sales_strength_score": pr[6],
        "profit_strength_score": pr[7], "evergreen_score": pr[8],
        "volatility_score": pr[9], "seasonality_confidence_score": pr[10],
        "peak_months": json.loads(pr[11]) if pr[11] else [],
        "ramp_months": json.loads(pr[12]) if pr[12] else [],
        "decay_months": json.loads(pr[13]) if pr[13] else [],
        "season_length_months": pr[14],
        "demand_vs_sales_gap": pr[15], "sales_vs_profit_gap": pr[16],
        "updated_at": str(pr[17]) if pr[17] else None,
    }

    # Monthly metrics (all years)
    cur.execute(f"""
        SELECT id, marketplace, entity_type, entity_id, year, month,
               sessions, page_views, clicks, impressions, purchases,
               units, orders, revenue, profit_cm1, profit_cm2, profit_np,
               unit_session_pct, ad_spend, refunds, stockout_days, suppression_days
        FROM seasonality_monthly_metrics
        WHERE {where}
        ORDER BY year, month
    """, params)
    monthly = [
        {"id": int(r[0]), "marketplace": r[1], "entity_type": r[2],
         "entity_id": r[3], "year": int(r[4]), "month": int(r[5]),
         "sessions": r[6], "page_views": r[7], "clicks": r[8],
         "impressions": r[9], "purchases": r[10], "units": r[11],
         "orders": r[12], "revenue": r[13], "profit_cm1": r[14],
         "profit_cm2": r[15], "profit_np": r[16],
         "unit_session_pct": r[17], "ad_spend": r[18],
         "refunds": r[19], "stockout_days": r[20], "suppression_days": r[21]}
        for r in _rows(cur)
    ]

    # Indices
    idx_where = "entity_type=? AND entity_id=?"
    idx_params: list = [entity_type, entity_id]
    if marketplace:
        idx_where += " AND marketplace=?"
        idx_params.append(marketplace)

    cur.execute(f"""
        SELECT month, demand_index, sales_index, profit_index
        FROM seasonality_index_cache
        WHERE {idx_where}
        ORDER BY month
    """, idx_params)
    indices = [{"month": int(r[0]), "demand_index": r[1],
                "sales_index": r[2], "profit_index": r[3]}
               for r in _rows(cur)]

    # Marketplace comparison (same entity across all marketplaces)
    cur.execute("""
        SELECT marketplace, demand_strength_score, sales_strength_score,
               profit_strength_score, seasonality_class, peak_months_json
        FROM seasonality_profile
        WHERE entity_type=? AND entity_id=?
        ORDER BY marketplace
    """, (entity_type, entity_id))
    mkt_comp = [{"marketplace": r[0], "demand_strength": r[1],
                 "sales_strength": r[2], "profit_strength": r[3],
                 "class": r[4], "peak_months": json.loads(r[5]) if r[5] else []}
                for r in _rows(cur)]

    # Demand vs execution gap detail
    gap = {"demand_vs_sales_gap": profile["demand_vs_sales_gap"],
           "sales_vs_profit_gap": profile["sales_vs_profit_gap"],
           "root_causes": []}

    # Check stockout overlap
    stockout_months = [m for m in monthly if (m.get("stockout_days") or 0) > 5]
    if stockout_months:
        gap["root_causes"].append({"type": "stockout", "months": len(stockout_months)})

    conn.close()

    return {
        "profile": profile,
        "monthly_metrics": monthly,
        "indices": indices,
        "demand_vs_execution_gap": gap,
        "marketplace_comparison": mkt_comp,
    }


# =====================================================================
# 5. SETTINGS HELPERS
# =====================================================================

def get_settings() -> dict:
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("SELECT setting_key, setting_value FROM seasonality_settings")
    result = {r[0]: r[1] for r in _rows(cur)}
    conn.close()
    return {"settings": result}


def update_settings(settings: dict) -> dict:
    conn = connect_acc()
    cur = conn.cursor()
    for k, v in settings.items():
        cur.execute("""
            UPDATE seasonality_settings SET setting_value=?, updated_at=SYSUTCDATETIME()
            WHERE setting_key=?
        """, (str(v), k))
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO seasonality_settings (setting_key, setting_value)
                VALUES (?, ?)
            """, (k, str(v)))
    conn.commit()
    conn.close()
    return get_settings()


# =====================================================================
# 6. OPPORTUNITIES QUERIES
# =====================================================================

def get_opportunities_page(*, marketplace: str | None = None,
                           opportunity_type: str | None = None,
                           status: str | None = None,
                           entity_type: str | None = None,
                           page: int = 1, page_size: int = 50) -> dict:
    conn = connect_acc()
    cur = conn.cursor()

    where_parts = ["1=1"]
    params: list = []
    if marketplace:
        where_parts.append("o.marketplace = ?")
        params.append(marketplace)
    if opportunity_type:
        where_parts.append("o.opportunity_type = ?")
        params.append(opportunity_type)
    if status:
        where_parts.append("o.status = ?")
        params.append(status)
    if entity_type:
        where_parts.append("o.entity_type = ?")
        params.append(entity_type)

    where = " AND ".join(where_parts)

    cur.execute(f"SELECT COUNT(*) FROM seasonality_opportunity o WHERE {where}", params)
    total = int(_rows(cur)[0][0])

    # Available filter values
    cur.execute("SELECT DISTINCT marketplace FROM seasonality_opportunity ORDER BY marketplace")
    avail_marketplaces = [r[0] for r in _rows(cur)]
    cur.execute("SELECT DISTINCT opportunity_type FROM seasonality_opportunity ORDER BY opportunity_type")
    avail_types = [r[0] for r in _rows(cur)]
    cur.execute("SELECT DISTINCT status FROM seasonality_opportunity ORDER BY status")
    avail_statuses = [r[0] for r in _rows(cur)]
    cur.execute("SELECT DISTINCT entity_type FROM seasonality_opportunity ORDER BY entity_type")
    avail_entity_types = [r[0] for r in _rows(cur)]

    offset = (page - 1) * page_size
    cur.execute(f"""
        SELECT o.id, o.marketplace, o.entity_type, o.entity_id, o.opportunity_type,
               o.title, o.description, o.priority_score, o.confidence_score,
               o.estimated_revenue_uplift, o.estimated_profit_uplift,
               o.recommended_start_date, o.status, o.source_signals_json, o.created_at,
               CASE WHEN o.entity_type = 'sku'
                    THEN (SELECT TOP 1 pr.title FROM acc_product pr WHERE pr.sku = o.entity_id)
                    ELSE NULL END AS product_title
        FROM seasonality_opportunity o
        WHERE {where}
        ORDER BY o.priority_score DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """, params + [offset, page_size])

    items = []
    for r in _rows(cur):
        items.append({
            "id": int(r[0]), "marketplace": r[1], "entity_type": r[2],
            "entity_id": r[3], "opportunity_type": r[4],
            "title": r[5], "description": r[6],
            "priority_score": r[7], "confidence_score": r[8],
            "estimated_revenue_uplift": r[9], "estimated_profit_uplift": r[10],
            "recommended_start_date": str(r[11]) if r[11] else None,
            "status": r[12],
            "source_signals": json.loads(r[13]) if r[13] else None,
            "created_at": str(r[14]) if r[14] else None,
            "product_title": r[15],
        })

    conn.close()
    return {
        "items": items, "total": total, "page": page, "page_size": page_size,
        "available_filters": {
            "marketplaces": avail_marketplaces,
            "opportunity_types": avail_types,
            "statuses": avail_statuses,
            "entity_types": avail_entity_types,
        },
    }


def change_opportunity_status(opp_id: int, new_status: str) -> dict:
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("""
        UPDATE seasonality_opportunity SET status = ?
        WHERE id = ?
    """, (new_status, opp_id))
    conn.commit()
    if cur.rowcount == 0:
        conn.close()
        return {"error": "not_found"}
    cur.execute("""
        SELECT id, marketplace, entity_type, entity_id, opportunity_type,
               title, status, priority_score
        FROM seasonality_opportunity WHERE id = ?
    """, (opp_id,))
    r = _rows(cur)[0]
    conn.close()
    return {"id": int(r[0]), "marketplace": r[1], "entity_type": r[2],
            "entity_id": r[3], "opportunity_type": r[4],
            "title": r[5], "status": r[6], "priority_score": r[7]}


# =====================================================================
# 7. CLUSTER HELPERS
# =====================================================================

def get_clusters() -> list:
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.id, c.cluster_name, c.description, c.rules_json,
               c.created_by, c.created_at,
               (SELECT COUNT(*) FROM seasonality_cluster_member m WHERE m.cluster_id = c.id) cnt
        FROM seasonality_cluster c
        ORDER BY c.cluster_name
    """)
    items = []
    for r in _rows(cur):
        items.append({
            "id": int(r[0]), "cluster_name": r[1], "description": r[2],
            "rules_json": json.loads(r[3]) if r[3] else None,
            "created_by": r[4], "created_at": str(r[5]) if r[5] else None,
            "members_count": int(r[6]),
        })
    conn.close()
    return items


def get_cluster_detail(cluster_id: int) -> dict | None:
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, cluster_name, description, rules_json, created_by, created_at
        FROM seasonality_cluster WHERE id = ?
    """, (cluster_id,))
    rows = _rows(cur)
    if not rows:
        conn.close()
        return None
    r = rows[0]
    cluster = {
        "id": int(r[0]), "cluster_name": r[1], "description": r[2],
        "rules_json": json.loads(r[3]) if r[3] else None,
        "created_by": r[4], "created_at": str(r[5]) if r[5] else None,
    }
    cur.execute("""
        SELECT sku, asin, product_type, category
        FROM seasonality_cluster_member WHERE cluster_id = ?
    """, (cluster_id,))
    cluster["members"] = [{"sku": r[0], "asin": r[1],
                           "product_type": r[2], "category": r[3]}
                          for r in _rows(cur)]
    cluster["members_count"] = len(cluster["members"])

    # Try to get seasonality profile for the cluster
    cur.execute("""
        SELECT seasonality_class, seasonality_confidence_score, peak_months_json
        FROM seasonality_profile
        WHERE entity_type = 'cluster' AND entity_id = ?
    """, (str(cluster_id),))
    prof = _rows(cur)
    if prof:
        cluster["seasonality_class"] = prof[0][0]
        cluster["confidence"] = prof[0][1]
        cluster["peak_months"] = json.loads(prof[0][2]) if prof[0][2] else []
    else:
        cluster["seasonality_class"] = None
        cluster["confidence"] = None
        cluster["peak_months"] = []

    conn.close()
    return cluster


def create_cluster(name: str, description: str | None,
                   rules_json: dict | None,
                   members: list[dict],
                   created_by: str | None = None) -> dict:
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO seasonality_cluster (cluster_name, description, rules_json, created_by)
        VALUES (?, ?, ?, ?)
    """, (name, description,
          json.dumps(rules_json) if rules_json else None,
          created_by))
    conn.commit()
    cur.execute("SELECT SCOPE_IDENTITY()")
    cluster_id = int(_rows(cur)[0][0])

    for m in members:
        cur.execute("""
            INSERT INTO seasonality_cluster_member (cluster_id, sku, asin, product_type, category)
            VALUES (?, ?, ?, ?, ?)
        """, (cluster_id, m.get("sku"), m.get("asin"),
              m.get("product_type"), m.get("category")))
    conn.commit()
    conn.close()
    return get_cluster_detail(cluster_id)


def update_cluster(cluster_id: int, name: str | None = None,
                   description: str | None = None,
                   rules_json: dict | None = None) -> dict | None:
    conn = connect_acc()
    cur = conn.cursor()
    sets = []
    params: list = []
    if name is not None:
        sets.append("cluster_name = ?")
        params.append(name)
    if description is not None:
        sets.append("description = ?")
        params.append(description)
    if rules_json is not None:
        sets.append("rules_json = ?")
        params.append(json.dumps(rules_json))
    if not sets:
        conn.close()
        return get_cluster_detail(cluster_id)

    params.append(cluster_id)
    cur.execute(f"UPDATE seasonality_cluster SET {', '.join(sets)} WHERE id = ?", params)
    conn.commit()
    conn.close()
    return get_cluster_detail(cluster_id)
