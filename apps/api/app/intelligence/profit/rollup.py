"""Profit Engine - rollup computation, queries, price simulator.

Extracted from profitability_service.py (Sprint 3 - S3.2).
Reads from pre-aggregated rollup tables; the enrichment and recompute
jobs are also housed here.
"""
from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc
from app.core.fee_taxonomy import get_profit_classification, rollup_bucket_map
from app.services.order_logistics_source import (
    profit_logistics_join_sql,
    profit_logistics_value_sql,
)

from app.intelligence.profit.helpers import (
    _f, _i, _mkt_code, _fetchall_dict,
)
from app.intelligence.profit.cost_model import _fx_case

log = structlog.get_logger(__name__)

def _build_enrichment_charge_lists() -> dict[str, list[str]]:
    """Build charge_type lists for rollup enrichment from fee_taxonomy.

    Returns dict with keys: 'storage', 'refund_cm2', 'other_cm2', 'overhead_np'.
    Each value is a list of charge_type strings whose taxonomy bucket maps
    to the corresponding rollup column.
    """
    bmap = rollup_bucket_map()
    cm2 = bmap.get("cm2", {})
    np_ = bmap.get("np", {})

    storage = cm2.get("fba_storage", []) + cm2.get("fba_aged", [])
    refund_cm2 = cm2.get("refund_cost", [])
    other_cm2 = (
        cm2.get("promo", [])
        + cm2.get("fba_removal", [])
        + cm2.get("fba_inbound", [])
        + cm2.get("fba_liquidation", [])
        + cm2.get("warehouse_loss", [])
        + cm2.get("amazon_other_fee", [])
    )
    overhead_np = []
    for bucket_list in np_.values():
        overhead_np.extend(bucket_list)
    return {
        "storage": storage,
        "refund_cm2": refund_cm2,
        "other_cm2": other_cm2,
        "overhead_np": overhead_np,
    }


def _sql_in_list(items: list[str]) -> str:
    """Build a SQL-safe IN list from charge_type names."""
    if not items:
        return "('')"
    return "(" + ", ".join(f"'{ct}'" for ct in items) + ")"


_METADATA_TABLE_VERIFIED = False


def _ensure_system_metadata_table(cur) -> None:
    """Create acc_system_metadata key-value table if it does not exist (idempotent).

    Uses a module-level flag to skip the DDL check after the first successful call,
    avoiding schema-lock overhead on the read path.
    """
    global _METADATA_TABLE_VERIFIED
    if _METADATA_TABLE_VERIFIED:
        return
    cur.execute("""
        IF OBJECT_ID('dbo.acc_system_metadata', 'U') IS NULL
        CREATE TABLE dbo.acc_system_metadata (
            meta_key   NVARCHAR(128) NOT NULL PRIMARY KEY,
            meta_value NVARCHAR(512) NULL,
            updated_at DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
        );
    """)
    _METADATA_TABLE_VERIFIED = True


def _upsert_system_metadata(cur, key: str, value: str) -> None:
    """Insert or update a single metadata key."""
    cur.execute("""
        MERGE dbo.acc_system_metadata WITH (HOLDLOCK) AS tgt
        USING (SELECT ? AS meta_key, ? AS meta_value) AS src
           ON tgt.meta_key = src.meta_key
        WHEN MATCHED THEN
            UPDATE SET meta_value = src.meta_value, updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (meta_key, meta_value, updated_at)
            VALUES (src.meta_key, src.meta_value, SYSUTCDATETIME());
    """, (key, value))


def ensure_rollup_layer_columns(cur) -> None:
    """Add cm1_pln / cm2_pln / overhead_pln columns to rollup tables if missing (idempotent)."""
    for table in ("acc_sku_profitability_rollup", "acc_marketplace_profitability_rollup"):
        for col in ("cm1_pln", "cm2_pln", "overhead_pln"):
            cur.execute(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID('dbo.{table}')
                      AND name = '{col}'
                )
                ALTER TABLE dbo.{table}
                    ADD {col} DECIMAL(14,2) NOT NULL DEFAULT 0;
            """)


_PROFIT_OVERVIEW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _profit_cache_get(key: str) -> dict[str, Any] | None:
    row = _PROFIT_OVERVIEW_CACHE.get(key)
    if not row:
        return None
    exp, value = row
    if time.monotonic() > exp:
        _PROFIT_OVERVIEW_CACHE.pop(key, None)
        return None
    return value


def _profit_cache_set(key: str, value: dict[str, Any], ttl_sec: int = 120) -> None:
    _PROFIT_OVERVIEW_CACHE[key] = (time.monotonic() + ttl_sec, value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_profitability_overview(
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict:
    cache_key = f"{date_from}:{date_to}:{marketplace_id or 'all'}"
    cached = _profit_cache_get(cache_key)
    if cached is not None:
        return cached
    conn = connect_acc(autocommit=False, timeout=20)
    try:
        cur = conn.cursor()

        # -- KPI aggregates --
        mkt_clause = "AND r.marketplace_id = ?" if marketplace_id else ""
        params: list = [date_from, date_to]
        if marketplace_id:
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT
                ISNULL(SUM(r.revenue_pln), 0),
                ISNULL(SUM(r.profit_pln), 0),
                ISNULL(SUM(r.orders_count), 0),
                ISNULL(SUM(r.units_sold), 0),
                ISNULL(SUM(r.ad_spend_pln), 0),
                ISNULL(SUM(r.refund_pln), 0),
                ISNULL(SUM(r.refund_units), 0),
                ISNULL(SUM(r.cm1_pln), 0),
                ISNULL(SUM(r.cm2_pln), 0)
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
            {mkt_clause}
        """, tuple(params))
        row = cur.fetchone()
        revenue = _f(row[0])
        profit = _f(row[1])
        orders = _i(row[2])
        units = _i(row[3])
        ad_spend = _f(row[4])
        refund = _f(row[5])
        refund_units = _i(row[6])
        cm1 = _f(row[7])
        cm2 = _f(row[8])

        kpi = {
            "total_revenue_pln": round(revenue, 2),
            "total_cm1_pln": round(cm1, 2),
            "total_cm2_pln": round(cm2, 2),
            "total_profit_pln": round(profit, 2),
            "profit_tier": "cm1_cm2_np",
            "total_margin_pct": round(profit / revenue * 100, 2) if revenue else 0,
            "cm1_margin_pct": round(cm1 / revenue * 100, 2) if revenue else 0,
            "total_orders": orders,
            "total_units": units,
            "total_ad_spend_pln": round(ad_spend, 2),
            "ad_spend_share_pct": round(ad_spend / revenue * 100, 2) if revenue else 0,
            "tacos_pct": round(ad_spend / revenue * 100, 2) if revenue else 0,
            "total_refund_pln": round(refund, 2),
            "return_rate_pct": round(refund_units / units * 100, 2) if units else 0,
        }

        # -- Best SKUs (top 20 by profit) --
        cur.execute(f"""
            SELECT TOP 20
                r.sku,
                MAX(r.asin) as asin,
                r.marketplace_id,
                SUM(r.revenue_pln) as revenue_pln,
                SUM(r.profit_pln) as profit_pln,
                CASE WHEN SUM(r.revenue_pln) <> 0
                     THEN SUM(r.profit_pln) / SUM(r.revenue_pln) * 100
                     ELSE 0 END as margin_pct,
                SUM(r.units_sold) as units,
                CASE WHEN SUM(r.revenue_pln) <> 0
                     THEN SUM(r.ad_spend_pln) / SUM(r.revenue_pln) * 100
                     ELSE NULL END as acos_pct,
                CASE WHEN SUM(r.units_sold) <> 0
                     THEN SUM(r.refund_units) * 100.0 / SUM(r.units_sold)
                     ELSE NULL END as return_rate_pct,
                MAX(cp.internal_sku) as internal_sku,
                MAX(cp.brand) as brand,
                MAX(cp.product_name) as product_name
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_marketplace_presence mp WITH (NOLOCK)
                ON mp.seller_sku = r.sku AND mp.marketplace_id = r.marketplace_id
            LEFT JOIN dbo.acc_canonical_product cp WITH (NOLOCK)
                ON cp.internal_sku = mp.internal_sku
            WHERE r.period_date >= ? AND r.period_date <= ?
            {mkt_clause}
            GROUP BY r.sku, r.marketplace_id
            HAVING SUM(r.revenue_pln) > 0
            ORDER BY SUM(r.profit_pln) DESC
        """, tuple(params))
        best_skus = [
            {
                "sku": r[0], "asin": r[1], "marketplace_id": r[2],
                "marketplace_code": _mkt_code(r[2]),
                "internal_sku": r[9] or None,
                "brand": r[10] or None,
                "product_name": r[11] or None,
                "revenue_pln": round(_f(r[3]), 2), "profit_pln": round(_f(r[4]), 2),
                "margin_pct": round(_f(r[5]), 2), "units": _i(r[6]),
                "acos_pct": round(_f(r[7]), 2) if r[7] is not None else None,
                "return_rate_pct": round(_f(r[8]), 2) if r[8] is not None else None,
            }
            for r in cur.fetchall()
        ]

        # -- Worst SKUs (bottom 20 by profit) --
        cur.execute(f"""
            SELECT TOP 20
                r.sku,
                MAX(r.asin) as asin,
                r.marketplace_id,
                SUM(r.revenue_pln) as revenue_pln,
                SUM(r.profit_pln) as profit_pln,
                CASE WHEN SUM(r.revenue_pln) <> 0
                     THEN SUM(r.profit_pln) / SUM(r.revenue_pln) * 100
                     ELSE 0 END as margin_pct,
                SUM(r.units_sold) as units,
                CASE WHEN SUM(r.revenue_pln) <> 0
                     THEN SUM(r.ad_spend_pln) / SUM(r.revenue_pln) * 100
                     ELSE NULL END as acos_pct,
                CASE WHEN SUM(r.units_sold) <> 0
                     THEN SUM(r.refund_units) * 100.0 / SUM(r.units_sold)
                     ELSE NULL END as return_rate_pct,
                MAX(cp.internal_sku) as internal_sku,
                MAX(cp.brand) as brand,
                MAX(cp.product_name) as product_name
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_marketplace_presence mp WITH (NOLOCK)
                ON mp.seller_sku = r.sku AND mp.marketplace_id = r.marketplace_id
            LEFT JOIN dbo.acc_canonical_product cp WITH (NOLOCK)
                ON cp.internal_sku = mp.internal_sku
            WHERE r.period_date >= ? AND r.period_date <= ?
            {mkt_clause}
            GROUP BY r.sku, r.marketplace_id
            ORDER BY SUM(r.profit_pln) ASC
        """, tuple(params))
        worst_skus = [
            {
                "sku": r[0], "asin": r[1], "marketplace_id": r[2],
                "marketplace_code": _mkt_code(r[2]),
                "internal_sku": r[9] or None,
                "brand": r[10] or None,
                "product_name": r[11] or None,
                "revenue_pln": round(_f(r[3]), 2), "profit_pln": round(_f(r[4]), 2),
                "margin_pct": round(_f(r[5]), 2), "units": _i(r[6]),
                "acos_pct": round(_f(r[7]), 2) if r[7] is not None else None,
                "return_rate_pct": round(_f(r[8]), 2) if r[8] is not None else None,
            }
            for r in cur.fetchall()
        ]

        # -- Loss orders (most recent 50 with negative CM) --
        # F2: deterministic primary SKU (by highest line revenue)
        # F3: inline CM formula instead of stored V1 contribution_margin_pln
        loss_params: list = [date_from, date_to]
        loss_mkt = "AND o.marketplace_id = ?" if marketplace_id else ""
        if marketplace_id:
            loss_params.append(marketplace_id)

        order_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        order_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")
        _loss_cm = (
            "ISNULL(o.revenue_pln,0) - ISNULL(o.amazon_fees_pln,0)"
            " - ISNULL(o.cogs_pln,0) - "
            f"{order_logistics_value_sql}"
        )
        cur.execute(f"""
            SELECT TOP 50
                o.amazon_order_id,
                o.marketplace_id,
                o.purchase_date,
                (SELECT TOP 1 ol.sku
                 FROM dbo.acc_order_line ol WITH (NOLOCK)
                 WHERE ol.order_id = o.id
                 ORDER BY ISNULL(ol.item_price, 0) DESC, ol.sku) as sku,
                ISNULL(o.revenue_pln, 0) as revenue_pln,
                ({_loss_cm}) as profit_pln,
                CASE WHEN ISNULL(o.revenue_pln,0) > 0
                     THEN ({_loss_cm}) * 100.0 / o.revenue_pln
                     ELSE 0 END as margin_pct
            FROM dbo.acc_order o WITH (NOLOCK)
            {order_logistics_join_sql}
            WHERE o.purchase_date >= ? AND o.purchase_date <= DATEADD(day, 1, CAST(? AS DATE))
              AND ({_loss_cm}) < 0
              AND o.status IN ('Shipped', 'Unshipped')
              {loss_mkt}
            ORDER BY ({_loss_cm}) ASC
        """, tuple(loss_params))
        loss_orders = [
            {
                "amazon_order_id": r[0],
                "marketplace_id": r[1],
                "marketplace_code": _mkt_code(r[1]),
                "purchase_date": r[2],
                "sku": r[3],
                "revenue_pln": round(_f(r[4]), 2),
                "profit_pln": round(_f(r[5]), 2),
                "margin_pct": round(_f(r[6]), 2) if r[6] is not None else None,
            }
            for r in cur.fetchall()
        ]

        # -- Data freshness: prefer acc_system_metadata, fallback to MAX(computed_at) --
        rollup_ts = None
        rollup_range = None
        try:
            _ensure_system_metadata_table(cur)
            cur.execute("""
                SELECT meta_key, meta_value FROM dbo.acc_system_metadata WITH (NOLOCK)
                WHERE meta_key IN ('rollup_recomputed_at', 'rollup_date_from', 'rollup_date_to')
            """)
            meta = {r[0]: r[1] for r in cur.fetchall()}
            if meta.get('rollup_recomputed_at'):
                rollup_ts = datetime.fromisoformat(meta['rollup_recomputed_at'])
            rollup_range = {
                "date_from": meta.get('rollup_date_from'),
                "date_to": meta.get('rollup_date_to'),
            }
        except Exception:
            pass
        if rollup_ts is None:
            try:
                cur.execute("""
                    SELECT MAX(computed_at)
                    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
                    WHERE period_date >= ? AND period_date <= ?
                """, (date_from, date_to))
                ts_row = cur.fetchone()
                if ts_row and ts_row[0]:
                    rollup_ts = ts_row[0]
            except Exception:
                pass  # freshness query is itself non-critical

        import time as _time
        cache_age = None
        cached_entry = _PROFIT_OVERVIEW_CACHE.get(cache_key)
        if cached_entry:
            expiry, _ = cached_entry
            cache_age = round(120.0 - (expiry - _time.monotonic()), 1)
            if cache_age < 0:
                cache_age = 0.0

        result = {
            "kpi": kpi,
            "best_skus": best_skus,
            "worst_skus": worst_skus,
            "loss_orders": loss_orders,
            "warnings": [],
            "data_freshness": {
                "rollup_recomputed_at": rollup_ts.isoformat() if rollup_ts else None,
                "cache_age_seconds": cache_age,
                "rollup_covers": rollup_range,
                "data_source": "mixed",  # KPIs from rollup, loss_orders from live acc_order
            },
        }
        _profit_cache_set(cache_key, result, ttl_sec=120)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2) Orders query (from acc_order with on-the-fly CM)
# ---------------------------------------------------------------------------

def get_profitability_orders(
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
    loss_only: bool = False,
    min_margin: float | None = None,
    max_margin: float | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    conn = connect_acc(autocommit=False, timeout=20)
    try:
        cur = conn.cursor()
        order_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        order_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

        wheres = [
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            "o.status IN ('Shipped', 'Unshipped')",
        ]
        params: list = [date_from, date_to]

        if marketplace_id:
            wheres.append("o.marketplace_id = ?")
            params.append(marketplace_id)
        if sku:
            wheres.append("EXISTS (SELECT 1 FROM dbo.acc_order_line sk WITH (NOLOCK) WHERE sk.order_id = o.id AND sk.sku = ?)")
            params.append(sku)
        # F3: use inline CM for loss/margin filters (not V1 stored contribution_margin_pln)
        _cm_expr = (
            "(ISNULL(o.revenue_pln,0) - ISNULL(o.amazon_fees_pln,0)"
            " - ISNULL(o.cogs_pln,0) - "
            f"{order_logistics_value_sql})"
        )
        _cm_pct_expr = (
            f"CASE WHEN ISNULL(o.revenue_pln,0) > 0"
            f" THEN {_cm_expr} * 100.0 / o.revenue_pln ELSE 0 END"
        )
        if loss_only:
            wheres.append(f"{_cm_expr} < 0")
        if min_margin is not None:
            wheres.append(f"{_cm_pct_expr} >= ?")
            params.append(min_margin)
        if max_margin is not None:
            wheres.append(f"{_cm_pct_expr} <= ?")
            params.append(max_margin)

        where_sql = " AND ".join(wheres)

        # Count
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM dbo.acc_order o WITH (NOLOCK)
            {order_logistics_join_sql}
            WHERE {where_sql}
            """,
            tuple(params),
        )
        total = cur.fetchone()[0]
        pages = math.ceil(total / page_size) if page_size else 1
        offset = (max(1, page) - 1) * page_size

        # Data - fees from order/line/rollup COALESCE; ad_cost/refund from rollup
        # F2 fix: deterministic primary_sku by highest line revenue, expose sku_count + all_skus
        # F3 fix: CM computed inline from displayed components (not V1 stored CM)
        # F4 fix: COALESCE fees/logistics from enriched order → line-level → rollup estimation
        cur.execute(f"""
            SELECT
                o.amazon_order_id,
                o.marketplace_id,
                o.purchase_date,
                ol_agg.primary_sku,
                ol_agg.primary_asin,
                ol_agg.sku_count,
                ISNULL(o.revenue_pln, 0),
                COALESCE(
                    o.amazon_fees_pln,
                    NULLIF(ol_agg.referral_fees_pln + ol_agg.fba_fees_pln, 0),
                    ROUND(ISNULL(o.revenue_pln, 0) * r_agg.fee_ratio, 2),
                    0
                ),
                ISNULL(ol_agg.fba_fees_pln, 0),
                COALESCE(
                    NULLIF(CAST({order_logistics_value_sql} AS FLOAT), 0),
                    ROUND(ISNULL(o.revenue_pln, 0) * r_agg.log_ratio, 2),
                    0
                ),
                ISNULL(o.cogs_pln, 0),
                ISNULL(r_agg.ad_cost_pln, 0),
                ISNULL(r_agg.refund_pln, 0),
                ol_agg.all_skus
            FROM dbo.acc_order o WITH (NOLOCK)
            {order_logistics_join_sql}
            CROSS APPLY (
                SELECT
                    MAX(CASE WHEN _rn = 1 THEN sku END)  AS primary_sku,
                    MAX(CASE WHEN _rn = 1 THEN asin END) AS primary_asin,
                    COUNT(DISTINCT sku)                   AS sku_count,
                    SUM(ISNULL(fba_fee_pln, 0))           AS fba_fees_pln,
                    SUM(ISNULL(referral_fee_pln, 0))      AS referral_fees_pln,
                    STRING_AGG(sku, ', ')                  AS all_skus
                FROM (
                    SELECT ol.sku, ol.asin, ol.fba_fee_pln, ol.referral_fee_pln,
                           ROW_NUMBER() OVER (
                               PARTITION BY ol.order_id
                               ORDER BY ISNULL(ol.item_price, 0) DESC, ol.sku
                           ) AS _rn
                    FROM dbo.acc_order_line ol WITH (NOLOCK)
                    WHERE ol.order_id = o.id
                ) ranked
            ) ol_agg
            OUTER APPLY (
                SELECT
                    ROUND(ISNULL(o.revenue_pln, 0) *
                        CASE WHEN SUM(ISNULL(r.revenue_pln, 0)) > 0
                             THEN SUM(ISNULL(r.ad_spend_pln, 0)) / SUM(r.revenue_pln)
                             ELSE 0 END
                    , 2) AS ad_cost_pln,
                    ROUND(ISNULL(o.revenue_pln, 0) *
                        CASE WHEN SUM(ISNULL(r.revenue_pln, 0)) > 0
                             THEN SUM(ISNULL(r.refund_pln, 0)) / SUM(r.revenue_pln)
                             ELSE 0 END
                    , 2) AS refund_pln,
                    CASE WHEN SUM(ISNULL(r.revenue_pln, 0)) > 0
                         THEN SUM(ISNULL(r.amazon_fees_pln, 0)) / SUM(r.revenue_pln)
                         ELSE NULL END AS fee_ratio,
                    CASE WHEN SUM(ISNULL(r.revenue_pln, 0)) > 0
                         THEN SUM(ISNULL(r.logistics_pln, 0)) / SUM(r.revenue_pln)
                         ELSE NULL END AS log_ratio
                FROM (
                    SELECT DISTINCT ol2.sku
                    FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                    WHERE ol2.order_id = o.id
                ) skus
                LEFT JOIN dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                    ON r.sku = skus.sku
                    AND r.marketplace_id = o.marketplace_id
                    AND r.period_date = CAST(o.purchase_date AS DATE)
            ) r_agg
            WHERE {where_sql}
            ORDER BY o.purchase_date DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, (*params, offset, page_size))

        rows = cur.fetchall()
        items = []
        for r in rows:
            rev = round(_f(r[6]), 2)
            fees = round(_f(r[7]), 2)
            fba = round(_f(r[8]), 2)
            logistics = round(_f(r[9]), 2)
            cogs = round(_f(r[10]), 2)
            ad = round(_f(r[11]), 2)
            refund = round(_f(r[12]), 2)
            cm = round(rev - fees - fba - logistics - cogs - ad - refund, 2)
            cm_pct = round(cm * 100.0 / rev, 2) if rev > 0 else 0.0
            sku_count = r[5] or 1
            all_skus_raw = r[13] or r[3] or ""
            items.append({
                "amazon_order_id": r[0],
                "marketplace_id": r[1],
                "marketplace_code": _mkt_code(r[1]),
                "purchase_date": r[2],
                "sku": r[3],
                "asin": r[4],
                "sku_count": sku_count,
                "all_skus": all_skus_raw if sku_count > 1 else None,
                "revenue_pln": rev,
                "amazon_fees_pln": fees,
                "fba_fees_pln": fba,
                "logistics_pln": logistics,
                "cogs_pln": cogs,
                "ad_cost_pln": ad,
                "refund_pln": refund,
                "profit_pln": cm,
                "margin_pct": cm_pct,
            })

        return {"total": total, "page": page, "page_size": page_size, "pages": pages, "items": items}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2b) Order-line detail for a single order
# ---------------------------------------------------------------------------

def get_order_lines(amazon_order_id: str) -> dict:
    """Return all order lines for a given amazon_order_id with cost breakdown."""
    conn = connect_acc(autocommit=False, timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                o.amazon_order_id,
                o.marketplace_id,
                o.purchase_date,
                o.revenue_pln   AS order_revenue_pln,
                o.amazon_fees_pln AS order_fees_pln,
                o.cogs_pln      AS order_cogs_pln,
                o.status,
                ol.sku,
                ol.asin,
                ol.title,
                ISNULL(ol.quantity_ordered, 0),
                ISNULL(ol.item_price, 0),
                ISNULL(ol.item_tax, 0),
                ISNULL(ol.promotion_discount, 0),
                ol.currency,
                ISNULL(ol.referral_fee_pln, 0),
                ISNULL(ol.fba_fee_pln, 0),
                ISNULL(ol.cogs_pln, 0),
                ISNULL(ol.purchase_price_pln, 0),
                ol.price_source
            FROM dbo.acc_order o WITH (NOLOCK)
            INNER JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
            WHERE o.amazon_order_id = ?
            ORDER BY ISNULL(ol.item_price, 0) DESC, ol.sku
        """, (amazon_order_id,))
        rows = cur.fetchall()
        if not rows:
            return {"amazon_order_id": amazon_order_id, "lines": [], "order": None}

        first = rows[0]
        order_info = {
            "amazon_order_id": first[0],
            "marketplace_id": first[1],
            "marketplace_code": _mkt_code(first[1]),
            "purchase_date": first[2],
            "revenue_pln": round(_f(first[3]), 2),
            "amazon_fees_pln": round(_f(first[4]), 2) if first[4] is not None else None,
            "cogs_pln": round(_f(first[5]), 2),
            "status": first[6],
        }

        lines = []
        for r in rows:
            ref_fee = round(_f(r[15]), 2)
            fba_fee = round(_f(r[16]), 2)
            cogs = round(_f(r[17]), 2)
            price = round(_f(r[11]), 2)
            lines.append({
                "sku": r[7],
                "asin": r[8],
                "title": r[9],
                "quantity": r[10],
                "item_price": price,
                "item_tax": round(_f(r[12]), 2),
                "promo_discount": round(_f(r[13]), 2),
                "currency": r[14],
                "referral_fee_pln": ref_fee,
                "fba_fee_pln": fba_fee,
                "cogs_pln": cogs,
                "purchase_price_pln": round(_f(r[18]), 2),
                "price_source": r[19],
                "line_profit_pln": round(price - ref_fee - fba_fee - cogs, 2),
            })
        return {"amazon_order_id": amazon_order_id, "order": order_info, "lines": lines}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 3) Products query (from rollup)
# ---------------------------------------------------------------------------

def get_profitability_products(
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
    sort_by: str = "profit_pln",
    sort_dir: str = "desc",
    page: int = 1,
    page_size: int = 50,
) -> dict:
    conn = connect_acc(autocommit=False, timeout=20)
    try:
        cur = conn.cursor()

        wheres = ["r.period_date >= ?", "r.period_date <= ?"]
        params: list = [date_from, date_to]
        if marketplace_id:
            wheres.append("r.marketplace_id = ?")
            params.append(marketplace_id)
        if sku:
            wheres.append("r.sku LIKE ?")
            params.append(f"%{sku}%")

        where_sql = " AND ".join(wheres)

        allowed_sorts = {
            "sku", "revenue_pln", "profit_pln", "cm1_pln", "cm2_pln",
            "margin_pct", "units", "acos_pct", "return_rate_pct", "ad_spend_pln",
        }
        sort_col = sort_by if sort_by in allowed_sorts else "profit_pln"
        direction = "ASC" if sort_dir.lower() == "asc" else "DESC"

        # Count distinct SKU+marketplace
        cur.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT r.sku, r.marketplace_id
                FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                WHERE {where_sql}
                GROUP BY r.sku, r.marketplace_id
            ) sub
        """, tuple(params))
        total = cur.fetchone()[0]
        pages = math.ceil(total / page_size) if page_size else 1
        offset = (max(1, page) - 1) * page_size

        cur.execute(f"""
            SELECT
                r.sku,
                MAX(r.asin) as asin,
                r.marketplace_id,
                SUM(r.units_sold) as units,
                SUM(r.orders_count) as orders,
                SUM(r.revenue_pln) as revenue_pln,
                SUM(r.cogs_pln) as cogs_pln,
                SUM(r.amazon_fees_pln) as amazon_fees_pln,
                SUM(r.logistics_pln) as logistics_pln,
                SUM(r.ad_spend_pln) as ad_spend_pln,
                SUM(r.refund_pln) as refund_pln,
                SUM(r.profit_pln) as profit_pln,
                CASE WHEN SUM(r.revenue_pln) <> 0
                     THEN SUM(r.profit_pln) / SUM(r.revenue_pln) * 100
                     ELSE 0 END as margin_pct,
                CASE WHEN SUM(r.revenue_pln) <> 0
                     THEN SUM(r.ad_spend_pln) / SUM(r.revenue_pln) * 100
                     ELSE NULL END as acos_pct,
                CASE WHEN SUM(r.units_sold) <> 0
                     THEN SUM(r.refund_units) * 100.0 / SUM(r.units_sold)
                     ELSE NULL END as return_rate_pct,
                SUM(r.cm1_pln) as cm1_pln,
                SUM(r.cm2_pln) as cm2_pln,
                MAX(cp.internal_sku) as internal_sku,
                MAX(cp.brand) as brand,
                MAX(cp.category) as category,
                MAX(cp.product_name) as product_name
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_marketplace_presence mp WITH (NOLOCK)
                ON mp.seller_sku = r.sku AND mp.marketplace_id = r.marketplace_id
            LEFT JOIN dbo.acc_canonical_product cp WITH (NOLOCK)
                ON cp.internal_sku = mp.internal_sku
            WHERE {where_sql}
            GROUP BY r.sku, r.marketplace_id
            ORDER BY {sort_col} {direction}
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, (*params, offset, page_size))

        items = []
        for r in cur.fetchall():
            units = _i(r[3])
            revenue = round(_f(r[5]), 2)
            cogs = round(_f(r[6]), 2)
            fees = round(_f(r[7]), 2)
            logistics = round(_f(r[8]), 2)
            ad_spend = round(_f(r[9]), 2)
            refund = round(_f(r[10]), 2)
            profit = round(_f(r[11]), 2)
            cm1 = round(_f(r[15]), 2)
            cm2 = round(_f(r[16]), 2)
            u = max(units, 1)
            items.append({
                "sku": r[0], "asin": r[1], "marketplace_id": r[2],
                "marketplace_code": _mkt_code(r[2]),
                "internal_sku": r[17] or None,
                "brand": r[18] or None,
                "category": r[19] or None,
                "product_name": r[20] or None,
                "fulfillment_channel": "FBA",
                "units": units, "order_count": _i(r[4]),
                "revenue_pln": revenue,
                "cogs_pln": cogs,
                "amazon_fees_pln": fees,
                "fba_fee_pln": 0.0,
                "referral_fee_pln": 0.0,
                "logistics_pln": logistics,
                "ad_spend_pln": ad_spend,
                "ads_cost_pln": ad_spend,
                "refund_pln": refund,
                "returns_net_pln": refund,
                "profit_pln": profit,
                "margin_pct": round(_f(r[12]), 2),
                "acos_pct": round(_f(r[13]), 2) if r[13] is not None else None,
                "return_rate_pct": round(_f(r[14]), 2) if r[14] is not None else None,
                "cm1_pln": cm1,
                "cm2_pln": cm2,
                # Frontend-compatible aliases
                "cm1_profit": cm1,
                "cm1_percent": round(cm1 / revenue * 100, 2) if revenue else 0.0,
                "cm2_profit": cm2,
                "cm2_percent": round(cm2 / revenue * 100, 2) if revenue else 0.0,
                "np_profit": profit,
                "np_percent": round(profit / revenue * 100, 2) if revenue else 0.0,
                "cogs_per_unit": round(cogs / u, 2),
                "fees_per_unit": round(fees / u, 2),
                "revenue_per_unit": round(revenue / u, 2),
                "cogs_coverage_pct": 100.0,
                "fees_coverage_pct": 100.0,
                "confidence_score": 100.0,
                "loss_orders_pct": 0.0,
                "flags": [],
            })

        # Build summary for frontend
        tot_rev = sum(i["revenue_pln"] for i in items)
        tot_cogs = sum(i["cogs_pln"] for i in items)
        tot_fees = sum(i["amazon_fees_pln"] for i in items)
        tot_cm1 = sum(i["cm1_pln"] for i in items)
        tot_cm2 = sum(i["cm2_pln"] for i in items)
        tot_ads = sum(i["ad_spend_pln"] for i in items)
        tot_logi = sum(i["logistics_pln"] for i in items)
        tot_refund = sum(i["refund_pln"] for i in items)
        tot_units = sum(i["units"] for i in items)
        summary = {
            "total_revenue_pln": round(tot_rev, 2),
            "total_cogs_pln": round(tot_cogs, 2),
            "total_fees_pln": round(tot_fees, 2),
            "total_cm1_pln": round(tot_cm1, 2),
            "total_cm1_pct": round(tot_cm1 / tot_rev * 100, 2) if tot_rev else 0.0,
            "total_ads_cost_pln": round(tot_ads, 2),
            "total_logistics_pln": round(tot_logi, 2),
            "total_cm2_pln": round(tot_cm2, 2),
            "total_cm2_pct": round(tot_cm2 / tot_rev * 100, 2) if tot_rev else 0.0,
            "total_returns_net_pln": round(tot_refund, 2),
            "total_units": tot_units,
            "avg_confidence": 100.0,
            "total_offers": len(items),
            "summary_scope": "page",
        }

        return {"total": total, "page": page, "page_size": page_size, "pages": pages, "summary": summary, "items": items}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 4) Marketplace rollup query
# ---------------------------------------------------------------------------

def get_marketplace_profitability(
    date_from: date,
    date_to: date,
) -> list[dict]:
    conn = connect_acc(autocommit=False, timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                m.marketplace_id,
                SUM(m.total_orders),
                SUM(m.total_units),
                SUM(m.unique_skus),
                SUM(m.revenue_pln),
                SUM(m.profit_pln),
                CASE WHEN SUM(m.revenue_pln) <> 0
                     THEN SUM(m.profit_pln) / SUM(m.revenue_pln) * 100
                     ELSE 0 END,
                SUM(m.ad_spend_pln),
                CASE WHEN SUM(m.revenue_pln) <> 0
                     THEN SUM(m.ad_spend_pln) / SUM(m.revenue_pln) * 100
                     ELSE NULL END,
                CASE WHEN SUM(m.total_units) <> 0
                     THEN SUM(m.refund_units) * 100.0 / SUM(m.total_units)
                     ELSE NULL END,
                SUM(m.cm1_pln),
                SUM(m.cm2_pln)
            FROM dbo.acc_marketplace_profitability_rollup m WITH (NOLOCK)
            WHERE m.period_date >= ? AND m.period_date <= ?
            GROUP BY m.marketplace_id
            ORDER BY SUM(m.revenue_pln) DESC
        """, (date_from, date_to))

        return [
            {
                "marketplace_id": r[0],
                "marketplace_code": _mkt_code(r[0]),
                "total_orders": _i(r[1]),
                "total_units": _i(r[2]),
                "unique_skus": _i(r[3]),
                "revenue_pln": round(_f(r[4]), 2),
                "profit_pln": round(_f(r[5]), 2),
                "margin_pct": round(_f(r[6]), 2),
                "ad_spend_pln": round(_f(r[7]), 2),
                "acos_pct": round(_f(r[8]), 2) if r[8] is not None else None,
                "return_rate_pct": round(_f(r[9]), 2) if r[9] is not None else None,
                "cm1_pln": round(_f(r[10]), 2),
                "cm2_pln": round(_f(r[11]), 2),
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 5) Price simulator (pure math, no DB)
# ---------------------------------------------------------------------------


def simulate_price(
    sale_price: float,
    purchase_cost: float,
    shipping_cost: float = 0,
    amazon_fee_pct: float = 15.0,
    fba_fee: float = 0,
    ad_cost: float = 0,
    currency: str = "EUR",
    fx_rate: float | None = None,
) -> dict:
    from app.core.fx_service import get_rate_safe
    fx = fx_rate or get_rate_safe(currency)
    amazon_fee = sale_price * (amazon_fee_pct / 100)
    total_cost = purchase_cost + shipping_cost + amazon_fee + fba_fee + ad_cost
    profit = sale_price - total_cost
    margin = (profit / sale_price * 100) if sale_price else 0
    # Breakeven = costs_without_price / (1 - amazon_fee_pct/100)
    fixed_costs = purchase_cost + shipping_cost + fba_fee + ad_cost
    fee_factor = 1 - (amazon_fee_pct / 100)
    breakeven = (fixed_costs / fee_factor) if fee_factor > 0 else 0

    return {
        "sale_price": round(sale_price, 2),
        "purchase_cost": round(purchase_cost, 2),
        "shipping_cost": round(shipping_cost, 2),
        "amazon_fee": round(amazon_fee, 2),
        "fba_fee": round(fba_fee, 2),
        "ad_cost": round(ad_cost, 2),
        "total_cost": round(total_cost, 2),
        "profit": round(profit, 2),
        "margin_pct": round(margin, 2),
        "breakeven_price": round(breakeven, 2),
        "currency": currency,
        "fx_rate": fx,
    }


# ---------------------------------------------------------------------------
# 6a) Enrich rollup from finance transactions
# ---------------------------------------------------------------------------

# Charge types that map to each rollup column - derived from fee_taxonomy
_ENRICHMENT_CHARGES = _build_enrichment_charge_lists()
_STORAGE_CHARGES_SQL = _sql_in_list(_ENRICHMENT_CHARGES["storage"])
_REFUND_CM2_CHARGES_SQL = _sql_in_list(_ENRICHMENT_CHARGES["refund_cm2"])
_OTHER_CM2_CHARGES_SQL = _sql_in_list(_ENRICHMENT_CHARGES["other_cm2"])
_OVERHEAD_NP_CHARGES_SQL = _sql_in_list(_ENRICHMENT_CHARGES["overhead_np"])


def _enrich_rollup_from_finance(cur, conn, date_from: date, date_to: date) -> dict:
    """
    Post-MERGE enrichment: update ad_spend_pln, refund_units, storage_fee_pln,
    refund_pln, other_fees_pln from source tables into acc_sku_profitability_rollup.

    Storage fees have no SKU - allocated proportionally by revenue per marketplace per month.
    Refund/other charges with SKU are assigned directly; those without are allocated by revenue.

    After updating these columns, recalculates profit_pln and margin_pct.
    Finally refreshes the marketplace rollup to reflect changes.
    """
    stats = {"storage_rows": 0, "refund_rows": 0, "other_rows": 0, "ads_rows": 0, "ads_catchall_rows": 0, "return_units_rows": 0}

    # -- 1. Storage fees: no SKU, allocate by revenue share per marketplace+month --
    cur.execute(f"""
        ;WITH storage AS (
            SELECT
                ft.marketplace_id,
                DATEFROMPARTS(YEAR(CAST(ft.posted_date AS DATE)), MONTH(CAST(ft.posted_date AS DATE)), 1) AS fee_month,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS total_storage_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN {_STORAGE_CHARGES_SQL}
              AND CAST(ft.posted_date AS DATE) >= ?
              AND CAST(ft.posted_date AS DATE) <= ?
            GROUP BY ft.marketplace_id,
                     DATEFROMPARTS(YEAR(CAST(ft.posted_date AS DATE)), MONTH(CAST(ft.posted_date AS DATE)), 1)
        ),
        rev_totals AS (
            SELECT
                r.marketplace_id,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS fee_month,
                SUM(CASE WHEN r.revenue_pln > 0 THEN r.revenue_pln ELSE 0 END) AS mkt_revenue
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
            GROUP BY r.marketplace_id,
                     DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
        )
        UPDATE r SET
            r.storage_fee_pln = CASE
                WHEN rt.mkt_revenue > 0
                THEN ROUND(s.total_storage_pln * (CASE WHEN r.revenue_pln > 0 THEN r.revenue_pln ELSE 0 END) / rt.mkt_revenue, 2)
                ELSE 0 END
        FROM dbo.acc_sku_profitability_rollup r
        JOIN storage s
          ON s.marketplace_id = r.marketplace_id
         AND s.fee_month = DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
        JOIN rev_totals rt
          ON rt.marketplace_id = r.marketplace_id
         AND rt.fee_month = s.fee_month
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to, date_from, date_to))
    stats["storage_rows"] = cur.rowcount

    # -- 2. Refund charges with SKU: assign directly --
    cur.execute(f"""
        ;WITH refunds AS (
            SELECT
                ft.marketplace_id,
                ft.sku,
                CAST(ft.posted_date AS DATE) AS ref_date,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS refund_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN {_REFUND_CM2_CHARGES_SQL}
              AND ft.sku IS NOT NULL AND ft.sku != ''
              AND CAST(ft.posted_date AS DATE) >= ?
              AND CAST(ft.posted_date AS DATE) <= ?
            GROUP BY ft.marketplace_id, ft.sku, CAST(ft.posted_date AS DATE)
        )
        UPDATE r SET
            r.refund_pln = ref.refund_pln
        FROM dbo.acc_sku_profitability_rollup r
        JOIN refunds ref
          ON ref.marketplace_id = r.marketplace_id
         AND ref.sku = r.sku
         AND ref.ref_date = r.period_date
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to))
    stats["refund_rows"] = cur.rowcount

    # -- 3. Refund charges WITHOUT SKU: allocate by revenue per marketplace+date --
    cur.execute(f"""
        ;WITH nosku_refunds AS (
            SELECT
                COALESCE(ft.marketplace_id, o.marketplace_id) AS marketplace_id,
                CAST(ft.posted_date AS DATE) AS ref_date,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS refund_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            LEFT JOIN dbo.acc_order o WITH (NOLOCK)
              ON o.amazon_order_id = ft.amazon_order_id
            WHERE ft.charge_type IN {_REFUND_CM2_CHARGES_SQL}
              AND (ft.sku IS NULL OR ft.sku = '')
              AND CAST(ft.posted_date AS DATE) >= ?
              AND CAST(ft.posted_date AS DATE) <= ?
            GROUP BY COALESCE(ft.marketplace_id, o.marketplace_id),
                     CAST(ft.posted_date AS DATE)
        ),
        daily_rev AS (
            SELECT marketplace_id, period_date,
                   SUM(CASE WHEN revenue_pln > 0 THEN revenue_pln ELSE 0 END) AS day_revenue
            FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
            WHERE period_date >= ? AND period_date <= ?
            GROUP BY marketplace_id, period_date
        )
        UPDATE r SET
            r.refund_pln = r.refund_pln + CASE
                WHEN dr.day_revenue > 0
                THEN ROUND(nr.refund_pln * (CASE WHEN r.revenue_pln > 0 THEN r.revenue_pln ELSE 0 END) / dr.day_revenue, 2)
                ELSE 0 END
        FROM dbo.acc_sku_profitability_rollup r
        JOIN nosku_refunds nr
          ON nr.marketplace_id = r.marketplace_id
         AND nr.ref_date = r.period_date
        JOIN daily_rev dr
          ON dr.marketplace_id = r.marketplace_id
         AND dr.period_date = r.period_date
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to, date_from, date_to))

    # -- 4. Other CM2 fees (with SKU) --
    cur.execute(f"""
        ;WITH other_fees AS (
            SELECT
                ft.marketplace_id,
                ft.sku,
                CAST(ft.posted_date AS DATE) AS fee_date,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS fee_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN {_OTHER_CM2_CHARGES_SQL}
              AND ft.sku IS NOT NULL AND ft.sku != ''
              AND CAST(ft.posted_date AS DATE) >= ?
              AND CAST(ft.posted_date AS DATE) <= ?
            GROUP BY ft.marketplace_id, ft.sku, CAST(ft.posted_date AS DATE)
        )
        UPDATE r SET
            r.other_fees_pln = oth.fee_pln
        FROM dbo.acc_sku_profitability_rollup r
        JOIN other_fees oth
          ON oth.marketplace_id = r.marketplace_id
         AND oth.sku = r.sku
         AND oth.fee_date = r.period_date
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to))
    stats["other_rows"] = cur.rowcount

    # -- 4b. Ad spend from acc_ads_product_day --
    cur.execute("""
        ;WITH ads AS (
            SELECT
                a.marketplace_id,
                a.sku,
                a.report_date,
                SUM(ISNULL(a.spend_pln, 0)) AS spend_total
            FROM dbo.acc_ads_product_day a WITH (NOLOCK)
            WHERE a.report_date >= ? AND a.report_date <= ?
              AND a.sku IS NOT NULL AND a.sku != ''
            GROUP BY a.marketplace_id, a.sku, a.report_date
        )
        UPDATE r SET
            r.ad_spend_pln = ads.spend_total,
            r.acos_pct = CASE WHEN r.revenue_pln > 0
                THEN ROUND(ads.spend_total / r.revenue_pln * 100, 2)
                ELSE NULL END
        FROM dbo.acc_sku_profitability_rollup r
        JOIN ads
          ON ads.marketplace_id = r.marketplace_id
         AND ads.sku = r.sku
         AND ads.report_date = r.period_date
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to))
    stats["ads_rows"] = cur.rowcount

    # -- 4b2. Catch-all: allocate unmatched monthly ads spend proportionally by revenue --
    # Step 4b matched ads by (sku, marketplace, day). If a SKU has ads on days
    # with no orders, those ads are lost. This step collects the monthly total,
    # subtracts what was already allocated, and distributes the remainder
    # proportionally by daily revenue across days that DO have rollup rows.
    cur.execute("""
        ;WITH monthly_ads AS (
            SELECT
                a.marketplace_id,
                a.sku,
                DATEFROMPARTS(YEAR(a.report_date), MONTH(a.report_date), 1) AS month_start,
                SUM(ISNULL(a.spend_pln, 0)) AS total_spend
            FROM dbo.acc_ads_product_day a WITH (NOLOCK)
            WHERE a.report_date >= ? AND a.report_date <= ?
              AND a.sku IS NOT NULL AND a.sku != ''
            GROUP BY a.marketplace_id, a.sku,
                     DATEFROMPARTS(YEAR(a.report_date), MONTH(a.report_date), 1)
        ),
        allocated AS (
            SELECT
                r.marketplace_id,
                r.sku,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS month_start,
                SUM(ISNULL(r.ad_spend_pln, 0)) AS already_allocated
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.ad_spend_pln > 0
            GROUP BY r.marketplace_id, r.sku,
                     DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
        ),
        unmatched AS (
            SELECT
                ma.marketplace_id,
                ma.sku,
                ma.month_start,
                ma.total_spend - ISNULL(al.already_allocated, 0) AS unmatched_spend
            FROM monthly_ads ma
            LEFT JOIN allocated al
              ON al.marketplace_id = ma.marketplace_id
             AND al.sku = ma.sku
             AND al.month_start = ma.month_start
            WHERE ma.total_spend - ISNULL(al.already_allocated, 0) > 0.01
        ),
        sku_month_rev AS (
            SELECT
                r.marketplace_id,
                r.sku,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS month_start,
                r.period_date,
                r.revenue_pln,
                SUM(r.revenue_pln) OVER (
                    PARTITION BY r.marketplace_id, r.sku,
                                 DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
                ) AS month_revenue
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.revenue_pln > 0
        )
        UPDATE r SET
            r.ad_spend_pln = ISNULL(r.ad_spend_pln, 0)
                + ROUND(u.unmatched_spend * smr.revenue_pln / smr.month_revenue, 2),
            r.acos_pct = CASE WHEN r.revenue_pln > 0
                THEN ROUND(
                    (ISNULL(r.ad_spend_pln, 0)
                     + ROUND(u.unmatched_spend * smr.revenue_pln / smr.month_revenue, 2))
                    / r.revenue_pln * 100, 2)
                ELSE NULL END
        FROM dbo.acc_sku_profitability_rollup r
        JOIN sku_month_rev smr
          ON smr.marketplace_id = r.marketplace_id
         AND smr.sku = r.sku
         AND smr.period_date = r.period_date
        JOIN unmatched u
          ON u.marketplace_id = smr.marketplace_id
         AND u.sku = smr.sku
         AND u.month_start = smr.month_start
        WHERE r.period_date >= ? AND r.period_date <= ?
          AND smr.month_revenue > 0
    """, (date_from, date_to, date_from, date_to,
          date_from, date_to, date_from, date_to))
    stats["ads_catchall_rows"] = cur.rowcount

    # -- 4b3. Campaign-level fallback: catch spend that never reached product-day --
    # SB/SD campaigns often have no product-level breakdown. Campaign-day totals
    # from acc_ads_campaign_day per marketplace are compared against what was
    # already allocated from product-day. The remainder is distributed to SKU
    # rollup rows proportionally by revenue within that marketplace+month.
    cur.execute("""
        ;WITH campaign_total AS (
            SELECT
                c.marketplace_id,
                DATEFROMPARTS(YEAR(d.report_date), MONTH(d.report_date), 1) AS month_start,
                SUM(ISNULL(d.spend_pln, 0)) AS total_spend_pln
            FROM dbo.acc_ads_campaign_day d WITH (NOLOCK)
            INNER JOIN dbo.acc_ads_campaign c WITH (NOLOCK)
                ON d.campaign_id = c.campaign_id AND d.ad_type = c.ad_type
            WHERE d.report_date >= ? AND d.report_date <= ?
              AND d.spend_pln IS NOT NULL
            GROUP BY c.marketplace_id,
                     DATEFROMPARTS(YEAR(d.report_date), MONTH(d.report_date), 1)
        ),
        already_in_rollup AS (
            SELECT
                r.marketplace_id,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS month_start,
                SUM(ISNULL(r.ad_spend_pln, 0)) AS allocated_spend
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
            GROUP BY r.marketplace_id,
                     DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
        ),
        gap AS (
            SELECT
                ct.marketplace_id,
                ct.month_start,
                ct.total_spend_pln - ISNULL(ar.allocated_spend, 0) AS gap_spend
            FROM campaign_total ct
            LEFT JOIN already_in_rollup ar
              ON ar.marketplace_id = ct.marketplace_id
             AND ar.month_start = ct.month_start
            WHERE ct.total_spend_pln - ISNULL(ar.allocated_spend, 0) > 0.50
        ),
        mkt_month_rev AS (
            SELECT
                r.marketplace_id,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS month_start,
                r.period_date,
                r.sku,
                r.revenue_pln,
                SUM(CASE WHEN r.revenue_pln > 0 THEN r.revenue_pln ELSE 0 END)
                    OVER (PARTITION BY r.marketplace_id,
                          DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1))
                    AS mkt_month_revenue
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.revenue_pln > 0
        )
        UPDATE r SET
            r.ad_spend_pln = ISNULL(r.ad_spend_pln, 0)
                + ROUND(g.gap_spend * mmr.revenue_pln / mmr.mkt_month_revenue, 2)
        FROM dbo.acc_sku_profitability_rollup r
        JOIN mkt_month_rev mmr
          ON mmr.marketplace_id = r.marketplace_id
         AND mmr.sku = r.sku
         AND mmr.period_date = r.period_date
        JOIN gap g
          ON g.marketplace_id = mmr.marketplace_id
         AND g.month_start = mmr.month_start
        WHERE r.period_date >= ? AND r.period_date <= ?
          AND mmr.mkt_month_revenue > 0
    """, (date_from, date_to, date_from, date_to,
          date_from, date_to, date_from, date_to))
    stats["ads_campaign_fallback_rows"] = cur.rowcount

    # -- 4c. Refund units from refund orders + finance-based returns --
    # Combines order-level is_refund=1 counts with finance-transaction-based
    # refund events (RefundComplete / Refund / RETURN) and takes MAX per SKU/day.
    cur.execute("""
        ;WITH order_ref AS (
            SELECT
                o.marketplace_id,
                ol.sku,
                CAST(o.purchase_date AS DATE) AS ref_date,
                SUM(ISNULL(ol.quantity_ordered, 0)) AS refund_units
            FROM dbo.acc_order o WITH (NOLOCK)
            JOIN dbo.acc_order_line ol WITH (NOLOCK)
              ON ol.order_id = o.id
            WHERE o.is_refund = 1
              AND CAST(o.purchase_date AS DATE) >= ?
              AND CAST(o.purchase_date AS DATE) <= ?
              AND ol.sku IS NOT NULL AND ol.sku != ''
            GROUP BY o.marketplace_id, ol.sku, CAST(o.purchase_date AS DATE)
        ),
        finance_ref AS (
            SELECT
                f.marketplace_id,
                f.sku,
                CAST(f.posted_date AS DATE) AS ref_date,
                COUNT(DISTINCT f.order_id) AS refund_units
            FROM dbo.acc_finance_transaction f WITH (NOLOCK)
            WHERE f.charge_type IN ('Refund', 'RefundComplete', 'RETURN')
              AND CAST(f.posted_date AS DATE) >= ?
              AND CAST(f.posted_date AS DATE) <= ?
              AND f.sku IS NOT NULL AND f.sku != ''
            GROUP BY f.marketplace_id, f.sku, CAST(f.posted_date AS DATE)
        ),
        combined AS (
            SELECT
                COALESCE(o.marketplace_id, fr.marketplace_id) AS marketplace_id,
                COALESCE(o.sku, fr.sku) AS sku,
                COALESCE(o.ref_date, fr.ref_date) AS ref_date,
                CASE WHEN ISNULL(o.refund_units, 0) >= ISNULL(fr.refund_units, 0)
                     THEN ISNULL(o.refund_units, 0)
                     ELSE ISNULL(fr.refund_units, 0) END AS refund_units
            FROM order_ref o
            FULL OUTER JOIN finance_ref fr
              ON fr.marketplace_id = o.marketplace_id
             AND fr.sku = o.sku
             AND fr.ref_date = o.ref_date
        )
        UPDATE r SET
            r.refund_units = c.refund_units,
            r.return_rate_pct = CASE WHEN r.units_sold > 0
                THEN ROUND(CAST(c.refund_units AS FLOAT) / r.units_sold * 100, 2)
                ELSE NULL END
        FROM dbo.acc_sku_profitability_rollup r
        JOIN combined c
          ON c.marketplace_id = r.marketplace_id
         AND c.sku = r.sku
         AND c.ref_date = r.period_date
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to,
          date_from, date_to))
    stats["return_units_rows"] = cur.rowcount

    # -- 4d. Logistics from acc_order_logistics_fact --
    # Uses centralised helpers from order_logistics_source to avoid duplication.
    # Prorates order-level logistics to SKU lines by line-revenue share.
    _lj = profit_logistics_join_sql(order_alias="o", fact_alias="f")
    _lv = profit_logistics_value_sql(order_alias="o", fact_alias="f")
    cur.execute(f"""
        SET LOCK_TIMEOUT 30000;
        ;WITH order_logistics AS (
            SELECT
                o.id AS order_id,
                o.marketplace_id,
                CAST(o.purchase_date AS DATE) AS order_date,
                {_lv} AS logistics_pln
            FROM dbo.acc_order o WITH (NOLOCK)
            {_lj}
            WHERE o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND o.status IN ('Shipped', 'Unshipped')
        ),
        line_rev AS (
            SELECT
                ol.order_id,
                ol.sku,
                ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)
                    - ISNULL(ol.promotion_discount, 0) AS line_net,
                SUM(ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)
                    - ISNULL(ol.promotion_discount, 0))
                    OVER (PARTITION BY ol.order_id) AS order_net
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            WHERE ol.sku IS NOT NULL
        ),
        sku_logistics AS (
            SELECT
                orl.marketplace_id,
                orl.order_date AS period_date,
                lr.sku,
                SUM(
                    CASE WHEN lr.order_net > 0
                         THEN ROUND(orl.logistics_pln * lr.line_net / lr.order_net, 4)
                         ELSE orl.logistics_pln  -- single-SKU order or zero-revenue
                    END
                ) AS logistics_pln
            FROM order_logistics orl
            JOIN line_rev lr ON lr.order_id = orl.order_id
            GROUP BY orl.marketplace_id, orl.order_date, lr.sku
        )
        UPDATE r SET
            r.logistics_pln = ROUND(sl.logistics_pln, 2)
        FROM dbo.acc_sku_profitability_rollup r
        JOIN sku_logistics sl
          ON sl.marketplace_id = r.marketplace_id
         AND sl.sku = r.sku
         AND sl.period_date = r.period_date
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to))
    stats["logistics_rows"] = cur.rowcount

    # -- 4e. Overhead NP fees (subscriptions, service fees, adjustments, EPR) --
    # These fees are typically account-level (no SKU). Allocate proportionally
    # by revenue per marketplace + month, same approach as storage fees.
    # SKU-level overhead (rare) is assigned directly first; the rest is spread.
    cur.execute(f"""
        ;WITH overhead_sku AS (
            SELECT
                ft.marketplace_id,
                ft.sku,
                CAST(ft.posted_date AS DATE) AS fee_date,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS overhead_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN {_OVERHEAD_NP_CHARGES_SQL}
              AND ft.sku IS NOT NULL AND ft.sku != ''
              AND CAST(ft.posted_date AS DATE) >= ?
              AND CAST(ft.posted_date AS DATE) <= ?
            GROUP BY ft.marketplace_id, ft.sku, CAST(ft.posted_date AS DATE)
        )
        UPDATE r SET
            r.overhead_pln = oh.overhead_pln
        FROM dbo.acc_sku_profitability_rollup r
        JOIN overhead_sku oh
          ON oh.marketplace_id = r.marketplace_id
         AND oh.sku = r.sku
         AND oh.fee_date = r.period_date
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to))
    stats["overhead_sku_rows"] = cur.rowcount

    # Overhead without SKU — allocate by revenue share per marketplace + month
    cur.execute(f"""
        ;WITH overhead_nosku AS (
            SELECT
                ft.marketplace_id,
                DATEFROMPARTS(YEAR(CAST(ft.posted_date AS DATE)), MONTH(CAST(ft.posted_date AS DATE)), 1) AS fee_month,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS total_overhead_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN {_OVERHEAD_NP_CHARGES_SQL}
              AND (ft.sku IS NULL OR ft.sku = '')
              AND CAST(ft.posted_date AS DATE) >= ?
              AND CAST(ft.posted_date AS DATE) <= ?
            GROUP BY ft.marketplace_id,
                     DATEFROMPARTS(YEAR(CAST(ft.posted_date AS DATE)), MONTH(CAST(ft.posted_date AS DATE)), 1)
        ),
        rev_totals AS (
            SELECT
                r.marketplace_id,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS fee_month,
                SUM(CASE WHEN r.revenue_pln > 0 THEN r.revenue_pln ELSE 0 END) AS mkt_revenue
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
            GROUP BY r.marketplace_id,
                     DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
        )
        UPDATE r SET
            r.overhead_pln = ISNULL(r.overhead_pln, 0) + CASE
                WHEN rt.mkt_revenue > 0
                THEN ROUND(o.total_overhead_pln * (CASE WHEN r.revenue_pln > 0 THEN r.revenue_pln ELSE 0 END) / rt.mkt_revenue, 2)
                ELSE 0 END
        FROM dbo.acc_sku_profitability_rollup r
        JOIN overhead_nosku o
          ON o.marketplace_id = r.marketplace_id
         AND o.fee_month = DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
        JOIN rev_totals rt
          ON rt.marketplace_id = r.marketplace_id
         AND rt.fee_month = o.fee_month
        WHERE r.period_date >= ? AND r.period_date <= ?
    """, (date_from, date_to, date_from, date_to, date_from, date_to))
    stats["overhead_alloc_rows"] = cur.rowcount

    # -- 5. Recalculate cm1_pln, cm2_pln, profit_pln (NP) and margin_pct --
    # CM1 = revenue - cogs - amazon_fees (referral) - fba_fees - logistics
    # CM2 = CM1 - ad_spend - refund - storage_fee - other_fees
    # NP  = CM2 - overhead (subscriptions, service fees, adjustments, EPR)
    cur.execute("""
        UPDATE dbo.acc_sku_profitability_rollup SET
            cm1_pln = revenue_pln - cogs_pln - amazon_fees_pln
                      - fba_fees_pln - logistics_pln,
            cm2_pln = revenue_pln - cogs_pln - amazon_fees_pln
                      - fba_fees_pln - logistics_pln
                      - ad_spend_pln - refund_pln - storage_fee_pln - other_fees_pln,
            profit_pln = revenue_pln - cogs_pln - amazon_fees_pln
                         - fba_fees_pln - logistics_pln - ad_spend_pln
                         - refund_pln - storage_fee_pln - other_fees_pln
                         - overhead_pln,
            margin_pct = CASE WHEN revenue_pln <> 0
                THEN (revenue_pln - cogs_pln - amazon_fees_pln
                      - fba_fees_pln - logistics_pln - ad_spend_pln
                      - refund_pln - storage_fee_pln - other_fees_pln
                      - overhead_pln)
                     / revenue_pln * 100
                ELSE 0 END,
            computed_at = SYSUTCDATETIME()
        WHERE period_date >= ? AND period_date <= ?
    """, (date_from, date_to))

    # -- 6. Refresh marketplace rollup to inherit enriched values --
    cur.execute("""
        UPDATE tgt SET
            tgt.logistics_pln = src.logistics_pln,
            tgt.storage_fee_pln = src.storage_fee_pln,
            tgt.refund_pln = src.refund_pln,
            tgt.other_fees_pln = src.other_fees_pln,
            tgt.overhead_pln = src.overhead_pln,
            tgt.ad_spend_pln = src.ad_spend_pln,
            tgt.refund_units = src.refund_units,
            tgt.cm1_pln = src.cm1_pln,
            tgt.cm2_pln = src.cm2_pln,
            tgt.profit_pln = src.profit_pln,
            tgt.margin_pct = CASE WHEN src.revenue_pln <> 0
                THEN src.profit_pln / src.revenue_pln * 100 ELSE 0 END,
            tgt.acos_pct = CASE WHEN src.revenue_pln <> 0
                THEN src.ad_spend_pln / src.revenue_pln * 100 ELSE NULL END,
            tgt.return_rate_pct = CASE WHEN src.total_units > 0
                THEN src.refund_units * 100.0 / src.total_units ELSE NULL END,
            tgt.computed_at = SYSUTCDATETIME()
        FROM dbo.acc_marketplace_profitability_rollup tgt
        JOIN (
            SELECT
                r.period_date,
                r.marketplace_id,
                SUM(r.revenue_pln) as revenue_pln,
                SUM(r.units_sold) as total_units,
                SUM(r.logistics_pln) as logistics_pln,
                SUM(r.storage_fee_pln) as storage_fee_pln,
                SUM(r.refund_pln) as refund_pln,
                SUM(r.other_fees_pln) as other_fees_pln,
                SUM(r.overhead_pln) as overhead_pln,
                SUM(r.ad_spend_pln) as ad_spend_pln,
                SUM(r.refund_units) as refund_units,
                SUM(r.cm1_pln) as cm1_pln,
                SUM(r.cm2_pln) as cm2_pln,
                SUM(r.profit_pln) as profit_pln
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
            GROUP BY r.period_date, r.marketplace_id
        ) src
          ON src.period_date = tgt.period_date
         AND src.marketplace_id = tgt.marketplace_id
        WHERE tgt.period_date >= ? AND tgt.period_date <= ?
    """, (date_from, date_to, date_from, date_to))

    log.info(
        "profitability.enrich_done",
        storage_rows=stats["storage_rows"],
        refund_rows=stats["refund_rows"],
        other_rows=stats["other_rows"],
        ads_rows=stats["ads_rows"],
        ads_catchall_rows=stats["ads_catchall_rows"],
        ads_campaign_fallback_rows=stats["ads_campaign_fallback_rows"],
        return_units_rows=stats["return_units_rows"],
        logistics_rows=stats["logistics_rows"],
        overhead_sku_rows=stats["overhead_sku_rows"],
        overhead_alloc_rows=stats["overhead_alloc_rows"],
    )
    return stats


# ---------------------------------------------------------------------------
# 6b) Rollup recompute job
# ---------------------------------------------------------------------------

def recompute_rollups(
    date_from: date | None = None,
    date_to: date | None = None,
    days_back: int = 7,
) -> dict:
    """
    Recompute SKU + marketplace rollup tables from source data.

    Uses MERGE (upsert) per day to be idempotent.
    """
    t0 = time.time()
    if date_from is None:
        date_from = date.today() - timedelta(days=days_back)
    if date_to is None:
        date_to = date.today()

    conn = connect_acc(autocommit=False, timeout=600)
    sku_total = 0
    mkt_total = 0
    order_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
    order_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

    try:
        cur = conn.cursor()
        ensure_rollup_layer_columns(cur)

        # -- SKU Rollup: aggregate from acc_order + acc_order_line --
        # Revenue = item_price − item_tax − promotion_discount + buyer-paid shipping
        cur.execute(f"""
            MERGE dbo.acc_sku_profitability_rollup AS tgt
            USING (
                SELECT
                    CAST(o.purchase_date AS DATE) as period_date,
                    o.marketplace_id,
                    ol.sku,
                    MAX(ol.asin) as asin,
                    ISNULL(SUM(ol.quantity_ordered), 0) as units_sold,
                    COUNT(DISTINCT o.id) as orders_count,
                    ISNULL(SUM(
                        (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                        * ISNULL(fx.rate_to_pln,
                            {_fx_case('o.currency')})
                    ), 0)
                    + ISNULL(SUM(
                        ISNULL(spo.shipping_charge_pln, 0) * CASE
                            WHEN ISNULL(olt.order_net, 0) > 0 THEN
                                (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                                / NULLIF(olt.order_net, 0)
                            WHEN ISNULL(olt.order_units_total, 0) > 0 THEN
                                ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    ), 0) as revenue_pln,
                    ISNULL(SUM(ISNULL(ol.cogs_pln, 0)), 0) as cogs_pln,
                    ISNULL(SUM(ISNULL(ol.referral_fee_pln, 0)), 0)
                    + ISNULL(SUM(
                        (
                            ISNULL(o.shipping_surcharge_pln, 0)
                            + ISNULL(o.promo_order_fee_pln, 0)
                            + ISNULL(o.refund_commission_pln, 0)
                        ) * CASE
                            WHEN ISNULL(olt.order_net, 0) > 0 THEN
                                (
                                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                                    / NULLIF(olt.order_net, 0)
                                )
                            WHEN ISNULL(olt.order_units_total, 0) > 0 THEN
                                ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    ), 0) as amazon_fees_pln,
                    ISNULL(SUM(ISNULL(ol.fba_fee_pln, 0)), 0) as fba_fees_pln,
                    ISNULL(SUM(
                        {order_logistics_value_sql} * CASE
                            WHEN ISNULL(olt.order_net, 0) > 0 THEN
                                (
                                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                                    / NULLIF(olt.order_net, 0)
                                )
                            WHEN ISNULL(olt.order_units_total, 0) > 0 THEN
                                ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    ), 0) as logistics_pln,
                    0 as ad_spend_pln,
                    0 as refund_pln,
                    0 as storage_fee_pln,
                    0 as other_fees_pln,
                    0 as refund_units
                FROM dbo.acc_order o WITH (NOLOCK)
                JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
                {order_logistics_join_sql}
                OUTER APPLY (
                    SELECT TOP 1 er.rate_to_pln
                    FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                    WHERE er.currency = o.currency AND er.rate_date <= o.purchase_date
                    ORDER BY er.rate_date DESC
                ) fx
                OUTER APPLY (
                    SELECT
                        SUM(CASE WHEN ft.charge_type IN ('ShippingCharge','ShippingTax','ShippingDiscount')
                            THEN ISNULL(ft.amount_pln,
                                ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")})
                            ELSE 0 END) AS shipping_charge_pln
                    FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                    WHERE ft.amazon_order_id = o.amazon_order_id
                      AND (ft.marketplace_id = o.marketplace_id
                           OR ft.marketplace_id IS NULL)
                ) spo
                OUTER APPLY (
                    SELECT
                        ISNULL(SUM(
                            ISNULL(ol2.item_price, 0)
                            - ISNULL(ol2.item_tax, 0)
                            - ISNULL(ol2.promotion_discount, 0)
                        ), 0) AS order_net,
                        ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                    FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                    WHERE ol2.order_id = o.id
                ) olt
                WHERE o.purchase_date >= CAST(? AS DATE)
                  AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
                  AND o.status IN ('Shipped', 'Unshipped')
                  AND ol.sku IS NOT NULL
                GROUP BY CAST(o.purchase_date AS DATE), o.marketplace_id, ol.sku
            ) AS src
            ON tgt.period_date = src.period_date
               AND tgt.marketplace_id = src.marketplace_id
               AND tgt.sku = src.sku
            WHEN MATCHED THEN UPDATE SET
                asin = src.asin,
                units_sold = src.units_sold,
                orders_count = src.orders_count,
                revenue_pln = src.revenue_pln,
                cogs_pln = src.cogs_pln,
                amazon_fees_pln = src.amazon_fees_pln,
                fba_fees_pln = src.fba_fees_pln,
                logistics_pln = src.logistics_pln,
                ad_spend_pln = ISNULL(tgt.ad_spend_pln, 0),
                refund_pln = ISNULL(tgt.refund_pln, 0),
                storage_fee_pln = ISNULL(tgt.storage_fee_pln, 0),
                other_fees_pln = ISNULL(tgt.other_fees_pln, 0),
                cm1_pln = src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                          - src.fba_fees_pln - src.logistics_pln,
                cm2_pln = src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                          - src.fba_fees_pln - src.logistics_pln
                          - ISNULL(tgt.ad_spend_pln, 0) - ISNULL(tgt.refund_pln, 0)
                          - ISNULL(tgt.storage_fee_pln, 0) - ISNULL(tgt.other_fees_pln, 0),
                profit_pln = src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                             - src.fba_fees_pln - src.logistics_pln
                             - ISNULL(tgt.ad_spend_pln, 0) - ISNULL(tgt.refund_pln, 0)
                             - ISNULL(tgt.storage_fee_pln, 0) - ISNULL(tgt.other_fees_pln, 0),
                margin_pct = CASE WHEN src.revenue_pln <> 0
                    THEN (src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                          - src.fba_fees_pln - src.logistics_pln
                          - ISNULL(tgt.ad_spend_pln, 0) - ISNULL(tgt.refund_pln, 0)
                          - ISNULL(tgt.storage_fee_pln, 0) - ISNULL(tgt.other_fees_pln, 0))
                         / src.revenue_pln * 100
                    ELSE 0 END,
                acos_pct = CASE WHEN src.revenue_pln <> 0
                    THEN ISNULL(tgt.ad_spend_pln, 0) / src.revenue_pln * 100 ELSE NULL END,
                refund_units = ISNULL(tgt.refund_units, 0),
                return_rate_pct = CASE WHEN src.units_sold <> 0
                    THEN ISNULL(tgt.refund_units, 0) * 100.0 / src.units_sold ELSE NULL END,
                computed_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                period_date, marketplace_id, sku, asin,
                units_sold, orders_count, revenue_pln,
                cogs_pln, amazon_fees_pln, fba_fees_pln, logistics_pln,
                ad_spend_pln, refund_pln, storage_fee_pln, other_fees_pln,
                cm1_pln, cm2_pln,
                profit_pln, margin_pct, acos_pct,
                refund_units, return_rate_pct, computed_at
            ) VALUES (
                src.period_date, src.marketplace_id, src.sku, src.asin,
                src.units_sold, src.orders_count, src.revenue_pln,
                src.cogs_pln, src.amazon_fees_pln, src.fba_fees_pln, src.logistics_pln,
                src.ad_spend_pln, src.refund_pln, src.storage_fee_pln, src.other_fees_pln,
                src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                - src.fba_fees_pln - src.logistics_pln,
                src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                - src.fba_fees_pln - src.logistics_pln - src.ad_spend_pln
                - src.refund_pln - src.storage_fee_pln - src.other_fees_pln,
                src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                - src.fba_fees_pln - src.logistics_pln - src.ad_spend_pln
                - src.refund_pln - src.storage_fee_pln - src.other_fees_pln,
                CASE WHEN src.revenue_pln <> 0
                    THEN (src.revenue_pln - src.cogs_pln - src.amazon_fees_pln
                          - src.fba_fees_pln - src.logistics_pln - src.ad_spend_pln
                          - src.refund_pln - src.storage_fee_pln - src.other_fees_pln)
                         / src.revenue_pln * 100
                    ELSE 0 END,
                CASE WHEN src.revenue_pln <> 0
                    THEN src.ad_spend_pln / src.revenue_pln * 100 ELSE NULL END,
                src.refund_units,
                CASE WHEN src.units_sold <> 0
                    THEN src.refund_units * 100.0 / src.units_sold ELSE NULL END,
                SYSUTCDATETIME()
            );
        """, (date_from, date_to))
        sku_total = cur.rowcount
        conn.commit()

        # -- Marketplace Rollup: aggregate from SKU rollup --
        cur.execute("""
            MERGE dbo.acc_marketplace_profitability_rollup AS tgt
            USING (
                SELECT
                    r.period_date,
                    r.marketplace_id,
                    SUM(r.orders_count) as total_orders,
                    SUM(r.units_sold) as total_units,
                    COUNT(DISTINCT r.sku) as unique_skus,
                    SUM(r.revenue_pln) as revenue_pln,
                    SUM(r.cogs_pln) as cogs_pln,
                    SUM(r.amazon_fees_pln) as amazon_fees_pln,
                    SUM(r.fba_fees_pln) as fba_fees_pln,
                    SUM(r.logistics_pln) as logistics_pln,
                    SUM(r.ad_spend_pln) as ad_spend_pln,
                    SUM(r.refund_pln) as refund_pln,
                    SUM(r.storage_fee_pln) as storage_fee_pln,
                    SUM(r.other_fees_pln) as other_fees_pln,
                    SUM(r.cm1_pln) as cm1_pln,
                    SUM(r.cm2_pln) as cm2_pln,
                    SUM(r.profit_pln) as profit_pln,
                    SUM(r.refund_units) as refund_units
                FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                WHERE r.period_date >= ? AND r.period_date <= ?
                GROUP BY r.period_date, r.marketplace_id
            ) AS src
            ON tgt.period_date = src.period_date AND tgt.marketplace_id = src.marketplace_id
            WHEN MATCHED THEN UPDATE SET
                total_orders = src.total_orders,
                total_units = src.total_units,
                unique_skus = src.unique_skus,
                revenue_pln = src.revenue_pln,
                cogs_pln = src.cogs_pln,
                amazon_fees_pln = src.amazon_fees_pln,
                fba_fees_pln = src.fba_fees_pln,
                logistics_pln = src.logistics_pln,
                ad_spend_pln = src.ad_spend_pln,
                refund_pln = src.refund_pln,
                storage_fee_pln = src.storage_fee_pln,
                other_fees_pln = src.other_fees_pln,
                cm1_pln = src.cm1_pln,
                cm2_pln = src.cm2_pln,
                profit_pln = src.profit_pln,
                margin_pct = CASE WHEN src.revenue_pln <> 0
                    THEN src.profit_pln / src.revenue_pln * 100 ELSE 0 END,
                acos_pct = CASE WHEN src.revenue_pln <> 0
                    THEN src.ad_spend_pln / src.revenue_pln * 100 ELSE NULL END,
                refund_units = src.refund_units,
                return_rate_pct = CASE WHEN src.total_units <> 0
                    THEN src.refund_units * 100.0 / src.total_units ELSE NULL END,
                computed_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                period_date, marketplace_id,
                total_orders, total_units, unique_skus, revenue_pln,
                cogs_pln, amazon_fees_pln, fba_fees_pln, logistics_pln,
                ad_spend_pln, refund_pln, storage_fee_pln, other_fees_pln,
                cm1_pln, cm2_pln,
                profit_pln, margin_pct, acos_pct,
                refund_units, return_rate_pct, computed_at
            ) VALUES (
                src.period_date, src.marketplace_id,
                src.total_orders, src.total_units, src.unique_skus, src.revenue_pln,
                src.cogs_pln, src.amazon_fees_pln, src.fba_fees_pln, src.logistics_pln,
                src.ad_spend_pln, src.refund_pln, src.storage_fee_pln, src.other_fees_pln,
                src.cm1_pln,
                src.cm2_pln,
                src.profit_pln,
                CASE WHEN src.revenue_pln <> 0
                    THEN src.profit_pln / src.revenue_pln * 100 ELSE 0 END,
                CASE WHEN src.revenue_pln <> 0
                    THEN src.ad_spend_pln / src.revenue_pln * 100 ELSE NULL END,
                src.refund_units,
                CASE WHEN src.total_units <> 0
                    THEN src.refund_units * 100.0 / src.total_units ELSE NULL END,
                SYSUTCDATETIME()
            );
        """, (date_from, date_to))
        mkt_total = cur.rowcount
        conn.commit()

        # -- Enrich rollup from acc_finance_transaction --
        # Use a separate connection so enrichment timeout doesn't lose the core rollup
        enriched: dict = {}
        try:
            enrich_conn = connect_acc(autocommit=False, timeout=900)
            try:
                enrich_cur = enrich_conn.cursor()
                enriched = _enrich_rollup_from_finance(enrich_cur, enrich_conn, date_from, date_to)
                enrich_conn.commit()
            except Exception as enrich_exc:
                log.warning("profitability.enrichment_failed", error=str(enrich_exc),
                            msg="Finance enrichment failed — core rollup is intact")
                try:
                    enrich_conn.rollback()
                except Exception:
                    pass
            finally:
                try:
                    enrich_conn.close()
                except Exception:
                    pass
        except Exception as conn_exc:
            log.warning("profitability.enrichment_conn_failed", error=str(conn_exc))

        # SF-05 guard: warn if enrichment populated zero rows for any category
        for field, count in enriched.items():
            if count == 0:
                log.warning(
                    "profitability.enrichment_empty",
                    field=field,
                    date_from=str(date_from),
                    date_to=str(date_to),
                    msg=f"Enrichment produced 0 rows for {field} - costs may be missing",
                )

        # -- Phase 4: persist recompute timestamp to acc_system_metadata --
        recomputed_at = datetime.now(timezone.utc).isoformat()
        try:
            _ensure_system_metadata_table(cur)
            _upsert_system_metadata(cur, 'rollup_recomputed_at', recomputed_at)
            _upsert_system_metadata(cur, 'rollup_date_from', str(date_from))
            _upsert_system_metadata(cur, 'rollup_date_to', str(date_to))
            conn.commit()
        except Exception as meta_exc:
            log.warning("profitability.metadata_write_failed", error=str(meta_exc))
            try:
                conn.rollback()
            except Exception:
                pass

        elapsed = time.time() - t0
        log.info(
            "profitability.rollup_done",
            sku_rows=sku_total, mkt_rows=mkt_total,
            enriched=enriched,
            date_from=str(date_from), date_to=str(date_to),
            elapsed=round(elapsed, 1),
        )
        return {
            "sku_rows_upserted": sku_total,
            "marketplace_rows_upserted": mkt_total,
            "enriched_fields": enriched,
            "date_from": date_from,
            "date_to": date_to,
            "elapsed_seconds": round(elapsed, 1),
            "recomputed_at": recomputed_at,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 7) Profitability alerts evaluation
# ---------------------------------------------------------------------------

def evaluate_profitability_alerts(date_from: date, date_to: date) -> dict:
    """
    Scan rollup data for alert conditions:
    - loss_order: orders with profit < 0
    - high_acos: SKUs with ACOS > threshold
    - high_return_rate: SKUs with return rate > threshold
    - low_margin: SKUs with margin < threshold
    - content_quality_low: SKUs with content score < threshold

    Creates alerts via existing alert infrastructure.
    """
    from app.connectors.mssql import create_alert

    conn = connect_acc(autocommit=False, timeout=30)
    alerts_created = 0
    try:
        cur = conn.cursor()

        # Load active rules
        cur.execute("""
            SELECT id, rule_type, threshold_value, threshold_operator, severity, marketplace_id, sku
            FROM dbo.acc_alert_rule WITH (NOLOCK)
            WHERE is_active = 1
              AND rule_type IN ('loss_order', 'high_acos', 'high_return_rate', 'low_margin', 'content_quality_low')
        """)
        rules = _fetchall_dict(cur)

        for rule in rules:
            rule_type = rule["rule_type"]
            threshold = float(rule.get("threshold_value") or 0)
            severity = rule.get("severity") or "warning"
            rule_mkt = rule.get("marketplace_id")
            rule_sku = rule.get("sku")

            mkt_clause = "AND r.marketplace_id = ?" if rule_mkt else ""
            sku_clause = "AND r.sku = ?" if rule_sku else ""
            extra_params: list = []
            if rule_mkt:
                extra_params.append(rule_mkt)
            if rule_sku:
                extra_params.append(rule_sku)

            if rule_type == "high_acos":
                cur.execute(f"""
                    SELECT TOP 10 r.sku, r.marketplace_id,
                        CASE WHEN SUM(r.revenue_pln) <> 0
                             THEN SUM(r.ad_spend_pln) / SUM(r.revenue_pln) * 100
                             ELSE 0 END as acos
                    FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                    WHERE r.period_date >= ? AND r.period_date <= ?
                    {mkt_clause} {sku_clause}
                    GROUP BY r.sku, r.marketplace_id
                    HAVING CASE WHEN SUM(r.revenue_pln) <> 0
                                THEN SUM(r.ad_spend_pln) / SUM(r.revenue_pln) * 100
                                ELSE 0 END > ?
                    ORDER BY acos DESC
                """, (date_from, date_to, *extra_params, threshold))
                for r in cur.fetchall():
                    create_alert(
                        title=f"High ACOS: {r[0]} ({_mkt_code(r[1])}) = {_f(r[2]):.1f}%",
                        detail=f"ACOS {_f(r[2]):.1f}% exceeds threshold {threshold:.1f}%",
                        severity=severity,
                        marketplace_id=r[1],
                        sku=r[0],
                        rule_id=rule["id"],
                    )
                    alerts_created += 1

            elif rule_type == "low_margin":
                cur.execute(f"""
                    SELECT TOP 10 r.sku, r.marketplace_id,
                        CASE WHEN SUM(r.revenue_pln) <> 0
                             THEN SUM(r.profit_pln) / SUM(r.revenue_pln) * 100
                             ELSE 0 END as margin
                    FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                    WHERE r.period_date >= ? AND r.period_date <= ?
                    {mkt_clause} {sku_clause}
                    GROUP BY r.sku, r.marketplace_id
                    HAVING SUM(r.revenue_pln) > 0
                       AND CASE WHEN SUM(r.revenue_pln) <> 0
                                THEN SUM(r.profit_pln) / SUM(r.revenue_pln) * 100
                                ELSE 0 END < ?
                    ORDER BY margin ASC
                """, (date_from, date_to, *extra_params, threshold))
                for r in cur.fetchall():
                    create_alert(
                        title=f"Low Margin: {r[0]} ({_mkt_code(r[1])}) = {_f(r[2]):.1f}%",
                        detail=f"Margin {_f(r[2]):.1f}% below threshold {threshold:.1f}%",
                        severity=severity,
                        marketplace_id=r[1],
                        sku=r[0],
                        rule_id=rule["id"],
                    )
                    alerts_created += 1

            elif rule_type == "high_return_rate":
                cur.execute(f"""
                    SELECT TOP 10 r.sku, r.marketplace_id,
                        CASE WHEN SUM(r.units_sold) <> 0
                             THEN SUM(r.refund_units) * 100.0 / SUM(r.units_sold)
                             ELSE 0 END as rr
                    FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                    WHERE r.period_date >= ? AND r.period_date <= ?
                    {mkt_clause} {sku_clause}
                    GROUP BY r.sku, r.marketplace_id
                    HAVING SUM(r.units_sold) >= 5
                       AND CASE WHEN SUM(r.units_sold) <> 0
                                THEN SUM(r.refund_units) * 100.0 / SUM(r.units_sold)
                                ELSE 0 END > ?
                    ORDER BY rr DESC
                """, (date_from, date_to, *extra_params, threshold))
                for r in cur.fetchall():
                    create_alert(
                        title=f"High Return Rate: {r[0]} ({_mkt_code(r[1])}) = {_f(r[2]):.1f}%",
                        detail=f"Return rate {_f(r[2]):.1f}% exceeds threshold {threshold:.1f}%",
                        severity=severity,
                        marketplace_id=r[1],
                        sku=r[0],
                        rule_id=rule["id"],
                    )
                    alerts_created += 1

            elif rule_type == "content_quality_low":
                cur.execute(f"""
                    SELECT TOP 10 cs.seller_sku, cs.marketplace_id, cs.total_score
                    FROM dbo.acc_content_score cs WITH (NOLOCK)
                    WHERE cs.total_score < ?
                      AND cs.scored_at >= DATEADD(DAY, -7, SYSUTCDATETIME())
                    {("AND cs.marketplace_id = ?" if rule_mkt else "")}
                    {("AND cs.seller_sku = ?" if rule_sku else "")}
                    ORDER BY cs.total_score ASC
                """, (threshold, *extra_params))
                for r in cur.fetchall():
                    create_alert(
                        title=f"Low Content Score: {r[0]} ({_mkt_code(r[1])}) = {_f(r[2]):.0f}/100",
                        detail=f"Content quality score {_f(r[2]):.0f} below threshold {threshold:.0f}",
                        severity=severity,
                        marketplace_id=r[1],
                        sku=r[0],
                        rule_id=rule["id"],
                    )
                    alerts_created += 1

        return {"alerts_created": alerts_created, "rules_evaluated": len(rules)}
    finally:
        conn.close()
