"""Profit Engine - CM2/NP allocation calculator.

Extracted from the monolithic profit_engine.py (Sprint 3).
Manages FBA component cost pool loading, marketplace weight totals,
inventory maps, overhead pool loading, and cost allocation to products.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import pyodbc
import structlog

from app.intelligence.profit.helpers import (
    _connect, _fetchall_dict, _f, _f_strict, _i,
    _warnings_reset, _warnings_append, _warnings_collect,
    _result_cache_get, _result_cache_set, _result_cache_invalidate,
)
from app.intelligence.profit.cost_model import (
    _classify_finance_charge, _classify_fba_component,
    _fx_case, _fx_rate_for_currency,
    _load_fx_cache,
    ensure_profit_cost_model_schema,
    _get_cost_config_decimal,
)

log = structlog.get_logger(__name__)

def _load_fba_component_pools(
    cur: pyodbc.Cursor,
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, dict[str, float]]:
    """Load CM2 cost pools per marketplace from finance transactions.

    Returns dict[marketplace_id → {bucket_name: total_pln}].
    CM2 buckets: storage, aged, removal, liquidation, refund_cost,
                 shipping_surcharge, fba_inbound.
    """
    cache_key = f"cm2_component_pools:{date_from}:{date_to}:{marketplace_id or '*'}"
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return dict(cached)

    _CM2_EMPTY = {
        "storage": 0.0, "aged": 0.0, "removal": 0.0, "liquidation": 0.0,
        "refund_cost": 0.0, "shipping_surcharge": 0.0, "fba_inbound": 0.0,
        "promo": 0.0, "warehouse_loss": 0.0, "amazon_other_fee": 0.0,
    }

    rows_all: list[Any] = []
    try:
        params_direct: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        market_direct = ""
        if marketplace_id:
            market_direct = " AND ft.marketplace_id = ?"
            params_direct.append(marketplace_id)
        cur.execute(
            f"""
            SELECT
                ft.marketplace_id AS marketplace_id,
                ft.charge_type,
                ft.transaction_type,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS amount_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.posted_date >= CAST(? AS DATE)
              AND ft.posted_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ft.marketplace_id IS NOT NULL
              {market_direct}
            GROUP BY ft.marketplace_id, ft.charge_type, ft.transaction_type
            """,
            params_direct,
        )
        rows_all.extend(cur.fetchall())
    except Exception as exc:
        log.warning("profit_engine.cm2_direct_query_failed", error=str(exc))
        _warnings_append("CM2 cost pools unavailable (direct query) — margins may be overstated")

    # Fallback: transactions without marketplace_id → resolve via order join
    try:
        params_join: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        market_join = ""
        if marketplace_id:
            market_join = " AND o.marketplace_id = ?"
            params_join.append(marketplace_id)
        cur.execute(
            f"""
            SELECT
                o.marketplace_id AS marketplace_id,
                ft.charge_type,
                ft.transaction_type,
                SUM(ABS(ISNULL(ft.amount_pln, 0))) AS amount_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK)
              ON o.amazon_order_id = ft.amazon_order_id
            WHERE ft.posted_date >= CAST(? AS DATE)
              AND ft.posted_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ft.marketplace_id IS NULL
              AND o.marketplace_id IS NOT NULL
              {market_join}
            GROUP BY o.marketplace_id, ft.charge_type, ft.transaction_type
            """,
            params_join,
        )
        rows_all.extend(cur.fetchall())
    except Exception as exc:
        log.warning("profit_engine.cm2_fallback_query_failed", error=str(exc))
        _warnings_append("CM2 cost pools unavailable (fallback query) — margins may be overstated")

    if not rows_all:
        _result_cache_set(cache_key, {}, ttl=600)
        return {}

    pools: dict[str, dict[str, float]] = {}
    for row in rows_all:
        mkt = str(row[0] or "").strip()
        if not mkt:
            continue
        classification = _classify_finance_charge(row[1], row[2])
        if classification is None:
            continue
        if classification["layer"] != "cm2":
            continue
        bucket = classification["bucket"]
        # Map detailed bucket to pool key
        pool_key = bucket
        if pool_key == "fba_aged":
            pool_key = "aged"
        elif pool_key == "fba_storage":
            pool_key = "storage"
        elif pool_key == "fba_removal":
            pool_key = "removal"
        elif pool_key == "fba_liquidation":
            pool_key = "liquidation"
        if pool_key not in _CM2_EMPTY:
            continue
        amount = _f(row[3])
        if amount <= 0:
            continue

        sign = classification["sign"]
        p = pools.setdefault(mkt, dict(_CM2_EMPTY))
        if sign < 0:
            p[pool_key] = round(p[pool_key] - amount, 4)  # recovery
        else:
            p[pool_key] = round(p[pool_key] + amount, 4)
    _result_cache_set(cache_key, pools, ttl=600)
    return pools


def _load_marketplace_weight_totals(
    cur: pyodbc.Cursor,
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, dict[str, float]]:
    """Load AFN units + revenue totals per marketplace for stable component allocation."""
    cache_key = f"marketplace_weight_totals:{date_from}:{date_to}:{marketplace_id or '*'}"
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return dict(cached)

    params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
    market_sql = ""
    if marketplace_id:
        market_sql = " AND o.marketplace_id = ?"
        params.append(marketplace_id)

    try:
        cur.execute(
            f"""
            SELECT
                o.marketplace_id,
                SUM(CASE WHEN ISNULL(o.fulfillment_channel, '') = 'AFN'
                    THEN ISNULL(ol.quantity_ordered, 0) ELSE 0 END) AS afn_units,
                SUM(
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                    * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')})
                )
                + ISNULL(SUM(
                    ISNULL(spo.shipping_charge_pln, 0) * CASE
                        WHEN ISNULL(olt.order_net, 0) > 0 THEN
                            (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                            / NULLIF(olt.order_net, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0 THEN
                            ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                ), 0) AS revenue_pln
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= o.purchase_date
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
                        ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                        - ISNULL(ol2.promotion_discount, 0)
                    ), 0) AS order_net,
                    ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            WHERE o.status = 'Shipped'
              AND o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
              AND o.amazon_order_id NOT LIKE 'S02-%'
              {market_sql}
            GROUP BY o.marketplace_id
            """,
            params,
        )
        rows = _fetchall_dict(cur)
    except Exception as exc:
        log.warning("profit_engine.marketplace_weight_totals_failed", error=str(exc))
        _warnings_append("Marketplace weight totals unavailable — CM2 cost allocation may be inaccurate")
        return {}

    out: dict[str, dict[str, float]] = {}
    for row in rows:
        mkt = str(row.get("marketplace_id") or "").strip()
        if not mkt:
            continue
        out[mkt] = {
            "afn_units": max(_f(row.get("afn_units")), 0.0),
            "revenue_pln": max(_f(row.get("revenue_pln")), 0.0),
        }
    _result_cache_set(cache_key, out, ttl=600)
    return out


def _load_latest_inventory_available_map(
    cur: pyodbc.Cursor,
    *,
    marketplace_id: str | None = None,
) -> dict[tuple[str, str], float]:
    """Latest estimated available stock map keyed by (asin, marketplace_id)."""
    cache_key = f"inventory_available_map:{marketplace_id or '*'}"
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return dict(cached)

    params: list[Any] = []
    market_sql = ""
    if marketplace_id:
        market_sql = " AND marketplace_id = ?"
        params.append(marketplace_id)
    try:
        cur.execute(
            f"""
            WITH latest_per_sku AS (
                SELECT
                    marketplace_id,
                    asin,
                    sku,
                    CAST(ISNULL(on_hand, 0) AS FLOAT) AS on_hand,
                    CAST(ISNULL(reserved, 0) AS FLOAT) AS reserved,
                    ROW_NUMBER() OVER (
                        PARTITION BY marketplace_id, asin, sku
                        ORDER BY snapshot_date DESC, created_at DESC
                    ) AS rn
                FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
                WHERE ISNULL(asin, '') <> ''
                  {market_sql}
            )
            SELECT
                marketplace_id,
                asin,
                SUM(CASE WHEN on_hand - reserved > 0 THEN on_hand - reserved ELSE 0 END) AS available_units
            FROM latest_per_sku
            WHERE rn = 1
            GROUP BY marketplace_id, asin
            """,
            params,
        )
        rows = _fetchall_dict(cur)
    except Exception:
        return {}

    out: dict[tuple[str, str], float] = {}
    for row in rows:
        asin = str(row.get("asin") or "").strip()
        mkt = str(row.get("marketplace_id") or "").strip()
        if not asin or not mkt:
            continue
        out[(asin, mkt)] = max(_f(row.get("available_units")), 0.0)
    _result_cache_set(cache_key, out, ttl=300)
    return out


def _load_overhead_pools(
    cur: pyodbc.Cursor,
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> list[dict[str, Any]]:
    """Load overhead (NP) pools: manual config + auto-detected from finance transactions."""
    out: list[dict[str, Any]] = []

    # 1. Manual pools from acc_profit_overhead_pool (admin-configured)
    try:
        ensure_profit_cost_model_schema()
        params: list[Any] = [date_to.isoformat(), date_from.isoformat()]
        market_sql = ""
        if marketplace_id:
            market_sql = " AND (marketplace_id = ? OR marketplace_id IS NULL)"
            params.append(marketplace_id)
        cur.execute(
            f"""
            SELECT
                marketplace_id,
                pool_name,
                SUM(amount_pln) AS amount_pln,
                MIN(allocation_method) AS allocation_method,
                AVG(CAST(confidence_pct AS FLOAT)) AS confidence_pct
            FROM dbo.acc_profit_overhead_pool WITH (NOLOCK)
            WHERE is_active = 1
              AND period_from <= CAST(? AS DATE)
              AND period_to >= CAST(? AS DATE)
              {market_sql}
            GROUP BY marketplace_id, pool_name
            HAVING SUM(amount_pln) <> 0
            """
            ,
            params,
        )
        for row in _fetchall_dict(cur):
            out.append(
                {
                    "marketplace_id": str(row.get("marketplace_id") or "").strip() or None,
                    "pool_name": str(row.get("pool_name") or "overhead"),
                    "amount_pln": _f(row.get("amount_pln")),
                    "allocation_method": str(row.get("allocation_method") or "revenue_share").strip().lower(),
                    "confidence_pct": _f(row.get("confidence_pct"), 50.0),
                }
            )
    except Exception as exc:
        log.warning("profit_engine.overhead_manual_pools_failed", error=str(exc))
        _warnings_append("Overhead manual pools unavailable — NP margins may be overstated")

    # 2. Auto-detect NP costs from finance transactions using charge classifier
    try:
        params_ft: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        market_ft = ""
        if marketplace_id:
            market_ft = " AND COALESCE(ft.marketplace_id, o.marketplace_id) = ?"
            params_ft.append(marketplace_id)
        cur.execute(
            f"""
            SELECT
                COALESCE(ft.marketplace_id, o.marketplace_id) AS marketplace_id,
                ft.charge_type,
                ft.transaction_type,
                SUM(ISNULL(ft.amount_pln, 0)) AS amount_pln
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            LEFT JOIN dbo.acc_order o WITH (NOLOCK)
              ON o.amazon_order_id = ft.amazon_order_id
            WHERE ft.posted_date >= CAST(? AS DATE)
              AND ft.posted_date < DATEADD(day, 1, CAST(? AS DATE))
              {market_ft}
            GROUP BY COALESCE(ft.marketplace_id, o.marketplace_id),
                     ft.charge_type, ft.transaction_type
            """,
            params_ft,
        )
        # Collect NP-layer rows by bucket
        manual_names = {p["pool_name"] for p in out}
        np_agg: dict[tuple[str | None, str], float] = {}
        for row in cur.fetchall():
            classification = _classify_finance_charge(row[1], row[2])
            if classification is None or classification["layer"] != "np":
                continue
            bucket = classification["bucket"]
            if bucket in manual_names:
                continue  # manual pool takes precedence
            mkt = str(row[0] or "").strip() or None
            key = (mkt, bucket)
            np_agg[key] = np_agg.get(key, 0.0) + float(row[3] or 0) * classification["sign"]
        for (mkt, bucket), total in np_agg.items():
            if abs(total) < 0.01:
                continue
            out.append(
                {
                    "marketplace_id": mkt,
                    "pool_name": bucket,
                    "amount_pln": round(abs(total), 4),
                    "allocation_method": "revenue_share",
                    "confidence_pct": 40.0,
                }
            )
    except Exception as exc:
        log.warning("profit_engine.overhead_np_autodetect_failed", error=str(exc))
        _warnings_append("NP cost auto-detection unavailable — NP margins may be overstated")

    return out


_CM2_POOL_KEYS = ("storage", "aged", "removal", "liquidation", "refund_cost", "shipping_surcharge", "fba_inbound", "promo", "warehouse_loss", "amazon_other_fee")


def _allocate_fba_component_costs(
    products: list[dict[str, Any]],
    pools: dict[str, dict[str, float]],
    marketplace_weight_totals: dict[str, dict[str, float]] | None = None,
) -> None:
    """Allocate all CM2 cost pools to products proportionally by AFN units (or revenue)."""
    _KEY_MAP = {
        "storage": "fba_storage_fee_pln",
        "aged": "fba_aged_fee_pln",
        "removal": "fba_removal_fee_pln",
        "liquidation": "fba_liquidation_fee_pln",
        "refund_cost": "refund_finance_pln",
        "shipping_surcharge": "shipping_surcharge_pln",
        "fba_inbound": "fba_inbound_fee_pln",
        "promo": "promo_cost_pln",
        "warehouse_loss": "warehouse_loss_pln",
        "amazon_other_fee": "amazon_other_fee_pln",
    }

    def _zero_cm2(p: dict) -> None:
        for field in _KEY_MAP.values():
            p[field] = 0.0

    if not products or not pools:
        for p in products:
            _zero_cm2(p)
        return

    by_marketplace: dict[str, list[dict[str, Any]]] = defaultdict(list)
    global_rows: list[dict[str, Any]] = []
    for p in products:
        mkt = str(p.get("marketplace_id") or "")
        if mkt == "__ALL__":
            global_rows.append(p)
        else:
            by_marketplace[mkt].append(p)

    for p in products:
        _zero_cm2(p)

    totals_by_marketplace = marketplace_weight_totals or {}

    def _alloc(
        rows: list[dict[str, Any]],
        pool_values: dict[str, float],
        *,
        external_afn_total: float = 0.0,
        external_revenue_total: float = 0.0,
    ) -> None:
        if not rows:
            return
        rows_afn_total = sum(max(_f(r.get("afn_units")), 0.0) for r in rows)
        rows_rev_total = sum(max(_f(r.get("revenue_pln")), 0.0) for r in rows)
        afn_total = external_afn_total if external_afn_total > 0 else rows_afn_total
        rev_total = external_revenue_total if external_revenue_total > 0 else rows_rev_total
        # SF-08: Prefer revenue-weighted allocation (better proxy for FBA
        # dimensional pricing) instead of pure unit count.  Fall back to
        # AFN units, then equal split with a warning.
        for r in rows:
            if rev_total > 0:
                weight = max(_f(r.get("revenue_pln")), 0.0) / rev_total
            elif afn_total > 0:
                weight = max(_f(r.get("afn_units")), 0.0) / afn_total
            else:
                weight = 1.0 / len(rows)
                log.warning("profit_engine.fba_alloc_equal_split",
                            sku=r.get("group_key") or r.get("internal_sku"),
                            msg="No revenue or AFN data — equal split used for FBA pool allocation")
            for pool_key, field in _KEY_MAP.items():
                r[field] = round(_f(r[field]) + _f(pool_values.get(pool_key)) * weight, 4)

    for mkt, rows in by_marketplace.items():
        totals = totals_by_marketplace.get(mkt, {})
        _alloc(
            rows,
            pools.get(mkt, {}),
            external_afn_total=max(_f(totals.get("afn_units")), 0.0),
            external_revenue_total=max(_f(totals.get("revenue_pln")), 0.0),
        )

    if global_rows:
        total_pool = {k: 0.0 for k in _CM2_POOL_KEYS}
        for val in pools.values():
            for k in _CM2_POOL_KEYS:
                total_pool[k] += _f(val.get(k))
        global_afn_total = sum(max(_f(v.get("afn_units")), 0.0) for v in totals_by_marketplace.values())
        global_rev_total = sum(max(_f(v.get("revenue_pln")), 0.0) for v in totals_by_marketplace.values())
        _alloc(
            global_rows,
            total_pool,
            external_afn_total=global_afn_total,
            external_revenue_total=global_rev_total,
        )


def _allocate_overhead_costs(
    products: list[dict[str, Any]],
    pools: list[dict[str, Any]],
) -> None:
    for p in products:
        p["overhead_allocated_pln"] = 0.0
        p["overhead_allocation_method"] = "none"
        p["overhead_confidence_pct"] = 0.0
        p["_oh_conf_wsum"] = 0.0
        p["_oh_amount"] = 0.0
        p["_oh_methods"] = set()

    if not products or not pools:
        for p in products:
            p.pop("_oh_conf_wsum", None)
            p.pop("_oh_amount", None)
            p.pop("_oh_methods", None)
        return

    by_marketplace: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in products:
        by_marketplace[str(p.get("marketplace_id") or "")].append(p)

    for pool in pools:
        amount = _f(pool.get("amount_pln"))
        # SF-11: NULL/zero overhead amounts must not be silently skipped
        if pool.get("amount_pln") is None:
            log.warning("profit_engine.overhead_null_amount",
                        pool_name=pool.get("pool_name"),
                        marketplace_id=pool.get("marketplace_id"),
                        msg="Overhead pool has NULL amount — skipped. Check configuration.")
            continue
        if amount == 0:
            log.warning("profit_engine.overhead_zero_amount",
                        pool_name=pool.get("pool_name"),
                        marketplace_id=pool.get("marketplace_id"),
                        msg="Overhead pool amount is 0 — verify this is intentional.")
            continue
        mkt = str(pool.get("marketplace_id") or "").strip()
        applicable = by_marketplace.get(mkt) if mkt else products
        if not applicable:
            continue
        method = str(pool.get("allocation_method") or "revenue_share").strip().lower()
        if method not in {"revenue_share", "units_share", "orders_share"}:
            method = "revenue_share"
        if method == "units_share":
            weights = [max(_i(r.get("units")), 0) for r in applicable]
        elif method == "orders_share":
            weights = [max(_i(r.get("order_count")), 0) for r in applicable]
        else:
            weights = [max(_f(r.get("revenue_pln")), 0.0) for r in applicable]
        denom = float(sum(weights))
        if denom <= 0:
            weights = [1.0 for _ in applicable]
            denom = float(len(applicable))
        confidence = _f(pool.get("confidence_pct"), 50.0)
        for row, weight in zip(applicable, weights):
            ratio = float(weight) / denom if denom else 0.0
            alloc = round(amount * ratio, 4)
            row["overhead_allocated_pln"] = round(_f(row["overhead_allocated_pln"]) + alloc, 4)
            row["_oh_amount"] = round(_f(row["_oh_amount"]) + alloc, 4)
            row["_oh_conf_wsum"] = round(_f(row["_oh_conf_wsum"]) + alloc * confidence, 4)
            row["_oh_methods"].add(method)

    for p in products:
        oh_amount = _f(p.pop("_oh_amount", 0.0))
        oh_conf_wsum = _f(p.pop("_oh_conf_wsum", 0.0))
        methods = p.pop("_oh_methods", set()) or set()
        p["overhead_confidence_pct"] = round(oh_conf_wsum / oh_amount, 1) if oh_amount > 0 else 0.0
        if len(methods) == 1:
            p["overhead_allocation_method"] = next(iter(methods))
        elif len(methods) > 1:
            p["overhead_allocation_method"] = "mixed"
        else:
            p["overhead_allocation_method"] = "none"


# ---------------------------------------------------------------------------
# Finance data cache — separate from main query for performance
# ---------------------------------------------------------------------------

def _load_finance_lookup(
    cur,
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, tuple[int, float]]:
    """Pre-compute finance transaction aggregation for shipped orders in date range.

    Returns dict keyed by amazon_order_id → (fin_rows, shipping_charge_pln).
    Cached with 30-minute TTL since finance data changes infrequently.
    """
    cache_key = f"fin_lookup:{date_from}:{date_to}:{marketplace_id or 'all'}"
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return cached

    mkt_filter = ""
    params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
    if marketplace_id:
        mkt_filter = "AND o_f.marketplace_id = ?"
        params.append(marketplace_id)

    cur.execute(f"""
        SELECT
            ft.amazon_order_id,
            ft.marketplace_id,
            COUNT_BIG(1) AS fin_rows,
            SUM(
                CASE
                    WHEN ft.charge_type IN ('ShippingCharge', 'ShippingTax', 'ShippingDiscount') THEN
                        ISNULL(
                            ft.amount_pln,
                            ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")}
                        )
                    ELSE 0
                END
            ) AS shipping_charge_pln
        FROM dbo.acc_order o_f WITH (NOLOCK)
        JOIN dbo.acc_finance_transaction ft WITH (NOLOCK)
            ON ft.amazon_order_id = o_f.amazon_order_id
        WHERE o_f.status = 'Shipped'
          AND o_f.purchase_date >= CAST(? AS DATE)
          AND o_f.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
          {mkt_filter}
        GROUP BY ft.amazon_order_id, ft.marketplace_id
    """, params)

    result: dict[str, tuple[int, float]] = {}
    for row in cur.fetchall():
        key = f"{row[0]}|{row[1] or ''}"
        result[key] = (int(row[2] or 0), float(row[3] or 0.0))

    _result_cache_set(cache_key, result, ttl=1800)  # 30 min cache
    return result


# ---------------------------------------------------------------------------
# Product Profit Table — ASIN-first with optional SKU/Parent grouping
