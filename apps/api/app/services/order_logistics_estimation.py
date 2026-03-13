from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

_ESTIMATE_CALC_VERSION = "estimate_v1"
_TKL_CACHE_KEY = "default"
_MULTI_SKU_BUCKET_CAP = 5
_MULTI_UNIT_BUCKET_CAP = 8

_MIN_SAMPLE = 5
_BLEND_SAMPLE = 15
_STABLE_P75_RATIO_MAX = 1.35
_BLEND_TKL_WEIGHT = 0.60
_BLEND_OBS_WEIGHT = 0.40


@dataclass(frozen=True)
class OrderLineInput:
    sku: str
    internal_sku: str
    qty: int


@dataclass(frozen=True)
class ComponentEstimate:
    packages_count: int
    plan_total: float
    decision_total: float
    decision_rule: str
    observed_samples: int


def _connect():
    return connect_acc(autocommit=False, timeout=60)


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_internal_sku(value: Any) -> str:
    text = _norm_text(value)
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _normalize_country(country_code: str | None, marketplace_id: str | None = None) -> str:
    country = _norm_text(country_code).upper()
    if not country and marketplace_id:
        info = MARKETPLACE_REGISTRY.get(str(marketplace_id), {})
        country = _norm_text(info.get("code")).upper()
    if country == "UK":
        return "GB"
    return country or "UNK"


def _choose_bucket_payload(bucket_map: dict[int, dict[str, float]], qty: int) -> dict[str, float]:
    if not bucket_map:
        return {}
    if qty in bucket_map:
        return bucket_map[qty]
    if 1 in bucket_map:
        return bucket_map[1]
    nearest = sorted(bucket_map.keys(), key=lambda item: abs(int(item) - int(qty)))[0]
    return bucket_map.get(nearest, {})


def _suggest_pack_qty(bucket_map: dict[int, dict[str, float]]) -> tuple[int, str]:
    if not bucket_map:
        return 1, "default"

    baseline = bucket_map.get(1)
    if baseline and _f(baseline.get("median")) > 0:
        baseline_cost = _f(baseline.get("median"))
        suggested = 1
        for qty, payload in bucket_map.items():
            if int(qty) <= 1:
                continue
            samples = _i(payload.get("samples"))
            median = _f(payload.get("median"))
            if samples >= 3 and median > 0 and median <= baseline_cost * 1.15:
                suggested = max(suggested, int(qty))
        return max(1, suggested), "historical_bucket_rule"

    candidates = [(int(qty), _i(payload.get("samples"))) for qty, payload in bucket_map.items() if int(qty) > 1]
    if candidates:
        qty, samples = max(candidates, key=lambda item: (item[1], item[0]))
        if samples >= 5:
            return max(1, qty), "historical_mode"
    return 1, "default"


def _choose_multi_bucket_payload(
    bucket_map: dict[tuple[int, int], dict[str, float]],
    *,
    sku_bucket: int,
    unit_bucket: int,
) -> dict[str, float]:
    if not bucket_map:
        return {}
    key = (sku_bucket, unit_bucket)
    if key in bucket_map:
        return bucket_map[key]
    nearest = sorted(
        bucket_map.keys(),
        key=lambda item: (abs(int(item[0]) - int(sku_bucket)) + abs(int(item[1]) - int(unit_bucket)), -_i(bucket_map[item].get("samples"))),
    )[0]
    return bucket_map.get(nearest, {})


def _load_tkl_maps(cur) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, dict[str, Any]]]:
    cur.execute(
        """
SELECT row_type, internal_sku, country_code, cost, courier, source, pack_qty, [rank]
FROM dbo.acc_tkl_cache_rows WITH (NOLOCK)
WHERE cache_key = ?
        """,
        [_TKL_CACHE_KEY],
    )
    country_cost: dict[tuple[str, str], dict[str, Any]] = {}
    sku_cost: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        row_type = _norm_text(row[0]).lower()
        internal_sku = _norm_internal_sku(row[1])
        country_code = _normalize_country(row[2])
        payload = {
            "cost": round(_f(row[3]), 4),
            "courier": _norm_text(row[4]).lower() or None,
            "source": _norm_text(row[5]) or None,
            "pack_qty": max(0, _i(row[6])),
            "rank": _i(row[7]),
        }
        if payload["cost"] <= 0 or not internal_sku:
            continue
        if row_type == "country":
            country_cost[(internal_sku, country_code)] = payload
        elif row_type == "sku":
            sku_cost.setdefault(internal_sku, payload)
    return country_cost, sku_cost


def _load_single_sku_buckets(cur, *, lookback_from: date, lookback_to: date) -> dict[tuple[str, str], dict[int, dict[str, float]]]:
    cur.execute(
        """
WITH latest_actual AS (
    SELECT
        amazon_order_id,
        CAST(total_logistics_pln AS FLOAT) AS total_logistics_pln,
        ROW_NUMBER() OVER (
            PARTITION BY amazon_order_id
            ORDER BY calculated_at DESC
        ) AS rn
    FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
    WHERE actual_shipments_count > 0
      AND total_logistics_pln > 0
),
order_mix AS (
    SELECT
        o.id AS order_id,
        COALESCE(NULLIF(o.ship_country, ''), NULLIF(o.buyer_country, ''), NULLIF(m.code, ''), 'UNK') AS country_code,
        MAX(COALESCE(NULLIF(p.internal_sku, ''), '')) AS internal_sku,
        SUM(ISNULL(ol.quantity_ordered, 0)) AS qty_total,
        COUNT(DISTINCT COALESCE(NULLIF(p.internal_sku, ''), ISNULL(ol.sku, ''))) AS sku_count,
        MAX(la.total_logistics_pln) AS logistics_pln
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN latest_actual la
      ON la.amazon_order_id = o.amazon_order_id
     AND la.rn = 1
    JOIN dbo.acc_order_line ol WITH (NOLOCK)
      ON ol.order_id = o.id
    LEFT JOIN dbo.acc_product p WITH (NOLOCK)
      ON p.id = ol.product_id
    LEFT JOIN dbo.acc_marketplace m WITH (NOLOCK)
      ON m.id = o.marketplace_id
    WHERE o.status = 'Shipped'
      AND ISNULL(o.fulfillment_channel, '') = 'MFN'
      AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
      AND CAST(o.purchase_date AS DATE) >= ?
      AND CAST(o.purchase_date AS DATE) <= ?
    GROUP BY o.id, COALESCE(NULLIF(o.ship_country, ''), NULLIF(o.buyer_country, ''), NULLIF(m.code, ''), 'UNK')
),
single_raw AS (
    SELECT
        internal_sku,
        UPPER(country_code) AS country_code,
        CASE WHEN qty_total >= 8 THEN 8 ELSE qty_total END AS qty_bucket,
        logistics_pln AS metric_value
    FROM order_mix
    WHERE sku_count = 1
      AND qty_total > 0
      AND internal_sku <> ''
      AND logistics_pln > 0
),
single_stats AS (
    SELECT
        internal_sku,
        country_code,
        qty_bucket,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric_value)
            OVER (PARTITION BY internal_sku, country_code, qty_bucket) AS median_value,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value)
            OVER (PARTITION BY internal_sku, country_code, qty_bucket) AS p75_value,
        COUNT(*) OVER (PARTITION BY internal_sku, country_code, qty_bucket) AS samples,
        ROW_NUMBER() OVER (
            PARTITION BY internal_sku, country_code, qty_bucket
            ORDER BY qty_bucket
        ) AS rn
    FROM single_raw
)
SELECT
    internal_sku,
    country_code,
    qty_bucket,
    CAST(median_value AS FLOAT) AS median_value,
    CAST(p75_value AS FLOAT) AS p75_value,
    samples
FROM single_stats
WHERE rn = 1
        """,
        [lookback_from.isoformat(), lookback_to.isoformat()],
    )
    out: dict[tuple[str, str], dict[int, dict[str, float]]] = defaultdict(dict)
    for row in cur.fetchall():
        internal_sku = _norm_internal_sku(row[0])
        country_code = _normalize_country(row[1])
        qty_bucket = max(1, _i(row[2], 1))
        if not internal_sku:
            continue
        p75 = _f(row[4])
        out[(internal_sku, country_code)][qty_bucket] = {
            "median": round(_f(row[3]), 4),
            "p75": round(p75 if p75 > 0 else _f(row[3]), 4),
            "samples": _i(row[5]),
        }
    return out


def _load_multi_order_buckets(cur, *, lookback_from: date, lookback_to: date) -> dict[str, dict[tuple[int, int], dict[str, float]]]:
    cur.execute(
        """
WITH latest_actual AS (
    SELECT
        amazon_order_id,
        CAST(total_logistics_pln AS FLOAT) AS total_logistics_pln,
        ROW_NUMBER() OVER (
            PARTITION BY amazon_order_id
            ORDER BY calculated_at DESC
        ) AS rn
    FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
    WHERE actual_shipments_count > 0
      AND total_logistics_pln > 0
),
order_mix AS (
    SELECT
        o.id AS order_id,
        COALESCE(NULLIF(o.ship_country, ''), NULLIF(o.buyer_country, ''), NULLIF(m.code, ''), 'UNK') AS country_code,
        COUNT(DISTINCT COALESCE(NULLIF(p.internal_sku, ''), ISNULL(ol.sku, ''))) AS sku_count,
        SUM(ISNULL(ol.quantity_ordered, 0)) AS qty_total,
        MAX(la.total_logistics_pln) AS logistics_pln
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN latest_actual la
      ON la.amazon_order_id = o.amazon_order_id
     AND la.rn = 1
    JOIN dbo.acc_order_line ol WITH (NOLOCK)
      ON ol.order_id = o.id
    LEFT JOIN dbo.acc_product p WITH (NOLOCK)
      ON p.id = ol.product_id
    LEFT JOIN dbo.acc_marketplace m WITH (NOLOCK)
      ON m.id = o.marketplace_id
    WHERE o.status = 'Shipped'
      AND ISNULL(o.fulfillment_channel, '') = 'MFN'
      AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
      AND CAST(o.purchase_date AS DATE) >= ?
      AND CAST(o.purchase_date AS DATE) <= ?
    GROUP BY o.id, COALESCE(NULLIF(o.ship_country, ''), NULLIF(o.buyer_country, ''), NULLIF(m.code, ''), 'UNK')
),
multi_raw AS (
    SELECT
        UPPER(country_code) AS country_code,
        CASE WHEN sku_count >= 5 THEN 5 ELSE sku_count END AS sku_bucket,
        CASE WHEN qty_total >= 8 THEN 8 ELSE qty_total END AS unit_bucket,
        logistics_pln AS metric_value
    FROM order_mix
    WHERE sku_count > 1
      AND qty_total > 0
      AND logistics_pln > 0
),
multi_stats AS (
    SELECT
        country_code,
        sku_bucket,
        unit_bucket,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric_value)
            OVER (PARTITION BY country_code, sku_bucket, unit_bucket) AS median_value,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value)
            OVER (PARTITION BY country_code, sku_bucket, unit_bucket) AS p75_value,
        COUNT(*) OVER (PARTITION BY country_code, sku_bucket, unit_bucket) AS samples,
        ROW_NUMBER() OVER (
            PARTITION BY country_code, sku_bucket, unit_bucket
            ORDER BY sku_bucket, unit_bucket
        ) AS rn
    FROM multi_raw
)
SELECT
    country_code,
    sku_bucket,
    unit_bucket,
    CAST(median_value AS FLOAT) AS median_value,
    CAST(p75_value AS FLOAT) AS p75_value,
    samples
FROM multi_stats
WHERE rn = 1
        """,
        [lookback_from.isoformat(), lookback_to.isoformat()],
    )
    out: dict[str, dict[tuple[int, int], dict[str, float]]] = defaultdict(dict)
    global_out: dict[tuple[int, int], dict[str, float]] = {}
    rows = cur.fetchall()
    for row in rows:
        country_code = _normalize_country(row[0])
        sku_bucket = max(2, _i(row[1], 2))
        unit_bucket = max(2, _i(row[2], 2))
        p75 = _f(row[4])
        payload = {
            "median": round(_f(row[3]), 4),
            "p75": round(p75 if p75 > 0 else _f(row[3]), 4),
            "samples": _i(row[5]),
        }
        out[country_code][(sku_bucket, unit_bucket)] = payload
        existing = global_out.get((sku_bucket, unit_bucket))
        if existing is None or payload["samples"] > existing["samples"]:
            global_out[(sku_bucket, unit_bucket)] = payload
    out["ALL"] = global_out
    return out


def _load_target_orders(
    cur,
    *,
    purchase_from: date | None,
    purchase_to: date | None,
    limit_orders: int,
    refresh_existing: bool,
) -> list[dict[str, Any]]:
    where = [
        "o.status = 'Shipped'",
        "ISNULL(o.fulfillment_channel, '') = 'MFN'",
        "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
        "NOT EXISTS (SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK) WHERE f.amazon_order_id = o.amazon_order_id AND f.actual_shipments_count > 0)",
        "ISNULL(o.logistics_pln, 0) <= 0",
    ]
    params: list[Any] = []
    if not refresh_existing:
        where.append(
            "NOT EXISTS (SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK) WHERE f.amazon_order_id = o.amazon_order_id AND f.calc_version = ?)"
        )
        params.append(_ESTIMATE_CALC_VERSION)
    if purchase_from:
        where.append("CAST(o.purchase_date AS DATE) >= ?")
        params.append(purchase_from.isoformat())
    if purchase_to:
        where.append("CAST(o.purchase_date AS DATE) <= ?")
        params.append(purchase_to.isoformat())

    cur.execute(
        f"""
WITH target_orders AS (
    SELECT TOP {int(limit_orders)}
        CAST(o.id AS NVARCHAR(40)) AS acc_order_id,
        o.amazon_order_id,
        o.marketplace_id,
        COALESCE(NULLIF(o.ship_country, ''), NULLIF(o.buyer_country, ''), NULLIF(m.code, ''), 'UNK') AS country_code,
        CAST(o.purchase_date AS DATE) AS purchase_date
    FROM dbo.acc_order o WITH (NOLOCK)
    LEFT JOIN dbo.acc_marketplace m WITH (NOLOCK)
      ON m.id = o.marketplace_id
    WHERE {' AND '.join(where)}
    ORDER BY o.purchase_date ASC, o.amazon_order_id ASC
)
SELECT
    t.acc_order_id,
    t.amazon_order_id,
    t.marketplace_id,
    t.country_code,
    t.purchase_date,
    ol.sku,
    COALESCE(NULLIF(p.internal_sku, ''), '') AS internal_sku,
    SUM(ISNULL(ol.quantity_ordered, 0)) AS qty_total
FROM target_orders t
JOIN dbo.acc_order_line ol WITH (NOLOCK)
  ON ol.order_id = CAST(t.acc_order_id AS UNIQUEIDENTIFIER)
LEFT JOIN dbo.acc_product p WITH (NOLOCK)
  ON p.id = ol.product_id
GROUP BY
    t.acc_order_id,
    t.amazon_order_id,
    t.marketplace_id,
    t.country_code,
    t.purchase_date,
    ol.sku,
    COALESCE(NULLIF(p.internal_sku, ''), '')
ORDER BY t.purchase_date ASC, t.amazon_order_id ASC
        """,
        params,
    )
    orders: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        amazon_order_id = _norm_text(row[1])
        if not amazon_order_id:
            continue
        bucket = orders.setdefault(
            amazon_order_id,
            {
                "acc_order_id": _norm_text(row[0]) or None,
                "amazon_order_id": amazon_order_id,
                "marketplace_id": _norm_text(row[2]) or None,
                "country_code": _normalize_country(row[3], _norm_text(row[2]) or None),
                "purchase_date": row[4],
                "lines": [],
            },
        )
        qty_total = max(1, _i(row[7], 1))
        bucket["lines"].append(
            OrderLineInput(
                sku=_norm_text(row[5]),
                internal_sku=_norm_internal_sku(row[6]),
                qty=qty_total,
            )
        )
    return list(orders.values())


def _estimate_component(
    *,
    internal_sku: str,
    qty: int,
    country_code: str,
    country_tkl: dict[tuple[str, str], dict[str, Any]],
    sku_tkl: dict[str, dict[str, Any]],
    observed_buckets: dict[tuple[str, str], dict[int, dict[str, float]]],
) -> ComponentEstimate:
    bucket_map = observed_buckets.get((internal_sku, country_code), {})
    if not bucket_map and country_code != "UNK":
        bucket_map = observed_buckets.get((internal_sku, "UNK"), {})

    hist_pack_qty, _ = _suggest_pack_qty(bucket_map)
    suggested_pack_qty = max(1, hist_pack_qty)

    tkl_country = country_tkl.get((internal_sku, country_code))
    tkl_sku = sku_tkl.get(internal_sku)
    if tkl_country and _i(tkl_country.get("pack_qty")) > 0:
        suggested_pack_qty = max(1, _i(tkl_country.get("pack_qty")))
    elif tkl_sku and _i(tkl_sku.get("pack_qty")) > 0:
        suggested_pack_qty = max(1, _i(tkl_sku.get("pack_qty")))

    packages_count = max(1, math.ceil(max(1, qty) / max(1, suggested_pack_qty)))

    plan_base = 0.0
    if tkl_country and _f(tkl_country.get("cost")) > 0:
        plan_base = _f(tkl_country.get("cost"))
    elif tkl_sku and _f(tkl_sku.get("cost")) > 0:
        plan_base = _f(tkl_sku.get("cost"))

    observed_payload = _choose_bucket_payload(bucket_map, min(max(1, qty), max(1, hist_pack_qty)))
    if not observed_payload:
        observed_payload = _choose_bucket_payload(bucket_map, 1)
    observed_base = _f(observed_payload.get("median"))
    observed_p75 = _f(observed_payload.get("p75"))
    if observed_p75 <= 0 and observed_base > 0:
        observed_p75 = observed_base
    observed_samples = _i(observed_payload.get("samples"))

    low_sample = observed_samples < _MIN_SAMPLE
    stable_sample = (
        observed_samples >= _BLEND_SAMPLE
        and observed_base > 0
        and observed_p75 > 0
        and observed_p75 <= observed_base * _STABLE_P75_RATIO_MAX
    )
    safe_max = max(plan_base, observed_p75 if observed_p75 > 0 else observed_base)

    if plan_base > 0 and (low_sample or observed_base <= 0):
        decision_base = plan_base
        decision_rule = "tkl_low_sample"
    elif plan_base > 0 and stable_sample:
        blend = (_BLEND_TKL_WEIGHT * plan_base) + (_BLEND_OBS_WEIGHT * observed_base)
        decision_base = max(blend, safe_max)
        decision_rule = "blend_safe_max"
    elif plan_base > 0 and safe_max > 0:
        decision_base = safe_max
        decision_rule = "safe_max"
    elif observed_base > 0:
        decision_base = observed_p75 if observed_p75 > 0 else observed_base
        decision_rule = "observed_only"
    else:
        decision_base = 0.0
        decision_rule = "missing"

    return ComponentEstimate(
        packages_count=packages_count,
        plan_total=round(plan_base * packages_count, 2),
        decision_total=round(decision_base * packages_count, 2),
        decision_rule=decision_rule,
        observed_samples=observed_samples,
    )


def _estimate_order(
    *,
    lines: list[OrderLineInput],
    country_code: str,
    country_tkl: dict[tuple[str, str], dict[str, Any]],
    sku_tkl: dict[str, dict[str, Any]],
    observed_buckets: dict[tuple[str, str], dict[int, dict[str, float]]],
    multi_buckets: dict[str, dict[tuple[int, int], dict[str, float]]],
) -> tuple[float, int, str]:
    components = [
        _estimate_component(
            internal_sku=line.internal_sku,
            qty=line.qty,
            country_code=country_code,
            country_tkl=country_tkl,
            sku_tkl=sku_tkl,
            observed_buckets=observed_buckets,
        )
        for line in lines
        if line.internal_sku
    ]
    if not components:
        return 0.0, 0, "missing"

    if len(lines) == 1:
        component = components[0]
        return component.decision_total, component.packages_count, f"single:{component.decision_rule}"

    sku_bucket = min(_MULTI_SKU_BUCKET_CAP, max(2, len(lines)))
    unit_bucket = min(_MULTI_UNIT_BUCKET_CAP, max(2, sum(max(1, line.qty) for line in lines)))
    multi_payload = _choose_multi_bucket_payload(multi_buckets.get(country_code, {}) or multi_buckets.get("ALL", {}), sku_bucket=sku_bucket, unit_bucket=unit_bucket)
    observed_multi = _f(multi_payload.get("p75"))
    if observed_multi <= 0:
        observed_multi = _f(multi_payload.get("median"))
    observed_samples = _i(multi_payload.get("samples"))

    max_line_total = max(component.decision_total for component in components)
    sum_line_total = sum(component.decision_total for component in components)
    sum_plan_total = sum(component.plan_total for component in components)

    if observed_multi > 0 and observed_samples >= _MIN_SAMPLE:
        cap = observed_multi * 1.35
        blended = min(sum_line_total, cap) if sum_line_total > 0 else observed_multi
        estimate_total = max(observed_multi, max_line_total, blended)
        decision_rule = "multi_observed_bucket"
    elif sum_line_total > 0:
        softened = max(max_line_total, round(sum_line_total * 0.75, 2))
        if sum_plan_total > 0:
            softened = max(softened, round(sum_plan_total * 0.65, 2))
        estimate_total = softened
        decision_rule = "multi_line_fallback"
    else:
        estimate_total = 0.0
        decision_rule = "missing"

    packages_count = max(1, max(component.packages_count for component in components))
    return round(estimate_total, 2), packages_count, decision_rule


def _upsert_estimated_order_fact(
    cur,
    *,
    amazon_order_id: str,
    acc_order_id: str | None,
    total_logistics_pln: float,
    estimated_shipments_count: int,
    source_system: str,
) -> None:
    cur.execute(
        """
SELECT amazon_order_id
FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
WHERE amazon_order_id = ?
  AND calc_version = ?
        """,
        [amazon_order_id, _ESTIMATE_CALC_VERSION],
    )
    row = cur.fetchone()
    params = [
        acc_order_id,
        acc_order_id,
        acc_order_id,
        max(1, estimated_shipments_count),
        0,
        0,
        max(1, estimated_shipments_count),
        round(max(total_logistics_pln, 0.0), 4),
        None,
        _ESTIMATE_CALC_VERSION,
        source_system,
        amazon_order_id,
        _ESTIMATE_CALC_VERSION,
    ]
    if row:
        cur.execute(
            """
UPDATE dbo.acc_order_logistics_fact
SET acc_order_id = CASE WHEN ? IS NULL OR ? = '' THEN acc_order_id ELSE CAST(? AS UNIQUEIDENTIFIER) END,
    shipments_count = ?,
    delivered_shipments_count = ?,
    actual_shipments_count = ?,
    estimated_shipments_count = ?,
    total_logistics_pln = ?,
    last_delivery_at = ?,
    calc_version = ?,
    source_system = ?,
    calculated_at = SYSUTCDATETIME()
WHERE amazon_order_id = ?
  AND calc_version = ?
            """,
            params,
        )
        return

    cur.execute(
        """
INSERT INTO dbo.acc_order_logistics_fact (
    amazon_order_id, acc_order_id, shipments_count, delivered_shipments_count,
    actual_shipments_count, estimated_shipments_count, total_logistics_pln,
    last_delivery_at, calc_version, source_system, calculated_at
)
VALUES (
    ?,
    CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CAST(? AS UNIQUEIDENTIFIER) END,
    ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME()
)
        """,
        [
            amazon_order_id,
            acc_order_id,
            acc_order_id,
            acc_order_id,
            max(1, estimated_shipments_count),
            0,
            0,
            max(1, estimated_shipments_count),
            round(max(total_logistics_pln, 0.0), 4),
            None,
            _ESTIMATE_CALC_VERSION,
            source_system,
        ],
    )


def estimate_order_logistics_for_open_orders(
    *,
    purchase_from: date | None,
    purchase_to: date | None,
    lookback_days: int = 180,
    limit_orders: int = 50000,
    refresh_existing: bool = False,
) -> dict[str, Any]:
    today = date.today()
    history_from = today.fromordinal(today.toordinal() - max(30, int(lookback_days or 180)))

    stats = {
        "orders_selected": 0,
        "orders_estimated": 0,
        "orders_missing_inputs": 0,
        "orders_single_sku": 0,
        "orders_multi_sku": 0,
        "tkl_country_pairs": 0,
        "tkl_sku_rows": 0,
        "single_bucket_keys": 0,
        "multi_bucket_keys": 0,
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        country_tkl, sku_tkl = _load_tkl_maps(cur)
        observed_buckets = _load_single_sku_buckets(cur, lookback_from=history_from, lookback_to=today)
        multi_buckets = _load_multi_order_buckets(cur, lookback_from=history_from, lookback_to=today)
        target_orders = _load_target_orders(
            cur,
            purchase_from=purchase_from,
            purchase_to=purchase_to,
            limit_orders=max(1, int(limit_orders)),
            refresh_existing=bool(refresh_existing),
        )

        stats["orders_selected"] = len(target_orders)
        stats["tkl_country_pairs"] = len(country_tkl)
        stats["tkl_sku_rows"] = len(sku_tkl)
        stats["single_bucket_keys"] = len(observed_buckets)
        stats["multi_bucket_keys"] = sum(len(item) for item in multi_buckets.values())

        for idx, order in enumerate(target_orders, start=1):
            lines = [line for line in order["lines"] if line.internal_sku]
            if not lines:
                stats["orders_missing_inputs"] += 1
                continue

            estimate_total, packages_count, decision_rule = _estimate_order(
                lines=lines,
                country_code=_normalize_country(order.get("country_code"), order.get("marketplace_id")),
                country_tkl=country_tkl,
                sku_tkl=sku_tkl,
                observed_buckets=observed_buckets,
                multi_buckets=multi_buckets,
            )
            if estimate_total <= 0:
                stats["orders_missing_inputs"] += 1
                continue

            if len(lines) == 1:
                stats["orders_single_sku"] += 1
                source_system = "order_estimate_tkl_hist_single"
            else:
                stats["orders_multi_sku"] += 1
                source_system = "order_estimate_tkl_hist_multi"

            _upsert_estimated_order_fact(
                cur,
                amazon_order_id=str(order["amazon_order_id"]),
                acc_order_id=order.get("acc_order_id"),
                total_logistics_pln=estimate_total,
                estimated_shipments_count=max(1, packages_count),
                source_system=f"{source_system}:{decision_rule}",
            )
            stats["orders_estimated"] += 1

            if idx % 250 == 0:
                conn.commit()

        conn.commit()
        return stats
    finally:
        conn.close()
