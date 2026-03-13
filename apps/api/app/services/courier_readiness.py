from __future__ import annotations

from datetime import date, timedelta
import time
from typing import Any

from app.core.db_connection import connect_acc
from app.connectors.mssql import list_jobs
from app.services.courier_order_universe_pipeline import (
    _carrier_predicate,
    _coverage_snapshot,
    _distribution_order_carrier_predicate,
)


def _dynamic_default_months() -> list[str]:
    """Return last 3 closed months dynamically."""
    today = date.today()
    months = []
    for offset in range(3, 0, -1):
        m = today.month - offset
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        months.append(f"{y}-{m:02d}")
    return months


_READINESS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _cache_key(months: list[str], carriers: list[str]) -> str:
    return f"{','.join(months)}|{','.join(carriers)}"


def _readiness_cache_get(key: str) -> dict[str, Any] | None:
    row = _READINESS_CACHE.get(key)
    if not row:
        return None
    exp, value = row
    if time.monotonic() > exp:
        _READINESS_CACHE.pop(key, None)
        return None
    return value


def _readiness_cache_set(key: str, value: dict[str, Any], ttl_sec: int = 180) -> None:
    _READINESS_CACHE[key] = (time.monotonic() + ttl_sec, value)


def _month_start(token: str) -> date:
    year_str, month_str = token.split("-", 1)
    return date(int(year_str), int(month_str), 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _billing_period_variants(token: str) -> list[str]:
    raw = str(token or "").strip()
    if not raw:
        raise ValueError("billing_period cannot be empty")
    variants: list[str] = []
    for candidate in (raw, raw.replace(".", "-"), raw.replace("-", ".")):
        value = candidate.strip()
        if value and value not in variants:
            variants.append(value)
    return variants


def get_courier_readiness_snapshot(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
) -> dict[str, Any]:
    _default = _dynamic_default_months()
    months_norm = [str(item).strip() for item in (months or _default) if str(item).strip()]
    carriers_norm = [str(item).strip().upper() for item in (carriers or ["DHL", "GLS"]) if str(item).strip()]
    cache_key = _cache_key(months_norm, carriers_norm)
    cached = _readiness_cache_get(cache_key)
    if cached is not None:
        return cached
    for carrier in carriers_norm:
        if carrier not in {"DHL", "GLS"}:
            raise ValueError(f"Unsupported carrier '{carrier}'")

    matrix: dict[str, dict[str, Any]] = {}
    total_scopes = 0
    go_scopes = 0
    for month_token in months_norm:
        start = _month_start(month_token)
        end = _next_month(start)
        by_carrier: dict[str, Any] = {}
        for carrier in carriers_norm:
            coverage = _coverage_snapshot(
                carrier=carrier,
                purchase_from=start,
                purchase_to_exclusive=end,
            )
            universe = int(coverage.get("orders_universe", 0) or 0)
            with_fact = int(coverage.get("orders_with_fact", 0) or 0)
            go = bool(universe > 0 and with_fact == universe)
            by_carrier[carrier] = {
                **coverage,
                "go_no_go": "GO" if go else "NO_GO",
            }
            total_scopes += 1
            if go:
                go_scopes += 1
        matrix[month_token] = by_carrier

    jobs = list_jobs(job_type="courier_order_universe_linking", page=1, page_size=10).get("items", [])
    running_jobs = [item for item in jobs if str(item.get("status") or "").lower() == "running"]
    result = {
        "overall_go_no_go": "GO" if total_scopes > 0 and go_scopes == total_scopes else "NO_GO",
        "summary": {
            "scopes_total": total_scopes,
            "scopes_go": go_scopes,
            "scopes_no_go": max(0, total_scopes - go_scopes),
            "running_jobs": len(running_jobs),
        },
        "matrix": matrix,
        "latest_jobs": jobs,
    }
    _readiness_cache_set(cache_key, result, ttl_sec=180)
    return result


def _shipment_month_coverage(*, carrier: str, month_start: date, month_end: date) -> dict[str, int | float]:
    conn = connect_acc(timeout=300)
    try:
        cur = conn.cursor()
        cur.execute(
            """
WITH scope_shipments AS (
    SELECT s.id
    FROM dbo.acc_shipment s WITH (NOLOCK)
    WHERE s.carrier = ?
      AND CAST(
            COALESCE(
                s.ship_date,
                CAST(s.created_at_carrier AS DATE),
                CAST(s.first_seen_at AS DATE)
            ) AS DATE
          ) >= ?
      AND CAST(
            COALESCE(
                s.ship_date,
                CAST(s.created_at_carrier AS DATE),
                CAST(s.first_seen_at AS DATE)
            ) AS DATE
          ) < ?
),
linked_shipments AS (
    SELECT DISTINCT l.shipment_id
    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
    JOIN scope_shipments ss
      ON ss.id = l.shipment_id
    WHERE l.is_primary = 1
),
actual_cost_shipments AS (
    SELECT DISTINCT c.shipment_id
    FROM dbo.acc_shipment_cost c WITH (NOLOCK)
    JOIN scope_shipments ss
      ON ss.id = c.shipment_id
    WHERE c.is_estimated = 0
)
SELECT
    COUNT_BIG(*) AS shipments_total,
    SUM(CASE WHEN ls.shipment_id IS NOT NULL THEN 1 ELSE 0 END) AS linked_shipments,
    SUM(CASE WHEN ac.shipment_id IS NOT NULL THEN 1 ELSE 0 END) AS costed_shipments_actual,
    SUM(CASE WHEN ls.shipment_id IS NOT NULL AND ac.shipment_id IS NOT NULL THEN 1 ELSE 0 END) AS linked_and_costed_shipments
FROM scope_shipments ss
LEFT JOIN linked_shipments ls
  ON ls.shipment_id = ss.id
LEFT JOIN actual_cost_shipments ac
  ON ac.shipment_id = ss.id
            """,
            (carrier, month_start.isoformat(), month_end.isoformat()),
        )
        row = cur.fetchone() or (0, 0, 0, 0)
    finally:
        conn.close()

    shipments_total = int(row[0] or 0)
    linked_shipments = int(row[1] or 0)
    costed_shipments_actual = int(row[2] or 0)
    linked_and_costed_shipments = int(row[3] or 0)
    return {
        "shipments_total": shipments_total,
        "linked_shipments": linked_shipments,
        "costed_shipments_actual": costed_shipments_actual,
        "linked_and_costed_shipments": linked_and_costed_shipments,
        "link_coverage_pct": round((linked_shipments / shipments_total) * 100, 2) if shipments_total else 100.0,
        "cost_coverage_pct": round((costed_shipments_actual / shipments_total) * 100, 2) if shipments_total else 100.0,
        "linked_among_costed_pct": round((linked_and_costed_shipments / costed_shipments_actual) * 100, 2)
        if costed_shipments_actual
        else 100.0,
    }


def _billing_period_coverage(*, carrier: str, billing_period: str) -> dict[str, int | float]:
    billing_period_tokens = _billing_period_variants(billing_period)
    placeholders = ",".join("?" for _ in billing_period_tokens)
    conn = connect_acc(timeout=300)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
WITH billed_shipments AS (
    SELECT DISTINCT c.shipment_id
    FROM dbo.acc_shipment_cost c WITH (NOLOCK)
    JOIN dbo.acc_shipment s WITH (NOLOCK)
      ON s.id = c.shipment_id
    WHERE s.carrier = ?
      AND c.is_estimated = 0
      AND c.billing_period IN ({placeholders})
),
linked_billed_shipments AS (
    SELECT DISTINCT l.shipment_id
    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
    JOIN billed_shipments bs
      ON bs.shipment_id = l.shipment_id
    WHERE l.is_primary = 1
)
SELECT
    COUNT_BIG(*) AS billed_shipments_total,
    SUM(CASE WHEN lbs.shipment_id IS NOT NULL THEN 1 ELSE 0 END) AS billed_shipments_linked
FROM billed_shipments bs
LEFT JOIN linked_billed_shipments lbs
  ON lbs.shipment_id = bs.shipment_id
            """,
            [carrier, *billing_period_tokens],
        )
        row = cur.fetchone() or (0, 0)
    finally:
        conn.close()

    billed_shipments_total = int(row[0] or 0)
    billed_shipments_linked = int(row[1] or 0)
    return {
        "billed_shipments_total": billed_shipments_total,
        "billed_shipments_linked": billed_shipments_linked,
        "link_coverage_pct": round((billed_shipments_linked / billed_shipments_total) * 100, 2)
        if billed_shipments_total
        else 100.0,
    }


def get_courier_coverage_matrix(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
) -> dict[str, Any]:
    _default = _dynamic_default_months()
    months_norm = [str(item).strip() for item in (months or _default) if str(item).strip()]
    carriers_norm = [str(item).strip().upper() for item in (carriers or ["DHL", "GLS"]) if str(item).strip()]
    for carrier in carriers_norm:
        if carrier not in {"DHL", "GLS"}:
            raise ValueError(f"Unsupported carrier '{carrier}'")

    matrix: dict[str, dict[str, Any]] = {}
    for month_token in months_norm:
        start = _month_start(month_token)
        end = _next_month(start)
        by_carrier: dict[str, Any] = {}
        for carrier in carriers_norm:
            purchase_month = _coverage_snapshot(
                carrier=carrier,
                purchase_from=start,
                purchase_to_exclusive=end,
            )
            shipment_month = _shipment_month_coverage(
                carrier=carrier,
                month_start=start,
                month_end=end,
            )
            billing_period = _billing_period_coverage(
                carrier=carrier,
                billing_period=month_token,
            )
            by_carrier[carrier] = {
                "purchase_month": purchase_month,
                "amazon_order_coverage": purchase_month,
                "shipment_month": shipment_month,
                "all_shipments_coverage": shipment_month,
                "billing_period": billing_period,
                "billed_shipments_coverage": billing_period,
            }
        matrix[month_token] = by_carrier

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "notes": {
            "purchase_month": "Denominator: order universe by acc_order.purchase_date.",
            "amazon_order_coverage": "Alias for purchase_month. This is Amazon order coverage for the carrier-attributable order universe.",
            "shipment_month": (
                "Denominator: shipments by acc_shipment ship_date/created_at; "
                "cost coverage counts only is_estimated=0."
            ),
            "all_shipments_coverage": "Alias for shipment_month. This is shipment-level coverage for all carrier shipments, not only Amazon-linked ones.",
            "billing_period": (
                "Denominator: shipments with actual courier cost where "
                "acc_shipment_cost.billing_period == month token."
            ),
            "billed_shipments_coverage": "Alias for billing_period. This is shipment-level linkage for billed shipments only.",
        },
        "matrix": matrix,
    }


def _order_level_gap_breakdown(*, carrier: str, month_start: date, month_end_exclusive: date) -> dict[str, int]:
    carrier_key = carrier.strip().upper()
    if carrier_key not in {"DHL", "GLS"}:
        raise ValueError(f"Unsupported carrier '{carrier}'")

    package_carrier_pred = _carrier_predicate("p", carrier_key)
    package_dis_carrier_pred = _carrier_predicate("dp", carrier_key)
    package_do_carrier_pred = _distribution_order_carrier_predicate("sdo", carrier_key)
    month_start_sql = month_start.isoformat()
    month_end_exclusive_sql = month_end_exclusive.isoformat()

    conn = connect_acc(timeout=300)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
IF OBJECT_ID('tempdb..#scope_orders') IS NOT NULL DROP TABLE #scope_orders;
IF OBJECT_ID('tempdb..#scope_bl_orders') IS NOT NULL DROP TABLE #scope_bl_orders;
IF OBJECT_ID('tempdb..#scope_distribution_orders') IS NOT NULL DROP TABLE #scope_distribution_orders;
IF OBJECT_ID('tempdb..#carrier_orders_raw') IS NOT NULL DROP TABLE #carrier_orders_raw;

CREATE TABLE #scope_orders (
    amazon_order_id NVARCHAR(80) NOT NULL PRIMARY KEY
);

CREATE TABLE #scope_bl_orders (
    amazon_order_id NVARCHAR(80) NOT NULL,
    bl_order_id BIGINT NOT NULL
);

CREATE INDEX IX_scope_bl_orders_bl_order_id ON #scope_bl_orders(bl_order_id);
CREATE INDEX IX_scope_bl_orders_amazon_order_id ON #scope_bl_orders(amazon_order_id);

CREATE TABLE #scope_distribution_orders (
    amazon_order_id NVARCHAR(80) NOT NULL,
    distribution_order_id BIGINT NOT NULL,
    resolved_bl_order_id BIGINT NOT NULL,
    delivery_method NVARCHAR(255) NULL,
    delivery_package_module NVARCHAR(255) NULL
);

CREATE INDEX IX_scope_distribution_orders_distribution_order_id
    ON #scope_distribution_orders(distribution_order_id, resolved_bl_order_id);
CREATE INDEX IX_scope_distribution_orders_amazon_order_id
    ON #scope_distribution_orders(amazon_order_id);

CREATE TABLE #carrier_orders_raw (
    amazon_order_id NVARCHAR(80) NOT NULL
);

CREATE INDEX IX_carrier_orders_raw_amazon_order_id ON #carrier_orders_raw(amazon_order_id);

INSERT INTO #scope_orders (amazon_order_id)
SELECT DISTINCT o.amazon_order_id
FROM dbo.acc_order o WITH (NOLOCK)
WHERE o.fulfillment_channel = 'MFN'
  AND o.amazon_order_id IS NOT NULL
  AND LTRIM(RTRIM(o.amazon_order_id)) <> ''
  AND CAST(o.purchase_date AS DATE) >= '{month_start_sql}'
  AND CAST(o.purchase_date AS DATE) < '{month_end_exclusive_sql}';

INSERT INTO #scope_bl_orders (amazon_order_id, bl_order_id)
SELECT DISTINCT
    so.amazon_order_id,
    CAST(bo.order_id AS BIGINT) AS bl_order_id
FROM #scope_orders so
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.external_order_id = so.amazon_order_id
WHERE bo.order_id IS NOT NULL;

INSERT INTO #scope_distribution_orders (
    amazon_order_id, distribution_order_id, resolved_bl_order_id, delivery_method, delivery_package_module
)
SELECT DISTINCT
    so.amazon_order_id,
    CAST(dco.order_id AS BIGINT) AS distribution_order_id,
    CAST(COALESCE(dm.holding_order_id, dco.order_id) AS BIGINT) AS resolved_bl_order_id,
    dco.delivery_method,
    dco.delivery_package_module
FROM #scope_orders so
JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
  ON dco.external_order_id = so.amazon_order_id
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = dco.order_id
WHERE dco.order_id IS NOT NULL;

INSERT INTO #carrier_orders_raw (amazon_order_id)
SELECT DISTINCT sbo.amazon_order_id
FROM #scope_bl_orders sbo
JOIN dbo.acc_cache_packages p WITH (NOLOCK)
  ON p.order_id = sbo.bl_order_id
WHERE {package_carrier_pred};

INSERT INTO #carrier_orders_raw (amazon_order_id)
SELECT DISTINCT sbo.amazon_order_id
FROM #scope_bl_orders sbo
JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.holding_order_id = sbo.bl_order_id
JOIN dbo.acc_cache_packages p WITH (NOLOCK)
  ON p.order_id = dm.dis_order_id
WHERE {package_carrier_pred};

INSERT INTO #carrier_orders_raw (amazon_order_id)
SELECT DISTINCT sdo.amazon_order_id
FROM #scope_distribution_orders sdo
LEFT JOIN dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
  ON dp.order_id = sdo.distribution_order_id
WHERE ({package_dis_carrier_pred} OR {package_do_carrier_pred});

INSERT INTO #carrier_orders_raw (amazon_order_id)
SELECT DISTINCT r.source_amazon_order_id
FROM dbo.acc_order_courier_relation r WITH (NOLOCK)
JOIN #scope_orders so
  ON so.amazon_order_id = r.source_amazon_order_id
WHERE r.carrier = '{carrier_key}'
  AND r.is_strong = 1
  AND r.source_purchase_date >= '{month_start_sql}'
  AND r.source_purchase_date < '{month_end_exclusive_sql}';

WITH carrier_orders AS (
    SELECT DISTINCT amazon_order_id
    FROM #carrier_orders_raw
),
linked_shipments AS (
    SELECT DISTINCT
        l.amazon_order_id,
        l.shipment_id
    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
    JOIN dbo.acc_shipment s WITH (NOLOCK)
      ON s.id = l.shipment_id
    JOIN carrier_orders co
      ON co.amazon_order_id = l.amazon_order_id
    WHERE l.is_primary = 1
      AND l.amazon_order_id IS NOT NULL
      AND s.carrier = '{carrier_key}'
),
cost_flags AS (
    SELECT
        ls.amazon_order_id,
        MAX(CASE WHEN c.is_estimated = 0 THEN 1 ELSE 0 END) AS has_actual_cost,
        MAX(CASE WHEN c.is_estimated = 1 THEN 1 ELSE 0 END) AS has_estimated_cost
    FROM linked_shipments ls
    LEFT JOIN dbo.acc_shipment_cost c WITH (NOLOCK)
      ON c.shipment_id = ls.shipment_id
    GROUP BY ls.amazon_order_id
),
order_flags AS (
    SELECT
        co.amazon_order_id,
        CASE WHEN ls.amazon_order_id IS NULL THEN 0 ELSE 1 END AS has_primary_link,
        ISNULL(cf.has_actual_cost, 0) AS has_actual_cost,
        ISNULL(cf.has_estimated_cost, 0) AS has_estimated_cost
    FROM carrier_orders co
    LEFT JOIN (
        SELECT DISTINCT amazon_order_id
    FROM linked_shipments
    ) ls
      ON ls.amazon_order_id = co.amazon_order_id
    LEFT JOIN cost_flags cf
      ON cf.amazon_order_id = co.amazon_order_id
)
SELECT
    COUNT_BIG(*) AS orders_universe,
    SUM(CASE WHEN has_primary_link = 1 THEN 1 ELSE 0 END) AS orders_with_primary_link,
    SUM(CASE WHEN has_primary_link = 0 THEN 1 ELSE 0 END) AS orders_without_primary_link,
    SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS orders_with_actual_cost,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS orders_with_estimated_only,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 0 AND has_estimated_cost = 0 THEN 1 ELSE 0 END) AS orders_linked_but_no_cost,
    SUM(CASE WHEN has_actual_cost = 0 THEN 1 ELSE 0 END) AS orders_missing_actual_cost
FROM order_flags
            """
        )
        row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0)
    finally:
        conn.close()

    return {
        "orders_universe": int(row[0] or 0),
        "orders_with_primary_link": int(row[1] or 0),
        "orders_without_primary_link": int(row[2] or 0),
        "orders_with_actual_cost": int(row[3] or 0),
        "orders_with_estimated_only": int(row[4] or 0),
        "orders_linked_but_no_cost": int(row[5] or 0),
        "orders_missing_actual_cost": int(row[6] or 0),
    }


def get_courier_closed_month_readiness(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    buffer_days: int = 45,
    as_of: date | None = None,
) -> dict[str, Any]:
    as_of_value = as_of or date.today()
    buffer_days_safe = max(0, int(buffer_days))
    _default = _dynamic_default_months()
    months_norm = [str(item).strip() for item in (months or _default) if str(item).strip()]
    carriers_norm = [str(item).strip().upper() for item in (carriers or ["DHL", "GLS"]) if str(item).strip()]
    for carrier in carriers_norm:
        if carrier not in {"DHL", "GLS"}:
            raise ValueError(f"Unsupported carrier '{carrier}'")

    if as_of_value == date.today():
        from app.services.courier_monthly_kpi import build_closed_month_readiness_from_snapshot

        snapshot = build_closed_month_readiness_from_snapshot(
            months=months_norm,
            carriers=carriers_norm,
            as_of=as_of_value,
            buffer_days=buffer_days_safe,
        )
        if snapshot is not None:
            return snapshot

    matrix: dict[str, dict[str, Any]] = {}
    scopes_total = 0
    scopes_go = 0
    scopes_no_go = 0
    scopes_pending = 0

    for month_token in months_norm:
        start = _month_start(month_token)
        end = _next_month(start)
        cutoff = end + timedelta(days=buffer_days_safe)
        month_ready = as_of_value >= cutoff
        by_carrier: dict[str, Any] = {}

        for carrier in carriers_norm:
            purchase_coverage = _coverage_snapshot(
                carrier=carrier,
                purchase_from=start,
                purchase_to_exclusive=end,
            )
            gaps = _order_level_gap_breakdown(
                carrier=carrier,
                month_start=start,
                month_end_exclusive=end,
            )

            explain = []
            if gaps["orders_without_primary_link"] > 0:
                explain.append(
                    {
                        "code": "missing_primary_link",
                        "orders": gaps["orders_without_primary_link"],
                    }
                )
            if gaps["orders_with_estimated_only"] > 0:
                explain.append(
                    {
                        "code": "estimated_only",
                        "orders": gaps["orders_with_estimated_only"],
                    }
                )
            if gaps["orders_linked_but_no_cost"] > 0:
                explain.append(
                    {
                        "code": "linked_without_cost",
                        "orders": gaps["orders_linked_but_no_cost"],
                    }
                )

            if month_ready:
                go = gaps["orders_universe"] > 0 and gaps["orders_missing_actual_cost"] == 0
                readiness = "GO" if go else "NO_GO"
                scopes_total += 1
                if go:
                    scopes_go += 1
                else:
                    scopes_no_go += 1
            else:
                readiness = "PENDING"
                scopes_pending += 1

            by_carrier[carrier] = {
                "readiness": readiness,
                "month_closed_cutoff": cutoff.isoformat(),
                "as_of": as_of_value.isoformat(),
                "purchase_month": purchase_coverage,
                "gaps": gaps,
                "explain": explain,
            }

        matrix[month_token] = {
            "is_closed_by_buffer": month_ready,
            "month_closed_cutoff": cutoff.isoformat(),
            "by_carrier": by_carrier,
        }

    overall = "GO" if scopes_total > 0 and scopes_no_go == 0 else ("PENDING" if scopes_total == 0 else "NO_GO")
    return {
        "overall_go_no_go": overall,
        "as_of": as_of_value.isoformat(),
        "buffer_days": buffer_days_safe,
        "summary": {
            "scopes_total_closed": scopes_total,
            "scopes_go": scopes_go,
            "scopes_no_go": scopes_no_go,
            "scopes_pending": scopes_pending,
        },
        "matrix": matrix,
    }
