from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Callable

from app.core.db_connection import connect_acc
from app.services.courier_order_relations import refresh_courier_order_relations
from app.services.courier_order_universe_linking import backfill_order_links_order_universe
from app.services.dhl_logistics_aggregation import aggregate_dhl_order_logistics, build_dhl_logistics_shadow
from app.services.gls_logistics_aggregation import aggregate_gls_order_logistics, build_gls_logistics_shadow

def _default_months() -> tuple[str, ...]:
    """Return last 3 closed months dynamically."""
    from datetime import date as _date
    today = _date.today()
    months = []
    for offset in range(3, 0, -1):
        m = today.month - offset
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        months.append(f"{y}-{m:02d}")
    return tuple(months)

_DEFAULT_MONTHS = _default_months()
_DEFAULT_CARRIERS = ("DHL", "GLS")
_CARRIER_SPECS = {
    "DHL": {"calc_version": "dhl_v1"},
    "GLS": {"calc_version": "gls_v1"},
}


def _month_start(token: str) -> date:
    year_str, month_str = token.split("-", 1)
    return date(int(year_str), int(month_str), 1)


def _month_end(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


def _normalize_months(months: list[str] | tuple[str, ...] | None) -> list[str]:
    raw = [str(item or "").strip() for item in (months or _DEFAULT_MONTHS)]
    result: list[str] = []
    for token in raw:
        if not token:
            continue
        # Validate format and values eagerly.
        _month_start(token)
        result.append(token)
    if not result:
        raise ValueError("months list cannot be empty")
    return result


def _normalize_carriers(carriers: list[str] | tuple[str, ...] | None) -> list[str]:
    result = [str(item or "").strip().upper() for item in (carriers or _DEFAULT_CARRIERS) if str(item or "").strip()]
    if not result:
        raise ValueError("carriers list cannot be empty")
    for carrier in result:
        if carrier not in _CARRIER_SPECS:
            raise ValueError(f"Unsupported carrier '{carrier}'")
    return result


def _contains_ci(expression: str, needle: str) -> str:
    return f"CHARINDEX('{needle}', LOWER(ISNULL({expression}, ''))) > 0"


def _carrier_predicate(alias: str, carrier: str) -> str:
    key = carrier.strip().upper()
    carrier_expr = (
        f"CASE WHEN {alias}.courier_code = 'blconnectpackages' "
        f"THEN {alias}.courier_other_name ELSE {alias}.courier_code END"
    )
    if key == "DHL":
        return (
            f"({_contains_ci(carrier_expr, 'dhl')} "
            f"OR {_contains_ci(f'{alias}.courier_other_name', 'dhl')})"
        )
    if key == "GLS":
        return (
            f"({_contains_ci(carrier_expr, 'gls')} "
            f"OR {_contains_ci(f'{alias}.courier_other_name', 'gls')})"
        )
    raise ValueError(f"Unsupported carrier '{carrier}'")


def _distribution_order_carrier_predicate(alias: str, carrier: str) -> str:
    key = carrier.strip().upper()
    if key == "DHL":
        return (
            f"({_contains_ci(f'{alias}.delivery_method', 'dhl')} "
            f"OR {_contains_ci(f'{alias}.delivery_package_module', 'dhl')})"
        )
    if key == "GLS":
        return (
            f"({_contains_ci(f'{alias}.delivery_method', 'gls')} "
            f"OR {_contains_ci(f'{alias}.delivery_package_module', 'gls')})"
        )
    raise ValueError(f"Unsupported carrier '{carrier}'")


def _coverage_snapshot(*, carrier: str, purchase_from: date, purchase_to_exclusive: date) -> dict[str, int | float]:
    carrier_key = carrier.strip().upper()
    calc_version = _CARRIER_SPECS[carrier_key]["calc_version"]
    purchase_to_inclusive = purchase_to_exclusive - timedelta(days=1)
    package_carrier_pred = _carrier_predicate("p", carrier_key)
    package_dis_carrier_pred = _carrier_predicate("dp", carrier_key)
    package_do_carrier_pred = _distribution_order_carrier_predicate("dco", carrier_key)
    purchase_from_sql = purchase_from.isoformat()
    purchase_to_sql = purchase_to_inclusive.isoformat()

    conn = connect_acc(timeout=300)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
WITH direct_carrier AS (
    SELECT DISTINCT o.amazon_order_id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.external_order_id = o.amazon_order_id
    JOIN (
        SELECT
            COALESCE(dm.holding_order_id, p.order_id) AS resolved_bl_order_id,
            p.courier_package_nr,
            p.courier_inner_number,
            p.courier_code,
            p.courier_other_name
        FROM dbo.acc_cache_packages p WITH (NOLOCK)
        LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
          ON dm.dis_order_id = p.order_id
    ) p
      ON p.resolved_bl_order_id = bo.order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND CAST(o.purchase_date AS DATE) >= '{purchase_from_sql}'
      AND CAST(o.purchase_date AS DATE) <= '{purchase_to_sql}'
      AND {package_carrier_pred}
),
distribution_carrier AS (
    SELECT DISTINCT o.amazon_order_id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
      ON dco.external_order_id = o.amazon_order_id
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = dco.order_id
    LEFT JOIN dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
      ON dp.order_id = dco.order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND CAST(o.purchase_date AS DATE) >= '{purchase_from_sql}'
      AND CAST(o.purchase_date AS DATE) <= '{purchase_to_sql}'
      AND ({package_dis_carrier_pred} OR {package_do_carrier_pred})
),
relation_carrier AS (
    SELECT DISTINCT r.source_amazon_order_id AS amazon_order_id
    FROM dbo.acc_order_courier_relation r WITH (NOLOCK)
    WHERE r.carrier = '{carrier_key}'
      AND r.is_strong = 1
      AND r.source_purchase_date >= '{purchase_from_sql}'
      AND r.source_purchase_date <= '{purchase_to_sql}'
),
carrier_orders AS (
    SELECT amazon_order_id FROM direct_carrier
    UNION
    SELECT amazon_order_id FROM distribution_carrier
    UNION
    SELECT amazon_order_id FROM relation_carrier
),
linked_primary AS (
    SELECT DISTINCT l.amazon_order_id
    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
    JOIN dbo.acc_shipment s WITH (NOLOCK)
      ON s.id = l.shipment_id
    JOIN carrier_orders co
      ON co.amazon_order_id = l.amazon_order_id
    WHERE l.is_primary = 1
      AND l.amazon_order_id IS NOT NULL
      AND s.carrier = '{carrier_key}'
),
fact_covered AS (
    SELECT DISTINCT f.amazon_order_id
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN carrier_orders co
      ON co.amazon_order_id = f.amazon_order_id
    WHERE f.calc_version = '{calc_version}'
      AND ISNULL(f.shipments_count, 0) > 0
)
SELECT
    (SELECT COUNT(*) FROM carrier_orders) AS orders_universe,
    (SELECT COUNT(*) FROM linked_primary) AS orders_linked_primary,
    (SELECT COUNT(*) FROM fact_covered) AS orders_with_fact
            """
        )
        row = cur.fetchone() or (0, 0, 0)
    finally:
        conn.close()

    universe = int(row[0] or 0)
    linked = int(row[1] or 0)
    facted = int(row[2] or 0)
    return {
        "orders_universe": universe,
        "orders_linked_primary": linked,
        "orders_with_fact": facted,
        "link_coverage_pct": round((linked / universe) * 100, 2) if universe else 0.0,
        "fact_coverage_pct": round((facted / universe) * 100, 2) if universe else 0.0,
    }


def _run_aggregate_and_shadow(
    *,
    carrier: str,
    month_start: date,
    month_end_exclusive: date,
    limit_orders: int,
) -> dict[str, dict]:
    month_end_inclusive = month_end_exclusive - timedelta(days=1)
    if carrier == "DHL":
        aggregate = aggregate_dhl_order_logistics(
            created_from=month_start,
            created_to=month_end_inclusive,
            limit_orders=limit_orders,
        )
        shadow = build_dhl_logistics_shadow(
            purchase_from=month_start,
            purchase_to=month_end_inclusive,
            limit_orders=limit_orders,
            replace_all_existing=False,
        )
    else:
        aggregate = aggregate_gls_order_logistics(
            created_from=month_start,
            created_to=month_end_inclusive,
            limit_orders=limit_orders,
        )
        shadow = build_gls_logistics_shadow(
            purchase_from=month_start,
            purchase_to=month_end_inclusive,
            limit_orders=limit_orders,
        )
    return {"aggregate": aggregate, "shadow": shadow}


def run_courier_order_universe_pipeline(
    *,
    months: list[str] | tuple[str, ...] | None = None,
    carriers: list[str] | tuple[str, ...] | None = None,
    reset_existing_in_scope: bool = False,
    run_aggregate_shadow: bool = False,
    limit_orders: int = 3_000_000,
    created_to_buffer_days: int = 31,
    refresh_relations: bool = True,
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> dict[str, dict[str, dict[str, dict]]]:
    norm_months = _normalize_months(months)
    norm_carriers = _normalize_carriers(carriers)
    limit_orders_safe = max(1, int(limit_orders or 1))
    created_to_buffer_days_safe = max(0, int(created_to_buffer_days or 0))

    report: dict[str, dict[str, dict[str, dict]]] = {}
    stages_per_pair = (4 if run_aggregate_shadow else 3) if refresh_relations else (3 if run_aggregate_shadow else 2)
    total_steps = max(1, len(norm_months) * len(norm_carriers) * stages_per_pair)
    completed_steps = 0

    def _emit(message: str) -> None:
        if progress_callback:
            progress_callback(message, completed_steps, total_steps)

    for month_token in norm_months:
        month_start = _month_start(month_token)
        month_end = _month_end(month_start)
        month_report: dict[str, dict[str, dict]] = {}
        for carrier in norm_carriers:
            relations = {}
            if refresh_relations:
                _emit(f"{month_token} {carrier}: relations")
                relations_result = refresh_courier_order_relations(
                    months=[month_token],
                    carriers=[carrier],
                    lookahead_days=max(7, created_to_buffer_days_safe),
                )
                relations = ((relations_result.get("matrix") or {}).get(month_token) or {}).get(carrier) or {}
                completed_steps += 1
            _emit(f"{month_token} {carrier}: linking")
            linking = backfill_order_links_order_universe(
                carrier=carrier,
                purchase_from=month_start,
                purchase_to=month_end - timedelta(days=1),
                created_from=month_start,
                created_to=(month_end - timedelta(days=1)) + timedelta(days=created_to_buffer_days_safe),
                reset_existing_in_scope=reset_existing_in_scope,
            )
            completed_steps += 1
            post_process = {}
            if run_aggregate_shadow:
                _emit(f"{month_token} {carrier}: aggregate+shadow")
                post_process = _run_aggregate_and_shadow(
                    carrier=carrier,
                    month_start=month_start,
                    month_end_exclusive=month_end,
                    limit_orders=limit_orders_safe,
                )
                completed_steps += 1
            _emit(f"{month_token} {carrier}: coverage")
            coverage = _coverage_snapshot(
                carrier=carrier,
                purchase_from=month_start,
                purchase_to_exclusive=month_end,
            )
            completed_steps += 1
            month_report[carrier] = {
                "relations": relations,
                "linking": linking,
                "coverage": coverage,
                "post_process": post_process,
            }
        report[month_token] = month_report
    return report
