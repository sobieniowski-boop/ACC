from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.connectors.mssql import mssql_store


_CARRIERS = {
    "DHL": {
        "source_system": "dhl_billing_files",
        "cost_source": "dhl_billing_files",
        "calc_version": "dhl_v1",
    },
    "GLS": {
        "source_system": "gls_billing_files",
        "cost_source": "gls_billing_files",
        "calc_version": "gls_v1",
    },
}


def _month_start(value: str) -> date:
    year_str, month_str = value.split("-", 1)
    return date(int(year_str), int(month_str), 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _pct(num: int, den: int) -> float:
    if den <= 0:
        return 0.0
    return round((num / den) * 100, 2)


def _shipment_metrics(cur, *, source_system: str, cost_source: str, month_start: date, month_end: date) -> dict:
    cur.execute(
        """
WITH base AS (
    SELECT s.id
    FROM dbo.acc_shipment s WITH (NOLOCK)
    WHERE s.source_system = ?
      AND s.ship_date >= ?
      AND s.ship_date < ?
),
costed AS (
    SELECT DISTINCT c.shipment_id
    FROM dbo.acc_shipment_cost c WITH (NOLOCK)
    JOIN base b
      ON b.id = c.shipment_id
    WHERE c.cost_source = ?
),
linked AS (
    SELECT DISTINCT l.shipment_id, l.amazon_order_id
    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
    JOIN base b
      ON b.id = l.shipment_id
    WHERE l.is_primary = 1
      AND l.amazon_order_id IS NOT NULL
)
SELECT
    (SELECT COUNT(*) FROM base) AS shipments_total,
    (SELECT COUNT(*) FROM costed) AS shipments_with_cost,
    (SELECT COUNT(*) FROM linked) AS shipments_linked,
    (SELECT COUNT(DISTINCT amazon_order_id) FROM linked) AS orders_linked
        """,
        [source_system, month_start.isoformat(), month_end.isoformat(), cost_source],
    )
    row = cur.fetchone()
    shipments_total = int(row[0] or 0)
    shipments_with_cost = int(row[1] or 0)
    shipments_linked = int(row[2] or 0)
    orders_linked = int(row[3] or 0)
    return {
        "shipments_total": shipments_total,
        "shipments_with_cost": shipments_with_cost,
        "shipments_linked": shipments_linked,
        "orders_linked": orders_linked,
        "shipment_cost_coverage_pct": _pct(shipments_with_cost, shipments_total),
        "shipment_link_coverage_pct": _pct(shipments_linked, shipments_total),
    }


def _order_metrics(cur, *, source_system: str, calc_version: str, month_start: date, month_end: date) -> dict:
    cur.execute(
        """
WITH linked_orders AS (
    SELECT DISTINCT l.amazon_order_id
    FROM dbo.acc_shipment s WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link l WITH (NOLOCK)
      ON l.shipment_id = s.id
     AND l.is_primary = 1
     AND l.amazon_order_id IS NOT NULL
    WHERE s.source_system = ?
      AND s.ship_date >= ?
      AND s.ship_date < ?
),
facted AS (
    SELECT DISTINCT f.amazon_order_id
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN linked_orders o
      ON o.amazon_order_id = f.amazon_order_id
    WHERE f.calc_version = ?
),
shadowed AS (
    SELECT comparison_status, COUNT(*) AS cnt
    FROM dbo.acc_order_logistics_shadow s WITH (NOLOCK)
    JOIN linked_orders o
      ON o.amazon_order_id = s.amazon_order_id
    WHERE s.calc_version = ?
    GROUP BY comparison_status
)
SELECT
    (SELECT COUNT(*) FROM linked_orders) AS orders_linked,
    (SELECT COUNT(*) FROM facted) AS orders_with_fact,
    ISNULL((SELECT SUM(cnt) FROM shadowed), 0) AS shadow_rows,
    ISNULL((SELECT SUM(cnt) FROM shadowed WHERE comparison_status = 'match'), 0) AS shadow_match,
    ISNULL((SELECT SUM(cnt) FROM shadowed WHERE comparison_status = 'match_zero'), 0) AS shadow_match_zero,
    ISNULL((SELECT SUM(cnt) FROM shadowed WHERE comparison_status = 'shadow_only'), 0) AS shadow_shadow_only,
    ISNULL((SELECT SUM(cnt) FROM shadowed WHERE comparison_status = 'legacy_only'), 0) AS shadow_legacy_only,
    ISNULL((SELECT SUM(cnt) FROM shadowed WHERE comparison_status = 'delta'), 0) AS shadow_delta
        """,
        [
            source_system,
            month_start.isoformat(),
            month_end.isoformat(),
            calc_version,
            calc_version,
        ],
    )
    row = cur.fetchone()
    orders_linked = int(row[0] or 0)
    orders_with_fact = int(row[1] or 0)
    shadow_rows = int(row[2] or 0)
    return {
        "orders_linked": orders_linked,
        "orders_with_fact": orders_with_fact,
        "order_fact_coverage_pct": _pct(orders_with_fact, orders_linked),
        "shadow_rows": shadow_rows,
        "shadow_match": int(row[3] or 0),
        "shadow_match_zero": int(row[4] or 0),
        "shadow_shadow_only": int(row[5] or 0),
        "shadow_legacy_only": int(row[6] or 0),
        "shadow_delta": int(row[7] or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit courier readiness for closed months.")
    parser.add_argument("--months", nargs="+", required=True, help="Months in YYYY-MM format.")
    args = parser.parse_args()

    report: dict[str, dict[str, dict[str, dict]]] = {}
    with mssql_store.connect_acc() as conn:
        cur = conn.cursor()
        for month_token in args.months:
            month_start = _month_start(month_token)
            month_end = _next_month(month_start)
            month_report: dict[str, dict[str, dict]] = {}
            for carrier, spec in _CARRIERS.items():
                shipments = _shipment_metrics(
                    cur,
                    source_system=spec["source_system"],
                    cost_source=spec["cost_source"],
                    month_start=month_start,
                    month_end=month_end,
                )
                orders = _order_metrics(
                    cur,
                    source_system=spec["source_system"],
                    calc_version=spec["calc_version"],
                    month_start=month_start,
                    month_end=month_end,
                )
                month_report[carrier] = {
                    "shipments": shipments,
                    "orders": orders,
                }
            report[month_token] = month_report

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
