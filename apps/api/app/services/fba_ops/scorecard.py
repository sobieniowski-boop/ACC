from __future__ import annotations

import math
from datetime import date, timedelta
from statistics import median
from typing import Any

import pyodbc

from ._helpers import (
    _build_component,
    _connect,
    _fetchall_dict,
    _get_json_config,
    _parse_quarter,
    _to_float,
    _to_int,
    ensure_fba_schema,
)


# ──────────── KPI compute functions ────────────


def _compute_kpi_top100_availability(cur: pyodbc.Cursor) -> tuple[float | None, bool, str | None]:
    cur.execute(
        """
        WITH revenue AS (
            SELECT
                ISNULL(ol.sku, p.sku) AS sku,
                SUM(ISNULL(ol.item_price, 0) * CASE
                    WHEN ISNULL(ol.quantity_shipped, 0) > 0 THEN ISNULL(ol.quantity_shipped, 0)
                    WHEN ISNULL(ol.quantity_ordered, 0) > 0 THEN ISNULL(ol.quantity_ordered, 0)
                    ELSE 1
                END) AS revenue_90d
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
            WHERE o.status = 'Shipped'
              AND CAST(o.purchase_date AS DATE) >= DATEADD(day, -90, CAST(GETUTCDATE() AS DATE))
              AND ISNULL(ol.sku, p.sku) LIKE 'FBA[_]%'
            GROUP BY ISNULL(ol.sku, p.sku)
        ),
        top_skus AS (
            SELECT TOP 100 sku, revenue_90d
            FROM revenue
            ORDER BY revenue_90d DESC
        ),
        snapshot_days AS (
            SELECT DISTINCT snapshot_date
            FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
            WHERE snapshot_date >= DATEADD(day, -30, CAST(GETUTCDATE() AS DATE))
        ),
        agg_inv AS (
            SELECT sku, snapshot_date, SUM(ISNULL(on_hand, 0)) AS total_on_hand
            FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
            WHERE snapshot_date >= DATEADD(day, -30, CAST(GETUTCDATE() AS DATE))
              AND sku LIKE 'FBA[_]%'
            GROUP BY sku, snapshot_date
        ),
        observations AS (
            SELECT
                ts.sku,
                sd.snapshot_date,
                CASE WHEN ISNULL(inv.total_on_hand, 0) > 0 THEN 1.0 ELSE 0.0 END AS is_available
            FROM top_skus ts
            CROSS JOIN snapshot_days sd
            LEFT JOIN agg_inv inv
              ON inv.sku = ts.sku AND inv.snapshot_date = sd.snapshot_date
        )
        SELECT
            (SELECT COUNT(*) FROM top_skus) AS sku_count,
            (SELECT COUNT(*) FROM snapshot_days) AS snap_days,
            COUNT(*) AS total_obs,
            SUM(is_available) AS available_obs
        FROM observations
        """,
    )
    row = cur.fetchone()
    if not row:
        return None, False, "No FBA revenue data found."
    sku_count = _to_int(row[0])
    snap_days = _to_int(row[1])
    total_obs = _to_int(row[2])
    available_obs = _to_int(row[3])
    if sku_count == 0:
        return None, False, "No shipped FBA orders in the last 90 days."
    if snap_days == 0:
        return None, False, "No FBA inventory snapshots in the last 30 days."
    value = round(available_obs / total_obs * 100, 2) if total_obs else 0.0
    note = (
        f"Top {sku_count} FBA children by revenue (90 d). "
        f"Avg availability across {snap_days} snapshot days."
    )
    return value, snap_days >= 3, note


def _compute_kpi_shipment_adherence(cur: pyodbc.Cursor, quarter: str) -> tuple[float | None, bool, str | None]:
    cur.execute(
        """
        SELECT plan_week_start, planned_ship_date, planned_units, actual_ship_date, actual_units, tolerance_pct
        FROM dbo.acc_fba_shipment_plan WITH (NOLOCK)
        WHERE quarter = ?
          AND status NOT IN ('cancelled', 'draft')
        """,
        (quarter,),
    )
    rows = _fetchall_dict(cur)
    if not rows:
        return None, False, "Shipment plan register is empty for this quarter."
    compliant = 0
    for row in rows:
        plan_week_start = row.get("plan_week_start")
        planned_ship_date = row.get("planned_ship_date") or plan_week_start
        actual_ship_date = row.get("actual_ship_date")
        planned_units = max(_to_int(row.get("planned_units")), 0)
        actual_units = max(_to_int(row.get("actual_units")), 0)
        tolerance_pct = _to_float(row.get("tolerance_pct"), 0.10)
        within_volume = planned_units > 0 and actual_units >= math.floor(planned_units * 0.90) and actual_units <= math.ceil(planned_units * (1 + tolerance_pct))
        if not planned_ship_date or not actual_ship_date:
            continue
        latest_ok_date = plan_week_start + timedelta(days=8) if plan_week_start else planned_ship_date + timedelta(days=1)
        within_date = actual_ship_date >= (plan_week_start or planned_ship_date) and actual_ship_date <= latest_ok_date
        if within_date and within_volume:
            compliant += 1
    return round(compliant / len(rows) * 100, 4), True, f"Based on {len(rows)} shipment plan rows with +/-10% volume tolerance."


def _compute_kpi_inventory_value_share(cur: pyodbc.Cursor, *, mode: str) -> tuple[float | None, bool, str | None]:
    cur.execute("SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)")
    row = cur.fetchone()
    latest_snapshot = row[0] if row and row[0] else None
    if not latest_snapshot:
        return None, False, "No FBA inventory snapshot cached yet."
    extra_expr = "ISNULL(inv.stranded_units, 0)" if mode == "stranded" else "(ISNULL(inv.aged_90_plus, 0) + ISNULL(inv.excess_units, 0))"
    cur.execute(
        f"""
        SELECT
            SUM(CAST((ISNULL(inv.on_hand, 0) + ISNULL(inv.inbound, 0)) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))) AS total_value,
            SUM(CAST({extra_expr} * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))) AS focus_value
        FROM dbo.acc_fba_inventory_snapshot inv WITH (NOLOCK)
        LEFT JOIN dbo.acc_product p WITH (NOLOCK)
          ON p.sku = inv.sku OR (p.asin = inv.asin AND inv.asin IS NOT NULL)
        WHERE inv.snapshot_date = ?
        """,
        (latest_snapshot,),
    )
    row = cur.fetchone()
    total_value = _to_float(row[0]) if row else 0.0
    focus_value = _to_float(row[1]) if row else 0.0
    if total_value <= 0:
        return None, False, "Inventory value base is missing or zero."
    note = f"Computed on latest cached FBA snapshot {latest_snapshot.isoformat()} using product COGS cache."
    return round(focus_value / total_value * 100, 4), True, note


def _compute_kpi_median_resolve_days(cur: pyodbc.Cursor, start_date: date, end_date: date) -> tuple[float | None, bool, str | None]:
    cur.execute(
        """
        SELECT detected_date, close_date
        FROM dbo.acc_fba_case WITH (NOLOCK)
        WHERE close_date BETWEEN ? AND ?
          AND status IN ('closed', 'resolved')
          AND case_type IN ('stranded', 'fc_issue', 'operations')
        """,
        (start_date, end_date),
    )
    rows = _fetchall_dict(cur)
    if not rows:
        return None, False, "Case register has no closed stranded / FC / operations items in this quarter."
    durations = [
        (row.get("close_date") - row.get("detected_date")).days
        for row in rows
        if row.get("close_date") and row.get("detected_date") and row.get("close_date") >= row.get("detected_date")
    ]
    if not durations:
        return None, False, "Case register rows are incomplete."
    return round(float(median(durations)), 4), True, f"Median from {len(durations)} resolved cases."


def _compute_kpi_fc_discrepancy(cur: pyodbc.Cursor, start_date: date, end_date: date) -> tuple[float | None, bool, str | None]:
    cur.execute(
        """
        SELECT
            SUM(CAST((ISNULL(r.shortage_units, 0) + ISNULL(r.damage_units, 0)) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))) AS discrepancy_value,
            SUM(CAST(ISNULL(r.shipped_units, 0) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))) AS shipped_value
        FROM dbo.acc_fba_receiving_reconciliation r WITH (NOLOCK)
        LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.sku = r.sku
        WHERE r.event_date BETWEEN ? AND ?
        """,
        (start_date, end_date),
    )
    row = cur.fetchone()
    shipped_value = _to_float(row[1]) if row else 0.0
    discrepancy_value = _to_float(row[0]) if row else 0.0
    if shipped_value <= 0:
        return None, False, "Receiving reconciliation register is empty for this quarter."
    return round(discrepancy_value / shipped_value * 100, 4), True, "Raw KPI without reimbursements offset."


def _compute_kpi_on_time_launch(cur: pyodbc.Cursor, quarter: str) -> tuple[float | None, bool, str | None]:
    cur.execute(
        """
        SELECT planned_go_live_date, actual_go_live_date, incident_free
        FROM dbo.acc_fba_launch WITH (NOLOCK)
        WHERE quarter = ?
          AND status NOT IN ('cancelled', 'draft')
        """,
        (quarter,),
    )
    rows = _fetchall_dict(cur)
    if not rows:
        return None, False, "Launch register is empty for this quarter."
    compliant = 0
    for row in rows:
        planned = row.get("planned_go_live_date")
        actual = row.get("actual_go_live_date")
        incident_free = bool(row.get("incident_free"))
        if planned and actual and actual <= planned and incident_free:
            compliant += 1
    return round(compliant / len(rows) * 100, 4), True, f"Based on {len(rows)} launch records."


def _compute_kpi_vine_coverage(cur: pyodbc.Cursor, quarter: str) -> tuple[float | None, bool, str | None]:
    cur.execute(
        """
        SELECT actual_go_live_date, vine_eligible_at, vine_submitted_at
        FROM dbo.acc_fba_launch WITH (NOLOCK)
        WHERE quarter = ?
          AND vine_eligible = 1
          AND status NOT IN ('cancelled', 'draft')
        """,
        (quarter,),
    )
    rows = _fetchall_dict(cur)
    if not rows:
        return None, False, "No Vine-eligible launches in the register for this quarter."
    covered = 0
    for row in rows:
        eligible_at = row.get("vine_eligible_at") or row.get("actual_go_live_date")
        submitted_at = row.get("vine_submitted_at")
        if eligible_at and submitted_at and submitted_at <= eligible_at + timedelta(days=14):
            covered += 1
    return round(covered / len(rows) * 100, 4), True, f"Based on {len(rows)} Vine-eligible launches with 14-day SLA."


def _compute_kpi_initiatives_completion(cur: pyodbc.Cursor, quarter: str) -> tuple[float | None, bool, str | None]:
    cur.execute(
        """
        SELECT planned, approved, status, live_stable_at
        FROM dbo.acc_fba_initiative WITH (NOLOCK)
        WHERE quarter = ?
        """,
        (quarter,),
    )
    rows = _fetchall_dict(cur)
    planned_rows = [row for row in rows if bool(row.get("planned")) and bool(row.get("approved"))]
    if not planned_rows:
        return None, False, "Initiative register is empty for this quarter."
    done = sum(1 for row in planned_rows if row.get("live_stable_at") is not None or str(row.get("status") or "").lower() in {"live_stable", "done", "completed"})
    return round(done / len(planned_rows) * 100, 4), True, f"{done} of {len(planned_rows)} approved initiatives reached Live & stable."


# ──────────── Scorecard entry‑point ────────────


def get_scorecard(*, quarter: str) -> dict[str, Any]:
    ensure_fba_schema()
    start_date, end_date = _parse_quarter(quarter)
    conn = _connect()
    try:
        cur = conn.cursor()
        configs = _get_json_config(cur, "scorecard_defaults", {})
        calculators = {
            "top100_availability": lambda: _compute_kpi_top100_availability(cur),
            "shipment_plan_adherence": lambda: _compute_kpi_shipment_adherence(cur, quarter),
            "stranded_inventory_value_pct": lambda: _compute_kpi_inventory_value_share(cur, mode="stranded"),
            "median_resolve_days": lambda: _compute_kpi_median_resolve_days(cur, start_date, end_date),
            "aging_excess_share": lambda: _compute_kpi_inventory_value_share(cur, mode="aging_excess"),
            "fc_discrepancy_rate": lambda: _compute_kpi_fc_discrepancy(cur, start_date, end_date),
            "on_time_launch_rate": lambda: _compute_kpi_on_time_launch(cur, quarter),
            "vine_coverage_rate": lambda: _compute_kpi_vine_coverage(cur, quarter),
            "initiatives_completion_rate": lambda: _compute_kpi_initiatives_completion(cur, quarter),
        }
        components: list[dict[str, Any]] = []
        missing_inputs: list[str] = []
        for key, calculate in calculators.items():
            config = configs.get(key, {})
            actual, data_ready, note = calculate()
            component = _build_component(key=key, actual=actual, data_ready=data_ready, note=note, config=config)
            components.append(component)
            if not data_ready:
                missing_inputs.append(f"{component['label']}: {note or 'missing source'}")
        score = round(sum(component["score_contribution"] for component in components), 4)
        data_ready = all(component["data_ready"] for component in components)
        explanation = (
            "Quarterly score is now driven by 9 KPI components. Some components still depend on manual FBA registers "
            "for shipment plan, cases, launches, Vine and initiatives; missing registers reduce data readiness and zero out that component."
        )
        return {
            "quarter": quarter,
            "data_ready": data_ready,
            "score": score,
            "score_pct_of_target": round(score * 100, 1),
            "safety_gate_passed": True,
            "explanation": explanation,
            "kpis": {component["key"]: component["actual"] or 0.0 for component in components},
            "factors": {component["key"]: component["factor"] for component in components},
            "weights": {component["key"]: component["weight"] for component in components},
            "components": components,
            "missing_inputs": missing_inputs,
        }
    finally:
        conn.close()
