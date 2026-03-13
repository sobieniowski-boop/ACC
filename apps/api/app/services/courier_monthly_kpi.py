from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from app.core.db_connection import connect_acc
from app.services.courier_order_universe_pipeline import _coverage_snapshot
from app.services.courier_readiness import (
    _billing_period_coverage,
    _dynamic_default_months,
    _month_start,
    _next_month,
    _order_level_gap_breakdown,
    _shipment_month_coverage,
)


def _connect():
    return connect_acc(autocommit=False, timeout=60)


def _normalize_months(months: list[str] | None = None) -> list[str]:
    result: list[str] = []
    for raw in (months or _dynamic_default_months()):
        token = str(raw or "").strip()
        if not token:
            continue
        _month_start(token)
        result.append(token)
    if not result:
        raise ValueError("months list cannot be empty")
    return result


def _normalize_carriers(carriers: list[str] | None = None) -> list[str]:
    result = [str(item or "").strip().upper() for item in (carriers or ["DHL", "GLS"]) if str(item or "").strip()]
    if not result:
        raise ValueError("carriers list cannot be empty")
    for carrier in result:
        if carrier not in {"DHL", "GLS"}:
            raise ValueError(f"Unsupported carrier '{carrier}'")
    return result


def _calc_version_for_carrier(carrier: str) -> str:
    return "dhl_v1" if carrier == "DHL" else "gls_v1"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _build_explain(gaps: dict[str, int]) -> list[dict[str, int | str]]:
    explain: list[dict[str, int | str]] = []
    if int(gaps.get("orders_without_primary_link", 0) or 0) > 0:
        explain.append(
            {
                "code": "missing_primary_link",
                "orders": int(gaps["orders_without_primary_link"]),
            }
        )
    if int(gaps.get("orders_with_estimated_only", 0) or 0) > 0:
        explain.append(
            {
                "code": "estimated_only",
                "orders": int(gaps["orders_with_estimated_only"]),
            }
        )
    if int(gaps.get("orders_linked_but_no_cost", 0) or 0) > 0:
        explain.append(
            {
                "code": "linked_without_cost",
                "orders": int(gaps["orders_linked_but_no_cost"]),
            }
        )
    return explain


def _pct(part: int, total: int) -> float:
    return round((part / total) * 100, 2) if total else 0.0


def _primary_gap_driver(*, missing_primary_link: int, estimated_only: int, linked_no_cost: int) -> str:
    cost_pending_after_link = max(0, int(estimated_only or 0)) + max(0, int(linked_no_cost or 0))
    missing_primary_link_safe = max(0, int(missing_primary_link or 0))
    if missing_primary_link_safe == 0 and cost_pending_after_link == 0:
        return "complete"
    if missing_primary_link_safe > cost_pending_after_link:
        return "missing_primary_link"
    if cost_pending_after_link > missing_primary_link_safe:
        if estimated_only > 0 and linked_no_cost == 0:
            return "estimated_only"
        if linked_no_cost > 0 and estimated_only == 0:
            return "linked_no_cost"
        return "cost_pending_after_link"
    return "mixed"


def _derive_operational_state(payload: dict[str, Any]) -> dict[str, Any]:
    universe = max(0, int(payload.get("purchase_orders_universe", 0) or 0))
    missing_primary_link = max(0, int(payload.get("purchase_orders_without_primary_link", 0) or 0))
    estimated_only = max(0, int(payload.get("purchase_orders_with_estimated_only", 0) or 0))
    linked_no_cost = max(0, int(payload.get("purchase_orders_linked_but_no_cost", 0) or 0))
    missing_actual_cost = max(0, int(payload.get("purchase_orders_missing_actual_cost", 0) or 0))
    cost_pending_after_link = estimated_only + linked_no_cost
    is_closed = bool(payload.get("is_closed_by_buffer"))
    readiness = str(payload.get("readiness") or "PENDING").upper()
    driver = _primary_gap_driver(
        missing_primary_link=missing_primary_link,
        estimated_only=estimated_only,
        linked_no_cost=linked_no_cost,
    )

    if is_closed:
        if readiness == "GO":
            status = "CLOSED_GO"
            reason = "closed_month_complete"
        elif missing_primary_link > 0 and cost_pending_after_link > 0:
            status = "CLOSED_NO_GO"
            reason = "closed_month_missing_links_and_missing_actual_cost"
        elif missing_primary_link > 0:
            status = "CLOSED_NO_GO"
            reason = "closed_month_missing_primary_links"
        else:
            status = "CLOSED_NO_GO"
            reason = "closed_month_missing_actual_cost_after_link"
    else:
        if universe == 0:
            status = "OPEN_NO_SCOPE"
            reason = "no_orders_in_scope"
        elif missing_actual_cost == 0:
            status = "OPEN_COMPLETE"
            reason = "all_orders_have_actual_cost"
        elif missing_primary_link > 0 and cost_pending_after_link > 0:
            status = "OPEN_MIXED"
            reason = "missing_primary_links_and_cost_pending_after_link"
        elif missing_primary_link > 0:
            status = "OPEN_LINK_GAP"
            reason = "missing_primary_links"
        elif estimated_only > 0 and linked_no_cost == 0:
            status = "OPEN_AWAITING_INVOICES"
            reason = "estimated_cost_only_pending_actual_invoice"
        elif linked_no_cost > 0 and estimated_only == 0:
            status = "OPEN_LINKED_NO_COST"
            reason = "linked_orders_without_any_cost_row"
        else:
            status = "OPEN_COST_PENDING"
            reason = "linked_orders_without_actual_cost"

    return {
        "status": status,
        "reason": reason,
        "primary_gap_driver": driver,
        "gap_orders": {
            "missing_primary_link": missing_primary_link,
            "estimated_only": estimated_only,
            "linked_no_cost": linked_no_cost,
            "cost_pending_after_link": cost_pending_after_link,
            "missing_actual_cost": missing_actual_cost,
        },
        "gap_share_pct": {
            "missing_primary_link": _pct(missing_primary_link, universe),
            "estimated_only": _pct(estimated_only, universe),
            "linked_no_cost": _pct(linked_no_cost, universe),
            "cost_pending_after_link": _pct(cost_pending_after_link, universe),
            "missing_actual_cost": _pct(missing_actual_cost, universe),
        },
    }


def ensure_courier_monthly_kpi_schema() -> None:
    """No-op: schema managed by Alembic migration eb014."""
    pass


def _upsert_snapshot_row(cur, payload: dict[str, Any]) -> None:
    cur.execute(
        """
        SELECT 1
        FROM dbo.acc_courier_monthly_kpi_snapshot WITH (NOLOCK)
        WHERE month_token = ?
          AND carrier = ?
        """,
        [payload["month_token"], payload["carrier"]],
    )
    exists = cur.fetchone() is not None
    params = [
        payload["month_start"],
        payload["calc_version"],
        payload["as_of_date"],
        payload["buffer_days"],
        1 if payload["is_closed_by_buffer"] else 0,
        payload["month_closed_cutoff"],
        payload["purchase_orders_universe"],
        payload["purchase_orders_linked_primary"],
        payload["purchase_orders_with_fact"],
        payload["purchase_orders_with_actual_cost"],
        payload["purchase_orders_without_primary_link"],
        payload["purchase_orders_with_estimated_only"],
        payload["purchase_orders_linked_but_no_cost"],
        payload["purchase_orders_missing_actual_cost"],
        payload["purchase_link_coverage_pct"],
        payload["purchase_fact_coverage_pct"],
        payload["purchase_actual_cost_coverage_pct"],
        payload["shipment_total"],
        payload["shipment_linked"],
        payload["shipment_actual_cost"],
        payload["shipment_link_coverage_pct"],
        payload["shipment_actual_cost_coverage_pct"],
        payload["billing_shipments_total"],
        payload["billing_shipments_linked"],
        payload["billing_link_coverage_pct"],
        payload["readiness"],
        json.dumps(payload["explain"], ensure_ascii=True),
    ]
    if exists:
        cur.execute(
            """
            UPDATE dbo.acc_courier_monthly_kpi_snapshot
            SET
                month_start = ?,
                calc_version = ?,
                as_of_date = ?,
                buffer_days = ?,
                is_closed_by_buffer = ?,
                month_closed_cutoff = ?,
                purchase_orders_universe = ?,
                purchase_orders_linked_primary = ?,
                purchase_orders_with_fact = ?,
                purchase_orders_with_actual_cost = ?,
                purchase_orders_without_primary_link = ?,
                purchase_orders_with_estimated_only = ?,
                purchase_orders_linked_but_no_cost = ?,
                purchase_orders_missing_actual_cost = ?,
                purchase_link_coverage_pct = ?,
                purchase_fact_coverage_pct = ?,
                purchase_actual_cost_coverage_pct = ?,
                shipment_total = ?,
                shipment_linked = ?,
                shipment_actual_cost = ?,
                shipment_link_coverage_pct = ?,
                shipment_actual_cost_coverage_pct = ?,
                billing_shipments_total = ?,
                billing_shipments_linked = ?,
                billing_link_coverage_pct = ?,
                readiness = ?,
                explain_json = ?,
                updated_at = SYSUTCDATETIME()
            WHERE month_token = ?
              AND carrier = ?
            """,
            params + [payload["month_token"], payload["carrier"]],
        )
        return

    cur.execute(
        """
        INSERT INTO dbo.acc_courier_monthly_kpi_snapshot
        (
            month_token, month_start, carrier, calc_version, as_of_date, buffer_days,
            is_closed_by_buffer, month_closed_cutoff,
            purchase_orders_universe, purchase_orders_linked_primary, purchase_orders_with_fact,
            purchase_orders_with_actual_cost, purchase_orders_without_primary_link,
            purchase_orders_with_estimated_only, purchase_orders_linked_but_no_cost,
            purchase_orders_missing_actual_cost, purchase_link_coverage_pct,
            purchase_fact_coverage_pct, purchase_actual_cost_coverage_pct,
            shipment_total, shipment_linked, shipment_actual_cost,
            shipment_link_coverage_pct, shipment_actual_cost_coverage_pct,
            billing_shipments_total, billing_shipments_linked, billing_link_coverage_pct,
            readiness, explain_json, created_at, updated_at
        )
        VALUES
        (
            ?, ?, ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME()
        )
        """,
        [
            payload["month_token"],
            payload["month_start"],
            payload["carrier"],
            payload["calc_version"],
            payload["as_of_date"],
            payload["buffer_days"],
            1 if payload["is_closed_by_buffer"] else 0,
            payload["month_closed_cutoff"],
            payload["purchase_orders_universe"],
            payload["purchase_orders_linked_primary"],
            payload["purchase_orders_with_fact"],
            payload["purchase_orders_with_actual_cost"],
            payload["purchase_orders_without_primary_link"],
            payload["purchase_orders_with_estimated_only"],
            payload["purchase_orders_linked_but_no_cost"],
            payload["purchase_orders_missing_actual_cost"],
            payload["purchase_link_coverage_pct"],
            payload["purchase_fact_coverage_pct"],
            payload["purchase_actual_cost_coverage_pct"],
            payload["shipment_total"],
            payload["shipment_linked"],
            payload["shipment_actual_cost"],
            payload["shipment_link_coverage_pct"],
            payload["shipment_actual_cost_coverage_pct"],
            payload["billing_shipments_total"],
            payload["billing_shipments_linked"],
            payload["billing_link_coverage_pct"],
            payload["readiness"],
            json.dumps(payload["explain"], ensure_ascii=True),
        ],
    )


def _payload_to_item(payload: dict[str, Any]) -> dict[str, Any]:
    operational = _derive_operational_state(payload)
    purchase_month = {
        "orders_universe": int(payload["purchase_orders_universe"]),
        "orders_linked_primary": int(payload["purchase_orders_linked_primary"]),
        "orders_with_fact": int(payload["purchase_orders_with_fact"]),
        "orders_with_actual_cost": int(payload["purchase_orders_with_actual_cost"]),
        "orders_without_primary_link": int(payload["purchase_orders_without_primary_link"]),
        "orders_with_estimated_only": int(payload["purchase_orders_with_estimated_only"]),
        "orders_linked_but_no_cost": int(payload["purchase_orders_linked_but_no_cost"]),
        "orders_missing_actual_cost": int(payload["purchase_orders_missing_actual_cost"]),
        "link_coverage_pct": _to_float(payload["purchase_link_coverage_pct"]),
        "fact_coverage_pct": _to_float(payload["purchase_fact_coverage_pct"]),
        "actual_cost_coverage_pct": _to_float(payload["purchase_actual_cost_coverage_pct"]),
    }
    shipment_month = {
        "shipments_total": int(payload["shipment_total"]),
        "linked_shipments": int(payload["shipment_linked"]),
        "costed_shipments_actual": int(payload["shipment_actual_cost"]),
        "link_coverage_pct": _to_float(payload["shipment_link_coverage_pct"]),
        "cost_coverage_pct": _to_float(payload["shipment_actual_cost_coverage_pct"]),
    }
    billing_period = {
        "billed_shipments_total": int(payload["billing_shipments_total"]),
        "billed_shipments_linked": int(payload["billing_shipments_linked"]),
        "link_coverage_pct": _to_float(payload["billing_link_coverage_pct"]),
    }
    return {
        "month": payload["month_token"],
        "month_start": payload["month_start"],
        "carrier": payload["carrier"],
        "calc_version": payload["calc_version"],
        "as_of": payload["as_of_date"],
        "buffer_days": int(payload["buffer_days"]),
        "is_closed_by_buffer": bool(payload["is_closed_by_buffer"]),
        "month_closed_cutoff": payload["month_closed_cutoff"],
        "readiness": payload["readiness"],
        "purchase_month": purchase_month,
        "amazon_order_coverage": purchase_month,
        "shipment_month": shipment_month,
        "all_shipments_coverage": shipment_month,
        "billing_period": billing_period,
        "billed_shipments_coverage": billing_period,
        "operational": operational,
        "explain": payload["explain"],
    }


def refresh_courier_monthly_kpi_snapshot(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    buffer_days: int = 45,
    as_of: date | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_courier_monthly_kpi_schema()

    from app.connectors.mssql.mssql_store import set_job_progress

    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    as_of_value = as_of or date.today()
    buffer_days_safe = max(0, int(buffer_days))
    total_rows = max(1, len(months_norm) * len(carriers_norm))

    stats: dict[str, Any] = {
        "status": "ok",
        "as_of": as_of_value.isoformat(),
        "buffer_days": buffer_days_safe,
        "rows_upserted": 0,
        "months": months_norm,
        "carriers": carriers_norm,
        "items": [],
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        for idx, month_token in enumerate(months_norm, start=0):
            month_start = _month_start(month_token)
            month_end = _next_month(month_start)
            cutoff = month_end + timedelta(days=buffer_days_safe)
            is_closed = as_of_value >= cutoff
            for carrier_index, carrier in enumerate(carriers_norm, start=1):
                coverage = _coverage_snapshot(
                    carrier=carrier,
                    purchase_from=month_start,
                    purchase_to_exclusive=month_end,
                )
                gaps = _order_level_gap_breakdown(
                    carrier=carrier,
                    month_start=month_start,
                    month_end_exclusive=month_end,
                )
                shipment = _shipment_month_coverage(
                    carrier=carrier,
                    month_start=month_start,
                    month_end=month_end,
                )
                billing = _billing_period_coverage(
                    carrier=carrier,
                    billing_period=month_token,
                )

                universe = int(gaps.get("orders_universe", 0) or 0)
                actual_orders = int(gaps.get("orders_with_actual_cost", 0) or 0)
                actual_pct = round((actual_orders / universe) * 100, 2) if universe else 0.0
                explain = _build_explain(gaps)
                readiness = "PENDING"
                if is_closed:
                    readiness = "GO" if universe > 0 and int(gaps.get("orders_missing_actual_cost", 0) or 0) == 0 else "NO_GO"

                payload = {
                    "month_token": month_token,
                    "month_start": month_start.isoformat(),
                    "carrier": carrier,
                    "calc_version": _calc_version_for_carrier(carrier),
                    "as_of_date": as_of_value.isoformat(),
                    "buffer_days": buffer_days_safe,
                    "is_closed_by_buffer": is_closed,
                    "month_closed_cutoff": cutoff.isoformat(),
                    "purchase_orders_universe": universe,
                    "purchase_orders_linked_primary": int(coverage.get("orders_linked_primary", 0) or 0),
                    "purchase_orders_with_fact": int(coverage.get("orders_with_fact", 0) or 0),
                    "purchase_orders_with_actual_cost": actual_orders,
                    "purchase_orders_without_primary_link": int(gaps.get("orders_without_primary_link", 0) or 0),
                    "purchase_orders_with_estimated_only": int(gaps.get("orders_with_estimated_only", 0) or 0),
                    "purchase_orders_linked_but_no_cost": int(gaps.get("orders_linked_but_no_cost", 0) or 0),
                    "purchase_orders_missing_actual_cost": int(gaps.get("orders_missing_actual_cost", 0) or 0),
                    "purchase_link_coverage_pct": round(_to_float(coverage.get("link_coverage_pct", 0.0)), 2),
                    "purchase_fact_coverage_pct": round(_to_float(coverage.get("fact_coverage_pct", 0.0)), 2),
                    "purchase_actual_cost_coverage_pct": actual_pct,
                    "shipment_total": int(shipment.get("shipments_total", 0) or 0),
                    "shipment_linked": int(shipment.get("linked_shipments", 0) or 0),
                    "shipment_actual_cost": int(shipment.get("costed_shipments_actual", 0) or 0),
                    "shipment_link_coverage_pct": round(_to_float(shipment.get("link_coverage_pct", 0.0)), 2),
                    "shipment_actual_cost_coverage_pct": round(_to_float(shipment.get("cost_coverage_pct", 0.0)), 2),
                    "billing_shipments_total": int(billing.get("billed_shipments_total", 0) or 0),
                    "billing_shipments_linked": int(billing.get("billed_shipments_linked", 0) or 0),
                    "billing_link_coverage_pct": round(_to_float(billing.get("link_coverage_pct", 0.0)), 2),
                    "readiness": readiness,
                    "explain": explain,
                }
                _upsert_snapshot_row(cur, payload)
                stats["rows_upserted"] += 1
                stats["items"].append(_payload_to_item(payload))

                completed = idx * len(carriers_norm) + carrier_index
                if job_id:
                    pct = 10 + int((completed / total_rows) * 85)
                    set_job_progress(
                        job_id,
                        progress_pct=min(95, pct),
                        records_processed=completed,
                        message=f"Courier monthly KPI snapshot {month_token} {carrier}",
                    )

            conn.commit()
        return stats
    finally:
        conn.close()


def load_courier_monthly_kpi_rows(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    ensure_courier_monthly_kpi_schema()

    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    month_placeholders = ",".join("?" for _ in months_norm)
    carrier_placeholders = ",".join("?" for _ in carriers_norm)

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                month_token,
                month_start,
                carrier,
                calc_version,
                as_of_date,
                buffer_days,
                is_closed_by_buffer,
                month_closed_cutoff,
                purchase_orders_universe,
                purchase_orders_linked_primary,
                purchase_orders_with_fact,
                purchase_orders_with_actual_cost,
                purchase_orders_without_primary_link,
                purchase_orders_with_estimated_only,
                purchase_orders_linked_but_no_cost,
                purchase_orders_missing_actual_cost,
                purchase_link_coverage_pct,
                purchase_fact_coverage_pct,
                purchase_actual_cost_coverage_pct,
                shipment_total,
                shipment_linked,
                shipment_actual_cost,
                shipment_link_coverage_pct,
                shipment_actual_cost_coverage_pct,
                billing_shipments_total,
                billing_shipments_linked,
                billing_link_coverage_pct,
                readiness,
                explain_json
            FROM dbo.acc_courier_monthly_kpi_snapshot WITH (NOLOCK)
            WHERE month_token IN ({month_placeholders})
              AND carrier IN ({carrier_placeholders})
            ORDER BY month_start ASC, carrier ASC
            """,
            months_norm + carriers_norm,
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        try:
            explain = json.loads(row[28]) if row[28] else []
        except Exception:
            explain = []
        payload = {
            "month_token": str(row[0]),
            "month_start": str(row[1]),
            "carrier": str(row[2]),
            "calc_version": str(row[3]),
            "as_of_date": str(row[4]),
            "buffer_days": int(row[5] or 0),
            "is_closed_by_buffer": bool(row[6]),
            "month_closed_cutoff": str(row[7]),
            "purchase_orders_universe": int(row[8] or 0),
            "purchase_orders_linked_primary": int(row[9] or 0),
            "purchase_orders_with_fact": int(row[10] or 0),
            "purchase_orders_with_actual_cost": int(row[11] or 0),
            "purchase_orders_without_primary_link": int(row[12] or 0),
            "purchase_orders_with_estimated_only": int(row[13] or 0),
            "purchase_orders_linked_but_no_cost": int(row[14] or 0),
            "purchase_orders_missing_actual_cost": int(row[15] or 0),
            "purchase_link_coverage_pct": _to_float(row[16]),
            "purchase_fact_coverage_pct": _to_float(row[17]),
            "purchase_actual_cost_coverage_pct": _to_float(row[18]),
            "shipment_total": int(row[19] or 0),
            "shipment_linked": int(row[20] or 0),
            "shipment_actual_cost": int(row[21] or 0),
            "shipment_link_coverage_pct": _to_float(row[22]),
            "shipment_actual_cost_coverage_pct": _to_float(row[23]),
            "billing_shipments_total": int(row[24] or 0),
            "billing_shipments_linked": int(row[25] or 0),
            "billing_link_coverage_pct": _to_float(row[26]),
            "readiness": str(row[27]),
            "explain": explain if isinstance(explain, list) else [],
        }
        out[(payload["month_token"], payload["carrier"])] = _payload_to_item(payload)
    return out


def get_courier_monthly_kpi_snapshot(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
) -> dict[str, Any]:
    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    rows = load_courier_monthly_kpi_rows(months=months_norm, carriers=carriers_norm)

    matrix: dict[str, dict[str, Any]] = {month: {} for month in months_norm}
    items: list[dict[str, Any]] = []
    missing_pairs: list[dict[str, str]] = []

    for month_token in months_norm:
        for carrier in carriers_norm:
            item = rows.get((month_token, carrier))
            if item is None:
                missing_pairs.append({"month": month_token, "carrier": carrier})
                continue
            matrix.setdefault(month_token, {})[carrier] = item
            items.append(item)

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows": len(items),
        "notes": {
            "amazon_order_coverage": "Order-level denominator: Amazon MFN orders attributable to the carrier, including strong replacement/reshipment relations.",
            "all_shipments_coverage": "Shipment-level denominator: all carrier shipments observed in the month, regardless of Amazon linkage.",
            "billed_shipments_coverage": "Shipment-level denominator: shipments with actual invoiced courier cost in the billing period.",
        },
        "missing_pairs": missing_pairs,
        "items": items,
        "matrix": matrix,
    }


def build_closed_month_readiness_from_snapshot(
    *,
    months: list[str],
    carriers: list[str],
    as_of: date,
    buffer_days: int,
) -> dict[str, Any] | None:
    rows = load_courier_monthly_kpi_rows(months=months, carriers=carriers)
    expected_pairs = {(month, carrier) for month in months for carrier in carriers}
    if set(rows.keys()) != expected_pairs:
        return None

    scopes_total = 0
    scopes_go = 0
    scopes_no_go = 0
    scopes_pending = 0
    matrix: dict[str, dict[str, Any]] = {}

    for month_token in months:
        by_carrier: dict[str, Any] = {}
        month_closed = False
        month_cutoff: str | None = None
        for carrier in carriers:
            item = rows[(month_token, carrier)]
            if item["as_of"] != as_of.isoformat() or int(item["buffer_days"]) != int(buffer_days):
                return None
            month_closed = bool(item["is_closed_by_buffer"])
            month_cutoff = str(item["month_closed_cutoff"])
            readiness = str(item["readiness"])
            if readiness == "GO":
                scopes_total += 1
                scopes_go += 1
            elif readiness == "NO_GO":
                scopes_total += 1
                scopes_no_go += 1
            else:
                scopes_pending += 1
            by_carrier[carrier] = {
                "readiness": readiness,
                "month_closed_cutoff": item["month_closed_cutoff"],
                "as_of": item["as_of"],
                "purchase_month": item["purchase_month"],
                "gaps": {
                    "orders_universe": item["purchase_month"]["orders_universe"],
                    "orders_with_primary_link": item["purchase_month"]["orders_linked_primary"],
                    "orders_without_primary_link": item["purchase_month"]["orders_without_primary_link"],
                    "orders_with_actual_cost": item["purchase_month"]["orders_with_actual_cost"],
                    "orders_with_estimated_only": item["purchase_month"]["orders_with_estimated_only"],
                    "orders_linked_but_no_cost": item["purchase_month"]["orders_linked_but_no_cost"],
                    "orders_missing_actual_cost": item["purchase_month"]["orders_missing_actual_cost"],
                },
                "explain": item["explain"],
            }
        matrix[month_token] = {
            "is_closed_by_buffer": month_closed,
            "month_closed_cutoff": month_cutoff,
            "by_carrier": by_carrier,
        }

    overall = "GO" if scopes_total > 0 and scopes_no_go == 0 else ("PENDING" if scopes_total == 0 else "NO_GO")
    return {
        "overall_go_no_go": overall,
        "as_of": as_of.isoformat(),
        "buffer_days": int(buffer_days),
        "summary": {
            "scopes_total_closed": scopes_total,
            "scopes_go": scopes_go,
            "scopes_no_go": scopes_no_go,
            "scopes_pending": scopes_pending,
        },
        "matrix": matrix,
    }
