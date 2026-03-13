from __future__ import annotations

from datetime import date

import app.services.courier_monthly_kpi as monthly_kpi
from app.services.courier_readiness import get_courier_closed_month_readiness


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = object()
        self.commit_calls = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


def _snapshot_item(
    *,
    month: str = "2026-01",
    carrier: str = "DHL",
    readiness: str = "GO",
    as_of: str = "2026-03-20",
    buffer_days: int = 45,
    is_closed_by_buffer: bool = True,
    orders_universe: int = 10,
    orders_with_actual_cost: int | None = None,
    orders_without_primary_link: int = 0,
    orders_with_estimated_only: int | None = None,
    orders_linked_but_no_cost: int | None = None,
    orders_missing_actual_cost: int | None = None,
) -> dict:
    actual_orders = orders_with_actual_cost if orders_with_actual_cost is not None else (10 if readiness == "GO" else 8)
    estimated_only = orders_with_estimated_only if orders_with_estimated_only is not None else (0 if readiness == "GO" else 1)
    linked_no_cost = orders_linked_but_no_cost if orders_linked_but_no_cost is not None else (0 if readiness == "GO" else 1)
    missing_actual = (
        orders_missing_actual_cost
        if orders_missing_actual_cost is not None
        else max(0, orders_universe - actual_orders)
    )
    explain = []
    if orders_without_primary_link > 0:
        explain.append({"code": "missing_primary_link", "orders": orders_without_primary_link})
    if estimated_only > 0:
        explain.append({"code": "estimated_only", "orders": estimated_only})
    if linked_no_cost > 0:
        explain.append({"code": "linked_without_cost", "orders": linked_no_cost})
    return monthly_kpi._payload_to_item(
        {
            "month_token": month,
            "month_start": f"{month}-01",
            "carrier": carrier,
            "calc_version": "dhl_v1" if carrier == "DHL" else "gls_v1",
            "as_of_date": as_of,
            "buffer_days": buffer_days,
            "is_closed_by_buffer": is_closed_by_buffer,
            "month_closed_cutoff": "2026-03-18",
            "purchase_orders_universe": orders_universe,
            "purchase_orders_linked_primary": max(0, orders_universe - orders_without_primary_link),
            "purchase_orders_with_fact": max(0, orders_universe - orders_without_primary_link),
            "purchase_orders_with_actual_cost": actual_orders,
            "purchase_orders_without_primary_link": orders_without_primary_link,
            "purchase_orders_with_estimated_only": estimated_only,
            "purchase_orders_linked_but_no_cost": linked_no_cost,
            "purchase_orders_missing_actual_cost": missing_actual,
            "purchase_link_coverage_pct": 100.0,
            "purchase_fact_coverage_pct": 100.0,
            "purchase_actual_cost_coverage_pct": round((actual_orders / orders_universe) * 100, 2) if orders_universe else 0.0,
            "shipment_total": 12,
            "shipment_linked": 11,
            "shipment_actual_cost": 10,
            "shipment_link_coverage_pct": 91.67,
            "shipment_actual_cost_coverage_pct": 83.33,
            "billing_shipments_total": 10,
            "billing_shipments_linked": 9,
            "billing_link_coverage_pct": 90.0,
            "readiness": readiness,
            "explain": explain,
        }
    )


def test_refresh_courier_monthly_kpi_snapshot_builds_closed_month_rows(monkeypatch):
    fake_conn = _FakeConn()
    upserts: list[dict] = []

    monkeypatch.setattr(monthly_kpi, "ensure_courier_monthly_kpi_schema", lambda: None)
    monkeypatch.setattr(monthly_kpi, "_connect", lambda: fake_conn)
    monkeypatch.setattr(
        monthly_kpi,
        "_coverage_snapshot",
        lambda **kwargs: {
            "orders_linked_primary": 10,
            "orders_with_fact": 10,
            "link_coverage_pct": 100.0,
            "fact_coverage_pct": 100.0,
        },
    )
    monkeypatch.setattr(
        monthly_kpi,
        "_order_level_gap_breakdown",
        lambda **kwargs: {
            "orders_universe": 10,
            "orders_with_primary_link": 10,
            "orders_without_primary_link": 0,
            "orders_with_actual_cost": 10,
            "orders_with_estimated_only": 0,
            "orders_linked_but_no_cost": 0,
            "orders_missing_actual_cost": 0,
        },
    )
    monkeypatch.setattr(
        monthly_kpi,
        "_shipment_month_coverage",
        lambda **kwargs: {
            "shipments_total": 12,
            "linked_shipments": 11,
            "costed_shipments_actual": 10,
            "link_coverage_pct": 91.67,
            "cost_coverage_pct": 83.33,
        },
    )
    monkeypatch.setattr(
        monthly_kpi,
        "_billing_period_coverage",
        lambda **kwargs: {
            "billed_shipments_total": 10,
            "billed_shipments_linked": 9,
            "link_coverage_pct": 90.0,
        },
    )
    monkeypatch.setattr(monthly_kpi, "_upsert_snapshot_row", lambda cur, payload: upserts.append(payload.copy()))

    result = monthly_kpi.refresh_courier_monthly_kpi_snapshot(
        months=["2026-01"],
        carriers=["DHL"],
        buffer_days=45,
        as_of=date(2026, 3, 20),
    )

    assert result["rows_upserted"] == 1
    assert result["items"][0]["readiness"] == "GO"
    assert result["items"][0]["amazon_order_coverage"]["orders_with_actual_cost"] == 10
    assert result["items"][0]["all_shipments_coverage"]["shipments_total"] == 12
    assert result["items"][0]["operational"]["status"] == "CLOSED_GO"
    assert upserts[0]["purchase_actual_cost_coverage_pct"] == 100.0
    assert upserts[0]["billing_shipments_linked"] == 9
    assert fake_conn.commit_calls == 1
    assert fake_conn.closed is True


def test_get_courier_monthly_kpi_snapshot_reports_missing_pairs(monkeypatch):
    item = _snapshot_item()
    monkeypatch.setattr(
        monthly_kpi,
        "load_courier_monthly_kpi_rows",
        lambda **kwargs: {
            ("2026-01", "DHL"): item,
        },
    )

    result = monthly_kpi.get_courier_monthly_kpi_snapshot(
        months=["2026-01"],
        carriers=["DHL", "GLS"],
    )

    assert result["rows"] == 1
    assert result["matrix"]["2026-01"]["DHL"]["purchase_month"]["actual_cost_coverage_pct"] == 100.0
    assert result["matrix"]["2026-01"]["DHL"]["amazon_order_coverage"]["actual_cost_coverage_pct"] == 100.0
    assert result["missing_pairs"] == [{"month": "2026-01", "carrier": "GLS"}]


def test_build_closed_month_readiness_from_snapshot_aggregates_rows(monkeypatch):
    monkeypatch.setattr(
        monthly_kpi,
        "load_courier_monthly_kpi_rows",
        lambda **kwargs: {
            ("2026-01", "DHL"): _snapshot_item(),
        },
    )

    result = monthly_kpi.build_closed_month_readiness_from_snapshot(
        months=["2026-01"],
        carriers=["DHL"],
        as_of=date(2026, 3, 20),
        buffer_days=45,
    )

    assert result is not None
    assert result["overall_go_no_go"] == "GO"
    assert result["summary"]["scopes_go"] == 1
    assert result["matrix"]["2026-01"]["by_carrier"]["DHL"]["gaps"]["orders_missing_actual_cost"] == 0


def test_payload_to_item_classifies_open_month_gap_states():
    awaiting_invoice = _snapshot_item(
        month="2026-02",
        readiness="PENDING",
        is_closed_by_buffer=False,
        orders_universe=10,
        orders_with_actual_cost=7,
        orders_without_primary_link=0,
        orders_with_estimated_only=3,
        orders_linked_but_no_cost=0,
        orders_missing_actual_cost=3,
    )
    assert awaiting_invoice["operational"]["status"] == "OPEN_AWAITING_INVOICES"
    assert awaiting_invoice["operational"]["primary_gap_driver"] == "estimated_only"
    assert awaiting_invoice["operational"]["gap_orders"]["cost_pending_after_link"] == 3

    linked_no_cost = _snapshot_item(
        month="2026-02",
        readiness="PENDING",
        is_closed_by_buffer=False,
        orders_universe=10,
        orders_with_actual_cost=8,
        orders_without_primary_link=0,
        orders_with_estimated_only=0,
        orders_linked_but_no_cost=2,
        orders_missing_actual_cost=2,
    )
    assert linked_no_cost["operational"]["status"] == "OPEN_LINKED_NO_COST"
    assert linked_no_cost["operational"]["primary_gap_driver"] == "linked_no_cost"

    link_gap = _snapshot_item(
        month="2026-02",
        readiness="PENDING",
        is_closed_by_buffer=False,
        orders_universe=10,
        orders_with_actual_cost=8,
        orders_without_primary_link=2,
        orders_with_estimated_only=0,
        orders_linked_but_no_cost=0,
        orders_missing_actual_cost=2,
    )
    assert link_gap["operational"]["status"] == "OPEN_LINK_GAP"
    assert link_gap["operational"]["primary_gap_driver"] == "missing_primary_link"

    mixed = _snapshot_item(
        month="2026-02",
        readiness="PENDING",
        is_closed_by_buffer=False,
        orders_universe=10,
        orders_with_actual_cost=5,
        orders_without_primary_link=2,
        orders_with_estimated_only=3,
        orders_linked_but_no_cost=0,
        orders_missing_actual_cost=5,
    )
    assert mixed["operational"]["status"] == "OPEN_MIXED"
    assert mixed["operational"]["primary_gap_driver"] == "estimated_only"


def test_closed_month_readiness_uses_snapshot_when_current_as_of(monkeypatch):
    expected = {
        "overall_go_no_go": "GO",
        "as_of": date.today().isoformat(),
        "buffer_days": 45,
        "summary": {
            "scopes_total_closed": 1,
            "scopes_go": 1,
            "scopes_no_go": 0,
            "scopes_pending": 0,
        },
        "matrix": {"2026-01": {"is_closed_by_buffer": True, "by_carrier": {}}},
    }

    monkeypatch.setattr(
        monthly_kpi,
        "build_closed_month_readiness_from_snapshot",
        lambda **kwargs: expected,
    )
    monkeypatch.setattr(
        "app.services.courier_readiness._coverage_snapshot",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("live coverage should not run")),
    )

    result = get_courier_closed_month_readiness(
        months=["2026-01"],
        carriers=["DHL"],
        as_of=date.today(),
        buffer_days=45,
    )

    assert result == expected
