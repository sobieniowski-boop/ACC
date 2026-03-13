from __future__ import annotations

import pytest

from app.services.gls_cost_sync import (
    ShipmentCostTarget,
    _apply_billing_corrections,
    _extract_identifiers,
    _load_cost_targets,
    _normalize_billing_period,
    _normalize_billing_period_filters,
    _select_actual_cost,
    sync_gls_shipment_costs,
)
from app.services.gls_logistics_aggregation import _classify_shadow_status


def test_extract_identifiers_uses_shipment_tokens():
    shipment = ShipmentCostTarget(
        shipment_id="00000000-0000-0000-0000-000000000001",
        shipment_number="30646220044",
        tracking_number="30646220044",
        piece_id="GLS-PIECE-1",
        source_payload_json=None,
    )
    identifiers = _extract_identifiers(shipment)
    values = {item.value for item in identifiers}
    assert "30646220044" in values
    assert "GLS-PIECE-1" in values


def test_select_actual_cost_matches_parcel_number():
    shipment = ShipmentCostTarget(
        shipment_id="00000000-0000-0000-0000-000000000001",
        shipment_number="30646220044",
        tracking_number=None,
        piece_id=None,
        source_payload_json=None,
    )
    actual = _select_actual_cost(
        identifiers=_extract_identifiers(shipment),
        billing_costs={
            "30646220044": {
                "parcel_number": "30646220044",
                "line_count": 1,
                "doc_count": 1,
                "first_document_number": "6501031953",
                "last_document_number": "6501031953",
                "billing_period": "2026.02",
                "first_row_date": None,
                "last_row_date": None,
                "last_delivery_date": None,
                "net_amount": 21.81,
                "toll_amount": 0.98,
                "fuel_amount": 3.2715,
                "storewarehouse_amount": 0.0,
                "surcharge_amount": 0.0,
                "gross_amount": 26.0615,
            }
        },
    )
    assert actual is not None
    assert actual["cost_source"] == "gls_billing_files"
    assert actual["resolved_via"] == "parcel_number"
    assert actual["gross_amount"] == 26.0615


def test_apply_billing_corrections_overrides_base_gls_cost():
    result = _apply_billing_corrections(
        {
            "30645415621": {
                "parcel_number": "30645415621",
                "line_count": 1,
                "doc_count": 1,
                "first_document_number": "6500900532",
                "last_document_number": "6500900532",
                "billing_period": "2025-11",
                "first_row_date": None,
                "last_row_date": None,
                "last_delivery_date": None,
                "net_amount": 65.51,
                "toll_amount": 0.98,
                "fuel_amount": 9.8265,
                "storewarehouse_amount": 0.0,
                "surcharge_amount": 0.0,
                "gross_amount": 76.3165,
            }
        },
        {
            "30645415621": {
                "parcel_number": "30645415621",
                "document_number": "6500900532",
                "issue_date": None,
                "sales_date": None,
                "original_net_amount": 65.51,
                "corrected_net_amount": 22.74,
                "net_delta_amount": 42.77,
                "fuel_rate_pct": 0.15,
                "fuel_correction_amount": 6.4155,
                "toll_amount": 0.98,
                "source_file": "N:/KURIERZY/GLS POLSKA/Korekty kosztowe/12.2025/Specyfikacja Netfox 6500900532.xlsx",
            }
        },
    )

    actual = result["30645415621"]
    assert actual["correction_applied"] is True
    assert actual["net_amount"] == 22.74
    assert actual["fuel_amount"] == pytest.approx(3.411, abs=0.0001)
    assert actual["gross_amount"] == pytest.approx(27.131, abs=0.0001)
    assert actual["correction_delta_gross_amount"] == pytest.approx(49.1855, abs=0.0001)


def test_normalize_billing_period_converts_gls_dot_format_to_canonical_month():
    assert _normalize_billing_period("2026.02") == "2026-02"
    assert _normalize_billing_period("2026-02") == "2026-02"
    assert _normalize_billing_period(None) is None


def test_normalize_billing_period_filters_deduplicates_and_canonicalizes():
    assert _normalize_billing_period_filters(["2026.02", "2026-02", "2026-03"]) == [
        "2026-02",
        "2026-03",
    ]


class _FakeCursor:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.executed_params: list[list[object]] = []

    def execute(self, sql: str, params=None) -> None:
        self.executed_sql.append(sql)
        self.executed_params.append(list(params or []))

    def fetchall(self):
        return [
            (
                "00000000-0000-0000-0000-000000000001",
                "30646220044",
                "30646220044",
                "30646220044",
                '{"billing_period":"2026.02"}',
            )
        ]


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def test_load_cost_targets_supports_safe_backfill_filters(monkeypatch):
    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.gls_cost_sync._connect", lambda: fake_conn)

    rows = _load_cost_targets(
        created_from=None,
        created_to=None,
        limit_shipments=100,
        refresh_existing=False,
        billing_periods=["2026.02"],
        seeded_only=True,
        only_primary_linked=True,
    )

    sql_dump = "\n".join(fake_conn.cursor_obj.executed_sql).lower()
    params = fake_conn.cursor_obj.executed_params[-1]
    assert "json_value(s.source_payload_json, '$.billing_period')" in sql_dump
    assert "acc_gls_billing_correction_line" in sql_dump
    assert "s.source_system = 'gls_billing_files'" in sql_dump
    assert "from dbo.acc_shipment_order_link l with (nolock)" in sql_dump
    assert "and l.is_primary = 1" in sql_dump
    assert "2026-02" in params
    assert rows[0].shipment_number == "30646220044"
    assert fake_conn.closed is True


def test_sync_gls_shipment_costs_passes_safe_backfill_filters(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr("app.services.gls_cost_sync.ensure_gls_schema", lambda: None)
    monkeypatch.setattr("app.services.gls_cost_sync.ensure_dhl_schema", lambda: None)

    def _fake_loader(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr("app.services.gls_cost_sync._load_cost_targets", _fake_loader)

    result = sync_gls_shipment_costs(
        billing_periods=["2026.01", "2026-02"],
        seeded_only=True,
        only_primary_linked=True,
        limit_shipments=250,
    )

    assert captured["billing_periods"] == ["2026-01", "2026-02"]
    assert captured["seeded_only"] is True
    assert captured["only_primary_linked"] is True
    assert result["shipments_selected"] == 0
    assert result["billing_periods"] == ["2026-01", "2026-02"]
    assert result["seeded_only"] is True
    assert result["only_primary_linked"] is True


def test_shadow_status_classification():
    assert _classify_shadow_status(0.0, 0.0) == "match_zero"
    assert _classify_shadow_status(10.0, 10.02) == "match"
    assert _classify_shadow_status(12.0, 0.0) == "legacy_only"
    assert _classify_shadow_status(0.0, 9.0) == "shadow_only"
    assert _classify_shadow_status(12.0, 9.0) == "delta"
