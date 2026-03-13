from __future__ import annotations

import pytest

from app.services.courier_link_diagnostics import get_courier_link_gap_diagnostics


class _FakeCursor:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.description = None
        self._one = None
        self._rows = []

    def execute(self, sql: str, params=None) -> None:
        self.executed_sql.append(sql)
        self.description = None
        self._one = None
        self._rows = []

        if "-- courier_link_gap_bucket_unlinked_buckets" in sql:
            self.description = [
                ("bucket",),
                ("shipments",),
                ("shipments_with_actual_cost",),
                ("shipments_with_estimated_only",),
            ]
            self._rows = [
                ("gls_bl_map_tracking_number", 10, 5, 0),
                ("carrier_package_token_match", 8, 3, 1),
            ]
            return

        if "-- courier_link_gap_bucket_source_systems" in sql:
            self.description = [
                ("source_system",),
                ("shipments",),
                ("shipments_with_actual_cost",),
                ("shipments_with_estimated_only",),
            ]
            self._rows = [
                ("gls_billing_files", 12, 8, 1),
                ("shipment_aggregate_gls", 6, 0, 2),
            ]
            return

        if "-- courier_link_gap_bucket_identifier_patterns" in sql:
            self.description = [
                ("identifier_pattern",),
                ("shipments",),
                ("shipments_with_actual_cost",),
                ("shipments_with_estimated_only",),
            ]
            self._rows = [
                ("numeric_core_token", 14, 7, 1),
                ("gls_note1_numeric_unmapped", 11, 2, 0),
            ]
            return

        if "-- courier_source_gap_gls_note1" in sql:
            self._one = (11, 9, 0, 1, 2, 0, 0)
            return

        if "-- courier_source_gap_gls_tracking" in sql:
            self._one = (14, 12, 10, 3, 4, 5, 2)
            return

        if "-- courier_source_gap_dhl_jjd" in sql:
            self._one = (7, 7, 7, 2, 1)
            return

        if "-- courier_source_gap_dhl_numeric" in sql:
            self._one = (15, 14, 6, 0, 0)
            return

        if "-- courier_order_identity_gap_gls_note1_summary" in sql:
            self._one = (11, 9, 4, 5, 1, 4, 4, 7)
            return

        if "-- courier_order_identity_gap_gls_note1_samples" in sql:
            self.description = [
                ("candidate_value",),
                ("shipments",),
                ("break_stage",),
                ("resolution_path",),
                ("resolved_bl_order_id",),
                ("external_order_id",),
                ("acc_order_id",),
            ]
            self._rows = [
                ("93567583", 3, "missing_acc_order", "via_dis_map", 999, "A000AP1E5H", None),
            ]
            return

        if "-- courier_order_identity_gap_dhl_numeric_summary" in sql:
            self._one = (25, 25, 6, 4, 1, 2, 3, 9)
            return

        if "-- courier_order_identity_gap_dhl_numeric_samples" in sql:
            self.description = [
                ("candidate_value",),
                ("shipments",),
                ("break_stage",),
                ("sample_package_order_id",),
                ("sample_resolved_bl_order_id",),
                ("sample_external_order_id",),
                ("sample_acc_order_id",),
                ("package_order_matches",),
                ("external_order_id_matches",),
                ("acc_order_matches",),
            ]
            self._rows = [
                ("29027540240", 2, "missing_acc_order", 123, 999, "A000AP1E5H", None, 1, 1, 0),
            ]
            return

        if "-- courier_order_identity_gap_dhl_jjd_summary" in sql:
            self._one = (25, 25, 3, 3, 2, 1, 1, 1, 4)
            return

        if "-- courier_order_identity_gap_dhl_jjd_samples" in sql:
            self.description = [
                ("candidate_value",),
                ("shipments",),
                ("break_stage",),
                ("sample_parcel_number_base",),
                ("sample_package_order_id",),
                ("sample_resolved_bl_order_id",),
                ("sample_external_order_id",),
                ("sample_acc_order_id",),
                ("parcel_map_matches",),
                ("package_order_matches",),
                ("external_order_id_matches",),
                ("acc_order_matches",),
            ]
            self._rows = [
                ("JJD000030214472000000170450", 4, "missing_acc_order", "29887679017", 123, 999, "A000AP1E5H", None, 1, 1, 1, 0),
            ]
            return

        if "-- courier_link_gap_bucket_summary" in sql:
            self._one = (120, 95, 70, 25, 9, 3, 61, 9, 4)
            return

        if "-- courier_link_gap_summary" in sql:
            self._one = (120, 95, 70, 25, 9, 3, 61, 9, 4)
            return

        if "-- courier_link_gap_unlinked_buckets" in sql:
            self.description = [
                ("bucket",),
                ("shipments",),
                ("shipments_with_actual_cost",),
                ("shipments_with_estimated_only",),
            ]
            self._rows = [
                ("gls_bl_map_tracking_number", 10, 5, 0),
                ("carrier_package_token_match", 8, 3, 1),
                ("missing_all_core_identifiers", 7, 0, 2),
            ]
            return

        if "-- courier_link_gap_cost_buckets" in sql:
            self.description = [
                ("bucket",),
                ("shipments",),
            ]
            self._rows = [
                ("seeded_from_billing_source", 5),
                ("estimated_only", 4),
            ]
            return

        if "-- courier_link_gap_unlinked_samples" in sql:
            self.description = [
                ("shipment_id",),
                ("bucket",),
                ("shipment_number",),
                ("tracking_number",),
                ("piece_id",),
                ("source_system",),
                ("note1",),
                ("has_actual_cost",),
                ("has_estimated_cost",),
                ("observed_at",),
            ]
            self._rows = [
                (
                    "00000000-0000-0000-0000-000000000111",
                    "gls_bl_map_tracking_number",
                    "30646318660",
                    "30646318660",
                    "30646318660",
                    "gls_billing_files",
                    "123456",
                    1,
                    0,
                    "2026-02-20T10:00:00",
                )
            ]
            return

        if "-- courier_link_gap_cost_samples" in sql:
            self.description = [
                ("shipment_id",),
                ("amazon_order_id",),
                ("bucket",),
                ("shipment_number",),
                ("tracking_number",),
                ("piece_id",),
                ("source_system",),
                ("note1",),
                ("has_estimated_cost",),
                ("observed_at",),
            ]
            self._rows = [
                (
                    "00000000-0000-0000-0000-000000000222",
                    "ORDER-1",
                    "seeded_from_billing_source",
                    "30640000000",
                    "30640000000",
                    "30640000000",
                    "gls_billing_files",
                    "654321",
                    0,
                    "2026-02-18T08:00:00",
                )
            ]

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def test_get_courier_link_gap_diagnostics_rejects_unsupported_carrier():
    with pytest.raises(ValueError):
        get_courier_link_gap_diagnostics(months=["2026-02"], carriers=["UPS"])


def test_get_courier_link_gap_diagnostics_runs_read_only(monkeypatch):
    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_link_diagnostics._connect", lambda: fake_conn)

    result = get_courier_link_gap_diagnostics(
        months=["2026-02"],
        carriers=["GLS"],
        created_to_buffer_days=31,
        sample_limit=5,
    )

    sql_dump = "\n".join(fake_conn.cursor_obj.executed_sql).lower()
    assert "netfox" not in sql_dump
    assert "acc_gls_bl_map" in sql_dump
    assert "gm.tracking_number = st.token" in sql_dump
    assert fake_conn.closed is True
    assert result["rows"] == 1
    assert result["items"][0]["summary"]["shipments_without_primary_link"] == 25
    assert result["items"][0]["unlinked_buckets"][0]["bucket"] == "gls_bl_map_tracking_number"
    assert result["items"][0]["sample_unlinked_shipments"][0]["has_actual_cost"] is True
    assert result["items"][0]["sample_linked_no_actual_cost_shipments"][0]["amazon_order_id"] == "ORDER-1"


def test_get_courier_link_gap_summary_runs_read_only(monkeypatch):
    from app.services.courier_link_diagnostics import get_courier_link_gap_summary

    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_link_diagnostics._connect", lambda: fake_conn)

    result = get_courier_link_gap_summary(
        months=["2026-02"],
        carriers=["GLS"],
        created_to_buffer_days=45,
    )

    sql_dump = "\n".join(fake_conn.cursor_obj.executed_sql).lower()
    assert "netfox" not in sql_dump
    assert "acc_gls_bl_map" in sql_dump
    assert fake_conn.closed is True
    assert result["rows"] == 1
    item = result["items"][0]
    assert item["summary"]["shipments_without_primary_link"] == 25
    assert item["summary"]["shipments_without_primary_link_pct"] == 26.32
    assert item["unlinked_buckets"][0]["bucket"] == "gls_bl_map_tracking_number"
    assert item["unlinked_source_systems"][0]["source_system"] == "gls_billing_files"
    assert item["unlinked_identifier_patterns"][0]["identifier_pattern"] == "numeric_core_token"


def test_get_courier_identifier_source_gap_summary_runs_read_only(monkeypatch):
    from app.services.courier_link_diagnostics import get_courier_identifier_source_gap_summary

    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_link_diagnostics._connect", lambda: fake_conn)

    result = get_courier_identifier_source_gap_summary(
        months=["2026-02"],
        carriers=["GLS", "DHL"],
        created_to_buffer_days=45,
    )

    sql_dump = "\n".join(fake_conn.cursor_obj.executed_sql).lower()
    assert "netfox" not in sql_dump
    assert "acc_gls_bl_map" in sql_dump
    assert "acc_dhl_parcel_map" in sql_dump
    assert fake_conn.closed is True
    assert result["rows"] == 2
    gls_item = next(item for item in result["items"] if item["carrier"] == "GLS")
    dhl_item = next(item for item in result["items"] if item["carrier"] == "DHL")
    assert gls_item["focus_areas"][0]["focus_area"] == "gls_note1_numeric_unmapped"
    assert gls_item["focus_areas"][1]["values_in_gls_bl_map"] == 10
    assert dhl_item["focus_areas"][0]["values_in_dhl_parcel_map"] == 7
    assert dhl_item["focus_areas"][1]["focus_area"] == "dhl_numeric_core"


def test_get_courier_order_identity_gap_summary_runs_read_only(monkeypatch):
    from app.services.courier_link_diagnostics import get_courier_order_identity_gap_summary

    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_link_diagnostics._connect", lambda: fake_conn)

    result = get_courier_order_identity_gap_summary(
        months=["2026-02"],
        carriers=["GLS", "DHL"],
        created_to_buffer_days=45,
        sample_limit=5,
    )

    sql_dump = "\n".join(fake_conn.cursor_obj.executed_sql).lower()
    assert "netfox" not in sql_dump
    assert "acc_cache_bl_orders" in sql_dump
    assert "acc_cache_packages" in sql_dump
    assert "acc_dhl_parcel_map" in sql_dump
    assert fake_conn.closed is True
    assert result["rows"] == 2

    gls_item = next(item for item in result["items"] if item["carrier"] == "GLS")
    dhl_item = next(item for item in result["items"] if item["carrier"] == "DHL")

    assert gls_item["focus_areas"][0]["focus_area"] == "gls_note1_order_identity"
    assert gls_item["focus_areas"][0]["values_missing_acc_order"] == 4
    assert gls_item["focus_areas"][0]["broken_identity_samples"][0]["external_order_id"] == "A000AP1E5H"
    assert dhl_item["focus_areas"][0]["focus_area"] == "dhl_numeric_order_identity"
    assert dhl_item["focus_areas"][1]["focus_area"] == "dhl_jjd_order_identity"
    assert dhl_item["focus_areas"][1]["broken_identity_samples"][0]["sample_parcel_number_base"] == "29887679017"
