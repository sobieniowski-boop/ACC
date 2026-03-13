from __future__ import annotations

from app.services.courier_identifier_backfill import CandidateValue, _candidate_sql, run_courier_identifier_backfill


class _NetfoxCursor:
    def __init__(self, rows_by_marker: dict[str, list[tuple]]) -> None:
        self.rows_by_marker = rows_by_marker
        self.executed: list[str] = []
        self._last_sql = ""

    def execute(self, sql: str, params=None):
        self.executed.append(sql)
        self._last_sql = sql
        return self

    def fetchall(self):
        for marker, rows in self.rows_by_marker.items():
            if marker in self._last_sql:
                return rows
        return []


class _NetfoxConn:
    def __init__(self, rows_by_marker: dict[str, list[tuple]]) -> None:
        self.cursor_obj = _NetfoxCursor(rows_by_marker)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


class _AccCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql: str, params=None):
        self.executed.append((sql, params))
        return self


class _AccConn:
    def __init__(self) -> None:
        self.cursor_obj = _AccCursor()
        self.commit_count = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        self.closed = True


def test_run_courier_identifier_backfill_gls_note1_writes_cache_rows(monkeypatch):
    acc_conn = _AccConn()
    netfox_conn = _NetfoxConn(
        {
            "courier_identifier_backfill_netfox_dis_map": [(999, 12345)],
            "courier_identifier_backfill_netfox_bl_orders": [(999, "AMZ-1")],
            "courier_identifier_backfill_netfox_packages_by_order": [
                (12345, "GLS-RAW", "INNER-1", "gls", "GLS Poland"),
                (999, "GLS-HOLDING", "INNER-2", "gls", "GLS Poland"),
            ],
        }
    )

    monkeypatch.setattr(
        "app.services.courier_identifier_backfill.ensure_courier_identifier_cache_schema",
        lambda: None,
    )
    monkeypatch.setattr(
        "app.services.courier_identifier_backfill._load_candidates_for_month",
        lambda **kwargs: [CandidateValue(value="12345", shipments=7, shipments_with_actual_cost=6)],
    )
    monkeypatch.setattr("app.services.courier_identifier_backfill._connect_acc", lambda: acc_conn)
    monkeypatch.setattr("app.services.courier_identifier_backfill.connect_netfox", lambda timeout=15: netfox_conn)

    result = run_courier_identifier_backfill(
        mode="gls_note1",
        months=["2026-02"],
    )

    sql_dump = "\n".join(sql for sql, _ in acc_conn.cursor_obj.executed)
    assert result["totals"]["candidate_values"] == 1
    assert result["totals"]["resolved_order_ids"] == 2
    assert result["totals"]["acc_package_rows_written"] == 2
    assert result["totals"]["acc_bl_order_rows_written"] == 1
    assert result["totals"]["acc_dis_map_rows_written"] == 1
    assert "MERGE dbo.acc_cache_packages" in sql_dump
    assert "MERGE dbo.acc_cache_bl_orders" in sql_dump
    assert "MERGE dbo.acc_cache_dis_map" in sql_dump
    assert acc_conn.commit_count >= 3
    assert acc_conn.closed is True
    assert netfox_conn.closed is True


def test_run_courier_identifier_backfill_dhl_jjd_writes_parcel_map(monkeypatch):
    acc_conn = _AccConn()
    netfox_conn = _NetfoxConn(
        {
            "courier_identifier_backfill_dhl_jjd_netfox_lookup": [("JJD123", "1234567890", 777)],
            "courier_identifier_backfill_netfox_dis_map": [],
            "courier_identifier_backfill_netfox_bl_orders": [(777, "AMZ-2")],
            "courier_identifier_backfill_netfox_packages_by_order": [
                (777, "1234567890", None, "dhl", "DHL"),
            ],
        }
    )

    monkeypatch.setattr(
        "app.services.courier_identifier_backfill.ensure_courier_identifier_cache_schema",
        lambda: None,
    )
    monkeypatch.setattr(
        "app.services.courier_identifier_backfill._load_candidates_for_month",
        lambda **kwargs: [CandidateValue(value="JJD123", shipments=3, shipments_with_actual_cost=3)],
    )
    monkeypatch.setattr("app.services.courier_identifier_backfill._connect_acc", lambda: acc_conn)
    monkeypatch.setattr("app.services.courier_identifier_backfill.connect_netfox", lambda timeout=15: netfox_conn)
    monkeypatch.setattr("app.services.dhl_integration.ensure_dhl_schema", lambda: None)

    result = run_courier_identifier_backfill(
        mode="dhl_jjd",
        months=["2026-02"],
    )

    sql_dump = "\n".join(sql for sql, _ in acc_conn.cursor_obj.executed)
    assert result["totals"]["candidate_values"] == 1
    assert result["totals"]["netfox_jjd_rows"] == 1
    assert result["totals"]["acc_dhl_parcel_map_rows_written"] == 1
    assert "MERGE dbo.acc_dhl_parcel_map" in sql_dump
    assert "MERGE dbo.acc_cache_packages" in sql_dump
    assert "MERGE dbo.acc_cache_bl_orders" in sql_dump
    assert acc_conn.closed is True
    assert netfox_conn.closed is True


def test_run_courier_identifier_backfill_external_order_mode_writes_only_bl_orders(monkeypatch):
    acc_conn = _AccConn()
    netfox_conn = _NetfoxConn(
        {
            "courier_identifier_backfill_netfox_bl_orders": [(12345, "403-7309572-9322704")],
        }
    )

    monkeypatch.setattr(
        "app.services.courier_identifier_backfill.ensure_courier_identifier_cache_schema",
        lambda: None,
    )
    monkeypatch.setattr(
        "app.services.courier_identifier_backfill._load_candidates_for_month",
        lambda **kwargs: [CandidateValue(value="12345", shipments=9, shipments_with_actual_cost=9)],
    )
    monkeypatch.setattr("app.services.courier_identifier_backfill._connect_acc", lambda: acc_conn)
    monkeypatch.setattr("app.services.courier_identifier_backfill.connect_netfox", lambda timeout=15: netfox_conn)

    result = run_courier_identifier_backfill(
        mode="dhl_numeric_external_order",
        months=["2026-02"],
    )

    sql_dump = "\n".join(sql for sql, _ in acc_conn.cursor_obj.executed)
    assert result["totals"]["candidate_values"] == 1
    assert result["totals"]["resolved_order_ids"] == 1
    assert result["totals"]["acc_bl_order_rows_written"] == 1
    assert result["totals"]["acc_package_rows_written"] == 0
    assert result["totals"]["acc_dis_map_rows_written"] == 0
    assert "MERGE dbo.acc_cache_bl_orders" in sql_dump
    assert "MERGE dbo.acc_cache_packages" not in sql_dump
    assert "MERGE dbo.acc_cache_dis_map" not in sql_dump
    assert acc_conn.closed is True
    assert netfox_conn.closed is True


def test_candidate_sql_uses_charindex_for_dhl_package_matching():
    sql = _candidate_sql("dhl_numeric_external_order", limit_values=10).lower()

    assert "charindex('dhl'" in sql
    assert "like '%dhl%'" not in sql
