from __future__ import annotations

from datetime import date

import pytest

from app.services.courier_order_universe_linking import backfill_order_links_order_universe


class _FakeCursor:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def execute(self, sql: str, params=None) -> None:
        self.executed_sql.append(sql)

    def fetchone(self):
        return (20, 15, 14, 33, 80, 6, 6, 4, 2, 1, 1, 0)


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


def test_backfill_order_links_order_universe_rejects_unsupported_carrier():
    with pytest.raises(ValueError):
        backfill_order_links_order_universe(
            carrier="UPS",
            purchase_from=date(2025, 12, 1),
            purchase_to=date(2025, 12, 31),
        )


def test_backfill_order_links_order_universe_runs_without_netfox(monkeypatch):
    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_order_universe_linking._connect", lambda: fake_conn)

    result = backfill_order_links_order_universe(
        carrier="DHL",
        purchase_from=date(2025, 12, 1),
        purchase_to=date(2025, 12, 31),
        reset_existing_in_scope=True,
    )

    sql_dump = "\n".join(fake_conn.cursor_obj.executed_sql).lower()
    assert "netfox" not in sql_dump
    assert "acc_cache_bl_orders" in sql_dump
    assert "acc_cache_packages" in sql_dump
    assert "acc_cache_dis_map" in sql_dump
    assert "acc_order_courier_relation" in sql_dump
    assert "acc_dhl_parcel_map" in sql_dump
    assert "charindex('dhl'" in sql_dump
    assert "like '%dhl%'" not in sql_dump
    assert "delete l" in sql_dump
    assert fake_conn.committed is True
    assert fake_conn.closed is True
    assert result["carrier"] == "DHL"
    assert result["shipments_in_scope"] == 20
    assert result["shipments_with_primary_link"] == 14


def test_backfill_order_links_order_universe_adds_gls_specific_sources(monkeypatch):
    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_order_universe_linking._connect", lambda: fake_conn)

    backfill_order_links_order_universe(
        carrier="GLS",
        purchase_from=date(2026, 1, 1),
        purchase_to=date(2026, 1, 31),
    )

    sql_dump = "\n".join(fake_conn.cursor_obj.executed_sql).lower()
    assert "acc_gls_bl_map" in sql_dump
    assert "join #ship_tokens st" in sql_dump
    assert "gm.tracking_number = st.token" in sql_dump
    assert "dm_note1.dis_order_id" in sql_dump
    assert "charindex('gls'" in sql_dump
    assert "like '%gls%'" not in sql_dump
