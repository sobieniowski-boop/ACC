from __future__ import annotations

from datetime import date, datetime, timezone

from app.services import bl_distribution_cache as svc


class _RecordingCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql: str, params=None):
        self.executed.append((sql, params))
        return self


class _RecordingConnection:
    def __init__(self) -> None:
        self._cursor = _RecordingCursor()
        self.commit_count = 0
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


def test_discover_distribution_source_ids_filters_numeric_blconnect(monkeypatch):
    monkeypatch.setattr(
        svc,
        "_call_baselinker",
        lambda method, params: {
            "sources": {
                "blconnect": {"645": "JDG-Distribution", "2952": "Amazon DIS", "foo": "ignore"},
                "personal": {"31888": "personal"},
            }
        },
    )

    assert svc.discover_distribution_source_ids() == [645, 2952]


def test_sync_bl_distribution_order_cache_syncs_orders_and_packages(monkeypatch):
    conn = _RecordingConnection()
    from_day = date(2025, 12, 1)
    from_ts = int(datetime(2025, 12, 1, tzinfo=timezone.utc).timestamp())

    order_page = [
        {
            "order_id": 25558607,
            "shop_order_id": "7001",
            "order_source": "blconnect",
            "order_source_id": 2952,
            "date_confirmed": from_ts,
            "confirmed": True,
            "delivery_package_nr": "H0510A0042265861",
            "external_order_id": "",
        },
        {
            "order_id": 25558608,
            "shop_order_id": "7002",
            "order_source": "blconnect",
            "order_source_id": 2952,
            "date_confirmed": from_ts + 60,
            "confirmed": True,
            "delivery_package_nr": "",
            "external_order_id": "AMZ-123",
        },
    ]

    def _fake_fetch_orders_page(*, source_id: int, cursor_ts: int):
        assert source_id == 2952
        if cursor_ts == from_ts:
            return order_page
        return []

    def _fake_fetch_packages(*, order_id: int):
        if order_id == 25558607:
            return [
                {
                    "package_id": 91001,
                    "courier_package_nr": "GLS123",
                    "courier_inner_number": "INNER123",
                    "courier_code": "gls",
                    "courier_other_name": "GLS Poland",
                    "tracking_status": "delivered",
                },
                {
                    "package_id": 91002,
                    "courier_package_nr": "DHL123",
                    "courier_inner_number": "INNER124",
                    "courier_code": "dhl",
                    "courier_other_name": "DHL",
                    "tracking_status": "in_transit",
                },
            ]
        return []

    monkeypatch.setattr(svc, "ensure_bl_distribution_cache_schema", lambda: None)
    monkeypatch.setattr(svc, "_connect", lambda: conn)
    monkeypatch.setattr(svc, "discover_distribution_source_ids", lambda: [2952])
    monkeypatch.setattr(svc, "_fetch_distribution_orders_page", _fake_fetch_orders_page)
    monkeypatch.setattr(svc, "_fetch_order_packages", _fake_fetch_packages)
    monkeypatch.setattr(svc.time, "sleep", lambda _: None)
    monkeypatch.setattr(svc.settings, "BASELINKER_DISTRIBUTION_TOKEN", "token", raising=False)
    monkeypatch.setattr(svc.settings, "BASELINKER_API", "https://api.baselinker.com/connector.php", raising=False)

    result = svc.sync_bl_distribution_order_cache(
        date_confirmed_from=from_day,
        date_confirmed_to=date(2025, 12, 2),
        source_ids=[2952],
        include_packages=True,
        limit_orders=10,
    )

    assert result["orders_synced"] == 2
    assert result["packages_synced"] == 2
    assert result["orders_with_delivery_package_nr"] == 1
    assert result["orders_with_external_order_id"] == 1
    assert result["api_calls"] == 3
    assert conn.commit_count >= 1
    assert conn.closed is True
    assert any("acc_bl_distribution_order_cache" in sql for sql, _ in conn._cursor.executed)
    assert any("acc_bl_distribution_package_cache" in sql for sql, _ in conn._cursor.executed)
