"""Tests for Sprint 19 – SQS Queue Topology.

Covers: constants, row mappers, queue registration, queue status,
topology health, DLQ lifecycle, event routing, multi-queue polling,
seed topology, API endpoints, and edge cases.
"""
from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.intelligence import sqs_topology as topo

# ── Local fakes ──────────────────────────────────────────────────────


class _FakeCursor:
    """Cursor that feeds rows from a pre-loaded list."""

    def __init__(self):
        self.executed: list[tuple] = []
        self.multi_rows: list = []
        self._idx = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        if self._idx < len(self.multi_rows):
            row = self.multi_rows[self._idx]
            self._idx += 1
            return row
        return None

    def fetchall(self):
        if self._idx < len(self.multi_rows):
            rest = self.multi_rows[self._idx:]
            self._idx = len(self.multi_rows)
            return rest
        return []

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor | None = None):
        self._cursor = cursor or _FakeCursor()

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


# ── Helpers ──────────────────────────────────────────────────────────

def _make_topology_row(
    id=1,
    domain="pricing",
    queue_url="https://sqs.eu-west-1.amazonaws.com/123/acc-pricing",
    queue_arn=None,
    dlq_url="https://sqs.eu-west-1.amazonaws.com/123/acc-pricing-dlq",
    dlq_arn=None,
    region="eu-west-1",
    max_receive_count=3,
    visibility_timeout_seconds=30,
    message_retention_days=14,
    polling_interval_seconds=120,
    batch_size=10,
    enabled=True,
    status="active",
    messages_received=100,
    messages_processed=95,
    messages_failed=5,
    messages_dlq=2,
    last_poll_at=None,
    last_error=None,
    created_at="2026-03-12 10:00:00",
    updated_at="2026-03-12 10:00:00",
):
    return (
        id, domain, queue_url, queue_arn, dlq_url, dlq_arn, region,
        max_receive_count, visibility_timeout_seconds, message_retention_days,
        polling_interval_seconds, batch_size, enabled, status,
        messages_received, messages_processed, messages_failed, messages_dlq,
        last_poll_at, last_error, created_at, updated_at,
    )


def _make_dlq_row(
    id=1,
    domain="pricing",
    queue_url="https://sqs.eu-west-1.amazonaws.com/123/acc-pricing-dlq",
    message_id="msg-abc-123",
    receipt_handle="rh-xyz",
    body='{"type": "ANY_OFFER_CHANGED"}',
    approximate_receive_count=3,
    original_event_id="evt-001",
    error_message="Processing timeout",
    status="unresolved",
    resolution=None,
    resolved_by=None,
    resolved_at=None,
    created_at="2026-03-12 10:05:00",
):
    return (
        id, domain, queue_url, message_id, receipt_handle, body,
        approximate_receive_count, original_event_id, error_message,
        status, resolution, resolved_by, resolved_at, created_at,
    )


_CONN = "app.intelligence.sqs_topology.connect_acc"


# ── S19-01: Constants ────────────────────────────────────────────────


class TestS19Constants:
    def test_valid_domains(self):
        assert topo.VALID_DOMAINS == {"pricing", "listing", "order", "inventory", "report", "feed"}

    def test_valid_domains_count(self):
        assert len(topo.VALID_DOMAINS) == 6

    def test_valid_queue_statuses(self):
        assert "active" in topo.VALID_QUEUE_STATUSES
        assert "paused" in topo.VALID_QUEUE_STATUSES
        assert "error" in topo.VALID_QUEUE_STATUSES
        assert "disabled" in topo.VALID_QUEUE_STATUSES

    def test_valid_dlq_statuses(self):
        for s in ("unresolved", "replayed", "discarded", "investigating"):
            assert s in topo.VALID_DLQ_STATUSES

    def test_valid_dlq_resolutions(self):
        for r in ("replayed", "discarded", "investigating"):
            assert r in topo.VALID_DLQ_RESOLUTIONS

    def test_default_queue_configs_all_domains(self):
        for domain in topo.VALID_DOMAINS:
            assert domain in topo.DEFAULT_QUEUE_CONFIG
            cfg = topo.DEFAULT_QUEUE_CONFIG[domain]
            assert "max_receive_count" in cfg
            assert "visibility_timeout_seconds" in cfg
            assert "batch_size" in cfg

    def test_notification_routing_all_types(self):
        expected_types = {
            "ANY_OFFER_CHANGED", "LISTINGS_ITEM_STATUS_CHANGE",
            "LISTINGS_ITEM_ISSUES_CHANGE", "REPORT_PROCESSING_FINISHED",
            "FBA_INVENTORY_AVAILABILITY_CHANGES", "ORDER_STATUS_CHANGE",
            "FEED_PROCESSING_FINISHED", "ITEM_PRODUCT_TYPE_CHANGE",
            "BRANDED_ITEM_CONTENT_CHANGE",
        }
        assert set(topo.NOTIFICATION_ROUTING.keys()) == expected_types

    def test_notification_routing_domains(self):
        routed_domains = set(topo.NOTIFICATION_ROUTING.values())
        assert routed_domains == {"pricing", "listing", "report", "inventory", "order", "feed"}

    def test_order_defaults_higher_max_receive(self):
        assert topo.DEFAULT_QUEUE_CONFIG["order"]["max_receive_count"] == 5

    def test_report_defaults_longer_visibility(self):
        assert topo.DEFAULT_QUEUE_CONFIG["report"]["visibility_timeout_seconds"] == 120


# ── S19-02: Row mappers ─────────────────────────────────────────────


class TestS19RowMappers:
    def test_topology_row_to_dict_all_fields(self):
        row = _make_topology_row()
        d = topo._topology_row_to_dict(row)
        assert d["id"] == 1
        assert d["domain"] == "pricing"
        assert d["queue_url"].endswith("acc-pricing")
        assert d["enabled"] is True
        assert d["status"] == "active"
        assert d["messages_received"] == 100
        assert d["messages_failed"] == 5
        assert d["messages_dlq"] == 2

    def test_topology_row_to_dict_disabled(self):
        row = _make_topology_row(enabled=False, status="disabled")
        d = topo._topology_row_to_dict(row)
        assert d["enabled"] is False
        assert d["status"] == "disabled"

    def test_topology_row_null_timestamps(self):
        row = _make_topology_row(last_poll_at=None, created_at=None, updated_at=None)
        d = topo._topology_row_to_dict(row)
        assert d["last_poll_at"] is None
        assert d["created_at"] is None
        assert d["updated_at"] is None

    def test_dlq_row_to_dict_all_fields(self):
        row = _make_dlq_row()
        d = topo._dlq_row_to_dict(row)
        assert d["id"] == 1
        assert d["domain"] == "pricing"
        assert d["message_id"] == "msg-abc-123"
        assert d["status"] == "unresolved"
        assert d["error_message"] == "Processing timeout"
        assert d["approximate_receive_count"] == 3

    def test_dlq_row_resolved(self):
        row = _make_dlq_row(status="replayed", resolution="replayed", resolved_by="admin", resolved_at="2026-03-12 11:00:00")
        d = topo._dlq_row_to_dict(row)
        assert d["status"] == "replayed"
        assert d["resolution"] == "replayed"
        assert d["resolved_by"] == "admin"
        assert d["resolved_at"] is not None

    def test_dlq_row_null_optionals(self):
        row = _make_dlq_row(receipt_handle=None, body=None, original_event_id=None, error_message=None, resolved_at=None, created_at=None)
        d = topo._dlq_row_to_dict(row)
        assert d["receipt_handle"] is None
        assert d["body"] is None
        assert d["original_event_id"] is None
        assert d["resolved_at"] is None
        assert d["created_at"] is None


# ── S19-03: safe_json helper ─────────────────────────────────────────


class TestS19SafeJson:
    def test_none_returns_empty_list(self):
        assert topo._safe_json(None) == []

    def test_already_list(self):
        assert topo._safe_json([1, 2, 3]) == [1, 2, 3]

    def test_already_dict(self):
        assert topo._safe_json({"a": 1}) == {"a": 1}

    def test_valid_json_string(self):
        assert topo._safe_json('{"x": 42}') == {"x": 42}

    def test_invalid_json_string(self):
        assert topo._safe_json("not json!") == []

    def test_json_list_string(self):
        assert topo._safe_json('[1, 2]') == [1, 2]


# ── S19-04: Queue registration ──────────────────────────────────────


class TestS19RegisterQueue:
    @patch(_CONN)
    def test_register_queue_success(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(42,)]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.register_queue(
            domain="pricing",
            queue_url="https://sqs.eu-west-1.amazonaws.com/123/acc-pricing",
            dlq_url="https://sqs.eu-west-1.amazonaws.com/123/acc-pricing-dlq",
        )
        assert result["id"] == 42
        assert result["domain"] == "pricing"
        assert result["status"] == "active"
        assert len(cur.executed) == 1
        assert "MERGE" in cur.executed[0][0]

    @patch(_CONN)
    def test_register_queue_invalid_domain(self, mock_conn):
        with pytest.raises(ValueError, match="Invalid domain"):
            topo.register_queue(domain="invalid", queue_url="https://example.com")

    @patch(_CONN)
    def test_register_queue_uses_defaults(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,)]
        mock_conn.return_value = _FakeConn(cur)

        topo.register_queue(domain="order", queue_url="https://example.com")
        params = cur.executed[0][1]
        # order domain defaults: max_receive_count=5
        assert 5 in params

    @patch(_CONN)
    def test_register_queue_custom_params(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,)]
        mock_conn.return_value = _FakeConn(cur)

        topo.register_queue(
            domain="pricing",
            queue_url="https://example.com",
            max_receive_count=10,
            visibility_timeout_seconds=60,
            batch_size=20,
        )
        params = cur.executed[0][1]
        assert 10 in params  # max_receive_count
        assert 60 in params  # visibility_timeout
        assert 20 in params  # batch_size


# ── S19-05: Queue status ────────────────────────────────────────────


class TestS19QueueStatus:
    @patch(_CONN)
    def test_update_enable(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        result = topo.update_queue_status("pricing", enabled=True)
        assert result["updated"] is True
        assert result["enabled"] is True

    @patch(_CONN)
    def test_update_status_paused(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        result = topo.update_queue_status("pricing", status="paused")
        assert result["status"] == "paused"

    @patch(_CONN)
    def test_invalid_status(self, mock_conn):
        with pytest.raises(ValueError, match="Invalid status"):
            topo.update_queue_status("pricing", status="bogus")

    @patch(_CONN)
    def test_invalid_domain(self, mock_conn):
        with pytest.raises(ValueError, match="Invalid domain"):
            topo.update_queue_status("bogus", enabled=True)

    @patch(_CONN)
    def test_update_both(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        result = topo.update_queue_status("listing", enabled=False, status="disabled")
        assert result["enabled"] is False
        assert result["status"] == "disabled"


# ── S19-06: Get topology ────────────────────────────────────────────


class TestS19GetTopology:
    @patch(_CONN)
    def test_get_all_queues(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row(domain="feed"), _make_topology_row(id=2, domain="order")]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.get_queue_topology()
        assert len(result) == 2
        assert result[0]["domain"] == "feed"
        assert result[1]["domain"] == "order"

    @patch(_CONN)
    def test_get_all_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_conn.return_value = _FakeConn(cur)
        assert topo.get_queue_topology() == []

    @patch(_CONN)
    def test_get_queue_for_domain(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row(domain="inventory")]
        mock_conn.return_value = _FakeConn(cur)
        result = topo.get_queue_for_domain("inventory")
        assert result is not None
        assert result["domain"] == "inventory"

    @patch(_CONN)
    def test_get_queue_for_domain_not_found(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_conn.return_value = _FakeConn(cur)
        assert topo.get_queue_for_domain("pricing") is None

    @patch(_CONN)
    def test_get_enabled_queues(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row(enabled=True, status="active")]
        mock_conn.return_value = _FakeConn(cur)
        result = topo.get_enabled_queues()
        assert len(result) == 1
        assert result[0]["enabled"] is True


# ── S19-07: Queue metrics ───────────────────────────────────────────


class TestS19Metrics:
    @patch(_CONN)
    def test_record_poll_result_success(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        topo.record_poll_result("pricing", messages_received=5, messages_processed=4, messages_failed=1)
        assert len(cur.executed) == 1
        sql = cur.executed[0][0]
        assert "messages_received" in sql
        assert "last_poll_at" in sql

    @patch(_CONN)
    def test_record_poll_result_with_error(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        topo.record_poll_result("pricing", error="Connection timeout")
        sql = cur.executed[0][0]
        assert "last_error" in sql
        assert "status = 'error'" in sql


# ── S19-08: Topology health ─────────────────────────────────────────


class TestS19Health:
    @patch(_CONN)
    def test_health_with_data(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (6, 4, 1, 1, 1000, 950, 50, 10),  # topology summary
            (5,),  # unresolved DLQ count
        ]
        mock_conn.return_value = _FakeConn(cur)

        h = topo.get_topology_health()
        assert h["total_queues"] == 6
        assert h["active_queues"] == 4
        assert h["error_queues"] == 1
        assert h["total_received"] == 1000
        assert h["total_processed"] == 950
        assert h["total_failed"] == 50
        assert h["unresolved_dlq"] == 5

    @patch(_CONN)
    def test_health_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []  # no rows
        mock_conn.return_value = _FakeConn(cur)
        h = topo.get_topology_health()
        assert h["total_queues"] == 0
        assert h["unresolved_dlq"] == 0


# ── S19-09: DLQ tracking ────────────────────────────────────────────


class TestS19DlqTracking:
    @patch(_CONN)
    def test_track_dlq_entry(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(99,)]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.track_dlq_entry(
            domain="pricing",
            queue_url="https://sqs.eu-west-1.amazonaws.com/123/acc-pricing-dlq",
            message_id="msg-001",
            error_message="Timeout",
        )
        assert result["id"] == 99
        assert result["domain"] == "pricing"
        assert "MERGE" in cur.executed[0][0]


# ── S19-10: DLQ resolution ──────────────────────────────────────────


class TestS19DlqResolution:
    @patch(_CONN)
    def test_resolve_replay(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        result = topo.resolve_dlq_entry(1, resolution="replayed")
        assert result["resolved"] is True
        assert result["resolution"] == "replayed"

    @patch(_CONN)
    def test_resolve_discard(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        result = topo.resolve_dlq_entry(2, resolution="discarded")
        assert result["resolution"] == "discarded"

    @patch(_CONN)
    def test_resolve_investigate(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        result = topo.resolve_dlq_entry(3, resolution="investigating", resolved_by="admin")
        assert result["resolution"] == "investigating"

    @patch(_CONN)
    def test_resolve_invalid_resolution(self, mock_conn):
        with pytest.raises(ValueError, match="Invalid resolution"):
            topo.resolve_dlq_entry(1, resolution="bogus")


# ── S19-11: DLQ queries ─────────────────────────────────────────────


class TestS19DlqQueries:
    @patch(_CONN)
    def test_get_dlq_entries_all(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(3,), _make_dlq_row(), _make_dlq_row(id=2, message_id="msg-002"), _make_dlq_row(id=3, message_id="msg-003")]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.get_dlq_entries()
        assert result["total"] == 3
        assert len(result["items"]) == 3

    @patch(_CONN)
    def test_get_dlq_entries_filtered(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,), _make_dlq_row()]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.get_dlq_entries("pricing", status="unresolved")
        assert result["total"] == 1
        sql = cur.executed[0][0]
        assert "domain" in sql

    @patch(_CONN)
    def test_get_dlq_entries_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_conn.return_value = _FakeConn(cur)
        result = topo.get_dlq_entries()
        assert result["total"] == 0
        assert result["items"] == []

    @patch(_CONN)
    def test_get_dlq_summary(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(10, 5, 3, 1, 1)]
        mock_conn.return_value = _FakeConn(cur)
        s = topo.get_dlq_summary()
        assert s["total"] == 10
        assert s["unresolved"] == 5
        assert s["replayed"] == 3
        assert s["discarded"] == 1
        assert s["investigating"] == 1

    @patch(_CONN)
    def test_get_dlq_summary_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_conn.return_value = _FakeConn(cur)
        s = topo.get_dlq_summary()
        assert s["total"] == 0


# ── S19-12: Event routing ───────────────────────────────────────────


class TestS19Routing:
    def test_route_any_offer_changed(self):
        assert topo.route_notification_type("ANY_OFFER_CHANGED") == "pricing"

    def test_route_order_status(self):
        assert topo.route_notification_type("ORDER_STATUS_CHANGE") == "order"

    def test_route_listings(self):
        assert topo.route_notification_type("LISTINGS_ITEM_STATUS_CHANGE") == "listing"
        assert topo.route_notification_type("LISTINGS_ITEM_ISSUES_CHANGE") == "listing"
        assert topo.route_notification_type("ITEM_PRODUCT_TYPE_CHANGE") == "listing"
        assert topo.route_notification_type("BRANDED_ITEM_CONTENT_CHANGE") == "listing"

    def test_route_report(self):
        assert topo.route_notification_type("REPORT_PROCESSING_FINISHED") == "report"

    def test_route_inventory(self):
        assert topo.route_notification_type("FBA_INVENTORY_AVAILABILITY_CHANGES") == "inventory"

    def test_route_feed(self):
        assert topo.route_notification_type("FEED_PROCESSING_FINISHED") == "feed"

    def test_route_unknown_defaults_to_report(self):
        assert topo.route_notification_type("UNKNOWN_TYPE") == "report"

    def test_routing_table(self):
        rt = topo.get_routing_table()
        assert rt["total_types"] == 9
        assert rt["total_domains"] == 6
        assert "pricing" in rt["domains"]
        assert "ANY_OFFER_CHANGED" in rt["domains"]["pricing"]
        assert len(rt["routes"]) == 9


# ── S19-13: Multi-queue polling ──────────────────────────────────────


class TestS19Polling:
    @patch(_CONN)
    def test_poll_domain_no_config(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []  # get_queue_for_domain returns None
        mock_conn.return_value = _FakeConn(cur)

        result = topo.poll_domain_queue("pricing")
        assert result["status"] == "no_config"
        assert result["messages_received"] == 0

    @patch(_CONN)
    def test_poll_domain_disabled(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row(enabled=False)]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.poll_domain_queue("pricing")
        assert result["status"] == "disabled"

    @patch(_CONN)
    def test_poll_domain_no_url(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row(queue_url="")]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.poll_domain_queue("pricing")
        assert result["status"] == "no_url"

    @patch("app.intelligence.sqs_topology.poll_sqs", create=True)
    @patch("app.intelligence.sqs_topology.record_poll_result")
    @patch(_CONN)
    def test_poll_domain_success(self, mock_conn, mock_record, mock_poll_sqs):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row()]
        mock_conn.return_value = _FakeConn(cur)
        mock_poll_sqs.return_value = {"messages_received": 3, "messages_processed": 2, "messages_failed": 1}

        with patch("app.services.event_backbone.poll_sqs", mock_poll_sqs):
            result = topo.poll_domain_queue("pricing")

        assert result["status"] == "polled"
        assert result["messages_received"] == 3

    @patch(_CONN)
    def test_poll_all_queues_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_conn.return_value = _FakeConn(cur)
        result = topo.poll_all_queues()
        assert result["queues_polled"] == 0
        assert result["total_received"] == 0


# ── S19-14: Seed topology ───────────────────────────────────────────


class TestS19Seed:
    @patch(_CONN)
    def test_seed_default_topology(self, mock_conn):
        cur = _FakeCursor()
        # Each register_queue calls fetchone once
        cur.multi_rows = [(1,), (2,), (3,), (4,), (5,), (6,)]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.seed_default_topology(
            base_queue_url="https://sqs.eu-west-1.amazonaws.com/123/acc",
            region="eu-west-1",
        )
        assert result["seeded"] == 6
        assert len(result["results"]) == 6
        domains = {r["domain"] for r in result["results"]}
        assert domains == topo.VALID_DOMAINS

    @patch(_CONN)
    def test_seed_empty_base_url(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,), (2,), (3,), (4,), (5,), (6,)]
        mock_conn.return_value = _FakeConn(cur)

        result = topo.seed_default_topology()
        assert result["seeded"] == 6
        # With empty base, queue_url should be empty string
        for r in result["results"]:
            assert r["queue_url"] == ""


# ── S19-15: Schema DDL idempotent ────────────────────────────────────


class TestS19Schema:
    @patch(_CONN)
    def test_ensure_topology_schema(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)
        topo.ensure_topology_schema()
        assert len(cur.executed) == 2  # 2 DDL statements
        assert "acc_sqs_queue_topology" in cur.executed[0][0]
        assert "acc_dlq_entry" in cur.executed[1][0]


# ── S19-16: API endpoints ───────────────────────────────────────────


@pytest.fixture()
def api_client():
    from app.api.v1.sqs_topology import router
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


class TestS19ApiQueues:
    @patch(_CONN)
    def test_list_queues(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row()]
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.get("/api/v1/sqs-topology/queues")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @patch(_CONN)
    def test_get_queue_found(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = [_make_topology_row(domain="order")]
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.get("/api/v1/sqs-topology/queues/order")
        assert resp.status_code == 200
        assert resp.json()["domain"] == "order"

    @patch(_CONN)
    def test_get_queue_not_found(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.get("/api/v1/sqs-topology/queues/bogus")
        assert resp.status_code == 404

    @patch(_CONN)
    def test_register_queue(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = [(1,)]
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.post("/api/v1/sqs-topology/queues", json={
            "domain": "pricing",
            "queue_url": "https://sqs.eu-west-1.amazonaws.com/123/acc-pricing",
        })
        assert resp.status_code == 200
        assert resp.json()["domain"] == "pricing"

    @patch(_CONN)
    def test_register_queue_invalid_domain(self, mock_conn, api_client):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.post("/api/v1/sqs-topology/queues", json={
            "domain": "bogus",
            "queue_url": "https://example.com",
        })
        assert resp.status_code == 400

    @patch(_CONN)
    def test_update_queue_status(self, mock_conn, api_client):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.patch("/api/v1/sqs-topology/queues/pricing/status", json={"enabled": False})
        assert resp.status_code == 200


class TestS19ApiHealth:
    @patch(_CONN)
    def test_topology_health(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = [
            (6, 5, 1, 0, 500, 480, 20, 5),
            (3,),
        ]
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.get("/api/v1/sqs-topology/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_queues"] == 6
        assert data["unresolved_dlq"] == 3


class TestS19ApiRouting:
    def test_routing_table(self, api_client):
        resp = api_client.get("/api/v1/sqs-topology/routing")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_types"] == 9
        assert "pricing" in data["domains"]


class TestS19ApiPolling:
    @patch(_CONN)
    def test_poll_domain(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = []  # no config → no_config
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.post("/api/v1/sqs-topology/poll/pricing")
        assert resp.status_code == 200
        assert resp.json()["status"] == "no_config"

    @patch(_CONN)
    def test_poll_all(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.post("/api/v1/sqs-topology/poll-all")
        assert resp.status_code == 200
        assert resp.json()["queues_polled"] == 0

    @patch(_CONN)
    def test_seed(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = [(1,), (2,), (3,), (4,), (5,), (6,)]
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.post("/api/v1/sqs-topology/seed", json={})
        assert resp.status_code == 200
        assert resp.json()["seeded"] == 6


class TestS19ApiDlq:
    @patch(_CONN)
    def test_list_dlq(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = [(2,), _make_dlq_row(), _make_dlq_row(id=2, message_id="msg-002")]
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.get("/api/v1/sqs-topology/dlq")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    @patch(_CONN)
    def test_dlq_summary(self, mock_conn, api_client):
        cur = _FakeCursor()
        cur.multi_rows = [(5, 2, 2, 1, 0)]
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.get("/api/v1/sqs-topology/dlq/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 5

    @patch(_CONN)
    def test_resolve_dlq(self, mock_conn, api_client):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        resp = api_client.post("/api/v1/sqs-topology/dlq/1/resolve", json={
            "resolution": "replayed",
        })
        assert resp.status_code == 200
        assert resp.json()["resolved"] is True

    @patch(_CONN)
    def test_resolve_dlq_invalid(self, mock_conn, api_client):
        resp = api_client.post("/api/v1/sqs-topology/dlq/1/resolve", json={
            "resolution": "bogus",
        })
        assert resp.status_code == 400


# ── S19-17: Edge cases ──────────────────────────────────────────────


class TestS19EdgeCases:
    def test_all_domains_have_routing(self):
        """Every domain should have at least one notification type routed to it."""
        routed_domains = set(topo.NOTIFICATION_ROUTING.values())
        assert routed_domains == topo.VALID_DOMAINS

    def test_domain_configs_positive_values(self):
        for domain, cfg in topo.DEFAULT_QUEUE_CONFIG.items():
            assert cfg["max_receive_count"] > 0
            assert cfg["visibility_timeout_seconds"] > 0
            assert cfg["batch_size"] > 0
            assert cfg["polling_interval_seconds"] > 0

    def test_no_duplicate_routing(self):
        """No notification type should map to multiple domains."""
        seen = set()
        for ntype in topo.NOTIFICATION_ROUTING:
            assert ntype not in seen, f"Duplicate routing for {ntype}"
            seen.add(ntype)

    @patch(_CONN)
    def test_register_all_valid_domains(self, mock_conn):
        for domain in topo.VALID_DOMAINS:
            cur = _FakeCursor()
            cur.multi_rows = [(1,)]
            mock_conn.return_value = _FakeConn(cur)
            result = topo.register_queue(domain=domain, queue_url=f"https://example.com/{domain}")
            assert result["domain"] == domain
