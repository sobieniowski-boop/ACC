"""Tests for Sprint 20 – Event Wiring & Replay.

Covers: constants, row mappers, wire CRUD, toggle, delete, seed,
wiring health, replay job CRUD, replay-and-process, DLQ replay,
domain handler stubs, register_all_domain_handlers,
poll_topology_queues, API endpoints, and edge cases.
"""
from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.intelligence import event_wiring as ew

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


def _make_wire_row(
    id=1,
    module_name="listing_state",
    event_domain="listing",
    event_action="listing_status_changed",
    handler_name="listing_state.status",
    description="Process listing status change notifications",
    enabled=True,
    priority=100,
    timeout_seconds=30,
    created_at="2026-03-12 10:00:00",
    updated_at="2026-03-12 10:00:00",
):
    return (
        id, module_name, event_domain, event_action, handler_name,
        description, 1 if enabled else 0, priority, timeout_seconds,
        created_at, updated_at,
    )


def _make_replay_row(
    id=1,
    replay_type="event_reset",
    scope_domain=None,
    scope_action=None,
    scope_event_ids=None,
    scope_since=None,
    scope_until=None,
    events_matched=0,
    events_replayed=0,
    events_processed=0,
    events_failed=0,
    status="pending",
    triggered_by=None,
    error_message=None,
    started_at="2026-03-12 10:00:00",
    completed_at=None,
):
    return (
        id, replay_type, scope_domain, scope_action, scope_event_ids,
        scope_since, scope_until, events_matched, events_replayed,
        events_processed, events_failed, status, triggered_by,
        error_message, started_at, completed_at,
    )


def _app():
    from app.api.v1.event_wiring import router
    a = FastAPI()
    a.include_router(router)
    return a


# =====================================================================
#  S20-T01  Constants and domain list
# =====================================================================


class TestS20Constants:
    def test_valid_replay_types(self):
        assert "event_reset" in ew.VALID_REPLAY_TYPES
        assert "dlq_reingest" in ew.VALID_REPLAY_TYPES
        assert "bulk_domain" in ew.VALID_REPLAY_TYPES
        assert "selective" in ew.VALID_REPLAY_TYPES
        assert len(ew.VALID_REPLAY_TYPES) == 4

    def test_valid_replay_statuses(self):
        for s in ("pending", "running", "completed", "failed", "cancelled"):
            assert s in ew.VALID_REPLAY_STATUSES

    def test_valid_wire_modules(self):
        assert "listing_state" in ew.VALID_WIRE_MODULES
        assert "pricing_state" in ew.VALID_WIRE_MODULES
        assert "profit" in ew.VALID_WIRE_MODULES
        assert "order_pipeline" in ew.VALID_WIRE_MODULES
        assert "inventory_sync" in ew.VALID_WIRE_MODULES
        assert "report_processor" in ew.VALID_WIRE_MODULES
        assert "feed_processor" in ew.VALID_WIRE_MODULES

    def test_all_event_domains(self):
        expected = {"pricing", "listing", "order", "inventory", "report", "feed", "ads", "finance"}
        assert ew.ALL_EVENT_DOMAINS == expected

    def test_default_wiring_count(self):
        assert len(ew.DEFAULT_WIRING) == 10

    def test_default_wiring_has_required_keys(self):
        for w in ew.DEFAULT_WIRING:
            assert "module_name" in w
            assert "event_domain" in w
            assert "event_action" in w
            assert "handler_name" in w

    def test_default_wiring_covers_all_domains(self):
        domains = {w["event_domain"] for w in ew.DEFAULT_WIRING}
        # Should cover at least listing, pricing, ads, finance, order, inventory, report, feed
        assert domains >= {"listing", "pricing", "ads", "finance", "order", "inventory", "report", "feed"}


# =====================================================================
#  S20-T02  Row mappers
# =====================================================================


class TestS20RowMappers:
    def test_wire_row_to_dict(self):
        row = _make_wire_row(id=5, module_name="profit", event_domain="ads")
        d = ew._wire_row_to_dict(row)
        assert d["id"] == 5
        assert d["module_name"] == "profit"
        assert d["event_domain"] == "ads"
        assert d["enabled"] is True
        assert d["priority"] == 100
        assert d["timeout_seconds"] == 30

    def test_wire_row_disabled(self):
        row = _make_wire_row(enabled=False)
        d = ew._wire_row_to_dict(row)
        assert d["enabled"] is False

    def test_wire_row_none_dates(self):
        row = _make_wire_row(created_at=None, updated_at=None)
        d = ew._wire_row_to_dict(row)
        assert d["created_at"] is None
        assert d["updated_at"] is None

    def test_replay_row_to_dict(self):
        row = _make_replay_row(id=10, replay_type="bulk_domain", scope_domain="order", status="completed")
        d = ew._replay_row_to_dict(row)
        assert d["id"] == 10
        assert d["replay_type"] == "bulk_domain"
        assert d["scope_domain"] == "order"
        assert d["status"] == "completed"

    def test_replay_row_with_all_fields(self):
        row = _make_replay_row(
            events_matched=50, events_replayed=45, events_processed=40,
            events_failed=5, triggered_by="admin", error_message=None,
            completed_at="2026-03-12 11:00:00",
        )
        d = ew._replay_row_to_dict(row)
        assert d["events_matched"] == 50
        assert d["events_replayed"] == 45
        assert d["events_processed"] == 40
        assert d["events_failed"] == 5
        assert d["triggered_by"] == "admin"
        assert d["completed_at"] == "2026-03-12 11:00:00"

    def test_replay_row_none_dates(self):
        row = _make_replay_row(scope_since=None, scope_until=None, started_at=None, completed_at=None)
        d = ew._replay_row_to_dict(row)
        assert d["scope_since"] is None
        assert d["scope_until"] is None
        assert d["started_at"] is None
        assert d["completed_at"] is None


# =====================================================================
#  S20-T03  Schema idempotent DDL
# =====================================================================


class TestS20Schema:
    @patch("app.intelligence.event_wiring.connect_acc")
    def test_ensure_wiring_schema(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn
        ew.ensure_wiring_schema()
        # Should execute 2 DDL statements
        assert len(cur.executed) == 2
        assert "acc_event_wire_config" in cur.executed[0][0]
        assert "acc_replay_job" in cur.executed[1][0]


# =====================================================================
#  S20-T04  Wire CRUD
# =====================================================================


class TestS20WireCRUD:
    @patch("app.intelligence.event_wiring.connect_acc")
    def test_register_wire(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(42,)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.register_wire(
            module_name="order_pipeline",
            event_domain="order",
            event_action="*",
            handler_name="order_pipeline.status_handler",
            description="Process orders",
        )
        assert result["id"] == 42
        assert result["handler_name"] == "order_pipeline.status_handler"
        assert result["event_domain"] == "order"
        assert len(cur.executed) == 1
        assert "MERGE" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_register_wire_defaults(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.register_wire(
            module_name="test_mod",
            event_domain="test_domain",
            handler_name="test_handler",
        )
        assert result["id"] == 1
        # Verify default action = "*", priority = 100, timeout = 30
        params = cur.executed[0][1]
        assert "*" in params  # event_action default

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_register_wire_rollback_on_error(self, mock_conn):
        cur = _FakeCursor()
        cur.execute = MagicMock(side_effect=Exception("duplicate key"))
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn

        with pytest.raises(Exception, match="duplicate key"):
            ew.register_wire(
                module_name="test", event_domain="test", handler_name="test_h")
        conn.rollback.assert_called_once()

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_wiring_no_filters(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_wire_row(id=1), _make_wire_row(id=2, handler_name="h2")]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring()
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
        assert "1=1" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_wiring_with_module_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_wire_row(module_name="profit")]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring(module_name="profit")
        assert len(result) == 1
        assert "module_name = ?" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_wiring_with_domain_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_wire_row(event_domain="order")]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring(event_domain="order")
        assert len(result) == 1
        assert "event_domain = ?" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_wiring_enabled_only(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_wire_row(enabled=True)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring(enabled_only=True)
        assert len(result) == 1
        assert "enabled = 1" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_wiring_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring()
        assert result == []


# =====================================================================
#  S20-T05  Toggle wire
# =====================================================================


class TestS20ToggleWire:
    @patch("app.intelligence.event_wiring.connect_acc")
    def test_toggle_wire_enable(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.toggle_wire("listing_state.status", enabled=True)
        assert result["handler_name"] == "listing_state.status"
        assert result["enabled"] is True
        assert result["updated"] is True
        assert "enabled = ?" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_toggle_wire_disable(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.toggle_wire("listing_state.status", enabled=False)
        assert result["enabled"] is False
        params = cur.executed[0][1]
        assert params[0] == 0  # disabled

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_toggle_rollback_on_error(self, mock_conn):
        cur = _FakeCursor()
        cur.execute = MagicMock(side_effect=Exception("db error"))
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn

        with pytest.raises(Exception, match="db error"):
            ew.toggle_wire("test_handler", enabled=True)
        conn.rollback.assert_called_once()


# =====================================================================
#  S20-T06  Delete wire
# =====================================================================


class TestS20DeleteWire:
    @patch("app.intelligence.event_wiring.connect_acc")
    def test_delete_wire(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.delete_wire("old_handler")
        assert result["handler_name"] == "old_handler"
        assert result["deleted"] is True
        assert "DELETE" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_delete_rollback_on_error(self, mock_conn):
        cur = _FakeCursor()
        cur.execute = MagicMock(side_effect=Exception("fk violation"))
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn

        with pytest.raises(Exception, match="fk violation"):
            ew.delete_wire("test_handler")
        conn.rollback.assert_called_once()


# =====================================================================
#  S20-T07  Seed default wiring
# =====================================================================


class TestS20SeedWiring:
    @patch("app.intelligence.event_wiring.register_wire")
    def test_seed_default_wiring(self, mock_reg):
        mock_reg.return_value = {"id": 1, "handler_name": "x", "event_domain": "y"}
        result = ew.seed_default_wiring()
        assert result["seeded"] == 10
        assert mock_reg.call_count == 10
        assert len(result["results"]) == 10

    @patch("app.intelligence.event_wiring.register_wire")
    def test_seed_handles_errors(self, mock_reg):
        mock_reg.side_effect = [
            {"id": 1, "handler_name": "x", "event_domain": "y"},
            Exception("duplicate"),
        ] + [{"id": i, "handler_name": "x", "event_domain": "y"} for i in range(3, 11)]
        result = ew.seed_default_wiring()
        assert result["seeded"] == 10
        # One entry should have error
        errors = [r for r in result["results"] if "error" in r]
        assert len(errors) == 1


# =====================================================================
#  S20-T08  Wiring health
# =====================================================================


class TestS20WiringHealth:
    @patch("app.intelligence.event_wiring.connect_acc")
    def test_wiring_health_full(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (10, 8, 2, 6, 5),  # summary row
            # domain coverage rows
            ("ads", 2, 2),
            ("feed", 1, 1),
            ("finance", 1, 1),
            ("inventory", 1, 1),
            ("listing", 2, 1),
            ("order", 1, 1),
        ]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring_health()
        assert result["total_wires"] == 10
        assert result["enabled_wires"] == 8
        assert result["disabled_wires"] == 2
        assert result["domains_covered"] == 6
        assert result["modules_wired"] == 5
        assert len(result["domain_coverage"]) == 6
        # pricing and report should be unwired
        assert sorted(result["unwired_domains"]) == ["pricing", "report"]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_wiring_health_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []  # no rows
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring_health()
        assert result["total_wires"] == 0
        assert len(result["unwired_domains"]) == 8  # all domains

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_wiring_health_all_domains_covered(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (10, 10, 0, 8, 6),  # summary
            # all 8 domains
            ("ads", 1, 1), ("feed", 1, 1), ("finance", 1, 1), ("inventory", 1, 1),
            ("listing", 2, 2), ("order", 1, 1), ("pricing", 2, 2), ("report", 1, 1),
        ]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_wiring_health()
        assert result["unwired_domains"] == []


# =====================================================================
#  S20-T09  Replay job CRUD
# =====================================================================


class TestS20ReplayJobCRUD:
    @patch("app.intelligence.event_wiring.connect_acc")
    def test_create_replay_job(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(99,)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.create_replay_job(
            replay_type="event_reset",
            scope_domain="order",
            triggered_by="admin",
        )
        assert result["id"] == 99
        assert result["replay_type"] == "event_reset"
        assert result["status"] == "pending"
        assert "INSERT INTO dbo.acc_replay_job" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_create_replay_job_with_event_ids(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(100,)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.create_replay_job(
            replay_type="selective",
            scope_event_ids=["ev1", "ev2", "ev3"],
        )
        assert result["id"] == 100
        # Verify event_ids were serialized as JSON
        params = cur.executed[0][1]
        assert '["ev1", "ev2", "ev3"]' in str(params)

    def test_create_replay_job_invalid_type(self):
        with pytest.raises(ValueError, match="Invalid replay_type"):
            ew.create_replay_job(replay_type="invalid_type")

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_update_replay_job_status(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        ew.update_replay_job(1, status="running")
        sql = cur.executed[0][0]
        assert "status = ?" in sql

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_update_replay_job_completed_sets_completed_at(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        ew.update_replay_job(1, status="completed")
        sql = cur.executed[0][0]
        assert "completed_at = SYSUTCDATETIME()" in sql

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_update_replay_job_counters(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        ew.update_replay_job(1, events_matched=50, events_replayed=45, events_processed=40, events_failed=5)
        sql = cur.executed[0][0]
        assert "events_matched = ?" in sql
        assert "events_replayed = ?" in sql
        assert "events_processed = ?" in sql
        assert "events_failed = ?" in sql

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_update_replay_job_no_changes(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        ew.update_replay_job(1)  # no kwargs
        assert len(cur.executed) == 0


# =====================================================================
#  S20-T10  Replay and process
# =====================================================================


class TestS20ReplayAndProcess:
    @patch("app.services.event_backbone.process_pending_events")
    @patch("app.services.event_backbone.replay_events")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_and_process_full(self, mock_create, mock_update, mock_replay, mock_process):
        mock_create.return_value = {"id": 1, "replay_type": "bulk_domain", "status": "pending"}
        mock_replay.return_value = {"replayed": 10}
        mock_process.return_value = {"processed": 8, "failed": 2}

        result = ew.replay_and_process(event_domain="order", triggered_by="admin")
        assert result["job_id"] == 1
        assert result["status"] == "completed"
        assert result["events_replayed"] == 10
        assert result["events_processed"] == 8
        assert result["events_failed"] == 2
        assert result["replay_type"] == "bulk_domain"

    @patch("app.services.event_backbone.replay_events")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_and_process_zero_events(self, mock_create, mock_update, mock_replay):
        mock_create.return_value = {"id": 2, "replay_type": "event_reset", "status": "pending"}
        mock_replay.return_value = {"replayed": 0}

        result = ew.replay_and_process()
        assert result["status"] == "completed"
        assert result["events_replayed"] == 0
        assert result["events_processed"] == 0

    @patch("app.services.event_backbone.replay_events")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_and_process_selective(self, mock_create, mock_update, mock_replay):
        mock_create.return_value = {"id": 3, "replay_type": "selective", "status": "pending"}
        mock_replay.return_value = {"replayed": 3}

        # Need to also patch process
        with patch("app.services.event_backbone.process_pending_events") as mock_process:
            mock_process.return_value = {"processed": 3, "failed": 0}
            result = ew.replay_and_process(event_ids=["e1", "e2", "e3"])
        assert result["replay_type"] == "selective"

    @patch("app.services.event_backbone.replay_events")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_and_process_error(self, mock_create, mock_update, mock_replay):
        mock_create.return_value = {"id": 4, "replay_type": "event_reset", "status": "pending"}
        mock_replay.side_effect = Exception("backbone error")

        result = ew.replay_and_process()
        assert result["status"] == "failed"
        assert "backbone error" in result["error"]
        mock_update.assert_any_call(4, status="failed", error_message="backbone error")

    @patch("app.services.event_backbone.process_pending_events")
    @patch("app.services.event_backbone.replay_events")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_with_time_range(self, mock_create, mock_update, mock_replay, mock_process):
        mock_create.return_value = {"id": 5, "replay_type": "event_reset", "status": "pending"}
        mock_replay.return_value = {"replayed": 5}
        mock_process.return_value = {"processed": 5, "failed": 0}

        result = ew.replay_and_process(since="2026-01-01", until="2026-03-01", limit=100)
        assert result["status"] == "completed"
        mock_replay.assert_called_once_with(
            event_ids=None, event_domain=None, notification_type=None,
            since="2026-01-01", until="2026-03-01", limit=100,
        )


# =====================================================================
#  S20-T11  DLQ replay
# =====================================================================


class TestS20DLQReplay:
    @patch("app.intelligence.sqs_topology.resolve_dlq_entry")
    @patch("app.services.event_backbone.ingest")
    @patch("app.intelligence.sqs_topology.get_dlq_entries")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_entries_success(self, mock_create, mock_update, mock_get_dlq, mock_ingest, mock_resolve):
        mock_create.return_value = {"id": 10, "replay_type": "dlq_reingest", "status": "pending"}
        mock_get_dlq.return_value = {
            "items": [
                {"id": 1, "domain": "order", "body": '{"event": "test"}'},
                {"id": 2, "domain": "order", "body": '{"event": "test2"}'},
            ]
        }

        result = ew.replay_dlq_entries(domain="order", triggered_by="admin")
        assert result["status"] == "completed"
        assert result["entries_matched"] == 2
        assert result["entries_replayed"] == 2
        assert result["entries_failed"] == 0
        assert mock_ingest.call_count == 2
        assert mock_resolve.call_count == 2

    @patch("app.intelligence.sqs_topology.resolve_dlq_entry")
    @patch("app.services.event_backbone.ingest")
    @patch("app.intelligence.sqs_topology.get_dlq_entries")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_partial_failure(self, mock_create, mock_update, mock_get_dlq, mock_ingest, mock_resolve):
        mock_create.return_value = {"id": 11, "replay_type": "dlq_reingest", "status": "pending"}
        mock_get_dlq.return_value = {
            "items": [
                {"id": 1, "domain": "order", "body": '{"event": "ok"}'},
                {"id": 2, "domain": "order", "body": '{"event": "bad"}'},
            ]
        }
        mock_ingest.side_effect = [None, Exception("parse error")]

        result = ew.replay_dlq_entries(domain="order")
        assert result["status"] == "completed"
        assert result["entries_replayed"] == 1
        assert result["entries_failed"] == 1

    @patch("app.intelligence.sqs_topology.resolve_dlq_entry")
    @patch("app.services.event_backbone.ingest")
    @patch("app.intelligence.sqs_topology.get_dlq_entries")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_empty(self, mock_create, mock_update, mock_get_dlq, mock_ingest, mock_resolve):
        mock_create.return_value = {"id": 12, "replay_type": "dlq_reingest", "status": "pending"}
        mock_get_dlq.return_value = {"items": []}

        result = ew.replay_dlq_entries()
        assert result["entries_matched"] == 0
        assert result["entries_replayed"] == 0

    @patch("app.intelligence.sqs_topology.get_dlq_entries")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_error(self, mock_create, mock_update, mock_get_dlq):
        mock_create.return_value = {"id": 13, "replay_type": "dlq_reingest", "status": "pending"}
        mock_get_dlq.side_effect = Exception("db error")

        result = ew.replay_dlq_entries()
        assert result["status"] == "failed"
        assert "db error" in result["error"]

    @patch("app.intelligence.sqs_topology.resolve_dlq_entry")
    @patch("app.services.event_backbone.ingest")
    @patch("app.intelligence.event_wiring.connect_acc")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_by_entry_ids(self, mock_create, mock_update, mock_conn, mock_ingest, mock_resolve):
        mock_create.return_value = {"id": 14, "replay_type": "dlq_reingest", "status": "pending"}

        cur = _FakeCursor()
        cur.multi_rows = [
            (1, "order", "https://q.url", "msg1", "r1", '{"test":1}', 3, None, None, "unresolved", None, None, None, "2026-03-12"),
        ]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.replay_dlq_entries(entry_ids=[1])
        assert result["entries_matched"] == 1

    @patch("app.intelligence.sqs_topology.resolve_dlq_entry")
    @patch("app.services.event_backbone.ingest")
    @patch("app.intelligence.sqs_topology.get_dlq_entries")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_dict_body(self, mock_create, mock_update, mock_get_dlq, mock_ingest, mock_resolve):
        mock_create.return_value = {"id": 15, "replay_type": "dlq_reingest", "status": "pending"}
        mock_get_dlq.return_value = {
            "items": [
                {"id": 1, "domain": "order", "body": {"already": "parsed"}},
            ]
        }
        result = ew.replay_dlq_entries()
        assert result["entries_replayed"] == 1
        mock_ingest.assert_called_once_with({"already": "parsed"}, source="dlq_replay")

    @patch("app.intelligence.sqs_topology.resolve_dlq_entry")
    @patch("app.services.event_backbone.ingest")
    @patch("app.intelligence.sqs_topology.get_dlq_entries")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_null_body(self, mock_create, mock_update, mock_get_dlq, mock_ingest, mock_resolve):
        mock_create.return_value = {"id": 16, "replay_type": "dlq_reingest", "status": "pending"}
        mock_get_dlq.return_value = {
            "items": [
                {"id": 1, "domain": "order", "body": None},
            ]
        }
        result = ew.replay_dlq_entries()
        assert result["entries_replayed"] == 1
        # Should use fallback payload
        mock_ingest.assert_called_once_with({"dlq_entry_id": 1, "domain": "order"}, source="dlq_replay")

    @patch("app.intelligence.sqs_topology.resolve_dlq_entry")
    @patch("app.services.event_backbone.ingest")
    @patch("app.intelligence.sqs_topology.get_dlq_entries")
    @patch("app.intelligence.event_wiring.update_replay_job")
    @patch("app.intelligence.event_wiring.create_replay_job")
    def test_replay_dlq_invalid_json_body(self, mock_create, mock_update, mock_get_dlq, mock_ingest, mock_resolve):
        mock_create.return_value = {"id": 17, "replay_type": "dlq_reingest", "status": "pending"}
        mock_get_dlq.return_value = {
            "items": [
                {"id": 1, "domain": "order", "body": "not-json{"},
            ]
        }
        result = ew.replay_dlq_entries()
        assert result["entries_replayed"] == 1
        # Should use raw_body fallback
        mock_ingest.assert_called_once_with({"raw_body": "not-json{", "dlq_entry_id": 1}, source="dlq_replay")


# =====================================================================
#  S20-T12  Replay jobs query & summary
# =====================================================================


class TestS20ReplayJobs:
    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_replay_jobs_no_filters(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (3,),  # count
            _make_replay_row(id=1, replay_type="event_reset"),
            _make_replay_row(id=2, replay_type="dlq_reingest"),
            _make_replay_row(id=3, replay_type="bulk_domain"),
        ]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_replay_jobs()
        assert result["total"] == 3
        assert len(result["items"]) == 3
        assert result["limit"] == 50
        assert result["offset"] == 0

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_replay_jobs_with_status_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (1,),
            _make_replay_row(id=1, status="completed"),
        ]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_replay_jobs(status="completed")
        assert result["total"] == 1
        assert "status = ?" in cur.executed[0][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_replay_jobs_with_type_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (2,),
            _make_replay_row(id=1, replay_type="dlq_reingest"),
            _make_replay_row(id=2, replay_type="dlq_reingest"),
        ]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_replay_jobs(replay_type="dlq_reingest")
        assert result["total"] == 2

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_replay_jobs_pagination(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(10,)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_replay_jobs(limit=5, offset=5)
        assert result["limit"] == 5
        assert result["offset"] == 5
        assert "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY" in cur.executed[1][0]

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_replay_summary(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(10, 7, 2, 1, 0, 100, 90, 10)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_replay_summary()
        assert result["total_jobs"] == 10
        assert result["completed"] == 7
        assert result["failed"] == 2
        assert result["running"] == 1
        assert result["pending"] == 0
        assert result["total_events_replayed"] == 100
        assert result["total_events_processed"] == 90
        assert result["total_events_failed"] == 10

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_replay_summary_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_replay_summary()
        assert result["total_jobs"] == 0


# =====================================================================
#  S20-T13  Domain handler stubs
# =====================================================================


class TestS20DomainHandlers:
    def test_handle_order_event(self):
        result = ew.handle_order_event({"event_id": "abc123", "event_action": "status_change", "amazon_order_id": "111"})
        assert result["handled"] is True
        assert result["domain"] == "order"

    def test_handle_inventory_event(self):
        result = ew.handle_inventory_event({"event_id": "def456", "sku": "X-100"})
        assert result["handled"] is True
        assert result["domain"] == "inventory"

    def test_handle_report_event(self):
        result = ew.handle_report_event({"event_id": "ghi789", "event_action": "ready"})
        assert result["handled"] is True
        assert result["domain"] == "report"

    def test_handle_feed_event(self):
        result = ew.handle_feed_event({"event_id": "jkl012", "event_action": "finished"})
        assert result["handled"] is True
        assert result["domain"] == "feed"

    def test_handlers_handle_empty_event(self):
        for handler in (ew.handle_order_event, ew.handle_inventory_event, ew.handle_report_event, ew.handle_feed_event):
            result = handler({})
            assert result["handled"] is True

    def test_handlers_handle_missing_keys(self):
        event = {"event_id": "short"}
        for handler in (ew.handle_order_event, ew.handle_inventory_event, ew.handle_report_event, ew.handle_feed_event):
            result = handler(event)
            assert result["handled"] is True


# =====================================================================
#  S20-T14  Register all domain handlers
# =====================================================================


class TestS20RegisterHandlers:
    @patch("app.services.event_backbone._HANDLER_REGISTRY", {})
    @patch("app.services.event_backbone.register_handler")
    def test_register_all_fresh(self, mock_reg):
        result = ew.register_all_domain_handlers()
        assert result["count"] == 4
        assert set(result["registered"]) == {"order:*", "inventory:*", "report:*", "feed:*"}
        assert mock_reg.call_count == 4

    @patch("app.services.event_backbone._HANDLER_REGISTRY", {"order:*": MagicMock(), "inventory:*": MagicMock()})
    @patch("app.services.event_backbone.register_handler")
    def test_register_skips_existing(self, mock_reg):
        result = ew.register_all_domain_handlers()
        assert result["count"] == 2
        assert "order:*" not in result["registered"]
        assert "inventory:*" not in result["registered"]
        assert "report:*" in result["registered"]
        assert "feed:*" in result["registered"]

    @patch("app.services.event_backbone._HANDLER_REGISTRY", {"order:*": 1, "inventory:*": 1, "report:*": 1, "feed:*": 1})
    @patch("app.services.event_backbone.register_handler")
    def test_register_all_existing(self, mock_reg):
        result = ew.register_all_domain_handlers()
        assert result["count"] == 0
        assert mock_reg.call_count == 0


# =====================================================================
#  S20-T15  Topology polling bridge
# =====================================================================


class TestS20PollTopology:
    @patch("app.intelligence.sqs_topology.poll_all_queues")
    def test_poll_topology_success(self, mock_poll):
        mock_poll.return_value = {"queues_polled": 5, "total_received": 20}
        result = ew.poll_topology_queues()
        assert result["queues_polled"] == 5
        assert result["total_received"] == 20

    @patch("app.intelligence.sqs_topology.poll_all_queues")
    def test_poll_topology_error(self, mock_poll):
        mock_poll.side_effect = Exception("sqs unreachable")
        result = ew.poll_topology_queues()
        assert result["status"] == "error"
        assert "sqs unreachable" in result["error"]


# =====================================================================
#  S20-T16  API endpoints
# =====================================================================


class TestS20API:
    @patch("app.intelligence.event_wiring.get_wiring")
    def test_list_wires(self, mock_get):
        mock_get.return_value = [{"id": 1, "handler_name": "test"}]
        client = TestClient(_app())
        r = client.get("/event-wiring/wires")
        assert r.status_code == 200
        assert len(r.json()) == 1

    @patch("app.intelligence.event_wiring.get_wiring")
    def test_list_wires_with_filters(self, mock_get):
        mock_get.return_value = []
        client = TestClient(_app())
        r = client.get("/event-wiring/wires?module_name=profit&event_domain=ads&enabled_only=true")
        assert r.status_code == 200
        mock_get.assert_called_once_with(module_name="profit", event_domain="ads", enabled_only=True)

    @patch("app.intelligence.event_wiring.register_wire")
    def test_register_wire_endpoint(self, mock_reg):
        mock_reg.return_value = {"id": 1, "handler_name": "test", "event_domain": "order"}
        client = TestClient(_app())
        r = client.post("/event-wiring/wires", json={
            "module_name": "order_pipeline",
            "event_domain": "order",
            "handler_name": "test_handler",
        })
        assert r.status_code == 200
        assert r.json()["handler_name"] == "test"

    @patch("app.intelligence.event_wiring.toggle_wire")
    def test_toggle_wire_endpoint(self, mock_toggle):
        mock_toggle.return_value = {"handler_name": "h1", "enabled": False, "updated": True}
        client = TestClient(_app())
        r = client.patch("/event-wiring/wires/h1/toggle", json={"enabled": False})
        assert r.status_code == 200
        assert r.json()["enabled"] is False

    @patch("app.intelligence.event_wiring.delete_wire")
    def test_delete_wire_endpoint(self, mock_del):
        mock_del.return_value = {"handler_name": "h1", "deleted": True}
        client = TestClient(_app())
        r = client.delete("/event-wiring/wires/h1")
        assert r.status_code == 200
        assert r.json()["deleted"] is True

    @patch("app.intelligence.event_wiring.seed_default_wiring")
    def test_seed_wiring_endpoint(self, mock_seed):
        mock_seed.return_value = {"seeded": 10, "results": []}
        client = TestClient(_app())
        r = client.post("/event-wiring/wires/seed")
        assert r.status_code == 200
        assert r.json()["seeded"] == 10

    @patch("app.intelligence.event_wiring.get_wiring_health")
    def test_health_endpoint(self, mock_health):
        mock_health.return_value = {
            "total_wires": 10, "enabled_wires": 8, "disabled_wires": 2,
            "domains_covered": 6, "modules_wired": 5,
            "domain_coverage": [], "unwired_domains": [],
        }
        client = TestClient(_app())
        r = client.get("/event-wiring/health")
        assert r.status_code == 200
        assert r.json()["total_wires"] == 10

    @patch("app.intelligence.event_wiring.register_all_domain_handlers")
    def test_register_handlers_endpoint(self, mock_reg):
        mock_reg.return_value = {"registered": ["order:*", "inventory:*"], "count": 2}
        client = TestClient(_app())
        r = client.post("/event-wiring/register-handlers")
        assert r.status_code == 200
        assert r.json()["count"] == 2

    @patch("app.intelligence.event_wiring.replay_and_process")
    def test_replay_endpoint(self, mock_replay):
        mock_replay.return_value = {"job_id": 1, "status": "completed", "events_replayed": 5}
        client = TestClient(_app())
        r = client.post("/event-wiring/replay", json={"event_domain": "order", "limit": 100})
        assert r.status_code == 200
        assert r.json()["status"] == "completed"

    @patch("app.intelligence.event_wiring.replay_dlq_entries")
    def test_replay_dlq_endpoint(self, mock_dlq):
        mock_dlq.return_value = {"job_id": 1, "status": "completed", "entries_replayed": 3}
        client = TestClient(_app())
        r = client.post("/event-wiring/replay/dlq", json={"domain": "order"})
        assert r.status_code == 200
        assert r.json()["entries_replayed"] == 3

    @patch("app.intelligence.event_wiring.get_replay_jobs")
    def test_list_replay_jobs_endpoint(self, mock_jobs):
        mock_jobs.return_value = {"items": [], "total": 0, "limit": 50, "offset": 0}
        client = TestClient(_app())
        r = client.get("/event-wiring/replay/jobs")
        assert r.status_code == 200
        assert r.json()["total"] == 0

    @patch("app.intelligence.event_wiring.get_replay_jobs")
    def test_list_replay_jobs_with_filters(self, mock_jobs):
        mock_jobs.return_value = {"items": [], "total": 0, "limit": 10, "offset": 0}
        client = TestClient(_app())
        r = client.get("/event-wiring/replay/jobs?status=completed&replay_type=dlq_reingest&limit=10&offset=5")
        assert r.status_code == 200
        mock_jobs.assert_called_once_with(status="completed", replay_type="dlq_reingest", limit=10, offset=5)

    @patch("app.intelligence.event_wiring.get_replay_summary")
    def test_replay_summary_endpoint(self, mock_sum):
        mock_sum.return_value = {
            "total_jobs": 10, "completed": 8, "failed": 1, "running": 1,
            "pending": 0, "total_events_replayed": 100,
            "total_events_processed": 90, "total_events_failed": 10,
        }
        client = TestClient(_app())
        r = client.get("/event-wiring/replay/summary")
        assert r.status_code == 200
        assert r.json()["total_jobs"] == 10

    @patch("app.intelligence.event_wiring.poll_topology_queues")
    def test_poll_topology_endpoint(self, mock_poll):
        mock_poll.return_value = {"queues_polled": 3, "total_received": 15}
        client = TestClient(_app())
        r = client.post("/event-wiring/poll-topology")
        assert r.status_code == 200
        assert r.json()["queues_polled"] == 3


# =====================================================================
#  S20-T17  Edge cases
# =====================================================================


class TestS20EdgeCases:
    def test_wire_row_mapper_all_fields(self):
        row = _make_wire_row(
            id=99, module_name="sqs_topology", event_domain="listing",
            event_action="*", handler_name="catalog_health.listing_watcher",
            description="Watch all listing changes", enabled=True,
            priority=50, timeout_seconds=60,
            created_at="2026-01-01 00:00:00", updated_at="2026-03-12 12:00:00",
        )
        d = ew._wire_row_to_dict(row)
        assert d["module_name"] == "sqs_topology"
        assert d["priority"] == 50
        assert d["timeout_seconds"] == 60

    def test_replay_row_mapper_with_event_ids(self):
        ids_json = '["ev1", "ev2"]'
        row = _make_replay_row(scope_event_ids=ids_json)
        d = ew._replay_row_to_dict(row)
        assert d["scope_event_ids"] == ids_json

    def test_all_replay_types_in_valid_set(self):
        for t in ("event_reset", "dlq_reingest", "bulk_domain", "selective"):
            assert t in ew.VALID_REPLAY_TYPES

    def test_all_replay_statuses_in_valid_set(self):
        for s in ("pending", "running", "completed", "failed", "cancelled"):
            assert s in ew.VALID_REPLAY_STATUSES

    def test_default_wiring_unique_handlers(self):
        handlers = [w["handler_name"] for w in ew.DEFAULT_WIRING]
        assert len(handlers) == len(set(handlers))

    def test_default_wiring_valid_modules(self):
        for w in ew.DEFAULT_WIRING:
            assert w["module_name"] in ew.VALID_WIRE_MODULES

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_wiring_combined_filters(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        ew.get_wiring(module_name="profit", event_domain="ads", enabled_only=True)
        sql = cur.executed[0][0]
        assert "module_name = ?" in sql
        assert "event_domain = ?" in sql
        assert "enabled = 1" in sql

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_get_replay_jobs_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        result = ew.get_replay_jobs()
        assert result["total"] == 0
        assert result["items"] == []

    def test_api_replay_default_body(self):
        """POST /replay with empty body should use defaults."""
        with patch("app.intelligence.event_wiring.replay_and_process") as mock_rap:
            mock_rap.return_value = {"job_id": 1, "status": "completed"}
            client = TestClient(_app())
            r = client.post("/event-wiring/replay", json={})
            assert r.status_code == 200

    def test_api_wire_registration_validation(self):
        """POST /wires requires module_name, event_domain, handler_name."""
        client = TestClient(_app())
        r = client.post("/event-wiring/wires", json={})
        assert r.status_code == 422  # validation error

    def test_api_toggle_requires_body(self):
        """PATCH toggle requires enabled field."""
        client = TestClient(_app())
        r = client.patch("/event-wiring/wires/h1/toggle", json={})
        assert r.status_code == 422

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_update_replay_job_error_message(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        ew.update_replay_job(1, status="failed", error_message="something broke")
        sql = cur.executed[0][0]
        assert "error_message = ?" in sql
        assert "completed_at = SYSUTCDATETIME()" in sql  # failed triggers completed_at

    @patch("app.intelligence.event_wiring.connect_acc")
    def test_update_replay_job_cancelled(self, mock_conn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn.return_value = conn

        ew.update_replay_job(1, status="cancelled")
        sql = cur.executed[0][0]
        assert "completed_at = SYSUTCDATETIME()" in sql
