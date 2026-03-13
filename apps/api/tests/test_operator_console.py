"""Tests for Sprint 23-24 – Operator Console v2.

Covers: constants, row mappers, unified feed, feed summary, case CRUD,
action queue lifecycle, auto-approve low-risk, expire stale actions,
operator dashboard, API endpoints.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.intelligence import operator_console as oc

# ── Local fakes ──────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, rowcount=1):
        self.executed: list[tuple] = []
        self.multi_rows: list = []
        self._idx = 0
        self._rowcount = rowcount

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

    @property
    def rowcount(self):
        return self._rowcount


class _FakeConn:
    def __init__(self, cursor=None):
        self._cursor = cursor or _FakeCursor()

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


# ── Row helpers ──────────────────────────────────────────────────────

def _make_case_row(
    id=1, title="Test case", description="desc", category="other",
    priority="medium", status="open", marketplace_id="A1PA6795UKMFR9",
    sku="SKU-001", asin="B000TEST", source_type=None, source_id=None,
    assigned_to=None, resolution_note=None, resolved_by=None,
    resolved_at=None, due_date=None, tags=None,
    created_at=datetime(2025, 3, 15, 10, 0),
    updated_at=datetime(2025, 3, 15, 10, 0),
):
    return (
        id, title, description, category, priority, status,
        marketplace_id, sku, asin, source_type, source_id,
        assigned_to, resolution_note, resolved_by, resolved_at,
        due_date, tags, created_at, updated_at,
    )


def _make_action_row(
    id=1, action_type="price_change", title="Change price",
    description=None, marketplace_id="A1PA6795UKMFR9",
    sku="SKU-001", asin="B000TEST", payload='{"new_price": 29.99}',
    risk_level="medium", requires_approval=1,
    status="pending_approval", requested_by="operator@test.com",
    approved_by=None, approved_at=None,
    rejected_by=None, rejected_at=None, rejection_reason=None,
    executed_at=None, execution_result=None, error_message=None,
    expires_at=datetime(2025, 3, 18, 10, 0),
    created_at=datetime(2025, 3, 15, 10, 0),
    updated_at=datetime(2025, 3, 15, 10, 0),
):
    return (
        id, action_type, title, description, marketplace_id, sku, asin,
        payload, risk_level, requires_approval, status, requested_by,
        approved_by, approved_at, rejected_by, rejected_at, rejection_reason,
        executed_at, execution_result, error_message, expires_at,
        created_at, updated_at,
    )


# ====================================================================
# 1. Constants
# ====================================================================

class TestConstants:
    def test_case_categories(self):
        assert "refund_anomaly" in oc.CASE_CATEGORIES
        assert "fee_dispute" in oc.CASE_CATEGORIES
        assert "inventory_discrepancy" in oc.CASE_CATEGORIES
        assert "listing_issue" in oc.CASE_CATEGORIES
        assert "buybox_loss" in oc.CASE_CATEGORIES
        assert "compliance" in oc.CASE_CATEGORIES
        assert "other" in oc.CASE_CATEGORIES
        assert len(oc.CASE_CATEGORIES) == 8

    def test_case_priorities(self):
        assert oc.CASE_PRIORITIES == {"critical", "high", "medium", "low"}

    def test_case_statuses(self):
        for s in ("open", "in_progress", "waiting", "resolved", "closed"):
            assert s in oc.CASE_STATUSES

    def test_action_queue_statuses(self):
        for s in ("pending_approval", "approved", "rejected", "executed", "failed", "expired"):
            assert s in oc.ACTION_QUEUE_STATUSES

    def test_auto_approve_low_risk_default(self):
        assert oc.AUTO_APPROVE_LOW_RISK is True


# ====================================================================
# 2. Row mappers
# ====================================================================

class TestRowMappers:
    def test_row_to_case(self):
        row = _make_case_row()
        d = oc._row_to_case(row)
        assert d["id"] == 1
        assert d["title"] == "Test case"
        assert d["category"] == "other"
        assert d["priority"] == "medium"
        assert d["status"] == "open"
        assert d["marketplace_id"] == "A1PA6795UKMFR9"
        assert d["sku"] == "SKU-001"
        assert "created_at" in d

    def test_row_to_case_with_resolved(self):
        row = _make_case_row(
            status="resolved",
            resolved_by="admin",
            resolved_at=datetime(2025, 3, 16, 12, 0),
        )
        d = oc._row_to_case(row)
        assert d["status"] == "resolved"
        assert d["resolved_by"] == "admin"
        assert "2025-03-16" in d["resolved_at"]

    def test_row_to_action(self):
        row = _make_action_row()
        d = oc._row_to_action(row)
        assert d["id"] == 1
        assert d["action_type"] == "price_change"
        assert d["risk_level"] == "medium"
        assert d["requires_approval"] is True
        assert d["status"] == "pending_approval"
        assert d["payload"] == {"new_price": 29.99}

    def test_row_to_action_null_payload(self):
        row = _make_action_row(payload=None)
        d = oc._row_to_action(row)
        assert d["payload"] is None

    def test_row_to_action_approved(self):
        row = _make_action_row(
            status="approved",
            approved_by="admin",
            approved_at=datetime(2025, 3, 15, 11, 0),
        )
        d = oc._row_to_action(row)
        assert d["status"] == "approved"
        assert d["approved_by"] == "admin"
        assert "2025-03-15" in d["approved_at"]


# ====================================================================
# 3. Unified Feed
# ====================================================================

class TestUnifiedFeed:
    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_unified_feed_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_unified_feed(days=7)
        assert result["total"] == 0
        assert result["items"] == []
        assert result["page"] == 1

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_unified_feed_with_items(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (3,),  # count
            ("alert", "1", "Test Alert", "Detail", "critical", "A1PA", None, None, "open", datetime(2025, 3, 15)),
            ("system", "2", "Guard fail", "Msg", "warning", None, None, None, "open", datetime(2025, 3, 14)),
        ]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_unified_feed(days=7, page=1, page_size=50)
        assert result["total"] == 3
        assert len(result["items"]) == 2
        assert result["items"][0]["source"] == "alert"
        assert result["items"][1]["source"] == "system"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_unified_feed_source_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,), ("alert", "1", "Alert", None, "high", "A1PA", None, None, "open", datetime(2025, 3, 15))]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_unified_feed(days=7, source="alert")
        assert result["total"] == 1
        # only alert source queried
        sql_text = cur.executed[0][0]
        assert "acc_al_alerts" in sql_text
        assert "acc_system_alert" not in sql_text

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_unified_feed_severity_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_conn.return_value = _FakeConn(cur)

        oc.get_unified_feed(days=7, severity="critical")
        # severity param should be in the query
        sql_text = cur.executed[0][0]
        assert "severity" in sql_text.lower()

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_unified_feed_invalid_source(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_unified_feed(source="nonexistent")
        assert result["items"] == []
        assert result["total"] == 0


# ====================================================================
# 4. Feed Summary
# ====================================================================

class TestFeedSummary:
    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_feed_summary(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (10, 2, 5),    # alerts: total, critical, unresolved
            (3, 1),        # system_alerts: total, critical
            (7, 3, 4),     # anomalies: total, critical, open
            (15, 8, 2),    # cases: total, open, critical
            (20, 5),       # action_queue: total, pending_approval
        ]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_feed_summary(days=7)
        assert result["alerts"]["total"] == 10
        assert result["alerts"]["critical"] == 2
        assert result["alerts"]["unresolved"] == 5
        assert result["system_alerts"]["total"] == 3
        assert result["anomalies"]["open"] == 4
        assert result["cases"]["total"] == 15
        assert result["cases"]["critical"] == 2
        assert result["action_queue"]["pending_approval"] == 5


# ====================================================================
# 5. Case CRUD
# ====================================================================

class TestCaseManagement:
    @patch("app.intelligence.operator_console.connect_acc")
    def test_list_cases_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.list_operator_cases()
        assert result["total"] == 0
        assert result["items"] == []

    @patch("app.intelligence.operator_console.connect_acc")
    def test_list_cases_with_rows(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(2,), _make_case_row(id=1), _make_case_row(id=2, title="Case 2")]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.list_operator_cases()
        assert result["total"] == 2
        assert len(result["items"]) == 2
        assert result["items"][0]["id"] == 1
        assert result["items"][1]["title"] == "Case 2"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_list_cases_with_status_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,), _make_case_row(status="in_progress")]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.list_operator_cases(status="in_progress")
        assert result["total"] == 1
        sql_text = cur.executed[0][0]
        assert "status" in sql_text.lower()

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_operator_case_found(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_case_row(id=5)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_operator_case(5)
        assert result is not None
        assert result["id"] == 5

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_operator_case_not_found(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_operator_case(999)
        assert result is None

    @patch("app.intelligence.operator_console.connect_acc")
    def test_create_operator_case(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(42,)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.create_operator_case(
            title="New case",
            category="refund_anomaly",
            priority="high",
        )
        assert result["id"] == 42
        assert result["status"] == "open"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_create_case_with_all_fields(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(10,)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.create_operator_case(
            title="Full case",
            description="Full desc",
            category="listing_issue",
            priority="critical",
            marketplace_id="A1PA6795UKMFR9",
            sku="SKU-X",
            asin="B000Y",
            source_type="alert",
            source_id="123",
            assigned_to="bob",
            due_date="2025-04-01",
            tags="urgent,vip",
        )
        assert result["id"] == 10

    @patch("app.intelligence.operator_console.connect_acc")
    def test_update_case_status(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.update_operator_case(1, status="in_progress")
        assert result is not None
        assert result["id"] == 1
        assert result["updated"] is True

    @patch("app.intelligence.operator_console.connect_acc")
    def test_update_case_resolved(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.update_operator_case(
            1,
            status="resolved",
            resolved_by="admin",
            resolution_note="Fixed",
        )
        assert result is not None
        sql_text = cur.executed[0][0]
        assert "resolved_by" in sql_text.lower()

    @patch("app.intelligence.operator_console.connect_acc")
    def test_update_case_not_found(self, mock_conn):
        cur = _FakeCursor(rowcount=0)
        mock_conn.return_value = _FakeConn(cur)

        result = oc.update_operator_case(999, status="closed")
        assert result is None


# ====================================================================
# 6. Action Queue
# ====================================================================

class TestActionQueue:
    @patch("app.intelligence.operator_console.connect_acc")
    def test_list_action_queue_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.list_action_queue()
        assert result["total"] == 0
        assert result["items"] == []

    @patch("app.intelligence.operator_console.connect_acc")
    def test_list_action_queue_with_items(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,), _make_action_row()]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.list_action_queue()
        assert result["total"] == 1
        assert len(result["items"]) == 1
        assert result["items"][0]["action_type"] == "price_change"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_list_action_queue_status_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_conn.return_value = _FakeConn(cur)

        oc.list_action_queue(status="approved")
        sql_text = cur.executed[0][0]
        assert "status" in sql_text.lower()

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_action_queue_item_found(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_action_row(id=7)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_action_queue_item(7)
        assert result is not None
        assert result["id"] == 7

    @patch("app.intelligence.operator_console.connect_acc")
    def test_get_action_queue_item_not_found(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.get_action_queue_item(999)
        assert result is None

    @patch("app.intelligence.operator_console.connect_acc")
    def test_submit_action_medium_risk(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(55,)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.submit_action(
            action_type="price_change",
            title="Update price",
            risk_level="medium",
            requested_by="ops@test.com",
        )
        assert result["id"] == 55
        assert result["status"] == "pending_approval"
        assert result["requires_approval"] is True

    @patch("app.intelligence.operator_console.connect_acc")
    def test_submit_action_low_risk_auto_approve(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(56,)]
        mock_conn.return_value = _FakeConn(cur)

        result = oc.submit_action(
            action_type="content_fix",
            title="Fix typo",
            risk_level="low",
            requested_by="ops@test.com",
        )
        assert result["id"] == 56
        assert result["status"] == "approved"
        assert result["requires_approval"] is False

    @patch("app.intelligence.operator_console.connect_acc")
    def test_submit_action_with_payload(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(57,)]
        mock_conn.return_value = _FakeConn(cur)

        payload = {"old_price": 25.0, "new_price": 29.99}
        result = oc.submit_action(
            action_type="price_change",
            title="Reprice SKU",
            payload=payload,
            risk_level="high",
            requested_by="admin@test.com",
            sku="SKU-001",
        )
        assert result["id"] == 57
        # Check payload was serialized in execute
        sql_params = cur.executed[0][1]
        assert json.dumps(payload) in [p for p in sql_params if isinstance(p, str)]

    @patch("app.intelligence.operator_console.connect_acc")
    def test_approve_action(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.approve_action(10, approved_by="admin")
        assert result is not None
        assert result["id"] == 10
        assert result["status"] == "approved"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_approve_action_not_pending(self, mock_conn):
        cur = _FakeCursor(rowcount=0)
        mock_conn.return_value = _FakeConn(cur)

        result = oc.approve_action(999, approved_by="admin")
        assert result is None

    @patch("app.intelligence.operator_console.connect_acc")
    def test_reject_action(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.reject_action(11, rejected_by="admin", reason="Not needed")
        assert result is not None
        assert result["id"] == 11
        assert result["status"] == "rejected"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_reject_action_not_pending(self, mock_conn):
        cur = _FakeCursor(rowcount=0)
        mock_conn.return_value = _FakeConn(cur)

        result = oc.reject_action(999, rejected_by="admin")
        assert result is None

    @patch("app.intelligence.operator_console.connect_acc")
    def test_mark_action_executed_success(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.mark_action_executed(12, result="Done")
        assert result is not None
        assert result["status"] == "executed"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_mark_action_executed_failure(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = oc.mark_action_executed(13, error="Timeout")
        assert result is not None
        assert result["status"] == "failed"

    @patch("app.intelligence.operator_console.connect_acc")
    def test_mark_action_not_approved(self, mock_conn):
        cur = _FakeCursor(rowcount=0)
        mock_conn.return_value = _FakeConn(cur)

        result = oc.mark_action_executed(999)
        assert result is None


# ====================================================================
# 7. Expire stale actions
# ====================================================================

class TestExpireStaleActions:
    @patch("app.intelligence.operator_console.connect_acc")
    def test_expire_stale(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        count = oc.expire_stale_actions()
        assert isinstance(count, int)


# ====================================================================
# 8. Dashboard
# ====================================================================

class TestDashboard:
    @patch("app.intelligence.operator_console.get_feed_summary")
    def test_get_operator_dashboard(self, mock_summary):
        mock_summary.return_value = {"alerts": {}, "cases": {}, "action_queue": {}}
        result = oc.get_operator_dashboard()
        mock_summary.assert_called_once_with(days=7)
        assert "alerts" in result


# ====================================================================
# 9. Schema DDL
# ====================================================================

class TestSchema:
    @patch("app.intelligence.operator_console.connect_acc")
    def test_ensure_schema(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        oc.ensure_operator_console_schema()
        assert len(cur.executed) == len(oc._SCHEMA_STATEMENTS)


# ====================================================================
# 10. API Endpoints
# ====================================================================

def _make_app():
    from app.api.v1.operator_console import router
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


class TestAPIDashboard:
    def test_get_dashboard(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_operator_dashboard") as mock:
            mock.return_value = {
                "alerts": {"total": 5, "critical": 1, "unresolved": 3},
                "system_alerts": {"total": 2, "critical": 0},
                "anomalies": {"total": 4, "critical": 1, "open": 2},
                "cases": {"total": 8, "open": 3, "critical": 1},
                "action_queue": {"total": 10, "pending_approval": 4},
            }
            resp = client.get("/api/v1/operator-console/dashboard")
            assert resp.status_code == 200
            data = resp.json()
            assert data["alerts"]["total"] == 5

    def test_dashboard_error(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_operator_dashboard") as mock:
            mock.side_effect = Exception("db error")
            resp = client.get("/api/v1/operator-console/dashboard")
            assert resp.status_code == 500


class TestAPIFeed:
    def test_get_feed(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_unified_feed") as mock:
            mock.return_value = {"items": [], "total": 0, "page": 1, "page_size": 50}
            resp = client.get("/api/v1/operator-console/feed")
            assert resp.status_code == 200
            assert resp.json()["total"] == 0

    def test_get_feed_with_filters(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_unified_feed") as mock:
            mock.return_value = {"items": [], "total": 0, "page": 1, "page_size": 50}
            resp = client.get("/api/v1/operator-console/feed?source=alert&severity=critical&days=3")
            assert resp.status_code == 200

    def test_get_feed_summary(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_feed_summary") as mock:
            mock.return_value = {"alerts": {}, "cases": {}, "action_queue": {}}
            resp = client.get("/api/v1/operator-console/feed/summary")
            assert resp.status_code == 200


class TestAPICases:
    def test_list_cases(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.list_operator_cases") as mock:
            mock.return_value = {"items": [], "total": 0, "page": 1, "page_size": 50}
            resp = client.get("/api/v1/operator-console/cases")
            assert resp.status_code == 200

    def test_get_case(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_operator_case") as mock:
            mock.return_value = {"id": 1, "title": "Test", "status": "open"}
            resp = client.get("/api/v1/operator-console/cases/1")
            assert resp.status_code == 200
            assert resp.json()["id"] == 1

    def test_get_case_not_found(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_operator_case") as mock:
            mock.return_value = None
            resp = client.get("/api/v1/operator-console/cases/999")
            assert resp.status_code == 404

    def test_create_case(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.create_operator_case") as mock:
            mock.return_value = {"id": 10, "status": "open"}
            resp = client.post("/api/v1/operator-console/cases", json={
                "title": "New case",
                "category": "refund_anomaly",
                "priority": "high",
            })
            assert resp.status_code == 201
            assert resp.json()["id"] == 10

    def test_update_case(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.update_operator_case") as mock:
            mock.return_value = {"id": 1, "updated": True}
            resp = client.patch("/api/v1/operator-console/cases/1", json={
                "status": "in_progress",
            })
            assert resp.status_code == 200

    def test_update_case_not_found(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.update_operator_case") as mock:
            mock.return_value = None
            resp = client.patch("/api/v1/operator-console/cases/999", json={"status": "closed"})
            assert resp.status_code == 404


class TestAPIActions:
    def test_list_actions(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.list_action_queue") as mock:
            mock.return_value = {"items": [], "total": 0, "page": 1, "page_size": 50}
            resp = client.get("/api/v1/operator-console/actions")
            assert resp.status_code == 200

    def test_get_action(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_action_queue_item") as mock:
            mock.return_value = {"id": 5, "status": "pending_approval"}
            resp = client.get("/api/v1/operator-console/actions/5")
            assert resp.status_code == 200

    def test_get_action_not_found(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.get_action_queue_item") as mock:
            mock.return_value = None
            resp = client.get("/api/v1/operator-console/actions/999")
            assert resp.status_code == 404

    def test_submit_action(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.submit_action") as mock:
            mock.return_value = {"id": 20, "status": "pending_approval", "requires_approval": True}
            resp = client.post("/api/v1/operator-console/actions", json={
                "action_type": "price_change",
                "title": "Change price",
                "requested_by": "ops@test.com",
                "risk_level": "high",
            })
            assert resp.status_code == 201
            assert resp.json()["id"] == 20

    def test_approve_action(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.approve_action") as mock:
            mock.return_value = {"id": 5, "status": "approved"}
            resp = client.post("/api/v1/operator-console/actions/5/approve", json={
                "approved_by": "admin",
            })
            assert resp.status_code == 200

    def test_approve_action_not_found(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.approve_action") as mock:
            mock.return_value = None
            resp = client.post("/api/v1/operator-console/actions/999/approve", json={
                "approved_by": "admin",
            })
            assert resp.status_code == 404

    def test_reject_action(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.reject_action") as mock:
            mock.return_value = {"id": 5, "status": "rejected"}
            resp = client.post("/api/v1/operator-console/actions/5/reject", json={
                "rejected_by": "admin",
                "reason": "Not approved",
            })
            assert resp.status_code == 200

    def test_reject_action_not_found(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.reject_action") as mock:
            mock.return_value = None
            resp = client.post("/api/v1/operator-console/actions/999/reject", json={
                "rejected_by": "admin",
            })
            assert resp.status_code == 404

    def test_mark_executed(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.mark_action_executed") as mock:
            mock.return_value = {"id": 5, "status": "executed"}
            resp = client.post("/api/v1/operator-console/actions/5/executed", json={
                "result": "Done successfully",
            })
            assert resp.status_code == 200

    def test_mark_executed_not_found(self):
        client = _make_app()
        with patch("app.intelligence.operator_console.mark_action_executed") as mock:
            mock.return_value = None
            resp = client.post("/api/v1/operator-console/actions/999/executed", json={})
            assert resp.status_code == 404
