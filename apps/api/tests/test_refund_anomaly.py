"""Tests for Sprint 21-22 – Refund / Fee Anomaly Engine.

Covers: constants, row mappers, refund spike detection, fee spike detection,
return rate spike detection, serial returner detection, reimbursement case
scanning (incl. damaged inbound & fee overcharge), CRUD operations,
detail lookups, trend queries, CSV export, dashboard, full scan
orchestration, API endpoints, scheduler integration.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.intelligence import refund_anomaly as ra

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


# ── Row helpers ──────────────────────────────────────────────────────

def _make_anomaly_row(
    id=1, sku="SKU-001", asin="B000TEST", marketplace_id="A1PA6795UKMFR9",
    anomaly_type="refund_spike", detection_date=date(2025, 3, 12),
    period_start=date(2025, 2, 12), period_end=date(2025, 3, 12),
    baseline_rate=0.02, current_rate=0.08, spike_ratio=4.0,
    refund_count=8, order_count=100, refund_amount_pln=400.0,
    estimated_loss_pln=120.0, severity="critical", status="open",
    resolution_note=None, resolved_by=None, resolved_at=None,
    created_at=datetime(2025, 3, 12, 10, 0), updated_at=datetime(2025, 3, 12, 10, 0),
):
    return (
        id, sku, asin, marketplace_id, anomaly_type,
        detection_date, period_start, period_end,
        baseline_rate, current_rate, spike_ratio,
        refund_count, order_count, refund_amount_pln, estimated_loss_pln,
        severity, status, resolution_note, resolved_by, resolved_at,
        created_at, updated_at,
    )


def _make_returner_row(
    id=1, buyer_identifier="Berlin|10115", marketplace_id="A1PA6795UKMFR9",
    detection_date=date(2025, 3, 12), return_count=7, order_count=10,
    return_rate=0.7, total_refund_pln=350.0, avg_refund_pln=50.0,
    first_return_date=date(2025, 1, 5), last_return_date=date(2025, 3, 10),
    top_skus='["SKU-001","SKU-002"]', risk_score=85, risk_tier="critical",
    status="flagged", notes=None,
    created_at=datetime(2025, 3, 12, 10, 0), updated_at=datetime(2025, 3, 12, 10, 0),
):
    return (
        id, buyer_identifier, marketplace_id, detection_date,
        return_count, order_count, return_rate,
        total_refund_pln, avg_refund_pln,
        first_return_date, last_return_date, top_skus,
        risk_score, risk_tier, status, notes,
        created_at, updated_at,
    )


def _make_case_row(
    id=1, case_type="lost_inventory", sku="SKU-001", asin="B000TEST",
    marketplace_id="A1PA6795UKMFR9", amazon_order_id="111-222-333",
    fnsku="FNSKU1", quantity=2, estimated_value_pln=80.0,
    evidence_summary="Return status: lost_in_transit. COGS per unit: 40.0.",
    amazon_case_id=None, status="identified",
    filed_at=None, resolved_at=None, reimbursed_amount_pln=0,
    resolution_note=None, created_at=datetime(2025, 3, 12, 10, 0),
):
    return (
        id, case_type, sku, asin, marketplace_id,
        amazon_order_id, fnsku, quantity, estimated_value_pln,
        evidence_summary, amazon_case_id, status,
        filed_at, resolved_at, reimbursed_amount_pln,
        resolution_note, created_at,
    )


# ====================================================================
# 1. Constants
# ====================================================================

class TestConstants:
    def test_anomaly_types(self):
        assert "refund_spike" in ra.ANOMALY_TYPES
        assert "fee_spike" in ra.ANOMALY_TYPES
        assert "return_rate_spike" in ra.ANOMALY_TYPES

    def test_anomaly_severities(self):
        assert ra.ANOMALY_SEVERITIES == {"critical", "high", "medium", "low"}

    def test_anomaly_statuses(self):
        assert "open" in ra.ANOMALY_STATUSES
        assert "investigating" in ra.ANOMALY_STATUSES
        assert "resolved" in ra.ANOMALY_STATUSES
        assert "dismissed" in ra.ANOMALY_STATUSES

    def test_returner_risk_tiers(self):
        assert ra.RETURNER_RISK_TIERS == {"critical", "high", "medium", "low"}

    def test_returner_statuses(self):
        assert "flagged" in ra.RETURNER_STATUSES
        assert "monitoring" in ra.RETURNER_STATUSES
        assert "cleared" in ra.RETURNER_STATUSES
        assert "blocked" in ra.RETURNER_STATUSES

    def test_case_types(self):
        assert "lost_inventory" in ra.CASE_TYPES
        assert "damaged_inbound" in ra.CASE_TYPES
        assert "fee_overcharge" in ra.CASE_TYPES
        assert "customer_return_not_received" in ra.CASE_TYPES

    def test_case_statuses(self):
        assert "identified" in ra.CASE_STATUSES
        assert "filed" in ra.CASE_STATUSES
        assert "paid" in ra.CASE_STATUSES

    def test_spike_thresholds(self):
        assert ra.SPIKE_RATIO_CRITICAL > ra.SPIKE_RATIO_HIGH > ra.SPIKE_RATIO_MEDIUM
        assert ra.SPIKE_RATIO_CRITICAL == 3.0
        assert ra.SPIKE_RATIO_HIGH == 2.0
        assert ra.SPIKE_RATIO_MEDIUM == 1.5


# ====================================================================
# 2. Row mappers
# ====================================================================

class TestAnomalyRowMapper:
    def test_maps_all_fields(self):
        row = _make_anomaly_row()
        d = ra._anomaly_row_to_dict(row)
        assert d["id"] == 1
        assert d["sku"] == "SKU-001"
        assert d["asin"] == "B000TEST"
        assert d["marketplace_id"] == "A1PA6795UKMFR9"
        assert d["anomaly_type"] == "refund_spike"
        assert d["spike_ratio"] == 4.0
        assert d["refund_count"] == 8
        assert d["order_count"] == 100
        assert d["severity"] == "critical"
        assert d["status"] == "open"
        assert d["created_at"] is not None

    def test_handles_none_dates(self):
        row = _make_anomaly_row(resolved_at=None, detection_date=None)
        d = ra._anomaly_row_to_dict(row)
        assert d["resolved_at"] is None
        assert d["detection_date"] is None

    def test_rounding(self):
        row = _make_anomaly_row(spike_ratio=2.555555, refund_amount_pln=123.456789)
        d = ra._anomaly_row_to_dict(row)
        assert d["spike_ratio"] == 2.56
        assert d["refund_amount_pln"] == 123.46


class TestReturnerRowMapper:
    def test_maps_all_fields(self):
        row = _make_returner_row()
        d = ra._returner_row_to_dict(row)
        assert d["id"] == 1
        assert d["buyer_identifier"] == "Berlin|10115"
        assert d["return_count"] == 7
        assert d["order_count"] == 10
        assert d["return_rate"] == 0.7
        assert d["risk_score"] == 85
        assert d["risk_tier"] == "critical"
        assert d["status"] == "flagged"

    def test_handles_none_dates(self):
        row = _make_returner_row(first_return_date=None, last_return_date=None)
        d = ra._returner_row_to_dict(row)
        assert d["first_return_date"] is None
        assert d["last_return_date"] is None


class TestCaseRowMapper:
    def test_maps_all_fields(self):
        row = _make_case_row()
        d = ra._case_row_to_dict(row)
        assert d["id"] == 1
        assert d["case_type"] == "lost_inventory"
        assert d["sku"] == "SKU-001"
        assert d["quantity"] == 2
        assert d["estimated_value_pln"] == 80.0
        assert d["status"] == "identified"

    def test_handles_none_reimbursement(self):
        row = _make_case_row(reimbursed_amount_pln=None)
        d = ra._case_row_to_dict(row)
        assert d["reimbursed_amount_pln"] == 0


# ====================================================================
# 3. Severity / Risk scoring helpers
# ====================================================================

class TestClassifySeverity:
    def test_critical(self):
        assert ra._classify_severity(3.5) == "critical"

    def test_high(self):
        assert ra._classify_severity(2.5) == "high"

    def test_medium(self):
        assert ra._classify_severity(1.7) == "medium"

    def test_low(self):
        assert ra._classify_severity(1.2) == "low"

    def test_boundary_critical(self):
        assert ra._classify_severity(3.0) == "critical"

    def test_boundary_high(self):
        assert ra._classify_severity(2.0) == "high"

    def test_boundary_medium(self):
        assert ra._classify_severity(1.5) == "medium"


class TestComputeRiskScore:
    def test_high_return_count_and_rate(self):
        score = ra._compute_risk_score(return_count=10, return_rate=0.9, total_refund=1500.0)
        assert score >= 80  # critical

    def test_moderate_scores(self):
        score = ra._compute_risk_score(return_count=4, return_rate=0.3, total_refund=200.0)
        assert 20 <= score <= 60

    def test_minimal_activity(self):
        score = ra._compute_risk_score(return_count=1, return_rate=0.1, total_refund=50.0)
        assert score < 30

    def test_max_cap_100(self):
        score = ra._compute_risk_score(return_count=100, return_rate=1.0, total_refund=10000.0)
        assert score == 100

    def test_zero_refund(self):
        score = ra._compute_risk_score(return_count=3, return_rate=0.5, total_refund=0)
        assert score >= 0


class TestClassifyRiskTier:
    def test_critical(self):
        assert ra._classify_risk_tier(85) == "critical"

    def test_high(self):
        assert ra._classify_risk_tier(65) == "high"

    def test_medium(self):
        assert ra._classify_risk_tier(45) == "medium"

    def test_low(self):
        assert ra._classify_risk_tier(20) == "low"


# ====================================================================
# 4. Refund spike detection
# ====================================================================

class TestDetectRefundSpikes:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_detects_spike_and_inserts(self, mock_conn_fn):
        cur = MagicMock()
        cur.executed = []
        real_execute = lambda sql, params=None: cur.executed.append((sql, params))
        cur.execute = MagicMock(side_effect=real_execute)
        # First fetchall → recent rows, second → baseline rows
        recent_rows = [("SKU-001", "B000TEST", "A1PA6795UKMFR9", 10, 50, 200.0)]
        baseline_rows = [("SKU-001", "A1PA6795UKMFR9", 2, 50)]
        cur.fetchall = MagicMock(side_effect=[recent_rows, baseline_rows])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_refund_spikes(lookback_days=28)
        assert result["anomalies_created"] == 1
        assert result["skus_analyzed"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_no_spike_when_below_threshold(self, mock_conn_fn):
        cur = MagicMock()
        real_execute = lambda sql, params=None: None
        cur.execute = MagicMock(side_effect=real_execute)
        # Recent: 3/50 = 6%, Baseline: 3/50 = 6% → ratio 1.0, below threshold
        recent = [("SKU-001", "B000TEST", "A1PA6795UKMFR9", 3, 50, 90.0)]
        baseline = [("SKU-001", "A1PA6795UKMFR9", 3, 50)]
        cur.fetchall = MagicMock(side_effect=[recent, baseline])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_refund_spikes(lookback_days=28)
        assert result["anomalies_created"] == 0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_spike_with_no_baseline(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        recent = [("SKU-NEW", "B000NEW", "A1PA6795UKMFR9", 5, 20, 100.0)]
        cur.multi_rows = [*recent]  # No baseline rows

        result = ra.detect_refund_spikes(lookback_days=28)
        # With no baseline and current rate > 0, spike_ratio = 10.0 → critical
        assert result["anomalies_created"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_marketplace_filter(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.detect_refund_spikes(marketplace_id="A1PA6795UKMFR9")
        assert result["anomalies_created"] == 0
        # Check marketplace filter was included in query
        sql = cur.executed[0][0]
        assert "marketplace_id" in sql

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_rollback_on_error(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn_fn.return_value = conn

        cur.execute = MagicMock(side_effect=[None, Exception("db error")])
        with pytest.raises(Exception, match="db error"):
            ra.detect_refund_spikes()
        conn.rollback.assert_called_once()


# ====================================================================
# 5. Serial returner detection
# ====================================================================

class TestDetectSerialReturners:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_detects_serial_returner(self, mock_conn_fn):
        cur = MagicMock()
        real_execute = lambda sql, params=None: None
        cur.execute = MagicMock(side_effect=real_execute)
        # First fetchall → buyer rows, second → top SKUs
        buyer_rows = [("Berlin|10115", "A1PA6795UKMFR9", 7, 10, 350.0,
                        date(2025, 1, 5), date(2025, 3, 10))]
        top_sku_rows = [("SKU-001", 4), ("SKU-002", 3)]
        cur.fetchall = MagicMock(side_effect=[buyer_rows, top_sku_rows])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_serial_returners(lookback_days=90)
        assert result["returners_flagged"] == 1
        assert result["buyers_analyzed"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_low_rate_not_flagged(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        # Buyer with 3 returns out of 100 orders (3%), below threshold
        buyer_rows = [("Munich|80331", "A1PA6795UKMFR9", 3, 100, 60.0,
                        date(2025, 2, 1), date(2025, 3, 1))]
        cur.multi_rows = [*buyer_rows]

        result = ra.detect_serial_returners(lookback_days=90)
        assert result["returners_flagged"] == 0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_marketplace_filter(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.detect_serial_returners(marketplace_id="A13V1IB3VIYZZH")
        assert "buyers_analyzed" in result
        sql = cur.executed[0][0]
        assert "marketplace_id" in sql


# ====================================================================
# 6. Reimbursement case scanning
# ====================================================================

class TestScanReimbursementOpportunities:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_creates_cases_for_lost_items(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        eligible = [
            ("SKU-001", "B000TEST", "A1PA6795UKMFR9", "111-222-333",
             "lost_in_transit", 40.0, 80.0, 2),
        ]
        cur.multi_rows = [*eligible]

        result = ra.scan_reimbursement_opportunities()
        assert result["cases_created"] == 1
        assert result["items_scanned"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_damaged_return_creates_case(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        eligible = [
            ("SKU-002", "B000DMG", "A1PA6795UKMFR9", "444-555-666",
             "damaged_return", 30.0, 60.0, 1),
        ]
        cur.multi_rows = [*eligible]

        result = ra.scan_reimbursement_opportunities()
        assert result["cases_created"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_no_eligible_items(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.scan_reimbursement_opportunities()
        assert result["cases_created"] == 0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_marketplace_filter(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.scan_reimbursement_opportunities(marketplace_id="A1PA6795UKMFR9")
        assert result["items_scanned"] == 0
        sql = cur.executed[0][0]
        assert "marketplace_id" in sql


# ====================================================================
# 7. Get anomalies (CRUD)
# ====================================================================

class TestGetAnomalies:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_returns_paginated(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        row = _make_anomaly_row()
        cur.multi_rows = [(1,), row]  # first = count, then data row

        result = ra.get_anomalies(limit=50, offset=0)
        assert result["total"] == 1
        assert len(result["items"]) == 1
        assert result["items"][0]["sku"] == "SKU-001"

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_filters_applied(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [(0,)]

        ra.get_anomalies(severity="critical", status="open", sku="SKU-001")
        sql = cur.executed[0][0]
        assert "severity" in sql
        # status and sku are passed as params

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_empty_result(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [(0,)]

        result = ra.get_anomalies()
        assert result["total"] == 0
        assert result["items"] == []


# ====================================================================
# 8. Update anomaly status
# ====================================================================

class TestUpdateAnomalyStatus:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_updates_status(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.update_anomaly_status(1, status="investigating")
        assert result["id"] == 1
        assert result["status"] == "investigating"
        assert result["updated"] is True

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_resolved_sets_resolved_at(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        ra.update_anomaly_status(1, status="resolved", resolved_by="admin")
        sql = cur.executed[0][0]
        assert "SYSUTCDATETIME()" in sql

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError, match="Invalid status"):
            ra.update_anomaly_status(1, status="invalid_status")


# ====================================================================
# 9. Get serial returners (CRUD)
# ====================================================================

class TestGetSerialReturners:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_returns_paginated(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        row = _make_returner_row()
        cur.multi_rows = [(1,), row]

        result = ra.get_serial_returners(limit=50, offset=0)
        assert result["total"] == 1
        assert len(result["items"]) == 1
        assert result["items"][0]["buyer_identifier"] == "Berlin|10115"

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_filters_by_risk_tier(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [(0,)]

        ra.get_serial_returners(risk_tier="critical")
        sql = cur.executed[0][0]
        assert "risk_tier" in sql


# ====================================================================
# 10. Update returner status
# ====================================================================

class TestUpdateReturnerStatus:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_updates_status(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.update_returner_status(1, status="monitoring", notes="Under observation")
        assert result["id"] == 1
        assert result["status"] == "monitoring"

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError, match="Invalid status"):
            ra.update_returner_status(1, status="bad_status")


# ====================================================================
# 11. Get reimbursement cases (CRUD)
# ====================================================================

class TestGetReimbursementCases:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_returns_paginated(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        row = _make_case_row()
        cur.multi_rows = [(1,), row]

        result = ra.get_reimbursement_cases(limit=50, offset=0)
        assert result["total"] == 1
        assert len(result["items"]) == 1
        assert result["items"][0]["case_type"] == "lost_inventory"

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_filters_by_type(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [(0,)]

        ra.get_reimbursement_cases(case_type="fee_overcharge")
        sql = cur.executed[0][0]
        assert "case_type" in sql


# ====================================================================
# 12. Update reimbursement case
# ====================================================================

class TestUpdateCaseStatus:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_updates_to_filed(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.update_case_status(1, status="filed")
        assert result["id"] == 1
        assert result["status"] == "filed"

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_accepted_with_case_id(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.update_case_status(
            1, status="accepted",
            amazon_case_id="CASE-12345",
            reimbursed_amount_pln=75.0,
        )
        assert result["status"] == "accepted"

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_paid_sets_resolved(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        ra.update_case_status(1, status="paid", reimbursed_amount_pln=80.0)
        sql = cur.executed[0][0]
        assert "resolved_at" in sql

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError, match="Invalid status"):
            ra.update_case_status(1, status="unknown")


# ====================================================================
# 13. Dashboard
# ====================================================================

class TestGetAnomalyDashboard:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_returns_all_sections(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        # Anomaly summary row
        anom = (10, 5, 2, 3, 1500.0, 900.0)
        # Returner summary row
        ret = (8, 2, 3, 2500.0)
        # Case summary row
        case = (15, 6, 4, 3, 8000.0, 2400.0)
        cur.multi_rows = [anom, ret, case]

        result = ra.get_anomaly_dashboard()
        assert result["anomalies"]["total"] == 10
        assert result["anomalies"]["open"] == 5
        assert result["anomalies"]["critical_open"] == 2
        assert result["serial_returners"]["total_active"] == 8
        assert result["serial_returners"]["critical"] == 2
        assert result["reimbursements"]["total_cases"] == 15
        assert result["reimbursements"]["pending"] == 6
        assert result["reimbursements"]["total_reimbursed_pln"] == 2400.0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_empty_dashboard(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        # All nulls
        cur.multi_rows = [(0, 0, 0, 0, None, None), (0, 0, 0, None), (0, 0, 0, 0, None, None)]

        result = ra.get_anomaly_dashboard()
        assert result["anomalies"]["total"] == 0
        assert result["serial_returners"]["total_active"] == 0
        assert result["reimbursements"]["total_cases"] == 0


# ====================================================================
# 14. Full scan orchestration
# ====================================================================

class TestRunFullScan:
    @patch("app.intelligence.refund_anomaly.scan_reimbursement_opportunities")
    @patch("app.intelligence.refund_anomaly.detect_serial_returners")
    @patch("app.intelligence.refund_anomaly.detect_return_rate_spikes")
    @patch("app.intelligence.refund_anomaly.detect_fee_spikes")
    @patch("app.intelligence.refund_anomaly.detect_refund_spikes")
    def test_runs_all_five_detectors(self, mock_spikes, mock_fee, mock_return, mock_returners, mock_reimb):
        mock_spikes.return_value = {"anomalies_created": 3, "skus_analyzed": 50}
        mock_fee.return_value = {"anomalies_created": 2, "skus_analyzed": 30}
        mock_return.return_value = {"anomalies_created": 1, "skus_analyzed": 20}
        mock_returners.return_value = {"returners_flagged": 2, "buyers_analyzed": 100}
        mock_reimb.return_value = {"cases_created": 5, "items_scanned": 20}

        result = ra.run_full_scan()
        assert result["refund_spikes"]["anomalies_created"] == 3
        assert result["fee_spikes"]["anomalies_created"] == 2
        assert result["return_rate_spikes"]["anomalies_created"] == 1
        assert result["serial_returners"]["returners_flagged"] == 2
        assert result["reimbursement"]["cases_created"] == 5
        mock_spikes.assert_called_once()
        mock_fee.assert_called_once()
        mock_return.assert_called_once()
        mock_returners.assert_called_once()
        mock_reimb.assert_called_once()

    @patch("app.intelligence.refund_anomaly.scan_reimbursement_opportunities")
    @patch("app.intelligence.refund_anomaly.detect_serial_returners")
    @patch("app.intelligence.refund_anomaly.detect_return_rate_spikes")
    @patch("app.intelligence.refund_anomaly.detect_fee_spikes")
    @patch("app.intelligence.refund_anomaly.detect_refund_spikes")
    def test_passes_marketplace(self, mock_spikes, mock_fee, mock_return, mock_returners, mock_reimb):
        mock_spikes.return_value = {}
        mock_fee.return_value = {}
        mock_return.return_value = {}
        mock_returners.return_value = {}
        mock_reimb.return_value = {}

        ra.run_full_scan(marketplace_id="A1PA6795UKMFR9")
        mock_spikes.assert_called_once_with(marketplace_id="A1PA6795UKMFR9")
        mock_fee.assert_called_once_with(marketplace_id="A1PA6795UKMFR9")
        mock_return.assert_called_once_with(marketplace_id="A1PA6795UKMFR9")
        mock_returners.assert_called_once_with(marketplace_id="A1PA6795UKMFR9")
        mock_reimb.assert_called_once_with(marketplace_id="A1PA6795UKMFR9")

    @patch("app.intelligence.refund_anomaly.scan_reimbursement_opportunities")
    @patch("app.intelligence.refund_anomaly.detect_serial_returners")
    @patch("app.intelligence.refund_anomaly.detect_return_rate_spikes")
    @patch("app.intelligence.refund_anomaly.detect_fee_spikes")
    @patch("app.intelligence.refund_anomaly.detect_refund_spikes")
    def test_handles_individual_failures(self, mock_spikes, mock_fee, mock_return, mock_returners, mock_reimb):
        mock_spikes.side_effect = RuntimeError("db down")
        mock_fee.return_value = {"anomalies_created": 1}
        mock_return.return_value = {"anomalies_created": 0}
        mock_returners.return_value = {"returners_flagged": 1}
        mock_reimb.return_value = {"cases_created": 2}

        result = ra.run_full_scan()
        assert "error" in result["refund_spikes"]
        assert result["fee_spikes"]["anomalies_created"] == 1
        assert result["serial_returners"]["returners_flagged"] == 1
        assert result["reimbursement"]["cases_created"] == 2

    @patch("app.intelligence.refund_anomaly.scan_reimbursement_opportunities")
    @patch("app.intelligence.refund_anomaly.detect_serial_returners")
    @patch("app.intelligence.refund_anomaly.detect_return_rate_spikes")
    @patch("app.intelligence.refund_anomaly.detect_fee_spikes")
    @patch("app.intelligence.refund_anomaly.detect_refund_spikes")
    def test_all_fail_gracefully(self, mock_spikes, mock_fee, mock_return, mock_returners, mock_reimb):
        mock_spikes.side_effect = RuntimeError("err1")
        mock_fee.side_effect = RuntimeError("err2")
        mock_return.side_effect = RuntimeError("err3")
        mock_returners.side_effect = RuntimeError("err4")
        mock_reimb.side_effect = RuntimeError("err5")

        result = ra.run_full_scan()
        assert "error" in result["refund_spikes"]
        assert "error" in result["fee_spikes"]
        assert "error" in result["return_rate_spikes"]
        assert "error" in result["serial_returners"]
        assert "error" in result["reimbursement"]


# ====================================================================
# 15. Schema DDL
# ====================================================================

class TestEnsureSchema:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_executes_all_ddl_statements(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        ra.ensure_anomaly_schema()
        assert len(cur.executed) == 3  # 3 tables


# ====================================================================
# 16. API Endpoints
# ====================================================================

def _make_app():
    """Create a FastAPI test app with the refund anomaly router."""
    from app.api.v1.refund_anomaly import router
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


class TestAPIDashboard:
    def test_get_dashboard(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomaly_dashboard") as mock:
            mock.return_value = {
                "anomalies": {"total": 5, "open": 3, "critical_open": 1, "high_open": 2, "total_estimated_loss_pln": 500.0, "open_estimated_loss_pln": 300.0},
                "serial_returners": {"total_active": 4, "critical": 1, "high": 2, "total_refund_exposure_pln": 1000.0},
                "reimbursements": {"total_cases": 10, "pending": 3, "filed": 2, "paid": 1, "total_estimated_value_pln": 5000.0, "total_reimbursed_pln": 800.0},
            }
            resp = client.get("/api/v1/refund-anomaly/dashboard")
            assert resp.status_code == 200
            data = resp.json()
            assert data["anomalies"]["total"] == 5

    def test_dashboard_error(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomaly_dashboard") as mock:
            mock.side_effect = Exception("db error")
            resp = client.get("/api/v1/refund-anomaly/dashboard")
            assert resp.status_code == 500


class TestAPIScan:
    def test_trigger_scan_no_body(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.run_full_scan") as mock:
            mock.return_value = {"refund_spikes": {}, "serial_returners": {}, "reimbursement": {}}
            resp = client.post("/api/v1/refund-anomaly/scan")
            assert resp.status_code == 200
            mock.assert_called_once_with(marketplace_id=None)

    def test_trigger_scan_with_marketplace(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.run_full_scan") as mock:
            mock.return_value = {}
            resp = client.post("/api/v1/refund-anomaly/scan", json={"marketplace_id": "A1PA6795UKMFR9"})
            assert resp.status_code == 200
            mock.assert_called_once_with(marketplace_id="A1PA6795UKMFR9")


class TestAPIAnomalies:
    def test_list_anomalies(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomalies") as mock:
            mock.return_value = {"items": [], "total": 0, "limit": 50, "offset": 0}
            resp = client.get("/api/v1/refund-anomaly/anomalies")
            assert resp.status_code == 200
            assert resp.json()["total"] == 0

    def test_list_anomalies_with_filters(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomalies") as mock:
            mock.return_value = {"items": [], "total": 0, "limit": 50, "offset": 0}
            resp = client.get("/api/v1/refund-anomaly/anomalies?severity=critical&status=open")
            assert resp.status_code == 200
            mock.assert_called_once()
            kw = mock.call_args
            assert kw.kwargs.get("severity") == "critical" or kw[1].get("severity") == "critical"

    def test_update_anomaly_status(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.update_anomaly_status") as mock:
            mock.return_value = {"id": 1, "status": "investigating", "updated": True}
            resp = client.put("/api/v1/refund-anomaly/anomalies/1/status", json={"status": "investigating"})
            assert resp.status_code == 200
            assert resp.json()["status"] == "investigating"

    def test_update_anomaly_invalid_status(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.update_anomaly_status") as mock:
            mock.side_effect = ValueError("Invalid status 'bad'")
            resp = client.put("/api/v1/refund-anomaly/anomalies/1/status", json={"status": "bad"})
            assert resp.status_code == 422


class TestAPISerialReturners:
    def test_list_returners(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_serial_returners") as mock:
            mock.return_value = {"items": [], "total": 0, "limit": 50, "offset": 0}
            resp = client.get("/api/v1/refund-anomaly/serial-returners")
            assert resp.status_code == 200

    def test_update_returner_status(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.update_returner_status") as mock:
            mock.return_value = {"id": 1, "status": "monitoring", "updated": True}
            resp = client.put("/api/v1/refund-anomaly/serial-returners/1/status",
                             json={"status": "monitoring", "notes": "test"})
            assert resp.status_code == 200

    def test_update_returner_invalid_status(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.update_returner_status") as mock:
            mock.side_effect = ValueError("Invalid status")
            resp = client.put("/api/v1/refund-anomaly/serial-returners/1/status",
                             json={"status": "nonexistent"})
            assert resp.status_code == 422


class TestAPIReimbursementCases:
    def test_list_cases(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_reimbursement_cases") as mock:
            mock.return_value = {"items": [], "total": 0, "limit": 50, "offset": 0}
            resp = client.get("/api/v1/refund-anomaly/reimbursement-cases")
            assert resp.status_code == 200

    def test_list_cases_with_type_filter(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_reimbursement_cases") as mock:
            mock.return_value = {"items": [], "total": 0, "limit": 50, "offset": 0}
            resp = client.get("/api/v1/refund-anomaly/reimbursement-cases?case_type=lost_inventory")
            assert resp.status_code == 200

    def test_update_case_status(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.update_case_status") as mock:
            mock.return_value = {"id": 1, "status": "filed", "updated": True}
            resp = client.put("/api/v1/refund-anomaly/reimbursement-cases/1/status",
                             json={"status": "filed"})
            assert resp.status_code == 200

    def test_update_case_with_details(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.update_case_status") as mock:
            mock.return_value = {"id": 1, "status": "paid", "updated": True}
            resp = client.put("/api/v1/refund-anomaly/reimbursement-cases/1/status",
                             json={"status": "paid", "amazon_case_id": "CASE-123",
                                    "reimbursed_amount_pln": 80.0})
            assert resp.status_code == 200

    def test_update_case_invalid(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.update_case_status") as mock:
            mock.side_effect = ValueError("Invalid status")
            resp = client.put("/api/v1/refund-anomaly/reimbursement-cases/1/status",
                             json={"status": "garbage"})
            assert resp.status_code == 422


# ====================================================================
# 17. Router registration
# ====================================================================

class TestRouterRegistered:
    def test_refund_anomaly_router_in_api_router(self):
        from app.api.v1.router import api_router
        paths = [r.path for r in api_router.routes]
        # Check that at least one refund-anomaly path exists
        assert any("/refund-anomaly" in p for p in paths), f"No refund-anomaly route found in: {paths}"


# ====================================================================
# 18. Scheduler job registration
# ====================================================================

class TestSchedulerRegistration:
    def test_refund_anomaly_scan_job_added(self):
        from app.platform.scheduler.system import register
        scheduler = MagicMock()
        register(scheduler)
        job_ids = [c.kwargs.get("id") or c[1].get("id", "")
                   for c in scheduler.add_job.call_args_list]
        assert "refund-anomaly-scan-nightly" in job_ids


# ====================================================================
# 19. Edge cases
# ====================================================================

class TestEdgeCases:
    def test_anomaly_row_mapper_zero_spike(self):
        row = _make_anomaly_row(spike_ratio=0, refund_amount_pln=0, estimated_loss_pln=0)
        d = ra._anomaly_row_to_dict(row)
        assert d["spike_ratio"] == 0
        assert d["refund_amount_pln"] == 0

    def test_returner_row_mapper_none_top_skus(self):
        row = _make_returner_row(top_skus=None)
        d = ra._returner_row_to_dict(row)
        assert d["top_skus"] is None

    def test_case_row_mapper_no_order_id(self):
        row = _make_case_row(amazon_order_id=None, fnsku=None)
        d = ra._case_row_to_dict(row)
        assert d["amazon_order_id"] is None
        assert d["fnsku"] is None

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_anomalies_no_filters(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [(0,)]

        result = ra.get_anomalies()
        assert result["total"] == 0
        sql = cur.executed[0][0]
        assert "1=1" in sql

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_returners_no_filters(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [(0,)]

        result = ra.get_serial_returners()
        assert result["total"] == 0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_cases_no_filters(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [(0,)]

        result = ra.get_reimbursement_cases()
        assert result["total"] == 0

    def test_risk_score_computation_edge_zero(self):
        score = ra._compute_risk_score(return_count=0, return_rate=0.0, total_refund=0.0)
        assert score == 0

    def test_classify_severity_exactly_at_boundaries(self):
        assert ra._classify_severity(1.5) == "medium"
        assert ra._classify_severity(2.0) == "high"
        assert ra._classify_severity(3.0) == "critical"
        assert ra._classify_severity(1.49) == "low"


# ====================================================================
# Sprint 22 — Fee spike detection
# ====================================================================

class TestFeeSpikeConstants:
    def test_fee_spike_thresholds_exist(self):
        assert ra.FEE_SPIKE_RATIO_CRITICAL == 3.0
        assert ra.FEE_SPIKE_RATIO_HIGH == 2.0
        assert ra.FEE_SPIKE_RATIO_MEDIUM == 1.5

    def test_fee_charge_types(self):
        assert len(ra.FEE_CHARGE_TYPES) == 4
        assert "FBAPerUnitFulfillmentFee" in ra.FEE_CHARGE_TYPES

    def test_classify_fee_severity(self):
        assert ra._classify_fee_severity(3.5) == "critical"
        assert ra._classify_fee_severity(2.5) == "high"
        assert ra._classify_fee_severity(1.7) == "medium"
        assert ra._classify_fee_severity(1.2) == "low"


class TestDetectFeeSpikes:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_detects_fee_spike(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        # Recent: avg_fee=5.0, fee_count=10, total_fee=50.0
        recent_rows = [("SKU-FEE", "B000FEE", "A1PA6795UKMFR9", 5.0, 10, 50.0)]
        # Baseline: avg_fee=2.0, fee_count=10
        baseline_rows = [("SKU-FEE", "A1PA6795UKMFR9", 2.0, 10)]
        cur.fetchall = MagicMock(side_effect=[recent_rows, baseline_rows])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_fee_spikes(lookback_days=28)
        assert result["anomalies_created"] == 1
        assert result["skus_analyzed"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_no_fee_spike_below_threshold(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        # Ratio=1.0
        recent = [("SKU-FEE", "B000FEE", "A1PA6795UKMFR9", 2.0, 10, 20.0)]
        baseline = [("SKU-FEE", "A1PA6795UKMFR9", 2.0, 10)]
        cur.fetchall = MagicMock(side_effect=[recent, baseline])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_fee_spikes(lookback_days=28)
        assert result["anomalies_created"] == 0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_fee_spike_no_baseline(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        recent = [("SKU-NEW", "B000NEW", "A1PA6795UKMFR9", 5.0, 10, 50.0)]
        cur.fetchall = MagicMock(side_effect=[recent, []])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_fee_spikes(lookback_days=28)
        assert result["anomalies_created"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_fee_spike_rollback_on_error(self, mock_conn_fn):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.execute = MagicMock(side_effect=[None, Exception("db error")])
        mock_conn_fn.return_value = conn

        with pytest.raises(Exception, match="db error"):
            ra.detect_fee_spikes()
        conn.rollback.assert_called_once()

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_fee_spike_marketplace_filter(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        ra.detect_fee_spikes(marketplace_id="A1PA6795UKMFR9")
        sql = cur.executed[0][0]
        assert "marketplace_id" in sql


# ====================================================================
# Sprint 22 — Return rate spike detection
# ====================================================================

class TestReturnRateSpikeConstants:
    def test_return_rate_spike_thresholds(self):
        assert ra.RETURN_RATE_SPIKE_CRITICAL == 3.0
        assert ra.RETURN_RATE_SPIKE_HIGH == 2.0
        assert ra.RETURN_RATE_SPIKE_MEDIUM == 1.5
        assert ra.MIN_UNITS_FOR_RETURN_SPIKE == 10

    def test_classify_return_severity(self):
        assert ra._classify_return_severity(3.5) == "critical"
        assert ra._classify_return_severity(2.5) == "high"
        assert ra._classify_return_severity(1.7) == "medium"
        assert ra._classify_return_severity(1.2) == "low"


class TestDetectReturnRateSpikes:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_detects_return_rate_spike(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        # Recent: return_count=10, units_returned=10, refund=200, order_count=20 → rate=0.5
        recent = [("SKU-RET", "B000RET", "A1PA6795UKMFR9", 10, 10, 200.0, 20)]
        # Baseline: return_count=2, order_count=20 → rate=0.1
        baseline = [("SKU-RET", "A1PA6795UKMFR9", 2, 20)]
        cur.fetchall = MagicMock(side_effect=[recent, baseline])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_return_rate_spikes(lookback_days=28)
        assert result["anomalies_created"] == 1
        assert result["skus_analyzed"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_no_spike_below_threshold(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        # Same rate → ratio=1.0
        recent = [("SKU-RET", "B000RET", "A1PA6795UKMFR9", 5, 5, 100.0, 20)]
        baseline = [("SKU-RET", "A1PA6795UKMFR9", 5, 20)]
        cur.fetchall = MagicMock(side_effect=[recent, baseline])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_return_rate_spikes(lookback_days=28)
        assert result["anomalies_created"] == 0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_skips_low_order_count(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        # order_count=5 < MIN_UNITS_FOR_RETURN_SPIKE (10) → skip
        recent = [("SKU-RET", "B000RET", "A1PA6795UKMFR9", 5, 5, 100.0, 5)]
        cur.fetchall = MagicMock(side_effect=[recent, []])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_return_rate_spikes(lookback_days=28)
        assert result["anomalies_created"] == 0

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_return_spike_no_baseline(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        recent = [("SKU-NEW", "B000NEW", "A1PA6795UKMFR9", 5, 5, 100.0, 20)]
        cur.fetchall = MagicMock(side_effect=[recent, []])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.detect_return_rate_spikes(lookback_days=28)
        assert result["anomalies_created"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_return_spike_rollback_on_error(self, mock_conn_fn):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.execute = MagicMock(side_effect=[None, Exception("db error")])
        mock_conn_fn.return_value = conn

        with pytest.raises(Exception, match="db error"):
            ra.detect_return_rate_spikes()
        conn.rollback.assert_called_once()


# ====================================================================
# Sprint 22 — Extended reimbursement scanning
# ====================================================================

class TestExtendedReimbursementScanning:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_scans_damaged_inbound(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        # 1st fetchall → lost/damaged eligible (empty)
        # 2nd fetchall → damaged inbound items
        # 3rd fetchall → fee overcharge (empty)
        damaged_inbound = [("SKU-DI", "B000DI", "A1PA6795UKMFR9", "SHIP-001", 3, 25.0)]
        cur.fetchall = MagicMock(side_effect=[[], damaged_inbound, []])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.scan_reimbursement_opportunities()
        assert result["cases_created"] == 1
        assert result["items_scanned"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_scans_fee_overcharge(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        # 1st empty, 2nd empty, 3rd fee overcharge rows
        overcharge = [("SKU-OC", "B000OC", "A1PA6795UKMFR9", 2.5, 8.0, 20, 1.5)]
        cur.fetchall = MagicMock(side_effect=[[], [], overcharge])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.scan_reimbursement_opportunities()
        assert result["cases_created"] == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_multiple_case_types_combined(self, mock_conn_fn):
        cur = MagicMock()
        cur.execute = MagicMock(side_effect=lambda sql, params=None: None)
        lost = [("SKU-A", "B000A", "MKT1", "ORD-1", "lost_in_transit", 30.0, 60.0, 1)]
        damaged = [("SKU-B", "B000B", "MKT1", "SHIP-2", 2, 20.0)]
        overcharge = [("SKU-C", "B000C", "MKT1", 3.0, 9.0, 15, 2.0)]
        cur.fetchall = MagicMock(side_effect=[lost, damaged, overcharge])
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn

        result = ra.scan_reimbursement_opportunities()
        assert result["cases_created"] == 3
        assert result["items_scanned"] == 3


# ====================================================================
# Sprint 22 — Detail lookups
# ====================================================================

class TestDetailLookups:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_anomaly_by_id_found(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [_make_anomaly_row(id=42)]

        result = ra.get_anomaly_by_id(42)
        assert result is not None
        assert result["id"] == 42

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_anomaly_by_id_not_found(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.get_anomaly_by_id(999)
        assert result is None

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_returner_by_id_found(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [_make_returner_row(id=7)]

        result = ra.get_returner_by_id(7)
        assert result is not None
        assert result["id"] == 7

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_returner_by_id_not_found(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.get_returner_by_id(999)
        assert result is None

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_case_by_id_found(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [_make_case_row(id=15)]

        result = ra.get_case_by_id(15)
        assert result is not None
        assert result["id"] == 15

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_get_case_by_id_not_found(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.get_case_by_id(999)
        assert result is None


# ====================================================================
# Sprint 22 — Trend queries
# ====================================================================

class TestAnomalyTrends:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_returns_trend_data(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [
            (date(2025, 3, 3), "refund_spike", 5, 2, 3, 500.0),
            (date(2025, 3, 10), "fee_spike", 3, 1, 1, 200.0),
        ]

        result = ra.get_anomaly_trends(days=90)
        assert len(result) == 2
        assert result[0]["anomaly_type"] == "refund_spike"
        assert result[0]["count"] == 5
        assert result[1]["anomaly_type"] == "fee_spike"

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_empty_trends(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.get_anomaly_trends(days=90)
        assert result == []

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_trends_with_filters(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        ra.get_anomaly_trends(days=30, anomaly_type="fee_spike", marketplace_id="A1PA6795UKMFR9")
        sql = cur.executed[0][0]
        assert "anomaly_type" in sql
        assert "marketplace_id" in sql


# ====================================================================
# Sprint 22 — CSV export functions
# ====================================================================

class TestExportCsv:
    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_export_anomalies(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [_make_anomaly_row(id=1), _make_anomaly_row(id=2)]

        result = ra.export_anomalies_csv()
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_export_anomalies_empty(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        result = ra.export_anomalies_csv()
        assert result == []

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_export_anomalies_with_filters(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = []

        ra.export_anomalies_csv(anomaly_type="fee_spike", severity="high")
        sql = cur.executed[0][0]
        assert "anomaly_type" in sql
        assert "severity" in sql

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_export_returners(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [_make_returner_row(id=1)]

        result = ra.export_returners_csv()
        assert len(result) == 1

    @patch("app.intelligence.refund_anomaly.connect_acc")
    def test_export_cases(self, mock_conn_fn):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        mock_conn_fn.return_value = conn
        cur.multi_rows = [_make_case_row(id=1)]

        result = ra.export_cases_csv()
        assert len(result) == 1


# ====================================================================
# Sprint 22 — API detail endpoints
# ====================================================================

class TestAPIDetailEndpoints:
    def test_get_anomaly_detail_found(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomaly_by_id") as mock:
            mock.return_value = {"id": 42, "sku": "SKU-001", "anomaly_type": "refund_spike"}
            resp = client.get("/api/v1/refund-anomaly/anomalies/42")
            assert resp.status_code == 200
            assert resp.json()["id"] == 42

    def test_get_anomaly_detail_not_found(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomaly_by_id") as mock:
            mock.return_value = None
            resp = client.get("/api/v1/refund-anomaly/anomalies/999")
            assert resp.status_code == 404

    def test_get_returner_detail_found(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_returner_by_id") as mock:
            mock.return_value = {"id": 7, "buyer_identifier": "Berlin|10115"}
            resp = client.get("/api/v1/refund-anomaly/serial-returners/7")
            assert resp.status_code == 200
            assert resp.json()["id"] == 7

    def test_get_returner_detail_not_found(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_returner_by_id") as mock:
            mock.return_value = None
            resp = client.get("/api/v1/refund-anomaly/serial-returners/999")
            assert resp.status_code == 404

    def test_get_case_detail_found(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_case_by_id") as mock:
            mock.return_value = {"id": 15, "case_type": "lost_inventory"}
            resp = client.get("/api/v1/refund-anomaly/reimbursement-cases/15")
            assert resp.status_code == 200

    def test_get_case_detail_not_found(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_case_by_id") as mock:
            mock.return_value = None
            resp = client.get("/api/v1/refund-anomaly/reimbursement-cases/999")
            assert resp.status_code == 404


# ====================================================================
# Sprint 22 — API trends endpoint
# ====================================================================

class TestAPITrends:
    def test_get_trends(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomaly_trends") as mock:
            mock.return_value = [
                {"week_start": "2025-03-03", "anomaly_type": "refund_spike", "count": 5,
                 "critical_count": 2, "high_count": 3, "total_loss_pln": 500.0},
            ]
            resp = client.get("/api/v1/refund-anomaly/trends")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 1
            assert data[0]["count"] == 5

    def test_get_trends_with_params(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomaly_trends") as mock:
            mock.return_value = []
            resp = client.get("/api/v1/refund-anomaly/trends?days=30&anomaly_type=fee_spike")
            assert resp.status_code == 200

    def test_get_trends_error(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.get_anomaly_trends") as mock:
            mock.side_effect = Exception("db error")
            resp = client.get("/api/v1/refund-anomaly/trends")
            assert resp.status_code == 500


# ====================================================================
# Sprint 22 — API CSV export endpoints
# ====================================================================

class TestAPIExportEndpoints:
    def test_export_anomalies_csv(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.export_anomalies_csv") as mock:
            mock.return_value = [{"id": 1, "sku": "SKU-001"}, {"id": 2, "sku": "SKU-002"}]
            resp = client.get("/api/v1/refund-anomaly/anomalies/export/csv")
            assert resp.status_code == 200
            assert "text/csv" in resp.headers.get("content-type", "")

    def test_export_anomalies_csv_empty(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.export_anomalies_csv") as mock:
            mock.return_value = []
            resp = client.get("/api/v1/refund-anomaly/anomalies/export/csv")
            assert resp.status_code == 200

    def test_export_returners_csv(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.export_returners_csv") as mock:
            mock.return_value = [{"id": 1, "buyer_identifier": "Berlin|10115"}]
            resp = client.get("/api/v1/refund-anomaly/serial-returners/export/csv")
            assert resp.status_code == 200
            assert "text/csv" in resp.headers.get("content-type", "")

    def test_export_cases_csv(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.export_cases_csv") as mock:
            mock.return_value = [{"id": 1, "case_type": "lost_inventory"}]
            resp = client.get("/api/v1/refund-anomaly/reimbursement-cases/export/csv")
            assert resp.status_code == 200
            assert "text/csv" in resp.headers.get("content-type", "")

    def test_export_anomalies_csv_with_filters(self):
        client = _make_app()
        with patch("app.intelligence.refund_anomaly.export_anomalies_csv") as mock:
            mock.return_value = []
            resp = client.get("/api/v1/refund-anomaly/anomalies/export/csv?severity=critical")
            assert resp.status_code == 200


# ====================================================================
# Sprint 22 — Fee spike severity classification boundary tests
# ====================================================================

class TestFeeSeverityBoundaries:
    def test_fee_severity_at_critical(self):
        assert ra._classify_fee_severity(3.0) == "critical"

    def test_fee_severity_at_high(self):
        assert ra._classify_fee_severity(2.0) == "high"

    def test_fee_severity_at_medium(self):
        assert ra._classify_fee_severity(1.5) == "medium"

    def test_fee_severity_below_medium(self):
        assert ra._classify_fee_severity(1.49) == "low"


class TestReturnSeverityBoundaries:
    def test_return_severity_at_critical(self):
        assert ra._classify_return_severity(3.0) == "critical"

    def test_return_severity_at_high(self):
        assert ra._classify_return_severity(2.0) == "high"

    def test_return_severity_at_medium(self):
        assert ra._classify_return_severity(1.5) == "medium"

    def test_return_severity_below_medium(self):
        assert ra._classify_return_severity(1.49) == "low"
