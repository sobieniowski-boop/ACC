"""Unit tests for growth_opportunity_access layer.

Tests the pure functions and DB interaction helpers:
  - priority_from_label: P1/P2/P3 → numeric score
  - _f: safe float coercion
  - insert_opportunity: SQL params passed correctly
  - deactivate_by_types: placeholders and rowcount
  - query_active: filtering, ordering, marketplace lookup

Sprint 8 – S8.5
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from app.services.growth_opportunity_access import (
    _f,
    priority_from_label,
    insert_opportunity,
    deactivate_by_types,
    query_active,
)


# ═══════════════════════════════════════════════════════════════════════════
#  Pure helpers
# ═══════════════════════════════════════════════════════════════════════════

class TestPriorityFromLabel:
    def test_p1(self):
        assert priority_from_label("P1") == 90.0

    def test_p2(self):
        assert priority_from_label("P2") == 70.0

    def test_p3(self):
        assert priority_from_label("P3") == 50.0

    def test_unknown_defaults_50(self):
        assert priority_from_label("P4") == 50.0
        assert priority_from_label("") == 50.0


class TestSafeFloat:
    def test_none_returns_zero(self):
        assert _f(None) == 0.0

    def test_decimal(self):
        assert _f(Decimal("3.14")) == pytest.approx(3.14)

    def test_int(self):
        assert _f(42) == 42.0

    def test_string_float(self):
        assert _f("9.99") == pytest.approx(9.99)


# ═══════════════════════════════════════════════════════════════════════════
#  insert_opportunity
# ═══════════════════════════════════════════════════════════════════════════

class TestInsertOpportunity:
    def test_executes_insert(self):
        cur = MagicMock()
        insert_opportunity(
            cur,
            opportunity_type="EXEC_RISK_LOW_MARGIN",
            marketplace_id="A1PA",
            sku="SKU-1",
            title="Low margin risk",
            priority_score=90.0,
            confidence_score=75.0,
        )
        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "INSERT INTO growth_opportunity" in sql
        assert params[0] == "EXEC_RISK_LOW_MARGIN"
        assert params[1] == "A1PA"
        assert params[2] == "SKU-1"

    def test_signals_serialised_as_json(self):
        cur = MagicMock()
        insert_opportunity(
            cur,
            opportunity_type="T",
            title="T",
            signals={"metric": 42},
        )
        params = cur.execute.call_args[0][1]
        # signals is the last param (index 19)
        assert '"metric": 42' in params[19]

    def test_none_blockers_stored_as_none(self):
        cur = MagicMock()
        insert_opportunity(cur, opportunity_type="T", title="T")
        params = cur.execute.call_args[0][1]
        # blockers is param index 18
        assert params[18] is None


# ═══════════════════════════════════════════════════════════════════════════
#  deactivate_by_types
# ═══════════════════════════════════════════════════════════════════════════

class TestDeactivateByTypes:
    def test_empty_list_returns_zero(self):
        cur = MagicMock()
        result = deactivate_by_types(cur, [])
        assert result == 0
        cur.execute.assert_not_called()

    def test_builds_correct_placeholders(self):
        cur = MagicMock()
        cur.rowcount = 3
        result = deactivate_by_types(cur, ["TYPE_A", "TYPE_B"])
        assert result == 3
        sql = cur.execute.call_args[0][0]
        assert "IN (?,?)" in sql

    def test_custom_status(self):
        cur = MagicMock()
        cur.rowcount = 1
        deactivate_by_types(cur, ["T"], status_from="in_review", status_to="archived")
        params = cur.execute.call_args[0][1]
        assert params[0] == "archived"
        assert params[1] == "in_review"
        assert params[2] == "T"


# ═══════════════════════════════════════════════════════════════════════════
#  query_active
# ═══════════════════════════════════════════════════════════════════════════

class TestQueryActive:
    def _setup_cursor(self, rows):
        cur = MagicMock()
        cur.fetchall.return_value = rows
        return cur

    @patch("app.core.config.MARKETPLACE_REGISTRY",
           {"A1PA": {"code": "DE"}})
    def test_returns_mapped_rows(self):
        now = datetime(2025, 1, 15, 12, 0, 0)
        cur = self._setup_cursor([
            (1, "TYPE_A", "A1PA", "SKU-1", "B0TEST",
             "Title", "Desc", 90.0, 75.0, 1000.0, 500.0, now),
        ])
        rows = query_active(cur, limit=10)
        assert len(rows) == 1
        r = rows[0]
        assert r["id"] == 1
        assert r["marketplace_code"] == "DE"
        assert r["priority_score"] == 90.0
        assert r["impact_estimate"] == 1000.0
        assert r["is_active"] is True

    @patch("app.core.config.MARKETPLACE_REGISTRY", {})
    def test_filters_by_type(self):
        cur = self._setup_cursor([])
        query_active(cur, opportunity_types=["T1", "T2"])
        sql = cur.execute.call_args[0][0]
        assert "IN (?,?)" in sql

    @patch("app.core.config.MARKETPLACE_REGISTRY", {})
    def test_filters_by_marketplace(self):
        cur = self._setup_cursor([])
        query_active(cur, marketplace_id="A1PA")
        sql = cur.execute.call_args[0][0]
        assert "marketplace_id = ?" in sql

    @patch("app.core.config.MARKETPLACE_REGISTRY",
           {"M1": {"code": "M1"}})
    def test_impact_falls_back_to_profit(self):
        """If revenue_uplift is None, impact_estimate uses profit_uplift."""
        now = datetime(2025, 1, 1)
        cur = self._setup_cursor([
            (1, "T", "M1", "S", "A", "Ti", "De", 50.0, 50.0, None, 200.0, now),
        ])
        rows = query_active(cur)
        assert rows[0]["impact_estimate"] == 200.0
