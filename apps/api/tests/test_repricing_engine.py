"""Unit tests for Repricing Decision Engine.

Tests the strategy algorithms, guardrail enforcement,
proposal pipeline, and CRUD operations.

Sprint 15 – S15.8
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from app.intelligence.repricing_engine import (
    VALID_STRATEGY_TYPES,
    VALID_STATUSES,
    DEFAULT_UNDERCUT_PCT,
    DEFAULT_TARGET_MARGIN_PCT,
    DEFAULT_MAX_DAILY_CHANGE_PCT,
    DEFAULT_AMAZON_FEE_PCT,
    AUTO_APPROVE_MAX_CHANGE_PCT,
    compute_buybox_match_price,
    compute_competitive_undercut_price,
    compute_margin_target_price,
    compute_velocity_based_price,
    enforce_margin_guardrail,
    enforce_daily_change_guardrail,
    estimate_margin,
    _apply_price_bounds,
    _fv,
    _build_reason,
    _compute_strategy_target,
    upsert_strategy,
    list_strategies,
    get_strategy,
    delete_strategy,
    compute_repricing_proposals,
    get_execution_proposals,
    approve_execution,
    reject_execution,
    get_repricing_dashboard,
    get_execution_history,
    ensure_repricing_schema,
    auto_approve_proposals,
    execute_approved_prices,
    bulk_approve_executions,
    bulk_reject_executions,
    compute_daily_analytics,
    get_analytics_trend,
    get_execution_summary_by_strategy,
)


# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════


class TestConstants:
    def test_valid_strategy_types(self):
        assert VALID_STRATEGY_TYPES == {"buybox_match", "competitive_undercut", "margin_target", "velocity_based"}

    def test_valid_statuses(self):
        assert "proposed" in VALID_STATUSES
        assert "approved" in VALID_STATUSES
        assert "executed" in VALID_STATUSES
        assert "rejected" in VALID_STATUSES
        assert "expired" in VALID_STATUSES
        assert "failed" in VALID_STATUSES

    def test_defaults(self):
        assert DEFAULT_UNDERCUT_PCT == 1.0
        assert DEFAULT_TARGET_MARGIN_PCT == 15.0
        assert DEFAULT_MAX_DAILY_CHANGE_PCT == 10.0
        assert DEFAULT_AMAZON_FEE_PCT == 15.0


# ═══════════════════════════════════════════════════════════════════════════
#  Pure computation — Buy Box Match
# ═══════════════════════════════════════════════════════════════════════════


class TestBuyboxMatchPrice:
    def test_match_buybox(self):
        result = compute_buybox_match_price(25.00, 22.99)
        assert result == 22.99

    def test_no_change_when_equal(self):
        result = compute_buybox_match_price(22.99, 22.99)
        assert result is None

    def test_none_when_buybox_missing(self):
        assert compute_buybox_match_price(25.00, None) is None

    def test_none_when_buybox_zero(self):
        assert compute_buybox_match_price(25.00, 0) is None

    def test_respects_min_price(self):
        result = compute_buybox_match_price(30.00, 15.00, min_price=20.00)
        assert result == 20.00

    def test_respects_max_price(self):
        result = compute_buybox_match_price(20.00, 35.00, max_price=30.00)
        assert result == 30.00

    def test_no_change_when_clamped_to_current(self):
        # BB is lower, but min_price clamps to current → no change
        result = compute_buybox_match_price(25.00, 20.00, min_price=25.00)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
#  Pure computation — Competitive Undercut
# ═══════════════════════════════════════════════════════════════════════════


class TestCompetitiveUndercutPrice:
    def test_undercut_by_1pct(self):
        result = compute_competitive_undercut_price(30.00, 25.00, undercut_pct=1.0)
        assert result == 24.75

    def test_undercut_by_custom_pct(self):
        result = compute_competitive_undercut_price(30.00, 20.00, undercut_pct=5.0)
        assert result == 19.00

    def test_no_change_when_already_undercut(self):
        # Our price is already at the undercut level
        result = compute_competitive_undercut_price(24.75, 25.00, undercut_pct=1.0)
        assert result is None

    def test_none_when_competitor_missing(self):
        assert compute_competitive_undercut_price(30.00, None) is None

    def test_none_when_competitor_zero(self):
        assert compute_competitive_undercut_price(30.00, 0) is None

    def test_respects_min_price(self):
        result = compute_competitive_undercut_price(30.00, 15.00, undercut_pct=5.0, min_price=20.00)
        assert result == 20.00

    def test_floor_at_001(self):
        result = compute_competitive_undercut_price(5.00, 0.01, undercut_pct=100)
        # 0.01 * (1 - 1.0) = 0, floor to 0.01
        assert result == 0.01


# ═══════════════════════════════════════════════════════════════════════════
#  Pure computation — Margin Target
# ═══════════════════════════════════════════════════════════════════════════


class TestMarginTargetPrice:
    def test_basic_margin_target(self):
        # purchase=10, fee=15%, target=15% → price = 10 / (1 - 0.15 - 0.15) = 10/0.70 ≈ 14.29
        result = compute_margin_target_price(10.0, target_margin_pct=15.0, amazon_fee_pct=15.0)
        assert result == 14.29

    def test_with_fba_and_shipping(self):
        result = compute_margin_target_price(
            10.0, target_margin_pct=20.0, amazon_fee_pct=15.0,
            fba_fee=3.0, shipping_cost=2.0,
        )
        # fixed_costs = 10 + 2 + 3 + 0 = 15
        # divisor = 1 - 0.15 - 0.20 = 0.65
        # target = 15 / 0.65 ≈ 23.08
        assert result == 23.08

    def test_impossible_margin_returns_none(self):
        # target_margin=90% + fee=15% = 105% → divisor ≤ 0
        assert compute_margin_target_price(10.0, target_margin_pct=90.0) is None

    def test_respects_price_bounds(self):
        result = compute_margin_target_price(
            10.0, target_margin_pct=15.0, min_price=20.0, max_price=50.0,
        )
        # Natural = 14.29, clamped to 20.0
        assert result == 20.0


# ═══════════════════════════════════════════════════════════════════════════
#  Pure computation — Velocity Based
# ═══════════════════════════════════════════════════════════════════════════


class TestVelocityBasedPrice:
    def test_accelerating_demand_raises_price(self):
        # 7d=3.0, 30d=1.0 → change = 200% >> 25% → up by 3%
        result = compute_velocity_based_price(100.0, 3.0, 1.0)
        assert result == 103.0

    def test_decelerating_demand_lowers_price(self):
        # 7d=0.5, 30d=1.0 → change = -50% << -25% → down by 5%
        result = compute_velocity_based_price(100.0, 0.5, 1.0)
        assert result == 95.0

    def test_stable_demand_no_change(self):
        # 7d=1.1, 30d=1.0 → change = 10% → within ±25% → no change
        assert compute_velocity_based_price(100.0, 1.1, 1.0) is None

    def test_zero_velocity_30d_returns_none(self):
        assert compute_velocity_based_price(100.0, 5.0, 0) is None

    def test_custom_pct_params(self):
        result = compute_velocity_based_price(
            100.0, 3.0, 1.0,
            price_up_pct=5.0, price_down_pct=8.0,
        )
        assert result == 105.0

    def test_respects_max_price(self):
        result = compute_velocity_based_price(100.0, 10.0, 1.0, max_price=101.0)
        assert result == 101.0


# ═══════════════════════════════════════════════════════════════════════════
#  Price bounds helper
# ═══════════════════════════════════════════════════════════════════════════


class TestApplyPriceBounds:
    def test_within_bounds(self):
        assert _apply_price_bounds(25.0, 20.0, 30.0) == 25.0

    def test_below_min(self):
        assert _apply_price_bounds(15.0, 20.0, 30.0) == 20.0

    def test_above_max(self):
        assert _apply_price_bounds(35.0, 20.0, 30.0) == 30.0

    def test_no_bounds(self):
        assert _apply_price_bounds(35.0, None, None) == 35.0


# ═══════════════════════════════════════════════════════════════════════════
#  Guardrail enforcement
# ═══════════════════════════════════════════════════════════════════════════


class TestMarginGuardrail:
    def test_passes_when_margin_sufficient(self):
        price, note = enforce_margin_guardrail(
            50.0, 20.0, min_margin_pct=10.0,
        )
        # margin = (50 - 20 - 50*0.15) / 50 = (50 - 20 - 7.5)/50 = 45%
        assert price == 50.0
        assert note is None

    def test_raises_floor_when_margin_too_low(self):
        price, note = enforce_margin_guardrail(
            20.0, 18.0, min_margin_pct=10.0,
            amazon_fee_pct=15.0,
        )
        # margin at 20: (20 - 18 - 3) / 20 = -0.05 → -5% < 10%
        # floor = 18 / (1 - 0.15 - 0.10) = 18/0.75 = 24.0
        assert price == 24.0
        assert note == "margin_floor_applied"

    def test_no_guardrail_when_min_margin_none(self):
        price, note = enforce_margin_guardrail(10.0, 20.0, min_margin_pct=None)
        assert price == 10.0
        assert note is None

    def test_impossible_margin(self):
        price, note = enforce_margin_guardrail(
            10.0, 5.0, min_margin_pct=90.0,
        )
        assert note == "margin_guardrail_impossible"


class TestDailyChangeGuardrail:
    def test_within_limit(self):
        price, note = enforce_daily_change_guardrail(100.0, 105.0, 10.0)
        assert price == 105.0
        assert note is None

    def test_capped_increase(self):
        price, note = enforce_daily_change_guardrail(100.0, 120.0, 10.0)
        assert price == 110.0
        assert "daily_change_capped" in note

    def test_capped_decrease(self):
        price, note = enforce_daily_change_guardrail(100.0, 80.0, 10.0)
        assert price == 90.0
        assert "daily_change_capped" in note

    def test_zero_current_price_skips(self):
        price, note = enforce_daily_change_guardrail(0, 50.0, 10.0)
        assert price == 50.0
        assert note is None


# ═══════════════════════════════════════════════════════════════════════════
#  Margin estimation
# ═══════════════════════════════════════════════════════════════════════════


class TestEstimateMargin:
    def test_positive_margin(self):
        margin = estimate_margin(50.0, 20.0, amazon_fee_pct=15.0)
        # fee=7.5, cost=20, total=27.5, margin=(50-27.5)/50*100=45%
        assert margin == 45.0

    def test_negative_margin(self):
        margin = estimate_margin(10.0, 15.0, amazon_fee_pct=15.0)
        assert margin < 0

    def test_zero_price(self):
        assert estimate_margin(0, 10.0) == 0.0


# ═══════════════════════════════════════════════════════════════════════════
#  Safe float coercion
# ═══════════════════════════════════════════════════════════════════════════


class TestFv:
    def test_float(self):
        assert _fv(3.14) == 3.14

    def test_string(self):
        assert _fv("3.14") == 3.14

    def test_none(self):
        assert _fv(None) is None

    def test_invalid(self):
        assert _fv("abc") is None


# ═══════════════════════════════════════════════════════════════════════════
#  Strategy dispatch
# ═══════════════════════════════════════════════════════════════════════════


class TestComputeStrategyTarget:
    def test_buybox_match(self):
        result = _compute_strategy_target(
            strategy_type="buybox_match",
            our_price=30.0, buybox_price=25.0, lowest_price=20.0,
            margin_info={}, velocity_info={}, params={},
            min_price=None, max_price=None,
        )
        assert result == 25.0

    def test_competitive_undercut(self):
        result = _compute_strategy_target(
            strategy_type="competitive_undercut",
            our_price=30.0, buybox_price=25.0, lowest_price=20.0,
            margin_info={}, velocity_info={}, params={"undercut_pct": 2.0},
            min_price=None, max_price=None,
        )
        assert result == 19.6

    def test_margin_target(self):
        result = _compute_strategy_target(
            strategy_type="margin_target",
            our_price=30.0, buybox_price=25.0, lowest_price=20.0,
            margin_info={"purchase_cost": 10.0}, velocity_info={},
            params={"target_margin_pct": 15.0},
            min_price=None, max_price=None,
        )
        assert result == 14.29

    def test_margin_target_no_cost_returns_none(self):
        result = _compute_strategy_target(
            strategy_type="margin_target",
            our_price=30.0, buybox_price=25.0, lowest_price=20.0,
            margin_info={}, velocity_info={}, params={},
            min_price=None, max_price=None,
        )
        assert result is None

    def test_velocity_based(self):
        result = _compute_strategy_target(
            strategy_type="velocity_based",
            our_price=100.0, buybox_price=90.0, lowest_price=85.0,
            margin_info={}, velocity_info={"velocity_7d": 5.0, "velocity_30d": 1.0},
            params={},
            min_price=None, max_price=None,
        )
        assert result == 103.0

    def test_unknown_strategy_returns_none(self):
        result = _compute_strategy_target(
            strategy_type="unknown",
            our_price=30.0, buybox_price=25.0, lowest_price=20.0,
            margin_info={}, velocity_info={}, params={},
            min_price=None, max_price=None,
        )
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
#  Reason building
# ═══════════════════════════════════════════════════════════════════════════


class TestBuildReason:
    def test_buybox_match_reason(self):
        code, text = _build_reason("buybox_match", 30.0, 25.0, {"buybox_price": 25.0}, {})
        assert code == "buybox_match"
        assert "Match Buy Box" in text
        assert "decrease" in text

    def test_competitive_undercut_reason(self):
        code, text = _build_reason("competitive_undercut", 30.0, 19.6, {"lowest_price_new": 20.0}, {"undercut_pct": 2.0})
        assert code == "competitive_undercut"
        assert "Undercut" in text

    def test_margin_target_reason(self):
        code, text = _build_reason("margin_target", 30.0, 35.0, {}, {"target_margin_pct": 20.0})
        assert code == "margin_target"
        assert "increase" in text

    def test_velocity_reason(self):
        code, text = _build_reason("velocity_based", 100.0, 95.0, {}, {})
        assert code == "velocity_adjustment"
        assert "decrease" in text


# ═══════════════════════════════════════════════════════════════════════════
#  DB mock helpers
# ═══════════════════════════════════════════════════════════════════════════


class _FakeConn:
    def __init__(self, rows=None):
        self.cursor_obj = _FakeCursor(rows)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class _FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.executed = []
        self._call_idx = 0
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


# ═══════════════════════════════════════════════════════════════════════════
#  Schema tests
# ═══════════════════════════════════════════════════════════════════════════


class TestEnsureSchema:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_runs_ddl(self, mock_conn):
        conn = _FakeConn()
        mock_conn.return_value = conn
        ensure_repricing_schema()
        assert conn.committed
        assert conn.closed
        # Sprint 15 (2 tables) + Sprint 16 (1 analytics table + 2 ALTER)
        assert len(conn.cursor_obj.executed) == 5


# ═══════════════════════════════════════════════════════════════════════════
#  Strategy CRUD
# ═══════════════════════════════════════════════════════════════════════════


class TestUpsertStrategy:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_upserts_strategy(self, mock_conn):
        conn = _FakeConn()
        mock_conn.return_value = conn
        result = upsert_strategy(
            "buybox_match",
            seller_sku="SKU1", marketplace_id="AEFP",
            min_price=10.0, max_price=50.0,
        )
        assert result["status"] == "upserted"
        assert result["strategy_type"] == "buybox_match"
        assert conn.committed
        assert conn.closed

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid strategy_type"):
            upsert_strategy("invalid_type")


class TestListStrategies:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_returns_paginated(self, mock_conn):
        # list_strategies does: fetchone for count, then fetchall for rows
        cur = _MultiQueryCursor([
            [(3,)],  # count query
            [],      # rows query (empty)
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        result = list_strategies("AEFP")
        assert "items" in result
        assert result["total"] == 3
        assert conn.close.called


class TestGetStrategy:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_found(self, mock_conn):
        conn = _FakeConn(rows=[
            (1, "SKU1", "AEFP", "buybox_match", True, None,
             20.0, 50.0, 5.0, 10.0, True, 100,
             "2025-01-01", "2025-01-01"),
        ])
        mock_conn.return_value = conn
        result = get_strategy(1)
        assert result is not None
        assert result["id"] == 1
        assert result["strategy_type"] == "buybox_match"

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_not_found(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        assert get_strategy(999) is None


class TestDeleteStrategy:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_deletes(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 1
        mock_conn.return_value = conn
        assert delete_strategy(1) is True
        assert conn.committed

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_not_found(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0
        mock_conn.return_value = conn
        assert delete_strategy(999) is False


# ═══════════════════════════════════════════════════════════════════════════
#  Execution management
# ═══════════════════════════════════════════════════════════════════════════


class TestApproveExecution:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_approve_ok(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 1
        mock_conn.return_value = conn
        assert approve_execution(1, "admin") is True
        assert conn.committed

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_approve_not_found(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0
        mock_conn.return_value = conn
        assert approve_execution(999) is False


class TestRejectExecution:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_reject_ok(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 1
        mock_conn.return_value = conn
        assert reject_execution(1) is True
        assert conn.committed

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_reject_not_found(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0
        mock_conn.return_value = conn
        assert reject_execution(999) is False


class TestGetExecutionProposals:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_returns_paginated(self, mock_conn):
        cur = _MultiQueryCursor([
            [(5,)],  # count query
            [],      # rows query (empty)
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        result = get_execution_proposals("AEFP", status="proposed")
        assert "items" in result
        assert result["total"] == 5
        assert conn.close.called


# ═══════════════════════════════════════════════════════════════════════════
#  Dashboard
# ═══════════════════════════════════════════════════════════════════════════


class TestRepricingDashboard:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_returns_summary(self, mock_conn):
        cur = _MultiQueryCursor([
            [(5, 3, 2)],  # strategy counts: total, active, types
            [(10, 4, 2, 1, 20, 1.5)],  # exec stats: proposed, approved, executed, rejected, total, avg_change
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        result = get_repricing_dashboard()
        assert result["strategies_total"] == 5
        assert result["proposed"] == 10
        assert conn.close.called


# ═══════════════════════════════════════════════════════════════════════════
#  Execution History
# ═══════════════════════════════════════════════════════════════════════════


class TestGetExecutionHistory:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_returns_list(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_execution_history("SKU1", "AEFP", days=30)
        assert isinstance(result, list)
        assert conn.closed


# ═══════════════════════════════════════════════════════════════════════════
#  Compute proposals — integration level
# ═══════════════════════════════════════════════════════════════════════════


class _MultiQueryCursor:
    """Cursor that returns different results for different queries."""

    def __init__(self, query_results):
        self._query_results = query_results
        self._idx = 0
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._idx += 1

    def fetchone(self):
        idx = self._idx - 1
        if idx < len(self._query_results):
            rows = self._query_results[idx]
            return rows[0] if rows else None
        return None

    def fetchall(self):
        idx = self._idx - 1
        if idx < len(self._query_results):
            return self._query_results[idx]
        return []


class TestComputeProposals:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_no_strategies_returns_zero(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = compute_repricing_proposals("AEFP")
        assert result == 0

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_generates_buybox_match_proposal(self, mock_conn):
        """Full pipeline: 1 strategy + 1 snapshot → 1 proposal."""
        # Build cursor with sequential query results
        query_results = [
            # Query 1: strategies
            [
                (1, "SKU1", "AEFP", "buybox_match", None, None, None, None, 10.0, True, 100),
            ],
            # Query 2: pricing snapshots
            [
                ("SKU1", "B01ASIN", "AEFP", 30.0, 25.0, False, 22.0),
            ],
            # Query 3: profitability data
            [
                ("SKU1", "AEFP", 20.0, 10.0, 4.5, 3.0, 30.0),
            ],
            # Query 4: velocity data (may fail, OK)
            [],
            # Query 5: expire old proposals
            [],
            # Query 6: supersede existing
            [],
            # Query 7: insert proposal
            [],
        ]
        cur = _MultiQueryCursor(query_results)
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn

        result = compute_repricing_proposals("AEFP")
        assert result == 1
        conn.commit.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  API endpoint tests
# ═══════════════════════════════════════════════════════════════════════════


class TestRepricingAPI:
    """Test that the FastAPI router endpoints are importable and configured."""

    def test_router_exists(self):
        from app.api.v1.repricing import router
        assert router.prefix == "/repricing"
        assert "repricing" in router.tags

    def test_strategy_in_body_validated(self):
        from app.api.v1.repricing import StrategyIn
        s = StrategyIn(strategy_type="buybox_match")
        assert s.strategy_type == "buybox_match"
        assert s.requires_approval is True

    def test_strategy_in_types(self):
        from app.api.v1.repricing import StrategyIn
        s = StrategyIn(
            strategy_type="competitive_undercut",
            min_price=10.0,
            max_price=50.0,
            parameters={"undercut_pct": 2.0},
        )
        assert s.parameters == {"undercut_pct": 2.0}


# ═══════════════════════════════════════════════════════════════════════════
#  Scheduler tests
# ═══════════════════════════════════════════════════════════════════════════


class TestRepricingScheduler:
    def test_register_adds_jobs(self):
        from app.platform.scheduler.repricing import register
        scheduler = MagicMock()
        register(scheduler)
        # Sprint 15 proposal computation + Sprint 16 auto-approve/execute + analytics
        assert scheduler.add_job.call_count == 3
        job_ids = [c.kwargs["id"] for c in scheduler.add_job.call_args_list]
        assert "repricing-proposal-computation" in job_ids
        assert "repricing-auto-approve-execute" in job_ids
        assert "repricing-daily-analytics" in job_ids


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Auto-approve constant
# ═══════════════════════════════════════════════════════════════════════════


class TestAutoApproveConstant:
    def test_default_value(self):
        assert AUTO_APPROVE_MAX_CHANGE_PCT == 5.0

    def test_is_positive(self):
        assert AUTO_APPROVE_MAX_CHANGE_PCT > 0


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Auto-approve proposals
# ═══════════════════════════════════════════════════════════════════════════


class TestAutoApproveProposals:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_returns_count(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 3
        mock_conn.return_value = conn
        result = auto_approve_proposals()
        assert result == 3
        assert conn.committed
        assert conn.closed

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_with_marketplace_filter(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 1
        mock_conn.return_value = conn
        result = auto_approve_proposals("MKT123")
        assert result == 1
        sql, params = conn.cursor_obj.executed[0]
        assert "marketplace_id" in sql
        assert "MKT123" in params

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_zero_when_nothing(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0
        mock_conn.return_value = conn
        result = auto_approve_proposals()
        assert result == 0

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_rollback_on_error(self, mock_conn):
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.side_effect = Exception("fail")
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        with pytest.raises(Exception):
            auto_approve_proposals()
        conn.rollback.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Execute approved prices
# ═══════════════════════════════════════════════════════════════════════════


class TestExecuteApprovedPrices:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_no_approved_returns_zero(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = execute_approved_prices("MKT123")
        assert result["submitted"] == 0
        assert result["feed_id"] is None

    @patch("app.intelligence.repricing_engine._submit_price_feed")
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_submits_and_updates(self, mock_conn, mock_feed):
        """If approved rows exist, calls feed and updates."""
        rows = [
            (1, "SKU1", "B01A", 19.99),
            (2, "SKU2", "B02A", 24.50),
        ]
        cur = _MultiQueryCursor([rows, [], []])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        mock_feed.return_value = ("FEED123", "DONE")

        with patch("app.core.config.settings") as mock_settings, \
             patch("app.core.config.MARKETPLACE_REGISTRY", {"MKT123": {"currency": "EUR"}}):
            mock_settings.SP_API_SELLER_ID = "SELLER1"
            result = execute_approved_prices("MKT123")

        assert result["submitted"] == 2
        assert result["feed_id"] == "FEED123"
        conn.commit.assert_called_once()

    @patch("app.intelligence.repricing_engine._submit_price_feed")
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_feed_error_marks_failed(self, mock_conn, mock_feed):
        """If feed submission fails, marks executions as failed."""
        rows = [(1, "SKU1", "B01", 19.99)]
        cur = _MultiQueryCursor([rows, []])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        mock_feed.side_effect = Exception("SP-API error")

        with patch("app.core.config.settings") as mock_settings, \
             patch("app.core.config.MARKETPLACE_REGISTRY", {"MKT123": {"currency": "EUR"}}):
            mock_settings.SP_API_SELLER_ID = "SELLER1"
            result = execute_approved_prices("MKT123")

        assert result["submitted"] == 1
        assert result["error"] is not None


class TestSubmitPriceFeed:
    @patch("app.intelligence.repricing_engine.asyncio")
    def test_calls_asyncio_run(self, mock_asyncio):
        from app.intelligence.repricing_engine import _submit_price_feed
        mock_asyncio.run.return_value = ("FEED1", "DONE")
        feed_id, status = _submit_price_feed("MKT1", {"header": {}, "messages": []})
        assert feed_id == "FEED1"
        assert status == "DONE"
        mock_asyncio.run.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Bulk approve / reject
# ═══════════════════════════════════════════════════════════════════════════


class TestBulkApproveExecutions:
    def test_empty_list_returns_zero(self):
        result = bulk_approve_executions([])
        assert result["approved"] == 0
        assert result["skipped"] == 0

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_approves_multiple(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 1
        mock_conn.return_value = conn
        result = bulk_approve_executions([1, 2, 3], approved_by="test_user")
        assert result["approved"] == 3
        assert result["skipped"] == 0
        assert conn.committed
        assert conn.closed

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_skips_non_proposed(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0  # none found
        mock_conn.return_value = conn
        result = bulk_approve_executions([1, 2])
        assert result["approved"] == 0
        assert result["skipped"] == 2

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_rollback_on_error(self, mock_conn):
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.side_effect = Exception("DB error")
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        with pytest.raises(Exception):
            bulk_approve_executions([1])
        conn.rollback.assert_called_once()


class TestBulkRejectExecutions:
    def test_empty_list_returns_zero(self):
        result = bulk_reject_executions([])
        assert result["rejected"] == 0
        assert result["skipped"] == 0

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_rejects_multiple(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 1
        mock_conn.return_value = conn
        result = bulk_reject_executions([10, 20])
        assert result["rejected"] == 2
        assert result["skipped"] == 0
        assert conn.committed

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_skips_non_proposed(self, mock_conn):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0
        mock_conn.return_value = conn
        result = bulk_reject_executions([1])
        assert result["rejected"] == 0
        assert result["skipped"] == 1


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Analytics
# ═══════════════════════════════════════════════════════════════════════════


class TestComputeDailyAnalytics:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_empty_returns_zero(self, mock_conn):
        conn = _FakeConn(rows=[(0, 0, 0, 0, 0, 0, 0, 0, None, None)])
        mock_conn.return_value = conn
        result = compute_daily_analytics()
        assert result["total"] == 0

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_computes_and_upserts(self, mock_conn):
        row = (10, 5, 2, 1, 4, 3, 1, 2, -1.5, 18.5)
        cur = _MultiQueryCursor([[row], []])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        result = compute_daily_analytics()
        assert result["proposals_created"] == 10
        assert result["proposals_approved"] == 5
        assert result["executions_failed"] == 1
        assert result["auto_approved_count"] == 2
        assert result["avg_price_change_pct"] == -1.5
        conn.commit.assert_called_once()

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_with_marketplace_filter(self, mock_conn):
        row = (3, 2, 0, 0, 1, 1, 0, 1, -0.5, 20.0)
        cur = _MultiQueryCursor([[row], []])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        result = compute_daily_analytics(marketplace_id="MKT1")
        assert result["marketplace_id"] == "MKT1"
        assert result["proposals_created"] == 3

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_with_specific_date(self, mock_conn):
        row = (5, 3, 1, 0, 2, 2, 0, 0, 2.0, 15.0)
        cur = _MultiQueryCursor([[row], []])
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn
        d = date(2025, 3, 10)
        result = compute_daily_analytics(target_date=d)
        assert result["date"] == "2025-03-10"


class TestGetAnalyticsTrend:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_returns_list(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_analytics_trend(days=30)
        assert isinstance(result, list)
        assert conn.closed

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_maps_columns(self, mock_conn):
        row = (
            date(2025, 3, 10), "MKT1",
            10, 5, 2, 1,
            4, 3, 1, 2,
            -1.5, 18.5, 500.0,
        )
        conn = _FakeConn(rows=[row])
        mock_conn.return_value = conn
        result = get_analytics_trend(days=7)
        assert len(result) == 1
        assert result[0]["date"] == "2025-03-10"
        assert result[0]["proposals_created"] == 10
        assert result[0]["executions_succeeded"] == 3
        assert result[0]["total_revenue_impact"] == 500.0

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_with_marketplace_filter(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_analytics_trend(days=14, marketplace_id="MKT1")
        assert isinstance(result, list)
        sql, params = conn.cursor_obj.executed[0]
        assert "marketplace_id" in sql


class TestExecutionSummaryByStrategy:
    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_returns_list(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_execution_summary_by_strategy(days=30)
        assert isinstance(result, list)

    @patch("app.intelligence.repricing_engine.connect_acc")
    def test_maps_strategy_rows(self, mock_conn):
        rows = [
            ("buybox_match", 10, 5, 2, 3, -1.0, 18.0),
            ("competitive_undercut", 5, 3, 1, 1, -2.5, 16.0),
        ]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn
        result = get_execution_summary_by_strategy()
        assert len(result) == 2
        assert result[0]["strategy_type"] == "buybox_match"
        assert result[0]["total"] == 10
        assert result[1]["strategy_type"] == "competitive_undercut"
        assert result[1]["avg_change_pct"] == -2.5


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Exec row extended dict
# ═══════════════════════════════════════════════════════════════════════════


class TestExecRowExtended:
    def test_includes_feed_id_and_auto_approved(self):
        from app.intelligence.repricing_engine import _exec_row_to_dict
        row = list(range(26))  # 26 columns
        row[0] = 1  # id
        row[17] = "proposed"  # status
        row[24] = "FEED123"
        row[25] = 1  # auto_approved
        result = _exec_row_to_dict(row)
        assert result["feed_id"] == "FEED123"
        assert result["auto_approved"] is True

    def test_backward_compat_short_row(self):
        from app.intelligence.repricing_engine import _exec_row_to_dict
        row = list(range(24))  # old 24-col query
        row[0] = 1
        row[17] = "proposed"
        result = _exec_row_to_dict(row)
        assert result["feed_id"] is None
        assert result["auto_approved"] is False


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — API endpoint tests
# ═══════════════════════════════════════════════════════════════════════════


class TestRepricingAPIv2:
    """Test Sprint 16 endpoints are importable and wired."""

    def test_router_has_bulk_approve(self):
        from app.api.v1.repricing import router
        paths = [r.path for r in router.routes]
        assert "/repricing/executions/bulk-approve" in paths

    def test_router_has_bulk_reject(self):
        from app.api.v1.repricing import router
        paths = [r.path for r in router.routes]
        assert "/repricing/executions/bulk-reject" in paths

    def test_router_has_auto_approve(self):
        from app.api.v1.repricing import router
        paths = [r.path for r in router.routes]
        assert "/repricing/executions/auto-approve" in paths

    def test_router_has_execute(self):
        from app.api.v1.repricing import router
        paths = [r.path for r in router.routes]
        assert "/repricing/executions/execute" in paths

    def test_router_has_analytics_compute(self):
        from app.api.v1.repricing import router
        paths = [r.path for r in router.routes]
        assert "/repricing/analytics/compute" in paths

    def test_router_has_analytics_trend(self):
        from app.api.v1.repricing import router
        paths = [r.path for r in router.routes]
        assert "/repricing/analytics/trend" in paths

    def test_router_has_analytics_by_strategy(self):
        from app.api.v1.repricing import router
        paths = [r.path for r in router.routes]
        assert "/repricing/analytics/by-strategy" in paths

    def test_bulk_action_model(self):
        from app.api.v1.repricing import BulkActionIn
        b = BulkActionIn(execution_ids=[1, 2, 3])
        assert len(b.execution_ids) == 3
        assert b.approved_by == "operator"

    def test_bulk_action_requires_ids(self):
        from app.api.v1.repricing import BulkActionIn
        with pytest.raises(Exception):
            BulkActionIn(execution_ids=[])
