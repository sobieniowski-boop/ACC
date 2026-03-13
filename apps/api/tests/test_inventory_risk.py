"""Unit tests for Inventory Risk Engine.

Sprint 13 – S13.6
Sprint 14 – Replenishment, alerts, velocity trends
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from app.intelligence.inventory_risk import (
    compute_stockout_probability,
    _compute_velocity_cv,
    compute_overstock_cost,
    compute_aging_risk,
    compute_composite_risk_score,
    ensure_inventory_risk_schema,
    get_risk_dashboard,
    get_risk_scores,
    get_risk_history,
    get_stockout_watchlist,
    get_overstock_report,
    DEFAULT_TARGET_DAYS,
    MONTHLY_STORAGE_FEE_PER_UNIT_EUR,
    CAPITAL_COST_ANNUAL_RATE,
    AGED_SURCHARGE_90_PLUS_PCT,
    # Sprint 14
    compute_velocity_trend,
    compute_suggested_reorder_qty,
    compute_reorder_urgency,
    compute_estimated_stockout_date,
    ensure_replenishment_schema,
    get_replenishment_plan,
    acknowledge_replenishment,
    get_risk_alerts,
    resolve_risk_alert,
    get_velocity_trends,
    DEFAULT_LEAD_TIME_DAYS,
    DEFAULT_SAFETY_STOCK_DAYS,
)


# ═══════════════════════════════════════════════════════════════════════════
#  DB mock helpers
# ═══════════════════════════════════════════════════════════════════════════

class _FakeConn:
    def __init__(self, rows=None) -> None:
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
    def __init__(self, rows=None) -> None:
        self.rows = rows or []
        self.executed: list[tuple] = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


# ═══════════════════════════════════════════════════════════════════════════
#  compute_stockout_probability
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeStockoutProbability:
    def test_no_velocity_with_stock_returns_zero(self):
        assert compute_stockout_probability(100, 0.0, 0.5, 7) == 0.0

    def test_no_velocity_no_stock_returns_one(self):
        assert compute_stockout_probability(0, 0.0, 0.5, 7) == 1.0

    def test_no_stock_returns_one(self):
        assert compute_stockout_probability(0, 5.0, 0.3, 7) == 1.0

    def test_deterministic_safe(self):
        # CV = 0, stock > demand → 0
        assert compute_stockout_probability(100, 5.0, 0.0, 7) == 0.0

    def test_deterministic_stockout(self):
        # CV = 0, stock < demand (5*14 = 70 > 50)
        assert compute_stockout_probability(50, 5.0, 0.0, 14) == 1.0

    def test_probabilistic_high_stock(self):
        # 1000 units, velocity 5/day, 7 days → demand 35, very safe
        p = compute_stockout_probability(1000, 5.0, 0.4, 7)
        assert p < 0.01

    def test_probabilistic_low_stock(self):
        # 10 units, velocity 5/day, 7 days → demand 35, likely stockout
        p = compute_stockout_probability(10, 5.0, 0.4, 7)
        assert p > 0.8

    def test_output_clamped_zero_to_one(self):
        p = compute_stockout_probability(50, 5.0, 0.5, 7)
        assert 0.0 <= p <= 1.0

    def test_longer_horizon_higher_prob(self):
        p7 = compute_stockout_probability(50, 5.0, 0.3, 7)
        p30 = compute_stockout_probability(50, 5.0, 0.3, 30)
        assert p30 >= p7


# ═══════════════════════════════════════════════════════════════════════════
#  _compute_velocity_cv
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeVelocityCv:
    def test_too_few_samples(self):
        assert _compute_velocity_cv([1.0, 2.0]) == 0.0

    def test_zero_mean(self):
        assert _compute_velocity_cv([0.0, 0.0, 0.0]) == 0.0

    def test_constant_sales(self):
        # No variance → CV = 0
        assert _compute_velocity_cv([5.0, 5.0, 5.0, 5.0]) == 0.0

    def test_typical_variance(self):
        sales = [3.0, 5.0, 7.0, 4.0, 6.0]
        cv = _compute_velocity_cv(sales)
        assert cv > 0
        assert cv < 1.0  # reasonable CV for moderate variance

    def test_high_variance(self):
        sales = [0.0, 0.0, 20.0, 0.0, 0.0]
        cv = _compute_velocity_cv(sales)
        assert cv > 1.0  # very spiky demand


# ═══════════════════════════════════════════════════════════════════════════
#  compute_overstock_cost
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeOverstockCost:
    def test_no_excess(self):
        # velocity 10/day → target = ceil(10*45) = 450; stock = 400 < 450
        result = compute_overstock_cost(400, 10.0, 20.0, 45)
        assert result["excess_units"] == 0
        assert result["total_pln"] == 0

    def test_excess_calculated(self):
        # velocity 2/day → target = ceil(2*45)=90; stock 200 → excess 110
        result = compute_overstock_cost(200, 2.0, 15.0, 45)
        assert result["excess_units"] == 110
        assert result["excess_value_pln"] == 110 * 15.0
        assert result["storage_fee_30d_pln"] > 0
        assert result["capital_tie_up_pln"] > 0
        assert result["total_pln"] == round(
            result["storage_fee_30d_pln"] + result["capital_tie_up_pln"], 2
        )

    def test_zero_velocity_all_excess(self):
        # No sales → target = 0, all stock is excess
        result = compute_overstock_cost(50, 0.0, 10.0, 45)
        assert result["excess_units"] == 50

    def test_negative_cost_treated_as_zero(self):
        result = compute_overstock_cost(100, 0.5, -5.0, 45)
        assert result["excess_value_pln"] == 0  # max(unit_cost, 0) = 0

    def test_storage_fee_formula(self):
        # 50 excess units
        result = compute_overstock_cost(50, 0.0, 10.0, 45)
        expected_storage = 50 * MONTHLY_STORAGE_FEE_PER_UNIT_EUR * 4.30
        assert result["storage_fee_30d_pln"] == round(expected_storage, 2)


# ═══════════════════════════════════════════════════════════════════════════
#  compute_aging_risk
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeAgingRisk:
    def test_no_aged_no_risk(self):
        result = compute_aging_risk(0, 10.0, 5.0, 100)
        assert result["aged_90_plus_value_pln"] == 0
        assert result["aging_risk_pln"] >= 0

    def test_all_aged(self):
        result = compute_aging_risk(100, 20.0, 0.0, 100)
        # All 100 units aged at 20 PLN = 2000 value
        assert result["aged_90_plus_value_pln"] == 2000.0
        # Risk includes 15% surcharge on aged value
        expected_risk = 2000.0 * AGED_SURCHARGE_90_PLUS_PCT
        # Plus projected new aged (remaining after 30d - already aged)
        assert result["aging_risk_pln"] >= expected_risk

    def test_projected_aging(self):
        # 50 on hand, 0 aged, velocity=0 → all units will age
        result = compute_aging_risk(0, 10.0, 0.0, 50)
        assert result["projected_aged_90_30d"] == 50  # everything ages

    def test_negative_cost_handled(self):
        result = compute_aging_risk(10, -5.0, 1.0, 50)
        assert result["aged_90_plus_value_pln"] == 0
        assert result["aging_risk_pln"] >= 0


# ═══════════════════════════════════════════════════════════════════════════
#  compute_composite_risk_score
# ═══════════════════════════════════════════════════════════════════════════

class TestCompositeRiskScore:
    def test_all_zeros_gives_low(self):
        score, tier = compute_composite_risk_score(0, 0, 0)
        assert score == 0
        assert tier == "low"

    def test_max_stockout_gives_dominant_score(self):
        score, tier = compute_composite_risk_score(1.0, 0, 0)
        assert score == 40  # 1.0 * 40
        assert tier == "medium"

    def test_critical_tier(self):
        score, tier = compute_composite_risk_score(1.0, 600.0, 300.0)
        assert score >= 70
        assert tier == "critical"

    def test_high_tier(self):
        score, tier = compute_composite_risk_score(0.8, 300.0, 100.0)
        assert 50 <= score < 70
        assert tier == "high"

    def test_medium_tier(self):
        score, tier = compute_composite_risk_score(0.5, 100.0, 50.0)
        assert 30 <= score < 70
        assert tier == "medium"

    def test_low_tier(self):
        score, tier = compute_composite_risk_score(0.1, 50.0, 20.0)
        assert score < 30
        assert tier == "low"

    def test_score_clamped_at_100(self):
        score, _ = compute_composite_risk_score(1.0, 99999, 99999)
        assert score == 100

    def test_score_never_negative(self):
        score, _ = compute_composite_risk_score(0.0, 0.0, 0.0)
        assert score >= 0


# ═══════════════════════════════════════════════════════════════════════════
#  ensure_inventory_risk_schema
# ═══════════════════════════════════════════════════════════════════════════

class TestEnsureSchema:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_executes_all_ddl_statements(self, mock_connect):
        conn = _FakeConn()
        mock_connect.return_value = conn
        ensure_inventory_risk_schema()
        assert conn.closed
        assert len(conn.cursor_obj.executed) >= 4  # 4 DDL statements


# ═══════════════════════════════════════════════════════════════════════════
#  get_risk_dashboard
# ═══════════════════════════════════════════════════════════════════════════

class TestGetRiskDashboard:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_dashboard_dict(self, mock_connect):
        row = (
            100,           # total_skus
            5,             # critical
            15,            # high
            30,            # medium
            50,            # low
            0.35,          # avg_stockout_prob_7d
            12500.0,       # total_holding_cost_pln
            3200.0,        # total_aging_risk_pln
            8000.0,        # total_excess_value_pln
            42.5,          # avg_risk_score
        )
        conn = _FakeConn(rows=[row])
        mock_connect.return_value = conn
        result = get_risk_dashboard("ATVPDKIKX0DER")
        assert result["total_skus"] == 100
        assert result["critical"] == 5
        assert result["avg_risk_score"] == 42.5
        assert conn.closed

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_no_data_returns_zeros(self, mock_connect):
        conn = _FakeConn(rows=[(None,)])
        mock_connect.return_value = conn
        result = get_risk_dashboard("ATVPDKIKX0DER")
        assert result["total_skus"] == 0
        assert conn.closed


# ═══════════════════════════════════════════════════════════════════════════
#  get_risk_scores
# ═══════════════════════════════════════════════════════════════════════════

class TestGetRiskScores:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_list(self, mock_connect):
        # First call: COUNT(*) → total
        count_conn = _FakeConn(rows=[(1,)])
        # The function calls cur.execute twice on same conn — need a smarter fake
        class _MultiCursor:
            def __init__(self):
                self.call = 0
                self.executed = []
            def execute(self, sql, params=None):
                self.executed.append((sql, params))
                self.call += 1
            def fetchone(self):
                return (1,)  # count query
            def fetchall(self):
                # 23 columns: seller_sku, asin, mkt, score_date,
                # p7, p14, p30, days_cover, v7, v30, cv, units,
                # overstock, storage, capital, excess_u, excess_v,
                # aging, aged_u, aged_v, projected, tier, score
                return [
                    ("SK1", "B0T", "MKT", date(2025, 3, 12),
                     0.45, 0.60, 0.80, 11.1, 3.2, 4.5, 0.35, 50,
                     75.0, 50.0, 25.0, 20, 400.0,
                     150.0, 10, 200.0, 5, "critical", 72),
                ]
        mc = _MultiCursor()
        conn = _FakeConn()
        conn.cursor_obj = mc
        mock_connect.return_value = conn
        result = get_risk_scores("ATVPDKIKX0DER")
        assert result["total"] == 1
        assert len(result["items"]) == 1
        assert result["items"][0]["seller_sku"] == "SK1"
        assert result["items"][0]["risk_tier"] == "critical"
        assert conn.closed

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_empty_result(self, mock_connect):
        class _MultiCursor:
            def __init__(self):
                self.executed = []
            def execute(self, sql, params=None):
                self.executed.append((sql, params))
            def fetchone(self):
                return (0,)
            def fetchall(self):
                return []
        mc = _MultiCursor()
        conn = _FakeConn()
        conn.cursor_obj = mc
        mock_connect.return_value = conn
        result = get_risk_scores("ATVPDKIKX0DER")
        assert result["total"] == 0
        assert result["items"] == []


# ═══════════════════════════════════════════════════════════════════════════
#  get_risk_history
# ═══════════════════════════════════════════════════════════════════════════

class TestGetRiskHistory:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_history(self, mock_connect):
        # 9 columns: score_date, risk_score, risk_tier, p7, overstock, aging,
        #            days_cover, units_available, velocity_30d
        rows = [
            (date(2025, 3, 11), 65, "high", 0.40, 80.0, 120.0, 9.2, 30, 3.3),
            (date(2025, 3, 12), 72, "critical", 0.50, 90.0, 150.0, 6.1, 20, 3.3),
        ]
        conn = _FakeConn(rows=rows)
        mock_connect.return_value = conn
        result = get_risk_history("SKU-001", "ATVPDKIKX0DER")
        assert len(result) == 2
        assert result[0]["risk_score"] == 65
        assert result[1]["risk_tier"] == "critical"
        assert conn.closed


# ═══════════════════════════════════════════════════════════════════════════
#  get_stockout_watchlist
# ═══════════════════════════════════════════════════════════════════════════

class TestGetStockoutWatchlist:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_watchlist(self, mock_connect):
        # 9 columns: sku, asin, mkt, p7, p14, p30, days_cover, vel30, units
        row = (
            "SKU-002", "B0RISK", "ATVPDKIKX0DER",
            0.85, 0.95, 0.99,
            5.3, 1.5, 8,
        )
        conn = _FakeConn(rows=[row])
        mock_connect.return_value = conn
        result = get_stockout_watchlist("ATVPDKIKX0DER")
        assert len(result) == 1
        assert result[0]["stockout_prob_7d"] == 0.85

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_empty_watchlist(self, mock_connect):
        conn = _FakeConn(rows=[])
        mock_connect.return_value = conn
        result = get_stockout_watchlist("ATVPDKIKX0DER")
        assert len(result) == 0


# ═══════════════════════════════════════════════════════════════════════════
#  get_overstock_report
# ═══════════════════════════════════════════════════════════════════════════

class TestGetOverstockReport:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_overstock_skus(self, mock_connect):
        # 10 columns: sku, asin, mkt, overstock, storage, capital,
        #             excess_units, excess_value, days_cover, vel30
        row = (
            "SKU-003", "B0OVER", "ATVPDKIKX0DER",
            350.0, 200.0, 150.0,
            100, 2000.0, 166.7, 3.0,
        )
        conn = _FakeConn(rows=[row])
        mock_connect.return_value = conn
        result = get_overstock_report("ATVPDKIKX0DER")
        assert len(result) == 1
        assert result[0]["overstock_holding_cost_pln"] == 350.0

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_empty_report(self, mock_connect):
        conn = _FakeConn(rows=[])
        mock_connect.return_value = conn
        result = get_overstock_report("ATVPDKIKX0DER")
        assert len(result) == 0


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — compute_velocity_trend
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeVelocityTrend:
    def test_accelerating(self):
        trend, pct_val = compute_velocity_trend(8.0, 5.0)
        assert trend == "accelerating"
        assert pct_val > 25.0

    def test_decelerating(self):
        trend, pct_val = compute_velocity_trend(3.0, 5.0)
        assert trend == "decelerating"
        assert pct_val < -25.0

    def test_stable(self):
        trend, pct_val = compute_velocity_trend(5.5, 5.0)
        assert trend == "stable"
        assert -25.0 <= pct_val <= 25.0

    def test_zero_30d_positive_7d(self):
        trend, pct_val = compute_velocity_trend(3.0, 0.0)
        assert trend == "accelerating"
        assert pct_val == 100.0

    def test_both_zero(self):
        trend, pct_val = compute_velocity_trend(0.0, 0.0)
        assert trend == "stable"
        assert pct_val == 0.0

    def test_boundary_exactly_25pct(self):
        # 6.25 / 5.0 = 25% change exactly → should be stable
        trend, _ = compute_velocity_trend(6.25, 5.0)
        assert trend == "stable"


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — compute_suggested_reorder_qty
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeSuggestedReorderQty:
    def test_typical_reorder(self):
        # velocity=5, target=45, safety=14 → (45+14)*5=295 - 100 = 195
        qty = compute_suggested_reorder_qty(100, 5.0)
        assert qty == 195

    def test_no_velocity_returns_zero(self):
        assert compute_suggested_reorder_qty(100, 0.0) == 0

    def test_sufficient_stock_returns_zero(self):
        # velocity=1, target=45, safety=14 → (45+14)*1=59 - 100 = negative → 0
        assert compute_suggested_reorder_qty(100, 1.0) == 0

    def test_custom_params(self):
        qty = compute_suggested_reorder_qty(0, 10.0, target_days=30, safety_stock_days=10, lead_time_days=14)
        # (30+10)*10=400
        assert qty == 400

    def test_negative_velocity_returns_zero(self):
        assert compute_suggested_reorder_qty(50, -2.0) == 0


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — compute_reorder_urgency
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeReorderUrgency:
    def test_critical_low_days_cover(self):
        assert compute_reorder_urgency(5.0, 0.3, "medium") == "critical"

    def test_critical_high_stockout(self):
        assert compute_reorder_urgency(30.0, 0.75, "medium") == "critical"

    def test_critical_tier(self):
        assert compute_reorder_urgency(30.0, 0.1, "critical") == "critical"

    def test_high_below_lead_time(self):
        assert compute_reorder_urgency(15.0, 0.1, "low") == "high"

    def test_high_stockout_threshold(self):
        assert compute_reorder_urgency(30.0, 0.45, "low") == "high"

    def test_high_tier(self):
        assert compute_reorder_urgency(30.0, 0.1, "high") == "high"

    def test_medium_near_lead_time(self):
        assert compute_reorder_urgency(30.0, 0.25, "low") == "medium"

    def test_low(self):
        assert compute_reorder_urgency(60.0, 0.05, "low") == "low"

    def test_none_days_cover_treated_as_safe(self):
        # None → 999.0, so won't trigger critical/high from days_cover
        assert compute_reorder_urgency(None, 0.05, "low") == "low"


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — compute_estimated_stockout_date
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeEstimatedStockoutDate:
    def test_typical(self):
        base = date(2025, 3, 15)
        result = compute_estimated_stockout_date(50, 5.0, base)
        assert result == date(2025, 3, 25)  # 50/5=10 days

    def test_no_velocity(self):
        assert compute_estimated_stockout_date(100, 0.0) is None

    def test_no_stock(self):
        assert compute_estimated_stockout_date(0, 5.0) is None

    def test_negative_velocity(self):
        assert compute_estimated_stockout_date(100, -1.0) is None


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — ensure_replenishment_schema
# ═══════════════════════════════════════════════════════════════════════════

class TestEnsureReplenishmentSchema:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_executes_ddl_statements(self, mock_connect):
        conn = _FakeConn()
        mock_connect.return_value = conn
        ensure_replenishment_schema()
        assert conn.closed
        assert len(conn.cursor_obj.executed) >= 2  # at least 2 tables


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — get_replenishment_plan
# ═══════════════════════════════════════════════════════════════════════════

class TestGetReplenishmentPlan:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_paginated(self, mock_connect):
        class _MC:
            def __init__(self):
                self.executed = []
            def execute(self, sql, params=None):
                self.executed.append((sql, params))
            def fetchone(self):
                return (1,)
            def fetchall(self):
                return [
                    (
                        "SK1", "B0X", "MKT1", date(2025, 3, 15),
                        72.0, "critical", 0.65,
                        8.5, 3.0, 4.5,
                        "decelerating", -32.0,
                        195, "critical",
                        45, 21, 14,
                        date(2025, 3, 23),
                        85.0, 40.0,
                        50, 0,
                    )
                ]
        mc = _MC()
        conn = _FakeConn()
        conn.cursor_obj = mc
        mock_connect.return_value = conn
        result = get_replenishment_plan("MKT1")
        assert result["total"] == 1
        assert len(result["items"]) == 1
        item = result["items"][0]
        assert item["seller_sku"] == "SK1"
        assert item["reorder_urgency"] == "critical"
        assert item["suggested_reorder_qty"] == 195
        assert item["is_acknowledged"] is False
        assert conn.closed

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_empty(self, mock_connect):
        class _MC:
            def __init__(self):
                self.executed = []
            def execute(self, sql, params=None):
                self.executed.append((sql, params))
            def fetchone(self):
                return (0,)
            def fetchall(self):
                return []
        mc = _MC()
        conn = _FakeConn()
        conn.cursor_obj = mc
        mock_connect.return_value = conn
        result = get_replenishment_plan()
        assert result["total"] == 0
        assert result["items"] == []


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — acknowledge_replenishment
# ═══════════════════════════════════════════════════════════════════════════

class TestAcknowledgeReplenishment:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_success(self, mock_connect):
        conn = _FakeConn()
        conn.cursor_obj.rows = []
        # Simulate rowcount = 1
        conn.cursor_obj.rowcount = 1
        mock_connect.return_value = conn
        result = acknowledge_replenishment("SK1", "MKT1", acknowledged_by="user")
        assert result is True
        assert conn.committed

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_already_acknowledged(self, mock_connect):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0
        mock_connect.return_value = conn
        result = acknowledge_replenishment("SK1", "MKT1")
        assert result is False


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — get_risk_alerts
# ═══════════════════════════════════════════════════════════════════════════

class TestGetRiskAlerts:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_alerts(self, mock_connect):
        from datetime import datetime
        class _MC:
            def __init__(self):
                self.executed = []
            def execute(self, sql, params=None):
                self.executed.append((sql, params))
            def fetchone(self):
                return (1,)
            def fetchall(self):
                return [
                    (
                        42, "SK1", "MKT1", "tier_escalation", "critical",
                        "Tier escalated to critical", "Score: 75",
                        75.0, 45.0, None,
                        75.0, "critical",
                        0, None, datetime(2025, 3, 15, 8, 0, 0),
                    )
                ]
        mc = _MC()
        conn = _FakeConn()
        conn.cursor_obj = mc
        mock_connect.return_value = conn
        result = get_risk_alerts("MKT1")
        assert result["total"] == 1
        assert result["items"][0]["id"] == 42
        assert result["items"][0]["alert_type"] == "tier_escalation"
        assert result["items"][0]["is_resolved"] is False

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_empty_alerts(self, mock_connect):
        class _MC:
            def __init__(self):
                self.executed = []
            def execute(self, sql, params=None):
                self.executed.append((sql, params))
            def fetchone(self):
                return (0,)
            def fetchall(self):
                return []
        mc = _MC()
        conn = _FakeConn()
        conn.cursor_obj = mc
        mock_connect.return_value = conn
        result = get_risk_alerts()
        assert result["total"] == 0
        assert result["items"] == []


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — resolve_risk_alert
# ═══════════════════════════════════════════════════════════════════════════

class TestResolveRiskAlert:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_success(self, mock_connect):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 1
        mock_connect.return_value = conn
        assert resolve_risk_alert(42) is True
        assert conn.committed

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_not_found(self, mock_connect):
        conn = _FakeConn()
        conn.cursor_obj.rowcount = 0
        mock_connect.return_value = conn
        assert resolve_risk_alert(999) is False


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 14 — get_velocity_trends
# ═══════════════════════════════════════════════════════════════════════════

class TestGetVelocityTrends:
    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_returns_trends(self, mock_connect):
        rows = [
            (date(2025, 3, 14), 3.2, 4.5, 0.35, 65, 0.40, 10.0, 30),
            (date(2025, 3, 15), 2.8, 4.4, 0.38, 72, 0.50, 8.5, 25),
        ]
        conn = _FakeConn(rows=rows)
        mock_connect.return_value = conn
        result = get_velocity_trends("SK1", "MKT1", days=7)
        assert len(result) == 2
        assert result[0]["velocity_7d"] == 3.2
        assert result[1]["risk_score"] == 72
        assert conn.closed

    @patch("app.intelligence.inventory_risk.connect_acc")
    def test_empty_trends(self, mock_connect):
        conn = _FakeConn(rows=[])
        mock_connect.return_value = conn
        result = get_velocity_trends("NONEXIST", "MKT1")
        assert result == []
