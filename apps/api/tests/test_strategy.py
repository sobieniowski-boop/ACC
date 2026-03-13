"""Tests for strategy_service: priority score, confidence, labels, helpers.

Sprint 4, Task S4-07.
"""
from decimal import Decimal

import pytest

from app.services.strategy_service import (
    _f,
    _i,
    _priority_label,
    _clamp,
    compute_priority_score,
    compute_confidence,
)


# ── _f / _i helpers ─────────────────────────────────────────────

class TestHelpers:
    def test_f_none(self):
        assert _f(None) == 0.0

    def test_f_decimal(self):
        assert _f(Decimal("9.99")) == 9.99

    def test_f_int(self):
        assert _f(7) == 7.0

    def test_i_none(self):
        assert _i(None) == 0

    def test_i_float(self):
        assert _i(3.9) == 3


# ── _clamp ───────────────────────────────────────────────────────

class TestClamp:
    def test_within_range(self):
        assert _clamp(50) == 50

    def test_below_range(self):
        assert _clamp(-10) == 0

    def test_above_range(self):
        assert _clamp(200) == 100

    def test_custom_bounds(self):
        assert _clamp(5, lo=10, hi=20) == 10
        assert _clamp(25, lo=10, hi=20) == 20


# ── _priority_label ─────────────────────────────────────────────

class TestPriorityLabel:
    def test_do_now(self):
        assert _priority_label(95) == "do_now"

    def test_exact_90(self):
        assert _priority_label(90) == "do_now"

    def test_this_week(self):
        assert _priority_label(80) == "this_week"

    def test_exact_75(self):
        assert _priority_label(75) == "this_week"

    def test_this_month(self):
        assert _priority_label(65) == "this_month"

    def test_exact_60(self):
        assert _priority_label(60) == "this_month"

    def test_backlog(self):
        assert _priority_label(50) == "backlog"

    def test_exact_40(self):
        assert _priority_label(40) == "backlog"

    def test_low(self):
        assert _priority_label(20) == "low"

    def test_zero(self):
        assert _priority_label(0) == "low"


# ── compute_priority_score ──────────────────────────────────────

class TestComputePriorityScore:
    def test_defaults_baseline(self):
        score = compute_priority_score()
        assert 0 <= score <= 100

    def test_maximum_impact(self):
        score = compute_priority_score(
            impact_profit=50000,
            confidence=100,
            urgency=100,
            effort=0,
            strategic_fit=100,
            readiness=100,
        )
        assert score == 100.0

    def test_zero_everything(self):
        score = compute_priority_score(
            impact_profit=0,
            confidence=0,
            urgency=0,
            effort=100,
            strategic_fit=0,
            readiness=0,
        )
        assert score == 0.0

    def test_high_effort_reduces_score(self):
        low_effort = compute_priority_score(effort=10)
        high_effort = compute_priority_score(effort=90)
        assert low_effort > high_effort

    def test_profit_impact_matters(self):
        low_profit = compute_priority_score(impact_profit=100)
        high_profit = compute_priority_score(impact_profit=30000)
        assert high_profit > low_profit

    def test_capped_at_100(self):
        score = compute_priority_score(
            impact_profit=999999,
            confidence=100,
            urgency=100,
            effort=0,
            strategic_fit=100,
            readiness=100,
        )
        assert score <= 100.0

    def test_floor_at_zero(self):
        score = compute_priority_score(
            impact_profit=0,
            confidence=0,
            urgency=0,
            effort=100,
            strategic_fit=0,
            readiness=0,
        )
        assert score >= 0.0

    def test_returns_float(self):
        score = compute_priority_score()
        assert isinstance(score, float)


# ── compute_confidence ──────────────────────────────────────────

class TestComputeConfidence:
    def test_minimum_no_data(self):
        score = compute_confidence(
            has_cost=False, has_traffic=False, has_ads=False,
            has_family=False, days_of_data=0, margin_stable=False,
        )
        assert score == 30.0

    def test_all_signals_present(self):
        score = compute_confidence(
            has_cost=True, has_traffic=True, has_ads=True,
            has_family=True, days_of_data=90, margin_stable=True,
        )
        assert score == 100.0

    def test_cost_adds_20(self):
        without = compute_confidence(has_cost=False)
        with_cost = compute_confidence(has_cost=True)
        assert with_cost - without == pytest.approx(20.0, abs=0.5)

    def test_traffic_adds_15(self):
        without = compute_confidence(has_traffic=False, has_cost=False,
                                     margin_stable=False, days_of_data=0)
        with_traffic = compute_confidence(has_traffic=True, has_cost=False,
                                          margin_stable=False, days_of_data=0)
        assert with_traffic - without == pytest.approx(15.0, abs=0.5)

    def test_days_bonus_capped(self):
        short = compute_confidence(has_cost=False, has_traffic=False,
                                   has_ads=False, has_family=False,
                                   margin_stable=False, days_of_data=10)
        long = compute_confidence(has_cost=False, has_traffic=False,
                                  has_ads=False, has_family=False,
                                  margin_stable=False, days_of_data=180)
        assert long >= short
        assert long <= 40.0  # 30 base + max 10 from days

    def test_clamped_at_100(self):
        score = compute_confidence(
            has_cost=True, has_traffic=True, has_ads=True,
            has_family=True, days_of_data=999, margin_stable=True,
        )
        assert score <= 100.0
