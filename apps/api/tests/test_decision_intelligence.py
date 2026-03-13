"""Tests for decision_intelligence_service: scoring, labels, delta calc.

Sprint 4, Task S4-06.
"""
from decimal import Decimal

import pytest

from app.services.decision_intelligence_service import (
    _f,
    _success_label,
    _calc_delta,
    _calc_confidence_adjustment,
    MONITORING_WINDOWS,
)


# ── _f (float conversion) ───────────────────────────────────────

class TestFloatConversion:
    def test_none_returns_zero(self):
        assert _f(None) == 0.0

    def test_decimal_converted(self):
        assert _f(Decimal("12.34")) == 12.34

    def test_int_converted(self):
        assert _f(42) == 42.0

    def test_float_passthrough(self):
        assert _f(3.14) == 3.14


# ── _success_label ───────────────────────────────────────────────

class TestSuccessLabel:
    def test_overperformed(self):
        assert _success_label(1.5) == "overperformed"

    def test_exact_overperformed_boundary(self):
        assert _success_label(1.2) == "overperformed"

    def test_on_target(self):
        assert _success_label(1.0) == "on_target"

    def test_exact_on_target_boundary(self):
        assert _success_label(0.8) == "on_target"

    def test_partial_success(self):
        assert _success_label(0.6) == "partial_success"

    def test_exact_partial_boundary(self):
        assert _success_label(0.4) == "partial_success"

    def test_failure(self):
        assert _success_label(0.1) == "failure"

    def test_zero_is_failure(self):
        assert _success_label(0.0) == "failure"

    def test_negative_is_failure(self):
        assert _success_label(-1.0) == "failure"


# ── _calc_confidence_adjustment ─────────────────────────────────

class TestConfidenceAdjustment:
    def test_none_returns_zero(self):
        assert _calc_confidence_adjustment(None) == 0.0

    def test_overperformed_boost(self):
        assert _calc_confidence_adjustment(1.5) == 0.05

    def test_exact_1_2_boost(self):
        assert _calc_confidence_adjustment(1.2) == 0.05

    def test_on_target_reinforce(self):
        assert _calc_confidence_adjustment(1.0) == 0.02

    def test_exact_0_8_reinforce(self):
        assert _calc_confidence_adjustment(0.8) == 0.02

    def test_partial_downgrade(self):
        assert _calc_confidence_adjustment(0.5) == -0.05

    def test_exact_0_4_downgrade(self):
        assert _calc_confidence_adjustment(0.4) == -0.05

    def test_failure_significant_downgrade(self):
        assert _calc_confidence_adjustment(0.1) == -0.12

    def test_zero_failure(self):
        assert _calc_confidence_adjustment(0.0) == -0.12

    def test_negative_failure(self):
        assert _calc_confidence_adjustment(-2.0) == -0.12


# ── _calc_delta ──────────────────────────────────────────────────

class TestCalcDelta:
    def test_positive_deltas(self):
        baseline = {"revenue_30d": 1000, "profit_30d": 200,
                    "margin_30d": 20.0, "units_30d": 50}
        actual = {"revenue_period": 1500, "profit_period": 350,
                  "margin_period": 23.3, "units_period": 70}
        d = _calc_delta(baseline, actual)
        assert d["revenue_delta"] == 500
        assert d["profit_delta"] == 150
        assert round(d["margin_delta"], 1) == 3.3
        assert d["units_delta"] == 20

    def test_negative_deltas(self):
        baseline = {"revenue_30d": 2000, "profit_30d": 500,
                    "margin_30d": 25.0, "units_30d": 100}
        actual = {"revenue_period": 1000, "profit_period": 200,
                  "margin_period": 20.0, "units_period": 60}
        d = _calc_delta(baseline, actual)
        assert d["revenue_delta"] == -1000
        assert d["profit_delta"] == -300

    def test_missing_keys_default_to_zero(self):
        d = _calc_delta({}, {})
        assert d["revenue_delta"] == 0
        assert d["profit_delta"] == 0
        assert d["margin_delta"] == 0
        assert d["units_delta"] == 0


# ── MONITORING_WINDOWS ───────────────────────────────────────────

class TestMonitoringWindows:
    def test_price_increase_windows(self):
        assert MONITORING_WINDOWS["PRICE_INCREASE"] == [14, 30]

    def test_ads_scale_up_has_three_windows(self):
        assert len(MONITORING_WINDOWS["ADS_SCALE_UP"]) == 3
        assert 7 in MONITORING_WINDOWS["ADS_SCALE_UP"]

    def test_content_fix_has_long_windows(self):
        assert MONITORING_WINDOWS["CONTENT_FIX"] == [30, 60]

    def test_all_windows_are_positive(self):
        for opp_type, windows in MONITORING_WINDOWS.items():
            for w in windows:
                assert w > 0, f"Window {w} for {opp_type} must be positive"

    def test_all_windows_sorted(self):
        for opp_type, windows in MONITORING_WINDOWS.items():
            assert windows == sorted(windows), \
                f"Windows for {opp_type} not sorted: {windows}"

    def test_exec_types_present(self):
        assert "EXEC_RISK_PROFIT_DECLINE" in MONITORING_WINDOWS
        assert "EXEC_RISK_LOW_MARGIN" in MONITORING_WINDOWS
