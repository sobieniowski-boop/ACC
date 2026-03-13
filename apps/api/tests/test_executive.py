"""Tests for executive_service: helpers, health labels.

Sprint 4, Task S4-08.
"""
from decimal import Decimal

import pytest

from app.services.executive_service import (
    _f,
    _i,
    _mkt_code,
    _health_label,
)


# ── _f helper ────────────────────────────────────────────────────

class TestFloatHelper:
    def test_none_returns_default(self):
        assert _f(None) == 0.0

    def test_custom_default(self):
        assert _f(None, default=5.5) == 5.5

    def test_valid_number(self):
        assert _f(12.34) == 12.34

    def test_string_number(self):
        assert _f("9.99") == 9.99

    def test_invalid_string_returns_default(self):
        assert _f("abc") == 0.0

    def test_decimal_converted(self):
        assert _f(Decimal("7.77")) == 7.77


# ── _i helper ────────────────────────────────────────────────────

class TestIntHelper:
    def test_none_returns_default(self):
        assert _i(None) == 0

    def test_custom_default(self):
        assert _i(None, default=42) == 42

    def test_valid_int(self):
        assert _i(7) == 7

    def test_float_truncated(self):
        assert _i(3.9) == 3

    def test_invalid_string_returns_default(self):
        assert _i("abc") == 0


# ── _mkt_code ────────────────────────────────────────────────────

class TestMktCode:
    def test_none_returns_empty(self):
        assert _mkt_code(None) == ""

    def test_empty_string_returns_empty(self):
        assert _mkt_code("") == ""

    def test_unknown_marketplace_uses_last_two(self):
        result = _mkt_code("XXXXXXXXPL")
        assert result.endswith("PL")


# ── _health_label ────────────────────────────────────────────────

class TestHealthLabel:
    def test_excellent(self):
        result = _health_label(95)
        assert result["label"] == "excellent"
        assert result["color"] == "green"
        assert result["score"] == 95

    def test_exact_90_excellent(self):
        result = _health_label(90)
        assert result["label"] == "excellent"

    def test_healthy(self):
        result = _health_label(80)
        assert result["label"] == "healthy"
        assert result["color"] == "blue"

    def test_exact_75_healthy(self):
        result = _health_label(75)
        assert result["label"] == "healthy"

    def test_watchlist(self):
        result = _health_label(65)
        assert result["label"] == "watchlist"
        assert result["color"] == "yellow"

    def test_exact_60_watchlist(self):
        result = _health_label(60)
        assert result["label"] == "watchlist"

    def test_risk(self):
        result = _health_label(50)
        assert result["label"] == "risk"
        assert result["color"] == "orange"

    def test_exact_40_risk(self):
        result = _health_label(40)
        assert result["label"] == "risk"

    def test_critical(self):
        result = _health_label(20)
        assert result["label"] == "critical"
        assert result["color"] == "red"

    def test_zero_critical(self):
        result = _health_label(0)
        assert result["label"] == "critical"

    def test_scores_preserved(self):
        for score in [0, 25, 45, 62, 78, 95]:
            result = _health_label(score)
            assert result["score"] == score
