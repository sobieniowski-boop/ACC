"""Tests for seasonality_service: scoring, classification, peak detection.

Sprint 4, Task S4-05.
"""
import statistics

import pytest

from app.services.seasonality_service import (
    _strength_score,
    _evergreen_score,
    _volatility_score,
    _confidence_score,
    _classify,
    _detect_peaks,
    _detect_ramp,
    _detect_decay,
    _gap_score,
)


# ── _strength_score ──────────────────────────────────────────────

class TestStrengthScore:
    def test_empty_list(self):
        assert _strength_score([]) == 0.0

    def test_all_zeros(self):
        assert _strength_score([0, 0, 0, 0]) == 0.0

    def test_flat_distribution(self):
        result = _strength_score([1.0, 1.0, 1.0, 1.0])
        assert result == 0.0

    def test_high_variance(self):
        result = _strength_score([0.0, 0.0, 0.0, 0.0, 0.0, 5.0])
        assert result > 50

    def test_capped_at_100(self):
        result = _strength_score([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100])
        assert result <= 100.0

    def test_moderate_season(self):
        indices = [0.8, 0.7, 1.0, 1.2, 1.5, 1.3, 1.1, 0.9, 0.8, 0.7, 0.6, 0.4]
        result = _strength_score(indices)
        assert 0 < result < 100

    def test_single_element(self):
        assert _strength_score([5.0]) == 0.0


# ── _evergreen_score ─────────────────────────────────────────────

class TestEvergreenScore:
    def test_empty_demand(self):
        assert _evergreen_score([], []) == 50.0

    def test_perfectly_flat(self):
        d = [1.0] * 12
        s = [1.0] * 12
        assert _evergreen_score(d, s) == 100.0

    def test_highly_varied(self):
        d = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 5.0]
        s = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 5.0]
        result = _evergreen_score(d, s)
        assert result < 50

    def test_clamped_non_negative(self):
        d = [0.0, 0.0, 0.0, 0.0, 0.0, 100.0]
        s = [0.0, 0.0, 0.0, 0.0, 0.0, 100.0]
        result = _evergreen_score(d, s)
        assert result >= 0

    def test_zero_mean_returns_100(self):
        # all zeros: stdev=0, mean=0 → cv=0 → (1-0)*100 = 100 (perfect flat)
        d = [0.0] * 12
        s = [0.0] * 12
        assert _evergreen_score(d, s) == 100.0


# ── _volatility_score ────────────────────────────────────────────

class TestVolatilityScore:
    def test_too_short(self):
        assert _volatility_score([1.0, 2.0], [1.0, 2.0]) == 0.0

    def test_flat_zero_volatility(self):
        d = [1.0] * 12
        s = [1.0] * 12
        assert _volatility_score(d, s) == 0.0

    def test_high_swings(self):
        d = [0, 5, 0, 5, 0, 5, 0, 5, 0, 5, 0, 5]
        s = [0, 5, 0, 5, 0, 5, 0, 5, 0, 5, 0, 5]
        result = _volatility_score(d, s)
        assert result > 50

    def test_capped_at_100(self):
        d = [0, 100, 0, 100, 0, 100]
        s = [0, 100, 0, 100, 0, 100]
        result = _volatility_score(d, s)
        assert result <= 100.0


# ── _confidence_score ────────────────────────────────────────────

class TestConfidenceScore:
    def test_full_coverage_full_indices(self):
        result = _confidence_score(24, list(range(1, 13)))
        assert result == 100.0

    def test_minimal_data(self):
        result = _confidence_score(3, [1.0, 2.0, 3.0])
        assert result < 30

    def test_12_months_indices(self):
        result = _confidence_score(12, list(range(12)))
        assert result >= 50

    def test_6_month_indices(self):
        result = _confidence_score(6, list(range(6)))
        assert 20 < result < 60

    def test_zero_months(self):
        result = _confidence_score(0, [])
        assert result >= 0


# ── _classify ────────────────────────────────────────────────────

class TestClassify:
    def test_evergreen(self):
        assert _classify(strength=10, evergreen=80, volatility=20) == "EVERGREEN"

    def test_irregular(self):
        assert _classify(strength=50, evergreen=50, volatility=80) == "IRREGULAR"

    def test_peak_seasonal(self):
        assert _classify(strength=85, evergreen=30, volatility=40) == "PEAK_SEASONAL"

    def test_strong_seasonal(self):
        assert _classify(strength=60, evergreen=50, volatility=40) == "STRONG_SEASONAL"

    def test_mild_seasonal(self):
        assert _classify(strength=30, evergreen=50, volatility=40) == "MILD_SEASONAL"

    def test_default_evergreen(self):
        assert _classify(strength=10, evergreen=50, volatility=20) == "EVERGREEN"

    def test_boundary_evergreen_75(self):
        assert _classify(strength=24, evergreen=75, volatility=20) == "EVERGREEN"

    def test_boundary_strength_25(self):
        assert _classify(strength=25, evergreen=50, volatility=20) == "MILD_SEASONAL"

    def test_boundary_strength_55(self):
        assert _classify(strength=55, evergreen=50, volatility=20) == "STRONG_SEASONAL"

    def test_boundary_volatility_70(self):
        assert _classify(strength=99, evergreen=99, volatility=71) == "IRREGULAR"


# ── _detect_peaks ────────────────────────────────────────────────

class TestDetectPeaks:
    def test_empty(self):
        assert _detect_peaks([], []) == []

    def test_single_month(self):
        assert _detect_peaks([5.0], [11]) == [11]

    def test_top_3(self):
        indices = [0.5, 0.6, 0.8, 1.0, 1.5, 2.0, 1.8, 1.2, 0.9, 0.7, 0.5, 0.4]
        months = list(range(1, 13))
        peaks = _detect_peaks(indices, months)
        assert len(peaks) == 3
        assert 6 in peaks  # highest=2.0 in June
        assert 7 in peaks  # 1.8 in July
        assert 5 in peaks  # 1.5 in May


# ── _detect_ramp ─────────────────────────────────────────────────

class TestDetectRamp:
    def test_empty_peaks(self):
        assert _detect_ramp([], [], []) == []

    def test_ramp_before_peak(self):
        months = list(range(1, 13))
        indices = [1.0] * 12
        peaks = [6, 7]
        ramp = _detect_ramp(indices, months, peaks)
        assert 5 in ramp  # month before June

    def test_wraparound_jan_peak(self):
        months = list(range(1, 13))
        indices = [1.0] * 12
        peaks = [1]
        ramp = _detect_ramp(indices, months, peaks)
        assert 12 in ramp  # Dec wraps to Jan

    def test_ramp_not_in_peaks(self):
        months = list(range(1, 13))
        indices = [1.0] * 12
        peaks = [5, 6]
        ramp = _detect_ramp(indices, months, peaks)
        assert 5 not in ramp  # 5 is a peak itself
        assert 4 in ramp


# ── _detect_decay ────────────────────────────────────────────────

class TestDetectDecay:
    def test_empty_peaks(self):
        assert _detect_decay([], [], []) == []

    def test_decay_after_peak(self):
        months = list(range(1, 13))
        indices = [1.0] * 12
        peaks = [6]
        decay = _detect_decay(indices, months, peaks)
        assert 7 in decay

    def test_wraparound_dec_peak(self):
        months = list(range(1, 13))
        indices = [1.0] * 12
        peaks = [12]
        decay = _detect_decay(indices, months, peaks)
        assert 1 in decay  # Jan after Dec

    def test_decay_not_in_peaks(self):
        months = list(range(1, 13))
        indices = [1.0] * 12
        peaks = [6, 7]
        decay = _detect_decay(indices, months, peaks)
        assert 6 not in decay
        assert 7 not in decay
        assert 8 in decay


# ── _gap_score ───────────────────────────────────────────────────

class TestGapScore:
    def test_empty(self):
        assert _gap_score([], []) == 0.0

    def test_identical(self):
        a = [1.0, 2.0, 3.0]
        assert _gap_score(a, a) == 0.0

    def test_known_gap(self):
        a = [1.0, 2.0, 3.0]
        b = [2.0, 3.0, 4.0]
        assert _gap_score(a, b) == 1.0

    def test_partial_empty(self):
        assert _gap_score([1.0], []) == 0.0
