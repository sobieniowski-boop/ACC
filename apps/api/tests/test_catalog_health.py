"""Unit tests for catalog health scoring functions.

Tests the pure scoring functions and diff detection in catalog_health module.
Sprint 9 – S9.6
"""
from __future__ import annotations

import json

import pytest

from app.intelligence.catalog_health import (
    compute_health_score,
    detect_and_record_diffs,
    score_basic_content,
    score_content_completeness,
    score_issues,
    score_listing_status,
    score_suppression,
    _score_fields_json,
    _to_str,
)


# ═══════════════════════════════════════════════════════════════════════════
#  score_listing_status
# ═══════════════════════════════════════════════════════════════════════════

class TestScoreListingStatus:
    def test_active(self):
        assert score_listing_status("ACTIVE") == 20

    def test_inactive(self):
        assert score_listing_status("INACTIVE") == 10

    def test_incomplete(self):
        assert score_listing_status("INCOMPLETE") == 5

    def test_suppressed(self):
        assert score_listing_status("SUPPRESSED") == 0

    def test_deleted(self):
        assert score_listing_status("DELETED") == 0

    def test_none(self):
        assert score_listing_status(None) == 0

    def test_unknown(self):
        assert score_listing_status("UNKNOWN") == 0

    def test_case_insensitive(self):
        assert score_listing_status("active") == 20


# ═══════════════════════════════════════════════════════════════════════════
#  score_issues
# ═══════════════════════════════════════════════════════════════════════════

class TestScoreIssues:
    def test_no_issues(self):
        assert score_issues(False, None) == 15

    def test_warning_issues(self):
        assert score_issues(True, "WARNING") == 8

    def test_error_issues(self):
        assert score_issues(True, "ERROR") == 0

    def test_none_severity_with_issues(self):
        # has_issues=True but severity is None → treated as warning
        assert score_issues(True, None) == 8


# ═══════════════════════════════════════════════════════════════════════════
#  score_suppression
# ═══════════════════════════════════════════════════════════════════════════

class TestScoreSuppression:
    def test_not_suppressed(self):
        assert score_suppression(False) == 15

    def test_suppressed(self):
        assert score_suppression(True) == 0


# ═══════════════════════════════════════════════════════════════════════════
#  score_basic_content
# ═══════════════════════════════════════════════════════════════════════════

class TestScoreBasicContent:
    def test_all_present(self):
        assert score_basic_content("My Title", "https://img/1.jpg", 29.99) == 30

    def test_missing_title(self):
        assert score_basic_content(None, "https://img/1.jpg", 29.99) == 20

    def test_missing_image(self):
        assert score_basic_content("Title", None, 29.99) == 20

    def test_zero_price(self):
        assert score_basic_content("Title", "https://img/1.jpg", 0.0) == 20

    def test_none_price(self):
        assert score_basic_content("Title", "https://img/1.jpg", None) == 20

    def test_empty_title(self):
        assert score_basic_content("", "https://img/1.jpg", 29.99) == 20

    def test_all_missing(self):
        assert score_basic_content(None, None, None) == 0


# ═══════════════════════════════════════════════════════════════════════════
#  score_content_completeness
# ═══════════════════════════════════════════════════════════════════════════

class TestScoreContentCompleteness:
    def test_full_content(self):
        # title(4) + 5*2(10) + desc(3) + kw(3) = 20
        assert score_content_completeness(
            has_title=True, bullet_count=5,
            has_description=True, has_keywords=True,
        ) == 20

    def test_empty(self):
        assert score_content_completeness() == 0

    def test_title_and_bullets_only(self):
        assert score_content_completeness(has_title=True, bullet_count=3) == 10

    def test_many_bullets_capped(self):
        # More than 5 bullets still capped at 10 pts
        assert score_content_completeness(bullet_count=10) == 10

    def test_no_bullets(self):
        assert score_content_completeness(has_title=True) == 4


# ═══════════════════════════════════════════════════════════════════════════
#  compute_health_score
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeHealthScore:
    def test_perfect_listing(self):
        # ACTIVE(20) + no_issues(15) + not_suppressed(15) + content(30) + completeness(20)
        score = compute_health_score(
            listing_status="ACTIVE",
            has_issues=False, issues_severity=None,
            is_suppressed=False,
            title="Great Product", image_url="https://img.jpg",
            current_price=29.99, content_completeness_pts=20,
        )
        assert score == 100

    def test_worst_listing(self):
        score = compute_health_score(
            listing_status="SUPPRESSED",
            has_issues=True, issues_severity="ERROR",
            is_suppressed=True,
            title=None, image_url=None,
            current_price=None, content_completeness_pts=0,
        )
        assert score == 0

    def test_typical_listing(self):
        # ACTIVE(20) + warning(8) + not_suppressed(15) + title+price(20) + some_content(10)
        score = compute_health_score(
            listing_status="ACTIVE",
            has_issues=True, issues_severity="WARNING",
            is_suppressed=False,
            title="Title", image_url=None,
            current_price=19.99, content_completeness_pts=10,
        )
        assert score == 73

    def test_completeness_capped_at_20(self):
        score = compute_health_score(
            listing_status="ACTIVE",
            has_issues=False, issues_severity=None,
            is_suppressed=False,
            title="T", image_url="I", current_price=1.0,
            content_completeness_pts=50,  # should be capped to 20
        )
        assert score == 100


# ═══════════════════════════════════════════════════════════════════════════
#  _score_fields_json
# ═══════════════════════════════════════════════════════════════════════════

class TestScoreFieldsJson:
    def test_full_fields(self):
        fields = json.dumps({
            "title": "Great Product",
            "bullets": ["b1", "b2", "b3", "b4", "b5"],
            "description": "A fine product.",
            "keywords": "keyword1 keyword2",
        })
        assert _score_fields_json(fields) == 20

    def test_empty_json(self):
        assert _score_fields_json("{}") == 0

    def test_invalid_json(self):
        assert _score_fields_json("not json") == 0

    def test_none_input(self):
        assert _score_fields_json(None) == 0

    def test_partial_fields(self):
        fields = json.dumps({"title": "T", "bullets": ["b1", "b2"]})
        assert _score_fields_json(fields) == 8  # title(4) + 2*2(4)


# ═══════════════════════════════════════════════════════════════════════════
#  _to_str + detect_and_record_diffs
# ═══════════════════════════════════════════════════════════════════════════

class TestToStr:
    def test_none(self):
        assert _to_str(None) == ""

    def test_bool_true(self):
        assert _to_str(True) == "1"

    def test_bool_false(self):
        assert _to_str(False) == "0"

    def test_string(self):
        assert _to_str(" hello ") == "hello"

    def test_number(self):
        assert _to_str(42.5) == "42.5"


class _FakeCursor:
    """Minimal cursor mock that records execute calls."""
    def __init__(self):
        self.executed: list[tuple] = []

    def execute(self, sql: str, params=None):
        self.executed.append((sql, params))


class TestDetectAndRecordDiffs:
    def test_no_changes(self):
        cur = _FakeCursor()
        count = detect_and_record_diffs(
            cur, "SKU-A", "MKT-1",
            {"title": "Old", "current_price": "10.0"},
            {"title": "Old", "current_price": "10.0"},
            change_source="test",
        )
        assert count == 0
        assert len(cur.executed) == 0

    def test_title_change(self):
        cur = _FakeCursor()
        count = detect_and_record_diffs(
            cur, "SKU-A", "MKT-1",
            {"title": "Old Title"},
            {"title": "New Title"},
            change_source="report",
        )
        assert count == 1
        assert len(cur.executed) == 1
        assert "acc_listing_field_diff" in cur.executed[0][0]

    def test_multiple_changes(self):
        cur = _FakeCursor()
        count = detect_and_record_diffs(
            cur, "SKU-A", "MKT-1",
            {"title": "Old", "brand": "Old Brand", "current_price": "10.0"},
            {"title": "New", "brand": "New Brand", "current_price": "20.0"},
            change_source="event",
        )
        assert count == 3

    def test_none_new_value_skipped(self):
        cur = _FakeCursor()
        count = detect_and_record_diffs(
            cur, "SKU-A", "MKT-1",
            {"title": "Old"},
            {"title": None},
            change_source="test",
        )
        assert count == 0

    def test_bool_diff(self):
        cur = _FakeCursor()
        count = detect_and_record_diffs(
            cur, "SKU-A", "MKT-1",
            {"is_suppressed": 0},
            {"is_suppressed": True},
            change_source="test",
        )
        assert count == 1


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 10 tests — snapshot helpers, score ranges, edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestHealthScoreRanges:
    """Verify health score is always within [0, 100]."""

    def test_minimum_bound(self):
        score = compute_health_score(
            listing_status="DELETED",
            has_issues=True, issues_severity="ERROR",
            is_suppressed=True,
            title=None, image_url=None,
            current_price=None, content_completeness_pts=0,
        )
        assert 0 <= score <= 100
        assert score == 0

    def test_maximum_bound(self):
        score = compute_health_score(
            listing_status="ACTIVE",
            has_issues=False, issues_severity=None,
            is_suppressed=False,
            title="T", image_url="I",
            current_price=1.0, content_completeness_pts=20,
        )
        assert 0 <= score <= 100
        assert score == 100

    def test_mid_range(self):
        # INACTIVE(10) + no_issues(15) + not_suppressed(15) + title(10) + 0
        score = compute_health_score(
            listing_status="INACTIVE",
            has_issues=False, issues_severity=None,
            is_suppressed=False,
            title="Title", image_url=None,
            current_price=None, content_completeness_pts=0,
        )
        assert score == 50

    def test_all_status_values(self):
        """Every status value should produce a score in [0, 100]."""
        for status in ["ACTIVE", "INACTIVE", "INCOMPLETE", "SUPPRESSED", "DELETED", "UNKNOWN", None]:
            score = compute_health_score(
                listing_status=status,
                has_issues=False, issues_severity=None,
                is_suppressed=False,
                title="T", image_url="I",
                current_price=1.0, content_completeness_pts=10,
            )
            assert 0 <= score <= 100, f"Out of range for status={status}: {score}"


class TestContentCompletenessEdgeCases:
    def test_negative_bullets_treated_as_zero(self):
        assert score_content_completeness(bullet_count=-1) == 0

    def test_zero_bullets(self):
        assert score_content_completeness(bullet_count=0) == 0

    def test_exactly_five_bullets(self):
        # title(4) + 5*2(10) + desc(3) + kw(3) = 20
        assert score_content_completeness(
            has_title=True, bullet_count=5,
            has_description=True, has_keywords=True,
        ) == 20


class TestFieldsJsonEdgeCases:
    def test_dict_input(self):
        """Accept dict directly (not just JSON string)."""
        fields = {"title": "T", "bullets": ["b1"], "description": "D"}
        assert _score_fields_json(fields) == 9  # title(4) + 1*2(2) + desc(3)

    def test_empty_bullets_list(self):
        fields = json.dumps({"title": "T", "bullets": []})
        assert _score_fields_json(fields) == 4  # title only

    def test_non_list_bullets(self):
        fields = json.dumps({"title": "T", "bullets": "not a list"})
        assert _score_fields_json(fields) == 4  # bullets ignored, title only
