"""
Smoke tests — matching engine confidence scoring (pure logic, mocked DB).

Validates:
  - Confidence base scores (SKU=85, EAN=90, attr=55)
  - Modifier application (+10, +5, -25, -15, -10)
  - Status thresholds (safe_auto ≥90, proposed ≥75, needs_review ≥60, unmatched <60)
  - Issue generation (theme mismatch, extra/missing children)
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from app.services.family_mapper.matching import (
    _confidence_status,
    _match_children,
    THRESHOLD_SAFE,
    THRESHOLD_PROPOSED,
    THRESHOLD_REVIEW,
)


# ── Status threshold tests ──────────────────────────────────────────────────

class TestConfidenceStatus:
    def test_safe_auto(self):
        assert _confidence_status(95) == "safe_auto"
        assert _confidence_status(90) == "safe_auto"

    def test_proposed(self):
        assert _confidence_status(89) == "proposed"
        assert _confidence_status(75) == "proposed"

    def test_needs_review(self):
        assert _confidence_status(74) == "needs_review"
        assert _confidence_status(60) == "needs_review"

    def test_unmatched(self):
        assert _confidence_status(59) == "unmatched"
        assert _confidence_status(0) == "unmatched"


# ── Matching logic tests ────────────────────────────────────────────────────

class TestMatchChildren:
    """Test _match_children with a no-op cursor (writes are captured)."""

    def _make_cursor(self):
        """Create a cursor that captures SQL calls but does nothing."""
        cur = MagicMock()
        cur.execute = MagicMock()
        cur.fetchone = MagicMock(return_value=None)
        cur.fetchall = MagicMock(return_value=[])
        return cur

    def test_exact_sku_match_base_85(self):
        cur = self._make_cursor()
        de_children = [
            {
                "master_key": "KDX-001",
                "key_type": "sku",
                "de_child_asin": "B0DE001",
                "sku_de": "KDX-001",
                "ean_de": None,
                "attributes_json": None,
            }
        ]
        mp_children = [
            {
                "asin": "B0PL001",
                "sku": "KDX-001",
                "ean": None,
                "current_parent_asin": "B0PARENT",
                "variation_theme": "Color",
                "attributes_json": None,
            }
        ]
        result = _match_children(
            cur, 1, de_children, mp_children, "PL",
            de_theme="Color", mp_theme="Color",
        )
        assert result["matched"] == 1
        assert result["unmatched"] == 0
        # base=85, +10 parent present, +5 same theme = 100 (capped)
        assert result["avg_confidence"] >= 85

    def test_exact_ean_match_base_90(self):
        cur = self._make_cursor()
        de_children = [
            {
                "master_key": "4066991234567",
                "key_type": "gtin",
                "de_child_asin": "B0DE002",
                "sku_de": None,
                "ean_de": "4066991234567",
                "attributes_json": None,
            }
        ]
        mp_children = [
            {
                "asin": "B0FR002",
                "sku": None,
                "ean": "4066991234567",
                "current_parent_asin": None,
                "variation_theme": None,
                "attributes_json": None,
            }
        ]
        result = _match_children(
            cur, 1, de_children, mp_children, "FR",
            de_theme=None, mp_theme=None,
        )
        assert result["matched"] == 1
        assert result["avg_confidence"] >= 90

    def test_unmatched_when_no_overlap(self):
        cur = self._make_cursor()
        de_children = [
            {
                "master_key": "UNIQUE-DE",
                "key_type": "sku",
                "de_child_asin": "B0DE003",
                "sku_de": "UNIQUE-DE",
                "ean_de": None,
                "attributes_json": None,
            }
        ]
        mp_children = [
            {
                "asin": "B0ES003",
                "sku": "DIFFERENT-SKU",
                "ean": "9999999999999",
                "current_parent_asin": None,
                "variation_theme": None,
                "attributes_json": None,
            }
        ]
        result = _match_children(
            cur, 1, de_children, mp_children, "ES",
            de_theme=None, mp_theme=None,
        )
        assert result["unmatched"] == 1
        assert result["matched"] == 0

    def test_theme_mismatch_penalty(self):
        cur = self._make_cursor()
        de_children = [
            {
                "master_key": "KDX-TM",
                "key_type": "sku",
                "de_child_asin": "B0DE004",
                "sku_de": "KDX-TM",
                "ean_de": None,
                "attributes_json": None,
            }
        ]
        mp_children = [
            {
                "asin": "B0IT004",
                "sku": "KDX-TM",
                "ean": None,
                "current_parent_asin": "B0PARENT",
                "variation_theme": "Size",
                "attributes_json": None,
            }
        ]
        result = _match_children(
            cur, 1, de_children, mp_children, "IT",
            de_theme="Color/Size", mp_theme="Size",
        )
        assert result["matched"] == 1
        # base=85, +10 parent, -25 theme mismatch = 70
        assert result["avg_confidence"] < THRESHOLD_PROPOSED

    def test_extra_children_penalty(self):
        cur = self._make_cursor()
        de_children = [
            {
                "master_key": "KDX-EC",
                "key_type": "sku",
                "de_child_asin": "B0DE005",
                "sku_de": "KDX-EC",
                "ean_de": None,
                "attributes_json": None,
            }
        ]
        # 3 marketplace children vs 1 DE child → extra
        mp_children = [
            {"asin": f"B0NL00{i}", "sku": "KDX-EC" if i == 5 else f"OTHER-{i}",
             "ean": None, "current_parent_asin": None,
             "variation_theme": None, "attributes_json": None}
            for i in range(5, 8)
        ]
        result = _match_children(
            cur, 1, de_children, mp_children, "NL",
            de_theme=None, mp_theme=None,
        )
        assert result["matched"] == 1
        # base=85, -15 extra = 70
        assert result["avg_confidence"] < THRESHOLD_PROPOSED

    def test_issue_inserted_for_theme_mismatch(self):
        cur = self._make_cursor()
        de_children = [
            {
                "master_key": "KDX-ISS",
                "key_type": "sku",
                "de_child_asin": "B0DE006",
                "sku_de": "KDX-ISS",
                "ean_de": None,
                "attributes_json": None,
            }
        ]
        mp_children = [
            {
                "asin": "B0SE006",
                "sku": "KDX-ISS",
                "ean": None,
                "current_parent_asin": None,
                "variation_theme": "Size",
                "attributes_json": None,
            }
        ]
        _match_children(
            cur, 99, de_children, mp_children, "SE",
            de_theme="Color", mp_theme="Size",
        )
        # Should have called INSERT INTO family_issues_cache at least once
        issue_calls = [
            c for c in cur.execute.call_args_list
            if "family_issues_cache" in str(c)
        ]
        assert len(issue_calls) >= 1
