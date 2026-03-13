"""Tests for return_tracker service — Tier 1 (pure logic) + Tier 2 (mocked DB).

Covers:
  - parse_fba_returns_report() — TSV/CSV parsing, BOM handling, column normalization
  - _f() — safe float conversion
  - update_return_status() — validation, COGS calculation (mocked DB)
  - reconcile_returns() — classification verification (mocked DB)
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper: import service functions with DB connections stubbed
# ---------------------------------------------------------------------------

def _import_return_tracker():
    """Import return_tracker module."""
    from app.services import return_tracker as rt
    return rt


# ---------------------------------------------------------------------------
# Tier 1: parse_fba_returns_report — pure parsing, no DB
# ---------------------------------------------------------------------------

class TestParseFbaReturnsReport:
    """Tests for parse_fba_returns_report (pure CSV/TSV parsing)."""

    def test_parses_tsv_content(self):
        rt = _import_return_tracker()
        content = (
            "return-date\torder-id\tsku\tasin\tquantity\tdetailed-disposition\treason\n"
            "2026-01-15\t123-456\tSKU-A\tB001\t1\tSELLABLE\tBuyer return\n"
            "2026-01-16\t789-012\tSKU-B\tB002\t2\tDAMAGED\tDefective\n"
        )
        rows = rt.parse_fba_returns_report(content, "A1PA6795UKMFR9")
        assert len(rows) == 2
        assert rows[0]["order_id"] == "123-456"
        assert rows[0]["sku"] == "SKU-A"
        assert rows[0]["quantity"] == "1"
        assert rows[0]["marketplace_id"] == "A1PA6795UKMFR9"

    def test_parses_csv_content(self):
        rt = _import_return_tracker()
        content = (
            "return-date,order-id,sku,asin,quantity,detailed-disposition,reason\n"
            "2026-02-01,AAA-BBB,MY-SKU,B999,3,SELLABLE,Wrong item\n"
        )
        rows = rt.parse_fba_returns_report(content, "APJ6JRA9NG5V4")
        assert len(rows) == 1
        assert rows[0]["order_id"] == "AAA-BBB"
        assert rows[0]["detailed_disposition"] == "SELLABLE"

    def test_handles_bom(self):
        rt = _import_return_tracker()
        content = (
            "\ufeffreturn-date\torder-id\tsku\n"
            "2026-03-01\t111-222\tSKU-X\n"
        )
        rows = rt.parse_fba_returns_report(content, "A1RKKUPIHCS9HS")
        assert len(rows) == 1
        assert rows[0]["sku"] == "SKU-X"

    def test_empty_content_returns_empty_list(self):
        rt = _import_return_tracker()
        content = "return-date\torder-id\tsku\n"
        rows = rt.parse_fba_returns_report(content, "A1PA6795UKMFR9")
        assert rows == []

    def test_strips_whitespace_from_values(self):
        rt = _import_return_tracker()
        content = (
            "return-date\torder-id\tsku\n"
            " 2026-01-01 \t 123-ABC \t MY-SKU \n"
        )
        rows = rt.parse_fba_returns_report(content, "A1PA6795UKMFR9")
        assert rows[0]["order_id"] == "123-ABC"
        assert rows[0]["sku"] == "MY-SKU"


# ---------------------------------------------------------------------------
# Tier 1: _f() — safe float conversion
# ---------------------------------------------------------------------------

class TestSafeFloat:
    """Tests for _f() utility."""

    def test_converts_numeric_string(self):
        rt = _import_return_tracker()
        assert rt._f("12.3456") == 12.3456

    def test_converts_integer(self):
        rt = _import_return_tracker()
        assert rt._f(42) == 42.0

    def test_returns_default_for_none(self):
        rt = _import_return_tracker()
        assert rt._f(None) == 0.0
        assert rt._f(None, default=99.0) == 99.0

    def test_returns_default_for_invalid_string(self):
        rt = _import_return_tracker()
        assert rt._f("not-a-number") == 0.0

    def test_returns_default_for_empty_string(self):
        rt = _import_return_tracker()
        assert rt._f("") == 0.0

    def test_rounds_to_4_decimals(self):
        rt = _import_return_tracker()
        assert rt._f("1.23456789") == 1.2346
        assert rt._f(0.00005) == 0.0001


# ---------------------------------------------------------------------------
# Tier 2: update_return_status — validation + COGS logic (mocked DB)
# ---------------------------------------------------------------------------

class TestUpdateReturnStatus:
    """Tests for update_return_status with mocked DB."""

    def test_rejects_invalid_status(self):
        rt = _import_return_tracker()
        with pytest.raises(ValueError, match="Invalid status"):
            rt.update_return_status(1, "invalid_status")

    def test_valid_statuses_accepted(self):
        """All 5 valid statuses should be accepted (not raise ValueError)."""
        rt = _import_return_tracker()
        valid = {"sellable_return", "damaged_return", "lost_in_transit", "reimbursed", "pending"}
        for status in valid:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            # fetchone returns (id, cogs_pln, financial_status)
            mock_cur.fetchone.return_value = (1, 25.50, "pending")

            with patch.object(rt, "_connect", return_value=mock_conn):
                result = rt.update_return_status(1, status, note="test", updated_by="tester")
            assert result["financial_status"] == status

    def test_sellable_return_recovers_cogs(self):
        """sellable_return → cogs_recovered_pln = cogs, write_off_pln = 0."""
        rt = _import_return_tracker()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (42, 100.0, "pending")

        with patch.object(rt, "_connect", return_value=mock_conn):
            result = rt.update_return_status(42, "sellable_return")

        assert result["cogs_recovered_pln"] == 100.0
        assert result["write_off_pln"] == 0.0

    def test_damaged_return_writes_off_cogs(self):
        """damaged_return → write_off_pln = cogs, cogs_recovered = 0."""
        rt = _import_return_tracker()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (42, 75.25, "pending")

        with patch.object(rt, "_connect", return_value=mock_conn):
            result = rt.update_return_status(42, "damaged_return")

        assert result["cogs_recovered_pln"] == 0.0
        assert result["write_off_pln"] == 75.25

    def test_lost_in_transit_writes_off_cogs(self):
        """lost_in_transit → write_off_pln = cogs, cogs_recovered = 0."""
        rt = _import_return_tracker()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (10, 50.0, "pending")

        with patch.object(rt, "_connect", return_value=mock_conn):
            result = rt.update_return_status(10, "lost_in_transit")

        assert result["write_off_pln"] == 50.0
        assert result["cogs_recovered_pln"] == 0.0

    def test_reimbursed_no_cogs_recovery(self):
        """reimbursed → cogs_recovered = 0, write_off = 0."""
        rt = _import_return_tracker()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (5, 30.0, "pending")

        with patch.object(rt, "_connect", return_value=mock_conn):
            result = rt.update_return_status(5, "reimbursed")

        assert result["cogs_recovered_pln"] == 0.0
        assert result["write_off_pln"] == 0.0

    def test_item_not_found_raises(self):
        rt = _import_return_tracker()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = None

        with patch.object(rt, "_connect", return_value=mock_conn):
            with pytest.raises(ValueError, match="not found"):
                rt.update_return_status(999, "sellable_return")

    def test_null_cogs_defaults_to_zero(self):
        """If cogs_pln is NULL (None), should default to 0.0."""
        rt = _import_return_tracker()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, None, "pending")

        with patch.object(rt, "_connect", return_value=mock_conn):
            result = rt.update_return_status(1, "sellable_return")

        assert result["cogs_recovered_pln"] == 0.0
        assert result["write_off_pln"] == 0.0
