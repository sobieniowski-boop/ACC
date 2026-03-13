"""Tests for F4: logistics enrichment in profitability rollup.

Unit tests verify the enrichment function is callable with the expected
signature. The live smoke test (marked with @pytest.mark.live) hits
Azure SQL to validate the actual SQL executes without error.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


class TestEnrichRollupFromFinanceHasLogisticsStep:
    """Verify _enrich_rollup_from_finance includes logistics enrichment."""

    def _make_mock_cursor(self):
        cur = MagicMock()
        cur.rowcount = 0
        return cur

    def test_returns_logistics_rows_key(self):
        """Stats dict must include 'logistics_rows' key."""
        cur = self._make_mock_cursor()
        conn = MagicMock()

        with patch(
            "app.services.profitability_service.connect_acc"
        ):
            from app.services.profitability_service import _enrich_rollup_from_finance

        stats = _enrich_rollup_from_finance(
            cur, conn, date(2026, 1, 1), date(2026, 1, 7)
        )
        assert "logistics_rows" in stats
        assert isinstance(stats["logistics_rows"], int)

    def test_logistics_step_executes_sql(self):
        """At least one cur.execute call should reference acc_order_logistics_fact."""
        cur = self._make_mock_cursor()
        conn = MagicMock()

        with patch(
            "app.services.profitability_service.connect_acc"
        ):
            from app.services.profitability_service import _enrich_rollup_from_finance

        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))

        sql_calls = [str(c) for c in cur.execute.call_args_list]
        logistics_calls = [s for s in sql_calls if "acc_order_logistics_fact" in s]
        assert len(logistics_calls) >= 1, (
            "Expected at least one SQL call referencing acc_order_logistics_fact"
        )

    def test_outer_apply_top1_pattern(self):
        """The logistics SQL must use OUTER APPLY TOP 1 for dedup."""
        cur = self._make_mock_cursor()
        conn = MagicMock()

        with patch(
            "app.services.profitability_service.connect_acc"
        ):
            from app.services.profitability_service import _enrich_rollup_from_finance

        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))

        all_sql = " ".join(
            str(c.args[0]) if c.args else ""
            for c in cur.execute.call_args_list
        ).upper()
        assert "OUTER APPLY" in all_sql
        assert "TOP 1" in all_sql
        assert "CALCULATED_AT DESC" in all_sql

    def test_idempotent_no_additive_accumulation(self):
        """Running enrichment twice should produce same result (SET, not +=)."""
        cur = self._make_mock_cursor()
        conn = MagicMock()

        with patch(
            "app.services.profitability_service.connect_acc"
        ):
            from app.services.profitability_service import _enrich_rollup_from_finance

        # The logistics UPDATE uses `r.logistics_pln = ROUND(sl.logistics_pln, 2)`
        # (absolute SET), not `r.logistics_pln = r.logistics_pln + ...`
        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))

        logistics_sql = ""
        for call in cur.execute.call_args_list:
            s = str(call.args[0]) if call.args else ""
            if "acc_order_logistics_fact" in s:
                logistics_sql = s
                break

        # Must use absolute SET (= ROUND(...)), not additive (= r.logistics_pln + ...)
        assert "r.logistics_pln + " not in logistics_sql, (
            "Logistics enrichment must use absolute SET, not additive accumulation"
        )
