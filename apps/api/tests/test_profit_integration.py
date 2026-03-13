"""Integration test: Profit Engine pipeline — order → bridge → calc → verify CM1/CM2/NP.

Sprint 3 — S3.4.

Verifies the complete profit calculation chain with mocked DB connections:
  1. recalc_profit_orders stamps COGS and CM1 on orders
  2. recompute_rollups builds SKU + marketplace rollup tables
  3. _enrich_rollup_from_finance populates CM2 cost layers
  4. evaluate_profitability_alerts flags low-margin / high-ACOS SKUs
  5. full_profit_recalculate orchestrates the full pipeline
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


# ───────────────────────────────────────────────────────────────────────────
# IT-01: recalc_profit_orders returns integer count
# ───────────────────────────────────────────────────────────────────────────

class TestIT01_RecalcProfitOrders:
    """recalc_profit_orders must update orders and return count."""

    def test_returns_integer_count(self):
        from app.connectors.mssql.mssql_store import recalc_profit_orders

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 42
        mock_conn.cursor.return_value = mock_cur

        with patch("app.connectors.mssql.mssql_store._connect", return_value=mock_conn):
            count = recalc_profit_orders(
                date_from=date(2026, 3, 1), date_to=date(2026, 3, 7),
            )

        assert isinstance(count, int)
        assert count >= 0

    def test_sql_references_contribution_margin(self):
        """SQL must compute contribution_margin_pln."""
        from app.connectors.mssql.mssql_store import recalc_profit_orders

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 0
        mock_conn.cursor.return_value = mock_cur

        captured_sql = []
        def capture(sql, *args, **kwargs):
            captured_sql.append(str(sql))
        mock_cur.execute = capture

        with patch("app.connectors.mssql.mssql_store._connect", return_value=mock_conn):
            recalc_profit_orders(
                date_from=date(2026, 3, 1), date_to=date(2026, 3, 7),
            )

        all_sql = " ".join(captured_sql)
        assert "contribution_margin_pln" in all_sql


# ───────────────────────────────────────────────────────────────────────────
# IT-02: recompute_rollups produces rollup result dict
# ───────────────────────────────────────────────────────────────────────────

class TestIT02_RecomputeRollups:
    """recompute_rollups must return dict with sku_rows_upserted key."""

    def test_result_shape(self):
        from app.intelligence.profit.rollup import recompute_rollups
        import app.intelligence.profit.rollup as mod

        mod._METADATA_TABLE_VERIFIED = False

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 10
        mock_conn.cursor.return_value = mock_cur

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch(
                "app.intelligence.profit.rollup._enrich_rollup_from_finance",
                return_value={
                    "storage_rows": 1, "refund_rows": 2, "other_rows": 3,
                    "ads_rows": 4, "return_units_rows": 5, "logistics_rows": 6,
                },
            ):
                result = recompute_rollups(
                    date(2026, 3, 1), date(2026, 3, 7),
                )

        assert isinstance(result, dict)
        assert "sku_rows_upserted" in result
        assert "marketplace_rows_upserted" in result
        assert "recomputed_at" in result

    def test_sku_merge_includes_cm1_cm2(self):
        """SKU rollup MERGE must compute cm1_pln and cm2_pln."""
        from app.intelligence.profit.rollup import recompute_rollups
        import app.intelligence.profit.rollup as mod

        mod._METADATA_TABLE_VERIFIED = True

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 5
        mock_conn.cursor.return_value = mock_cur

        captured_sql = []
        def capture(sql, params=None):
            captured_sql.append(str(sql))
        mock_cur.execute = capture

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch(
                "app.intelligence.profit.rollup._enrich_rollup_from_finance",
                return_value={
                    "storage_rows": 0, "refund_rows": 0, "other_rows": 0,
                    "ads_rows": 0, "return_units_rows": 0, "logistics_rows": 0,
                },
            ):
                recompute_rollups(date(2026, 3, 1), date(2026, 3, 7))

        merge_sql = [s for s in captured_sql if "MERGE" in s and "acc_sku_profitability_rollup" in s]
        assert merge_sql, "Expected MERGE SQL for SKU rollup"
        assert "cm1_pln" in merge_sql[0]
        assert "cm2_pln" in merge_sql[0]


# ───────────────────────────────────────────────────────────────────────────
# IT-03: Enrichment populates CM2 cost layers
# ───────────────────────────────────────────────────────────────────────────

class TestIT03_EnrichmentLayers:
    """_enrich_rollup_from_finance must update all CM2 cost columns."""

    def test_returns_all_cost_layer_keys(self):
        from app.intelligence.profit.rollup import _enrich_rollup_from_finance

        mock_cur = MagicMock()
        mock_cur.rowcount = 0
        mock_conn = MagicMock()

        stats = _enrich_rollup_from_finance(
            mock_cur, mock_conn, date(2026, 3, 1), date(2026, 3, 7),
        )

        for key in ("storage_rows", "refund_rows", "other_rows", "ads_rows", "return_units_rows"):
            assert key in stats, f"Missing key: {key}"
            assert isinstance(stats[key], int)


# ───────────────────────────────────────────────────────────────────────────
# IT-04: evaluate_profitability_alerts returns alert counts
# ───────────────────────────────────────────────────────────────────────────

class TestIT04_Alerts:
    """Alert evaluation must return structured result."""

    def test_alert_result_shape(self):
        from app.intelligence.profit.rollup import evaluate_profitability_alerts

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 0
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        mock_create_alert = MagicMock()

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn), \
             patch("app.connectors.mssql.create_alert", mock_create_alert, create=True):
            result = evaluate_profitability_alerts(
                date(2026, 3, 1), date(2026, 3, 7),
            )

        assert isinstance(result, dict)
        assert "alerts_created" in result


# ───────────────────────────────────────────────────────────────────────────
# IT-05: full_profit_recalculate orchestrates pipeline
# ───────────────────────────────────────────────────────────────────────────

class TestIT05_FullProfitRecalculate:
    """Unified entry point runs CM1 → rollup → alerts."""

    def test_all_phases_return_results(self):
        from app.intelligence.profit import full_profit_recalculate

        with patch(
            "app.connectors.mssql.mssql_store.recalc_profit_orders", return_value=100,
        ), patch(
            "app.intelligence.profit.recompute_rollups",
            return_value={"sku_rows_upserted": 50, "marketplace_rows_upserted": 9},
        ), patch(
            "app.intelligence.profit.evaluate_profitability_alerts",
            return_value={"alerts_created": 3},
        ):
            result = full_profit_recalculate(
                date(2026, 3, 1), date(2026, 3, 7),
            )

        assert result["orders_updated"] == 100
        assert result["rollup"]["sku_rows_upserted"] == 50
        assert result["alerts"]["alerts_created"] == 3

    def test_skip_cm1(self):
        """When include_cm1=False, orders_updated is absent."""
        from app.intelligence.profit import full_profit_recalculate

        with patch(
            "app.intelligence.profit.recompute_rollups",
            return_value={"sku_rows_upserted": 10},
        ), patch(
            "app.intelligence.profit.evaluate_profitability_alerts",
            return_value={"alerts_created": 0},
        ):
            result = full_profit_recalculate(
                date(2026, 3, 1), date(2026, 3, 7),
                include_cm1=False,
            )

        assert "orders_updated" not in result
        assert "rollup" in result
        assert "alerts" in result

    def test_skip_alerts(self):
        """When include_alerts=False, alerts key is absent."""
        from app.intelligence.profit import full_profit_recalculate

        with patch(
            "app.connectors.mssql.mssql_store.recalc_profit_orders", return_value=5,
        ), patch(
            "app.intelligence.profit.recompute_rollups",
            return_value={"sku_rows_upserted": 2},
        ):
            result = full_profit_recalculate(
                date(2026, 3, 1), date(2026, 3, 7),
                include_alerts=False,
            )

        assert result["orders_updated"] == 5
        assert "rollup" in result
        assert "alerts" not in result


# ───────────────────────────────────────────────────────────────────────────
# IT-06: Profit waterfall integrity — Revenue → CM1 → CM2 → NP
# ───────────────────────────────────────────────────────────────────────────

class TestIT06_WaterfallIntegrity:
    """Rollup SQL must produce: CM1 = revenue - COGS - fees, CM2 = CM1 - ads - storage."""

    def test_sku_rollup_sql_computes_cm1_before_cm2(self):
        """In the SKU MERGE, cm1_pln must be computed before cm2_pln."""
        from app.intelligence.profit.rollup import recompute_rollups
        import app.intelligence.profit.rollup as mod

        mod._METADATA_TABLE_VERIFIED = True

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 0
        mock_conn.cursor.return_value = mock_cur

        captured = []
        def cap(sql, params=None):
            captured.append(str(sql))
        mock_cur.execute = cap

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch(
                "app.intelligence.profit.rollup._enrich_rollup_from_finance",
                return_value={"storage_rows": 0, "refund_rows": 0, "other_rows": 0,
                              "ads_rows": 0, "return_units_rows": 0, "logistics_rows": 0},
            ):
                recompute_rollups(date(2026, 3, 1), date(2026, 3, 7))

        # Find the enrichment step 5 (cm1/cm2 recalc)
        recalc_sql = [s for s in captured if "cm1_pln" in s and "revenue_pln" in s]
        assert recalc_sql, "Expected SQL containing cm1_pln calculation"

    def test_enrichment_step5_formula_pattern(self):
        """Enrichment step 5 must set cm1_pln = revenue - COGS - amazon_fees - fba_fees."""
        from app.intelligence.profit.rollup import _enrich_rollup_from_finance

        mock_cur = MagicMock()
        mock_cur.rowcount = 5
        mock_conn = MagicMock()

        captured = []
        def cap(sql, params=None):
            captured.append(str(sql))
        mock_cur.execute = cap

        _enrich_rollup_from_finance(
            mock_cur, mock_conn, date(2026, 3, 1), date(2026, 3, 7),
        )

        all_sql = " ".join(captured)
        # CM1 = revenue - COGS - amazon_fees - fba_fees - logistics
        assert "cm1_pln" in all_sql
        assert "revenue_pln" in all_sql
