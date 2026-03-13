"""Tests for Phase 0 — Revenue GROSS→NET fix in mssql_store.get_profit_by_sku.

Validates:
  P0-01: Revenue uses (item_price - item_tax - promotion_discount) * FX, not item_price * FX
  P0-02: Logistics pro-rata uses NET line values (consistent with revenue)
  P0-03: profit_service.py already computes NET revenue (regression guard)
  P0-04: profit_engine.py line_share_sql & OLT subqueries use NET values
  P0-05: profit.py _OLT_JOIN_SQL template uses NET values
"""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# P0-01: get_profit_by_sku must use NET revenue
# ═══════════════════════════════════════════════════════════════════════════


class TestP0_01_GetProfitBySku_NetRevenue:
    """get_profit_by_sku SQL must subtract item_tax and promotion_discount."""

    def test_revenue_sql_subtracts_tax_and_promo(self):
        """The generated SQL must contain (item_price - item_tax - promotion_discount)."""
        from app.connectors.mssql import mssql_store

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_cur.description = []
        mock_conn.cursor.return_value = mock_cur

        with patch.object(mssql_store, "_connect", return_value=mock_conn):
            mssql_store.get_profit_by_sku(
                date_from=date(2026, 3, 1),
                date_to=date(2026, 3, 7),
            )

        sql = str(mock_cur.execute.call_args[0][0])

        # Revenue must use NET formula
        assert "ol.item_tax" in sql, (
            "P0-01: revenue SQL must reference item_tax for NET deduction"
        )
        assert "ol.promotion_discount" in sql, (
            "P0-01: revenue SQL must reference promotion_discount for NET deduction"
        )
        # Verify the subtraction pattern (item_price - item_tax - promotion_discount)
        assert "ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)" in sql, (
            "P0-01: revenue SQL must use (item_price - item_tax - promotion_discount)"
        )

    def test_revenue_sql_not_gross(self):
        """The generated SQL must NOT use bare item_price * FX for revenue."""
        from app.connectors.mssql import mssql_store

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_cur.description = []
        mock_conn.cursor.return_value = mock_cur

        with patch.object(mssql_store, "_connect", return_value=mock_conn):
            mssql_store.get_profit_by_sku(
                date_from=date(2026, 3, 1),
                date_to=date(2026, 3, 7),
            )

        sql = str(mock_cur.execute.call_args[0][0])

        # The old GROSS pattern was: SUM(ROUND(ISNULL(ol.item_price, 0)\n * ISNULL(fx...
        # Make sure "AS revenue_pln" is preceded by a subtraction, not bare item_price
        revenue_line_idx = sql.find("AS revenue_pln")
        assert revenue_line_idx > 0
        # Extract the full expression before "AS revenue_pln"
        revenue_expr = sql[max(0, revenue_line_idx - 2000):revenue_line_idx]
        assert "item_tax" in revenue_expr, (
            "P0-01: revenue expression must subtract item_tax"
        )
        assert "promotion_discount" in revenue_expr, (
            "P0-01: revenue expression must subtract promotion_discount"
        )


# ═══════════════════════════════════════════════════════════════════════════
# P0-02: Logistics pro-rata must use NET line values
# ═══════════════════════════════════════════════════════════════════════════


class TestP0_02_LogisticsProRata_Net:
    """Logistics allocation and order_line_total subquery must use NET values."""

    def test_order_line_total_subquery_uses_net(self):
        """The olt subquery computing order_line_total must subtract tax & promo."""
        from app.connectors.mssql import mssql_store

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_cur.description = []
        mock_conn.cursor.return_value = mock_cur

        with patch.object(mssql_store, "_connect", return_value=mock_conn):
            mssql_store.get_profit_by_sku(
                date_from=date(2026, 3, 1),
                date_to=date(2026, 3, 7),
            )

        sql = str(mock_cur.execute.call_args[0][0])

        # The olt subquery should reference ol2.item_tax and ol2.promotion_discount
        olt_start = sql.find("OUTER APPLY")
        assert olt_start > 0, "Expected OUTER APPLY subquery for olt"
        olt_section = sql[olt_start:olt_start + 400]

        assert "ol2.item_tax" in olt_section, (
            "P0-02: olt subquery must subtract item_tax for NET order_line_total"
        )
        assert "ol2.promotion_discount" in olt_section, (
            "P0-02: olt subquery must subtract promotion_discount for NET order_line_total"
        )

    def test_logistics_pro_rata_uses_net_line_value(self):
        """The logistics pro-rata CASE must use NET line value, not gross item_price."""
        from app.connectors.mssql import mssql_store

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_cur.description = []
        mock_conn.cursor.return_value = mock_cur

        with patch.object(mssql_store, "_connect", return_value=mock_conn):
            mssql_store.get_profit_by_sku(
                date_from=date(2026, 3, 1),
                date_to=date(2026, 3, 7),
            )

        sql = str(mock_cur.execute.call_args[0][0])

        # Find the logistics CASE expression referencing order_line_total
        logistics_idx = sql.find("AS logistics_pln")
        assert logistics_idx > 0
        logistics_expr = sql[max(0, logistics_idx - 500):logistics_idx]

        # The pro-rata numerator must be NET: (item_price - item_tax - promotion_discount)
        assert "ol.item_tax" in logistics_expr, (
            "P0-02: logistics pro-rata numerator must subtract item_tax"
        )


# ═══════════════════════════════════════════════════════════════════════════
# P0-03: profit_service.py — regression guard for NET revenue
# ═══════════════════════════════════════════════════════════════════════════


class TestP0_03_ProfitService_RegressionGuard:
    """profit_service.calculate_order_profit must continue using NET revenue."""

    @pytest.mark.asyncio
    async def test_revenue_is_net_with_tax_and_promo(self):
        """Revenue = (item_price - item_tax - promotion_discount) * fx_rate."""
        from app.services.profit_service import calculate_order_profit

        line = MagicMock()
        line.cogs_pln = Decimal("10.00")
        line.product_id = None
        line.quantity_shipped = 1
        line.quantity_ordered = 1
        line.fba_fee_pln = Decimal("0")
        line.referral_fee_pln = Decimal("0")
        line.item_price = Decimal("120.00")   # GROSS price
        line.item_tax = Decimal("20.00")       # VAT
        line.promotion_discount = Decimal("10.00")  # coupon

        order = MagicMock()
        order.currency = "EUR"
        order.order_total = Decimal("120.00")
        order.purchase_date = date(2026, 3, 1)
        order.lines = [line]
        order.ads_cost_pln = Decimal("0")
        order.amazon_order_id = "TEST-P0-001"
        order.logistics_pln = Decimal("0")

        db = AsyncMock()
        fx_rate = 4.30

        with patch("app.services.profit_service.resolve_profit_logistics_pln",
                    new_callable=AsyncMock, return_value=0.0):
            await calculate_order_profit(db, order, fx_rate=fx_rate)

        # (120 - 20 - 10) * 4.30 = 90 * 4.30 = 387.00
        expected = round(90.0 * 4.30, 2)
        assert order.revenue_pln == expected, (
            f"P0-03: Expected NET revenue {expected}, got {order.revenue_pln}"
        )
        # Must NOT be gross: 120 * 4.30 = 516.00
        assert order.revenue_pln != round(120.0 * 4.30, 2), (
            "P0-03: Revenue must NOT be GROSS (item_price * fx)"
        )

    @pytest.mark.asyncio
    async def test_cm_formula_matches_expected(self):
        """CM = revenue - cogs - fees - ads - logistics."""
        from app.services.profit_service import calculate_order_profit

        line = MagicMock()
        line.cogs_pln = Decimal("30.00")
        line.product_id = None
        line.quantity_shipped = 2
        line.quantity_ordered = 2
        line.fba_fee_pln = Decimal("3.00")
        line.referral_fee_pln = Decimal("5.00")
        line.item_price = Decimal("100.00")
        line.item_tax = Decimal("15.00")
        line.promotion_discount = Decimal("0.00")

        order = MagicMock()
        order.currency = "PLN"
        order.order_total = Decimal("200.00")
        order.purchase_date = date(2026, 3, 1)
        order.lines = [line]
        order.ads_cost_pln = Decimal("12.00")
        order.amazon_order_id = "TEST-P0-002"
        order.logistics_pln = Decimal("8.00")
        order.shipping_surcharge_pln = None
        order.promo_order_fee_pln = None
        order.refund_commission_pln = None

        db = AsyncMock()

        with patch("app.services.profit_service.resolve_profit_logistics_pln",
                    new_callable=AsyncMock, return_value=8.0):
            await calculate_order_profit(db, order, fx_rate=1.0)

        # revenue = (100 - 15 - 0) * 1.0 = 85.00
        assert order.revenue_pln == 85.00
        # cogs = 30 * 2 = 60
        assert order.cogs_pln == 60.00
        # fees = (3 + 5) * 2 = 16
        assert order.amazon_fees_pln == 16.00
        # cm = 85 - 60 - 16 - 12(ads) - 8(logistics) = -11.00
        assert order.contribution_margin_pln == -11.00


# ═══════════════════════════════════════════════════════════════════════════
# P0-04: profit_engine.py — all OLT and line_share must use NET values
# ═══════════════════════════════════════════════════════════════════════════


class TestP0_04_ProfitEngine_NetConsistency:
    """profit_engine.py OLT subqueries and line_share must use NET, not GROSS."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        engine_path = (
            Path(__file__).resolve().parents[1]
            / "app" / "intelligence" / "profit" / "query.py"
        )
        self.source = engine_path.read_text(encoding="utf-8")

    def test_no_gross_only_olt_subqueries(self):
        """Every OUTER APPLY computing order_line_total must subtract tax & promo."""
        # Find all OUTER APPLY blocks that produce order_line_total
        olt_blocks = re.findall(
            r"OUTER\s+APPLY\s*\(.*?order_line_total.*?\)\s*olt",
            self.source,
            re.DOTALL | re.IGNORECASE,
        )
        assert len(olt_blocks) > 0, "Expected at least one OLT OUTER APPLY block"

        for i, block in enumerate(olt_blocks):
            # Each block must reference item_tax and promotion_discount
            assert "item_tax" in block, (
                f"P0-04: OLT block #{i+1} missing item_tax — still uses GROSS"
            )
            assert "promotion_discount" in block, (
                f"P0-04: OLT block #{i+1} missing promotion_discount — still uses GROSS"
            )

    def test_line_share_numerator_uses_net(self):
        """line_share_sql CASE expressions must use NET numerator."""
        # Find CASE expressions that compute line_share or shipping/logistics share
        # Pattern: ISNULL(ol.item_price, 0) ... / ... olt.order_line_total
        # After the fix, the numerator must be NET: (item_price - item_tax - promo)
        #
        # A GROSS numerator looks like:
        #   ISNULL(ol.item_price, 0) / ... olt.order_line_total
        # without subtracting item_tax and promotion_discount.
        #
        # We search for the pattern: ISNULL(ol.item_price, 0)\s*\n\s*/\s*\n?\s*NULLIF
        # which would indicate a bare item_price divided by NULLIF(olt.order_line_total)
        bare_share = re.findall(
            r"ISNULL\(ol\.item_price,\s*0\)\s*\n\s*/"
            r"\s*\n?\s*NULLIF\(olt\.order_line_total",
            self.source,
        )
        assert bare_share == [], (
            f"P0-04: Found {len(bare_share)} bare GROSS line_share numerator(s). "
            "All must use (item_price - item_tax - promotion_discount)."
        )

    def test_order_totals_cte_uses_net(self):
        """The order_totals CTE/subquery (ads cost history) must use NET."""
        # Look for 'order_totals' or 'order_revenue' patterns and ensure NET formula
        if "order_totals" in self.source:
            idx = self.source.find("order_totals")
            block = self.source[max(0, idx - 200):idx + 600]
            assert "item_tax" in block, (
                "P0-04: order_totals CTE must subtract item_tax for NET"
            )
            assert "promotion_discount" in block, (
                "P0-04: order_totals CTE must subtract promotion_discount for NET"
            )


# ═══════════════════════════════════════════════════════════════════════════
# P0-05: profit.py — _OLT_JOIN_SQL template must use NET values
# ═══════════════════════════════════════════════════════════════════════════


class TestP0_05_ProfitV1_OltTemplate:
    """profit.py _OLT_JOIN_SQL constant must compute NET order_line_total."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        profit_path = (
            Path(__file__).resolve().parents[1]
            / "app" / "api" / "v1" / "profit.py"
        )
        self.source = profit_path.read_text(encoding="utf-8")

    def test_olt_join_sql_subtracts_tax(self):
        """_OLT_JOIN_SQL must reference item_tax for NET deduction."""
        olt_start = self.source.find("_OLT_JOIN_SQL")
        assert olt_start > 0, "Expected _OLT_JOIN_SQL constant in profit.py"
        olt_block = self.source[olt_start:olt_start + 400]
        assert "item_tax" in olt_block, (
            "P0-05: _OLT_JOIN_SQL must subtract item_tax for NET order_line_total"
        )

    def test_olt_join_sql_subtracts_promo(self):
        """_OLT_JOIN_SQL must reference promotion_discount for NET deduction."""
        olt_start = self.source.find("_OLT_JOIN_SQL")
        assert olt_start > 0
        olt_block = self.source[olt_start:olt_start + 400]
        assert "promotion_discount" in olt_block, (
            "P0-05: _OLT_JOIN_SQL must subtract promotion_discount for NET"
        )
