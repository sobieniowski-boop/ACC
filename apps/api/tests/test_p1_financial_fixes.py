"""Tests for P1 Silent Financial Error fixes (SF-01 through SF-05).

Validates:
  SF-01: COGS uses NET purchase price (no VAT multiplication)
  SF-02: FX fallback raises instead of returning 1.0
  SF-03: Revenue computed as NET (item_price - item_tax - promotion_discount)
  SF-04: Profit tier labels present in API responses
  SF-05: MERGE WHEN MATCHED preserves enriched cost columns
"""
from __future__ import annotations

import asyncio
import types
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# SF-01: COGS must NOT be multiplied by DEFAULT_VAT (1.23)
# ═══════════════════════════════════════════════════════════════════════════

class TestSF01_COGS_NoVAT:
    """COGS should be NET purchase price, not inflated by 23% VAT."""

    @pytest.mark.asyncio
    async def test_cogs_uses_net_price_not_gross(self):
        """When product.netto_purchase_price_pln is 100, COGS should be 100, not 123."""
        from app.services.profit_service import calculate_order_profit

        # Build mock order with one line
        line = MagicMock()
        line.cogs_pln = None  # force product lookup path
        line.product_id = "prod-1"
        line.quantity_shipped = 1
        line.quantity_ordered = 1
        line.fba_fee_pln = Decimal("5.00")
        line.referral_fee_pln = Decimal("3.00")
        line.item_price = Decimal("50.00")
        line.item_tax = Decimal("8.00")
        line.promotion_discount = Decimal("0.00")

        product = MagicMock()
        product.netto_purchase_price_pln = Decimal("100.00")

        order = MagicMock()
        order.currency = "PLN"
        order.order_total = Decimal("50.00")
        order.purchase_date = date(2026, 3, 1)
        order.lines = [line]
        order.ads_cost_pln = Decimal("0")
        order.amazon_order_id = "TEST-001"
        order.logistics_pln = Decimal("0")

        db = AsyncMock()
        # Product lookup returns our mock product
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = product
        db.execute.return_value = result_mock

        with patch("app.services.profit_service.resolve_profit_logistics_pln",
                    new_callable=AsyncMock, return_value=0.0):
            await calculate_order_profit(db, order, fx_rate=1.0)

        # Key assertion: COGS should be 100.00 (NET), NOT 123.00 (GROSS)
        assert line.cogs_pln == 100.00, (
            f"SF-01: COGS should be NET (100.00), got {line.cogs_pln} "
            f"(was it multiplied by DEFAULT_VAT=1.23?)"
        )
        assert order.cogs_pln == 100.00

    @pytest.mark.asyncio
    async def test_cogs_from_order_line_not_multiplied(self):
        """When line already has cogs_pln set, it should be used directly."""
        from app.services.profit_service import calculate_order_profit

        line = MagicMock()
        line.cogs_pln = Decimal("80.00")  # already set
        line.product_id = None
        line.quantity_shipped = 2
        line.quantity_ordered = 2
        line.fba_fee_pln = Decimal("5.00")
        line.referral_fee_pln = Decimal("3.00")
        line.item_price = Decimal("50.00")
        line.item_tax = Decimal("8.00")
        line.promotion_discount = Decimal("0.00")

        order = MagicMock()
        order.currency = "PLN"
        order.order_total = Decimal("100.00")
        order.purchase_date = date(2026, 3, 1)
        order.lines = [line]
        order.ads_cost_pln = Decimal("0")
        order.amazon_order_id = "TEST-002"
        order.logistics_pln = Decimal("0")

        db = AsyncMock()

        with patch("app.services.profit_service.resolve_profit_logistics_pln",
                    new_callable=AsyncMock, return_value=0.0):
            await calculate_order_profit(db, order, fx_rate=1.0)

        # cogs_pln = 80.00 * 2 (qty) = 160.00
        assert order.cogs_pln == 160.00


# ═══════════════════════════════════════════════════════════════════════════
# SF-02: FX fallback must NOT silently return 1.0
# ═══════════════════════════════════════════════════════════════════════════

class TestSF02_FX_NoSilentFallback:
    """FX service must raise or log when no rate exists, never return 1.0 silently."""

    def test_get_rate_safe_raises_when_no_data(self):
        """get_rate_safe must raise FxRateMissingError when no rates exist."""
        from app.core.fx_service import get_rate_safe, FxRateMissingError, _cache

        # Empty the cache
        with patch("app.core.fx_service._load_cache", return_value={}):
            with patch("app.core.fx_service.get_rate") as mock_get:
                from app.core.fx_service import StaleFxRateError
                mock_get.side_effect = StaleFxRateError("stale")

                with pytest.raises(FxRateMissingError, match="No exchange rate data"):
                    get_rate_safe("SEK", date(2026, 3, 1))

    def test_get_rate_safe_returns_stale_rate_with_warning(self):
        """get_rate_safe returns last known rate when stale (not 1.0)."""
        from app.core.fx_service import get_rate_safe, StaleFxRateError

        stale_cache = {"SEK": [("2026-02-01", 0.42)]}

        with patch("app.core.fx_service.get_rate") as mock_get:
            mock_get.side_effect = StaleFxRateError("stale")
            with patch("app.core.fx_service._load_cache", return_value=stale_cache):
                rate = get_rate_safe("SEK", date(2026, 3, 1))
                assert rate == 0.42, "Should return last known rate, not 1.0"

    def test_pln_always_returns_1(self):
        """PLN→PLN conversion should always return 1.0."""
        from app.core.fx_service import get_rate_safe
        assert get_rate_safe("PLN") == 1.0
        assert get_rate_safe("PLN", date(2026, 3, 1)) == 1.0

    def test_build_fx_case_sql_uses_null_fallback(self):
        """SQL CASE expression should use NULL (not 1.0) for unknown currencies."""
        from app.core.fx_service import build_fx_case_sql

        cache = {"EUR": [("2026-03-01", 4.30)]}
        with patch("app.core.fx_service._load_cache", return_value=cache):
            sql = build_fx_case_sql("o.currency")
            assert "ELSE NULL END" in sql, (
                f"SF-02: SQL fallback should be NULL, not 1.0. Got: {sql}"
            )
            assert "ELSE 1.0 END" not in sql


# ═══════════════════════════════════════════════════════════════════════════
# SF-03: Revenue must be NET (item_price - item_tax - promotion_discount)
# ═══════════════════════════════════════════════════════════════════════════

class TestSF03_Revenue_NET:
    """Revenue in profit_service.py must match profit_engine.py (NET, not GROSS)."""

    @pytest.mark.asyncio
    async def test_revenue_subtracts_tax_and_promo(self):
        """Revenue = (item_price - item_tax - promotion_discount) * fx, NOT order_total * fx."""
        from app.services.profit_service import calculate_order_profit

        line = MagicMock()
        line.cogs_pln = Decimal("10.00")
        line.product_id = None
        line.quantity_shipped = 1
        line.quantity_ordered = 1
        line.fba_fee_pln = Decimal("0")
        line.referral_fee_pln = Decimal("0")
        line.item_price = Decimal("100.00")
        line.item_tax = Decimal("19.00")       # 19% VAT
        line.promotion_discount = Decimal("5.00")

        order = MagicMock()
        order.currency = "EUR"
        order.order_total = Decimal("100.00")  # GROSS — should NOT be used
        order.purchase_date = date(2026, 3, 1)
        order.lines = [line]
        order.ads_cost_pln = Decimal("0")
        order.amazon_order_id = "TEST-003"
        order.logistics_pln = Decimal("0")

        db = AsyncMock()
        fx_rate = 4.30  # EUR→PLN

        with patch("app.services.profit_service.resolve_profit_logistics_pln",
                    new_callable=AsyncMock, return_value=0.0):
            await calculate_order_profit(db, order, fx_rate=fx_rate)

        # Expected: (100.00 - 19.00 - 5.00) * 4.30 = 76.00 * 4.30 = 326.80
        expected_revenue = round((100.00 - 19.00 - 5.00) * 4.30, 2)
        assert order.revenue_pln == expected_revenue, (
            f"SF-03: Revenue should be NET ({expected_revenue}), "
            f"got {order.revenue_pln}. Was order_total used instead?"
        )
        # It should NOT be order_total * fx = 100 * 4.30 = 430.00
        assert order.revenue_pln != round(100.00 * 4.30, 2), (
            "SF-03: Revenue must NOT use order_total (GROSS)"
        )

    @pytest.mark.asyncio
    async def test_revenue_handles_null_tax_and_promo(self):
        """Null item_tax or promotion_discount should be treated as 0."""
        from app.services.profit_service import calculate_order_profit

        line = MagicMock()
        line.cogs_pln = Decimal("10.00")
        line.product_id = None
        line.quantity_shipped = 1
        line.quantity_ordered = 1
        line.fba_fee_pln = Decimal("0")
        line.referral_fee_pln = Decimal("0")
        line.item_price = Decimal("50.00")
        line.item_tax = None  # NULL
        line.promotion_discount = None  # NULL

        order = MagicMock()
        order.currency = "PLN"
        order.order_total = Decimal("50.00")
        order.purchase_date = date(2026, 3, 1)
        order.lines = [line]
        order.ads_cost_pln = Decimal("0")
        order.amazon_order_id = "TEST-004"
        order.logistics_pln = Decimal("0")

        db = AsyncMock()

        with patch("app.services.profit_service.resolve_profit_logistics_pln",
                    new_callable=AsyncMock, return_value=0.0):
            await calculate_order_profit(db, order, fx_rate=1.0)

        assert order.revenue_pln == 50.00


class TestSF03b_OrderLevelCM1DirectFees:
    """Direct-order CM1 costs must flow into total Amazon fees exactly once."""

    def test_order_level_direct_fees_included_once(self):
        from app.services.profit_service import calculate_order_profit

        line = MagicMock()
        line.cogs_pln = Decimal("10.00")
        line.product_id = None
        line.quantity_shipped = 1
        line.quantity_ordered = 1
        line.fba_fee_pln = Decimal("5.00")
        line.referral_fee_pln = Decimal("3.00")
        line.item_price = Decimal("100.00")
        line.item_tax = Decimal("20.00")
        line.promotion_discount = Decimal("0.00")

        order = MagicMock()
        order.currency = "PLN"
        order.order_total = Decimal("100.00")
        order.purchase_date = date(2026, 3, 1)
        order.lines = [line]
        order.ads_cost_pln = Decimal("0")
        order.amazon_order_id = "TEST-CM1-DIRECT-001"
        order.logistics_pln = Decimal("0")
        order.shipping_surcharge_pln = Decimal("4.00")
        order.promo_order_fee_pln = Decimal("6.00")
        order.refund_commission_pln = Decimal("2.00")

        db = AsyncMock()

        async def run_test() -> None:
            with patch("app.services.profit_service.resolve_profit_logistics_pln",
                    new_callable=AsyncMock, return_value=0.0):
                await calculate_order_profit(db, order, fx_rate=1.0)

        asyncio.run(run_test())

        assert order.amazon_fees_pln == 20.00
        assert order.contribution_margin_pln == 50.00


# ═══════════════════════════════════════════════════════════════════════════
# SF-04: Profit tier labels must be present in API responses
# ═══════════════════════════════════════════════════════════════════════════

class TestSF04_ProfitTierLabels:
    """API responses must include profit_tier to disambiguate profit definitions."""

    def test_profitability_overview_has_profit_tier(self):
        """get_profitability_overview KPI must include profit_tier='cm1_cm2_np'."""
        from app.services.profitability_service import get_profitability_overview
        import app.intelligence.profit.rollup as mod

        mod._METADATA_TABLE_VERIFIED = True
        mod._PROFIT_OVERVIEW_CACHE.clear()

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # fetchone calls: 1st = KPI row (9 cols), 2nd = MAX(computed_at) fallback for rollup_ts
        mock_cur.fetchone.side_effect = [
            (
                10000.0,  # revenue
                5000.0,   # profit
                100,      # orders
                200,      # units
                500.0,    # ad_spend
                200.0,    # refund
                10,       # refund_units
                3000.0,   # cm1
                2000.0,   # cm2
            ),
            (None,),  # MAX(computed_at) - no rollup timestamp
        ]
        # Simulate best/worst/loss queries
        mock_cur.fetchall.return_value = []

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            result = get_profitability_overview(date(2026, 3, 1), date(2026, 3, 7))

        assert "profit_tier" in result["kpi"], "SF-04: profit_tier missing from KPI"
        assert result["kpi"]["profit_tier"] == "cm1_cm2_np"

    def test_executive_overview_has_profit_tier(self):
        """get_exec_overview KPI must include profit_tier='net_profit'."""
        from app.services.executive_service import get_exec_overview

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # First call: current period KPIs (10 columns: rev, profit, orders, units, ad_spend, refund, sessions, return_rate, cm1, cm2)
        # Second call: previous period (2 cols)
        # Third: health score row (none)
        mock_cur.fetchone.side_effect = [
            (1000.0, 500.0, 50, 100, 50.0, 20.0, 200, 3.5, 400.0, 300.0),  # current KPIs
            (800.0, 400.0, 300.0, 200.0),                                     # previous period (rev, profit, cm1, cm2)
            None,                                                              # health score (none)
        ]
        mock_cur.fetchall.return_value = []

        with patch("app.services.executive_service.connect_acc", return_value=mock_conn):
            with patch("app.services.executive_service._exec_cache_get", return_value=None):
                with patch("app.services.executive_service._exec_cache_set"):
                    result = get_exec_overview(date(2026, 3, 1), date(2026, 3, 7))

        assert "profit_tier" in result["kpi"], "SF-04: profit_tier missing from executive KPI"
        assert result["kpi"]["profit_tier"] == "net_profit"


# ═══════════════════════════════════════════════════════════════════════════
# SF-05: MERGE WHEN MATCHED must preserve enriched cost columns
# ═══════════════════════════════════════════════════════════════════════════

class TestSF05_MergePreservesEnrichedColumns:
    """The SKU rollup MERGE must not overwrite ad_spend, refund, storage, etc. with zeros."""

    def test_merge_sql_uses_tgt_for_enriched_columns(self):
        """WHEN MATCHED must reference tgt.ad_spend_pln etc., not src (which is 0)."""
        from app.services.profitability_service import recompute_rollups

        captured_sql = []

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 5
        mock_conn.cursor.return_value = mock_cur

        def capture_execute(sql, params=None):
            captured_sql.append(str(sql))

        mock_cur.execute = capture_execute

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch("app.intelligence.profit.rollup._enrich_rollup_from_finance",
                        return_value={"storage_rows": 1, "refund_rows": 1, "other_rows": 1,
                                     "ads_rows": 1, "return_units_rows": 1}):
                try:
                    recompute_rollups(date(2026, 3, 1), date(2026, 3, 7))
                except Exception:
                    pass  # May fail on mock but SQL is captured

        # Find the SKU rollup MERGE statement
        merge_sql = None
        for sql in captured_sql:
            if "acc_sku_profitability_rollup" in sql and "MERGE" in sql:
                merge_sql = sql
                break

        assert merge_sql is not None, "MERGE SQL for SKU rollup not found"

        # Key assertions: WHEN MATCHED must use tgt.* for enriched columns
        assert "tgt.ad_spend_pln" in merge_sql, (
            "SF-05: MERGE WHEN MATCHED must preserve tgt.ad_spend_pln, not overwrite with 0"
        )
        assert "tgt.refund_pln" in merge_sql, (
            "SF-05: MERGE WHEN MATCHED must preserve tgt.refund_pln"
        )
        assert "tgt.storage_fee_pln" in merge_sql, (
            "SF-05: MERGE WHEN MATCHED must preserve tgt.storage_fee_pln"
        )
        assert "tgt.other_fees_pln" in merge_sql, (
            "SF-05: MERGE WHEN MATCHED must preserve tgt.other_fees_pln"
        )
        assert "tgt.refund_units" in merge_sql, (
            "SF-05: MERGE WHEN MATCHED must preserve tgt.refund_units"
        )

    def test_merge_sql_rebuilds_logistics_from_canonical_source(self):
        """The WHEN MATCHED UPDATE SET must rebuild logistics from canonical source."""
        from app.services.profitability_service import recompute_rollups

        captured_sql = []

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 5
        mock_conn.cursor.return_value = mock_cur

        def capture_execute(sql, params=None):
            captured_sql.append(str(sql))

        mock_cur.execute = capture_execute

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch("app.intelligence.profit.rollup._enrich_rollup_from_finance",
                        return_value={"storage_rows": 0, "refund_rows": 0, "other_rows": 0,
                                     "ads_rows": 0, "return_units_rows": 0, "logistics_rows": 0}):
                try:
                    recompute_rollups(date(2026, 3, 1), date(2026, 3, 7))
                except Exception:
                    pass

        merge_sql = ""
        for sql in captured_sql:
            if "acc_sku_profitability_rollup" in sql and "MERGE" in sql:
                merge_sql = sql
                break

        matched_section = merge_sql.split("WHEN MATCHED")[1].split("WHEN NOT MATCHED")[0] if "WHEN MATCHED" in merge_sql else ""

        assert "acc_order_logistics_fact" in merge_sql, (
            "Courier fix: MERGE source must read canonical acc_order_logistics_fact"
        )
        assert "logistics_pln = src.logistics_pln" in matched_section, (
            "Courier fix: WHEN MATCHED must rebuild logistics_pln from canonical source"
        )
        assert "= src.ad_spend_pln" not in matched_section, (
            "SF-05: WHEN MATCHED must NOT set ad_spend_pln = src.ad_spend_pln (which is 0)"
        )
        assert "= src.refund_pln" not in matched_section, (
            "SF-05: WHEN MATCHED must NOT set refund_pln = src.refund_pln (which is 0)"
        )
        assert "- src.fba_fees_pln - src.logistics_pln" in matched_section, (
            "Courier fix: profit and margin formulas must use rebuilt src.logistics_pln"
        )

    def test_enrichment_empty_warning_logged(self):
        """When enrichment returns 0 rows for any category, a warning must be logged."""
        from app.services.profitability_service import recompute_rollups

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 5
        mock_conn.cursor.return_value = mock_cur

        enriched = {
            "storage_rows": 0,
            "refund_rows": 10,
            "other_rows": 0,
            "ads_rows": 5,
            "return_units_rows": 0,
            "logistics_rows": 7,
        }

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch("app.intelligence.profit.rollup._enrich_rollup_from_finance",
                        return_value=enriched):
                with patch("app.intelligence.profit.rollup.log") as mock_log:
                    try:
                        recompute_rollups(date(2026, 3, 1), date(2026, 3, 7))
                    except Exception:
                        pass

                    # Check that warning was logged for empty enrichments
                    warning_calls = [
                        c for c in mock_log.warning.call_args_list
                        if c[0][0] == "profitability.enrichment_empty"
                    ]
                    empty_fields = {c[1]["field"] for c in warning_calls}
                    assert "storage_rows" in empty_fields
                    assert "other_rows" in empty_fields
                    assert "return_units_rows" in empty_fields
                    # refund_rows=10 and ads_rows=5 should NOT trigger warning
                    assert "refund_rows" not in empty_fields
                    assert "ads_rows" not in empty_fields


# ═══════════════════════════════════════════════════════════════════════════
# SF-02 (related): ads_sync FX fallback
# ═══════════════════════════════════════════════════════════════════════════

class TestSF02_AdsSyncFXFallback:
    """ads_sync must log warning when FX rate is missing, not silently use 1.0."""

    def test_missing_rate_produces_none_not_1(self):
        """When rate is missing, spend_pln should be None, not spend * 1.0."""
        from app.services.ads_sync import _get_exchange_rates

        # Get rates for SEK when no SEK data exists in DB
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []  # no rates in DB
        mock_conn.cursor.return_value = mock_cur

        with patch("app.services.ads_sync.connect_acc", return_value=mock_conn):
            rates = _get_exchange_rates({"SEK"}, {date(2026, 3, 1)})

        # SEK should NOT be in rates (or should have no fallback to 1.0)
        sek_rate = rates.get(("SEK", date(2026, 3, 1)))
        assert sek_rate is None, (
            f"SF-02: Missing FX rate should be None, not {sek_rate}. "
            f"The 1.0 fallback happens in the caller, which now logs it."
        )
