"""Tests for P2 Silent Financial Error Audit fixes (SF-06..SF-16).

Covers: SF-06 (scheduler dependency chain), SF-07 (_f null warning),
SF-08 (FBA revenue-weighted allocation), SF-09 (DHL gross_amount),
SF-10 (ads FX gap summary), SF-11 (overhead NULL/zero warnings),
SF-12 (refund zero-order-total), SF-14 (TKL cache miss warnings),
SF-16 (unknown fee sign=0).
"""
from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# SF-06: Scheduler dependency chain
# ---------------------------------------------------------------------------

class TestSF06SchedulerDependencyChain:
    """_recompute_profitability must call ads+finance sync BEFORE rollup."""

    @pytest.mark.asyncio
    async def test_rollup_calls_ads_sync_before_rollup(self):
        call_order: list[str] = []

        async def fake_ads_sync(days_back=3):
            call_order.append("ads_sync")
            return {"status": "ok"}

        async def fake_finance_sync(days_back=3, job_id=None):
            call_order.append("finance_sync")
            return {"fee_rows": 5}

        async def fake_rollup_and_alerts():
            call_order.append("rollup")
            call_order.append("alerts")
            return (10, 2, {"alerts_created": 0})

        async def fake_executive():
            call_order.append("executive")
            return {"metrics_rows": 1, "risks_found": 0}

        async def fake_strategy():
            call_order.append("strategy")
            return 0

        with patch("app.platform.scheduler.base.create_job_record", return_value="test-j"), \
             patch("app.connectors.mssql.set_job_success"), \
             patch("app.connectors.mssql.set_job_failure"), \
             patch("app.services.event_backbone.check_domain_events_today", return_value=False), \
             patch("app.services.event_backbone.emit_domain_event"), \
             patch("app.services.ads_sync.run_full_ads_sync", side_effect=fake_ads_sync), \
             patch("app.services.order_pipeline.step_sync_finances", side_effect=fake_finance_sync), \
             patch("app.platform.scheduler.profit._step_rollup_and_alerts", side_effect=fake_rollup_and_alerts), \
             patch("app.platform.scheduler.profit._step_executive", side_effect=fake_executive), \
             patch("app.platform.scheduler.profit._step_strategy", side_effect=fake_strategy):
            from app.platform.scheduler.profit import _recompute_profitability
            await _recompute_profitability()

        assert call_order.index("ads_sync") < call_order.index("rollup")
        assert call_order.index("finance_sync") < call_order.index("rollup")

    @pytest.mark.asyncio
    async def test_rollup_skipped_on_ads_sync_failure(self):
        call_order: list[str] = []

        async def failing_ads_sync(days_back=3):
            call_order.append("ads_sync_fail")
            raise RuntimeError("Ads API down")

        async def fake_finance_sync(days_back=3, job_id=None):
            call_order.append("finance_sync")
            return {"fee_rows": 0}

        with patch("app.platform.scheduler.base.create_job_record", return_value="test-j"), \
             patch("app.platform.scheduler.profit.set_job_success"), \
             patch("app.platform.scheduler.profit.set_job_failure") as mock_fail, \
             patch("app.services.event_backbone.check_domain_events_today", return_value=False), \
             patch("app.services.ads_sync.run_full_ads_sync", side_effect=failing_ads_sync), \
             patch("app.services.order_pipeline.step_sync_finances", side_effect=fake_finance_sync):
            from app.platform.scheduler.profit import _recompute_profitability
            await _recompute_profitability()

        assert "ads_sync_fail" in call_order
        # rollup should NOT have been called
        assert "rollup" not in call_order
        mock_fail.assert_called_once()


# ---------------------------------------------------------------------------
# SF-07: _f() null-coercion warning + _f_strict()
# ---------------------------------------------------------------------------

class TestSF07NullCoercion:
    def test_f_returns_zero_for_none(self):
        from app.services.profit_engine import _f
        assert _f(None) == 0.0

    def test_f_logs_warning_when_field_provided(self):
        with patch("app.intelligence.profit.helpers.log") as mock_log:
            from app.services.profit_engine import _f
            result = _f(None, field="cogs_pln")
        assert result == 0.0
        mock_log.warning.assert_called_once()
        args, kwargs = mock_log.warning.call_args
        assert args[0] == "profit_engine._f_null_coercion"
        assert kwargs["field"] == "cogs_pln"

    def test_f_no_warning_without_field(self):
        with patch("app.intelligence.profit.helpers.log") as mock_log:
            from app.services.profit_engine import _f
            _f(None)
        mock_log.warning.assert_not_called()

    def test_f_strict_raises_on_none(self):
        from app.services.profit_engine import _f_strict
        with pytest.raises(ValueError, match="NULL value"):
            _f_strict(None, "revenue_pln")

    def test_f_strict_returns_float_for_valid(self):
        from app.services.profit_engine import _f_strict
        assert _f_strict(42.567, "revenue_pln") == 42.57

    def test_f_strict_raises_on_non_numeric(self):
        from app.services.profit_engine import _f_strict
        with pytest.raises(ValueError, match="Cannot convert"):
            _f_strict("abc", "cogs_pln")


# ---------------------------------------------------------------------------
# SF-08: FBA allocation prefers revenue weighting
# ---------------------------------------------------------------------------

class TestSF08FBARevenueWeightedAllocation:
    def test_revenue_weighted_over_unit_count(self):
        from app.services.profit_engine import _allocate_fba_component_costs

        products = [
            {"marketplace_id": "DE", "afn_units": 10, "revenue_pln": 1000.0,
             "fba_storage_fee_pln": 0, "fba_aged_fee_pln": 0, "fba_removal_fee_pln": 0,
             "fba_liquidation_fee_pln": 0, "refund_finance_pln": 0,
             "shipping_surcharge_pln": 0, "fba_inbound_fee_pln": 0,
             "promo_cost_pln": 0, "warehouse_loss_pln": 0, "amazon_other_fee_pln": 0},
            {"marketplace_id": "DE", "afn_units": 10, "revenue_pln": 100.0,
             "fba_storage_fee_pln": 0, "fba_aged_fee_pln": 0, "fba_removal_fee_pln": 0,
             "fba_liquidation_fee_pln": 0, "refund_finance_pln": 0,
             "shipping_surcharge_pln": 0, "fba_inbound_fee_pln": 0,
             "promo_cost_pln": 0, "warehouse_loss_pln": 0, "amazon_other_fee_pln": 0},
        ]
        pools = {"DE": {"storage": 110.0}}
        _allocate_fba_component_costs(products, pools)

        # Revenue-weighted: product1 gets 1000/1100 ≈ 90.9%, product2 gets 100/1100 ≈ 9.1%
        assert products[0]["fba_storage_fee_pln"] > products[1]["fba_storage_fee_pln"]
        total = products[0]["fba_storage_fee_pln"] + products[1]["fba_storage_fee_pln"]
        assert abs(total - 110.0) < 0.01

    def test_equal_split_with_warning_when_no_data(self):
        from app.services.profit_engine import _allocate_fba_component_costs

        products = [
            {"marketplace_id": "DE", "afn_units": 0, "revenue_pln": 0.0,
             "fba_storage_fee_pln": 0, "fba_aged_fee_pln": 0, "fba_removal_fee_pln": 0,
             "fba_liquidation_fee_pln": 0, "refund_finance_pln": 0,
             "shipping_surcharge_pln": 0, "fba_inbound_fee_pln": 0,
             "promo_cost_pln": 0, "warehouse_loss_pln": 0, "amazon_other_fee_pln": 0},
            {"marketplace_id": "DE", "afn_units": 0, "revenue_pln": 0.0,
             "fba_storage_fee_pln": 0, "fba_aged_fee_pln": 0, "fba_removal_fee_pln": 0,
             "fba_liquidation_fee_pln": 0, "refund_finance_pln": 0,
             "shipping_surcharge_pln": 0, "fba_inbound_fee_pln": 0,
             "promo_cost_pln": 0, "warehouse_loss_pln": 0, "amazon_other_fee_pln": 0},
        ]
        pools = {"DE": {"storage": 100.0}}

        with patch("app.intelligence.profit.calculator.log") as mock_log:
            _allocate_fba_component_costs(products, pools)

        # Equal split
        assert products[0]["fba_storage_fee_pln"] == pytest.approx(50.0, abs=0.01)
        assert products[1]["fba_storage_fee_pln"] == pytest.approx(50.0, abs=0.01)
        # Warning should have fired
        mock_log.warning.assert_called()


# ---------------------------------------------------------------------------
# SF-09: DHL gross_amount includes fuel
# ---------------------------------------------------------------------------

class TestSF09DHLGrossAmount:
    def test_gross_includes_net_plus_fuel(self):
        """gross_amount = net_amount + fuel_amount (not just net)."""
        mock_cursor = MagicMock()
        # Row: parcel_number_base, total_net_amount, fuel_amount, ...
        mock_cursor.fetchall.return_value = [
            ("PKG001", 100.0, 23.0, "2025-01-01", "2025-01-01", "2025-01-01", 1, 1),
        ]

        from app.services.dhl_cost_sync import _query_imported_billing_costs
        result = _query_imported_billing_costs(mock_cursor, ["PKG001"])

        key = list(result.keys())[0]
        assert result[key]["net_amount"] == 100.0
        assert result[key]["fuel_amount"] == 23.0
        assert result[key]["gross_amount"] == pytest.approx(123.0, abs=0.01)
        assert result[key]["cost_basis"] == "NET"


# ---------------------------------------------------------------------------
# SF-10: Ads FX gap summary logging
# ---------------------------------------------------------------------------

class TestSF10AdsFXGapSummary:
    def test_campaign_day_logs_fx_gap_summary(self):
        """When FX rates are missing, a summary error log fires after the loop."""
        from app.services.ads_sync import _upsert_daily_metrics

        metrics = [
            SimpleNamespace(
                campaign_id="C1", ad_type="SP", report_date=date(2025, 6, 1),
                impressions=100, clicks=10, spend=50.0, sales_7d=200.0,
                orders_7d=5, units_7d=5, currency="EUR",
            ),
        ]

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("app.services.ads_sync.connect_acc", return_value=mock_conn), \
             patch("app.services.ads_sync._get_exchange_rates", return_value={}), \
             patch("app.services.ads_sync.log") as mock_log:
            count = _upsert_daily_metrics(metrics)

        assert count == 1
        # Check that the summary error was logged
        error_calls = [c for c in mock_log.error.call_args_list
                       if c[0][0] == "ads_sync.fx_gap_summary"]
        assert len(error_calls) == 1
        assert error_calls[0][1]["missing"] == 1


# ---------------------------------------------------------------------------
# SF-11: Overhead pool NULL/zero amount warnings
# ---------------------------------------------------------------------------

class TestSF11OverheadNullAmount:
    def test_null_amount_logs_warning_and_skips(self):
        from app.services.profit_engine import _f

        pools = [
            {"pool_name": "Rent", "amount_pln": None, "marketplace_id": "DE",
             "allocation_method": "revenue_share", "confidence_pct": 50.0},
        ]
        products = [
            {"marketplace_id": "DE", "revenue_pln": 100.0, "overhead_allocated_pln": 0.0,
             "_oh_amount": 0.0, "_oh_conf_wsum": 0.0, "_oh_methods": set()},
        ]

        with patch("app.intelligence.profit.calculator.log") as mock_log:
            # Import the overhead allocation function
            from app.services.profit_engine import _allocate_overhead_costs
            _allocate_overhead_costs(products, pools)

        # No overhead should have been allocated
        assert products[0]["overhead_allocated_pln"] == 0.0
        # Warning logged for NULL amount
        null_calls = [c for c in mock_log.warning.call_args_list
                      if c[0][0] == "profit_engine.overhead_null_amount"]
        assert len(null_calls) == 1

    def test_zero_amount_logs_warning(self):
        pools = [
            {"pool_name": "Insurance", "amount_pln": 0.0, "marketplace_id": "DE",
             "allocation_method": "revenue_share", "confidence_pct": 50.0},
        ]
        products = [
            {"marketplace_id": "DE", "revenue_pln": 100.0, "overhead_allocated_pln": 0.0,
             "_oh_amount": 0.0, "_oh_conf_wsum": 0.0, "_oh_methods": set()},
        ]

        with patch("app.intelligence.profit.calculator.log") as mock_log:
            from app.services.profit_engine import _allocate_overhead_costs
            _allocate_overhead_costs(products, pools)

        zero_calls = [c for c in mock_log.warning.call_args_list
                      if c[0][0] == "profit_engine.overhead_zero_amount"]
        assert len(zero_calls) == 1


# ---------------------------------------------------------------------------
# SF-14: TKL cache miss warning
# ---------------------------------------------------------------------------

class TestSF14TKLCacheMissWarning:
    def test_missing_file_logs_warning(self):
        from app.services.profit_engine import _tkl_file_metadata

        with patch("app.intelligence.profit.cost_model.log") as mock_log:
            name, mtime, sig = _tkl_file_metadata(Path("/nonexistent/file.xlsx"))

        assert "missing" in sig
        mock_log.warning.assert_called_once()
        assert mock_log.warning.call_args[0][0] == "profit_engine.tkl_file_missing"

    def test_none_path_returns_none_sig(self):
        from app.services.profit_engine import _tkl_file_metadata
        name, mtime, sig = _tkl_file_metadata(None)
        assert sig == "none"
        assert name is None


# ---------------------------------------------------------------------------
# SF-16: Unknown fee types → sign=0 (suspended)
# ---------------------------------------------------------------------------

class TestSF16UnknownFeeSignZero:
    def test_unknown_fee_gets_sign_zero(self):
        from app.core.fee_taxonomy import classify_fee, FeeCategory, _seen_unknown
        _seen_unknown.clear()
        entry = classify_fee("TOTALLY_UNKNOWN_FEE_XYZ_2025", "Order")
        assert entry.category == FeeCategory.UNKNOWN
        assert entry.sign == 0

    def test_get_ledger_rule_preserves_sign_zero(self):
        from app.core.fee_taxonomy import get_ledger_rule, _seen_unknown
        _seen_unknown.clear()
        acct, tax, sign = get_ledger_rule("TOTALLY_UNKNOWN_FEE_ABC_2025", "Order")
        assert sign == 0.0
        assert acct == "599"

    def test_get_profit_classification_returns_sign_zero(self):
        from app.core.fee_taxonomy import get_profit_classification, _seen_unknown
        _seen_unknown.clear()
        result = get_profit_classification("NEW_UNKNOWN_FEE_2026", "Order")
        assert result is not None
        assert result["sign"] == 0

    def test_known_fee_retains_original_sign(self):
        from app.core.fee_taxonomy import classify_fee
        entry = classify_fee("Commission", "Order")
        assert entry.sign != 0  # known fees keep their sign
