"""Tests for profitability service: simulate_price, allocation algorithms, helpers.

Sprint 3, Task S3-09.
"""

import pytest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# 1. Helper functions from helpers.py
# ---------------------------------------------------------------------------

from app.intelligence.profit.helpers import (
    _f, _f_strict, _i, _mkt_code, _norm_text, _norm_internal_sku,
)


class TestSafeFloatHelper:
    """Tests for _f() — float coercion with default."""

    def test_converts_numeric_string(self):
        assert _f("12.345") == 12.35  # rounds to 2 decimals

    def test_converts_integer(self):
        assert _f(42) == 42.0

    def test_returns_default_for_none(self):
        assert _f(None) == 0.0

    def test_custom_default_for_none(self):
        assert _f(None, default=99.0) == 99.0

    def test_returns_default_for_invalid(self):
        assert _f("abc") == 0.0

    def test_returns_default_for_empty(self):
        assert _f("") == 0.0

    def test_rounds_to_2_decimals(self):
        assert _f("1.23456") == 1.23


class TestStrictFloat:
    """Tests for _f_strict() — raises on NULL."""

    def test_converts_valid_float(self):
        assert _f_strict("10.5", "revenue") == 10.5

    def test_raises_on_none(self):
        with pytest.raises(ValueError, match="NULL value"):
            _f_strict(None, "revenue")

    def test_raises_on_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert"):
            _f_strict("abc", "cost")


class TestSafeInt:
    """Tests for _i() — int coercion with default."""

    def test_converts_string(self):
        assert _i("42") == 42

    def test_returns_default_for_none(self):
        assert _i(None) == 0

    def test_returns_default_for_invalid(self):
        assert _i("abc") == 0

    def test_custom_default(self):
        assert _i(None, default=-1) == -1


class TestMktCode:
    """Tests for _mkt_code() — marketplace ID to short code."""

    def test_returns_empty_for_none(self):
        assert _mkt_code(None) == ""

    def test_returns_empty_for_empty_string(self):
        assert _mkt_code("") == ""

    def test_known_marketplace_returns_code(self):
        # A1PA6795UKMFR9 is DE marketplace in MARKETPLACE_REGISTRY
        code = _mkt_code("A1PA6795UKMFR9")
        assert code  # should return non-empty string like "DE"

    def test_unknown_marketplace_returns_truncated(self):
        assert _mkt_code("UNKNOWNLONG123") == "UNKNO"


class TestNormText:
    """Tests for _norm_text() — text cleanup."""

    def test_strips_whitespace(self):
        assert _norm_text("  hello  ") == "hello"

    def test_replaces_newlines(self):
        assert _norm_text("line1\nline2\rline3") == "line1 line2 line3"

    def test_handles_none(self):
        assert _norm_text(None) == ""


class TestNormInternalSku:
    """Tests for _norm_internal_sku() — removes trailing .0."""

    def test_strips_trailing_dot_zero(self):
        assert _norm_internal_sku("SKU123.0") == "SKU123"

    def test_preserves_normal_sku(self):
        assert _norm_internal_sku("SKU-123-ABC") == "SKU-123-ABC"

    def test_handles_none(self):
        assert _norm_internal_sku(None) == ""


# ---------------------------------------------------------------------------
# 2. simulate_price() from rollup.py
# ---------------------------------------------------------------------------


class TestSimulatePrice:
    """Tests for simulate_price() — pure price/profit calculation."""

    def _call(self, **kw):
        from app.intelligence.profit.rollup import simulate_price
        # Always provide fx_rate to avoid external get_rate_safe() call
        kw.setdefault("fx_rate", 4.30)
        return simulate_price(**kw)

    def test_basic_profit(self):
        r = self._call(sale_price=100, purchase_cost=30)
        assert r["amazon_fee"] == 15.0  # 15% of 100
        assert r["total_cost"] == 45.0  # 30 + 15
        assert r["profit"] == 55.0      # 100 - 45
        assert r["margin_pct"] == 55.0

    def test_all_costs(self):
        r = self._call(
            sale_price=100,
            purchase_cost=20,
            shipping_cost=5,
            amazon_fee_pct=15.0,
            fba_fee=10,
            ad_cost=8,
        )
        # amazon_fee = 15
        # total = 20 + 5 + 15 + 10 + 8 = 58
        assert r["total_cost"] == 58.0
        assert r["profit"] == 42.0
        assert r["margin_pct"] == 42.0

    def test_zero_sale_price(self):
        r = self._call(sale_price=0, purchase_cost=10)
        assert r["margin_pct"] == 0  # no division by zero
        assert r["profit"] == -10.0

    def test_breakeven_formula(self):
        r = self._call(
            sale_price=100,
            purchase_cost=50,
            fba_fee=10,
            amazon_fee_pct=15.0,
        )
        # fixed_costs = 50 + 0 + 10 + 0 = 60
        # breakeven = 60 / (1 - 0.15) = 60 / 0.85 = 70.5882..
        assert r["breakeven_price"] == 70.59

    def test_100_pct_fee_breakeven(self):
        r = self._call(
            sale_price=100,
            purchase_cost=50,
            amazon_fee_pct=100.0,
        )
        # fee_factor = 1 - 1.0 = 0 → breakeven = 0
        assert r["breakeven_price"] == 0

    def test_negative_profit(self):
        r = self._call(sale_price=10, purchase_cost=50)
        assert r["profit"] == -41.5  # 10 - (50 + 1.5)
        assert r["margin_pct"] < 0

    def test_currency_and_fx_passthrough(self):
        r = self._call(sale_price=100, purchase_cost=30, currency="GBP", fx_rate=5.20)
        assert r["currency"] == "GBP"
        assert r["fx_rate"] == 5.20

    def test_all_values_rounded(self):
        r = self._call(sale_price=33.333, purchase_cost=11.111)
        assert r["sale_price"] == 33.33
        assert r["purchase_cost"] == 11.11


# ---------------------------------------------------------------------------
# 3. _allocate_fba_component_costs() from calculator.py
# ---------------------------------------------------------------------------

from app.intelligence.profit.calculator import (
    _allocate_fba_component_costs,
    _allocate_overhead_costs,
)


class TestAllocateFbaComponentCosts:
    """Tests for FBA CM2 pool allocation — pure in-memory algorithm."""

    def _make_product(self, sku="SKU-A", marketplace="MKT1",
                      revenue=100.0, afn_units=10):
        return {
            "group_key": sku,
            "marketplace_id": marketplace,
            "revenue_pln": revenue,
            "afn_units": afn_units,
        }

    def test_empty_products_no_error(self):
        _allocate_fba_component_costs([], {"MKT1": {"storage": 100}})

    def test_empty_pools_zeroes_fields(self):
        p = self._make_product()
        _allocate_fba_component_costs([p], {})
        assert p["fba_storage_fee_pln"] == 0.0
        assert p["fba_removal_fee_pln"] == 0.0

    def test_single_product_gets_full_pool(self):
        p = self._make_product()
        pools = {"MKT1": {"storage": 50.0, "aged": 10.0}}
        _allocate_fba_component_costs([p], pools)
        assert p["fba_storage_fee_pln"] == 50.0
        assert p["fba_aged_fee_pln"] == 10.0

    def test_two_products_revenue_weighted(self):
        p1 = self._make_product(sku="A", revenue=300.0)
        p2 = self._make_product(sku="B", revenue=100.0)
        pools = {"MKT1": {"storage": 100.0}}
        _allocate_fba_component_costs([p1, p2], pools)
        # 300/(300+100) = 75%, 100/(300+100) = 25%
        assert p1["fba_storage_fee_pln"] == 75.0
        assert p2["fba_storage_fee_pln"] == 25.0

    def test_zero_revenue_falls_back_to_afn_units(self):
        p1 = self._make_product(sku="A", revenue=0, afn_units=30)
        p2 = self._make_product(sku="B", revenue=0, afn_units=10)
        pools = {"MKT1": {"storage": 80.0}}
        _allocate_fba_component_costs([p1, p2], pools)
        # 30/(30+10) = 75%, 10/40 = 25%
        assert p1["fba_storage_fee_pln"] == 60.0
        assert p2["fba_storage_fee_pln"] == 20.0

    def test_zero_revenue_and_afn_equal_split(self):
        p1 = self._make_product(sku="A", revenue=0, afn_units=0)
        p2 = self._make_product(sku="B", revenue=0, afn_units=0)
        pools = {"MKT1": {"storage": 100.0}}
        _allocate_fba_component_costs([p1, p2], pools)
        assert p1["fba_storage_fee_pln"] == 50.0
        assert p2["fba_storage_fee_pln"] == 50.0

    def test_multiple_marketplaces_isolated(self):
        p1 = self._make_product(sku="A", marketplace="MKT1", revenue=100.0)
        p2 = self._make_product(sku="B", marketplace="MKT2", revenue=200.0)
        pools = {
            "MKT1": {"storage": 40.0},
            "MKT2": {"storage": 60.0},
        }
        _allocate_fba_component_costs([p1, p2], pools)
        assert p1["fba_storage_fee_pln"] == 40.0  # solo in MKT1
        assert p2["fba_storage_fee_pln"] == 60.0  # solo in MKT2

    def test_external_weight_totals_used(self):
        p1 = self._make_product(sku="A", revenue=50.0)
        pools = {"MKT1": {"storage": 100.0}}
        # External totals say total revenue is 200, so p1 (50) = 25%
        weight_totals = {"MKT1": {"revenue_pln": 200.0, "afn_units": 50}}
        _allocate_fba_component_costs([p1], pools, weight_totals)
        assert p1["fba_storage_fee_pln"] == 25.0


# ---------------------------------------------------------------------------
# 4. _allocate_overhead_costs() from calculator.py
# ---------------------------------------------------------------------------


class TestAllocateOverheadCosts:
    """Tests for overhead cost allocation — pure in-memory algorithm."""

    def _make_product(self, sku="SKU-A", marketplace="MKT1",
                      revenue=100.0, units=10, order_count=5):
        return {
            "group_key": sku,
            "marketplace_id": marketplace,
            "revenue_pln": revenue,
            "units": units,
            "order_count": order_count,
        }

    def _make_pool(self, amount=100.0, method="revenue_share",
                   marketplace="MKT1", confidence=80.0, name="Pool1"):
        return {
            "pool_name": name,
            "amount_pln": amount,
            "allocation_method": method,
            "marketplace_id": marketplace,
            "confidence_pct": confidence,
        }

    def test_empty_products_no_error(self):
        _allocate_overhead_costs([], [self._make_pool()])

    def test_empty_pools_zeroes_fields(self):
        p = self._make_product()
        _allocate_overhead_costs([p], [])
        assert p["overhead_allocated_pln"] == 0.0
        assert p["overhead_allocation_method"] == "none"

    def test_single_product_revenue_share(self):
        p = self._make_product()
        pool = self._make_pool(amount=100.0, method="revenue_share")
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_allocated_pln"] == 100.0
        assert p["overhead_allocation_method"] == "revenue_share"

    def test_units_share_allocation(self):
        p1 = self._make_product(sku="A", units=30)
        p2 = self._make_product(sku="B", units=10)
        pool = self._make_pool(amount=80.0, method="units_share")
        _allocate_overhead_costs([p1, p2], [pool])
        assert p1["overhead_allocated_pln"] == 60.0
        assert p2["overhead_allocated_pln"] == 20.0

    def test_orders_share_allocation(self):
        p1 = self._make_product(sku="A", order_count=3)
        p2 = self._make_product(sku="B", order_count=7)
        pool = self._make_pool(amount=100.0, method="orders_share")
        _allocate_overhead_costs([p1, p2], [pool])
        assert p1["overhead_allocated_pln"] == 30.0
        assert p2["overhead_allocated_pln"] == 70.0

    def test_null_amount_skipped(self):
        p = self._make_product()
        pool = self._make_pool(amount=None)
        pool["amount_pln"] = None  # Ensure it's actually None
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_allocated_pln"] == 0.0

    def test_zero_amount_skipped(self):
        p = self._make_product()
        pool = self._make_pool(amount=0.0)
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_allocated_pln"] == 0.0

    def test_invalid_method_defaults_to_revenue_share(self):
        p = self._make_product()
        pool = self._make_pool(amount=100.0, method="bogus_method")
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_allocated_pln"] == 100.0
        assert p["overhead_allocation_method"] == "revenue_share"

    def test_confidence_calculation(self):
        p = self._make_product()
        pool = self._make_pool(amount=100.0, confidence=80.0)
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_confidence_pct"] == 80.0

    def test_mixed_methods(self):
        p = self._make_product()
        pool1 = self._make_pool(amount=50.0, method="revenue_share", name="P1")
        pool2 = self._make_pool(amount=50.0, method="units_share", name="P2")
        _allocate_overhead_costs([p], [pool1, pool2])
        assert p["overhead_allocated_pln"] == 100.0
        assert p["overhead_allocation_method"] == "mixed"

    def test_no_internal_keys_leak(self):
        p = self._make_product()
        pool = self._make_pool(amount=100.0)
        _allocate_overhead_costs([p], [pool])
        assert "_oh_conf_wsum" not in p
        assert "_oh_amount" not in p
        assert "_oh_methods" not in p
