"""Unit tests for profit calculator allocation functions.

Tests the two pure allocation functions that distribute cost pools to products:
  - _allocate_fba_component_costs: CM2 cost pools (storage, removal, etc.)
  - _allocate_overhead_costs: NP overhead pools (rent, salaries, etc.)

Sprint 8 – S8.5
"""
from __future__ import annotations

import pytest

from app.intelligence.profit.calculator import (
    _allocate_fba_component_costs,
    _allocate_overhead_costs,
)


# ── Helpers ──────────────────────────────────────────────────────────────

def _make_product(
    sku: str = "SKU-A",
    marketplace_id: str = "A1PA6795UKMFR9",
    revenue_pln: float = 1000.0,
    afn_units: int = 10,
    units: int = 10,
    order_count: int = 5,
) -> dict:
    return {
        "group_key": sku,
        "internal_sku": sku,
        "marketplace_id": marketplace_id,
        "revenue_pln": revenue_pln,
        "afn_units": afn_units,
        "units": units,
        "order_count": order_count,
    }


def _make_overhead_pool(
    pool_name: str = "rent",
    amount_pln: float = 100.0,
    allocation_method: str = "revenue_share",
    confidence_pct: float = 80.0,
    marketplace_id: str | None = None,
) -> dict:
    return {
        "pool_name": pool_name,
        "amount_pln": amount_pln,
        "allocation_method": allocation_method,
        "confidence_pct": confidence_pct,
        "marketplace_id": marketplace_id,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  _allocate_fba_component_costs
# ═══════════════════════════════════════════════════════════════════════════

class TestAllocateFbaComponentCosts:
    """Tests for the CM2 cost pool allocator."""

    def test_empty_products_no_crash(self):
        _allocate_fba_component_costs([], {})

    def test_empty_pools_zeros_products(self):
        p = _make_product()
        _allocate_fba_component_costs([p], {})
        assert p["fba_storage_fee_pln"] == 0.0
        assert p["fba_removal_fee_pln"] == 0.0

    def test_single_product_gets_full_pool(self):
        p = _make_product(marketplace_id="M1", revenue_pln=500.0)
        pools = {"M1": {"storage": 100.0, "aged": 50.0}}
        _allocate_fba_component_costs([p], pools)
        assert p["fba_storage_fee_pln"] == pytest.approx(100.0, abs=0.01)
        assert p["fba_aged_fee_pln"] == pytest.approx(50.0, abs=0.01)

    def test_revenue_weighted_split_two_products(self):
        p1 = _make_product(sku="A", marketplace_id="M1", revenue_pln=300.0)
        p2 = _make_product(sku="B", marketplace_id="M1", revenue_pln=700.0)
        pools = {"M1": {"storage": 1000.0}}
        _allocate_fba_component_costs([p1, p2], pools)
        assert p1["fba_storage_fee_pln"] == pytest.approx(300.0, abs=0.01)
        assert p2["fba_storage_fee_pln"] == pytest.approx(700.0, abs=0.01)

    def test_fallback_to_afn_when_no_revenue(self):
        p1 = _make_product(sku="A", marketplace_id="M1", revenue_pln=0.0, afn_units=2)
        p2 = _make_product(sku="B", marketplace_id="M1", revenue_pln=0.0, afn_units=8)
        pools = {"M1": {"storage": 100.0}}
        _allocate_fba_component_costs([p1, p2], pools)
        assert p1["fba_storage_fee_pln"] == pytest.approx(20.0, abs=0.01)
        assert p2["fba_storage_fee_pln"] == pytest.approx(80.0, abs=0.01)

    def test_equal_split_when_no_revenue_no_afn(self):
        p1 = _make_product(sku="A", marketplace_id="M1", revenue_pln=0.0, afn_units=0)
        p2 = _make_product(sku="B", marketplace_id="M1", revenue_pln=0.0, afn_units=0)
        pools = {"M1": {"storage": 100.0}}
        _allocate_fba_component_costs([p1, p2], pools)
        assert p1["fba_storage_fee_pln"] == pytest.approx(50.0, abs=0.01)
        assert p2["fba_storage_fee_pln"] == pytest.approx(50.0, abs=0.01)

    def test_marketplace_isolation(self):
        """Products in different marketplaces only get costs from their pool."""
        p_de = _make_product(sku="DE-1", marketplace_id="M_DE", revenue_pln=100.0)
        p_fr = _make_product(sku="FR-1", marketplace_id="M_FR", revenue_pln=100.0)
        pools = {
            "M_DE": {"storage": 200.0},
            "M_FR": {"storage": 300.0},
        }
        _allocate_fba_component_costs([p_de, p_fr], pools)
        assert p_de["fba_storage_fee_pln"] == pytest.approx(200.0, abs=0.01)
        assert p_fr["fba_storage_fee_pln"] == pytest.approx(300.0, abs=0.01)

    def test_global_all_aggregates_all_pools(self):
        """__ALL__ marketplace row should get sum of all marketplace pools."""
        p_global = _make_product(marketplace_id="__ALL__", revenue_pln=1000.0)
        pools = {
            "M_DE": {"storage": 100.0},
            "M_FR": {"storage": 200.0},
        }
        weight_totals = {
            "M_DE": {"afn_units": 10, "revenue_pln": 500.0},
            "M_FR": {"afn_units": 10, "revenue_pln": 500.0},
        }
        _allocate_fba_component_costs([p_global], pools, weight_totals)
        assert p_global["fba_storage_fee_pln"] == pytest.approx(300.0, abs=0.01)

    def test_all_ten_cm2_keys_zeroed_on_empty_pool(self):
        p = _make_product()
        _allocate_fba_component_costs([p], {})
        for field in [
            "fba_storage_fee_pln", "fba_aged_fee_pln", "fba_removal_fee_pln",
            "fba_liquidation_fee_pln", "refund_finance_pln",
            "shipping_surcharge_pln", "fba_inbound_fee_pln",
            "promo_cost_pln", "warehouse_loss_pln", "amazon_other_fee_pln",
        ]:
            assert p[field] == 0.0, f"{field} should be 0.0"

    def test_external_weight_totals_used(self):
        """When marketplace_weight_totals are provided, they override row sums."""
        p = _make_product(marketplace_id="M1", revenue_pln=200.0, afn_units=5)
        pools = {"M1": {"storage": 100.0}}
        # External total is 1000, so product with 200 revenue gets 20%
        weight_totals = {"M1": {"afn_units": 50, "revenue_pln": 1000.0}}
        _allocate_fba_component_costs([p], pools, weight_totals)
        assert p["fba_storage_fee_pln"] == pytest.approx(20.0, abs=0.01)


# ═══════════════════════════════════════════════════════════════════════════
#  _allocate_overhead_costs
# ═══════════════════════════════════════════════════════════════════════════

class TestAllocateOverheadCosts:
    """Tests for the NP overhead allocator."""

    def test_empty_products_no_crash(self):
        _allocate_overhead_costs([], [])

    def test_empty_pools_zeros_products(self):
        p = _make_product()
        _allocate_overhead_costs([p], [])
        assert p["overhead_allocated_pln"] == 0.0
        assert p["overhead_allocation_method"] == "none"
        assert p["overhead_confidence_pct"] == 0.0

    def test_revenue_share_single_product(self):
        p = _make_product(revenue_pln=1000.0)
        pool = _make_overhead_pool(amount_pln=500.0, allocation_method="revenue_share")
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_allocated_pln"] == pytest.approx(500.0, abs=0.01)
        assert p["overhead_allocation_method"] == "revenue_share"

    def test_units_share_split(self):
        p1 = _make_product(sku="A", units=30)
        p2 = _make_product(sku="B", units=70)
        pool = _make_overhead_pool(amount_pln=1000.0, allocation_method="units_share")
        _allocate_overhead_costs([p1, p2], [pool])
        assert p1["overhead_allocated_pln"] == pytest.approx(300.0, abs=0.01)
        assert p2["overhead_allocated_pln"] == pytest.approx(700.0, abs=0.01)

    def test_orders_share_split(self):
        p1 = _make_product(sku="A", order_count=1)
        p2 = _make_product(sku="B", order_count=4)
        pool = _make_overhead_pool(amount_pln=500.0, allocation_method="orders_share")
        _allocate_overhead_costs([p1, p2], [pool])
        assert p1["overhead_allocated_pln"] == pytest.approx(100.0, abs=0.01)
        assert p2["overhead_allocated_pln"] == pytest.approx(400.0, abs=0.01)

    def test_confidence_weighted_average(self):
        p = _make_product()
        pool1 = _make_overhead_pool(amount_pln=100.0, confidence_pct=80.0)
        pool2 = _make_overhead_pool(amount_pln=200.0, confidence_pct=50.0)
        _allocate_overhead_costs([p], [pool1, pool2])
        # Weighted avg: (100*80 + 200*50) / 300 = 18000/300 = 60
        assert p["overhead_confidence_pct"] == pytest.approx(60.0, abs=0.1)

    def test_null_amount_skipped(self):
        p = _make_product()
        pool = _make_overhead_pool()
        pool["amount_pln"] = None
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_allocated_pln"] == 0.0

    def test_zero_amount_skipped(self):
        p = _make_product()
        pool = _make_overhead_pool(amount_pln=0.0)
        _allocate_overhead_costs([p], [pool])
        assert p["overhead_allocated_pln"] == 0.0

    def test_mixed_methods_label(self):
        p = _make_product()
        pool1 = _make_overhead_pool(amount_pln=100.0, allocation_method="revenue_share")
        pool2 = _make_overhead_pool(amount_pln=100.0, allocation_method="units_share")
        _allocate_overhead_costs([p], [pool1, pool2])
        assert p["overhead_allocation_method"] == "mixed"

    def test_marketplace_scoped_pool(self):
        """A pool with marketplace_id only applies to products in that marketplace."""
        p_de = _make_product(sku="DE", marketplace_id="M_DE", revenue_pln=100.0)
        p_fr = _make_product(sku="FR", marketplace_id="M_FR", revenue_pln=100.0)
        pool = _make_overhead_pool(amount_pln=200.0, marketplace_id="M_DE")
        _allocate_overhead_costs([p_de, p_fr], [pool])
        assert p_de["overhead_allocated_pln"] == pytest.approx(200.0, abs=0.01)
        assert p_fr["overhead_allocated_pln"] == 0.0

    def test_global_pool_applies_to_all(self):
        """A pool with no marketplace_id applies to all products."""
        p_de = _make_product(sku="DE", marketplace_id="M_DE", revenue_pln=300.0)
        p_fr = _make_product(sku="FR", marketplace_id="M_FR", revenue_pln=700.0)
        pool = _make_overhead_pool(amount_pln=1000.0, marketplace_id=None)
        _allocate_overhead_costs([p_de, p_fr], [pool])
        assert p_de["overhead_allocated_pln"] == pytest.approx(300.0, abs=0.01)
        assert p_fr["overhead_allocated_pln"] == pytest.approx(700.0, abs=0.01)

    def test_internal_fields_cleaned_up(self):
        p = _make_product()
        pool = _make_overhead_pool(amount_pln=100.0)
        _allocate_overhead_costs([p], [pool])
        assert "_oh_conf_wsum" not in p
        assert "_oh_amount" not in p
        assert "_oh_methods" not in p

    def test_multiple_pools_accumulate(self):
        p = _make_product(revenue_pln=100.0)
        pool1 = _make_overhead_pool(amount_pln=100.0, confidence_pct=80.0)
        pool2 = _make_overhead_pool(amount_pln=200.0, confidence_pct=80.0)
        _allocate_overhead_costs([p], [pool1, pool2])
        assert p["overhead_allocated_pln"] == pytest.approx(300.0, abs=0.01)
