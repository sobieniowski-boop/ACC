"""
Regression tests for step_bridge_fees() charge-type constants.

Ensures bridge constants stay in sync with FEE_REGISTRY so that
no FBA_FEE / REFERRAL_FEE variants are silently dropped.
"""
from __future__ import annotations

import importlib

import pytest

from app.core.fee_taxonomy import FEE_REGISTRY, FeeCategory

order_pipeline = importlib.import_module("app.services.order_pipeline")

_BRIDGE_ALL_CHARGE_TYPES = getattr(order_pipeline, "_BRIDGE_ALL_CHARGE_TYPES")
_BRIDGE_FBA_CHARGE_TYPES = getattr(order_pipeline, "_BRIDGE_FBA_CHARGE_TYPES")
_BRIDGE_ORDER_ALL_CHARGE_TYPES = getattr(order_pipeline, "_BRIDGE_ORDER_ALL_CHARGE_TYPES")
_BRIDGE_ORDER_PROMO_CHARGE_TYPES = getattr(order_pipeline, "_BRIDGE_ORDER_PROMO_CHARGE_TYPES")
_BRIDGE_ORDER_REFUND_COMMISSION_TYPES = getattr(order_pipeline, "_BRIDGE_ORDER_REFUND_COMMISSION_TYPES")
_BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES = getattr(order_pipeline, "_BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES")
_BRIDGE_REF_CHARGE_TYPES = getattr(order_pipeline, "_BRIDGE_REF_CHARGE_TYPES")
_bridge_all_in_clause = getattr(order_pipeline, "_bridge_all_in_clause")
_bridge_fba_in_clause = getattr(order_pipeline, "_bridge_fba_in_clause")
_bridge_order_all_in_clause = getattr(order_pipeline, "_bridge_order_all_in_clause")
_bridge_order_promo_in_clause = getattr(order_pipeline, "_bridge_order_promo_in_clause")
_bridge_order_refund_commission_in_clause = getattr(order_pipeline, "_bridge_order_refund_commission_in_clause")
_bridge_order_shipping_in_clause = getattr(order_pipeline, "_bridge_order_shipping_in_clause")
_bridge_ref_in_clause = getattr(order_pipeline, "_bridge_ref_in_clause")

# ---------------------------------------------------------------------------
# Expected sets derived from FEE_REGISTRY (source of truth)
# ---------------------------------------------------------------------------
_REGISTRY_FBA = {
    ct
    for ct, entry in FEE_REGISTRY.items()
    if entry.category == FeeCategory.FBA_FEE
    and entry.sign == 1
    and entry.profit_layer is None
}

_REGISTRY_REF = {
    ct
    for ct, entry in FEE_REGISTRY.items()
    if entry.category == FeeCategory.REFERRAL_FEE
    and entry.sign == 1
    and entry.profit_layer is None
}

_REGISTRY_ORDER_SHIPPING = {
    ct
    for ct, entry in FEE_REGISTRY.items()
    if entry.profit_layer == "cm1" and entry.profit_bucket == "shipping_surcharge" and entry.sign == 1
}

_REGISTRY_ORDER_PROMO = {
    ct
    for ct, entry in FEE_REGISTRY.items()
    if entry.profit_layer == "cm1" and entry.profit_bucket == "promo_order" and entry.sign == 1
}

_REGISTRY_ORDER_REFUND_COMMISSION = {
    ct
    for ct, entry in FEE_REGISTRY.items()
    if entry.profit_layer == "cm1" and entry.profit_bucket == "refund_commission" and entry.sign == 1
}


# ---------------------------------------------------------------------------
# 1. Constants match FEE_REGISTRY
# ---------------------------------------------------------------------------
class TestBridgeConstants:
    def test_fba_types_match_registry(self):
        """Every cost-side FBA_FEE in FEE_REGISTRY is in the bridge."""
        assert set(_BRIDGE_FBA_CHARGE_TYPES) == _REGISTRY_FBA

    def test_ref_types_match_registry(self):
        """Every cost-side REFERRAL_FEE in FEE_REGISTRY is in the bridge."""
        assert set(_BRIDGE_REF_CHARGE_TYPES) == _REGISTRY_REF

    def test_all_is_union(self):
        """_BRIDGE_ALL_CHARGE_TYPES is exactly FBA + REF combined."""
        assert set(_BRIDGE_ALL_CHARGE_TYPES) == set(_BRIDGE_FBA_CHARGE_TYPES) | set(
            _BRIDGE_REF_CHARGE_TYPES
        )

    def test_no_duplicates_in_fba(self):
        assert len(_BRIDGE_FBA_CHARGE_TYPES) == len(set(_BRIDGE_FBA_CHARGE_TYPES))

    def test_no_duplicates_in_ref(self):
        assert len(_BRIDGE_REF_CHARGE_TYPES) == len(set(_BRIDGE_REF_CHARGE_TYPES))

    def test_refund_commission_excluded(self):
        """RefundCommission has profit_layer='cm1' — needs separate bridge, not referral."""
        assert "RefundCommission" not in _BRIDGE_ALL_CHARGE_TYPES

    def test_order_shipping_types_match_registry(self):
        assert set(_BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES) == _REGISTRY_ORDER_SHIPPING

    def test_order_promo_types_match_registry(self):
        assert set(_BRIDGE_ORDER_PROMO_CHARGE_TYPES) == _REGISTRY_ORDER_PROMO

    def test_order_refund_commission_matches_registry(self):
        assert set(_BRIDGE_ORDER_REFUND_COMMISSION_TYPES) == _REGISTRY_ORDER_REFUND_COMMISSION

    def test_order_all_is_union(self):
        assert set(_BRIDGE_ORDER_ALL_CHARGE_TYPES) == (
            set(_BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES)
            | set(_BRIDGE_ORDER_PROMO_CHARGE_TYPES)
            | set(_BRIDGE_ORDER_REFUND_COMMISSION_TYPES)
        )


# ---------------------------------------------------------------------------
# 2. IN-clause helpers produce valid SQL fragments
# ---------------------------------------------------------------------------
class TestBridgeInClauses:
    def test_fba_in_clause_format(self):
        clause = _bridge_fba_in_clause()
        for ct in _BRIDGE_FBA_CHARGE_TYPES:
            assert f"'{ct}'" in clause

    def test_ref_in_clause_format(self):
        clause = _bridge_ref_in_clause()
        for ct in _BRIDGE_REF_CHARGE_TYPES:
            assert f"'{ct}'" in clause

    def test_all_in_clause_format(self):
        clause = _bridge_all_in_clause()
        for ct in _BRIDGE_ALL_CHARGE_TYPES:
            assert f"'{ct}'" in clause

    def test_clauses_comma_separated(self):
        clause = _bridge_all_in_clause()
        parts = [p.strip() for p in clause.split(",")]
        assert len(parts) == len(_BRIDGE_ALL_CHARGE_TYPES)

    def test_order_shipping_in_clause_format(self):
        clause = _bridge_order_shipping_in_clause()
        for ct in _BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES:
            assert f"'{ct}'" in clause

    def test_order_promo_in_clause_format(self):
        clause = _bridge_order_promo_in_clause()
        for ct in _BRIDGE_ORDER_PROMO_CHARGE_TYPES:
            assert f"'{ct}'" in clause

    def test_order_refund_commission_in_clause_format(self):
        clause = _bridge_order_refund_commission_in_clause()
        for ct in _BRIDGE_ORDER_REFUND_COMMISSION_TYPES:
            assert f"'{ct}'" in clause

    def test_order_all_in_clause_format(self):
        clause = _bridge_order_all_in_clause()
        for ct in _BRIDGE_ORDER_ALL_CHARGE_TYPES:
            assert f"'{ct}'" in clause


# ---------------------------------------------------------------------------
# 3. Specific charge types that caused BUG #4
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "charge_type",
    [
        "FBAWeightHandlingFee",
        "FBAOrderHandlingFee",
        "FBAPerUnitFulfillment",
        "FBADeliveryServicesFee",
        "ReferralFee",
        "VariableClosingFee",
        "FixedClosingFee",
    ],
)
def test_previously_missing_type_is_bridged(charge_type: str):
    """Charge types that were missing before the BUG #4 fix."""
    assert charge_type in _BRIDGE_ALL_CHARGE_TYPES


@pytest.mark.parametrize(
    "charge_type",
    [
        "ShippingHB",
        "ShippingChargeback",
        "FBAOverSizeSurcharge",
        "CouponRedemptionFee",
        "PrimeExclusiveDiscountFee",
        "SubscribeAndSavePerformanceFee",
        "RefundCommission",
    ],
)
def test_direct_order_cm1_type_is_bridged_to_order(charge_type: str):
    assert charge_type in _BRIDGE_ORDER_ALL_CHARGE_TYPES
