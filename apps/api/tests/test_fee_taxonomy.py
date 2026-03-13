"""Tests for the unified Amazon fee taxonomy.

Validates:
  - Exact-match classification for all major fee categories
  - Fuzzy/substring classification for variant fee names
  - Unknown fee alerting (structlog warning on first encounter)
  - GL account mapping consistency between taxonomy and ledger
  - Profit-engine classification parity (same result via taxonomy and old code path)
  - Registry completeness (70+ entries)
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from app.core.fee_taxonomy import (
    FEE_REGISTRY,
    FeeCategory,
    FeeEntry,
    classify_fee,
    get_ledger_rule,
    get_profit_classification,
    registry_stats,
    _seen_unknown,
)


# ── Registry size & structure ───────────────────────────────────────────────

class TestRegistryCompleteness:
    def test_at_least_70_entries(self):
        assert len(FEE_REGISTRY) >= 70, f"Only {len(FEE_REGISTRY)} entries"

    def test_all_entries_are_fee_entry(self):
        for key, entry in FEE_REGISTRY.items():
            assert isinstance(entry, FeeEntry), f"{key} is not FeeEntry"

    def test_all_categories_present(self):
        used = {e.category for e in FEE_REGISTRY.values()}
        for cat in (FeeCategory.REVENUE, FeeCategory.FBA_FEE,
                    FeeCategory.REFERRAL_FEE, FeeCategory.FBA_STORAGE,
                    FeeCategory.REFUND, FeeCategory.ADJUSTMENT,
                    FeeCategory.CASH_FLOW):
            assert cat in used, f"Missing category: {cat.value}"

    def test_registry_stats(self):
        stats = registry_stats()
        assert sum(stats.values()) == len(FEE_REGISTRY)


# ── Exact-match classification ──────────────────────────────────────────────

class TestExactMatch:
    @pytest.mark.parametrize("charge_type,expected_category", [
        ("Principal",                    FeeCategory.REVENUE),
        ("Tax",                          FeeCategory.REVENUE),
        ("ShippingCharge",               FeeCategory.REVENUE),
        ("Commission",                   FeeCategory.REFERRAL_FEE),
        ("ReferralFee",                  FeeCategory.REFERRAL_FEE),
        ("VariableClosingFee",           FeeCategory.REFERRAL_FEE),
        ("FixedClosingFee",              FeeCategory.REFERRAL_FEE),
        ("RefundCommission",             FeeCategory.REFERRAL_FEE),
        ("FBAPerUnitFulfillmentFee",     FeeCategory.FBA_FEE),
        ("FBAPerOrderFulfillmentFee",    FeeCategory.FBA_FEE),
        ("FBAWeightBasedFee",            FeeCategory.FBA_FEE),
        ("FBAPickAndPackFee",            FeeCategory.FBA_FEE),
        ("FBADeliveryServicesFee",       FeeCategory.FBA_FEE),
        ("StorageFee",                   FeeCategory.FBA_STORAGE),
        ("FBALongTermStorageFee",        FeeCategory.FBA_STORAGE),
        ("FBAAgedInventorySurcharge",    FeeCategory.FBA_STORAGE),
        ("FBALowInventoryLevelFee",      FeeCategory.FBA_STORAGE),
        ("FBAInboundConvenienceFee",     FeeCategory.FBA_INBOUND),
        ("FBAInboundTransportationFee",  FeeCategory.FBA_INBOUND),
        ("InventoryPlacementServiceFee", FeeCategory.FBA_INBOUND),
        ("FBARemovalFee",                FeeCategory.FBA_REMOVAL),
        ("FBADisposalFee",               FeeCategory.FBA_REMOVAL),
        ("LiquidationsProceeds",         FeeCategory.FBA_LIQUIDATION),
        ("WAREHOUSE_DAMAGE",             FeeCategory.WAREHOUSE_LOSS),
        ("WAREHOUSE_LOST",               FeeCategory.WAREHOUSE_LOSS),
        ("CompensatedClawback",          FeeCategory.WAREHOUSE_LOSS),
        ("ReturnPostage",                FeeCategory.REFUND),
        ("RestockingFee",                FeeCategory.REFUND),
        ("Goodwill",                     FeeCategory.REFUND),
        ("Chargeback",                   FeeCategory.REFUND),
        ("ShippingHB",                   FeeCategory.SHIPPING_SURCHARGE),
        ("DealParticipationFee",         FeeCategory.PROMO_FEE),
        ("LightningDealFee",             FeeCategory.PROMO_FEE),
        ("CouponRedemptionFee",          FeeCategory.PROMO_FEE),
        ("VineFee",                      FeeCategory.PROMO_FEE),
        ("CostOfAdvertising",            FeeCategory.ADS_FEE),
        ("Adjustment",                   FeeCategory.ADJUSTMENT),
        ("MiscAdjustment",               FeeCategory.ADJUSTMENT),
        ("Subscription",                 FeeCategory.SERVICE_FEE),
        ("FBAPrepServiceFee",            FeeCategory.SERVICE_FEE),
        ("RegulatoryFee",                FeeCategory.REGULATORY_FEE),
        ("EPRFee",                       FeeCategory.REGULATORY_FEE),
        ("AmazonForAllFee",              FeeCategory.OTHER_FEE),
        ("ReserveDebit",                 FeeCategory.CASH_FLOW),
        ("FailedDisbursement",           FeeCategory.CASH_FLOW),
    ])
    def test_exact_match_category(self, charge_type, expected_category):
        entry = classify_fee(charge_type)
        assert entry.category == expected_category, (
            f"{charge_type}: got {entry.category}, expected {expected_category}"
        )


# ── Fuzzy/substring classification ──────────────────────────────────────────

class TestFuzzyMatch:
    @pytest.mark.parametrize("charge_type,expected_bucket", [
        ("FBALongTermStorageFeeV2",     "fba_aged"),
        ("AgedInventorySurcharge2025",  "fba_aged"),
        ("WarehouseDamageNew",          "warehouse_loss"),
        ("RemovalCompleteFBA",          "fba_removal"),
        ("DisposalProcessing",          "fba_removal"),
        ("LiquidationSaleProceeds",     "fba_liquidation"),
        ("ReturnPostagePrime",          "refund_cost"),
        ("RestockingFeeRefund",         "refund_cost"),
        ("GoodwillRefund",              "refund_cost"),
        ("ShippingHBOversize",          "shipping_surcharge"),
        ("FBAInboundPlacement2025",     "fba_inbound"),
        ("LightningDealFee2025",        "promo"),
        ("CouponRedemption",            "promo"),
        ("VineEnrollmentPremium",       "promo"),
        ("AmazonForAllSurchargeEU",     "amazon_other_fee"),
        ("EPRPackagingDE",              "other_overhead"),
        ("SubscriptionMonthly",         "service_fee"),
    ])
    def test_fuzzy_match_bucket(self, charge_type, expected_bucket):
        entry = classify_fee(charge_type)
        assert entry.profit_bucket == expected_bucket, (
            f"{charge_type}: got bucket={entry.profit_bucket}, expected={expected_bucket}"
        )


# ── Revenue / CM1 skip (returns None for profit layer) ─────────────────────

class TestSkippedFees:
    @pytest.mark.parametrize("charge_type", [
        "Principal", "Tax", "ShippingCharge", "GiftWrap",
        "PromotionDiscount", "ExportCharge",
        "MarketplaceFacilitatorVAT-Principal",
        "LowValueGoodsTax-Shipping",
    ])
    def test_revenue_returns_no_profit_layer(self, charge_type):
        result = get_profit_classification(charge_type)
        assert result is None, f"{charge_type} should be None for profit"

    @pytest.mark.parametrize("charge_type", [
        "ReserveDebit", "ReserveCredit", "FailedDisbursement",
    ])
    def test_cash_flow_returns_no_profit_layer(self, charge_type):
        result = get_profit_classification(charge_type)
        assert result is None

    @pytest.mark.parametrize("charge_type", [
        "FBAPerUnitFulfillmentFee", "Commission", "FixedClosingFee",
        "VariableClosingFee",
    ])
    def test_cm1_fees_return_no_profit_layer(self, charge_type):
        result = get_profit_classification(charge_type)
        assert result is None, f"CM1 fee {charge_type} should be None"


# ── Profit classification sign correctness ──────────────────────────────────

class TestProfitSign:
    def test_storage_is_cost(self):
        r = get_profit_classification("StorageFee")
        assert r is not None and r["sign"] == 1

    def test_warehouse_damage_is_recovery(self):
        r = get_profit_classification("WAREHOUSE_DAMAGE")
        assert r is not None and r["sign"] == -1

    def test_restocking_is_recovery(self):
        r = get_profit_classification("RestockingFee")
        assert r is not None and r["sign"] == -1

    def test_refund_commission_is_recovery(self):
        r = get_profit_classification("RefundCommission")
        assert r is not None and r["sign"] == 1

    def test_clawback_is_cost(self):
        r = get_profit_classification("CompensatedClawback")
        assert r is not None and r["sign"] == 1


# ── GL account mapping ──────────────────────────────────────────────────────

class TestGLMapping:
    def test_revenue_gl(self):
        acct, tax, _ = get_ledger_rule("Principal")
        assert acct == "700"
        assert tax == "VAT0"

    def test_commission_gl(self):
        acct, tax, _ = get_ledger_rule("Commission")
        assert acct == "520"

    def test_fba_fee_gl(self):
        acct, _, _ = get_ledger_rule("FBAPerUnitFulfillmentFee")
        assert acct == "530"

    def test_storage_gl(self):
        acct, _, _ = get_ledger_rule("StorageFee")
        assert acct == "540"

    def test_ads_gl(self):
        acct, _, _ = get_ledger_rule("CostOfAdvertising")
        assert acct == "550"

    def test_adjustment_gl(self):
        acct, _, _ = get_ledger_rule("Adjustment")
        assert acct == "580"

    def test_unknown_gl(self):
        _seen_unknown.clear()
        acct, _, _ = get_ledger_rule("CompletelyNewFee2025")
        assert acct == "599"


# ── Ledger parity — ensure resolve_mapping_rule uses taxonomy ───────────────

class TestLedgerParity:
    def test_default_rules_from_taxonomy(self):
        from app.services.finance_center.mappers.amazon_to_ledger import DEFAULT_RULES
        # Every key in DEFAULT_RULES should correspond to an FEE_REGISTRY entry
        for key in DEFAULT_RULES:
            assert key in FEE_REGISTRY, f"Ledger key '{key}' not in taxonomy"

    def test_resolve_falls_through_to_taxonomy(self):
        from app.services.finance_center.mappers.amazon_to_ledger import resolve_mapping_rule
        _seen_unknown.clear()
        rule = resolve_mapping_rule("FBAInboundDefectFee")
        assert rule.account_code == "530"

    def test_unknown_falls_through(self):
        from app.services.finance_center.mappers.amazon_to_ledger import resolve_mapping_rule
        _seen_unknown.clear()
        rule = resolve_mapping_rule("TotallyNewFeeType")
        assert rule.account_code == "599"


# ── Unknown fee alerting ────────────────────────────────────────────────────

class TestUnknownAlert:
    def test_unknown_fee_logs_warning(self):
        _seen_unknown.clear()
        with patch("app.core.fee_taxonomy.log") as mock_log:
            classify_fee("NeverSeenBefore2025")
            mock_log.warning.assert_called_once()
            call_kwargs = mock_log.warning.call_args
            assert "NeverSeenBefore2025" in str(call_kwargs)

    def test_unknown_fee_logged_only_once(self):
        _seen_unknown.clear()
        with patch("app.core.fee_taxonomy.log") as mock_log:
            classify_fee("RepeatedUnknown")
            classify_fee("RepeatedUnknown")
            assert mock_log.warning.call_count == 1

    def test_unknown_returns_unknown_category(self):
        _seen_unknown.clear()
        entry = classify_fee("CompletelyNotMapped")
        assert entry.category == FeeCategory.UNKNOWN

    def test_unknown_has_profit_layer(self):
        _seen_unknown.clear()
        r = get_profit_classification("UnknownXYZ123")
        assert r is not None
        assert r["layer"] == "np"
        assert r["bucket"] == "other_overhead"


# ── Transaction-type override ───────────────────────────────────────────────

class TestTransactionTypeOverride:
    def test_service_fee_event_list(self):
        entry = classify_fee("SomeRandomCharge", "ServiceFeeEventList")
        assert entry.profit_layer == "np"
        assert entry.profit_bucket == "service_fee"

    def test_adjustment_event_list(self):
        entry = classify_fee("SomeRandomCharge", "AdjustmentEventList")
        assert entry.profit_layer == "np"
        assert entry.profit_bucket == "adjustment"


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1 — P&L layer reclassification regression tests
# ═══════════════════════════════════════════════════════════════════════════


class TestPhase1_ShippingSurcharges_CM1:
    """Shipping surcharges must be CM1, not CM2."""

    @pytest.mark.parametrize("charge_type", [
        "ShippingHB", "ShippingChargeback", "FBAOverSizeSurcharge",
    ])
    def test_layer_is_cm1(self, charge_type):
        r = get_profit_classification(charge_type)
        assert r is not None
        assert r["layer"] == "cm1", f"{charge_type}: expected cm1, got {r['layer']}"
        assert r["bucket"] == "shipping_surcharge"

    def test_fuzzy_shipping_hb_is_cm1(self):
        r = get_profit_classification("ShippingHBOversize")
        assert r is not None and r["layer"] == "cm1"


class TestPhase1_OrderLevelPromos_CM1:
    """Order-level promo fees must be CM1/promo_order."""

    @pytest.mark.parametrize("charge_type", [
        "CouponRedemptionFee", "PrimeExclusiveDiscountFee",
        "SubscribeAndSavePerformanceFee",
    ])
    def test_layer_is_cm1(self, charge_type):
        r = get_profit_classification(charge_type)
        assert r is not None
        assert r["layer"] == "cm1", f"{charge_type}: expected cm1, got {r['layer']}"
        assert r["bucket"] == "promo_order"


class TestPhase1_ProgramPromos_StayCM2:
    """Program-level promo fees must remain CM2/promo."""

    @pytest.mark.parametrize("charge_type", [
        "DealParticipationFee", "DealPerformanceFee", "LightningDealFee",
        "CouponParticipationFee", "CouponPerformanceFee",
        "VineFee", "VineEnrollmentFee",
    ])
    def test_layer_is_cm2(self, charge_type):
        r = get_profit_classification(charge_type)
        assert r is not None
        assert r["layer"] == "cm2", f"{charge_type}: expected cm2, got {r['layer']}"
        assert r["bucket"] == "promo"


class TestPhase1_DigitalServicesFee_NP:
    """DigitalServicesFee / DigitalServicesFeeFBA must be NP/service_fee."""

    @pytest.mark.parametrize("charge_type", [
        "DigitalServicesFee", "DigitalServicesFeeFBA",
    ])
    def test_layer_is_np(self, charge_type):
        r = get_profit_classification(charge_type)
        assert r is not None
        assert r["layer"] == "np", f"{charge_type}: expected np, got {r['layer']}"
        assert r["bucket"] == "service_fee"


class TestPhase1_RegulatoryFee_NP:
    """RegulatoryFee must be NP/other_overhead (like EPR)."""

    def test_regulatory_fee_is_np(self):
        r = get_profit_classification("RegulatoryFee")
        assert r is not None
        assert r["layer"] == "np"
        assert r["bucket"] == "other_overhead"

    def test_fuzzy_regulatory_is_np(self):
        r = get_profit_classification("RegulatoryFeeNew2026")
        assert r is not None
        assert r["layer"] == "np"
        assert r["bucket"] == "other_overhead"


class TestPhase1_HighVolumeListingFee_NP:
    """HighVolumeListingFee must be NP/service_fee (not ADS/promo)."""

    def test_layer_is_np(self):
        r = get_profit_classification("HighVolumeListingFee")
        assert r is not None
        assert r["layer"] == "np"
        assert r["bucket"] == "service_fee"

    def test_category_is_service_fee(self):
        entry = classify_fee("HighVolumeListingFee")
        assert entry.category == FeeCategory.SERVICE_FEE


class TestPhase1_AdsFees_CM2Ads:
    """CostOfAdvertising / AdvertisingCostOfSales must be CM2/ads."""

    @pytest.mark.parametrize("charge_type", [
        "CostOfAdvertising", "AdvertisingCostOfSales",
    ])
    def test_layer_is_cm2_ads(self, charge_type):
        r = get_profit_classification(charge_type)
        assert r is not None
        assert r["layer"] == "cm2"
        assert r["bucket"] == "ads", f"{charge_type}: expected ads, got {r['bucket']}"


class TestPhase1_RefundCommission_CM1:
    """RefundCommission must be CM1/refund_commission as a direct cost."""

    def test_layer_is_cm1(self):
        r = get_profit_classification("RefundCommission")
        assert r is not None
        assert r["layer"] == "cm1"
        assert r["bucket"] == "refund_commission"
        assert r["sign"] == 1

    def test_fuzzy_refund_commission_is_cm1(self):
        r = get_profit_classification("RefundCommissionAlt")
        assert r is not None
        assert r["layer"] == "cm1"
        assert r["bucket"] == "refund_commission"


class TestPhase1_Liquidations_Sign:
    """LiquidationsProceeds / LiquidationsRevenueAdjustment must have sign=-1."""

    @pytest.mark.parametrize("charge_type", [
        "LiquidationsProceeds", "LiquidationsRevenueAdjustment",
    ])
    def test_sign_is_negative(self, charge_type):
        r = get_profit_classification(charge_type)
        assert r is not None
        assert r["sign"] == -1, f"{charge_type}: expected sign=-1, got {r['sign']}"

    def test_liquidation_fee_sign_is_positive(self):
        """LiquidationsFee (processing cost) stays sign=+1."""
        r = get_profit_classification("LiquidationsFee")
        assert r is not None
        assert r["sign"] == 1
