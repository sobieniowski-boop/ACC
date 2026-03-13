"""Unified Amazon Fee Taxonomy.

Single source of truth for every Amazon charge_type → P&L classification
and GL account mapping.  Both the finance ledger (amazon_to_ledger.py) and
the profit engine (_classify_finance_charge) import from here so the two
subsystems can never drift apart.

Taxonomy categories:
    REVENUE         – Sales principal, tax, shipping charge, gift-wrap
    REFERRAL_FEE    – Commission, variable/fixed closing fees, digital services
    FBA_FEE         – Per-unit fulfilment, weight-handling, pick & pack
    FBA_STORAGE     – Monthly / long-term / aged inventory storage
    FBA_INBOUND     – Inbound transport, convenience, placement fees
    FBA_REMOVAL     – Removal orders, disposal
    FBA_LIQUIDATION – Liquidation proceeds / adjustments
    WAREHOUSE_LOSS  – Damage, lost, missing, clawback, SAFE-T reimbursement
    REFUND          – Return postage, restocking, goodwill, concession, HRR
    SHIPPING_SURCHARGE – Heavy/bulky, shipping chargeback
    PROMO_FEE       – Deal, lightning, coupon, Vine fees
    ADS_FEE         – Sponsored Products / Brands / Display cost
    STORAGE_FEE     – (alias into FBA_STORAGE for ledger compat)
    ADJUSTMENT      – Misc adjustments, non-subscription fee adj
    SERVICE_FEE     – Subscription, paid-services, account-level fees
    REGULATORY_FEE  – EPR, digital-services tax, other regulatory
    OTHER_FEE       – AmazonForAll, regulatory catch-alls
    CASH_FLOW       – Reserve debit/credit, failed disbursement (skip)
    UNKNOWN         – Not recognised — triggers alert
"""
from __future__ import annotations

import enum
import structlog
from dataclasses import dataclass
from typing import Any

log = structlog.get_logger(__name__)

# ── Category enum ───────────────────────────────────────────────────────────


class FeeCategory(str, enum.Enum):
    REVENUE = "REVENUE"
    REFERRAL_FEE = "REFERRAL_FEE"
    FBA_FEE = "FBA_FEE"
    FBA_STORAGE = "FBA_STORAGE"
    FBA_INBOUND = "FBA_INBOUND"
    FBA_REMOVAL = "FBA_REMOVAL"
    FBA_LIQUIDATION = "FBA_LIQUIDATION"
    WAREHOUSE_LOSS = "WAREHOUSE_LOSS"
    REFUND = "REFUND"
    SHIPPING_SURCHARGE = "SHIPPING_SURCHARGE"
    PROMO_FEE = "PROMO_FEE"
    ADS_FEE = "ADS_FEE"
    ADJUSTMENT = "ADJUSTMENT"
    SERVICE_FEE = "SERVICE_FEE"
    REGULATORY_FEE = "REGULATORY_FEE"
    OTHER_FEE = "OTHER_FEE"
    CASH_FLOW = "CASH_FLOW"
    UNKNOWN = "UNKNOWN"


# ── Fee entry definition ────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class FeeEntry:
    category: FeeCategory
    gl_account: str               # Chart of accounts code
    gl_tax_code: str | None       # VAT code or None
    profit_layer: str | None      # "cm1", "cm2", "np", or None (skip)
    profit_bucket: str | None     # bucket inside layer, or None
    sign: int                     # +1 = cost, -1 = recovery/credit
    description: str


# ── Master fee registry ─────────────────────────────────────────────────────
# 70+ Amazon charge_type values mapped to unified classification.
# Keys are the EXACT charge_type strings from Amazon Settlement / Finance API.

FEE_REGISTRY: dict[str, FeeEntry] = {
    # ━━ REVENUE (not fees — mapped for GL completeness) ━━━━━━━━━━━━━━━━━━━━
    "Principal":                              FeeEntry(FeeCategory.REVENUE, "700", "VAT0",  None, None, 1, "Item sale principal"),
    "Tax":                                    FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Item sale tax"),
    "ShippingCharge":                         FeeEntry(FeeCategory.REVENUE, "700", "VAT23", None, None, 1, "Buyer-paid shipping"),
    "ShippingTax":                            FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Tax on shipping"),
    "GiftWrap":                               FeeEntry(FeeCategory.REVENUE, "700", "VAT23", None, None, 1, "Gift-wrap charge"),
    "GiftWrapTax":                            FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Tax on gift-wrap"),
    "GiftWrapCharge":                         FeeEntry(FeeCategory.REVENUE, "700", "VAT23", None, None, 1, "Gift-wrap charge (alt)"),
    "PromotionDiscount":                      FeeEntry(FeeCategory.REVENUE, "700", None,    None, None, -1, "Promotion discount"),
    "ShippingDiscount":                       FeeEntry(FeeCategory.REVENUE, "700", None,    None, None, -1, "Shipping discount"),
    "ExportCharge":                           FeeEntry(FeeCategory.REVENUE, "700", None,    None, None, 1, "Cross-border export charge"),
    "PrincipalTax":                           FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Principal tax (alt)"),
    "ShippingChargeTax":                      FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Shipping charge tax (alt)"),
    "MarketplaceFacilitatorVAT-Principal":    FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "MPF VAT on principal"),
    "MarketplaceFacilitatorVAT-Shipping":     FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "MPF VAT on shipping"),
    "MarketplaceFacilitatorVAT-Giftwrap":     FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "MPF VAT on gift-wrap"),
    "LowValueGoodsTax-Principal":             FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Low-value-goods tax (principal)"),
    "LowValueGoodsTax-Shipping":              FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Low-value-goods tax (shipping)"),
    "OtherTransactionFee":                    FeeEntry(FeeCategory.REVENUE, "700", None,    None, None, 1, "Other transaction fee"),
    "MarketplaceWithheldTax":                 FeeEntry(FeeCategory.REVENUE, "220", "VAT23", None, None, 1, "Marketplace withheld tax"),
    "TCSCGSTFee":                             FeeEntry(FeeCategory.REVENUE, "220", None,    None, None, 1, "India TCS CGST"),
    "TCSSGSTFee":                             FeeEntry(FeeCategory.REVENUE, "220", None,    None, None, 1, "India TCS SGST"),
    "TCSIGSTFee":                             FeeEntry(FeeCategory.REVENUE, "220", None,    None, None, 1, "India TCS IGST"),

    # ━━ REFERRAL / COMMISSION FEES (CM1 — already on order_line) ━━━━━━━━━━━
    "Commission":                             FeeEntry(FeeCategory.REFERRAL_FEE, "520", None, None, None, 1, "Referral fee (commission)"),
    "ReferralFee":                            FeeEntry(FeeCategory.REFERRAL_FEE, "520", None, None, None, 1, "Referral fee (alt name)"),
    "VariableClosingFee":                     FeeEntry(FeeCategory.REFERRAL_FEE, "520", None, None, None, 1, "Variable closing fee (media)"),
    "FixedClosingFee":                        FeeEntry(FeeCategory.REFERRAL_FEE, "520", None, None, None, 1, "Fixed closing fee (media)"),
    "DigitalServicesFee":                     FeeEntry(FeeCategory.SERVICE_FEE, "520", None, "np", "service_fee", 1, "Digital services fee"),
    "DigitalServicesFeeFBA":                  FeeEntry(FeeCategory.SERVICE_FEE, "520", None, "np", "service_fee", 1, "Digital services fee (FBA)"),
    "RefundCommission":                       FeeEntry(FeeCategory.REFERRAL_FEE, "520", None, "cm1", "refund_commission", 1, "Refund-processing commission (cost)"),

    # ━━ FBA FULFILMENT FEES (CM1 — already on order_line) ━━━━━━━━━━━━━━━━━━
    "FBAPerUnitFulfillmentFee":               FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA per-unit fulfilment"),
    "FBAPerOrderFulfillmentFee":              FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA per-order fulfilment"),
    "FBAWeightBasedFee":                      FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA weight-based fee"),
    "FBAPickAndPackFee":                      FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA pick & pack"),
    "FBAWeightHandlingFee":                   FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA weight handling"),
    "FBAOrderHandlingFee":                    FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA order handling"),
    "FBAPerUnitFulfillment":                  FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA per-unit (alt)"),
    "FBADeliveryServicesFee":                 FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, "FBA delivery services"),

    # ━━ FBA STORAGE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "StorageFee":                             FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "Monthly storage fee"),
    "StorageRenewalBilling":                  FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "Storage renewal billing"),
    "FBAStorageFee":                          FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "FBA storage fee (alt)"),
    "FBALongTermStorageFee":                  FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_aged", 1, "Long-term storage fee"),
    "FBAAgedInventorySurcharge":              FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_aged", 1, "Aged inventory surcharge"),
    "FBAInventoryStorageOverageFee":          FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "Storage overage fee"),
    "FBAStorageOverageFee":                   FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "Storage overage (alt)"),
    "FBALowInventoryLevelFee":               FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "Low inventory penalty"),

    # ━━ FBA INBOUND ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "FBAInboundConvenienceFee":               FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "Inbound convenience fee"),
    "FBAInboundTransportationFee":            FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "Inbound transportation fee"),
    "FBAInboundTransportFee":                 FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "Inbound transport (alt)"),
    "InventoryPlacementServiceFee":           FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "Inventory placement fee"),
    "FBAInboundDefectFee":                    FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "Inbound defect fee"),

    # ━━ FBA REMOVAL / DISPOSAL ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "FBARemovalFee":                          FeeEntry(FeeCategory.FBA_REMOVAL, "535", None, "cm2", "fba_removal", 1, "Removal order fee"),
    "FBADisposalFee":                         FeeEntry(FeeCategory.FBA_REMOVAL, "535", None, "cm2", "fba_removal", 1, "Disposal fee"),
    "FBARemovalOrderAdjustment":              FeeEntry(FeeCategory.FBA_REMOVAL, "535", None, "cm2", "fba_removal", 1, "Removal adjustment"),
    "RemovalComplete":                        FeeEntry(FeeCategory.FBA_REMOVAL, "535", None, "cm2", "fba_removal", 1, "Removal completion charge"),
    "DisposalComplete":                       FeeEntry(FeeCategory.FBA_REMOVAL, "535", None, "cm2", "fba_removal", 1, "Disposal completion charge"),

    # ━━ FBA LIQUIDATION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "LiquidationsProceeds":                   FeeEntry(FeeCategory.FBA_LIQUIDATION, "535", None, "cm2", "fba_liquidation", -1, "Liquidation proceeds"),
    "LiquidationsRevenueAdjustment":          FeeEntry(FeeCategory.FBA_LIQUIDATION, "535", None, "cm2", "fba_liquidation", -1, "Liquidation revenue adj"),
    "LiquidationsFee":                        FeeEntry(FeeCategory.FBA_LIQUIDATION, "535", None, "cm2", "fba_liquidation", 1, "Liquidation processing fee"),

    # ━━ WAREHOUSE LOSS / DAMAGE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "WAREHOUSE_DAMAGE":                       FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse damage reimbursement"),
    "WAREHOUSE_LOST":                         FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse lost reimbursement"),
    "REMOVAL_ORDER_LOST":                     FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Removal order lost reimbursement"),
    "WarehouseDamage":                        FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse damage (alt)"),
    "WarehouseLost":                          FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse lost (alt)"),
    "MissingFromInbound":                     FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Missing from inbound shipment"),
    "CompensatedClawback":                    FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", 1, "Clawback of reimbursement"),
    "ReversalReimbursement":                  FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", 1, "Reversal of reimbursement"),
    "FBACustomerReturnPerUnitFee":            FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", 1, "Return processing per-unit"),
    "SAFE-TReimbursement":                    FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", 1, "SAFE-T claim reimbursement"),

    # ━━ REFUND / RETURN COSTS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "ReturnPostage":                          FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Return postage charge"),
    "ReturnShipping":                         FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Return shipping (alt name)"),
    "RestockingFee":                          FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", -1, "Restocking fee recovery"),
    "Goodwill":                               FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Goodwill refund cost"),
    "Concession":                             FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Buyer concession"),
    "CustomerReturnHRRUnitFee":               FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "High-return-rate unit fee"),
    "BuyerRecharge":                          FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Buyer recharge"),
    "GiftwrapChargeback":                     FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Gift-wrap chargeback"),
    "Chargeback":                             FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "General chargeback"),
    "ChargebackRefund":                       FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", -1, "Chargeback refund recovery"),
    "ReturnReferralFee":                      FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", -1, "Return referral fee credit"),

    # ━━ SHIPPING SURCHARGES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "ShippingHB":                             FeeEntry(FeeCategory.SHIPPING_SURCHARGE, "530", None, "cm1", "shipping_surcharge", 1, "Heavy/bulky surcharge"),
    "ShippingChargeback":                     FeeEntry(FeeCategory.SHIPPING_SURCHARGE, "530", None, "cm1", "shipping_surcharge", 1, "Shipping chargeback"),
    "FBAOverSizeSurcharge":                   FeeEntry(FeeCategory.SHIPPING_SURCHARGE, "530", None, "cm1", "shipping_surcharge", 1, "Oversize surcharge"),

    # ━━ PROMO / DEAL FEES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "DealParticipationFee":                   FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Deal participation fee"),
    "DealPerformanceFee":                     FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Deal performance fee"),
    "LightningDealFee":                       FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Lightning deal fee"),
    "CouponRedemptionFee":                    FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm1", "promo_order", 1, "Coupon redemption fee"),
    "CouponParticipationFee":                 FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Coupon participation fee"),
    "CouponPerformanceFee":                   FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Coupon performance fee"),
    "VineFee":                                FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Vine programme fee"),
    "VineEnrollmentFee":                      FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Vine enrollment fee"),
    "PrimeExclusiveDiscountFee":              FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm1", "promo_order", 1, "Prime exclusive discount"),
    "SubscribeAndSavePerformanceFee":         FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm1", "promo_order", 1, "Subscribe & Save perf fee"),

    # ━━ ADVERTISING FEES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "CostOfAdvertising":                      FeeEntry(FeeCategory.ADS_FEE, "550", None, "cm2", "ads", 1, "Sponsored ads cost"),
    "AdvertisingCostOfSales":                 FeeEntry(FeeCategory.ADS_FEE, "550", None, "cm2", "ads", 1, "ACOS advertising"),
    "HighVolumeListingFee":                   FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "High volume listing fee"),

    # ━━ ADJUSTMENT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "Adjustment":                             FeeEntry(FeeCategory.ADJUSTMENT, "580", None, "np", "adjustment", 1, "Misc adjustment"),
    "NonSubscriptionFeeAdj":                  FeeEntry(FeeCategory.ADJUSTMENT, "580", None, "np", "adjustment", 1, "Non-subscription fee adj"),
    "MiscAdjustment":                         FeeEntry(FeeCategory.ADJUSTMENT, "580", None, "np", "adjustment", 1, "Miscellaneous adjustment"),
    "PostageBillingAdjustment":               FeeEntry(FeeCategory.ADJUSTMENT, "580", None, "np", "adjustment", 1, "Postage billing adjustment"),
    "FreeReplacementRefundItems":             FeeEntry(FeeCategory.ADJUSTMENT, "580", None, "np", "adjustment", 1, "Free replacement refund"),
    "BalanceAdjustment":                      FeeEntry(FeeCategory.ADJUSTMENT, "580", None, "np", "adjustment", 1, "Balance adjustment"),
    "ItemFeeAdjustment":                      FeeEntry(FeeCategory.ADJUSTMENT, "580", None, "np", "adjustment", 1, "Item fee adjustment"),

    # ━━ SERVICE / SUBSCRIPTION FEES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "Subscription":                           FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Pro seller subscription"),
    "PaidServicesFee":                        FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Paid services fee"),
    "ServiceFee":                             FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Generic service fee"),
    "FBAPrepServiceFee":                      FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "FBA prep service"),
    "FBALabelServiceFee":                     FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "FBA label service"),
    "TranscriptionFee":                       FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Transcription fee"),
    "GlobalSellingAccountFee":                FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Global selling fee"),
    "BrandRegistryIPAcceleratorFee":          FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Brand Registry IP fee"),

    # ━━ REGULATORY FEES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "RegulatoryFee":                          FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "General regulatory fee"),
    "EPRFee":                                 FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "EPR packaging fee"),
    "EPRBatteryFee":                          FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "EPR battery fee"),
    "EPREEEFee":                              FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "EPR EEE fee"),
    "EPRTextileFee":                          FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "EPR textile fee"),
    "EPRFurnitureFee":                        FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "EPR furniture fee"),

    # ━━ OTHER AMAZON FEES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    "AmazonForAllFee":                        FeeEntry(FeeCategory.OTHER_FEE, "580", None, "cm2", "amazon_other_fee", 1, "Amazon For All surcharge"),
    "AmazonForAllReimbursement":              FeeEntry(FeeCategory.OTHER_FEE, "580", None, "cm2", "amazon_other_fee", -1, "Amazon For All refund"),
    "CrossBorderFulfilmentFee":               FeeEntry(FeeCategory.OTHER_FEE, "530", None, "cm2", "fba_inbound", 1, "Cross-border fulfilment"),
    "FBAMultiChannelFulfillmentFee":          FeeEntry(FeeCategory.OTHER_FEE, "530", None, "cm2", "fba_inbound", 1, "MCF fulfilment fee"),

    # ━━ CASH FLOW (not real costs — skip for P&L) ━━━━━━━━━━━━━━━━━━━━━━━━━━
    "ReserveDebit":                           FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Reserve debit"),
    "ReserveCredit":                          FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Reserve credit"),
    "FailedDisbursement":                     FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Failed disbursement"),
    "SuccessfulCharge":                       FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Successful charge"),
    "Payable":                                FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Payable"),
    "CurrentReserveAmount":                   FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Current reserve amount"),
    "PreviousReserveAmount":                  FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Previous reserve amount"),
}


# ── Transaction-type based classification ───────────────────────────────────
# Some charge_types are ambiguous — the transaction_type disambiguates them.

_TRANSACTION_TYPE_OVERRIDES: dict[str, tuple[str, str, int]] = {
    # transaction_type → (layer, bucket, sign)
    "ServiceFeeEventList":     ("np", "service_fee", 1),
    "AdjustmentEventList":     ("np", "adjustment", 1),
}


# ── Fuzzy pattern rules (substring match, case-insensitive) ─────────────────
# Applied when exact match fails.  Order matters — first match wins.
# Each tuple: (substring_pattern, FeeEntry)

_FUZZY_RULES: list[tuple[str, FeeEntry]] = [
    # Storage
    ("LONGTERM",             FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_aged", 1, "Long-term/aged storage (fuzzy)")),
    ("AGED",                 FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_aged", 1, "Aged storage (fuzzy)")),
    ("STORAGEFEE",           FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "Storage fee (fuzzy)")),
    ("STORAGERENEW",         FeeEntry(FeeCategory.FBA_STORAGE, "540", None, "cm2", "fba_storage", 1, "Storage renewal (fuzzy)")),
    # Warehouse
    ("WAREHOUSE_DAMAGE",     FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse damage (fuzzy)")),
    ("WAREHOUSEDAMAGE",      FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse damage (fuzzy)")),
    ("WAREHOUSE_LOST",       FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse lost (fuzzy)")),
    ("WAREHOUSELOST",        FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Warehouse lost (fuzzy)")),
    ("REMOVAL_ORDER_LOST",   FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Removal order lost (fuzzy)")),
    ("REMOVALORDERLOST",     FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", -1, "Removal order lost (fuzzy)")),
    ("COMPENSATED",          FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", 1, "Clawback (fuzzy)")),
    ("REVERSAL",             FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", 1, "Reversal (fuzzy)")),
    ("SAFET",                FeeEntry(FeeCategory.WAREHOUSE_LOSS, "595", None, "cm2", "warehouse_loss", 1, "SAFE-T (fuzzy)")),
    # Removal / disposal
    ("REMOV",                FeeEntry(FeeCategory.FBA_REMOVAL, "535", None, "cm2", "fba_removal", 1, "Removal (fuzzy)")),
    ("DISPOS",               FeeEntry(FeeCategory.FBA_REMOVAL, "535", None, "cm2", "fba_removal", 1, "Disposal (fuzzy)")),
    # Liquidation
    ("LIQUIDAT",             FeeEntry(FeeCategory.FBA_LIQUIDATION, "535", None, "cm2", "fba_liquidation", 1, "Liquidation (fuzzy)")),
    # Return costs
    ("RETURNPOSTAGE",        FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Return postage (fuzzy)")),
    ("RETURNSHIPPING",       FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Return shipping (fuzzy)")),
    ("RESTOCKING",           FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", -1, "Restocking (fuzzy)")),
    ("REFUNDCOMMISSION",     FeeEntry(FeeCategory.REFERRAL_FEE, "520", None, "cm1", "refund_commission", 1, "Refund commission (fuzzy)")),
    ("GOODWILL",             FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Goodwill (fuzzy)")),
    ("CONCESSION",           FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Concession (fuzzy)")),
    ("CUSTOMERRETURNHRR",    FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "HRR (fuzzy)")),
    # Shipping surcharges (CM1 — order-level) — must be before generic CHARGEBACK
    ("SHIPPINGCHARGEBACK",   FeeEntry(FeeCategory.SHIPPING_SURCHARGE, "530", None, "cm1", "shipping_surcharge", 1, "Shipping chargeback (fuzzy)")),
    ("CHARGEBACK",           FeeEntry(FeeCategory.REFUND, "580", None, "cm2", "refund_cost", 1, "Chargeback (fuzzy)")),
    # Shipping surcharges (CM1 — order-level)
    ("SHIPPINGHB",           FeeEntry(FeeCategory.SHIPPING_SURCHARGE, "530", None, "cm1", "shipping_surcharge", 1, "Heavy/bulky (fuzzy)")),
    # FBA inbound
    ("FBAINBOUND",           FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "FBA inbound (fuzzy)")),
    ("INBOUNDTRANS",         FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "Inbound transport (fuzzy)")),
    ("INVENTORYPLACEMENT",   FeeEntry(FeeCategory.FBA_INBOUND, "530", None, "cm2", "fba_inbound", 1, "Inventory placement (fuzzy)")),
    # Promo
    ("DEALFEE",              FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Deal fee (fuzzy)")),
    ("LIGHTNINGDEAL",        FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Lightning deal (fuzzy)")),
    ("COUPON",               FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Coupon (fuzzy)")),
    ("VINE",                 FeeEntry(FeeCategory.PROMO_FEE, "550", None, "cm2", "promo", 1, "Vine (fuzzy)")),
    # Amazon other
    ("AMAZONFORALL",         FeeEntry(FeeCategory.OTHER_FEE, "580", None, "cm2", "amazon_other_fee", 1, "Amazon For All (fuzzy)")),
    ("REGULATORYFEE",        FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "Regulatory (fuzzy)")),
    # EPR
    ("EPR",                  FeeEntry(FeeCategory.REGULATORY_FEE, "570", None, "np", "other_overhead", 1, "EPR (fuzzy)")),
    # Subscription / service
    ("SUBSCRIPTION",         FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Subscription (fuzzy)")),
    ("SERVICEFEE",           FeeEntry(FeeCategory.SERVICE_FEE, "560", None, "np", "service_fee", 1, "Service fee (fuzzy)")),
]


# ── Tracking for unknown fees ───────────────────────────────────────────────

_seen_unknown: set[str] = set()


# ── Public API ──────────────────────────────────────────────────────────────


def classify_fee(
    charge_type: Any,
    transaction_type: Any = None,
) -> FeeEntry:
    """Classify an Amazon charge_type into the unified taxonomy.

    Resolution order:
        1. Exact match in FEE_REGISTRY
        2. Transaction-type override (e.g. AdjustmentEventList → adjustment)
        3. Fuzzy substring match against _FUZZY_RULES
        4. UNKNOWN — logged as warning

    Returns a FeeEntry; never returns None.
    """
    c = str(charge_type or "").strip()
    t = str(transaction_type or "").strip()
    cu = c.upper().replace(" ", "")

    # 1. Exact match
    if c in FEE_REGISTRY:
        return FEE_REGISTRY[c]

    # 2. Transaction-type override
    if t in _TRANSACTION_TYPE_OVERRIDES:
        layer, bucket, sign = _TRANSACTION_TYPE_OVERRIDES[t]
        return FeeEntry(
            FeeCategory.OTHER_FEE, "580", None, layer, bucket, sign,
            f"Classified by transaction_type={t}",
        )

    # 3. Cash-flow skip (prefix match)
    if cu.startswith("RESERVE") or cu == "FAILEDDISBURSEMENT":
        return FeeEntry(FeeCategory.CASH_FLOW, "199", None, None, None, 0, "Cash flow (dynamic)")

    # 4. Fuzzy substring rules
    for pattern, entry in _FUZZY_RULES:
        if pattern in cu:
            return entry

    # 5. Revenue components that arrive with unusual casing
    if cu in {
        "PRINCIPAL", "TAX", "SHIPPINGCHARGE", "SHIPPINGTAX",
        "GIFTWRAP", "GIFTWRAPCHARGE", "GIFTWRAPTAX",
        "PROMOTIONDISCOUNT", "SHIPPINGDISCOUNT", "EXPORTCHARGE",
        "OTHERTRANSACTIONFEE", "PRINCIPALTAX", "SHIPPINGCHARGETAX",
    } or cu.startswith("MARKETPLACEFACILITATORVAT") or cu.startswith("LOWVALUEGOODSTAX"):
        return FeeEntry(FeeCategory.REVENUE, "700", None, None, None, 1, f"Revenue component ({c})")

    # 6. CM1 fees already on order_line
    if cu in {
        "FBAPERUNITFULFILLMENTFEE", "COMMISSION", "FIXEDCLOSINGFEE",
        "VARIABLECLOSINGFEE", "FBAWEIGHTHANDLINGFEE", "FBAORDERHANDLINGFEE",
        "FBAPERORDERFULFILLMENTFEE", "FBAPERUNITFULFILLMENT",
        "REFERRALFEE",
        "FBAWEIGHTBASEDFEE", "FBAPICKANDPACKFEE", "FBADELIVERYSERVICESFEE",
    }:
        if c in FEE_REGISTRY:
            return FEE_REGISTRY[c]
        return FeeEntry(FeeCategory.FBA_FEE, "530", None, None, None, 1, f"CM1 fee ({c})")

    # 7. Unknown — alert + suspend (SF-16: sign=0, not +1)
    _alert_unknown_fee(c, t)
    return FeeEntry(
        FeeCategory.UNKNOWN, "599", None, "np", "other_overhead", 0,
        f"UNKNOWN fee type: {c} — suspended (sign=0), needs manual classification",
    )


def _alert_unknown_fee(charge_type: str, transaction_type: str) -> None:
    """Log a warning the first time an unknown fee type is encountered."""
    key = f"{charge_type}|{transaction_type}"
    if key in _seen_unknown:
        return
    _seen_unknown.add(key)
    log.warning(
        "fee_taxonomy.unknown_fee_type",
        charge_type=charge_type,
        transaction_type=transaction_type,
        msg="Unrecognised Amazon fee type — add to FEE_REGISTRY in core/fee_taxonomy.py",
    )


def get_ledger_rule(charge_type: Any, transaction_type: Any = None) -> tuple[str, str | None, float]:
    """Return (gl_account, tax_code, sign_multiplier) for GL posting.

    Direct replacement for amazon_to_ledger.resolve_mapping_rule
    backed by the unified taxonomy.
    SF-16: sign=0 (unknown/suspended) is preserved — callers must handle it.
    """
    entry = classify_fee(charge_type, transaction_type)
    return entry.gl_account, entry.gl_tax_code, float(entry.sign)


def get_profit_classification(
    charge_type: Any,
    transaction_type: Any = None,
) -> dict[str, Any] | None:
    """Return profit-engine classification dict or None (skip).

    Direct replacement for profit_engine._classify_finance_charge
    backed by the unified taxonomy.

    Returns:
        {"layer": "cm2"|"np", "bucket": str, "sign": 1|-1} or None
    """
    entry = classify_fee(charge_type, transaction_type)
    if entry.profit_layer is None:
        return None
    return {
        "layer": entry.profit_layer,
        "bucket": entry.profit_bucket or "other_overhead",
        "sign": entry.sign,
    }


def list_categories() -> list[str]:
    """Return all fee category names."""
    return [c.value for c in FeeCategory]


def list_fees_by_category(category: FeeCategory) -> dict[str, FeeEntry]:
    """Return all FEE_REGISTRY entries for a given category."""
    return {k: v for k, v in FEE_REGISTRY.items() if v.category == category}


def registry_stats() -> dict[str, int]:
    """Return count of mapped fees per category."""
    counts: dict[str, int] = {}
    for entry in FEE_REGISTRY.values():
        counts[entry.category.value] = counts.get(entry.category.value, 0) + 1
    return counts


def charge_types_for_bucket(bucket: str) -> list[str]:
    """Return all FEE_REGISTRY charge_type keys whose profit_bucket matches *bucket*."""
    return [k for k, v in FEE_REGISTRY.items() if v.profit_bucket == bucket]


def charge_types_for_layer(layer: str) -> list[str]:
    """Return all FEE_REGISTRY charge_type keys whose profit_layer matches *layer*."""
    return [k for k, v in FEE_REGISTRY.items() if v.profit_layer == layer]


def rollup_bucket_map() -> dict[str, dict[str, list[str]]]:
    """Return {layer: {bucket: [charge_type, ...]}} for all classified fees.

    Used by profitability_service to build SQL IN-lists from the canonical
    taxonomy instead of hardcoded charge_type constants.
    """
    result: dict[str, dict[str, list[str]]] = {}
    for ct, entry in FEE_REGISTRY.items():
        if entry.profit_layer is None:
            continue
        layer = entry.profit_layer
        bucket = entry.profit_bucket or "other"
        result.setdefault(layer, {}).setdefault(bucket, []).append(ct)
    return result
