"""ACC Canonical P&L (Profit & Loss) Model.

Every financial computation in ACC must flow through this model.
This is the single source of truth for the P&L waterfall:

    Revenue
    ├── item_price + buyer_paid_shipping − tax − promotion_discount
    │
    CM1 — Contribution Margin 1 (order-level, HIGH confidence)
    ├── Revenue
    │   − COGS (purchase_price × qty)
    │   − Amazon referral fees
    │   − Amazon FBA fulfillment fees
    │   − Courier / logistics cost
    │   − Shipping surcharges (heavy/bulky)
    │   − Order-level promo / coupons
    │   − Refund commission
    │
    │   → "Did the sale of this order make sense operationally?"
    │
    CM2 — Contribution Margin 2 (event-linked + allocated, MEDIUM confidence)
    ├── CM1
    │   − Advertising cost (PPC)
    │   − FBA storage fees
    │   − FBA aged inventory surcharge
    │   − FBA removal fees
    │   − FBA liquidation fees
    │   − Warehouse loss / damage
    │   − Return / refund net cost
    │   − Other indirect operating costs
    │
    │   → "Was the sale healthy for the business under normal operations?"
    │
    NP — Net Profit (periodic/shared, LOW confidence)
    ├── CM2
    │   − SaaS / tools subscriptions
    │   − Digital services tax
    │   − Regulatory fees (EPR, compliance)
    │   − Other overheads
    │
    │   → "How much does the company really earn?"

The fee_taxonomy module classifies each Amazon charge_type into the correct
P&L layer and bucket.  This model defines the structure and computation rules.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ProfitLayer(str, Enum):
    """P&L waterfall layers."""
    REVENUE = "revenue"
    CM1 = "cm1"
    CM2 = "cm2"
    NP = "np"


class Confidence(str, Enum):
    """Data confidence level for each margin tier."""
    HIGH = "high"       # CM1 — order-matchable, deterministic
    MEDIUM = "medium"   # CM2 — event-linked + allocated
    LOW = "low"         # NP  — periodic/shared, estimated


# ── CM1 cost buckets (order-matchable) ──────────────────────────────────────

@dataclass(frozen=True, slots=True)
class CM1Costs:
    """All direct costs deducted from Revenue to produce CM1."""
    cogs_pln: float = 0.0
    referral_fee_pln: float = 0.0
    fba_fee_pln: float = 0.0
    logistics_pln: float = 0.0
    shipping_surcharge_pln: float = 0.0
    promo_order_fee_pln: float = 0.0
    refund_commission_pln: float = 0.0

    @property
    def total(self) -> float:
        return (
            self.cogs_pln
            + self.referral_fee_pln
            + self.fba_fee_pln
            + self.logistics_pln
            + self.shipping_surcharge_pln
            + self.promo_order_fee_pln
            + self.refund_commission_pln
        )


# ── CM2 cost buckets (event-linked + allocated) ────────────────────────────

@dataclass(frozen=True, slots=True)
class CM2Costs:
    """Indirect operating costs deducted from CM1 to produce CM2."""
    ads_cost_pln: float = 0.0
    fba_storage_fee_pln: float = 0.0
    fba_aged_fee_pln: float = 0.0
    fba_removal_fee_pln: float = 0.0
    fba_liquidation_fee_pln: float = 0.0
    warehouse_loss_pln: float = 0.0
    return_net_cost_pln: float = 0.0
    other_cm2_pln: float = 0.0

    @property
    def total(self) -> float:
        return (
            self.ads_cost_pln
            + self.fba_storage_fee_pln
            + self.fba_aged_fee_pln
            + self.fba_removal_fee_pln
            + self.fba_liquidation_fee_pln
            + self.warehouse_loss_pln
            + self.return_net_cost_pln
            + self.other_cm2_pln
        )


# ── NP cost buckets (overhead / shared) ─────────────────────────────────────

@dataclass(frozen=True, slots=True)
class NPCosts:
    """Overhead / shared costs deducted from CM2 to produce NP."""
    saas_pln: float = 0.0
    digital_services_tax_pln: float = 0.0
    regulatory_fee_pln: float = 0.0
    subscription_pln: float = 0.0
    other_overhead_pln: float = 0.0

    @property
    def total(self) -> float:
        return (
            self.saas_pln
            + self.digital_services_tax_pln
            + self.regulatory_fee_pln
            + self.subscription_pln
            + self.other_overhead_pln
        )


# ── Full P&L record ─────────────────────────────────────────────────────────

@dataclass(slots=True)
class ProfitRecord:
    """Complete P&L waterfall for a single entity (order, SKU, marketplace, period).

    This is the canonical output of any profit calculation in ACC.
    """
    # Revenue
    item_price_pln: float = 0.0
    shipping_charge_pln: float = 0.0
    tax_pln: float = 0.0
    promotion_discount_pln: float = 0.0

    # Cost structures
    cm1_costs: CM1Costs = field(default_factory=CM1Costs)
    cm2_costs: CM2Costs = field(default_factory=CM2Costs)
    np_costs: NPCosts = field(default_factory=NPCosts)

    # Quantity info for per-unit calculations
    units: int = 0

    # ── Computed properties ──────────────────────────────────────────────

    @property
    def revenue(self) -> float:
        """Net revenue = item_price + shipping − tax − promotion."""
        return (
            self.item_price_pln
            + self.shipping_charge_pln
            - self.tax_pln
            - self.promotion_discount_pln
        )

    @property
    def cm1(self) -> float:
        """Contribution Margin 1 = Revenue − direct order costs."""
        return self.revenue - self.cm1_costs.total

    @property
    def cm1_pct(self) -> float:
        """CM1 as percentage of revenue."""
        return (self.cm1 / self.revenue * 100) if self.revenue else 0.0

    @property
    def cm2(self) -> float:
        """Contribution Margin 2 = CM1 − indirect operating costs."""
        return self.cm1 - self.cm2_costs.total

    @property
    def cm2_pct(self) -> float:
        """CM2 as percentage of revenue."""
        return (self.cm2 / self.revenue * 100) if self.revenue else 0.0

    @property
    def net_profit(self) -> float:
        """Net Profit = CM2 − overhead costs."""
        return self.cm2 - self.np_costs.total

    @property
    def np_pct(self) -> float:
        """NP as percentage of revenue."""
        return (self.net_profit / self.revenue * 100) if self.revenue else 0.0

    @property
    def cm1_per_unit(self) -> float:
        return (self.cm1 / self.units) if self.units else 0.0

    @property
    def cm2_per_unit(self) -> float:
        return (self.cm2 / self.units) if self.units else 0.0

    @property
    def np_per_unit(self) -> float:
        return (self.net_profit / self.units) if self.units else 0.0

    @property
    def tacos(self) -> float:
        """Total Advertising Cost of Sale = ads / revenue × 100."""
        return (self.cm2_costs.ads_cost_pln / self.revenue * 100) if self.revenue else 0.0

    @property
    def confidence(self) -> Confidence:
        """Confidence level based on data completeness."""
        if self.cm1_costs.cogs_pln == 0 and self.units > 0:
            return Confidence.LOW  # Missing COGS = unreliable
        if self.cm2_costs.total == 0 and self.revenue > 0:
            return Confidence.MEDIUM  # CM2 not allocated yet
        return Confidence.HIGH

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for API responses and DB writes."""
        return {
            # Revenue
            "revenue_pln": round(self.revenue, 2),
            "item_price_pln": round(self.item_price_pln, 2),
            "shipping_charge_pln": round(self.shipping_charge_pln, 2),
            "vat_pln": round(self.tax_pln, 2),
            "promotion_discount_pln": round(self.promotion_discount_pln, 2),
            # CM1
            "cm1_pln": round(self.cm1, 2),
            "cm1_pct": round(self.cm1_pct, 2),
            "cogs_pln": round(self.cm1_costs.cogs_pln, 2),
            "referral_fee_pln": round(self.cm1_costs.referral_fee_pln, 2),
            "fba_fee_pln": round(self.cm1_costs.fba_fee_pln, 2),
            "logistics_pln": round(self.cm1_costs.logistics_pln, 2),
            "shipping_surcharge_pln": round(self.cm1_costs.shipping_surcharge_pln, 2),
            "promo_order_fee_pln": round(self.cm1_costs.promo_order_fee_pln, 2),
            "refund_commission_pln": round(self.cm1_costs.refund_commission_pln, 2),
            # CM2
            "cm2_pln": round(self.cm2, 2),
            "cm2_pct": round(self.cm2_pct, 2),
            "ads_cost_pln": round(self.cm2_costs.ads_cost_pln, 2),
            "fba_storage_fee_pln": round(self.cm2_costs.fba_storage_fee_pln, 2),
            "fba_aged_fee_pln": round(self.cm2_costs.fba_aged_fee_pln, 2),
            "fba_removal_fee_pln": round(self.cm2_costs.fba_removal_fee_pln, 2),
            "fba_liquidation_fee_pln": round(self.cm2_costs.fba_liquidation_fee_pln, 2),
            "warehouse_loss_pln": round(self.cm2_costs.warehouse_loss_pln, 2),
            "return_net_cost_pln": round(self.cm2_costs.return_net_cost_pln, 2),
            "other_cm2_pln": round(self.cm2_costs.other_cm2_pln, 2),
            # NP
            "net_profit_pln": round(self.net_profit, 2),
            "np_pct": round(self.np_pct, 2),
            "saas_pln": round(self.np_costs.saas_pln, 2),
            "digital_services_tax_pln": round(self.np_costs.digital_services_tax_pln, 2),
            "regulatory_fee_pln": round(self.np_costs.regulatory_fee_pln, 2),
            "subscription_pln": round(self.np_costs.subscription_pln, 2),
            "other_overhead_pln": round(self.np_costs.other_overhead_pln, 2),
            # Meta
            "units": self.units,
            "tacos": round(self.tacos, 2),
            "confidence": self.confidence.value,
            "cm1_per_unit": round(self.cm1_per_unit, 2),
            "cm2_per_unit": round(self.cm2_per_unit, 2),
            "np_per_unit": round(self.np_per_unit, 2),
        }


# ── Layer mapping (for fee_taxonomy integration) ────────────────────────────

LAYER_CONFIG = {
    ProfitLayer.CM1: {
        "confidence": Confidence.HIGH,
        "description": "Order-matchable direct costs. Did the sale make sense operationally?",
        "buckets": [
            "cogs", "referral_fee", "fba_fee", "logistics",
            "shipping_surcharge", "promo_order", "refund_commission",
        ],
    },
    ProfitLayer.CM2: {
        "confidence": Confidence.MEDIUM,
        "description": "Event-linked + allocated indirect costs. Was the sale healthy?",
        "buckets": [
            "ads", "fba_storage", "fba_aged", "fba_removal", "fba_liquidation",
            "warehouse_loss", "refund_cost", "other_cm2",
        ],
    },
    ProfitLayer.NP: {
        "confidence": Confidence.LOW,
        "description": "Periodic/shared overhead. How much does the company really earn?",
        "buckets": [
            "saas", "digital_services_tax", "regulatory_fee",
            "subscription", "other_overhead",
        ],
    },
}
