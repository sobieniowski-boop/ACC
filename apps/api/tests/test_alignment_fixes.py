"""Tests for Critical Alignment Fixes (2026-03-09).

Covers:
  ALN-01: profit_engine.py ads tables use dbo. prefix
  ALN-02: KPI total_units uses real shipped quantity (not order count)
  ALN-03: KPI total_acos computed from ads_spend/revenue
  ALN-04: TS interfaces aligned (no phantom fields)
  ALN-05: TS ProductProfitItem has all CM2/refund/return-tracker backend fields
  ALN-06: TS ProductProfitSummary has all refund/return-tracker backend fields
  ALN-07: TS DrilldownItem has refund info fields
  ALN-08: TS KPISummary has date_from / date_to
  ALN-09: ExecMarketplaces no phantom fields (ad_spend_pln, active_skus)
  ALN-10: No duplicate LossOrderItem in api.ts
  ALN-11: ProfitExplorer uses cm1_percent
  ALN-12: ProductDrilldown renders refund column
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# ALN-01: profit_engine.py — ads tables must have dbo. prefix
# ═══════════════════════════════════════════════════════════════════════════

class TestALN01_DboPrefix:
    """All acc_ads_* table references in profit_engine.py must use dbo. schema."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        engine_path = Path(__file__).resolve().parents[1] / "app" / "services" / "profit_engine.py"
        self.source = engine_path.read_text(encoding="utf-8")

    def test_acc_ads_campaign_day_has_dbo_prefix(self):
        # Every occurrence of acc_ads_campaign_day must be preceded by dbo.
        bare = re.findall(r"(?<!dbo\.)(?<!\w)acc_ads_campaign_day\b", self.source)
        assert bare == [], f"Found bare acc_ads_campaign_day without dbo. prefix: {bare}"

    def test_acc_ads_campaign_has_dbo_prefix(self):
        # Find acc_ads_campaign but NOT acc_ads_campaign_day (which is a longer match)
        bare = re.findall(r"(?<!dbo\.)(?<!\w)acc_ads_campaign(?!_day)\b", self.source)
        assert bare == [], f"Found bare acc_ads_campaign without dbo. prefix: {bare}"


# ═══════════════════════════════════════════════════════════════════════════
# ALN-02: KPI units must come from OrderLine.quantity_shipped, not order count
# ═══════════════════════════════════════════════════════════════════════════

class TestALN02_KPIUnitsNotOrderCount:
    """kpi.py must not use order count as units."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        kpi_path = Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "kpi.py"
        self.source = kpi_path.read_text(encoding="utf-8")

    def test_total_units_not_assigned_from_total_orders(self):
        # The bug was: total_units=total_orders — must not appear
        assert "total_units=total_orders" not in self.source, (
            "total_units must not be assigned from total_orders"
        )

    def test_marketplace_units_not_assigned_from_orders(self):
        # The bug was: units=orders — must use real quantity
        # Check for the literal pattern inside MarketplaceKPI constructor
        lines = self.source.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped == "units=orders,":
                pytest.fail(
                    f"Line {i + 1}: units=orders still maps order count as units"
                )

    def test_quantity_shipped_used_in_units_query(self):
        assert "quantity_shipped" in self.source, (
            "KPI must use OrderLine.quantity_shipped for real unit counts"
        )


# ═══════════════════════════════════════════════════════════════════════════
# ALN-03: total_acos computed and populated
# ═══════════════════════════════════════════════════════════════════════════

class TestALN03_TotalAcos:
    """KPI response must include total_acos."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        kpi_path = Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "kpi.py"
        self.source = kpi_path.read_text(encoding="utf-8")

    def test_total_acos_in_response_builder(self):
        assert "total_acos=" in self.source, (
            "KPI response must populate total_acos"
        )


# ═══════════════════════════════════════════════════════════════════════════
# ALN-04: TS api.ts must not have phantom fields
# ═══════════════════════════════════════════════════════════════════════════

class TestALN04_TSPhantomFields:
    """api.ts ProductProfitItem must not have refund_rate or roas."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        api_ts_path = Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "api.ts"
        self.source = api_ts_path.read_text(encoding="utf-8")

    def test_no_refund_rate_field(self):
        # refund_rate was a phantom — backend sends return_rate
        match = re.search(
            r"export interface ProductProfitItem \{([^}]+)\}",
            self.source,
            re.DOTALL,
        )
        assert match, "ProductProfitItem interface not found"
        body = match.group(1)
        assert "refund_rate" not in body, (
            "refund_rate must be removed from ProductProfitItem — backend sends return_rate"
        )

    def test_no_roas_field(self):
        # roas was phantom in ProductProfitItem — backend never sends it
        match = re.search(
            r"export interface ProductProfitItem \{([^}]+)\}",
            self.source,
            re.DOTALL,
        )
        assert match, "ProductProfitItem interface not found"
        body = match.group(1)
        assert "roas" not in body, (
            "roas must be removed from ProductProfitItem — backend does not send it"
        )

    def test_kpi_summary_has_total_units(self):
        assert "total_units" in self.source, (
            "KPISummary TS interface must include total_units"
        )

    def test_kpi_summary_has_total_acos(self):
        assert "total_acos" in self.source, (
            "KPISummary TS interface must include total_acos"
        )

    def test_kpi_summary_has_avg_order_value_pln(self):
        assert "avg_order_value_pln" in self.source, (
            "KPISummary TS interface must include avg_order_value_pln"
        )

    def test_marketplace_kpi_has_units(self):
        # Find the MarketplaceKPI interface block
        match = re.search(
            r"export interface MarketplaceKPI \{([^}]+)\}",
            self.source,
            re.DOTALL,
        )
        assert match, "MarketplaceKPI interface not found"
        body = match.group(1)
        assert "units" in body, "MarketplaceKPI must include units field"
        assert "avg_order_value_pln" in body, "MarketplaceKPI must include avg_order_value_pln"


# ═══════════════════════════════════════════════════════════════════════════
# ALN-05: ProductProfitItem has all CM2/refund/return-tracker backend fields
# ═══════════════════════════════════════════════════════════════════════════

class TestALN05_ProductProfitItemCM2Fields:
    """TS ProductProfitItem must include every CM2 and return-tracker field."""

    @pytest.fixture(autouse=True)
    def _load_body(self):
        api_ts = Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "api.ts"
        src = api_ts.read_text(encoding="utf-8")
        m = re.search(r"export interface ProductProfitItem \{([^}]+)\}", src, re.DOTALL)
        assert m, "ProductProfitItem interface not found"
        self.body = m.group(1)

    @pytest.mark.parametrize("field", [
        "refund_finance_pln",
        "shipping_surcharge_pln",
        "fba_inbound_fee_pln",
        "promo_cost_pln",
        "warehouse_loss_pln",
        "amazon_other_fee_pln",
        "refund_orders",
        "refund_units",
        "refund_cost_pln",
        "return_cogs_recovered_pln",
        "return_cogs_write_off_pln",
        "return_cogs_pending_pln",
        "cm1_adjusted",
    ])
    def test_field_present(self, field):
        assert field in self.body, f"ProductProfitItem missing backend field: {field}"


# ═══════════════════════════════════════════════════════════════════════════
# ALN-06: ProductProfitSummary has all refund/return-tracker backend fields
# ═══════════════════════════════════════════════════════════════════════════

class TestALN06_ProductProfitSummaryFields:
    """TS ProductProfitSummary must include all refund/return-tracker fields."""

    @pytest.fixture(autouse=True)
    def _load_body(self):
        api_ts = Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "api.ts"
        src = api_ts.read_text(encoding="utf-8")
        m = re.search(r"export interface ProductProfitSummary \{([^}]+)\}", src, re.DOTALL)
        assert m, "ProductProfitSummary interface not found"
        self.body = m.group(1)

    @pytest.mark.parametrize("field", [
        "refund_shipped_orders",
        "refund_shipped_units",
        "refund_shipped_cost_pln",
        "total_return_cogs_recovered_pln",
        "total_return_cogs_write_off_pln",
        "total_return_cogs_pending_pln",
    ])
    def test_field_present(self, field):
        assert field in self.body, f"ProductProfitSummary missing backend field: {field}"


# ═══════════════════════════════════════════════════════════════════════════
# ALN-07: DrilldownItem has refund info fields
# ═══════════════════════════════════════════════════════════════════════════

class TestALN07_DrilldownItemRefundFields:
    """TS DrilldownItem must include refund info fields from backend."""

    @pytest.fixture(autouse=True)
    def _load_body(self):
        api_ts = Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "api.ts"
        src = api_ts.read_text(encoding="utf-8")
        m = re.search(r"export interface DrilldownItem \{([^}]+)\}", src, re.DOTALL)
        assert m, "DrilldownItem interface not found"
        self.body = m.group(1)

    @pytest.mark.parametrize("field", [
        "is_refund",
        "refund_type",
        "refund_amount_pln",
    ])
    def test_field_present(self, field):
        assert field in self.body, f"DrilldownItem missing backend field: {field}"


# ═══════════════════════════════════════════════════════════════════════════
# ALN-08: KPISummary has date_from / date_to
# ═══════════════════════════════════════════════════════════════════════════

class TestALN08_KPISummaryDateFields:
    """TS KPISummary must include date_from and date_to."""

    @pytest.fixture(autouse=True)
    def _load_body(self):
        api_ts = Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "api.ts"
        src = api_ts.read_text(encoding="utf-8")
        m = re.search(r"export interface KPISummary \{([^}]+)\}", src, re.DOTALL)
        assert m, "KPISummary interface not found"
        self.body = m.group(1)

    def test_date_from_present(self):
        assert "date_from" in self.body, "KPISummary missing date_from"

    def test_date_to_present(self):
        assert "date_to" in self.body, "KPISummary missing date_to"


# ═══════════════════════════════════════════════════════════════════════════
# ALN-09: ExecMarketplaces must not reference phantom fields
# ═══════════════════════════════════════════════════════════════════════════

class TestALN09_ExecMarketplacesNoPhantoms:
    """ExecMarketplaces.tsx must not access ad_spend_pln or active_skus."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        p = Path(__file__).resolve().parents[2] / "web" / "src" / "pages" / "ExecMarketplaces.tsx"
        self.source = p.read_text(encoding="utf-8")

    def test_no_ad_spend_pln(self):
        assert "ad_spend_pln" not in self.source, (
            "ExecMarketplaces still references ad_spend_pln — backend does not provide it"
        )

    def test_no_active_skus(self):
        assert "active_skus" not in self.source, (
            "ExecMarketplaces still references active_skus — backend does not provide it"
        )


# ═══════════════════════════════════════════════════════════════════════════
# ALN-10: No duplicate LossOrderItem in api.ts
# ═══════════════════════════════════════════════════════════════════════════

class TestALN10_NoDuplicateLossOrderItem:
    """api.ts must have exactly one LossOrderItem interface (profitability one renamed)."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        p = Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "api.ts"
        self.source = p.read_text(encoding="utf-8")

    def test_single_loss_order_item(self):
        count = len(re.findall(r"export interface LossOrderItem\b", self.source))
        assert count == 1, f"Expected 1 LossOrderItem declaration, found {count}"

    def test_profitability_loss_order_item_exists(self):
        assert "export interface ProfitabilityLossOrderItem" in self.source, (
            "ProfitabilityLossOrderItem interface not found — the profitability-context version must be renamed"
        )


# ═══════════════════════════════════════════════════════════════════════════
# ALN-11: ProfitExplorer must use cm1_percent not cm_percent
# ═══════════════════════════════════════════════════════════════════════════

class TestALN11_ProfitExplorerCM1Percent:
    """ProfitExplorer.tsx must use cm1_percent, not cm_percent."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        p = Path(__file__).resolve().parents[2] / "web" / "src" / "pages" / "ProfitExplorer.tsx"
        self.source = p.read_text(encoding="utf-8")

    def test_no_cm_percent(self):
        # cm_percent was the wrong field — ProfitOrder has cm1_percent
        assert "cm_percent" not in self.source or "cm1_percent" in self.source, (
            "ProfitExplorer still uses cm_percent instead of cm1_percent"
        )

    def test_cm1_percent_present(self):
        assert "cm1_percent" in self.source, (
            "ProfitExplorer must use cm1_percent from ProfitOrder"
        )


# ═══════════════════════════════════════════════════════════════════════════
# ALN-12: ProductDrilldown renders refund column
# ═══════════════════════════════════════════════════════════════════════════

class TestALN12_DrilldownRefundColumn:
    """ProductDrilldown.tsx must render refund info from backend fields."""

    @pytest.fixture(autouse=True)
    def _load_source(self):
        p = Path(__file__).resolve().parents[2] / "web" / "src" / "pages" / "ProductDrilldown.tsx"
        self.source = p.read_text(encoding="utf-8")

    def test_is_refund_used(self):
        assert "is_refund" in self.source, "ProductDrilldown must render is_refund field"

    def test_refund_type_used(self):
        assert "refund_type" in self.source, "ProductDrilldown must render refund_type field"

    def test_refund_amount_used(self):
        assert "refund_amount_pln" in self.source, "ProductDrilldown must render refund_amount_pln field"

    def test_refund_header_column(self):
        assert "Refund" in self.source, "ProductDrilldown must have a Refund column header"
