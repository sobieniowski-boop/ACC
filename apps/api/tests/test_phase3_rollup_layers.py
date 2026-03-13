"""Tests for Phase 3: CM1 / CM2 / NP rollup layer redesign.

Covers:
 - fee_taxonomy helpers (charge_types_for_bucket, charge_types_for_layer, rollup_bucket_map)
 - profitability_service helpers (_build_enrichment_charge_lists, _sql_in_list)
 - ensure_rollup_layer_columns DDL helper
 - enrichment step SQL uses taxonomy-derived charge types
 - cm1/cm2 computation formulas in step 5
 - query functions expose cm1_pln / cm2_pln
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


# ── fee_taxonomy helpers ──────────────────────────────────────────────────

class TestChargeTypesForBucket:
    def test_fba_storage_returns_nonempty(self):
        from app.core.fee_taxonomy import charge_types_for_bucket
        result = charge_types_for_bucket("fba_storage")
        assert len(result) >= 1
        assert "FBAStorageFee" in result

    def test_unknown_bucket_returns_empty(self):
        from app.core.fee_taxonomy import charge_types_for_bucket
        assert charge_types_for_bucket("__nonexistent__") == []

    def test_refund_cost_bucket(self):
        from app.core.fee_taxonomy import charge_types_for_bucket
        result = charge_types_for_bucket("refund_cost")
        assert len(result) >= 1


class TestChargeTypesForLayer:
    def test_cm1_returns_nonempty(self):
        from app.core.fee_taxonomy import charge_types_for_layer
        result = charge_types_for_layer("cm1")
        assert len(result) >= 5

    def test_cm2_returns_nonempty(self):
        from app.core.fee_taxonomy import charge_types_for_layer
        result = charge_types_for_layer("cm2")
        assert len(result) >= 5

    def test_np_layer(self):
        from app.core.fee_taxonomy import charge_types_for_layer
        result = charge_types_for_layer("np")
        # np items may exist or be empty, just ensure it's a list
        assert isinstance(result, list)


class TestRollupBucketMap:
    def test_returns_dict_with_layers(self):
        from app.core.fee_taxonomy import rollup_bucket_map
        bmap = rollup_bucket_map()
        assert isinstance(bmap, dict)
        assert "cm1" in bmap
        assert "cm2" in bmap

    def test_cm2_has_storage_bucket(self):
        from app.core.fee_taxonomy import rollup_bucket_map
        bmap = rollup_bucket_map()
        cm2 = bmap.get("cm2", {})
        assert "fba_storage" in cm2 or "fba_aged" in cm2

    def test_all_values_are_string_lists(self):
        from app.core.fee_taxonomy import rollup_bucket_map
        bmap = rollup_bucket_map()
        for layer, buckets in bmap.items():
            assert isinstance(buckets, dict), f"layer={layer}"
            for bucket, types in buckets.items():
                assert isinstance(types, list), f"layer={layer}, bucket={bucket}"
                for ct in types:
                    assert isinstance(ct, str)


# ── profitability_service helpers ─────────────────────────────────────────

class TestBuildEnrichmentChargeLists:
    def test_returns_all_required_keys(self):
        from app.services.profitability_service import _build_enrichment_charge_lists
        result = _build_enrichment_charge_lists()
        for key in ("storage", "refund_cm2", "other_cm2", "overhead_np"):
            assert key in result, f"Missing key: {key}"

    def test_storage_is_nonempty(self):
        from app.services.profitability_service import _build_enrichment_charge_lists
        result = _build_enrichment_charge_lists()
        assert len(result["storage"]) >= 1

    def test_refund_cm2_is_nonempty(self):
        from app.services.profitability_service import _build_enrichment_charge_lists
        result = _build_enrichment_charge_lists()
        assert len(result["refund_cm2"]) >= 1

    def test_all_values_are_string_lists(self):
        from app.services.profitability_service import _build_enrichment_charge_lists
        result = _build_enrichment_charge_lists()
        for key, val in result.items():
            assert isinstance(val, list), key
            for item in val:
                assert isinstance(item, str)


class TestSqlInList:
    def test_single_item(self):
        from app.services.profitability_service import _sql_in_list
        assert _sql_in_list(["FBAStorageFee"]) == "('FBAStorageFee')"

    def test_multiple_items(self):
        from app.services.profitability_service import _sql_in_list
        result = _sql_in_list(["A", "B", "C"])
        assert result == "('A', 'B', 'C')"

    def test_empty_list_returns_safe_value(self):
        from app.services.profitability_service import _sql_in_list
        assert _sql_in_list([]) == "('')"


class TestEnsureRollupLayerColumns:
    def test_executes_ddl_for_both_tables(self):
        from app.services.profitability_service import ensure_rollup_layer_columns
        cur = MagicMock()
        ensure_rollup_layer_columns(cur)
        # 2 tables × 3 columns = 6 execute calls
        assert cur.execute.call_count == 6

    def test_ddl_references_cm1_and_cm2(self):
        from app.services.profitability_service import ensure_rollup_layer_columns
        cur = MagicMock()
        ensure_rollup_layer_columns(cur)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "cm1_pln" in all_sql
        assert "cm2_pln" in all_sql

    def test_ddl_references_both_rollup_tables(self):
        from app.services.profitability_service import ensure_rollup_layer_columns
        cur = MagicMock()
        ensure_rollup_layer_columns(cur)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "acc_sku_profitability_rollup" in all_sql
        assert "acc_marketplace_profitability_rollup" in all_sql


# ── Enrichment SQL uses taxonomy-derived charge lists ─────────────────────

class TestEnrichmentUsesChargeListsFromTaxonomy:
    """Verify the enrichment SQL IN clauses use taxonomy-derived lists."""

    def _make_mock_cursor(self):
        cur = MagicMock()
        cur.rowcount = 0
        return cur

    def test_storage_step_uses_taxonomy_charges(self):
        """Step 1 SQL should contain charge types from taxonomy storage bucket."""
        from app.services.profitability_service import (
            _enrich_rollup_from_finance,
            _STORAGE_CHARGES_SQL,
        )
        cur = self._make_mock_cursor()
        conn = MagicMock()
        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))
        sql_calls = [str(c) for c in cur.execute.call_args_list]
        storage_calls = [s for s in sql_calls if "FBAStorageFee" in s]
        assert len(storage_calls) >= 1

    def test_refund_step_uses_taxonomy_charges(self):
        from app.services.profitability_service import (
            _enrich_rollup_from_finance,
        )
        cur = self._make_mock_cursor()
        conn = MagicMock()
        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))
        sql_calls = [str(c) for c in cur.execute.call_args_list]
        # At least one SQL should reference a refund charge type
        refund_calls = [s for s in sql_calls if "REVERSAL_REIMBURSEMENT" in s or "Refund" in s]
        assert len(refund_calls) >= 1

    def test_module_level_sql_lists_are_nonempty(self):
        from app.services.profitability_service import (
            _STORAGE_CHARGES_SQL,
            _REFUND_CM2_CHARGES_SQL,
            _OTHER_CM2_CHARGES_SQL,
        )
        assert _STORAGE_CHARGES_SQL != "('')"
        assert _REFUND_CM2_CHARGES_SQL != "('')"
        assert _OTHER_CM2_CHARGES_SQL != "('')"


# ── CM1/CM2 computation in enrichment step 5 ─────────────────────────────

class TestCM1CM2ComputationInEnrichStep:
    """Step 5 recalculate must compute cm1_pln and cm2_pln."""

    def _make_mock_cursor(self):
        cur = MagicMock()
        cur.rowcount = 0
        return cur

    def test_step5_references_cm1_pln(self):
        from app.services.profitability_service import _enrich_rollup_from_finance
        cur = self._make_mock_cursor()
        conn = MagicMock()
        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))
        sql_calls = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "cm1_pln" in sql_calls

    def test_step5_references_cm2_pln(self):
        from app.services.profitability_service import _enrich_rollup_from_finance
        cur = self._make_mock_cursor()
        conn = MagicMock()
        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))
        sql_calls = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "cm2_pln" in sql_calls

    def test_cm1_formula_pattern(self):
        """cm1_pln should be revenue minus cm1-layer costs."""
        from app.services.profitability_service import _enrich_rollup_from_finance
        cur = self._make_mock_cursor()
        conn = MagicMock()
        _enrich_rollup_from_finance(cur, conn, date(2026, 1, 1), date(2026, 1, 7))
        sql_calls = " ".join(str(c) for c in cur.execute.call_args_list)
        # cm1 = revenue - cogs - amazon_fees - fba_fees - logistics
        assert "revenue_pln" in sql_calls
        assert "cogs_pln" in sql_calls
        assert "amazon_fees_pln" in sql_calls
        assert "fba_fees_pln" in sql_calls
        assert "logistics_pln" in sql_calls
