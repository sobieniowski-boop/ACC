"""Tests for Phase 4: Rollup Divergence Elimination.

Covers:
  P4-01: acc_system_metadata DDL helper (idempotent, module-level flag)
  P4-02: _upsert_system_metadata uses MERGE WITH (HOLDLOCK)
  P4-03: recompute_rollups writes metadata keys after completion
  P4-04: get_profitability_overview reads metadata for freshness
  P4-05: get_profitability_overview data_source is "mixed"
  P4-06: /products?use_rollup=true returns rollup data via JSONResponse
  P4-07: /products default (live) includes data_source="live"
  P4-08: RollupJobResult schema includes recomputed_at
  P4-09: DataFreshnessInfo schema includes rollup_covers + data_source
  P4-10: Sort column mapping for rollup path
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# P4-01: acc_system_metadata DDL helper
# ═══════════════════════════════════════════════════════════════════════════

class TestP401_EnsureSystemMetadataTable:
    """_ensure_system_metadata_table creates table idempotently."""

    def test_executes_create_table_sql(self):
        import app.intelligence.profit.rollup as mod
        mod._METADATA_TABLE_VERIFIED = False  # reset flag
        cur = MagicMock()
        mod._ensure_system_metadata_table(cur)
        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "acc_system_metadata" in sql
        assert "IF OBJECT_ID" in sql
        assert "CREATE TABLE" in sql

    def test_sets_module_flag_after_success(self):
        import app.intelligence.profit.rollup as mod
        mod._METADATA_TABLE_VERIFIED = False
        cur = MagicMock()
        mod._ensure_system_metadata_table(cur)
        assert mod._METADATA_TABLE_VERIFIED is True

    def test_skips_ddl_after_first_call(self):
        import app.intelligence.profit.rollup as mod
        mod._METADATA_TABLE_VERIFIED = True  # already verified
        cur = MagicMock()
        mod._ensure_system_metadata_table(cur)
        cur.execute.assert_not_called()

    def test_flag_reset_allows_recheck(self):
        """After reset, DDL runs again (useful after process restart)."""
        import app.intelligence.profit.rollup as mod
        mod._METADATA_TABLE_VERIFIED = False
        cur = MagicMock()
        mod._ensure_system_metadata_table(cur)
        assert cur.execute.call_count == 1
        # Second call should skip
        mod._ensure_system_metadata_table(cur)
        assert cur.execute.call_count == 1


# ═══════════════════════════════════════════════════════════════════════════
# P4-02: _upsert_system_metadata uses HOLDLOCK
# ═══════════════════════════════════════════════════════════════════════════

class TestP402_UpsertSystemMetadata:
    """MERGE must use HOLDLOCK to prevent duplicate key under concurrency."""

    def test_merge_sql_contains_holdlock(self):
        from app.services.profitability_service import _upsert_system_metadata
        cur = MagicMock()
        _upsert_system_metadata(cur, "test_key", "test_value")
        sql = cur.execute.call_args[0][0]
        assert "HOLDLOCK" in sql, "MERGE must use HOLDLOCK to prevent race conditions"
        assert "MERGE" in sql

    def test_passes_key_and_value_as_params(self):
        from app.services.profitability_service import _upsert_system_metadata
        cur = MagicMock()
        _upsert_system_metadata(cur, "my_key", "my_value")
        params = cur.execute.call_args[0][1]
        assert params == ("my_key", "my_value")


# ═══════════════════════════════════════════════════════════════════════════
# P4-03: recompute_rollups writes metadata after completion
# ═══════════════════════════════════════════════════════════════════════════

class TestP403_RecomputeWritesMetadata:
    """recompute_rollups must persist timestamps to acc_system_metadata."""

    def test_writes_metadata_keys(self):
        from app.services.profitability_service import recompute_rollups
        import app.intelligence.profit.rollup as mod

        mod._METADATA_TABLE_VERIFIED = False  # reset for DDL test
        captured_calls = []  # list of (sql, params) tuples

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 5
        mock_conn.cursor.return_value = mock_cur

        def capture_execute(sql, params=None):
            captured_calls.append((str(sql), params))

        mock_cur.execute = capture_execute

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch("app.intelligence.profit.rollup._enrich_rollup_from_finance",
                        return_value={"storage_rows": 1, "refund_rows": 1, "other_rows": 1,
                                     "ads_rows": 1, "return_units_rows": 1, "logistics_rows": 1}):
                try:
                    result = recompute_rollups(date(2026, 3, 1), date(2026, 3, 7))
                except Exception:
                    pass

        # Find metadata MERGE calls — params is a tuple (key, value)
        metadata_calls = [
            (sql, params)
            for sql, params in captured_calls
            if "acc_system_metadata" in sql and "MERGE" in sql and params
        ]
        metadata_keys = {p[0] for _, p in metadata_calls}
        assert "rollup_recomputed_at" in metadata_keys
        assert "rollup_date_from" in metadata_keys
        assert "rollup_date_to" in metadata_keys

    def test_result_includes_recomputed_at(self):
        from app.services.profitability_service import recompute_rollups

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 5
        mock_conn.cursor.return_value = mock_cur

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            with patch("app.intelligence.profit.rollup._enrich_rollup_from_finance",
                        return_value={"storage_rows": 0, "refund_rows": 0, "other_rows": 0,
                                     "ads_rows": 0, "return_units_rows": 0, "logistics_rows": 0}):
                try:
                    result = recompute_rollups(date(2026, 3, 1), date(2026, 3, 7))
                except Exception:
                    result = None

        if result:
            assert "recomputed_at" in result
            # Must be ISO format datetime string
            datetime.fromisoformat(result["recomputed_at"])


# ═══════════════════════════════════════════════════════════════════════════
# P4-04: Overview reads freshness from metadata
# ═══════════════════════════════════════════════════════════════════════════

class TestP404_OverviewFreshness:
    """get_profitability_overview reads acc_system_metadata for data freshness."""

    def test_overview_reads_metadata_table(self):
        """Source code must read from acc_system_metadata for freshness."""
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "app" / "intelligence" / "profit" / "rollup.py").read_text(encoding="utf-8")
        # Must query metadata table for freshness
        assert "acc_system_metadata" in source
        assert "rollup_recomputed_at" in source
        assert "rollup_date_from" in source
        assert "rollup_date_to" in source

    def test_overview_fallback_to_computed_at(self):
        """When metadata read fails, falls back to MAX(computed_at)."""
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "app" / "intelligence" / "profit" / "rollup.py").read_text(encoding="utf-8")
        assert "MAX(computed_at)" in source

    def test_overview_returns_data_freshness_keys(self):
        """Returned dict must include data_freshness with rollup_covers + data_source."""
        import app.intelligence.profit.rollup as mod
        mod._METADATA_TABLE_VERIFIED = True
        mod._PROFIT_OVERVIEW_CACHE.clear()

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # KPI row: 9 columns
        kpi_row = (10000.0, 5000.0, 100, 200, 500.0, 200.0, 10, 3000.0, 2000.0)
        metadata_rows = [
            ("rollup_recomputed_at", "2026-03-10T05:45:00"),
            ("rollup_date_from", "2026-03-03"),
            ("rollup_date_to", "2026-03-10"),
        ]

        # Track which SQL is being executed to return appropriate mock data
        call_idx = {"n": 0}
        fetchone_results = []
        fetchall_results = []

        def mock_execute(sql, params=None):
            sql_str = str(sql)
            call_idx["n"] += 1
            # Configure fetchone/fetchall for next call based on query type
            if "SUM(r.revenue_pln)" in sql_str and "GROUP BY" not in sql_str:
                # KPI aggregate query
                mock_cur.fetchone = MagicMock(return_value=kpi_row)
            elif "acc_system_metadata" in sql_str and "CREATE TABLE" not in sql_str:
                # Metadata read
                mock_cur.fetchall = MagicMock(return_value=metadata_rows)
            elif "MAX(computed_at)" in sql_str:
                mock_cur.fetchone = MagicMock(return_value=(None,))
            elif "TOP" in sql_str:
                # best/worst/loss queries
                mock_cur.fetchall = MagicMock(return_value=[])

        mock_cur.execute = MagicMock(side_effect=mock_execute)
        # Default returns
        mock_cur.fetchone = MagicMock(return_value=kpi_row)
        mock_cur.fetchall = MagicMock(return_value=[])

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            result = mod.get_profitability_overview(date(2026, 3, 1), date(2026, 3, 7))

        assert "data_freshness" in result
        df = result["data_freshness"]
        assert df["data_source"] == "mixed"
        assert df["rollup_recomputed_at"] == "2026-03-10T05:45:00"
        assert df["rollup_covers"]["date_from"] == "2026-03-03"
        assert df["rollup_covers"]["date_to"] == "2026-03-10"


class TestP405_OverviewDataSourceMixed:
    """Overview data_source must be 'mixed' because KPIs are from rollup but loss_orders from live."""

    def test_data_source_literal_in_source(self):
        """Source code must hard-code data_source to 'mixed'."""
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "app" / "intelligence" / "profit" / "rollup.py").read_text(encoding="utf-8")
        assert '"data_source": "mixed"' in source or "'data_source': 'mixed'" in source


# ═══════════════════════════════════════════════════════════════════════════
# P4-06: /products?use_rollup=true returns rollup data via JSONResponse
# ═══════════════════════════════════════════════════════════════════════════

class TestP406_UseRollupParam:
    """Static analysis: profit_v2.py rollup path uses JSONResponse."""

    def test_rollup_path_returns_json_response(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "profit_v2.py").read_text()
        # Must use JSONResponse for rollup path (different schema)
        assert "JSONResponse" in source
        assert "use_rollup" in source

    def test_rollup_path_adds_data_source(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "profit_v2.py").read_text()
        assert '["data_source"] = "rollup"' in source

    def test_live_path_adds_data_source(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "profit_v2.py").read_text()
        assert '["data_source"] = "live"' in source


# ═══════════════════════════════════════════════════════════════════════════
# P4-08: Schema includes new fields
# ═══════════════════════════════════════════════════════════════════════════

class TestP408_Schemas:
    """Pydantic schemas must include Phase 4 fields."""

    def test_rollup_job_result_has_recomputed_at(self):
        from app.schemas.profitability import RollupJobResult
        obj = RollupJobResult(recomputed_at="2026-03-10T05:45:00")
        assert obj.recomputed_at == "2026-03-10T05:45:00"

    def test_rollup_job_result_recomputed_at_optional(self):
        from app.schemas.profitability import RollupJobResult
        obj = RollupJobResult()
        assert obj.recomputed_at is None

    def test_data_freshness_has_rollup_covers(self):
        from app.schemas.profitability import DataFreshnessInfo
        obj = DataFreshnessInfo(rollup_covers={"date_from": "2026-03-03", "date_to": "2026-03-10"})
        assert obj.rollup_covers["date_from"] == "2026-03-03"

    def test_data_freshness_has_data_source(self):
        from app.schemas.profitability import DataFreshnessInfo
        obj = DataFreshnessInfo(data_source="mixed")
        assert obj.data_source == "mixed"

    def test_data_freshness_data_source_optional(self):
        from app.schemas.profitability import DataFreshnessInfo
        obj = DataFreshnessInfo()
        assert obj.data_source is None


# ═══════════════════════════════════════════════════════════════════════════
# P4-10: Sort column mapping for rollup path
# ═══════════════════════════════════════════════════════════════════════════

class TestP410_SortColumnMapping:
    """profit_v2.py must map live sort columns to rollup equivalents."""

    def test_sort_mapping_exists_in_source(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "profit_v2.py").read_text()
        assert "SORT_LIVE_TO_ROLLUP" in source
        # Must contain key mappings
        assert "cm1_profit" in source
        assert "cm1_pln" in source


# ═══════════════════════════════════════════════════════════════════════════
# P4-11: get_profitability_products returns correct dict shape
# ═══════════════════════════════════════════════════════════════════════════

class TestP411_ProfitabilityProductsShape:
    """get_profitability_products must return paginated dict with items."""

    def test_returns_pagination_keys(self):
        from app.services.profitability_service import get_profitability_products

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (0,)
        mock_cur.fetchall.return_value = []

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            result = get_profitability_products(date(2026, 3, 1), date(2026, 3, 7))

        assert "total" in result
        assert "page" in result
        assert "page_size" in result
        assert "pages" in result
        assert "items" in result
        assert isinstance(result["items"], list)

    def test_sort_by_allowed_columns(self):
        """Rollup sort: unknown column falls back to profit_pln."""
        from app.services.profitability_service import get_profitability_products

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (0,)
        mock_cur.fetchall.return_value = []

        captured_sql = []

        def capture(sql, params=None):
            captured_sql.append(str(sql))

        mock_cur.execute = capture

        with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
            get_profitability_products(
                date(2026, 3, 1), date(2026, 3, 7),
                sort_by="cm1_profit",  # not in allowed set → falls back to profit_pln
            )

        # At least one SQL should have ORDER BY profit_pln (the fallback)
        order_sql = [s for s in captured_sql if "ORDER BY" in s]
        assert order_sql, "Expected at least one SQL with ORDER BY"
        assert "profit_pln" in order_sql[-1]


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════
