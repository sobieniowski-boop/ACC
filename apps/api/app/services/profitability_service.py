"""Profitability Service - backward-compatible facade.

All logic has been moved to ``app.intelligence.profit.rollup``.
This file re-exports every symbol that the rest of the codebase imports
so that ``from app.services.profitability_service import X`` continues to work.

Sprint 3 - S3.2.
"""
# ruff: noqa: F401
import structlog

log = structlog.get_logger(__name__)

from app.core.db_connection import connect_acc  # noqa: F401 - tests patch this path

from app.intelligence.profit.rollup import (
    _build_enrichment_charge_lists,
    _sql_in_list,
    _ensure_system_metadata_table,
    _upsert_system_metadata,
    _METADATA_TABLE_VERIFIED,
    _ENRICHMENT_CHARGES,
    _STORAGE_CHARGES_SQL,
    _REFUND_CM2_CHARGES_SQL,
    _OTHER_CM2_CHARGES_SQL,
    _OVERHEAD_NP_CHARGES_SQL,
    _PROFIT_OVERVIEW_CACHE,
    ensure_rollup_layer_columns,
    get_profitability_overview,
    get_profitability_orders,
    get_profitability_products,
    get_marketplace_profitability,
    simulate_price,
    _enrich_rollup_from_finance,
    recompute_rollups,
    evaluate_profitability_alerts,
)
