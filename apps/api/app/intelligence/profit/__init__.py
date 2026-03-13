"""app.intelligence.profit - Profit Engine sub-package.

Re-exports all public symbols from sub-modules for convenient imports.
"""
from __future__ import annotations

from datetime import date

# --- helpers ---
from app.intelligence.profit.helpers import (  # noqa: F401
    _connect,
    _fetchall_dict,
    _f,
    _f_strict,
    _i,
    _mkt_code,
    _norm_text,
    _norm_internal_sku,
    _cm1_direct_order_fee_total_sql,
    _cm1_direct_order_fee_alloc_sql,
    _parse_csv_list,
    _parse_search_tokens,
    _warnings_reset,
    _warnings_append,
    _warnings_collect,
    _result_cache_get,
    _result_cache_set,
    _result_cache_invalidate,
    RENEWED_SKU_FILTER,
)

# --- cost_model ---
from app.intelligence.profit.cost_model import (  # noqa: F401
    _find_official_price_workbook,
    _load_google_sku_to_isk_rows,
    _load_official_price_map,
    _fx_case,
    _load_fx_cache,
    _fx_rate_sql_fragment,
    _fx_rate_for_currency,
    _choose_bucket_value,
    _choose_bucket_payload,
    _suggest_pack_qty,
    _parse_tkl_number,
    _extract_pack_qty_from_name,
    _find_latest_tkl_file,
    ensure_profit_tkl_cache_schema,
    _tkl_file_metadata,
    _tkl_signature,
    _load_tkl_maps_from_sql,
    _save_tkl_maps_to_sql,
    _parse_tkl_priority_maps_from_files,
    refresh_tkl_sql_cache,
    _load_tkl_priority_maps,
    ensure_profit_data_quality_schema,
    ensure_profit_cost_model_schema,
    _get_cost_config_decimal,
    _classify_finance_charge,
    _classify_fba_component,
    _PRICE_SOURCE_PRIORITY_CASE,
    _price_source_label,
    _lookup_best_price_for_internal_sku,
    _find_unique_candidate_by_field,
    _lookup_ai_candidate,
    _find_same_ean_sibling_suggestion,
    _find_google_sheet_official_suggestion,
    _build_missing_cogs_suggestions,
    _apply_manual_price_to_internal_sku,
    upsert_purchase_price,
    _resolve_manual_mapping_target,
    map_and_price,
)

# --- calculator ---
from app.intelligence.profit.calculator import (  # noqa: F401
    _load_fba_component_pools,
    _load_marketplace_weight_totals,
    _load_latest_inventory_available_map,
    _load_overhead_pools,
    _CM2_POOL_KEYS,
    _allocate_fba_component_costs,
    _allocate_overhead_costs,
    _load_finance_lookup,
)

# --- query ---
from app.intelligence.profit.query import (  # noqa: F401
    get_product_profit_table,
    _ensure_offer_fee_expected_schema,
    get_product_what_if_table,
    create_product_task,
    _normalize_task_status,
    list_product_tasks,
    update_product_task,
    add_product_task_comment,
    list_product_task_comments,
    list_task_owner_rules,
    create_task_owner_rule,
    delete_task_owner_rule,
    get_product_drilldown,
    get_loss_orders,
    get_fee_breakdown,
    get_data_quality,
    _fetch_fee_gap_orders,
    get_fee_gap_diagnostics,
    seed_fee_gap_watch,
    recheck_fee_gap_watch,
    get_profit_kpis,
)

# --- export ---
from app.intelligence.profit.export import (  # noqa: F401
    export_product_profit_xlsx,
)

# --- rollup ---
from app.intelligence.profit.rollup import (  # noqa: F401
    _build_enrichment_charge_lists,
    _sql_in_list,
    _ensure_system_metadata_table,
    _upsert_system_metadata,
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


# ---------------------------------------------------------------------------
# Unified profit recalculation entry point  (S3.3)
# ---------------------------------------------------------------------------
def full_profit_recalculate(
    date_from: date,
    date_to: date,
    *,
    include_cm1: bool = True,
    include_rollup: bool = True,
    include_alerts: bool = True,
) -> dict:
    """Single canonical entry point for all profit recalculation.

    Combines CM1 order-level recalc, rollup enrichment, and alerting
    into one call with toggleable phases.

    Returns dict with keys present per enabled phase:
      - orders_updated (int)
      - rollup (dict from recompute_rollups)
      - alerts (dict from evaluate_profitability_alerts)
    """
    import structlog
    _log = structlog.get_logger(__name__)

    results: dict = {}

    if include_cm1:
        from app.connectors.mssql.mssql_store import recalc_profit_orders
        results["orders_updated"] = recalc_profit_orders(
            date_from=date_from, date_to=date_to,
        )
        _log.info("profit.cm1_complete", orders=results["orders_updated"])

    if include_rollup:
        results["rollup"] = recompute_rollups(date_from, date_to)
        _log.info("profit.rollup_complete",
                  sku_rows=results["rollup"].get("sku_rows_upserted", 0))

    if include_alerts:
        results["alerts"] = evaluate_profitability_alerts(date_from, date_to)
        _log.info("profit.alerts_complete",
                  alerts=results["alerts"].get("alerts_created", 0))

    return results
