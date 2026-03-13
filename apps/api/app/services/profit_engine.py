"""Profit Engine — backward-compatible facade.

All logic has been moved to ``app.intelligence.profit`` sub-modules.
This file re-exports every symbol that the rest of the codebase imports
so that ``from app.services.profit_engine import X`` continues to work.

Sprint 3 — Profit Engine Split.
"""
# ruff: noqa: F401
import structlog

log = structlog.get_logger(__name__)

from app.intelligence.profit.helpers import (
    _RESULT_CACHE,
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

from app.intelligence.profit.cost_model import (
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

from app.intelligence.profit.calculator import (
    _load_fba_component_pools,
    _load_marketplace_weight_totals,
    _load_latest_inventory_available_map,
    _load_overhead_pools,
    _CM2_POOL_KEYS,
    _allocate_fba_component_costs,
    _allocate_overhead_costs,
    _load_finance_lookup,
)

from app.intelligence.profit.query import (
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

from app.intelligence.profit.export import (
    export_product_profit_xlsx,
)