"""Profit Engine - API data-access queries.

Extracted from the monolithic profit_engine.py (Sprint 3).
Contains all read-path functions: product profit table, what-if analysis,
product drilldown, loss orders, fee breakdown, data quality, fee gap
diagnostics, KPIs, and product task CRUD.
"""
from __future__ import annotations

import math
import re
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import pyodbc
import structlog

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc
from app.services.order_logistics_source import (
    profit_logistics_join_sql,
    profit_logistics_value_sql,
)

from app.intelligence.profit.helpers import (
    _connect, _fetchall_dict, _f, _f_strict, _i, _mkt_code,
    _norm_text, _norm_internal_sku,
    _parse_csv_list, _parse_search_tokens,
    _warnings_reset, _warnings_append, _warnings_collect,
    _result_cache_get, _result_cache_set, _result_cache_invalidate,
    _cm1_direct_order_fee_alloc_sql,
    RENEWED_SKU_FILTER,
)
from app.intelligence.profit.cost_model import (
    _fx_case, _fx_rate_for_currency, _fx_rate_sql_fragment,
    _load_fx_cache,
    _load_tkl_priority_maps, _choose_bucket_value, _choose_bucket_payload,
    _suggest_pack_qty,
    ensure_profit_cost_model_schema, ensure_profit_data_quality_schema,
    _get_cost_config_decimal,
    _classify_finance_charge,
    _load_official_price_map, _load_google_sku_to_isk_rows,
    _SUGGESTION_CACHE_TTL_SECONDS,
    _SKU_TO_ISK_CACHE, _OFFICIAL_PRICE_CACHE,
    _OFFER_FEE_EXPECTED_SCHEMA_READY,
    _WHATIF_LOGISTICS_MIN_SAMPLE,
    _WHATIF_LOGISTICS_BLEND_SAMPLE,
    _WHATIF_LOGISTICS_STABLE_P75_RATIO_MAX,
    _WHATIF_LOGISTICS_BLEND_TKL_WEIGHT,
    _WHATIF_LOGISTICS_BLEND_OBS_WEIGHT,
    _WHATIF_LOGISTICS_DRIFT_SAMPLE,
    _WHATIF_LOGISTICS_DRIFT_MEDIAN_RATIO,
    _WHATIF_LOGISTICS_DRIFT_P75_RATIO,
    _PRICE_SOURCE_PRIORITY_CASE,
    _price_source_label,
    _lookup_best_price_for_internal_sku,
    _find_unique_candidate_by_field,
    _lookup_ai_candidate,
    _find_same_ean_sibling_suggestion,
    _find_google_sheet_official_suggestion,
    _build_missing_cogs_suggestions,
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

log = structlog.get_logger(__name__)

def get_product_profit_table(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    brand: str | None = None,
    sku_search: str | None = None,
    fulfillment: str | None = None,
    parent_asin: str | None = None,
    profit_mode: str = "cm1",          # cm1 | cm2 | np
    include_cost_components: bool = False,
    only_loss: bool = False,
    only_low_confidence: bool = False,
    sort_by: str = "cm1_profit",
    sort_dir: str = "desc",
    group_by: str = "asin_marketplace",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return product-level profit aggregation (ASIN-first, optional parent rollup)."""
    _warnings_reset()

    # Check in-memory cache
    cache_key = (
        f"ppt:{date_from}:{date_to}:{marketplace_id}:{brand}:{sku_search}"
        f":{fulfillment}:{parent_asin}:{profit_mode}:{include_cost_components}:{only_loss}:{only_low_confidence}"
        f":{sort_by}:{sort_dir}:{group_by}:{page}:{page_size}"
    )
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return cached

    conn = _connect()
    try:
        cur = conn.cursor()
        ensure_profit_cost_model_schema()
        return_handling_per_unit = _get_cost_config_decimal(cur, "return_handling_per_unit_pln", 0.0)

        # --- WHERE ---
        wheres = [
            "o.status = 'Shipped'",
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            # Exclude removal/return orders and Non-Amazon transfers
            "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            "o.amazon_order_id NOT LIKE 'S02-%'",
            # Exclude Amazon Renewed (used) products from analytics
            RENEWED_SKU_FILTER,
        ]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]

        if marketplace_id:
            wheres.append("o.marketplace_id = ?")
            params.append(marketplace_id)
        if fulfillment:
            wheres.append("o.fulfillment_channel = ?")
            params.append(fulfillment)
        parent_asin_filter = str(parent_asin or "").strip()
        sku_tokens = _parse_search_tokens(sku_search)
        if sku_tokens:
            token_clauses: list[str] = []
            for token in sku_tokens:
                token_clauses.append("(ol.sku LIKE ? OR ol.asin LIKE ?)")
                like = f"%{token}%"
                params.extend([like, like])
            wheres.append("(" + " OR ".join(token_clauses) + ")")
        if parent_asin_filter:
            wheres.append(
                "ISNULL(NULLIF(COALESCE(NULLIF(p.parent_asin, ''), NULLIF(reg.parent_asin, ''), NULLIF(ol.asin, '')), ''), '') = ?"
            )
            params.append(parent_asin_filter)

        where_sql = " AND ".join(wheres)
        group_mode = (group_by or "asin_marketplace").lower()
        group_configs: dict[str, dict[str, Any]] = {
            "sku_marketplace": {
                "entity_type": "sku",
                "group_expr": "ISNULL(ol.sku, 'UNKNOWN')",
                "group_marketplace": True,
            },
            "sku": {
                "entity_type": "sku",
                "group_expr": "ISNULL(ol.sku, 'UNKNOWN')",
                "group_marketplace": False,
            },
            "asin_marketplace": {
                "entity_type": "asin",
                "group_expr": "ISNULL(NULLIF(ol.asin, ''), CONCAT('SKU:', ISNULL(ol.sku, 'UNKNOWN')))",
                "group_marketplace": True,
            },
            "asin": {
                "entity_type": "asin",
                "group_expr": "ISNULL(NULLIF(ol.asin, ''), CONCAT('SKU:', ISNULL(ol.sku, 'UNKNOWN')))",
                "group_marketplace": False,
            },
            "parent_marketplace": {
                "entity_type": "parent",
                "group_expr": (
                    "ISNULL(NULLIF(COALESCE(NULLIF(p.parent_asin, ''), NULLIF(reg.parent_asin, ''), NULLIF(ol.asin, '')), ''), "
                    "CONCAT('SKU:', ISNULL(ol.sku, 'UNKNOWN')))"
                ),
                "group_marketplace": True,
            },
            "parent": {
                "entity_type": "parent",
                "group_expr": (
                    "ISNULL(NULLIF(COALESCE(NULLIF(p.parent_asin, ''), NULLIF(reg.parent_asin, ''), NULLIF(ol.asin, '')), ''), "
                    "CONCAT('SKU:', ISNULL(ol.sku, 'UNKNOWN')))"
                ),
                "group_marketplace": False,
            },
        }
        group_cfg = group_configs.get(group_mode, group_configs["asin_marketplace"])
        entity_type = str(group_cfg["entity_type"])
        group_expr = str(group_cfg["group_expr"])
        group_marketplace = bool(group_cfg["group_marketplace"])
        select_marketplace = "o.marketplace_id" if group_marketplace else "CAST(NULL AS NVARCHAR(160)) AS marketplace_id"
        group_sql = f"{group_expr}, o.marketplace_id" if group_marketplace else group_expr
        shipping_scope_wheres = [
            "o_scope.status = 'Shipped'",
            "o_scope.purchase_date >= CAST(? AS DATE)",
            "o_scope.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            "ISNULL(o_scope.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            "o_scope.amazon_order_id NOT LIKE 'S02-%'",
        ]
        shipping_scope_params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        if marketplace_id:
            shipping_scope_wheres.append("o_scope.marketplace_id = ?")
            shipping_scope_params.append(marketplace_id)
        shipping_scope_sql = " AND ".join(shipping_scope_wheres)
        agg_params = [*shipping_scope_params, *params]

        order_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        order_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

        # --- Main aggregation query ---
        # Revenue per line = (item_price - ISNULL(item_tax, 0) - ISNULL(promotion_discount, 0)) × FX
        # CM1 = revenue_pln - cogs_pln - fba_fee_pln - referral_fee_pln
        agg_sql = f"""
            WITH order_scope AS (
                SELECT DISTINCT
                    o_scope.amazon_order_id,
                    o_scope.marketplace_id
                FROM dbo.acc_order o_scope WITH (NOLOCK)
                WHERE {shipping_scope_sql}
            ),
            shipping_per_order AS (
                SELECT
                    os.amazon_order_id,
                    os.marketplace_id,
                    COUNT_BIG(ft.amazon_order_id) AS finance_rows,
                    SUM(
                        CASE
                            WHEN ft.charge_type IN ('ShippingCharge', 'ShippingTax', 'ShippingDiscount') THEN
                                ISNULL(
                                    ft.amount_pln,
                                    ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")}
                                )
                            ELSE 0
                        END
                    ) AS shipping_charge_pln
                FROM order_scope os
                LEFT JOIN dbo.acc_finance_transaction ft WITH (NOLOCK)
                    ON ft.amazon_order_id = os.amazon_order_id
                   AND (
                       ft.marketplace_id = os.marketplace_id
                       OR ft.marketplace_id IS NULL
                       OR os.marketplace_id IS NULL
                   )
                GROUP BY os.amazon_order_id, os.marketplace_id
            )
            SELECT
                {group_expr}                         AS group_key,
                MIN(ISNULL(ol.sku, 'UNKNOWN'))       AS sample_sku,
                MIN(ol.asin)                         AS asin,
                MIN(COALESCE(NULLIF(p.parent_asin, ''), NULLIF(reg.parent_asin, ''), NULLIF(ol.asin, ''))) AS parent_asin,
                {select_marketplace},
                MIN(ol.title)                        AS title,
                MIN(o.fulfillment_channel)            AS fulfillment_channel,
                MIN(p.brand)                         AS brand,
                MIN(p.category)                      AS category,
                MIN(p.internal_sku)                   AS internal_sku,
                STRING_AGG(CAST(NULLIF(ol.asin, '') AS NVARCHAR(MAX)), ',') AS asin_list,

                -- Sales
                SUM(ISNULL(ol.quantity_ordered, 0))                AS units,
                COUNT(DISTINCT o.id)                               AS order_count,
                COUNT(DISTINCT ISNULL(ol.sku, 'UNKNOWN'))          AS sku_count,
                COUNT(DISTINCT ISNULL(NULLIF(ol.asin, ''), CONCAT('SKU:', ISNULL(ol.sku, 'UNKNOWN')))) AS child_count,

                -- Revenue shipped (netto, in PLN) + ShippingCharge allocation for FBM
                -- Refund impact is handled in CM2 as returns_net
                SUM(
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                    * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')})
                )                                                  AS revenue_pln,
                -- Buyer-paid shipping = revenue (all channels)
                SUM(
                    ISNULL(spo.shipping_charge_pln, 0) * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (
                                ISNULL(ol.item_price, 0)
                                - ISNULL(ol.item_tax, 0)
                                - ISNULL(ol.promotion_discount, 0)
                            ) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                )                                                  AS shipping_charge_pln,

                -- Direct costs at shipment moment (CM1 basis)
                SUM(ISNULL(ol.cogs_pln, 0))                       AS cogs_pln,
                SUM(ISNULL(ol.fba_fee_pln, 0))                    AS fba_fee_pln,
                SUM(ISNULL(ol.referral_fee_pln, 0))               AS referral_fee_pln,
                SUM(
                    (ISNULL(ol.fba_fee_pln, 0) + ISNULL(ol.referral_fee_pln, 0))
                    + (
                        {_cm1_direct_order_fee_total_sql('o')} * CASE
                            WHEN ISNULL(olt.order_line_total, 0) > 0
                                THEN (
                                    ISNULL(ol.item_price, 0)
                                    - ISNULL(ol.item_tax, 0)
                                    - ISNULL(ol.promotion_discount, 0)
                                ) / NULLIF(olt.order_line_total, 0)
                            WHEN ISNULL(olt.order_units_total, 0) > 0
                                THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    )
                )                                                  AS amazon_fees_pln,
                SUM(
                    {order_logistics_value_sql} * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (
                                ISNULL(ol.item_price, 0)
                                - ISNULL(ol.item_tax, 0)
                                - ISNULL(ol.promotion_discount, 0)
                            ) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                )                                                  AS logistics_pln,
                SUM(CASE WHEN ISNULL(o.fulfillment_channel, '') = 'AFN'
                    THEN ISNULL(ol.quantity_ordered, 0) ELSE 0 END) AS afn_units,

                -- Data quality
                SUM(CASE WHEN ol.cogs_pln IS NOT NULL AND ol.cogs_pln > 0
                    THEN 1 ELSE 0 END)                            AS lines_with_cogs,
                SUM(CASE WHEN ol.fba_fee_pln IS NOT NULL AND ol.fba_fee_pln > 0
                    THEN 1 ELSE 0 END)                            AS lines_with_fees,
                COUNT(*)                                           AS total_lines,
                COUNT(DISTINCT CASE WHEN ISNULL(spo.finance_rows, 0) > 0
                    THEN o.id END)                                 AS orders_with_finance,
                COUNT(DISTINCT CASE
                    WHEN ISNULL(spo.shipping_charge_pln, 0) > 0
                    THEN o.id
                END)                                               AS orders_with_shipping_charge,

                -- Returns/refunds components for CM2 (returns_net)
                SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1
                    THEN 1 ELSE 0 END)                            AS refund_lines,
                COUNT(DISTINCT CASE WHEN ISNULL(o.is_refund, 0) = 1
                    THEN o.id END)                                AS refund_orders,
                SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1
                    THEN ISNULL(ol.quantity_ordered, 0) ELSE 0 END) AS refund_units,
                SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1 THEN
                    ABS(ISNULL(o.refund_amount_pln, 0))
                    * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (
                                ISNULL(ol.item_price, 0)
                                - ISNULL(ol.item_tax, 0)
                                - ISNULL(ol.promotion_discount, 0)
                            ) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 1.0 / NULLIF(olt.order_line_count, 0)
                    END
                ELSE 0 END)                                       AS refund_gross_pln,
                SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1 AND o.refund_type = 'full'
                    THEN ISNULL(ol.cogs_pln, 0) ELSE 0 END)      AS return_cogs_recovered_pln,

                -- Loss orders (lines with CM1 < 0)
                SUM(CASE WHEN (
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                    * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')})
                    + ISNULL(spo.shipping_charge_pln, 0) * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                    - ISNULL(ol.cogs_pln, 0)
                    - ISNULL(ol.fba_fee_pln, 0)
                    - ISNULL(ol.referral_fee_pln, 0)
                    - (
                        {_cm1_direct_order_fee_total_sql('o')} * CASE
                            WHEN ISNULL(olt.order_line_total, 0) > 0
                                THEN (
                                    ISNULL(ol.item_price, 0)
                                    - ISNULL(ol.item_tax, 0)
                                    - ISNULL(ol.promotion_discount, 0)
                                ) / NULLIF(olt.order_line_total, 0)
                            WHEN ISNULL(olt.order_units_total, 0) > 0
                                THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    )
                    - (
                        {order_logistics_value_sql} * CASE
                            WHEN ISNULL(olt.order_line_total, 0) > 0
                                THEN (
                                    ISNULL(ol.item_price, 0)
                                    - ISNULL(ol.item_tax, 0)
                                    - ISNULL(ol.promotion_discount, 0)
                                ) / NULLIF(olt.order_line_total, 0)
                            WHEN ISNULL(olt.order_units_total, 0) > 0
                                THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                      )
                ) < 0 THEN 1 ELSE 0 END)                         AS loss_lines

            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            {order_logistics_join_sql}
            LEFT JOIN shipping_per_order spo
                ON spo.amazon_order_id = o.amazon_order_id
               AND spo.marketplace_id = o.marketplace_id
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
            LEFT JOIN (
                SELECT merchant_sku, MAX(parent_asin) AS parent_asin
                FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
                WHERE parent_asin IS NOT NULL AND parent_asin != ''
                GROUP BY merchant_sku
            ) reg ON reg.merchant_sku = ol.sku
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= o.purchase_date
                ORDER BY er.rate_date DESC
            ) fx
            OUTER APPLY (
                SELECT
                    ISNULL(SUM(
                        ISNULL(ol2.item_price, 0)
                        - ISNULL(ol2.item_tax, 0)
                        - ISNULL(ol2.promotion_discount, 0)
                    ), 0) AS order_line_total,
                    ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total,
                    COUNT(*) AS order_line_count
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            WHERE {where_sql}
            GROUP BY {group_sql}
        """

        # Execute base query into temp results
        import time as _time
        _t0 = _time.monotonic()
        cur.execute(agg_sql, agg_params)
        rows = _fetchall_dict(cur)
        log.info("ppt.main_sql", elapsed=round(_time.monotonic() - _t0, 1), rows=len(rows))

        # --- Load ShippingCharge revenue pool from finance transactions ---
        # Net shipping = ShippingCharge + ShippingTax + ShippingDiscount (discount is negative → sum gives netto).
        # Pool-based: total per marketplace, distributed to products by revenue share in Python.
        _shipping_pool: dict[str, float] = {}  # marketplace_id → total_shipping_pln
        # Legacy marketplace-level shipping pool is intentionally disabled.

        profit_mode_norm = str(profit_mode or "cm1").strip().lower()
        need_extended_costs = (
            bool(include_cost_components)
            or profit_mode_norm in {"cm2", "np"}
            or sort_by in {"cm2_profit", "cm2_percent", "np_profit", "np_percent"}
        )
        need_ads_costs = need_extended_costs or sort_by == "ads_cost_pln"
        taxonomy_by_sku: dict[str, dict[str, Any]] = {}
        taxonomy_by_asin: dict[str, dict[str, Any]] = {}
        try:
            from app.services.taxonomy import ensure_taxonomy_schema, load_taxonomy_lookup

            ensure_taxonomy_schema()
            taxonomy_by_sku, taxonomy_by_asin, _taxonomy_by_ean = load_taxonomy_lookup(
                cur,
                skus=[str(row.get("sample_sku") or "") for row in rows if row.get("sample_sku")],
                asins=[str(row.get("asin") or "") for row in rows if row.get("asin")],
                min_confidence=0.75,
            )
        except Exception as exc:
            log.warning("profit_engine.taxonomy_lookup_failed", error=str(exc))
        log.info("ppt.taxonomy", elapsed=round(_time.monotonic() - _t0, 1))

        # --- Fetch ads cost per ASIN from acc_ads_product_day ---
        ads_cost_map: dict[tuple[str, str], float] = {}  # (asin, marketplace_id) → spend_pln
        try:
            if not need_ads_costs:
                raise RuntimeError("skip_ads_lookup")
            ads_where = "report_date >= CAST(? AS DATE) AND report_date < DATEADD(day, 1, CAST(? AS DATE))"
            ads_params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
            if marketplace_id:
                ads_where += " AND marketplace_id = ?"
                ads_params.append(marketplace_id)
            cur.execute(f"""
                SELECT asin, marketplace_id, SUM(ISNULL(spend_pln, 0)) AS ads_spend_pln
                FROM dbo.acc_ads_product_day WITH (NOLOCK)
                WHERE {ads_where}
                GROUP BY asin, marketplace_id
                HAVING SUM(ISNULL(spend_pln, 0)) > 0
            """, ads_params)
            for arow in cur.fetchall():
                ads_cost_map[(str(arow[0]), str(arow[1]))] = float(arow[2])
        except Exception as exc:
            if need_ads_costs:
                log.warning("profit_engine.ads_product_day_error", error=str(exc))

        # --- Fallback: campaign_day ads cost for marketplaces missing product_day ---
        # When product_day has no ASIN-level data for a marketplace (e.g. DE),
        # use campaign_day total spend distributed proportionally by revenue.
        try:
            if not need_ads_costs:
                raise RuntimeError("skip_ads_fallback")
            product_mkts = {mp for (_, mp) in ads_cost_map}
            fb_where = ("cd.report_date >= CAST(? AS DATE) "
                        "AND cd.report_date < DATEADD(day, 1, CAST(? AS DATE))")
            fb_params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
            if marketplace_id:
                fb_where += " AND c.marketplace_id = ?"
                fb_params.append(marketplace_id)

            cur.execute(f"""
                SELECT c.marketplace_id,
                       SUM(ISNULL(cd.spend_pln, 0)) AS total_spend_pln
                FROM dbo.acc_ads_campaign_day cd WITH (NOLOCK)
                JOIN dbo.acc_ads_campaign c WITH (NOLOCK)
                     ON cd.campaign_id = c.campaign_id AND cd.ad_type = c.ad_type
                WHERE {fb_where}
                GROUP BY c.marketplace_id
                HAVING SUM(ISNULL(cd.spend_pln, 0)) > 0
            """, fb_params)

            campaign_spend: dict[str, float] = {}
            for crow in cur.fetchall():
                mp = str(crow[0])
                if mp not in product_mkts:
                    campaign_spend[mp] = float(crow[1])

            if campaign_spend and rows:
                target_mkts = sorted(campaign_spend.keys())
                placeholders = ",".join("?" for _ in target_mkts)
                cur.execute(
                    f"""
                    SELECT
                        ol.asin,
                        o.marketplace_id,
                        SUM(
                            (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                            * ISNULL(fx.rate_to_pln,
                                {_fx_case('o.currency')})
                        )
                        + ISNULL(SUM(
                            ISNULL(spo.shipping_charge_pln, 0) * CASE
                                WHEN ISNULL(olt.order_line_total, 0) > 0
                                    THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                                WHEN ISNULL(olt.order_units_total, 0) > 0
                                    THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                                ELSE 0
                            END
                        ), 0) AS revenue_pln
                    FROM dbo.acc_order_line ol WITH (NOLOCK)
                    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                    OUTER APPLY (
                        SELECT TOP 1 rate_to_pln
                        FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                        WHERE er.currency = o.currency
                          AND er.rate_date <= o.purchase_date
                        ORDER BY er.rate_date DESC
                    ) fx
                    OUTER APPLY (
                        SELECT
                            ISNULL(SUM(
                                ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                                - ISNULL(ol2.promotion_discount, 0)
                            ), 1) AS order_line_total,
                            ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                        FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                        WHERE ol2.order_id = o.id
                    ) olt
                    OUTER APPLY (
                        SELECT SUM(
                            CASE WHEN ft.charge_type IN ('ShippingCharge','ShippingTax','ShippingDiscount')
                                THEN ISNULL(ft.amount_pln,
                                    ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")})
                                ELSE 0
                            END
                        ) AS shipping_charge_pln
                        FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                        WHERE ft.amazon_order_id = o.amazon_order_id
                          AND (ft.marketplace_id = o.marketplace_id OR ft.marketplace_id IS NULL)
                    ) spo
                    WHERE {where_sql}
                      AND ol.asin IS NOT NULL
                      AND ol.asin <> ''
                      AND o.marketplace_id IN ({placeholders})
                    GROUP BY ol.asin, o.marketplace_id
                    """,
                    [*params, *target_mkts],
                )
                mp_asin_rev: dict[str, dict[str, float]] = {}
                for rev_row in cur.fetchall():
                    asin = str(rev_row[0] or "")
                    mp = str(rev_row[1] or "")
                    rev = _f(rev_row[2])
                    if asin and mp and rev > 0:
                        mp_asin_rev.setdefault(mp, {})[asin] = rev
                for mp, spend_total in campaign_spend.items():
                    asin_revs = mp_asin_rev.get(mp, {})
                    mp_revenue = sum(asin_revs.values())
                    if mp_revenue <= 0:
                        continue
                    for asin, rev in asin_revs.items():
                        ads_cost_map[(asin, mp)] = round(spend_total * rev / mp_revenue, 4)
        except Exception as exc:
            if need_ads_costs:
                log.warning("profit_engine.ads_fallback_error", error=str(exc))
        log.info("ppt.ads_done", elapsed=round(_time.monotonic() - _t0, 1))

        inventory_available_map = _load_latest_inventory_available_map(
            cur,
            marketplace_id=marketplace_id,
        )
        inventory_available_global_by_asin: dict[str, float] = {}
        for (asin_key, _mkt_key), available_units in inventory_available_map.items():
            inventory_available_global_by_asin[asin_key] = (
                inventory_available_global_by_asin.get(asin_key, 0.0) + max(_f(available_units), 0.0)
            )
        period_days = max(1, (date_to - date_from).days + 1)
        log.info("ppt.inventory_done", elapsed=round(_time.monotonic() - _t0, 1))

        # shipping_charge_pln is already allocated per order in the SQL layer.

        # --- Compute derived fields & apply filters in Python ---
        products: list[dict[str, Any]] = []
        for r in rows:
            line_revenue = _f(r["revenue_pln"])
            shipping_charge_revenue = round(_f(r.get("shipping_charge_pln")), 2)
            rev = round(line_revenue + shipping_charge_revenue, 2)
            cogs = _f(r["cogs_pln"])
            fba = _f(r["fba_fee_pln"])
            ref = _f(r["referral_fee_pln"])
            logistics = round(_f(r.get("logistics_pln")), 2)
            # amazon_fees_pln from SQL = fba + referral + order-level (surcharge + promo + refund_comm)
            fees = round(_f(r["amazon_fees_pln"]), 2)
            units = _i(r["units"])
            afn_units = _i(r.get("afn_units"))
            total_lines = _i(r["total_lines"], 1)
            order_count = _i(r["order_count"])
            orders_with_finance = _i(r.get("orders_with_finance"))
            orders_with_shipping_charge = _i(r.get("orders_with_shipping_charge"))

            cm1 = round(rev - cogs - fees - logistics, 2)
            cm1_pct = round(cm1 / rev * 100, 2) if rev else 0.0

            # Ads cost lookup by ASIN + marketplace (ASIN/parent-first)
            group_key = str(r.get("group_key") or "")
            sample_sku = str(r.get("sample_sku") or "")
            asin_val = str(r.get("asin") or "")
            mp_val = str(r.get("marketplace_id") or "")
            taxonomy = taxonomy_by_sku.get(sample_sku) or taxonomy_by_asin.get(asin_val) or {}
            resolved_brand = str(r.get("brand") or "").strip() or taxonomy.get("brand")
            resolved_category = str(r.get("category") or "").strip() or taxonomy.get("category")
            asin_list_raw = str(r.get("asin_list") or "")
            asin_list = {a.strip() for a in asin_list_raw.split(",") if a and a.strip()}
            if asin_val:
                asin_list.add(asin_val)
            ads_cost = 0.0
            if entity_type == "sku":
                if asin_val and group_marketplace:
                    ads_cost = round(ads_cost_map.get((asin_val, mp_val), 0.0), 2)
                elif asin_val:
                    ads_cost = round(sum(v for (a, _), v in ads_cost_map.items() if a == asin_val), 2)
            else:
                if group_marketplace:
                    ads_cost = round(sum(ads_cost_map.get((asin, mp_val), 0.0) for asin in asin_list), 2)
                else:
                    ads_cost = round(sum(v for (a, _), v in ads_cost_map.items() if a in asin_list), 2)

            refund_gross = round(_f(r.get("refund_gross_pln")), 2)
            return_cogs_recovered = round(_f(r.get("return_cogs_recovered_pln")), 2)
            return_handling = round(_i(r.get("refund_units")) * return_handling_per_unit, 2)
            returns_net = round(max(0.0, refund_gross - return_cogs_recovered) + return_handling, 2)

            cogs_cov = round(_i(r["lines_with_cogs"]) / total_lines * 100, 1) if total_lines else 0.0
            fees_cov = round(_i(r["lines_with_fees"]) / total_lines * 100, 1) if total_lines else 0.0
            confidence = round((cogs_cov * 0.6 + fees_cov * 0.4), 1)
            loss_pct = round(_i(r["loss_lines"]) / total_lines * 100, 1) if total_lines else 0.0

            # Refund stats for Shipped+refund orders (cost is ours)
            refund_orders = _i(r.get("refund_orders"))
            refund_units = _i(r.get("refund_units"))
            refund_cost = returns_net
            return_rate = round(refund_units / units * 100, 2) if units > 0 else None
            tacos = round(ads_cost / rev * 100, 2) if rev > 0 else None
            finance_match_pct = round(orders_with_finance / order_count * 100, 1) if order_count > 0 else None
            shipping_match_pct = (
                round(orders_with_shipping_charge / order_count * 100, 1)
                if order_count > 0
                else None
            )
            days_cover: float | None = None
            if units > 0:
                daily_velocity = units / float(period_days)
                if daily_velocity > 0:
                    available_units_est = 0.0
                    if group_marketplace:
                        for asin in asin_list:
                            available_units_est += max(_f(inventory_available_map.get((asin, mp_val))), 0.0)
                    else:
                        for asin in asin_list:
                            available_units_est += max(_f(inventory_available_global_by_asin.get(asin)), 0.0)
                    days_cover = round(available_units_est / daily_velocity, 1)

            # Filters
            if only_loss and cm1 >= 0:
                continue
            if only_low_confidence and confidence >= 70:
                continue
            if brand and resolved_brand and brand.lower() not in str(resolved_brand).lower():
                continue

            resolved_asin = asin_val
            if entity_type == "asin" and group_key and not group_key.startswith("SKU:"):
                resolved_asin = group_key

            products.append({
                "entity_type": entity_type,
                "group_key": group_key,
                "sku": group_key or sample_sku or str(r.get("sku") or ""),
                "sample_sku": sample_sku or None,
                "asin": resolved_asin or None,
                "parent_asin": str(r.get("parent_asin") or "").strip() or None,
                "marketplace_id": r.get("marketplace_id") or ("__ALL__" if not group_marketplace else ""),
                "marketplace_code": _mkt_code(r.get("marketplace_id")) if group_marketplace else "ALL",
                "title": r.get("title"),
                "brand": resolved_brand,
                "category": resolved_category,
                "internal_sku": r.get("internal_sku"),
                "fulfillment_channel": r.get("fulfillment_channel") or "",
                # Sales
                "units": units,
                "order_count": _i(r["order_count"]),
                "sku_count": _i(r.get("sku_count")),
                "child_count": _i(r.get("child_count")),
                "revenue_pln": rev,
                "shipping_charge_pln": shipping_charge_revenue,
                # Unit economics
                "cogs_per_unit": round(cogs / units, 2) if units else 0.0,
                "fees_per_unit": round(fees / units, 2) if units else 0.0,
                "revenue_per_unit": round(rev / units, 2) if units else 0.0,
                # Costs total
                "cogs_pln": cogs,
                "amazon_fees_pln": fees,
                "fba_fee_pln": fba,
                "referral_fee_pln": ref,
                "logistics_pln": logistics,
                "afn_units": afn_units,
                # CM1
                "cm1_profit": cm1,
                "cm1_percent": cm1_pct,
                # CM2 components
                "ads_cost_pln": ads_cost,
                "returns_net_pln": returns_net,
                "refund_gross_pln": refund_gross,
                "return_handling_pln": return_handling,
                "return_cogs_recovered_pln": return_cogs_recovered,
                "fba_storage_fee_pln": 0.0,
                "fba_aged_fee_pln": 0.0,
                "fba_removal_fee_pln": 0.0,
                "fba_liquidation_fee_pln": 0.0,
                "refund_finance_pln": 0.0,
                "shipping_surcharge_pln": 0.0,
                "fba_inbound_fee_pln": 0.0,
                "promo_cost_pln": 0.0,
                "warehouse_loss_pln": 0.0,
                "amazon_other_fee_pln": 0.0,
                # NP components
                "overhead_allocated_pln": 0.0,
                "overhead_allocation_method": "none",
                "overhead_confidence_pct": 0.0,
                "cm2_profit": cm1,
                "cm2_percent": cm1_pct,
                "np_profit": cm1,
                "np_percent": cm1_pct,
                # Data quality
                "cogs_coverage_pct": cogs_cov,
                "fees_coverage_pct": fees_cov,
                "confidence_score": confidence,
                "loss_orders_pct": loss_pct,
                "return_rate": return_rate,
                "tacos": tacos,
                "days_of_cover": days_cover,
                "shipping_match_pct": shipping_match_pct,
                "finance_match_pct": finance_match_pct,
                # Refund impact (Shipped orders that were refunded — our cost)
                "refund_orders": refund_orders,
                "refund_units": refund_units,
                "refund_cost_pln": refund_cost,
            })

        # Allocate CM2/NP components only when explicitly needed (keeps CM1-first views responsive).
        if need_extended_costs:
            fba_component_pools = _load_fba_component_pools(
                cur,
                date_from=date_from,
                date_to=date_to,
                marketplace_id=marketplace_id,
            )
            marketplace_weight_totals = _load_marketplace_weight_totals(
                cur,
                date_from=date_from,
                date_to=date_to,
                marketplace_id=marketplace_id,
            )
            _allocate_fba_component_costs(
                products,
                fba_component_pools,
                marketplace_weight_totals=marketplace_weight_totals,
            )

            overhead_pools = _load_overhead_pools(
                cur,
                date_from=date_from,
                date_to=date_to,
                marketplace_id=marketplace_id,
            )
            _allocate_overhead_costs(products, overhead_pools)
        else:
            _allocate_fba_component_costs(products, {})
            _allocate_overhead_costs(products, [])

        for p in products:
            cm1_val = _f(p.get("cm1_profit"))
            ads_val = _f(p.get("ads_cost_pln"))
            returns_val = _f(p.get("returns_net_pln"))
            storage_val = _f(p.get("fba_storage_fee_pln"))
            aged_val = _f(p.get("fba_aged_fee_pln"))
            removal_val = _f(p.get("fba_removal_fee_pln"))
            liquidation_val = _f(p.get("fba_liquidation_fee_pln"))
            refund_fin_val = _f(p.get("refund_finance_pln"))
            ship_surcharge_val = _f(p.get("shipping_surcharge_pln"))
            fba_inbound_val = _f(p.get("fba_inbound_fee_pln"))
            promo_val = _f(p.get("promo_cost_pln"))
            wh_loss_val = _f(p.get("warehouse_loss_pln"))
            amz_other_val = _f(p.get("amazon_other_fee_pln"))
            overhead_val = _f(p.get("overhead_allocated_pln"))
            rev_val = _f(p.get("revenue_pln"))

            # shipping_surcharge & promo are already in CM1 (via amazon_fees_pln)
            # so exclude them from CM2 pool deduction to avoid double-counting
            cm2_val = round(
                cm1_val - ads_val - returns_val
                - storage_val - aged_val - removal_val - liquidation_val
                - refund_fin_val - fba_inbound_val
                - wh_loss_val - amz_other_val,
                2,
            )
            np_val = round(cm2_val - overhead_val, 2)
            p["cm2_profit"] = cm2_val
            p["cm2_percent"] = round(cm2_val / rev_val * 100, 2) if rev_val else 0.0
            p["np_profit"] = np_val
            p["np_percent"] = round(np_val / rev_val * 100, 2) if rev_val else 0.0
            p.pop("afn_units", None)

        # --- Sort ---
        sort_key = sort_by if (products and sort_by in products[0]) else "cm1_profit"
        reverse = sort_dir.lower() == "desc"
        products.sort(key=lambda x: x.get(sort_key, 0) or 0, reverse=reverse)

        # --- Paginate ---
        total = len(products)
        pages = math.ceil(total / page_size) if total else 0
        start = (page - 1) * page_size
        page_items = products[start:start + page_size]

        # --- Enrich with import flag ---
        try:
            from app.services.import_products import get_import_skus
            import_skus = get_import_skus()
            for p in page_items:
                p["is_import"] = p.get("entity_type") == "sku" and p.get("sku", "") in import_skus
        except Exception as exc:
            log.warning("profit_engine.import_flag_enrichment_failed", error=str(exc))
            _warnings_append("Import flag enrichment failed — is_import field may be missing")

        # --- Summary ---
        total_rev = sum(p["revenue_pln"] for p in products)
        total_cogs = sum(p["cogs_pln"] for p in products)
        total_fees = sum(p["amazon_fees_pln"] for p in products)
        total_logistics = sum(p.get("logistics_pln", 0) for p in products)
        total_cm1 = sum(p["cm1_profit"] for p in products)
        total_units = sum(p["units"] for p in products)
        total_ads_cost = sum(p.get("ads_cost_pln", 0) for p in products)
        total_cm2 = sum(p.get("cm2_profit", 0) for p in products)
        total_np = sum(p.get("np_profit", 0) for p in products)
        total_returns_net = sum(p.get("returns_net_pln", 0) for p in products)
        total_refund_gross = sum(p.get("refund_gross_pln", 0) for p in products)
        total_return_handling = sum(p.get("return_handling_pln", 0) for p in products)
        total_return_cogs_recovered = sum(p.get("return_cogs_recovered_pln", 0) for p in products)
        total_fba_storage = sum(p.get("fba_storage_fee_pln", 0) for p in products)
        total_fba_aged = sum(p.get("fba_aged_fee_pln", 0) for p in products)
        total_fba_removal = sum(p.get("fba_removal_fee_pln", 0) for p in products)
        total_fba_liquidation = sum(p.get("fba_liquidation_fee_pln", 0) for p in products)
        total_overhead_allocated = sum(p.get("overhead_allocated_pln", 0) for p in products)
        weighted_overhead_conf = sum(
            _f(p.get("overhead_allocated_pln")) * _f(p.get("overhead_confidence_pct"))
            for p in products
        )
        total_refund_orders = sum(p.get("refund_orders", 0) for p in products)
        total_refund_units = sum(p.get("refund_units", 0) for p in products)
        total_refund_cost = sum(p.get("refund_cost_pln", 0) for p in products)

        # --- Refund summary ---
        # Return status orders are truly excluded (not in Shipped filter)
        # Shipped+refund are now INCLUDED in profit with revenue deduction
        refund_params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        refund_where_extra = ""
        if marketplace_id:
            refund_where_extra += " AND o.marketplace_id = ?"
            refund_params.append(marketplace_id)
        cur.execute(f"""
            SELECT
                SUM(CASE WHEN o.status = 'Return' THEN 1 ELSE 0 END)  AS return_count,
                SUM(CASE WHEN o.status = 'Return'
                    THEN ISNULL(o.refund_amount_pln, 0) ELSE 0 END)   AS return_total_pln,
                SUM(CASE WHEN o.refund_type = 'full' THEN 1 ELSE 0 END)    AS full_count,
                SUM(CASE WHEN o.refund_type = 'partial' THEN 1 ELSE 0 END) AS partial_count
            FROM dbo.acc_order o WITH (NOLOCK)
            WHERE (o.is_refund = 1 OR o.status = 'Return')
              AND o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              {refund_where_extra}
        """, refund_params)
        rr = cur.fetchone()
        refund_count = rr[0] or 0
        refund_total_pln = float(rr[1] or 0)
        refund_full = rr[2] or 0
        refund_partial = rr[3] or 0

        result = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "summary": {
                "total_revenue_pln": round(total_rev, 2),
                "total_cogs_pln": round(total_cogs, 2),
                "total_fees_pln": round(total_fees, 2),
                "total_cm1_pln": round(total_cm1, 2),
                "total_cm1_pct": round(total_cm1 / total_rev * 100, 2) if total_rev else 0.0,
                "total_ads_cost_pln": round(total_ads_cost, 2),
                "total_logistics_pln": round(total_logistics, 2),
                "total_cm2_pln": round(total_cm2, 2),
                "total_cm2_pct": round(total_cm2 / total_rev * 100, 2) if total_rev else 0.0,
                "total_np_pln": round(total_np, 2),
                "total_np_pct": round(total_np / total_rev * 100, 2) if total_rev else 0.0,
                "total_units": total_units,
                "avg_confidence": round(sum(p["confidence_score"] for p in products) / total, 1) if total else 0.0,
                "total_returns_net_pln": round(total_returns_net, 2),
                "total_refund_gross_pln": round(total_refund_gross, 2),
                "total_return_handling_pln": round(total_return_handling, 2),
                "total_return_cogs_recovered_pln": round(total_return_cogs_recovered, 2),
                "total_fba_storage_fee_pln": round(total_fba_storage, 2),
                "total_fba_aged_fee_pln": round(total_fba_aged, 2),
                "total_fba_removal_fee_pln": round(total_fba_removal, 2),
                "total_fba_liquidation_fee_pln": round(total_fba_liquidation, 2),
                "total_overhead_allocated_pln": round(total_overhead_allocated, 2),
                "overhead_allocation_method": (
                    "mixed"
                    if len({str(p.get("overhead_allocation_method") or "none") for p in products if _f(p.get("overhead_allocated_pln")) > 0}) > 1
                    else next(iter({str(p.get("overhead_allocation_method") or "none") for p in products if _f(p.get("overhead_allocated_pln")) > 0}), "none")
                ),
                "overhead_confidence_pct": (
                    round(weighted_overhead_conf / total_overhead_allocated, 1)
                    if total_overhead_allocated > 0
                    else 0.0
                ),
                # Refund info (included in profit as cost — Shipped+refund orders)
                "refund_shipped_orders": total_refund_orders,
                "refund_shipped_units": total_refund_units,
                "refund_shipped_cost_pln": round(total_refund_cost, 2),
                # Return status orders (not Shipped — truly excluded)
                "refund_orders_excluded": refund_count,
                "refund_full_count": refund_full,
                "refund_partial_count": refund_partial,
                "refund_total_pln": round(refund_total_pln, 2),
            },
            "items": page_items,
            "warnings": _warnings_collect(),
        }
        _result_cache_set(cache_key, result)
        return result
    finally:
        conn.close()


def _ensure_offer_fee_expected_schema() -> None:
    """Ensure expected-fee cache table exists before using it in what-if query."""
    global _OFFER_FEE_EXPECTED_SCHEMA_READY
    if _OFFER_FEE_EXPECTED_SCHEMA_READY:
        return
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.acc_offer_fee_expected', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.acc_offer_fee_expected (
                    id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
                    marketplace_id NVARCHAR(64) NOT NULL,
                    sku NVARCHAR(120) NOT NULL,
                    asin NVARCHAR(32) NULL,
                    offer_price DECIMAL(18,4) NOT NULL,
                    currency NVARCHAR(8) NOT NULL,
                    fulfillment_channel NVARCHAR(20) NULL,
                    expected_fba_fee DECIMAL(18,4) NULL,
                    expected_referral_fee DECIMAL(18,4) NULL,
                    expected_total_fee DECIMAL(18,4) NULL,
                    expected_referral_rate DECIMAL(18,6) NULL,
                    fee_detail_json NVARCHAR(MAX) NULL,
                    source NVARCHAR(40) NOT NULL DEFAULT 'product_fees_v0',
                    status NVARCHAR(32) NOT NULL DEFAULT 'ok',
                    error_message NVARCHAR(1000) NULL,
                    synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
                );
            END;

            IF NOT EXISTS (
                SELECT 1
                FROM sys.indexes
                WHERE name = 'UX_acc_offer_fee_expected_key'
                  AND object_id = OBJECT_ID('dbo.acc_offer_fee_expected')
            )
            BEGIN
                CREATE UNIQUE INDEX UX_acc_offer_fee_expected_key
                    ON dbo.acc_offer_fee_expected(marketplace_id, sku, source);
            END;

            IF NOT EXISTS (
                SELECT 1
                FROM sys.indexes
                WHERE name = 'IX_acc_offer_fee_expected_lookup'
                  AND object_id = OBJECT_ID('dbo.acc_offer_fee_expected')
            )
            BEGIN
                CREATE INDEX IX_acc_offer_fee_expected_lookup
                    ON dbo.acc_offer_fee_expected(marketplace_id, sku, synced_at DESC);
            END;
            """
        )
        conn.commit()
        _OFFER_FEE_EXPECTED_SCHEMA_READY = True
    except Exception as exc:
        log.warning("profit_engine.offer_fee_expected_schema_error", error=str(exc))
    finally:
        conn.close()


def get_product_what_if_table(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    marketplace_ids: str | None = None,
    sku_search: str | None = None,
    fulfillment_channels: str | None = None,
    parent_asin: str | None = None,
    profit_mode: str = "cm1",
    include_cost_components: bool = False,
    quantity: int = 1,
    include_shipping_charge: bool = True,
    only_open: bool = True,
    group_by: str = "offer",
    sort_by: str = "cm2_profit",
    sort_dir: str = "desc",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Simulate what-if margin for currently open offers (offer/ASIN/parent grouping)."""
    scenario_qty = max(1, int(quantity or 1))
    normalized_sort_dir = "desc" if str(sort_dir).lower() == "desc" else "asc"
    profit_mode_norm = str(profit_mode or "cm1").strip().lower()
    parent_asin_filter = str(parent_asin or "").strip()
    need_extended_costs = (
        bool(include_cost_components)
        or profit_mode_norm in {"cm2", "np"}
        or sort_by in {"cm2_profit", "cm2_percent", "np_profit", "np_percent"}
    )
    need_ads_costs = (
        bool(include_cost_components)
        or
        profit_mode_norm in {"cm2", "np"}
        or sort_by in {"cm2_profit", "cm2_percent", "np_profit", "np_percent", "ads_cost_pln"}
    )

    cache_key = (
        f"pwi:{date_from}:{date_to}:{marketplace_id}:{marketplace_ids}:{sku_search}:"
        f"{fulfillment_channels}:{parent_asin_filter}:{profit_mode}:{include_cost_components}:"
        f"{scenario_qty}:{include_shipping_charge}:{only_open}:"
        f"{group_by}:{sort_by}:{normalized_sort_dir}:{page}:{page_size}"
    )
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return cached

    _ensure_offer_fee_expected_schema()

    market_list = _parse_csv_list(marketplace_ids)
    if marketplace_id and marketplace_id not in market_list:
        market_list.append(marketplace_id)
    raw_fulfillment_list = [x.upper() for x in _parse_csv_list(fulfillment_channels)]
    fulfillment_list: list[str] = []
    include_other_fulfillment = False
    for val in raw_fulfillment_list:
        if val in {"AFN", "FBA"}:
            fulfillment_list.append("FBA")
        elif val in {"MFN", "FBM"}:
            fulfillment_list.append("FBM")
        elif val in {"OTHER", "SELLERFLEX", "SELLERFLEX/OTHER", "SELLER_FLEX"}:
            include_other_fulfillment = True
        elif val:
            fulfillment_list.append(val)
    if fulfillment_list:
        fulfillment_list = sorted(set(fulfillment_list))

    group_mode = (group_by or "offer").lower()
    allowed_group_modes = {"offer", "asin_marketplace", "asin", "parent_marketplace", "parent"}
    if group_mode not in allowed_group_modes:
        group_mode = "offer"

    conn = _connect()
    try:
        cur = conn.cursor()
        ensure_profit_cost_model_schema()
        return_handling_per_unit = _get_cost_config_decimal(cur, "return_handling_per_unit_pln", 0.0)
        wheres = ["1=1"]
        # Exclude Amazon Renewed (used) products from what-if analysis
        wheres.append("o.sku NOT LIKE 'amzn.gr.%%'")
        params: list[Any] = []

        if only_open:
            wheres.append("ISNULL(o.status, '') IN ('Active', 'Incomplete')")
        if market_list:
            placeholders = ",".join("?" for _ in market_list)
            wheres.append(f"o.marketplace_id IN ({placeholders})")
            params.extend(market_list)
        if fulfillment_list or include_other_fulfillment:
            fc_clauses: list[str] = []
            if fulfillment_list:
                placeholders = ",".join("?" for _ in fulfillment_list)
                fc_clauses.append(f"UPPER(ISNULL(o.fulfillment_channel,'')) IN ({placeholders})")
                params.extend(fulfillment_list)
            if include_other_fulfillment:
                fc_clauses.append(
                    "("
                    "ISNULL(o.fulfillment_channel,'') <> '' AND "
                    "UPPER(ISNULL(o.fulfillment_channel,'')) NOT IN ('FBA','FBM','AFN','MFN')"
                    ")"
                )
            wheres.append("(" + " OR ".join(fc_clauses) + ")")

        sku_tokens = _parse_search_tokens(sku_search)
        if sku_tokens:
            token_clauses: list[str] = []
            for token in sku_tokens:
                like = f"%{token}%"
                token_clauses.append(
                    "("
                    "o.sku LIKE ? OR o.asin LIKE ? OR ISNULL(p.title,'') LIKE ? "
                    "OR ISNULL(reg.product_name,'') LIKE ?"
                    ")"
                )
                params.extend([like, like, like, like])
            wheres.append("(" + " OR ".join(token_clauses) + ")")
        if parent_asin_filter:
            wheres.append(
                "ISNULL(NULLIF(COALESCE(NULLIF(reg.parent_asin, ''), NULLIF(o.asin, '')), ''), '') = ?"
            )
            params.append(parent_asin_filter)

        where_sql = " AND ".join(wheres)
        needs_listing_search = bool(sku_tokens or parent_asin_filter)
        count_from = """
            FROM dbo.acc_offer o WITH (NOLOCK)
            INNER JOIN dbo.acc_marketplace m WITH (NOLOCK) ON m.id = o.marketplace_id
        """
        if needs_listing_search:
            count_from += """
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = o.product_id
            OUTER APPLY (
                SELECT TOP 1 product_name, brand, category_1, parent_asin
                FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
                WHERE r.merchant_sku = o.sku
                   OR r.merchant_sku_alt = o.sku
                   OR (o.asin IS NOT NULL AND r.asin = o.asin)
                ORDER BY r.updated_at DESC
            ) reg
            """
        count_sql = f"SELECT COUNT(*) {count_from} WHERE {where_sql}"
        cur.execute(count_sql, params)
        total = _i(cur.fetchone()[0])
        pages = math.ceil(total / page_size) if total else 0
        offset = max(0, (page - 1) * page_size)
        grouped_mode = group_mode != "offer"
        use_light_offer_query = grouped_mode and not need_extended_costs and not need_ads_costs and not needs_listing_search

        if use_light_offer_query:
            offer_sql = f"""
                SELECT
                    o.sku,
                    o.asin,
                    ISNULL(NULLIF(o.asin, ''), NULL) AS parent_asin,
                    o.marketplace_id,
                    m.code AS marketplace_code,
                    ISNULL(p.title, '') AS title,
                    ISNULL(p.brand, '') AS brand,
                    ISNULL(p.category, '') AS category,
                    ISNULL(p.internal_sku, '') AS internal_sku,
                    ISNULL(o.fulfillment_channel, '') AS fulfillment_channel,
                    ISNULL(o.status, '') AS offer_status,
                    ISNULL(o.currency, m.currency) AS offer_currency,
                    CAST(ISNULL(o.price, 0) AS FLOAT) AS offer_price,
                    CAST(ISNULL(o.fba_fee, 0) AS FLOAT) AS offer_fba_fee,
                    CAST(ISNULL(o.referral_fee_rate, 0) AS FLOAT) AS offer_referral_fee_rate,
                    CAST(ISNULL(p.netto_purchase_price_pln, 0) AS FLOAT) AS purchase_price_pln,
                    CAST(0 AS FLOAT) AS expected_fba_fee,
                    CAST(0 AS FLOAT) AS expected_referral_fee,
                    CAST(0 AS FLOAT) AS expected_referral_rate,
                    '' AS expected_fee_status
                FROM dbo.acc_offer o WITH (NOLOCK)
                INNER JOIN dbo.acc_marketplace m WITH (NOLOCK) ON m.id = o.marketplace_id
                LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = o.product_id
                WHERE {where_sql}
                ORDER BY o.updated_at DESC, o.sku ASC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            cur.execute(offer_sql, [*params, offset, page_size])
        else:
            offer_sql = f"""
                SELECT
                    o.sku,
                    o.asin,
                    ISNULL(NULLIF(COALESCE(NULLIF(reg.parent_asin, ''), NULLIF(o.asin, '')), ''), NULL) AS parent_asin,
                    o.marketplace_id,
                    m.code AS marketplace_code,
                    ISNULL(reg.product_name, p.title) AS title,
                    ISNULL(reg.brand, p.brand) AS brand,
                    ISNULL(reg.category_1, p.category) AS category,
                    ISNULL(p.internal_sku, reg.internal_sku) AS internal_sku,
                    ISNULL(o.fulfillment_channel, '') AS fulfillment_channel,
                    ISNULL(o.status, '') AS offer_status,
                    ISNULL(o.currency, m.currency) AS offer_currency,
                    CAST(ISNULL(o.price, 0) AS FLOAT) AS offer_price,
                    CAST(ISNULL(o.fba_fee, 0) AS FLOAT) AS offer_fba_fee,
                    CAST(ISNULL(o.referral_fee_rate, 0) AS FLOAT) AS offer_referral_fee_rate,
                    CAST(ISNULL(p.netto_purchase_price_pln, 0) AS FLOAT) AS purchase_price_pln,
                    CAST(ISNULL(exp.expected_fba_fee, 0) AS FLOAT) AS expected_fba_fee,
                    CAST(ISNULL(exp.expected_referral_fee, 0) AS FLOAT) AS expected_referral_fee,
                    CAST(ISNULL(exp.expected_referral_rate, 0) AS FLOAT) AS expected_referral_rate,
                    ISNULL(exp.status, '') AS expected_fee_status
                FROM dbo.acc_offer o WITH (NOLOCK)
                INNER JOIN dbo.acc_marketplace m WITH (NOLOCK) ON m.id = o.marketplace_id
                LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = o.product_id
                OUTER APPLY (
                    SELECT TOP 1 product_name, brand, category_1, internal_sku, parent_asin
                    FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
                    WHERE r.merchant_sku = o.sku
                       OR r.merchant_sku_alt = o.sku
                       OR (o.asin IS NOT NULL AND r.asin = o.asin)
                    ORDER BY r.updated_at DESC
                ) reg
                OUTER APPLY (
                    SELECT TOP 1
                        expected_fba_fee,
                        expected_referral_fee,
                        expected_referral_rate,
                        status
                    FROM dbo.acc_offer_fee_expected e WITH (NOLOCK)
                    WHERE e.marketplace_id = o.marketplace_id
                      AND e.sku = o.sku
                      AND e.source = 'product_fees_v0'
                    ORDER BY e.synced_at DESC
                ) exp
                WHERE {where_sql}
                ORDER BY o.updated_at DESC, o.sku ASC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            if grouped_mode:
                cur.execute(offer_sql, [*params, 0, max(total, 1)])
            else:
                cur.execute(offer_sql, [*params, offset, page_size])
        offer_rows = _fetchall_dict(cur)

        if not offer_rows:
            empty_result = {
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": pages,
                "scenario_qty": scenario_qty,
                "include_shipping_charge": include_shipping_charge,
                "summary": {
                    "summary_scope": "page",
                    "total_revenue_pln": 0.0,
                    "total_cogs_pln": 0.0,
                    "total_fees_pln": 0.0,
                    "total_logistics_pln": 0.0,
                    "total_shipping_charge_pln": 0.0,
                    "total_ads_pln": 0.0,
                    "total_returns_net_pln": 0.0,
                    "total_fba_storage_fee_pln": 0.0,
                    "total_fba_aged_fee_pln": 0.0,
                    "total_fba_removal_fee_pln": 0.0,
                    "total_fba_liquidation_fee_pln": 0.0,
                    "total_overhead_allocated_pln": 0.0,
                    "total_cm1_pln": 0.0,
                    "total_cm2_pln": 0.0,
                    "total_np_pln": 0.0,
                    "total_cm1_pct": 0.0,
                    "total_cm2_pct": 0.0,
                    "total_np_pct": 0.0,
                    "total_offers": 0,
                    "avg_confidence": 0.0,
                },
                "items": [],
            }
            _result_cache_set(cache_key, empty_result)
            return empty_result

        cur.execute(
            """
            IF OBJECT_ID('tempdb..#targets') IS NOT NULL DROP TABLE #targets;
            CREATE TABLE #targets(
                sku NVARCHAR(120) NOT NULL,
                marketplace_id NVARCHAR(160) NOT NULL,
                fulfillment_channel NVARCHAR(20) NULL,
                PRIMARY KEY (sku, marketplace_id)
            );
            """
        )
        target_rows = [
            (
                str(r.get("sku") or "").strip(),
                str(r.get("marketplace_id") or "").strip(),
                str(r.get("fulfillment_channel") or "").strip(),
            )
            for r in offer_rows
            if str(r.get("sku") or "").strip() and str(r.get("marketplace_id") or "").strip()
        ]
        cur.executemany(
            "INSERT INTO #targets (sku, marketplace_id, fulfillment_channel) VALUES (?, ?, ?)",
            target_rows,
        )

        if need_ads_costs:
            history_sql = """
                SELECT
                    t.sku,
                    t.marketplace_id,
                    SUM(ISNULL(ol.quantity_ordered, 0)) AS units_hist,
                    COUNT(DISTINCT o.id) AS orders_hist,
                    SUM(ISNULL(ol.cogs_pln, 0)) AS cogs_hist,
                    SUM(ISNULL(ol.fba_fee_pln, 0)) AS fba_hist,
                    SUM(ISNULL(ol.referral_fee_pln, 0)) AS referral_hist,
                    SUM(
                        ISNULL(o.ads_cost_pln, 0) * CASE
                            WHEN ISNULL(order_totals.order_revenue, 0) > 0
                                THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(order_totals.order_revenue, 0)
                            WHEN ISNULL(order_totals.order_units, 0) > 0
                                THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(order_totals.order_units, 0)
                            ELSE 0
                        END
                    ) AS ads_hist
                FROM #targets t
                INNER JOIN dbo.acc_order_line ol WITH (NOLOCK)
                    ON ol.sku = t.sku
                INNER JOIN dbo.acc_order o WITH (NOLOCK)
                    ON o.id = ol.order_id
                   AND o.marketplace_id = t.marketplace_id
                OUTER APPLY (
                    SELECT
                        SUM(ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0) - ISNULL(ol2.promotion_discount, 0)) AS order_revenue,
                        SUM(ISNULL(ol2.quantity_ordered, 0)) AS order_units
                    FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                    WHERE ol2.order_id = o.id
                ) order_totals
                WHERE o.status = 'Shipped'
                  AND o.purchase_date >= CAST(? AS DATE)
                  AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
                  AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
                  AND o.amazon_order_id NOT LIKE 'S02-%'
                GROUP BY t.sku, t.marketplace_id
            """
        else:
            history_sql = """
                SELECT
                    t.sku,
                    t.marketplace_id,
                    SUM(ISNULL(ol.quantity_ordered, 0)) AS units_hist,
                    COUNT(DISTINCT o.id) AS orders_hist,
                    SUM(ISNULL(ol.cogs_pln, 0)) AS cogs_hist,
                    SUM(ISNULL(ol.fba_fee_pln, 0)) AS fba_hist,
                    SUM(ISNULL(ol.referral_fee_pln, 0)) AS referral_hist,
                    CAST(0 AS FLOAT) AS ads_hist
                FROM #targets t
                INNER JOIN dbo.acc_order_line ol WITH (NOLOCK)
                    ON ol.sku = t.sku
                INNER JOIN dbo.acc_order o WITH (NOLOCK)
                    ON o.id = ol.order_id
                   AND o.marketplace_id = t.marketplace_id
                WHERE o.status = 'Shipped'
                  AND o.purchase_date >= CAST(? AS DATE)
                  AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
                  AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
                  AND o.amazon_order_id NOT LIKE 'S02-%'
                GROUP BY t.sku, t.marketplace_id
            """
        hist_rows: list[dict[str, Any]] = []
        if need_extended_costs or need_ads_costs:
            cur.execute(history_sql, [date_from.isoformat(), date_to.isoformat()])
            hist_rows = _fetchall_dict(cur)
        hist_map: dict[tuple[str, str], dict[str, float]] = {}
        for row in hist_rows:
            key = (str(row["sku"]), str(row["marketplace_id"]))
            units_hist = max(1, _i(row.get("units_hist")))
            hist_map[key] = {
                "units_hist": _i(row.get("units_hist")),
                "orders_hist": _i(row.get("orders_hist")),
                "cogs_per_unit_hist": _f(row.get("cogs_hist")) / units_hist,
                "fba_per_unit_hist": _f(row.get("fba_hist")) / units_hist,
                "ref_per_unit_hist": _f(row.get("referral_hist")) / units_hist,
                "ads_per_unit_hist": _f(row.get("ads_hist")) / units_hist,
            }

        if need_extended_costs:
            fba_component_pools = _load_fba_component_pools(
                cur,
                date_from=date_from,
                date_to=date_to,
                marketplace_id=marketplace_id,
            )
            overhead_pools = _load_overhead_pools(
                cur,
                date_from=date_from,
                date_to=date_to,
                marketplace_id=marketplace_id,
            )
        else:
            fba_component_pools = {}
            overhead_pools = []

        ratio_by_marketplace: dict[str, dict[str, float]] = {}
        if need_extended_costs:
            cur.execute(
                f"""
            SELECT
                o.marketplace_id,
                SUM(
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                    * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')})
                )
                + ISNULL(SUM(
                    ISNULL(spo.shipping_charge_pln, 0) * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                ), 0) AS revenue_pln,
                SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1 THEN
                    ABS(ISNULL(o.refund_amount_pln, 0))
                    * (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                    / NULLIF(olt.order_line_total, 0)
                ELSE 0 END) AS refund_gross_pln,
                SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1 AND o.refund_type = 'full'
                    THEN ISNULL(ol.cogs_pln, 0) ELSE 0 END) AS return_cogs_recovered_pln,
                SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1
                    THEN ISNULL(ol.quantity_ordered, 0) ELSE 0 END) AS refund_units,
                SUM(CASE WHEN ISNULL(o.fulfillment_channel, '') = 'AFN'
                    THEN ISNULL(ol.quantity_ordered, 0) ELSE 0 END) AS afn_units
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            INNER JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            INNER JOIN (SELECT DISTINCT marketplace_id FROM #targets) t
              ON t.marketplace_id = o.marketplace_id
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= o.purchase_date
                ORDER BY er.rate_date DESC
            ) fx
            OUTER APPLY (
                SELECT
                    ISNULL(SUM(
                        ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                        - ISNULL(ol2.promotion_discount, 0)
                    ), 0) AS order_line_total,
                    ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            OUTER APPLY (
                SELECT SUM(
                    CASE WHEN ft.charge_type IN ('ShippingCharge','ShippingTax','ShippingDiscount')
                        THEN ISNULL(ft.amount_pln,
                            ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")})
                        ELSE 0
                    END
                ) AS shipping_charge_pln
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                WHERE ft.amazon_order_id = o.amazon_order_id
                  AND (ft.marketplace_id = o.marketplace_id OR ft.marketplace_id IS NULL)
            ) spo
            WHERE o.status = 'Shipped'
              AND o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
              AND o.amazon_order_id NOT LIKE 'S02-%'
            GROUP BY o.marketplace_id
            """,
                [date_from.isoformat(), date_to.isoformat()],
            )
            ratio_rows = _fetchall_dict(cur)
            for rr in ratio_rows:
                mkt = str(rr.get("marketplace_id") or "")
                revenue_total = max(_f(rr.get("revenue_pln")), 0.0)
                refund_gross = _f(rr.get("refund_gross_pln"))
                recovered = _f(rr.get("return_cogs_recovered_pln"))
                refund_units = _i(rr.get("refund_units"))
                afn_units = max(_i(rr.get("afn_units")), 0)
                returns_net_total = max(0.0, refund_gross - recovered) + refund_units * return_handling_per_unit

                fba_pool = fba_component_pools.get(
                    mkt,
                    {"storage": 0.0, "aged": 0.0, "removal": 0.0, "liquidation": 0.0},
                )

                oh_rows = [x for x in overhead_pools if not x.get("marketplace_id") or x.get("marketplace_id") == mkt]
                oh_total = sum(_f(x.get("amount_pln")) for x in oh_rows)
                oh_conf = 0.0
                if oh_total > 0:
                    oh_conf = sum(_f(x.get("amount_pln")) * _f(x.get("confidence_pct"), 50.0) for x in oh_rows) / oh_total
                oh_methods = {str(x.get("allocation_method") or "revenue_share").lower() for x in oh_rows if _f(x.get("amount_pln")) != 0}
                oh_method = "none"
                if len(oh_methods) == 1:
                    oh_method = next(iter(oh_methods))
                elif len(oh_methods) > 1:
                    oh_method = "mixed"

                ratio_by_marketplace[mkt] = {
                    "returns_net_ratio": (returns_net_total / revenue_total) if revenue_total > 0 else 0.0,
                    "storage_per_afn_unit": (_f(fba_pool.get("storage")) / afn_units) if afn_units > 0 else 0.0,
                    "aged_per_afn_unit": (_f(fba_pool.get("aged")) / afn_units) if afn_units > 0 else 0.0,
                    "removal_per_afn_unit": (_f(fba_pool.get("removal")) / afn_units) if afn_units > 0 else 0.0,
                    "liquidation_per_afn_unit": (_f(fba_pool.get("liquidation")) / afn_units) if afn_units > 0 else 0.0,
                    "overhead_ratio": (oh_total / revenue_total) if revenue_total > 0 else 0.0,
                    "overhead_confidence_pct": oh_conf if oh_total > 0 else 0.0,
                    "overhead_method": oh_method,
                }

        bucket_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        bucket_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")
        bucket_sql = f"""
            WITH target_orders AS (
                SELECT DISTINCT
                    t.sku,
                    t.marketplace_id,
                    o.id AS order_id,
                    o.amazon_order_id,
                    {bucket_logistics_value_sql} AS logistics_pln
                FROM #targets t
                INNER JOIN dbo.acc_order_line ol WITH (NOLOCK)
                    ON ol.sku = t.sku
                INNER JOIN dbo.acc_order o WITH (NOLOCK)
                    ON o.id = ol.order_id
                   AND o.marketplace_id = t.marketplace_id
                {bucket_logistics_join_sql}
                WHERE o.status = 'Shipped'
                  AND o.purchase_date >= CAST(? AS DATE)
                  AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
                  AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
                  AND o.amazon_order_id NOT LIKE 'S02-%'
            ),
            order_mix AS (
                SELECT
                    to1.sku,
                    to1.marketplace_id,
                    to1.order_id,
                    to1.amazon_order_id,
                    to1.logistics_pln,
                    SUM(CASE WHEN ol.sku = to1.sku THEN ISNULL(ol.quantity_ordered, 0) ELSE 0 END) AS sku_qty,
                    COUNT(DISTINCT ISNULL(ol.sku, '')) AS sku_count
                FROM target_orders to1
                INNER JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = to1.order_id
                GROUP BY to1.sku, to1.marketplace_id, to1.order_id, to1.amazon_order_id, to1.logistics_pln
            ),
            shipping_per_order AS (
                SELECT
                    om.amazon_order_id,
                    om.marketplace_id,
                    SUM(
                        CASE
                            WHEN ft.charge_type IN ('ShippingCharge', 'ShippingTax', 'ShippingDiscount') THEN
                                ISNULL(ft.amount_pln,
                                    ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")}
                                )
                            ELSE 0
                        END
                    ) AS shipping_charge_pln
                FROM (SELECT DISTINCT amazon_order_id, marketplace_id FROM order_mix) om
                LEFT JOIN dbo.acc_finance_transaction ft WITH (NOLOCK)
                    ON om.amazon_order_id = ft.amazon_order_id
                   AND (
                       om.marketplace_id = ft.marketplace_id
                       OR ft.marketplace_id IS NULL
                       OR om.marketplace_id IS NULL
                   )
                GROUP BY om.amazon_order_id, om.marketplace_id
            ),
            courier_raw AS (
                SELECT
                    om.sku,
                    om.marketplace_id,
                    CAST(om.sku_qty AS INT) AS qty_bucket,
                    om.logistics_pln AS metric_value
                FROM order_mix om
                WHERE om.sku_count = 1
                  AND om.sku_qty > 0
                  AND om.logistics_pln > 0
            ),
            shipping_raw AS (
                SELECT
                    om.sku,
                    om.marketplace_id,
                    CAST(om.sku_qty AS INT) AS qty_bucket,
                    CAST(ISNULL(spo.shipping_charge_pln, 0) AS FLOAT) AS metric_value
                FROM order_mix om
                INNER JOIN shipping_per_order spo
                    ON spo.amazon_order_id = om.amazon_order_id
                   AND spo.marketplace_id = om.marketplace_id
                WHERE om.sku_count = 1
                  AND om.sku_qty > 0
                  AND ISNULL(spo.shipping_charge_pln, 0) > 0
            ),
            courier_stats AS (
                SELECT
                    sku,
                    marketplace_id,
                    qty_bucket,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric_value)
                        OVER (PARTITION BY sku, marketplace_id, qty_bucket) AS median_value,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value)
                        OVER (PARTITION BY sku, marketplace_id, qty_bucket) AS p75_value,
                    COUNT(*) OVER (PARTITION BY sku, marketplace_id, qty_bucket) AS samples,
                    ROW_NUMBER() OVER (PARTITION BY sku, marketplace_id, qty_bucket ORDER BY qty_bucket) AS rn
                FROM courier_raw
            ),
            shipping_stats AS (
                SELECT
                    sku,
                    marketplace_id,
                    qty_bucket,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric_value)
                        OVER (PARTITION BY sku, marketplace_id, qty_bucket) AS median_value,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value)
                        OVER (PARTITION BY sku, marketplace_id, qty_bucket) AS p75_value,
                    COUNT(*) OVER (PARTITION BY sku, marketplace_id, qty_bucket) AS samples,
                    ROW_NUMBER() OVER (PARTITION BY sku, marketplace_id, qty_bucket ORDER BY qty_bucket) AS rn
                FROM shipping_raw
            ),
            single_order_samples AS (
                SELECT sku, marketplace_id, COUNT(*) AS sample_count
                FROM order_mix
                WHERE sku_count = 1 AND sku_qty > 0
                GROUP BY sku, marketplace_id
            )
            SELECT
                'courier' AS metric_type,
                cs.sku,
                cs.marketplace_id,
                cs.qty_bucket,
                CAST(cs.median_value AS FLOAT) AS median_value,
                CAST(cs.p75_value AS FLOAT) AS p75_value,
                cs.samples,
                ISNULL(sos.sample_count, 0) AS single_order_samples
            FROM courier_stats cs
            LEFT JOIN single_order_samples sos
                ON sos.sku = cs.sku AND sos.marketplace_id = cs.marketplace_id
            WHERE cs.rn = 1
            UNION ALL
            SELECT
                'shipping' AS metric_type,
                ss.sku,
                ss.marketplace_id,
                ss.qty_bucket,
                CAST(ss.median_value AS FLOAT) AS median_value,
                CAST(ss.p75_value AS FLOAT) AS p75_value,
                ss.samples,
                ISNULL(sos.sample_count, 0) AS single_order_samples
            FROM shipping_stats ss
            LEFT JOIN single_order_samples sos
                ON sos.sku = ss.sku AND sos.marketplace_id = ss.marketplace_id
            WHERE ss.rn = 1
        """
        bucket_rows: list[dict[str, Any]] = []
        # For CM1-first what-if, avoid expensive percentile windows and use TKL-priority fallback.
        if need_extended_costs or need_ads_costs:
            cur.execute(bucket_sql, [date_from.isoformat(), date_to.isoformat()])
            bucket_rows = _fetchall_dict(cur)

        courier_buckets: dict[tuple[str, str], dict[int, dict[str, float]]] = defaultdict(dict)
        shipping_buckets: dict[tuple[str, str], dict[int, dict[str, float]]] = defaultdict(dict)
        single_samples: dict[tuple[str, str], int] = {}
        for row in bucket_rows:
            key = (str(row["sku"]), str(row["marketplace_id"]))
            qty_bucket = max(1, _i(row.get("qty_bucket"), 1))
            p75 = _f(row.get("p75_value"))
            payload = {
                "median": _f(row.get("median_value")),
                "p75": p75 if p75 > 0 else _f(row.get("median_value")),
                "samples": _i(row.get("samples")),
            }
            if str(row.get("metric_type")) == "shipping":
                shipping_buckets[key][qty_bucket] = payload
            else:
                courier_buckets[key][qty_bucket] = payload
            single_samples[key] = max(single_samples.get(key, 0), _i(row.get("single_order_samples")))

        tkl_country_cost, tkl_sku_cost = _load_tkl_priority_maps()
        mp_to_country = {
            "UK": "GB",
        }

        items: list[dict[str, Any]] = []
        for row in offer_rows:
            sku = str(row.get("sku") or "")
            mkt = str(row.get("marketplace_id") or "")
            key = (sku, mkt)
            hist = hist_map.get(key, {})
            courier_map = courier_buckets.get(key, {})
            shipping_map = shipping_buckets.get(key, {})
            single_sample_count = single_samples.get(key, 0)

            offer_currency = str(row.get("offer_currency") or "EUR")
            fx_rate = _fx_rate_for_currency(offer_currency, as_of=date_to)

            offer_price = _f(row.get("offer_price"))
            offer_price_pln = round(offer_price * fx_rate, 2)
            fc = str(row.get("fulfillment_channel") or "").upper()
            marketplace_code = str(row.get("marketplace_code") or _mkt_code(mkt)).upper()
            country_code = mp_to_country.get(marketplace_code, marketplace_code)
            internal_sku = _norm_internal_sku(row.get("internal_sku"))
            tkl_country = tkl_country_cost.get((internal_sku, country_code)) if internal_sku else None
            tkl_sku = tkl_sku_cost.get(internal_sku) if internal_sku else None

            purchase_price = _f(row.get("purchase_price_pln"))
            cogs_unit_hist = _f(hist.get("cogs_per_unit_hist"))
            if purchase_price > 0:
                cogs_per_unit = round(purchase_price, 4)
                cogs_source = "purchase_price"
            elif cogs_unit_hist > 0:
                cogs_per_unit = round(cogs_unit_hist, 4)
                cogs_source = "historical_line_cogs"
            else:
                cogs_per_unit = 0.0
                cogs_source = "missing"

            offer_fba_fee = _f(row.get("offer_fba_fee"))
            fba_hist_unit = _f(hist.get("fba_per_unit_hist"))
            try:
                expected_fba_fee = float(row.get("expected_fba_fee") or 0)
            except Exception:
                expected_fba_fee = 0.0
            if fc == "FBA":
                if offer_fba_fee > 0:
                    fba_fee_unit = round(offer_fba_fee * fx_rate, 4)
                    fba_fee_source = "offer_fee_snapshot"
                elif fba_hist_unit > 0:
                    fba_fee_unit = round(fba_hist_unit, 4)
                    fba_fee_source = "historical_line_fee"
                elif expected_fba_fee > 0:
                    fba_fee_unit = round(expected_fba_fee * fx_rate, 4)
                    fba_fee_source = "expected_api_fee"
                else:
                    fba_fee_unit = 0.0
                    fba_fee_source = "missing"
            else:
                fba_fee_unit = 0.0
                fba_fee_source = "not_applicable"

            offer_ref_rate = _f(row.get("offer_referral_fee_rate"))
            ref_hist_unit = _f(hist.get("ref_per_unit_hist"))
            try:
                expected_referral_fee = float(row.get("expected_referral_fee") or 0)
            except Exception:
                expected_referral_fee = 0.0
            try:
                expected_referral_rate = float(row.get("expected_referral_rate") or 0)
            except Exception:
                expected_referral_rate = 0.0
            if offer_ref_rate > 0 and offer_price > 0:
                referral_fee_unit = round(offer_price * offer_ref_rate * fx_rate, 4)
                referral_fee_source = "offer_fee_snapshot"
            elif ref_hist_unit > 0:
                referral_fee_unit = round(ref_hist_unit, 4)
                referral_fee_source = "historical_line_fee"
            elif expected_referral_rate > 0 and offer_price > 0:
                referral_fee_unit = round(offer_price * expected_referral_rate * fx_rate, 4)
                referral_fee_source = "expected_api_fee"
            elif expected_referral_fee > 0:
                referral_fee_unit = round(expected_referral_fee * fx_rate, 4)
                referral_fee_source = "expected_api_fee"
            else:
                referral_fee_unit = 0.0
                referral_fee_source = "missing"

            ads_per_unit = _f(hist.get("ads_per_unit_hist"))
            estimated_ads = round(ads_per_unit * scenario_qty, 2)

            if fc == "FBM":
                hist_pack_qty, hist_pack_source = _suggest_pack_qty(courier_map)
                suggested_pack_qty = max(1, hist_pack_qty)
                pack_source = hist_pack_source

                tkl_pack_qty = 0
                if tkl_country and _i(tkl_country.get("pack_qty")) > 0:
                    tkl_pack_qty = _i(tkl_country.get("pack_qty"))
                    pack_source = "tkl_country_pack"
                elif tkl_sku and _i(tkl_sku.get("pack_qty")) > 0:
                    tkl_pack_qty = _i(tkl_sku.get("pack_qty"))
                    pack_source = "tkl_sku_pack"
                if tkl_pack_qty > 0:
                    suggested_pack_qty = max(1, tkl_pack_qty)

                packages_count = max(1, math.ceil(scenario_qty / max(1, suggested_pack_qty)))

                plan_base = 0.0
                plan_source = "missing"
                if tkl_country and _f(tkl_country.get("cost")) > 0:
                    plan_base = _f(tkl_country.get("cost"))
                    plan_source = str(tkl_country.get("source") or "tkl_country_matrix")
                elif tkl_sku and _f(tkl_sku.get("cost")) > 0:
                    plan_base = _f(tkl_sku.get("cost"))
                    plan_source = str(tkl_sku.get("source") or "tkl_sku_fallback")

                hist_bucket_qty = min(scenario_qty, max(1, hist_pack_qty))
                observed_payload = _choose_bucket_payload(courier_map, hist_bucket_qty)
                if not observed_payload:
                    observed_payload = _choose_bucket_payload(courier_map, 1)
                observed_base = _f(observed_payload.get("median"))
                observed_p75 = _f(observed_payload.get("p75"))
                if observed_p75 <= 0 and observed_base > 0:
                    observed_p75 = observed_base
                observed_samples = _i(observed_payload.get("samples"))
                if observed_samples <= 0:
                    observed_samples = max(0, single_sample_count)
                observed_source = "historical_single_sku_median" if observed_base > 0 else "missing"

                low_sample = observed_samples < _WHATIF_LOGISTICS_MIN_SAMPLE
                stable_sample = (
                    observed_samples >= _WHATIF_LOGISTICS_BLEND_SAMPLE
                    and observed_base > 0
                    and observed_p75 > 0
                    and observed_p75 <= observed_base * _WHATIF_LOGISTICS_STABLE_P75_RATIO_MAX
                )
                safe_max = max(
                    plan_base,
                    observed_p75 if observed_p75 > 0 else observed_base,
                )

                if plan_base > 0 and (low_sample or observed_base <= 0):
                    decision_base = plan_base
                    decision_rule = "tkl_low_sample"
                elif plan_base > 0 and stable_sample:
                    blend = (
                        _WHATIF_LOGISTICS_BLEND_TKL_WEIGHT * plan_base
                        + _WHATIF_LOGISTICS_BLEND_OBS_WEIGHT * observed_base
                    )
                    decision_base = max(blend, safe_max)
                    decision_rule = "blend_safe_max"
                elif plan_base > 0 and safe_max > 0:
                    decision_base = safe_max
                    decision_rule = "safe_max"
                elif observed_base > 0:
                    decision_base = observed_p75 if observed_p75 > 0 else observed_base
                    decision_rule = "observed_only"
                else:
                    decision_base = 0.0
                    decision_rule = "missing"

                plan_logistics = round(plan_base * packages_count, 2)
                observed_logistics = round(observed_base * packages_count, 2)
                decision_logistics = round(decision_base * packages_count, 2)
                estimated_logistics = decision_logistics
                logistics_source = f"decision:{decision_rule}|plan:{plan_source}|obs:{observed_source}"

                logistics_gap_pct: float | None = None
                if plan_base > 0 and observed_base > 0:
                    logistics_gap_pct = round(((observed_base - plan_base) / plan_base) * 100, 2)

                execution_drift = (
                    plan_base > 0
                    and observed_samples >= _WHATIF_LOGISTICS_DRIFT_SAMPLE
                    and observed_base >= plan_base * _WHATIF_LOGISTICS_DRIFT_MEDIAN_RATIO
                    and observed_p75 >= plan_base * _WHATIF_LOGISTICS_DRIFT_P75_RATIO
                )

                base_shipping = _choose_bucket_value(shipping_map, min(scenario_qty, max(1, hist_pack_qty)), "median")
                if base_shipping <= 0:
                    base_shipping = _choose_bucket_value(shipping_map, 1, "median")
                estimated_shipping = round(base_shipping * packages_count, 2) if include_shipping_charge else 0.0
                shipping_source = (
                    "historical_finance_shippingcharge"
                    if include_shipping_charge and estimated_shipping > 0
                    else ("disabled" if not include_shipping_charge else "missing")
                )
                if not include_shipping_charge:
                    shipping_mode = "disabled"
                elif estimated_shipping <= 0:
                    shipping_mode = "free_or_missing"
                elif packages_count <= 1:
                    shipping_mode = "per_order"
                elif packages_count < scenario_qty:
                    shipping_mode = "per_package"
                else:
                    shipping_mode = "per_item"
            else:
                suggested_pack_qty = scenario_qty
                packages_count = 1
                pack_source = "not_applicable"
                plan_logistics = 0.0
                observed_logistics = 0.0
                decision_logistics = 0.0
                logistics_gap_pct = None
                decision_rule = "not_applicable"
                plan_source = "not_applicable"
                observed_source = "not_applicable"
                observed_samples = 0
                execution_drift = False
                estimated_logistics = 0.0
                logistics_source = "not_applicable"
                estimated_shipping = 0.0
                shipping_source = "not_applicable"
                shipping_mode = "not_applicable"

            mkt_ratio = ratio_by_marketplace.get(mkt, {})
            estimated_returns_net = round(
                _f(mkt_ratio.get("returns_net_ratio")) * max(0.0, offer_price_pln * scenario_qty + estimated_shipping),
                2,
            )
            if fc == "FBA":
                estimated_storage_fee = round(_f(mkt_ratio.get("storage_per_afn_unit")) * scenario_qty, 2)
                estimated_aged_fee = round(_f(mkt_ratio.get("aged_per_afn_unit")) * scenario_qty, 2)
                estimated_removal_fee = round(_f(mkt_ratio.get("removal_per_afn_unit")) * scenario_qty, 2)
                estimated_liquidation_fee = round(_f(mkt_ratio.get("liquidation_per_afn_unit")) * scenario_qty, 2)
            else:
                estimated_storage_fee = 0.0
                estimated_aged_fee = 0.0
                estimated_removal_fee = 0.0
                estimated_liquidation_fee = 0.0

            revenue = round(offer_price_pln * scenario_qty + estimated_shipping, 2)
            cogs_total = round(cogs_per_unit * scenario_qty, 2)
            fees_total = round((fba_fee_unit + referral_fee_unit) * scenario_qty, 2)
            cm1 = round(revenue - cogs_total - fees_total - estimated_logistics, 2)
            cm2 = round(
                cm1
                - estimated_ads
                - estimated_returns_net
                - estimated_storage_fee
                - estimated_aged_fee
                - estimated_removal_fee
                - estimated_liquidation_fee,
                2,
            )
            overhead_allocated = round(_f(mkt_ratio.get("overhead_ratio")) * revenue, 2)
            np_profit = round(cm2 - overhead_allocated, 2)
            cm1_pct = round(cm1 / revenue * 100, 2) if revenue else 0.0
            cm2_pct = round(cm2 / revenue * 100, 2) if revenue else 0.0
            np_pct = round(np_profit / revenue * 100, 2) if revenue else 0.0

            confidence = 35.0
            if cogs_source != "missing":
                confidence += 20
            if fba_fee_source not in {"missing"}:
                confidence += 10
            if referral_fee_source != "missing":
                confidence += 10
            if logistics_source not in {"missing", "not_applicable"}:
                confidence += 10
            if "tkl" in plan_source:
                confidence += 10
            if shipping_source not in {"missing", "disabled", "not_applicable"}:
                confidence += 5
            orders_hist = _i(hist.get("orders_hist"))
            if orders_hist >= 20:
                confidence += 5
            if orders_hist >= 100:
                confidence += 5
            confidence = round(min(99.0, confidence), 1)

            flags: list[str] = []
            if cogs_source == "missing":
                flags.append("missing_cogs")
            if referral_fee_source == "missing":
                flags.append("missing_referral_fee")
            if fc == "FBA" and fba_fee_source == "missing":
                flags.append("missing_fba_fee")
            if fc == "FBM" and logistics_source == "missing":
                flags.append("missing_logistics_history")
            if fc == "FBM" and include_shipping_charge and shipping_source == "missing":
                flags.append("missing_shipping_charge_history")
            if single_sample_count < 3 and fc == "FBM":
                flags.append("low_single_order_samples")
            if execution_drift:
                flags.append("execution_drift")

            items.append(
                {
                    "entity_type": "offer",
                    "group_key": None,
                    "sku": sku,
                    "sample_sku": sku,
                    "asin": row.get("asin"),
                    "parent_asin": row.get("parent_asin"),
                    "marketplace_id": mkt,
                    "marketplace_code": marketplace_code or _mkt_code(mkt),
                    "title": row.get("title"),
                    "brand": row.get("brand"),
                    "category": row.get("category"),
                    "internal_sku": internal_sku or row.get("internal_sku"),
                    "fulfillment_channel": fc,
                    "offer_status": row.get("offer_status"),
                    "offer_currency": offer_currency,
                    "offer_price": round(offer_price, 4),
                    "offer_price_pln": offer_price_pln,
                    "scenario_qty": scenario_qty,
                    "offer_count": 1,
                    "sku_count": 1,
                    "child_count": 1,
                    "suggested_pack_qty": max(1, suggested_pack_qty),
                    "packages_count": max(1, packages_count),
                    "plan_logistics_pln": plan_logistics,
                    "observed_logistics_pln": observed_logistics,
                    "decision_logistics_pln": decision_logistics,
                    "logistics_gap_pct": logistics_gap_pct,
                    "logistics_decision_rule": decision_rule,
                    "logistics_plan_source": plan_source,
                    "logistics_observed_source": observed_source,
                    "logistics_observed_samples": observed_samples,
                    "execution_drift": execution_drift,
                    "estimated_shipping_charge_pln": estimated_shipping,
                    "estimated_logistics_pln": estimated_logistics,
                    "estimated_ads_pln": estimated_ads,
                    "estimated_returns_net_pln": estimated_returns_net,
                    "estimated_fba_storage_fee_pln": estimated_storage_fee,
                    "estimated_fba_aged_fee_pln": estimated_aged_fee,
                    "estimated_fba_removal_fee_pln": estimated_removal_fee,
                    "estimated_fba_liquidation_fee_pln": estimated_liquidation_fee,
                    "overhead_allocated_pln": overhead_allocated,
                    "overhead_allocation_method": str(mkt_ratio.get("overhead_method") or "none"),
                    "overhead_confidence_pct": round(_f(mkt_ratio.get("overhead_confidence_pct")), 1),
                    "cogs_per_unit_pln": round(cogs_per_unit, 4),
                    "fba_fee_per_unit_pln": round(fba_fee_unit, 4),
                    "referral_fee_per_unit_pln": round(referral_fee_unit, 4),
                    "revenue_pln": revenue,
                    "cogs_pln": cogs_total,
                    "amazon_fees_pln": fees_total,
                    "cm1_profit": cm1,
                    "cm1_percent": cm1_pct,
                    "cm2_profit": cm2,
                    "cm2_percent": cm2_pct,
                    "np_profit": np_profit,
                    "np_percent": np_pct,
                    "history_orders": orders_hist,
                    "history_units": _i(hist.get("units_hist")),
                    "single_order_samples": single_sample_count,
                    "confidence_score": confidence,
                    "cogs_source": cogs_source,
                    "fba_fee_source": fba_fee_source,
                    "referral_fee_source": referral_fee_source,
                    "logistics_source": logistics_source,
                    "shipping_charge_source": shipping_source,
                    "shipping_charge_mode": shipping_mode,
                    "pack_suggestion_source": pack_source,
                    "flags": flags,
                }
            )

        if grouped_mode and items:
            grouped_items: dict[tuple[str, str], dict[str, Any]] = {}
            for item in items:
                asin_key = str(item.get("asin") or "").strip() or f"SKU:{item.get('sku')}"
                parent_key = str(item.get("parent_asin") or "").strip() or asin_key
                if group_mode in {"asin_marketplace", "asin"}:
                    key_value = asin_key
                    entity_label = "asin"
                else:
                    key_value = parent_key
                    entity_label = "parent"
                mp_key = str(item.get("marketplace_id") or "") if group_mode.endswith("_marketplace") else "__ALL__"
                gkey = (key_value, mp_key)
                if gkey not in grouped_items:
                    grouped_items[gkey] = {
                        "entity_type": entity_label,
                        "group_key": key_value,
                        "sku": key_value,
                        "sample_sku": item.get("sample_sku") or item.get("sku"),
                        "asin": asin_key if entity_label == "asin" else item.get("asin"),
                        "parent_asin": parent_key if entity_label == "parent" else item.get("parent_asin"),
                        "marketplace_id": item.get("marketplace_id") if group_mode.endswith("_marketplace") else "__ALL__",
                        "marketplace_code": item.get("marketplace_code") if group_mode.endswith("_marketplace") else "ALL",
                        "title": item.get("title"),
                        "brand": item.get("brand"),
                        "category": item.get("category"),
                        "internal_sku": item.get("internal_sku"),
                        "fulfillment_channel": item.get("fulfillment_channel") or "",
                        "offer_status": item.get("offer_status"),
                        "offer_currency": item.get("offer_currency") or "",
                        "offer_price": 0.0,
                        "offer_price_pln": 0.0,
                        "scenario_qty": scenario_qty,
                        "offer_count": 0,
                        "sku_count": 0,
                        "child_count": 0,
                        "suggested_pack_qty": 0.0,
                        "packages_count": 0,
                        "plan_logistics_pln": 0.0,
                        "observed_logistics_pln": 0.0,
                        "decision_logistics_pln": 0.0,
                        "logistics_gap_pct": None,
                        "logistics_decision_rule": "mixed",
                        "logistics_plan_source": "mixed",
                        "logistics_observed_source": "mixed",
                        "logistics_observed_samples": 0,
                        "execution_drift": False,
                        "estimated_shipping_charge_pln": 0.0,
                        "estimated_logistics_pln": 0.0,
                        "estimated_ads_pln": 0.0,
                        "estimated_returns_net_pln": 0.0,
                        "estimated_fba_storage_fee_pln": 0.0,
                        "estimated_fba_aged_fee_pln": 0.0,
                        "estimated_fba_removal_fee_pln": 0.0,
                        "estimated_fba_liquidation_fee_pln": 0.0,
                        "overhead_allocated_pln": 0.0,
                        "overhead_allocation_method": "none",
                        "overhead_confidence_pct": 0.0,
                        "cogs_per_unit_pln": 0.0,
                        "fba_fee_per_unit_pln": 0.0,
                        "referral_fee_per_unit_pln": 0.0,
                        "revenue_pln": 0.0,
                        "cogs_pln": 0.0,
                        "amazon_fees_pln": 0.0,
                        "cm1_profit": 0.0,
                        "cm1_percent": 0.0,
                        "cm2_profit": 0.0,
                        "cm2_percent": 0.0,
                        "np_profit": 0.0,
                        "np_percent": 0.0,
                        "history_orders": 0,
                        "history_units": 0,
                        "single_order_samples": 0,
                        "confidence_score": 0.0,
                        "cogs_source": item.get("cogs_source") or "missing",
                        "fba_fee_source": item.get("fba_fee_source") or "missing",
                        "referral_fee_source": item.get("referral_fee_source") or "missing",
                        "logistics_source": item.get("logistics_source") or "missing",
                        "shipping_charge_source": item.get("shipping_charge_source") or "missing",
                        "shipping_charge_mode": item.get("shipping_charge_mode") or "missing",
                        "pack_suggestion_source": item.get("pack_suggestion_source") or "default",
                        "flags": [],
                        "_seen_skus": set(),
                        "_seen_asins": set(),
                        "_shipping_mode_set": set(),
                        "_fba_fee_total_pln": 0.0,
                        "_referral_fee_total_pln": 0.0,
                        "_overhead_conf_amount_sum": 0.0,
                        "_overhead_method_set": set(),
                    }
                acc = grouped_items[gkey]
                acc["offer_count"] += 1
                acc["sku_count"] = _i(acc["sku_count"]) + 1
                acc["suggested_pack_qty"] = _f(acc["suggested_pack_qty"]) + _f(item.get("suggested_pack_qty"))
                acc["packages_count"] = _i(acc["packages_count"]) + _i(item.get("packages_count"))
                acc["plan_logistics_pln"] = _f(acc["plan_logistics_pln"]) + _f(item.get("plan_logistics_pln"))
                acc["observed_logistics_pln"] = _f(acc["observed_logistics_pln"]) + _f(item.get("observed_logistics_pln"))
                acc["decision_logistics_pln"] = _f(acc["decision_logistics_pln"]) + _f(item.get("decision_logistics_pln"))
                acc["logistics_observed_samples"] = _i(acc["logistics_observed_samples"]) + _i(item.get("logistics_observed_samples"))
                acc["execution_drift"] = bool(acc["execution_drift"]) or bool(item.get("execution_drift"))
                acc["estimated_shipping_charge_pln"] = _f(acc["estimated_shipping_charge_pln"]) + _f(item.get("estimated_shipping_charge_pln"))
                acc["estimated_logistics_pln"] = _f(acc["estimated_logistics_pln"]) + _f(item.get("estimated_logistics_pln"))
                acc["estimated_ads_pln"] = _f(acc["estimated_ads_pln"]) + _f(item.get("estimated_ads_pln"))
                acc["estimated_returns_net_pln"] = _f(acc["estimated_returns_net_pln"]) + _f(item.get("estimated_returns_net_pln"))
                acc["estimated_fba_storage_fee_pln"] = _f(acc["estimated_fba_storage_fee_pln"]) + _f(item.get("estimated_fba_storage_fee_pln"))
                acc["estimated_fba_aged_fee_pln"] = _f(acc["estimated_fba_aged_fee_pln"]) + _f(item.get("estimated_fba_aged_fee_pln"))
                acc["estimated_fba_removal_fee_pln"] = _f(acc["estimated_fba_removal_fee_pln"]) + _f(item.get("estimated_fba_removal_fee_pln"))
                acc["estimated_fba_liquidation_fee_pln"] = _f(acc["estimated_fba_liquidation_fee_pln"]) + _f(item.get("estimated_fba_liquidation_fee_pln"))
                acc["overhead_allocated_pln"] = _f(acc["overhead_allocated_pln"]) + _f(item.get("overhead_allocated_pln"))
                acc["offer_price_pln"] = _f(acc["offer_price_pln"]) + _f(item.get("offer_price_pln"))
                acc["offer_price"] = _f(acc["offer_price"]) + _f(item.get("offer_price"))
                acc["revenue_pln"] = _f(acc["revenue_pln"]) + _f(item.get("revenue_pln"))
                acc["cogs_pln"] = _f(acc["cogs_pln"]) + _f(item.get("cogs_pln"))
                acc["amazon_fees_pln"] = _f(acc["amazon_fees_pln"]) + _f(item.get("amazon_fees_pln"))
                acc["cm1_profit"] = _f(acc["cm1_profit"]) + _f(item.get("cm1_profit"))
                acc["cm2_profit"] = _f(acc["cm2_profit"]) + _f(item.get("cm2_profit"))
                acc["history_orders"] = _i(acc["history_orders"]) + _i(item.get("history_orders"))
                acc["history_units"] = _i(acc["history_units"]) + _i(item.get("history_units"))
                acc["single_order_samples"] = _i(acc["single_order_samples"]) + _i(item.get("single_order_samples"))
                acc["confidence_score"] = _f(acc["confidence_score"]) + _f(item.get("confidence_score"))
                fba_total_item = _f(item.get("fba_fee_per_unit_pln")) * max(1, _i(item.get("scenario_qty"), scenario_qty))
                ref_total_item = _f(item.get("referral_fee_per_unit_pln")) * max(1, _i(item.get("scenario_qty"), scenario_qty))
                acc["_fba_fee_total_pln"] = _f(acc["_fba_fee_total_pln"]) + fba_total_item
                acc["_referral_fee_total_pln"] = _f(acc["_referral_fee_total_pln"]) + ref_total_item
                overhead_amount = _f(item.get("overhead_allocated_pln"))
                overhead_conf = _f(item.get("overhead_confidence_pct"), 0.0)
                acc["_overhead_conf_amount_sum"] = _f(acc["_overhead_conf_amount_sum"]) + (overhead_amount * overhead_conf)
                overhead_method = str(item.get("overhead_allocation_method") or "none")
                if overhead_method:
                    acc["_overhead_method_set"].add(overhead_method)
                shipping_mode = str(item.get("shipping_charge_mode") or "").strip()
                if shipping_mode:
                    acc["_shipping_mode_set"].add(shipping_mode)
                acc["flags"] = sorted(set(list(acc.get("flags") or []) + list(item.get("flags") or [])))
                sku_val = str(item.get("sample_sku") or item.get("sku") or "").strip()
                asin_val = str(item.get("asin") or "").strip()
                if sku_val:
                    acc["_seen_skus"].add(sku_val)
                if asin_val:
                    acc["_seen_asins"].add(asin_val)
                gap_val = item.get("logistics_gap_pct")
                if isinstance(gap_val, (float, int)):
                    current_gap = acc.get("logistics_gap_pct")
                    if current_gap is None:
                        acc["logistics_gap_pct"] = float(gap_val)
                    else:
                        acc["logistics_gap_pct"] = (float(current_gap) + float(gap_val)) / 2.0

            normalized_grouped: list[dict[str, Any]] = []
            for acc in grouped_items.values():
                offers = max(1, _i(acc.get("offer_count"), 1))
                revenue = _f(acc.get("revenue_pln"))
                cogs_total = _f(acc.get("cogs_pln"))
                fees_total = _f(acc.get("amazon_fees_pln"))
                ads_total = _f(acc.get("estimated_ads_pln"))
                logistics_total = _f(acc.get("estimated_logistics_pln"))
                returns_total = _f(acc.get("estimated_returns_net_pln"))
                storage_total = _f(acc.get("estimated_fba_storage_fee_pln"))
                aged_total = _f(acc.get("estimated_fba_aged_fee_pln"))
                removal_total = _f(acc.get("estimated_fba_removal_fee_pln"))
                liquidation_total = _f(acc.get("estimated_fba_liquidation_fee_pln"))
                overhead_total = _f(acc.get("overhead_allocated_pln"))
                cm1 = round(revenue - cogs_total - fees_total - logistics_total, 2)
                cm2 = round(
                    cm1
                    - ads_total
                    - returns_total
                    - storage_total
                    - aged_total
                    - removal_total
                    - liquidation_total,
                    2,
                )
                np_val = round(cm2 - overhead_total, 2)
                acc["cm1_profit"] = cm1
                acc["cm2_profit"] = cm2
                acc["np_profit"] = np_val
                acc["cm1_percent"] = round(cm1 / revenue * 100, 2) if revenue else 0.0
                acc["cm2_percent"] = round(cm2 / revenue * 100, 2) if revenue else 0.0
                acc["np_percent"] = round(np_val / revenue * 100, 2) if revenue else 0.0
                acc["offer_price"] = round(_f(acc["offer_price"]) / offers, 4)
                acc["offer_price_pln"] = round(_f(acc["offer_price_pln"]) / offers, 4)
                acc["suggested_pack_qty"] = max(1, int(round(_f(acc["suggested_pack_qty"]) / offers)))
                qty_base = max(1, scenario_qty * offers)
                acc["cogs_per_unit_pln"] = round(cogs_total / qty_base, 4)
                acc["fba_fee_per_unit_pln"] = round(_f(acc.get("_fba_fee_total_pln")) / qty_base, 4)
                acc["referral_fee_per_unit_pln"] = round(_f(acc.get("_referral_fee_total_pln")) / qty_base, 4)
                acc["confidence_score"] = round(_f(acc["confidence_score"]) / offers, 1)
                methods = {str(m).strip() for m in acc.pop("_overhead_method_set", set()) if str(m).strip()}
                if not methods:
                    acc["overhead_allocation_method"] = "none"
                elif len(methods) == 1:
                    acc["overhead_allocation_method"] = next(iter(methods))
                else:
                    acc["overhead_allocation_method"] = "mixed"
                shipping_modes = {str(m).strip() for m in acc.pop("_shipping_mode_set", set()) if str(m).strip()}
                if not shipping_modes:
                    acc["shipping_charge_mode"] = "missing"
                elif len(shipping_modes) == 1:
                    acc["shipping_charge_mode"] = next(iter(shipping_modes))
                else:
                    acc["shipping_charge_mode"] = "mixed"
                if overhead_total > 0:
                    acc["overhead_confidence_pct"] = round(_f(acc.pop("_overhead_conf_amount_sum")) / overhead_total, 1)
                else:
                    acc["overhead_confidence_pct"] = 0.0
                    acc.pop("_overhead_conf_amount_sum", None)
                acc.pop("_fba_fee_total_pln", None)
                acc.pop("_referral_fee_total_pln", None)
                acc["sku_count"] = len(acc.pop("_seen_skus", set()))
                acc["child_count"] = len(acc.pop("_seen_asins", set()))
                normalized_grouped.append(acc)

            items = normalized_grouped

        sort_map = {
            "cm1_profit": "cm1_profit",
            "cm2_profit": "cm2_profit",
            "cm2_percent": "cm2_percent",
            "np_profit": "np_profit",
            "np_percent": "np_percent",
            "revenue_pln": "revenue_pln",
            "confidence_score": "confidence_score",
            "offer_price_pln": "offer_price_pln",
            "sku": "sku",
        }
        sort_key = sort_map.get(str(sort_by or "").lower(), "cm2_profit")
        items.sort(
            key=lambda x: x.get(sort_key, 0) if sort_key != "sku" else str(x.get("sku", "")),
            reverse=(normalized_sort_dir == "desc"),
        )

        if grouped_mode:
            total_items = len(items)
            total_pages = math.ceil(total_items / page_size) if total_items else 0
            offset_items = max(0, (page - 1) * page_size)
            page_items = items[offset_items:offset_items + page_size]
        else:
            total_items = total
            total_pages = pages
            page_items = items

        total_revenue = sum(_f(x.get("revenue_pln")) for x in page_items)
        total_cogs = sum(_f(x.get("cogs_pln")) for x in page_items)
        total_fees = sum(_f(x.get("amazon_fees_pln")) for x in page_items)
        total_logistics = sum(_f(x.get("estimated_logistics_pln")) for x in page_items)
        total_shipping = sum(_f(x.get("estimated_shipping_charge_pln")) for x in page_items)
        total_ads = sum(_f(x.get("estimated_ads_pln")) for x in page_items)
        total_returns_net = sum(_f(x.get("estimated_returns_net_pln")) for x in page_items)
        total_fba_storage = sum(_f(x.get("estimated_fba_storage_fee_pln")) for x in page_items)
        total_fba_aged = sum(_f(x.get("estimated_fba_aged_fee_pln")) for x in page_items)
        total_fba_removal = sum(_f(x.get("estimated_fba_removal_fee_pln")) for x in page_items)
        total_fba_liquidation = sum(_f(x.get("estimated_fba_liquidation_fee_pln")) for x in page_items)
        total_overhead = sum(_f(x.get("overhead_allocated_pln")) for x in page_items)
        total_cm1 = sum(_f(x.get("cm1_profit")) for x in page_items)
        total_cm2 = sum(_f(x.get("cm2_profit")) for x in page_items)
        total_np = sum(_f(x.get("np_profit")) for x in page_items)

        result = {
            "total": total_items,
            "page": page,
            "page_size": page_size,
            "pages": total_pages,
            "scenario_qty": scenario_qty,
            "include_shipping_charge": bool(include_shipping_charge),
            "summary": {
                "summary_scope": "page",
                "total_revenue_pln": round(total_revenue, 2),
                "total_cogs_pln": round(total_cogs, 2),
                "total_fees_pln": round(total_fees, 2),
                "total_logistics_pln": round(total_logistics, 2),
                "total_shipping_charge_pln": round(total_shipping, 2),
                "total_ads_pln": round(total_ads, 2),
                "total_returns_net_pln": round(total_returns_net, 2),
                "total_fba_storage_fee_pln": round(total_fba_storage, 2),
                "total_fba_aged_fee_pln": round(total_fba_aged, 2),
                "total_fba_removal_fee_pln": round(total_fba_removal, 2),
                "total_fba_liquidation_fee_pln": round(total_fba_liquidation, 2),
                "total_overhead_allocated_pln": round(total_overhead, 2),
                "total_cm1_pln": round(total_cm1, 2),
                "total_cm2_pln": round(total_cm2, 2),
                "total_np_pln": round(total_np, 2),
                "total_cm1_pct": round((total_cm1 / total_revenue * 100), 2) if total_revenue else 0.0,
                "total_cm2_pct": round((total_cm2 / total_revenue * 100), 2) if total_revenue else 0.0,
                "total_np_pct": round((total_np / total_revenue * 100), 2) if total_revenue else 0.0,
                "total_offers": len(page_items),
                "avg_confidence": round(
                    sum(_f(x.get("confidence_score")) for x in page_items) / len(page_items), 1
                ) if page_items else 0.0,
            },
            "items": page_items,
        }
        _result_cache_set(cache_key, result, ttl=180)
        return result
    finally:
        conn.close()



# ---------------------------------------------------------------------------
# Product Task CRUD
# ---------------------------------------------------------------------------

def _detect_brand_for_sku(cur: pyodbc.Cursor, sku: str) -> str | None:
    """Best-effort brand lookup for SKU from product mapping and order history."""
    cur.execute(
        """
        SELECT TOP 1 p.brand
        FROM dbo.acc_product p WITH (NOLOCK)
        WHERE p.internal_sku = ?
          AND p.brand IS NOT NULL
          AND LTRIM(RTRIM(p.brand)) <> ''
        """,
        (sku,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0]).strip()

    cur.execute(
        """
        SELECT TOP 1 p.brand
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
        WHERE ol.sku = ?
          AND p.brand IS NOT NULL
          AND LTRIM(RTRIM(p.brand)) <> ''
        ORDER BY ol.id DESC
        """,
        (sku,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0]).strip()
    return None


def _auto_assign_owner(
    cur: pyodbc.Cursor,
    *,
    task_type: str,
    marketplace_id: str | None,
    sku: str,
) -> str | None:
    """Resolve owner using rules table (task_type + marketplace + brand)."""
    brand = _detect_brand_for_sku(cur, sku)
    cur.execute(
        """
        SELECT TOP 1 owner
        FROM dbo.acc_al_task_owner_rules WITH (NOLOCK)
        WHERE is_active = 1
          AND (task_type IS NULL OR task_type = ?)
          AND (marketplace_id IS NULL OR marketplace_id = ?)
          AND (brand IS NULL OR brand = ?)
        ORDER BY
          priority ASC,
          CASE WHEN brand IS NOT NULL THEN 0 ELSE 1 END,
          CASE WHEN marketplace_id IS NOT NULL THEN 0 ELSE 1 END,
          CASE WHEN task_type IS NOT NULL THEN 0 ELSE 1 END
        """,
        (task_type, marketplace_id, brand),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0]).strip()
    return None


def create_product_task(
    *,
    task_type: str,
    sku: str,
    marketplace_id: str | None = None,
    title: str | None = None,
    note: str | None = None,
    owner: str | None = None,
    source_page: str = "product_profit",
    payload_json: str | None = None,
    created_by: str | None = "system",
) -> dict[str, Any]:
    """Persist product task (pricing/content/watchlist)."""
    if task_type not in {"pricing", "content", "watchlist"}:
        raise ValueError("task_type must be one of: pricing, content, watchlist")
    if not sku:
        raise ValueError("sku is required")

    task_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        resolved_owner = (owner or "").strip() or _auto_assign_owner(
            cur,
            task_type=task_type,
            marketplace_id=marketplace_id,
            sku=sku,
        )
        cur.execute(
            """
            INSERT INTO dbo.acc_al_product_tasks
                (id, task_type, sku, marketplace_id, status, title, note, owner, source_page, payload_json, created_by)
            VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                task_type,
                sku,
                marketplace_id,
                title,
                note,
                resolved_owner,
                source_page,
                payload_json,
                created_by,
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1 id, task_type, sku, marketplace_id, status, title, note, owner, source_page, created_at
            FROM dbo.acc_al_product_tasks WITH (NOLOCK)
            WHERE id = ?
            """,
            (task_id,),
        )
        row = _fetchall_dict(cur)[0]
        return {
            "id": str(row["id"]),
            "task_type": row["task_type"],
            "sku": row["sku"],
            "marketplace_id": row["marketplace_id"],
            "status": row["status"],
            "title": row["title"],
            "note": row["note"],
            "owner": row.get("owner"),
            "source_page": row["source_page"],
            "created_at": str(row["created_at"]),
        }
    finally:
        conn.close()


def _normalize_task_status(status: str) -> str:
    normalized = (status or "").strip().lower()
    if normalized not in {"open", "investigating", "resolved"}:
        raise ValueError("status must be one of: open, investigating, resolved")
    return normalized


def list_product_tasks(
    *,
    status: str | None = None,
    task_type: str | None = None,
    owner: str | None = None,
    sku_search: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []

        if status:
            where.append("t.status = ?")
            params.append(_normalize_task_status(status))
        if task_type:
            where.append("t.task_type = ?")
            params.append(task_type)
        if owner:
            where.append("t.owner = ?")
            params.append(owner)
        if sku_search:
            where.append("t.sku LIKE ?")
            params.append(f"%{sku_search}%")

        where_sql = " AND ".join(where)

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_al_product_tasks t WITH (NOLOCK) WHERE {where_sql}", params)
        total = _i(cur.fetchone()[0])
        pages = math.ceil(total / page_size) if total else 0
        offset = (max(1, page) - 1) * page_size

        cur.execute(
            f"""
            SELECT
                t.id, t.task_type, t.sku, t.marketplace_id, t.status,
                t.title, t.note, t.owner, t.source_page, t.created_at
            FROM dbo.acc_al_product_tasks t WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY t.created_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            (*params, offset, page_size),
        )
        rows = _fetchall_dict(cur)
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "items": [
                {
                    "id": str(r["id"]),
                    "task_type": r["task_type"],
                    "sku": r["sku"],
                    "marketplace_id": r["marketplace_id"],
                    "status": r["status"],
                    "title": r["title"],
                    "note": r["note"],
                    "owner": r.get("owner"),
                    "source_page": r["source_page"],
                    "created_at": str(r["created_at"]),
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


def update_product_task(
    *,
    task_id: str,
    status: str | None = None,
    owner: str | None = None,
    title: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    updates: list[str] = []
    params: list[Any] = []

    if status is not None:
        updates.append("status = ?")
        params.append(_normalize_task_status(status))
    if owner is not None:
        updates.append("owner = ?")
        params.append(owner)
    if title is not None:
        updates.append("title = ?")
        params.append(title)
    if note is not None:
        updates.append("note = ?")
        params.append(note)

    if not updates:
        raise ValueError("no update fields provided")

    updates.append("updated_at = SYSUTCDATETIME()")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE dbo.acc_al_product_tasks
            SET {", ".join(updates)}
            WHERE id = ?
            """,
            (*params, task_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            raise ValueError("task not found")

        cur.execute(
            """
            SELECT TOP 1 id, task_type, sku, marketplace_id, status, title, note, owner, source_page, created_at
            FROM dbo.acc_al_product_tasks WITH (NOLOCK)
            WHERE id = ?
            """,
            (task_id,),
        )
        row = _fetchall_dict(cur)[0]
        return {
            "id": str(row["id"]),
            "task_type": row["task_type"],
            "sku": row["sku"],
            "marketplace_id": row["marketplace_id"],
            "status": row["status"],
            "title": row["title"],
            "note": row["note"],
            "owner": row.get("owner"),
            "source_page": row["source_page"],
            "created_at": str(row["created_at"]),
        }
    finally:
        conn.close()


def add_product_task_comment(
    *,
    task_id: str,
    comment: str,
    author: str | None = None,
) -> dict[str, Any]:
    text = (comment or "").strip()
    if not text:
        raise ValueError("comment is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo.acc_al_product_tasks WITH (NOLOCK) WHERE id = ?", (task_id,))
        if _i(cur.fetchone()[0]) == 0:
            raise ValueError("task not found")

        cur.execute(
            """
            INSERT INTO dbo.acc_al_product_task_comments (task_id, comment, author)
            VALUES (?, ?, ?)
            """,
            (task_id, text, author or "system"),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1 id, task_id, comment, author, created_at
            FROM dbo.acc_al_product_task_comments WITH (NOLOCK)
            WHERE task_id = ?
            ORDER BY id DESC
            """,
            (task_id,),
        )
        row = _fetchall_dict(cur)[0]
        return {
            "id": _i(row["id"]),
            "task_id": str(row["task_id"]),
            "comment": row["comment"],
            "author": row["author"],
            "created_at": str(row["created_at"]),
        }
    finally:
        conn.close()


def list_product_task_comments(*, task_id: str) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, task_id, comment, author, created_at
            FROM dbo.acc_al_product_task_comments WITH (NOLOCK)
            WHERE task_id = ?
            ORDER BY created_at DESC, id DESC
            """,
            (task_id,),
        )
        rows = _fetchall_dict(cur)
        return [
            {
                "id": _i(r["id"]),
                "task_id": str(r["task_id"]),
                "comment": r["comment"],
                "author": r["author"],
                "created_at": str(r["created_at"]),
            }
            for r in rows
        ]
    finally:
        conn.close()


def list_task_owner_rules() -> list[dict[str, Any]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, owner, priority, task_type, marketplace_id, brand, is_active, created_at
            FROM dbo.acc_al_task_owner_rules WITH (NOLOCK)
            ORDER BY priority ASC, id ASC
            """
        )
        rows = _fetchall_dict(cur)
        return [
            {
                "id": _i(r["id"]),
                "owner": r["owner"],
                "priority": _i(r["priority"], 100),
                "task_type": r["task_type"],
                "marketplace_id": r["marketplace_id"],
                "brand": r["brand"],
                "is_active": bool(r.get("is_active")),
                "created_at": str(r["created_at"]),
            }
            for r in rows
        ]
    finally:
        conn.close()


def create_task_owner_rule(
    *,
    owner: str,
    priority: int = 100,
    task_type: str | None = None,
    marketplace_id: str | None = None,
    brand: str | None = None,
    is_active: bool = True,
) -> dict[str, Any]:
    owner_value = (owner or "").strip()
    if not owner_value:
        raise ValueError("owner is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_al_task_owner_rules
                (is_active, priority, task_type, marketplace_id, brand, owner)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                1 if is_active else 0,
                int(priority),
                task_type,
                marketplace_id,
                brand,
                owner_value,
            ),
        )
        conn.commit()
        cur.execute(
            """
            SELECT TOP 1 id, owner, priority, task_type, marketplace_id, brand, is_active, created_at
            FROM dbo.acc_al_task_owner_rules WITH (NOLOCK)
            ORDER BY id DESC
            """
        )
        r = _fetchall_dict(cur)[0]
        return {
            "id": _i(r["id"]),
            "owner": r["owner"],
            "priority": _i(r["priority"], 100),
            "task_type": r["task_type"],
            "marketplace_id": r["marketplace_id"],
            "brand": r["brand"],
            "is_active": bool(r.get("is_active")),
            "created_at": str(r["created_at"]),
        }
    finally:
        conn.close()


def delete_task_owner_rule(*, rule_id: int) -> bool:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_al_task_owner_rules WHERE id = ?", (rule_id,))
        deleted = _i(cur.rowcount) > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Product Drilldown — order lines for a specific SKU
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Product Drilldown
# ---------------------------------------------------------------------------

def get_product_drilldown(
    *,
    sku: str,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return order-line level detail for a SKU with canonical CM1 waterfall."""
    conn = _connect()
    try:
        cur = conn.cursor()
        order_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        order_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

        wheres = [
            "ol.sku = ?",
            "o.status = 'Shipped'",
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            # Exclude removal/return orders and Non-Amazon transfers
            "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            "o.amazon_order_id NOT LIKE 'S02-%'",
            # Exclude Amazon Renewed (used) products from analytics
            RENEWED_SKU_FILTER,
        ]
        params: list[Any] = [sku, date_from.isoformat(), date_to.isoformat()]
        if marketplace_id:
            wheres.append("o.marketplace_id = ?")
            params.append(marketplace_id)

        where_sql = " AND ".join(wheres)
        fx_rate_sql = f"""
            {_fx_case('o.currency')}
        """
        line_share_sql = """
            CASE
                WHEN ISNULL(olt.order_line_total, 0) > 0
                    THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                WHEN ISNULL(olt.order_units_total, 0) > 0
                    THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                ELSE 0
            END
        """
        shipping_alloc_sql = f"ISNULL(spo.shipping_charge_pln, 0) * {line_share_sql}"
        revenue_sql = f"""
            (
                (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                * {fx_rate_sql}
                + {shipping_alloc_sql}
                + CASE
                    WHEN ISNULL(o.is_refund, 0) = 1
                        THEN ISNULL(o.refund_amount_pln, 0) * {line_share_sql}
                    ELSE 0
                  END
            )
        """
        cogs_sql = """
            CASE
                WHEN ISNULL(o.is_refund, 0) = 1 AND o.refund_type = 'full' THEN 0
                ELSE ISNULL(ol.cogs_pln, 0)
            END
        """
        direct_order_fee_sql = _cm1_direct_order_fee_alloc_sql("o", line_share_sql)
        fees_sql = f"(ISNULL(ol.fba_fee_pln, 0) + ISNULL(ol.referral_fee_pln, 0) + {direct_order_fee_sql})"
        logistics_sql = f"({order_logistics_value_sql} * {line_share_sql})"
        cm1_sql = f"({revenue_sql} - {cogs_sql} - {fees_sql} - {logistics_sql})"

        shipping_outer_apply_sql = f"""
            OUTER APPLY (
                SELECT SUM(
                    CASE WHEN ft.charge_type IN ('ShippingCharge','ShippingTax','ShippingDiscount')
                        THEN ISNULL(ft.amount_pln,
                            ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")})
                        ELSE 0
                    END
                ) AS shipping_charge_pln
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                WHERE ft.amazon_order_id = o.amazon_order_id
                  AND (ft.marketplace_id = o.marketplace_id OR ft.marketplace_id IS NULL)
            ) spo
        """

        # Count
        cur.execute(f"""
            SELECT COUNT(*)
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE {where_sql}
        """, params)
        total = _i(cur.fetchone()[0])

        cur.execute(f"""
            SELECT
                SUM({revenue_sql}) AS revenue_pln,
                SUM({cogs_sql}) AS cogs_pln,
                SUM({fees_sql}) AS fees_pln,
                SUM({logistics_sql}) AS logistics_pln,
                SUM({cm1_sql}) AS cm1_pln,
                SUM(ISNULL(ol.quantity_ordered, 0)) AS units,
                SUM({shipping_alloc_sql}) AS shipping_pln
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            {order_logistics_join_sql}
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= o.purchase_date
                ORDER BY er.rate_date DESC
            ) fx
            OUTER APPLY (
                SELECT
                    ISNULL(SUM(
                        ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                        - ISNULL(ol2.promotion_discount, 0)
                    ), 0) AS order_line_total,
                    ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            {shipping_outer_apply_sql}
            WHERE {where_sql}
        """, params)
        summary_row = _fetchall_dict(cur)[0]

        # Paginated detail
        offset = (max(1, page) - 1) * page_size
        sql = f"""
            SELECT
                o.amazon_order_id,
                o.marketplace_id,
                o.purchase_date,
                o.fulfillment_channel,
                o.currency,
                ol.sku,
                ol.asin,
                ol.title,
                ISNULL(ol.quantity_ordered, 0)                  AS qty,
                ISNULL(ol.item_price, 0)                        AS item_price,
                ISNULL(ol.item_tax, 0)                          AS item_tax,
                ISNULL(ol.promotion_discount, 0)                AS promo_discount,
                ISNULL(ol.cogs_pln, 0)                          AS cogs_pln,
                ISNULL(ol.fba_fee_pln, 0)                       AS fba_fee_pln,
                ISNULL(ol.referral_fee_pln, 0)                  AS referral_fee_pln,
                ISNULL(ol.purchase_price_pln, 0)                AS purchase_price_pln,
                ol.price_source,
                {fx_rate_sql}                                   AS fx_rate,
                ISNULL(o.is_refund, 0)                          AS is_refund,
                o.refund_type,
                ISNULL(o.refund_amount_pln, 0)                  AS refund_amount_pln,
                {shipping_alloc_sql}                            AS shipping_charge_pln,
                {revenue_sql}                                   AS revenue_pln,
                {cogs_sql}                                      AS effective_cogs_pln,
                {fees_sql}                                      AS amazon_fees_pln,
                {logistics_sql}                                 AS logistics_pln,
                {cm1_sql}                                       AS cm1_profit
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            {order_logistics_join_sql}
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= o.purchase_date
                ORDER BY er.rate_date DESC
            ) fx
            OUTER APPLY (
                SELECT
                    ISNULL(SUM(
                        ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                        - ISNULL(ol2.promotion_discount, 0)
                    ), 0) AS order_line_total,
                    ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            {shipping_outer_apply_sql}
            WHERE {where_sql}
            ORDER BY o.purchase_date DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        cur.execute(sql, (*params, offset, page_size))
        rows = _fetchall_dict(cur)

        items = []
        for r in rows:
            fx = _f(r["fx_rate"], 1.0)
            price = _f(r["item_price"])
            tax = _f(r["item_tax"])
            promo = _f(r["promo_discount"])
            is_refund = bool(r.get("is_refund"))
            refund_amount = _f(r.get("refund_amount_pln"))
            shipping = round(_f(r.get("shipping_charge_pln")), 2)
            rev_pln = round(_f(r.get("revenue_pln")), 2)
            # For refunded orders, deduct refund from revenue
            cogs = round(_f(r.get("effective_cogs_pln")), 2)
            fba = round(_f(r["fba_fee_pln"]), 2)
            # Full refund → product returns to inventory → COGS recovered
            ref = round(_f(r["referral_fee_pln"]), 2)
            fees = round(_f(r.get("amazon_fees_pln")), 2)
            logistics = round(_f(r.get("logistics_pln")), 2)
            cm1 = round(_f(r.get("cm1_profit")), 2)
            needs_logistics = str(r.get("fulfillment_channel") or "").upper() in {"MFN", "FBM"}

            has_cogs = cogs > 0
            has_fees = fees > 0
            has_logistics = logistics > 0 or not needs_logistics
            if has_cogs and has_fees and has_logistics:
                cost_source = "Actual"
            elif has_cogs or has_fees or logistics > 0:
                cost_source = "Partial"
            else:
                cost_source = "Missing"

            items.append({
                "amazon_order_id": r["amazon_order_id"],
                "marketplace_id": r["marketplace_id"],
                "marketplace_code": _mkt_code(r["marketplace_id"]),
                "purchase_date": str(r["purchase_date"]),
                "fulfillment_channel": r["fulfillment_channel"],
                "sku": r["sku"],
                "asin": r["asin"],
                "title": r["title"],
                "qty": _i(r["qty"]),
                "currency": r["currency"],
                "fx_rate": fx,
                # Waterfall
                "item_price": price,
                "item_tax": tax,
                "promo_discount": promo,
                "revenue_pln": rev_pln,
                "shipping_charge_pln": shipping,
                "cogs_pln": cogs,
                "fba_fee_pln": fba,
                "referral_fee_pln": ref,
                "amazon_fees_pln": fees,
                "logistics_pln": logistics,
                "cm1_profit": cm1,
                "cm1_percent": round(cm1 / rev_pln * 100, 2) if rev_pln else 0.0,
                # Meta
                "purchase_price_pln": _f(r["purchase_price_pln"]),
                "price_source": r.get("price_source"),
                "cost_source": cost_source,
                # Refund info
                "is_refund": is_refund,
                "refund_type": r.get("refund_type"),
                "refund_amount_pln": round(refund_amount, 2) if is_refund else None,
            })

        # Summary for this SKU
        total_rev = round(_f(summary_row.get("revenue_pln")), 2)
        total_cm1 = round(_f(summary_row.get("cm1_pln")), 2)
        total_shipping = round(_f(summary_row.get("shipping_pln")), 2)

        pages = math.ceil(total / page_size) if total else 0
        return {
            "sku": sku,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "summary": {
                "revenue_pln": total_rev,
                "shipping_charge_pln": total_shipping,
                "cogs_pln": round(_f(summary_row.get("cogs_pln")), 2),
                "fees_pln": round(_f(summary_row.get("fees_pln")), 2),
                "logistics_pln": round(_f(summary_row.get("logistics_pln")), 2),
                "cm1_pln": total_cm1,
                "cm1_pct": round(total_cm1 / total_rev * 100, 2) if total_rev else 0.0,
                "units": _i(summary_row.get("units")),
            },
            "items": items,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Loss Orders — where CM1 < 0
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Loss Orders
# ---------------------------------------------------------------------------

def get_loss_orders(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku_search: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return order lines where CM1 < 0 (loss-making)."""
    cache_key = f"loss_orders:{date_from}:{date_to}:{marketplace_id}:{sku_search}:{page}:{page_size}"
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return cached

    conn = _connect()
    try:
        cur = conn.cursor()
        order_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        order_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

        wheres = [
            "o.status = 'Shipped'",
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            # Exclude removal/return orders (S02-*) and Non-Amazon transfers
            "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            "o.amazon_order_id NOT LIKE 'S02-%'",
            # Exclude cancelled lines (qty=0, no revenue)
            "ol.quantity_ordered > 0",
            "ISNULL(ol.item_price, 0) > 0",
        ]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        if marketplace_id:
            wheres.append("o.marketplace_id = ?")
            params.append(marketplace_id)
        if sku_search:
            wheres.append("ol.sku LIKE ?")
            params.append(f"%{sku_search}%")

        where_sql = " AND ".join(wheres)
        fx_rate_sql = f"""
            {_fx_case('o.currency')}
        """
        line_share_sql = """
            CASE
                WHEN ISNULL(olt.order_line_total, 0) > 0
                    THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                WHEN ISNULL(olt.order_units_total, 0) > 0
                    THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                ELSE 0
            END
        """
        shipping_alloc_sql = f"ISNULL(spo.shipping_charge_pln, 0) * {line_share_sql}"
        revenue_sql = f"""
            (
                (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                * {fx_rate_sql}
                + {shipping_alloc_sql}
                + CASE
                    WHEN ISNULL(o.is_refund, 0) = 1
                        THEN ISNULL(o.refund_amount_pln, 0) * {line_share_sql}
                    ELSE 0
                  END
            )
        """
        cogs_sql = """
            CASE
                WHEN ISNULL(o.is_refund, 0) = 1 AND o.refund_type = 'full' THEN 0
                ELSE ISNULL(ol.cogs_pln, 0)
            END
        """
        direct_order_fee_sql = _cm1_direct_order_fee_alloc_sql("o", line_share_sql)
        fees_sql = f"(ISNULL(ol.fba_fee_pln, 0) + ISNULL(ol.referral_fee_pln, 0) + {direct_order_fee_sql})"
        logistics_sql = f"({order_logistics_value_sql} * {line_share_sql})"
        cm1_sql = f"({revenue_sql} - {cogs_sql} - {fees_sql} - {logistics_sql})"

        shipping_outer_apply_sql = f"""
            OUTER APPLY (
                SELECT SUM(
                    CASE WHEN ft.charge_type IN ('ShippingCharge','ShippingTax','ShippingDiscount')
                        THEN ISNULL(ft.amount_pln,
                            ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")})
                        ELSE 0
                    END
                ) AS shipping_charge_pln
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                WHERE ft.amazon_order_id = o.amazon_order_id
                  AND (ft.marketplace_id = o.marketplace_id OR ft.marketplace_id IS NULL)
            ) spo
        """

        # CTE with CM1 computed, then filter for negative
        cte_sql = f"""
            ;WITH line_cm AS (
                SELECT
                    o.amazon_order_id,
                    o.marketplace_id,
                    o.purchase_date,
                    o.fulfillment_channel,
                    o.currency,
                    ol.sku,
                    ol.asin,
                    ol.title,
                    COALESCE(p.title, ol.title) AS product_title,
                    ISNULL(ol.quantity_ordered, 0)           AS qty,
                    ISNULL(ol.item_price, 0)                 AS item_price,
                    ISNULL(ol.item_tax, 0)                   AS item_tax,
                    ISNULL(ol.promotion_discount, 0)         AS promo_discount,
                    {cogs_sql}                               AS cogs_pln,
                    ISNULL(ol.fba_fee_pln, 0)                AS fba_fee_pln,
                    ISNULL(ol.referral_fee_pln, 0)           AS referral_fee_pln,
                    {fx_rate_sql}                            AS fx_rate,
                    {shipping_alloc_sql}                     AS shipping_charge_pln,
                    {revenue_sql}                            AS revenue_pln,
                    {fees_sql}                               AS amazon_fees_pln,
                    {logistics_sql}                          AS logistics_pln,
                    {cm1_sql}                                AS cm1_profit
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                {order_logistics_join_sql}
                LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
                OUTER APPLY (
                    SELECT TOP 1 rate_to_pln
                    FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                    WHERE er.currency = o.currency
                      AND er.rate_date <= o.purchase_date
                    ORDER BY er.rate_date DESC
                ) fx
                OUTER APPLY (
                    SELECT
                        ISNULL(SUM(
                            ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                            - ISNULL(ol2.promotion_discount, 0)
                        ), 0) AS order_line_total,
                        ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                    FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                    WHERE ol2.order_id = o.id
                ) olt
                {shipping_outer_apply_sql}
                WHERE {where_sql}
            )
        """

        cur.execute(f"""
            {cte_sql}
            SELECT
                COUNT(*) AS total,
                ISNULL(SUM(cm1_profit), 0) AS total_loss_pln
            FROM line_cm
            WHERE cm1_profit < 0
        """, params)
        totals_row = _fetchall_dict(cur)[0]
        total = _i(totals_row.get("total"))

        # Paginated
        offset = (max(1, page) - 1) * page_size
        page_sql = f"""
            {cte_sql}
            SELECT * FROM line_cm
            WHERE cm1_profit < 0
            ORDER BY cm1_profit ASC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        cur.execute(page_sql, (*params, offset, page_size))
        rows = _fetchall_dict(cur)

        items = []
        for r in rows:
            rev = _f(r["revenue_pln"])
            cm1 = _f(r["cm1_profit"])
            cogs = _f(r["cogs_pln"])
            fees = round(_f(r.get("amazon_fees_pln")), 2)
            logistics = round(_f(r.get("logistics_pln")), 2)
            shipping = round(_f(r.get("shipping_charge_pln")), 2)

            # Vine detection: promotion ≥ 90% of net price → free product for reviewer
            item_price_raw = _f(r.get("item_price", 0))
            item_tax_raw = _f(r.get("item_tax", 0))
            promo_raw = _f(r.get("promo_discount", 0))
            net_price = item_price_raw - item_tax_raw
            is_vine = promo_raw > 0 and net_price > 0 and promo_raw >= net_price * 0.90

            # Primary loss driver
            if is_vine:
                driver = "Vine"
                driver_amount = round(promo_raw * _f(r.get("fx_rate", 1.0)), 2)
            elif cogs == 0 and fees == 0 and logistics == 0:
                driver = "Missing cost data"
                driver_amount = rev  # all revenue looks like profit but isn't
            elif logistics > rev and logistics >= max(cogs, fees):
                driver = "Logistics too high"
                driver_amount = logistics
            elif fees > rev and fees >= max(cogs, logistics):
                driver = "Fees anomaly"
                driver_amount = fees
            elif cogs > rev and cogs >= max(fees, logistics):
                # COGS is fixed cost — seller controls sell price, not COGS
                driver = "Sell price too low"
                driver_amount = rev
            elif rev < (cogs + fees + logistics) * 0.5:
                driver = "Sell price too low"
                driver_amount = rev
            else:
                driver = "Combined costs"
                driver_amount = round(cogs + fees + logistics - rev, 2)

            items.append({
                "amazon_order_id": r.get("amazon_order_id"),
                "marketplace_id": r.get("marketplace_id"),
                "marketplace_code": _mkt_code(r.get("marketplace_id")),
                "purchase_date": str(r.get("purchase_date")),
                "fulfillment_channel": r.get("fulfillment_channel"),
                "sku": r.get("sku"),
                "asin": r.get("asin"),
                "title": r.get("title"),
                "product_title": r.get("product_title") or r.get("title"),
                "qty": _i(r.get("qty")),
                "currency": r.get("currency"),
                "revenue_pln": rev,
                "shipping_charge_pln": shipping,
                "cogs_pln": cogs,
                "amazon_fees_pln": fees,
                "logistics_pln": logistics,
                "cm1_profit": cm1,
                "cm1_percent": round(cm1 / rev * 100, 2) if rev else 0.0,
                "primary_loss_driver": driver,
                "driver_amount": driver_amount,
            })

        pages = math.ceil(total / page_size) if total else 0
        total_loss = _f(totals_row.get("total_loss_pln"))

        result = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "total_loss_pln": round(total_loss, 2),
            "items": items,
        }
        _result_cache_set(cache_key, result, ttl=180)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Granular P&L — fee breakdown (50+ lines, like external tool)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Fee Breakdown
# ---------------------------------------------------------------------------

def get_fee_breakdown(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
) -> dict[str, Any]:
    """Return granular P&L: every charge_type from acc_finance_transaction
    grouped by fee_taxonomy category/bucket/layer, with amounts in PLN.

    This provides the same detail level as external P&L tools (~50+ line items).
    """
    from app.core.fee_taxonomy import classify_fee, FeeCategory

    cache_key = f"fee_breakdown:{date_from}:{date_to}:{marketplace_id}:{sku}"
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return cached

    conn = _connect()
    try:
        cur = conn.cursor()

        wheres = [
            "ft.posted_date >= CAST(? AS DATE)",
            "ft.posted_date < DATEADD(day, 1, CAST(? AS DATE))",
        ]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]

        if marketplace_id:
            wheres.append("ft.marketplace_id = ?")
            params.append(marketplace_id)
        if sku:
            wheres.append("ft.sku = ?")
            params.append(sku)

        where_sql = " AND ".join(wheres)

        sql = f"""
            SELECT
                ft.charge_type,
                ft.transaction_type,
                SUM(ISNULL(ft.amount_pln,
                    ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")}
                )) AS amount_pln,
                COUNT(*) AS txn_count
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE {where_sql}
            GROUP BY ft.charge_type, ft.transaction_type
            ORDER BY ft.charge_type
        """
        cur.execute(sql, params)
        rows = _fetchall_dict(cur)

        # Also get CM1 components from order_line level for the same period
        ol_wheres = [
            "o.status = 'Shipped'",
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            "o.amazon_order_id NOT LIKE 'S02-%'",
            "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            "ol.quantity_ordered > 0",
        ]
        ol_params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        if marketplace_id:
            ol_wheres.append("o.marketplace_id = ?")
            ol_params.append(marketplace_id)
        if sku:
            ol_wheres.append("ol.sku = ?")
            ol_params.append(sku)

        ol_where_sql = " AND ".join(ol_wheres)

        cur.execute(f"""
            SELECT
                SUM(
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)
                     - ISNULL(ol.promotion_discount, 0))
                    * {_fx_case('o.currency')}
                ) AS item_revenue_pln,
                SUM(ISNULL(ol.cogs_pln, 0))         AS cogs_pln,
                SUM(ISNULL(ol.fba_fee_pln, 0))      AS fba_fee_pln,
                SUM(ISNULL(ol.referral_fee_pln, 0))  AS referral_fee_pln,
                SUM(
                    ISNULL(o.shipping_surcharge_pln, 0) * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (
                                ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)
                                - ISNULL(ol.promotion_discount, 0)
                            ) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                ) AS shipping_surcharge_pln,
                SUM(
                    ISNULL(o.promo_order_fee_pln, 0) * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (
                                ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)
                                - ISNULL(ol.promotion_discount, 0)
                            ) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                ) AS promo_order_fee_pln,
                SUM(
                    ISNULL(o.refund_commission_pln, 0) * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (
                                ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)
                                - ISNULL(ol.promotion_discount, 0)
                            ) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                ) AS refund_commission_pln,
                SUM(ISNULL(ol.quantity_ordered, 0))   AS units
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= o.purchase_date
                ORDER BY er.rate_date DESC
            ) fx
            OUTER APPLY (
                SELECT
                    ISNULL(SUM(
                        ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                        - ISNULL(ol2.promotion_discount, 0)
                    ), 0) AS order_line_total,
                    ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            WHERE {ol_where_sql}
        """, ol_params)
        ol_row = _fetchall_dict(cur)[0]

        # Build granular lines
        # 1) Order-level revenue & costs (from order_line)
        item_revenue = round(_f(ol_row.get("item_revenue_pln")), 2)
        cogs_total = round(_f(ol_row.get("cogs_pln")), 2)
        fba_fee = round(_f(ol_row.get("fba_fee_pln")), 2)
        referral_fee = round(_f(ol_row.get("referral_fee_pln")), 2)
        shipping_surcharge = round(_f(ol_row.get("shipping_surcharge_pln")), 2)
        promo_order_fee = round(_f(ol_row.get("promo_order_fee_pln")), 2)
        refund_commission = round(_f(ol_row.get("refund_commission_pln")), 2)
        units = _i(ol_row.get("units"))

        # Polish descriptions for fee categories
        _CAT_PL: dict[str, str] = {
            "REVENUE": "Przychody",
            "COGS": "Koszt towaru",
            "FBA_FEE": "Opłata FBA",
            "REFERRAL_FEE": "Prowizja Amazon",
            "FBA_STORAGE": "Magazynowanie FBA",
            "FBA_INBOUND": "Transport FBA (inbound)",
            "FBA_REMOVAL": "Usunięcie / utylizacja",
            "FBA_LIQUIDATION": "Likwidacja FBA",
            "WAREHOUSE_LOSS": "Straty magazynowe",
            "REFUND": "Zwroty i reklamacje",
            "SHIPPING_SURCHARGE": "Dopłata transportowa",
            "PROMO_FEE": "Opłaty promocyjne",
            "ADS_FEE": "Reklama (Ads)",
            "ADJUSTMENT": "Korekty Amazon",
            "SERVICE_FEE": "Opłaty serwisowe",
            "REGULATORY_FEE": "Opłaty regulacyjne (EPR)",
            "OTHER_FEE": "Inne opłaty",
        }

        def _polish_desc(cat: str, charge_type: str, orig: str) -> str:
            if orig and not orig.startswith("Classified by"):
                return orig
            return _CAT_PL.get(cat, "") + (f" — {charge_type}" if charge_type else "")

        # Collect lines by layer
        cm1_revenue: list[dict[str, Any]] = []
        cm1_costs: list[dict[str, Any]] = []
        cm2_items: list[dict[str, Any]] = []
        np_items: list[dict[str, Any]] = []

        cm1_revenue.append({
            "line_type": "revenue",
            "charge_type": "ItemRevenue",
            "category": "REVENUE",
            "description": "Przychód ze sprzedaży (cena × ilość − VAT − rabat)",
            "profit_layer": "cm1",
            "profit_bucket": "revenue",
            "amount_pln": item_revenue,
            "txn_count": units,
            "source": "orders",
        })
        cm1_costs.append({
            "line_type": "cost",
            "charge_type": "COGS",
            "category": "COGS",
            "description": "Koszt własny sprzedaży (cena zakupu)",
            "profit_layer": "cm1",
            "profit_bucket": "cogs",
            "amount_pln": -cogs_total,
            "txn_count": units,
            "source": "orders",
        })
        cm1_costs.append({
            "line_type": "cost",
            "charge_type": "FBAFee",
            "category": "FBA_FEE",
            "description": "Opłata FBA za realizację zamówienia",
            "profit_layer": "cm1",
            "profit_bucket": "fba_fee",
            "amount_pln": -fba_fee,
            "txn_count": units,
            "source": "orders",
        })
        cm1_costs.append({
            "line_type": "cost",
            "charge_type": "ReferralFee",
            "category": "REFERRAL_FEE",
            "description": "Prowizja Amazon od sprzedaży",
            "profit_layer": "cm1",
            "profit_bucket": "referral_fee",
            "amount_pln": -referral_fee,
            "txn_count": units,
            "source": "orders",
        })

        if shipping_surcharge > 0:
            cm1_costs.append({
                "line_type": "cost",
                "charge_type": "ShippingSurcharge",
                "category": "SHIPPING_SURCHARGE",
                "description": "Dopłata transportowa (heavy/bulky)",
                "profit_layer": "cm1",
                "profit_bucket": "shipping_surcharge",
                "amount_pln": -shipping_surcharge,
                "txn_count": units,
                "source": "orders",
            })

        if promo_order_fee > 0:
            cm1_costs.append({
                "line_type": "cost",
                "charge_type": "PromoOrderFee",
                "category": "PROMO_FEE",
                "description": "Opłata promocyjna na poziomie zamówienia",
                "profit_layer": "cm1",
                "profit_bucket": "promo_order",
                "amount_pln": -promo_order_fee,
                "txn_count": units,
                "source": "orders",
            })

        if refund_commission > 0:
            cm1_costs.append({
                "line_type": "cost",
                "charge_type": "RefundCommission",
                "category": "REFERRAL_FEE",
                "description": "Prowizja Amazon za obsługę zwrotu",
                "profit_layer": "cm1",
                "profit_bucket": "refund_commission",
                "amount_pln": -refund_commission,
                "txn_count": units,
                "source": "orders",
            })

        cm1_direct = round(
            item_revenue - cogs_total - fba_fee - referral_fee
            - shipping_surcharge - promo_order_fee - refund_commission,
            2,
        )

        # 2) Finance transaction lines (CM2 / NP)
        shipping_total = 0.0
        cm2_costs = 0.0
        np_costs = 0.0

        merged: dict[str, dict[str, Any]] = {}
        for r in rows:
            charge_type = r.get("charge_type") or ""
            transaction_type = r.get("transaction_type") or ""
            amount = round(_f(r.get("amount_pln")), 2)
            count = _i(r.get("txn_count"))

            fee = classify_fee(charge_type, transaction_type)

            if fee.category == FeeCategory.CASH_FLOW:
                continue
            if fee.profit_layer is None and fee.category in (
                FeeCategory.FBA_FEE, FeeCategory.REFERRAL_FEE,
            ):
                continue

            is_shipping_revenue = (
                fee.category == FeeCategory.REVENUE
                and charge_type in ("ShippingCharge", "ShippingTax", "ShippingDiscount")
            )
            if fee.category == FeeCategory.REVENUE and not is_shipping_revenue:
                continue

            cat_val = fee.category.value
            desc = _polish_desc(cat_val, charge_type, fee.description)

            if charge_type in merged:
                merged[charge_type]["amount_pln"] = round(
                    merged[charge_type]["amount_pln"] + amount, 2
                )
                merged[charge_type]["txn_count"] += count
            else:
                if is_shipping_revenue:
                    merged[charge_type] = {
                        "line_type": "revenue",
                        "charge_type": charge_type,
                        "category": cat_val,
                        "description": desc,
                        "profit_layer": "cm1",
                        "profit_bucket": "shipping",
                        "amount_pln": amount,
                        "txn_count": count,
                        "source": "finance",
                    }
                else:
                    layer = fee.profit_layer or "np"
                    merged[charge_type] = {
                        "line_type": "cost",
                        "charge_type": charge_type,
                        "category": cat_val,
                        "description": desc,
                        "profit_layer": layer,
                        "profit_bucket": fee.profit_bucket or "unclassified",
                        "amount_pln": amount,
                        "txn_count": count,
                        "source": "finance",
                    }

        # Distribute merged lines into layer buckets
        for line in merged.values():
            if line["profit_bucket"] == "shipping":
                cm1_revenue.append(line)
                shipping_total += line["amount_pln"]
            elif line["profit_layer"] == "cm1":
                cm1_costs.append(line)
            elif line["profit_layer"] == "cm2":
                cm2_items.append(line)
                cm2_costs += line["amount_pln"]
            else:
                np_items.append(line)
                np_costs += line["amount_pln"]

        # Sort each section: revenue first, costs by |amount| descending
        def _sort_key(l: dict) -> tuple:
            t = 0 if l["line_type"] == "revenue" else 1
            return (t, -abs(l["amount_pln"]))

        cm1_revenue.sort(key=_sort_key)
        cm1_costs.sort(key=_sort_key)
        cm2_items.sort(key=_sort_key)
        np_items.sort(key=_sort_key)

        # Compute layer subtotals
        cm1_val = round(cm1_direct + shipping_total, 2)
        cm2_val = round(cm1_val + cm2_costs, 2)
        np_val = round(cm2_val + np_costs, 2)
        total_revenue = round(item_revenue + shipping_total, 2)

        # Assemble final waterfall: sections → items → subtotal
        final_lines: list[dict[str, Any]] = []

        # --- Revenue section ---
        final_lines.append({
            "line_type": "section_header", "charge_type": "", "category": "REVENUE",
            "description": "Przychody", "profit_layer": "cm1",
            "profit_bucket": None, "amount_pln": 0, "txn_count": 0, "source": "",
        })
        final_lines.extend(cm1_revenue)

        # --- CM1 costs section ---
        final_lines.append({
            "line_type": "section_header", "charge_type": "", "category": "COGS",
            "description": "Koszty bezpośrednie (CM1)", "profit_layer": "cm1",
            "profit_bucket": None, "amount_pln": 0, "txn_count": 0, "source": "",
        })
        final_lines.extend(cm1_costs)
        final_lines.append({
            "line_type": "subtotal", "charge_type": "CM1", "category": "SUBTOTAL",
            "description": "Marża pokrycia 1", "profit_layer": "cm1",
            "profit_bucket": None, "amount_pln": cm1_val, "txn_count": 0, "source": "",
        })

        # --- CM2 costs section ---
        if cm2_items:
            final_lines.append({
                "line_type": "section_header", "charge_type": "", "category": "FBA_STORAGE",
                "description": "Koszty pośrednie (CM2)", "profit_layer": "cm2",
                "profit_bucket": None, "amount_pln": 0, "txn_count": 0, "source": "",
            })
            final_lines.extend(cm2_items)
        final_lines.append({
            "line_type": "subtotal", "charge_type": "CM2", "category": "SUBTOTAL",
            "description": "Marża pokrycia 2", "profit_layer": "cm2",
            "profit_bucket": None, "amount_pln": cm2_val, "txn_count": 0, "source": "",
        })

        # --- NP costs section ---
        if np_items:
            final_lines.append({
                "line_type": "section_header", "charge_type": "", "category": "SERVICE_FEE",
                "description": "Pozostałe koszty (NP)", "profit_layer": "np",
                "profit_bucket": None, "amount_pln": 0, "txn_count": 0, "source": "",
            })
            final_lines.extend(np_items)
        final_lines.append({
            "line_type": "subtotal", "charge_type": "NP", "category": "SUBTOTAL",
            "description": "Zysk netto", "profit_layer": "np",
            "profit_bucket": None, "amount_pln": np_val, "txn_count": 0, "source": "",
        })

        # Add pct_of_revenue to every line
        for line in final_lines:
            if line["line_type"] == "section_header":
                line["pct_of_revenue"] = 0
            elif total_revenue:
                line["pct_of_revenue"] = round(
                    line["amount_pln"] / total_revenue * 100, 1
                )
            else:
                line["pct_of_revenue"] = 0

        result = {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "marketplace_id": marketplace_id,
            "sku": sku,
            "total_lines": len(final_lines),
            "summary": {
                "revenue_pln": total_revenue,
                "cogs_pln": cogs_total,
                "cm1_pln": cm1_val,
                "cm2_pln": cm2_val,
                "np_pln": np_val,
                "units": units,
            },
            "lines": final_lines,
        }
        _result_cache_set(cache_key, result, ttl=300)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Data Quality & Coverage
# ---------------------------------------------------------------------------

_PRICE_SOURCE_PRIORITY_CASE = """
CASE {alias}.source
    WHEN 'manual'         THEN 1
    WHEN 'import_xlsx'    THEN 2
    WHEN 'xlsx_oficjalne' THEN 3
    WHEN 'holding'        THEN 4
    WHEN 'erp_holding'    THEN 5
    WHEN 'import_csv'     THEN 6
    WHEN 'cogs_xlsx'      THEN 7
    WHEN 'acc_product'    THEN 8
    WHEN 'ai_match'       THEN 9
    ELSE 99
END
"""



# ---------------------------------------------------------------------------
# Data Quality & Coverage
# ---------------------------------------------------------------------------

def get_data_quality(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Return data quality metrics showing coverage of cost components."""
    # In-memory cache (data quality is expensive, 10 min TTL)
    dq_cache_key = f"dq:{date_from}:{date_to}:{marketplace_id}"
    cached = _result_cache_get(dq_cache_key)
    if cached is not None:
        return cached

    conn = _connect()
    try:
        cur = conn.cursor()

        wheres = [
            "o.status = 'Shipped'",
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            # Exclude removal/return orders and Non-Amazon transfers
            "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            "o.amazon_order_id NOT LIKE 'S02-%'",
            # Exclude cancelled lines
            "ol.quantity_ordered > 0",
            # Exclude Amazon Renewed (used) products from analytics
            RENEWED_SKU_FILTER,
        ]
        # Order-only filters (for queries without ol join)
        order_wheres = [w for w in wheres if not w.startswith("ol.")]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        order_params: list[Any] = list(params)
        if marketplace_id:
            wheres.append("o.marketplace_id = ?")
            order_wheres.append("o.marketplace_id = ?")
            params.append(marketplace_id)
            order_params.append(marketplace_id)

        where_sql = " AND ".join(wheres)
        order_where_sql = " AND ".join(order_wheres)

        # Overall coverage
        cur.execute(f"""
            SELECT
                COUNT(*)                                                       AS total_lines,
                SUM(CASE WHEN ISNULL(o.fulfillment_channel, '') = 'AFN'
                    THEN 1 ELSE 0 END)                                         AS fba_eligible_lines,
                SUM(CASE WHEN ol.cogs_pln IS NOT NULL AND ol.cogs_pln > 0
                    THEN 1 ELSE 0 END)                                         AS lines_with_cogs,
                SUM(CASE WHEN ol.purchase_price_pln IS NOT NULL AND ol.purchase_price_pln > 0
                    THEN 1 ELSE 0 END)                                         AS lines_with_purchase_price,
                SUM(CASE WHEN ISNULL(o.fulfillment_channel, '') = 'AFN'
                          AND ol.fba_fee_pln IS NOT NULL AND ol.fba_fee_pln > 0
                    THEN 1 ELSE 0 END)                                         AS lines_with_fba_fee,
                SUM(CASE WHEN ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln > 0
                    THEN 1 ELSE 0 END)                                         AS lines_with_referral_fee,
                SUM(CASE WHEN ol.product_id IS NOT NULL
                    THEN 1 ELSE 0 END)                                         AS lines_with_product,
                COUNT(DISTINCT ol.sku)                                          AS distinct_skus,
                COUNT(DISTINCT o.id)                                            AS distinct_orders,
                SUM(ISNULL(ol.item_price, 0))                                  AS total_revenue_orig,
                SUM(ISNULL(ol.cogs_pln, 0))                                    AS total_cogs_pln
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE {where_sql}
        """, params)
        ovr = _fetchall_dict(cur)[0]

        total = _i(ovr["total_lines"], 1)
        fba_eligible_total = _i(ovr["fba_eligible_lines"], 1)

        # Finance coverage — how many orders have matching finance transactions
        cur.execute(f"""
            SELECT
                COUNT(DISTINCT o.id) AS orders_total,
                COUNT(DISTINCT ft.amazon_order_id) AS orders_with_finance
            FROM dbo.acc_order o WITH (NOLOCK)
            LEFT JOIN dbo.acc_finance_transaction ft WITH (NOLOCK)
                ON ft.amazon_order_id = o.amazon_order_id
            WHERE {order_where_sql}
        """, order_params)
        fin = _fetchall_dict(cur)[0]

        orders_total = _i(fin["orders_total"], 1)
        orders_with_finance = _i(fin["orders_with_finance"])

        # Exchange rate coverage
        cur.execute(f"""
            SELECT
                COUNT(DISTINCT o.currency) AS currencies_used,
                COUNT(DISTINCT CASE
                    WHEN o.currency = 'PLN' OR fx.rate_to_pln IS NOT NULL THEN o.currency
                END) AS currencies_with_rate
            FROM dbo.acc_order o WITH (NOLOCK)
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= o.purchase_date
                ORDER BY er.rate_date DESC
            ) fx
            WHERE {order_where_sql}
        """, order_params)
        fxr = _fetchall_dict(cur)[0]

        # Missing COGS — top SKUs by revenue without COGS
        cur.execute(f"""
            SELECT TOP 50
                ol.sku,
                MIN(ol.asin) AS asin,
                MIN(p.internal_sku) AS internal_sku,
                MIN(p.ean) AS ean,
                SUM(ISNULL(ol.quantity_ordered, 0)) AS units,
                SUM(ISNULL(ol.item_price, 0)) AS revenue_orig,
                COUNT(*) AS line_count
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
            WHERE {where_sql}
              AND (ol.cogs_pln IS NULL OR ol.cogs_pln = 0)
              AND ISNULL(ol.quantity_ordered, 0) > 0
              AND ol.sku IS NOT NULL
            GROUP BY ol.sku
            ORDER BY SUM(ISNULL(ol.item_price, 0)) DESC
        """, params)
        missing_cogs = _fetchall_dict(cur)

        # Enrich with current purchase price
        for mc in missing_cogs:
            current_price, current_price_source, hard_suggestion, ai_candidate = _build_missing_cogs_suggestions(
                cur,
                sku=str(mc.get("sku") or ""),
                asin=mc.get("asin"),
                ean=mc.get("ean"),
                internal_sku=mc.get("internal_sku"),
            )
            mc["current_price_pln"] = current_price["price_pln"] if current_price else None
            mc["current_price_source"] = current_price_source
            mc["hard_suggestion"] = hard_suggestion
            mc["ai_candidate"] = ai_candidate

        # Coverage by marketplace
        cur.execute(f"""
            SELECT
                o.marketplace_id,
                COUNT(*) AS total_lines,
                SUM(CASE WHEN ISNULL(o.fulfillment_channel, '') = 'AFN' THEN 1 ELSE 0 END) AS fba_eligible_lines,
                SUM(CASE WHEN ol.cogs_pln > 0 THEN 1 ELSE 0 END) AS with_cogs,
                SUM(CASE WHEN ISNULL(o.fulfillment_channel, '') = 'AFN' AND ol.fba_fee_pln > 0 THEN 1 ELSE 0 END) AS with_fba_fee,
                SUM(CASE
                        WHEN ISNULL(o.fulfillment_channel, '') = 'AFN'
                            THEN CASE WHEN ol.fba_fee_pln > 0 AND ol.referral_fee_pln > 0 THEN 1 ELSE 0 END
                        ELSE CASE WHEN ol.referral_fee_pln > 0 THEN 1 ELSE 0 END
                    END) AS with_fees
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE {where_sql}
            GROUP BY o.marketplace_id
        """, params)
        mkt_rows = _fetchall_dict(cur)
        by_marketplace = []
        for mr in mkt_rows:
            mt = _i(mr["total_lines"], 1)
            fba_mt = _i(mr["fba_eligible_lines"], 1)
            by_marketplace.append({
                "marketplace_id": mr["marketplace_id"],
                "marketplace_code": _mkt_code(mr["marketplace_id"]),
                "total_lines": mt,
                "cogs_coverage_pct": round(_i(mr["with_cogs"]) / mt * 100, 1),
                "fees_coverage_pct": round(_i(mr["with_fees"]) / mt * 100, 1),
                "fba_fee_coverage_pct": round(_i(mr["with_fba_fee"]) / fba_mt * 100, 1),
            })

        result = {
            "period": {"date_from": str(date_from), "date_to": str(date_to)},
            "overview": {
                "total_order_lines": total,
                "distinct_orders": _i(ovr["distinct_orders"]),
                "distinct_skus": _i(ovr["distinct_skus"]),
                "cogs_coverage_pct": round(_i(ovr["lines_with_cogs"]) / total * 100, 1),
                "purchase_price_coverage_pct": round(_i(ovr["lines_with_purchase_price"]) / total * 100, 1),
                "fba_fee_coverage_pct": round(_i(ovr["lines_with_fba_fee"]) / fba_eligible_total * 100, 1),
                "referral_fee_coverage_pct": round(_i(ovr["lines_with_referral_fee"]) / total * 100, 1),
                "product_mapping_pct": round(_i(ovr["lines_with_product"]) / total * 100, 1),
                "finance_match_pct": round(orders_with_finance / orders_total * 100, 1),
                "fx_rate_coverage": f"{_i(fxr['currencies_with_rate'])}/{_i(fxr['currencies_used'])}",
            },
            "missing_cogs_top": [
                {
                    "sku": r["sku"],
                    "asin": r.get("asin"),
                    "internal_sku": r.get("internal_sku"),
                    "ean": r.get("ean"),
                    "units": _i(r["units"]),
                    "revenue_orig": _f(r["revenue_orig"]),
                    "line_count": _i(r["line_count"]),
                    "current_price_pln": r.get("current_price_pln"),
                    "current_price_source": r.get("current_price_source"),
                    "hard_suggestion": r.get("hard_suggestion"),
                    "ai_candidate": r.get("ai_candidate"),
                }
                for r in missing_cogs
            ],
            "by_marketplace": by_marketplace,
        }
        _result_cache_set(dq_cache_key, result, ttl=600)  # 10 min for data quality
        return result
    finally:
        conn.close()


_FBA_FEE_SQL_TYPES = (
    "'FBAPerUnitFulfillmentFee','FBAPerOrderFulfillmentFee',"
    "'FBAWeightBasedFee','FBAPickAndPackFee'"
)
_REFERRAL_FEE_SQL_TYPES = (
    "'Commission','VariableClosingFee','FixedClosingFee'"
)



# ---------------------------------------------------------------------------
# Fee Gap Diagnostics
# ---------------------------------------------------------------------------

def _fetch_fee_gap_orders(
    cur,
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
    market_sql = ""
    if marketplace_id:
        market_sql = " AND o.marketplace_id = ?"
        params.append(marketplace_id)

    sql = f"""
        WITH base_lines AS (
            SELECT
                o.id AS order_id,
                o.amazon_order_id,
                o.marketplace_id,
                CAST(o.purchase_date AS DATE) AS purchase_date,
                ISNULL(o.fulfillment_channel, '') AS fulfillment_channel,
                ol.sku,
                ol.asin,
                CASE
                    WHEN ISNULL(o.fulfillment_channel, '') = 'AFN' AND (ol.fba_fee_pln IS NULL OR ol.fba_fee_pln <= 0)
                        THEN 'fba'
                    WHEN (ol.referral_fee_pln IS NULL OR ol.referral_fee_pln <= 0)
                        THEN 'referral'
                    ELSE NULL
                END AS gap_type
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            INNER JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE o.status = 'Shipped'
              AND o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
              AND o.amazon_order_id NOT LIKE 'S02-%'
              AND ISNULL(ol.quantity_ordered, 0) > 0
              AND ol.sku NOT LIKE 'amzn.gr.%%'
              {market_sql}
        ),
        aggregated AS (
            SELECT
                bl.amazon_order_id,
                bl.marketplace_id,
                MIN(bl.purchase_date) AS purchase_date,
                MIN(bl.fulfillment_channel) AS fulfillment_channel,
                MIN(NULLIF(bl.sku, '')) AS sample_sku,
                MIN(NULLIF(bl.asin, '')) AS sample_asin,
                bl.gap_type,
                COUNT(*) AS missing_lines
            FROM base_lines bl
            WHERE bl.gap_type IS NOT NULL
            GROUP BY bl.amazon_order_id, bl.marketplace_id, bl.gap_type
        )
        SELECT
            a.marketplace_id,
            a.amazon_order_id,
            a.purchase_date,
            a.fulfillment_channel,
            a.sample_sku,
            a.sample_asin,
            a.gap_type,
            a.missing_lines,
            ISNULL(fin.finance_rows, 0) AS finance_rows,
            ISNULL(fin.fba_charge_rows, 0) AS fba_charge_rows,
            ISNULL(fin.referral_charge_rows, 0) AS referral_charge_rows,
            ISNULL(fin.fee_rows_without_sku, 0) AS fee_rows_without_sku,
            ISNULL(fin.charge_types_csv, '') AS charge_types_csv
        FROM aggregated a
        OUTER APPLY (
            SELECT
                COUNT(*) AS finance_rows,
                SUM(CASE WHEN ft.charge_type IN ({_FBA_FEE_SQL_TYPES}) THEN 1 ELSE 0 END) AS fba_charge_rows,
                SUM(CASE WHEN ft.charge_type IN ({_REFERRAL_FEE_SQL_TYPES}) THEN 1 ELSE 0 END) AS referral_charge_rows,
                SUM(CASE WHEN ft.charge_type IN ({_FBA_FEE_SQL_TYPES}, {_REFERRAL_FEE_SQL_TYPES})
                          AND (ft.sku IS NULL OR LTRIM(RTRIM(ft.sku)) = '')
                    THEN 1 ELSE 0 END) AS fee_rows_without_sku,
                STRING_AGG(CAST(ft.charge_type AS NVARCHAR(MAX)), ',') AS charge_types_csv
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.amazon_order_id = a.amazon_order_id
        ) fin
    """
    cur.execute(sql, params)
    rows = _fetchall_dict(cur)
    for row in rows:
        gap_type = str(row["gap_type"] or "")
        finance_rows = _i(row["finance_rows"])
        fba_charge_rows = _i(row["fba_charge_rows"])
        referral_charge_rows = _i(row["referral_charge_rows"])
        fee_rows_without_sku = _i(row["fee_rows_without_sku"])
        if finance_rows <= 0:
            gap_reason = "no_finance_rows"
        elif gap_type == "fba" and fba_charge_rows <= 0:
            gap_reason = "finance_exists_no_fba_charge_type"
        elif gap_type == "referral" and referral_charge_rows <= 0:
            gap_reason = "finance_exists_no_referral_charge_type"
        elif fee_rows_without_sku > 0:
            gap_reason = "finance_rows_without_sku"
        else:
            gap_reason = "sku_mismatch_or_unallocated"
        row["gap_reason"] = gap_reason
        row["ownership_bucket"] = (
            "amazon_missing"
            if gap_reason == "no_finance_rows"
            else "internal_fixable"
        )
        csv_value = _norm_text(row.get("charge_types_csv"))
        row["charge_types"] = [part for part in csv_value.split(",") if part]
    return rows


def get_fee_gap_diagnostics(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    cache_key = f"fee-gap:{date_from}:{date_to}:{marketplace_id}"
    cached = _result_cache_get(cache_key)
    if cached is not None:
        return cached

    quality = get_data_quality(
        date_from=date_from,
        date_to=date_to,
        marketplace_id=marketplace_id,
    )

    conn = _connect()
    try:
        cur = conn.cursor()
        rows = _fetch_fee_gap_orders(
            cur,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
    finally:
        conn.close()

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row["marketplace_id"]),
            _mkt_code(row["marketplace_id"]),
            str(row["gap_type"]),
            str(row["gap_reason"]),
        )
        item = grouped.setdefault(
            key,
            {
                "marketplace_id": key[0],
                "marketplace_code": key[1],
                "gap_type": key[2],
                "gap_reason": key[3],
                "missing_lines": 0,
                "missing_orders": 0,
            },
        )
        item["missing_lines"] += _i(row["missing_lines"])
        item["missing_orders"] += 1

    de_no_fba = [
        {
            "marketplace_id": row["marketplace_id"],
            "marketplace_code": _mkt_code(row["marketplace_id"]),
            "gap_type": row["gap_type"],
            "gap_reason": row["gap_reason"],
            "amazon_order_id": row["amazon_order_id"],
            "purchase_date": str(row["purchase_date"]),
            "fulfillment_channel": row["fulfillment_channel"],
            "sample_sku": row["sample_sku"],
            "sample_asin": row["sample_asin"],
            "missing_lines": _i(row["missing_lines"]),
            "finance_rows": _i(row["finance_rows"]),
            "order_fee_rows": _i(row["fba_charge_rows"] if row["gap_type"] == "fba" else row["referral_charge_rows"]),
            "fee_rows_without_sku": _i(row["fee_rows_without_sku"]),
            "charge_types": row["charge_types"],
            "ownership_bucket": row["ownership_bucket"],
        }
        for row in rows
        if row["marketplace_id"] == "A1PA6795UKMFR9" and row["gap_reason"] == "finance_exists_no_fba_charge_type"
    ]

    likely_amazon = [
        {
            "marketplace_id": row["marketplace_id"],
            "marketplace_code": _mkt_code(row["marketplace_id"]),
            "gap_type": row["gap_type"],
            "gap_reason": row["gap_reason"],
            "amazon_order_id": row["amazon_order_id"],
            "purchase_date": str(row["purchase_date"]),
            "fulfillment_channel": row["fulfillment_channel"],
            "sample_sku": row["sample_sku"],
            "sample_asin": row["sample_asin"],
            "missing_lines": _i(row["missing_lines"]),
            "finance_rows": _i(row["finance_rows"]),
            "order_fee_rows": _i(row["fba_charge_rows"] if row["gap_type"] == "fba" else row["referral_charge_rows"]),
            "fee_rows_without_sku": _i(row["fee_rows_without_sku"]),
            "charge_types": row["charge_types"],
            "ownership_bucket": row["ownership_bucket"],
        }
        for row in rows
        if row["ownership_bucket"] == "amazon_missing"
    ][:100]

    likely_internal = [
        {
            "marketplace_id": row["marketplace_id"],
            "marketplace_code": _mkt_code(row["marketplace_id"]),
            "gap_type": row["gap_type"],
            "gap_reason": row["gap_reason"],
            "amazon_order_id": row["amazon_order_id"],
            "purchase_date": str(row["purchase_date"]),
            "fulfillment_channel": row["fulfillment_channel"],
            "sample_sku": row["sample_sku"],
            "sample_asin": row["sample_asin"],
            "missing_lines": _i(row["missing_lines"]),
            "finance_rows": _i(row["finance_rows"]),
            "order_fee_rows": _i(row["fba_charge_rows"] if row["gap_type"] == "fba" else row["referral_charge_rows"]),
            "fee_rows_without_sku": _i(row["fee_rows_without_sku"]),
            "charge_types": row["charge_types"],
            "ownership_bucket": row["ownership_bucket"],
        }
        for row in rows
        if row["ownership_bucket"] == "internal_fixable"
    ][:100]

    result = {
        "period": quality["period"],
        "overview": quality["overview"],
        "reasons": sorted(
            grouped.values(),
            key=lambda item: (item["marketplace_code"], item["gap_type"], -item["missing_orders"], item["gap_reason"]),
        ),
        "de_finance_exists_no_fba_charge": de_no_fba[:100],
        "likely_amazon_missing": likely_amazon,
        "likely_internal_fixable": likely_internal,
    }
    _result_cache_set(cache_key, result, ttl=300)
    return result


def seed_fee_gap_watch(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    ensure_profit_data_quality_schema()
    conn = _connect()
    inserted = 0
    updated = 0
    try:
        cur = conn.cursor()
        rows = _fetch_fee_gap_orders(
            cur,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
        for row in rows:
            cur.execute(
                """
                SELECT id, status
                FROM dbo.acc_fee_gap_watch
                WHERE gap_type = ?
                  AND marketplace_id = ?
                  AND amazon_order_id = ?
                """,
                [row["gap_type"], row["marketplace_id"], row["amazon_order_id"]],
            )
            existing = cur.fetchone()
            now = datetime.now(timezone.utc).isoformat(sep=" ", timespec="seconds")
            if existing:
                cur.execute(
                    """
                    UPDATE dbo.acc_fee_gap_watch
                    SET gap_reason = ?,
                        sample_sku = ?,
                        sample_asin = ?,
                        fulfillment_channel = ?,
                        status = 'open',
                        last_seen_at = SYSUTCDATETIME(),
                        resolved_at = NULL,
                        last_note = NULL
                    WHERE id = ?
                    """,
                    [
                        row["gap_reason"],
                        row["sample_sku"],
                        row["sample_asin"],
                        row["fulfillment_channel"],
                        existing[0],
                    ],
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO dbo.acc_fee_gap_watch
                        (id, gap_type, gap_reason, marketplace_id, amazon_order_id,
                         sample_sku, sample_asin, fulfillment_channel, status,
                         first_seen_at, last_seen_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    [
                        str(uuid.uuid4()),
                        row["gap_type"],
                        row["gap_reason"],
                        row["marketplace_id"],
                        row["amazon_order_id"],
                        row["sample_sku"],
                        row["sample_asin"],
                        row["fulfillment_channel"],
                    ],
                )
                inserted += 1
        cur.execute(
            "SELECT COUNT(*) FROM dbo.acc_fee_gap_watch WITH (NOLOCK) WHERE status = 'open'"
        )
        open_total = _i(cur.fetchone()[0])
        conn.commit()
        _result_cache_invalidate("fee-gap:")
        return {
            "period": {"date_from": str(date_from), "date_to": str(date_to)},
            "inserted": inserted,
            "updated": updated,
            "open_total": open_total,
        }
    finally:
        conn.close()


def recheck_fee_gap_watch(
    *,
    limit: int = 25,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    ensure_profit_data_quality_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        params: list[Any] = [limit]
        market_sql = ""
        if marketplace_id:
            market_sql = " AND marketplace_id = ?"
            params.append(marketplace_id)
        cur.execute(
            f"""
            SELECT TOP (?) id, gap_type, gap_reason, marketplace_id, amazon_order_id,
                sample_sku, sample_asin, fulfillment_channel,
                first_seen_at, last_seen_at, last_checked_at, resolved_at,
                status, last_amazon_event_count, last_note
            FROM dbo.acc_fee_gap_watch WITH (NOLOCK)
            WHERE status IN ('open', 'amazon_events_available')
              {market_sql}
            ORDER BY last_checked_at ASC, last_seen_at DESC
            """,
            params,
        )
        rows = _fetchall_dict(cur)
        run_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO dbo.acc_fee_gap_recheck_run(id, started_at, scope_json)
            VALUES (?, SYSUTCDATETIME(), ?)
            """,
            [run_id, f'{{"limit": {limit}, "marketplace_id": "{marketplace_id or ""}"}}'],
        )
        conn.commit()
    finally:
        conn.close()

    results: list[dict[str, Any]] = []
    checked = resolved = amazon_events_available = still_missing = api_errors = 0
    from app.connectors.amazon_sp_api.finances import FinancesClient

    conn = _connect()
    try:
        cur = conn.cursor()
        for row in rows:
            checked += 1
            order_id = str(row["amazon_order_id"])
            gap_type = str(row["gap_type"])
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                INNER JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                WHERE o.amazon_order_id = ?
                  AND o.status = 'Shipped'
                  AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
                  AND o.amazon_order_id NOT LIKE 'S02-%'
                  AND ISNULL(ol.quantity_ordered, 0) > 0
                  AND (
                        (? = 'fba' AND ISNULL(o.fulfillment_channel, '') = 'AFN' AND (ol.fba_fee_pln IS NULL OR ol.fba_fee_pln <= 0))
                     OR (? = 'referral' AND (ol.referral_fee_pln IS NULL OR ol.referral_fee_pln <= 0))
                  )
                """,
                [order_id, gap_type, gap_type],
            )
            still_missing_local = _i(cur.fetchone()[0]) > 0

            event_count = 0
            api_error_note: str | None = None
            client = FinancesClient(marketplace_id=row["marketplace_id"])
            try:
                events = asyncio.run(client.list_financial_events_by_order_id(order_id, max_results=500))
                event_count = sum(len(v or []) for v in events.values())
            except Exception as exc:
                api_errors += 1
                api_error_note = f"amazon_api_error:{str(exc)[:160]}"
            now_note = "still_missing"
            next_status = "open"
            resolved_at = None
            if not still_missing_local:
                resolved += 1
                next_status = "resolved"
                resolved_at = "SYSUTCDATETIME()"
                now_note = "resolved_after_bridge"
            elif api_error_note:
                still_missing += 1
                now_note = api_error_note
            elif event_count > 0:
                amazon_events_available += 1
                next_status = "amazon_events_available"
                now_note = "amazon_now_returns_events_rerun_bridge"
            else:
                still_missing += 1
                now_note = "amazon_returns_zero_events"

            if resolved_at:
                cur.execute(
                    """
                    UPDATE dbo.acc_fee_gap_watch
                    SET status = ?,
                        last_checked_at = SYSUTCDATETIME(),
                        resolved_at = SYSUTCDATETIME(),
                        last_amazon_event_count = ?,
                        last_note = ?
                    WHERE id = ?
                    """,
                    [next_status, event_count, now_note, row["id"]],
                )
            else:
                cur.execute(
                    """
                    UPDATE dbo.acc_fee_gap_watch
                    SET status = ?,
                        last_checked_at = SYSUTCDATETIME(),
                        last_amazon_event_count = ?,
                        last_note = ?
                    WHERE id = ?
                    """,
                    [next_status, event_count, now_note, row["id"]],
                )

            cur.execute(
                """
                SELECT id, gap_type, gap_reason, marketplace_id, amazon_order_id,
                    sample_sku, sample_asin, fulfillment_channel, status,
                    CONVERT(NVARCHAR(19), first_seen_at, 120) AS first_seen_at,
                    CONVERT(NVARCHAR(19), last_seen_at, 120) AS last_seen_at,
                    CONVERT(NVARCHAR(19), last_checked_at, 120) AS last_checked_at,
                    CONVERT(NVARCHAR(19), resolved_at, 120) AS resolved_at,
                    last_amazon_event_count, last_note
                FROM dbo.acc_fee_gap_watch WITH (NOLOCK)
                WHERE id = ?
                """,
                [row["id"]],
            )
            refreshed = _fetchall_dict(cur)[0]
            refreshed["id"] = str(refreshed.get("id") or "")
            refreshed["marketplace_code"] = _mkt_code(refreshed["marketplace_id"])
            refreshed["last_amazon_event_count"] = _i(refreshed.get("last_amazon_event_count"))
            results.append(refreshed)

        cur.execute(
            """
            UPDATE dbo.acc_fee_gap_recheck_run
            SET finished_at = SYSUTCDATETIME(),
                checked_count = ?,
                resolved_count = ?,
                amazon_events_available_count = ?,
                still_missing_count = ?,
                note = ?
            WHERE id = ?
            """,
            [
                checked,
                resolved,
                amazon_events_available,
                still_missing,
                f"manual_recheck_completed api_errors={api_errors}",
                run_id,
            ],
        )
        conn.commit()
        _result_cache_invalidate("fee-gap:")
        return {
            "checked": checked,
            "resolved": resolved,
            "amazon_events_available": amazon_events_available,
            "still_missing": still_missing,
            "api_errors": api_errors,
            "items": results,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Profit Overview KPIs — for Executive Dashboard
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Profit KPIs
# ---------------------------------------------------------------------------

def get_profit_kpis(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    prev_date_from: date | None = None,
    prev_date_to: date | None = None,
) -> dict[str, Any]:
    """Return CM1-based KPIs with optional period comparison."""
    # In-memory cache
    kpi_cache_key = f"pkpi:{date_from}:{date_to}:{marketplace_id}:{prev_date_from}:{prev_date_to}"
    cached = _result_cache_get(kpi_cache_key)
    if cached is not None:
        return cached

    conn = _connect()
    try:
        cur = conn.cursor()

        def _compute_kpis(d_from: date, d_to: date) -> dict[str, float]:
            wheres = [
                "o.status = 'Shipped'",
                "o.purchase_date >= CAST(? AS DATE)",
                "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
                # Exclude Amazon Renewed (used) products from KPI
                RENEWED_SKU_FILTER,
            ]
            params: list[Any] = [d_from.isoformat(), d_to.isoformat()]
            if marketplace_id:
                wheres.append("o.marketplace_id = ?")
                params.append(marketplace_id)
            order_logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
            order_logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

            cur.execute(f"""
                SELECT
                    SUM(
                        (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)
                         - ISNULL(ol.promotion_discount, 0))
                        * ISNULL(fx.rate_to_pln,
                            {_fx_case('o.currency')})
                        + CASE WHEN ISNULL(o.is_refund, 0) = 1 THEN
                            ISNULL(o.refund_amount_pln, 0)
                            * (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                            / NULLIF(olt.order_line_total, 0)
                          ELSE 0 END
                    )                                                  AS revenue_pln,
                    SUM(CASE WHEN ISNULL(o.is_refund, 0) = 1 AND o.refund_type = 'full'
                        THEN 0 ELSE ISNULL(ol.cogs_pln, 0) END)   AS cogs_pln,
                    SUM(ISNULL(ol.fba_fee_pln, 0)
                        + ISNULL(ol.referral_fee_pln, 0))             AS fees_pln,
                    SUM(
                        {order_logistics_value_sql} * CASE
                            WHEN ISNULL(olt.order_line_total, 0) > 0
                                THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                            WHEN ISNULL(olt.order_units_total, 0) > 0
                                THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    )                                                  AS logistics_pln,
                    SUM(ISNULL(ol.quantity_ordered, 0))               AS units,
                    COUNT(DISTINCT o.id)                               AS orders,
                    COUNT(*)                                           AS total_lines,
                    SUM(CASE WHEN ol.cogs_pln > 0 THEN 1 ELSE 0 END) AS lines_with_cogs
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                {order_logistics_join_sql}
                OUTER APPLY (
                    SELECT TOP 1 rate_to_pln
                    FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                    WHERE er.currency = o.currency
                      AND er.rate_date <= o.purchase_date
                    ORDER BY er.rate_date DESC
                ) fx
                OUTER APPLY (
                    SELECT
                        ISNULL(SUM(
                            ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0)
                            - ISNULL(ol2.promotion_discount, 0)
                        ), 1) AS order_line_total,
                        ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                    FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                    WHERE ol2.order_id = o.id
                ) olt
                WHERE {" AND ".join(wheres)}
            """, params)
            row = _fetchall_dict(cur)[0]
            rev = _f(row["revenue_pln"])
            cogs = _f(row["cogs_pln"])
            fees = _f(row["fees_pln"])
            logistics = _f(row.get("logistics_pln"))
            cm1 = round(rev - cogs - fees - logistics, 2)
            tl = _i(row["total_lines"], 1)
            return {
                "revenue_pln": rev,
                "cogs_pln": cogs,
                "fees_pln": fees,
                "logistics_pln": logistics,
                "cm1_pln": cm1,
                "cm1_pct": round(cm1 / rev * 100, 2) if rev else 0.0,
                "units": _i(row["units"]),
                "orders": _i(row["orders"]),
                "cogs_coverage_pct": round(_i(row["lines_with_cogs"]) / tl * 100, 1),
            }

        current = _compute_kpis(date_from, date_to)

        # Previous period for delta
        if prev_date_from and prev_date_to:
            prev = _compute_kpis(prev_date_from, prev_date_to)
        else:
            delta_days = (date_to - date_from).days + 1
            prev = _compute_kpis(
                date_from - timedelta(days=delta_days),
                date_from - timedelta(days=1),
            )

        # Deltas
        def _delta(curr: float, prv: float) -> float | None:
            if prv == 0:
                return None
            return round((curr - prv) / abs(prv) * 100, 1)

        result = {
            "current": current,
            "previous": prev,
            "deltas": {
                "revenue_delta_pct": _delta(current["revenue_pln"], prev["revenue_pln"]),
                "cm1_delta_pct": _delta(current["cm1_pln"], prev["cm1_pln"]),
                "units_delta_pct": _delta(current["units"], prev["units"]),
                "orders_delta_pct": _delta(current["orders"], prev["orders"]),
            },
        }
        _result_cache_set(kpi_cache_key, result)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Manual Purchase Price — upsert from Data Quality page
# ---------------------------------------------------------------------------

