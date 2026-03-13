"""Unified schema registry — single entry-point to ensure all DDL is current.

Instead of each service calling its own ``ensure_*_schema()`` at import
time (or not at all), the registry enumerates every known schema function
and runs them in dependency order during application startup.

Usage (in main.py lifespan):
    from app.platform.schema_registry import ensure_all_schemas
    ensure_all_schemas()
"""
from __future__ import annotations

import structlog

log = structlog.get_logger(__name__)

# Each entry: (human_label, import_path, function_name)
# Ordered by dependency tier: core → connectors → services
_SCHEMA_REGISTRY: list[tuple[str, str, str]] = [
    # Tier 0 — Core connectors
    ("mssql_v2",               "app.connectors.mssql.mssql_store",         "ensure_v2_schema"),
    ("allegro_v2",             "app.connectors.mssql.allegro_store",       "ensure_v2_schema"),

    # Tier 1 — Finance / profit
    ("finance_center",         "app.services.finance_center.service",      "ensure_finance_center_schema"),
    # profit_tkl_cache, profit_data_quality, profit_cost_model migrated to Alembic eb023-eb025
    ("controlling",            "app.services.controlling",                 "ensure_controlling_tables"),

    # Tier 2 — Product / catalog
    ("ads",                    "app.services.ads_sync",                    "ensure_ads_tables"),
    # taxonomy migrated to Alembic eb016
    # amazon_listing_registry migrated to Alembic eb021
    # ptd_cache migrated to Alembic eb017
    ("pricing_state",          "app.services.pricing_state",               "ensure_pricing_state_schema"),
    ("listing_state",          "app.services.listing_state",               "ensure_listing_state_schema"),
    ("catalog_health",         "app.intelligence.catalog_health",          "ensure_catalog_health_schema"),
    # import_products migrated to Alembic eb020

    # Tier 3 — Inventory / FBA
    ("manage_inventory",       "app.services.manage_inventory",            "ensure_manage_inventory_schema"),
    ("fba",                    "app.services.fba_ops._helpers",            "ensure_fba_schema"),
    # fba_fee_audit migrated to Alembic eb022

    # Tier 4 — Logistics  (DHL, GLS, courier_verification, courier_monthly_kpi,
    #   bl_distribution_cache migrated to Alembic eb011-eb015)
    ("courier_identifier",     "app.services.courier_identifier_backfill", "ensure_courier_identifier_cache_schema"),

    # Tier 5 — Events / compliance / tracking
    ("event_backbone",         "app.services.event_backbone",              "ensure_event_backbone_schema"),
    # sp_api_usage migrated to Alembic eb018
    ("tax_compliance",         "app.services.tax_compliance.schema",       "ensure_tax_compliance_schema"),
    # sellerboard_history migrated to Alembic eb019

    # Tier 6 — Platform (action center)
    ("action_center",          "app.platform.action_center",               "ensure_action_center_schema"),
]


def ensure_all_schemas() -> dict[str, str]:
    """Run every registered ensure_*_schema function.

    Returns a dict mapping label → 'ok' | error message.
    """
    import importlib

    results: dict[str, str] = {}
    for label, module_path, func_name in _SCHEMA_REGISTRY:
        try:
            mod = importlib.import_module(module_path)
            fn = getattr(mod, func_name)
            fn()
            results[label] = "ok"
        except Exception as exc:
            log.error("schema_registry.failed", label=label, error=str(exc))
            results[label] = str(exc)

    ok = sum(1 for v in results.values() if v == "ok")
    failed = len(results) - ok
    log.info("schema_registry.done", total=len(results), ok=ok, failed=failed)
    return results
