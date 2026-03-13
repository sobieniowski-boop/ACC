"""Job dispatch — routes a JobRun to the correct service handler.

Extracted from mssql_store.py (Sprint 7 S7.1) to keep the data-layer
module focused on SQL and let the platform own execution orchestration.
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import structlog

from app.core.config import settings

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Small helpers (local to dispatch, no DB dependency)
# ---------------------------------------------------------------------------

def _run_async_job(coro):
    return asyncio.run(coro)


def _cleanup_staged_job_file(file_path: str | None) -> None:
    if not file_path:
        return
    try:
        Path(file_path).unlink(missing_ok=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Job dispatch
# ---------------------------------------------------------------------------

def run_job_type(
    job_id: str,
    params: dict[str, Any] | None = None,
    *,
    worker_id: str | None = None,
    lease_seconds: int = 900,
) -> dict[str, Any]:
    # Late imports — keeps module importable without DB at import-time and
    # avoids circular-import issues with mssql_store helpers.
    from app.connectors.mssql.mssql_store import (
        _DEFAULT_ACTOR,
        _QUEUE_HEAVY_SET,
        _to_int_list,
        _to_text_list,
        JobTransientError,
        acquire_db_heavy_slot,
        claim_job_lease,
        evaluate_alert_rules,
        get_job,
        get_profit_by_sku,
        handle_job_failure,
        recalc_profit_orders,
        refresh_plan_actuals,
        release_db_heavy_slot,
        resolve_job_queue,
        set_job_failure,
        set_job_progress,
        set_job_running,
        set_job_success,
        sync_profit_snapshot,
    )

    job = get_job(job_id, include_params=True)
    if not job:
        raise RuntimeError("Job not found")

    job_type = str(job.get("job_type") or "")
    payload = params if params is not None else dict(job.get("params") or {})
    if worker_id:
        if not claim_job_lease(job_id, worker_id=worker_id, lease_seconds=max(60, int(lease_seconds))):
            return get_job(job_id) or {}
    set_job_running(job_id, f"Starting {job_type}")

    has_heavy_slot = False
    try:
        if resolve_job_queue(job_type) in _QUEUE_HEAVY_SET:
            has_heavy_slot = acquire_db_heavy_slot(
                job_id,
                lease_seconds=max(300, int(settings.WORKER_DB_HEAVY_LEASE_SEC or 14400)),
            )
            if not has_heavy_slot:
                raise JobTransientError("db-heavy semaphore busy")
        today = date.today()
        if job_type == "calc_profit":
            from_date = date.fromisoformat(payload["date_from"]) if payload.get("date_from") else (today - timedelta(days=30))
            to_date = date.fromisoformat(payload["date_to"]) if payload.get("date_to") else today
            synced = recalc_profit_orders(date_from=from_date, date_to=to_date)
            # Also update snapshot
            sync_profit_snapshot(date_from=from_date, date_to=to_date)
            generated = evaluate_alert_rules(days=7)
            set_job_success(job_id, records_processed=synced, message=f"Profit recalculated={synced}, alerts={generated}")
        elif job_type == "recompute_profitability":
            from app.intelligence.profit.rollup import recompute_rollups
            from app.services.executive_service import recompute_executive_metrics
            days = int(payload.get("days_back", 7))
            rollup = recompute_rollups(days_back=days)
            exec_m = recompute_executive_metrics(days_back=days)
            sku = rollup.get("sku_rows_upserted", 0)
            mkt = rollup.get("marketplace_rows_upserted", 0)
            em = exec_m.get("metrics_rows", 0)
            set_job_success(job_id, records_processed=sku + mkt, message=f"SKU={sku} MKT={mkt} exec_metrics={em}")
        elif job_type == "sync_orders":
            from app.services.order_pipeline import step_sync_orders

            result = asyncio.run(
                step_sync_orders(
                    days_back=int(payload.get("days_back", 1)),
                    marketplace_id=job.get("marketplace_id"),
                    max_results=None,
                    sync_profile=str(payload.get("sync_profile") or "core_sync"),
                )
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("orders", 0) or 0),
                message=(
                    f"sync_orders orders={result.get('orders', 0)}, "
                    f"new={result.get('new_orders', 0)}, "
                    f"updated={result.get('updated_orders', 0)}, "
                    f"hash_skipped={result.get('orders_hash_skipped', 0)}, "
                    f"profile={result.get('sync_profile', 'core_sync')}"
                ),
            )
        elif job_type in {"sync_finances", "sync_inventory"}:
            from_date = date.fromisoformat(payload["date_from"]) if payload.get("date_from") else (today - timedelta(days=7))
            to_date = date.fromisoformat(payload["date_to"]) if payload.get("date_to") else today
            synced = sync_profit_snapshot(date_from=from_date, date_to=to_date)
            set_job_success(job_id, records_processed=synced, message=f"{job_type} synced snapshot rows={synced}")
        elif job_type == "sync_pricing":
            from app.services.sync_service import sync_pricing as _sync_pricing

            try:
                loop = asyncio.get_running_loop()
                # If already in async context, create task
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        _sync_pricing(
                            marketplace_id=job.get("marketplace_id"),
                            job_id=job_id,
                        )
                    )
                    total = future.result()
            except RuntimeError:
                # No running loop, safe to use asyncio.run
                total = asyncio.run(
                    _sync_pricing(
                        marketplace_id=job.get("marketplace_id"),
                        job_id=job_id,
                    )
                )
            # Alert rules after pricing refresh
            alerts_created = evaluate_alert_rules(days=3)
            set_job_success(
                job_id,
                records_processed=int(total or 0),
                message=f"Pricing synced offers={total or 0}, alerts={alerts_created}",
            )
        elif job_type == "sync_offer_fee_estimates":
            from app.services.sync_service import sync_offer_fee_estimates as _sync_offer_fee_estimates

            only_missing_raw = payload.get("only_missing", True)
            only_missing = (
                only_missing_raw
                if isinstance(only_missing_raw, bool)
                else str(only_missing_raw).strip().lower() in {"1", "true", "yes", "y"}
            )
            result = asyncio.run(
                _sync_offer_fee_estimates(
                    marketplace_id=job.get("marketplace_id"),
                    job_id=job_id,
                    max_offers=int(payload.get("max_offers", 600)),
                    only_missing=only_missing,
                )
            )
            synced = int((result or {}).get("synced", 0) or 0)
            errors = int((result or {}).get("errors", 0) or 0)
            processed = int((result or {}).get("processed", 0) or 0)
            set_job_success(
                job_id,
                records_processed=synced,
                message=(
                    f"Expected fees synced={synced}, "
                    f"processed={processed}, errors={errors}"
                ),
            )
        elif job_type == "sync_tkl_cache":
            from app.services.profit_engine import refresh_tkl_sql_cache as _refresh_tkl_sql_cache

            result = _refresh_tkl_sql_cache(force=bool(payload.get("force", True)))
            set_job_success(
                job_id,
                records_processed=int(result.get("country_pairs", 0) or 0) + int(result.get("sku_rows", 0) or 0),
                message=(
                    f"TKL cache refreshed, country_pairs={result.get('country_pairs', 0)}, "
                    f"sku_rows={result.get('sku_rows', 0)}"
                ),
            )
        elif job_type == "generate_ai_report":
            summary = get_profit_by_sku(date_from=today - timedelta(days=30), date_to=today, marketplace_id=None)
            set_job_success(
                job_id,
                records_processed=summary.get("total_skus", 0),
                message=f"AI report source prepared for {summary.get('total_skus', 0)} skus",
            )
        elif job_type == "sync_purchase_prices":
            from app.services.sync_service import sync_purchase_prices as _spp
            updated = asyncio.run(_spp(job_id=None))
            set_job_success(job_id, records_processed=updated, message=f"Purchase prices synced={updated}")
        elif job_type == "sync_product_mapping":
            from app.services.sync_service import sync_product_mapping as _spm
            mapped = asyncio.run(_spm(job_id=None))
            set_job_success(job_id, records_processed=mapped, message=f"Product mapping synced={mapped}")
        elif job_type == "sync_amazon_listing_registry":
            from app.services.amazon_listing_registry import sync_amazon_listing_registry

            result = sync_amazon_listing_registry(
                force=bool(payload.get("force", False)),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("row_count", 0) or 0),
                message=f"Amazon listing registry {result.get('status')} rows={result.get('row_count', 0)}",
            )
        elif job_type == "sync_listings_to_products":
            from app.services.sync_listings_to_products import sync_listings_to_products

            result = _run_async_job(
                sync_listings_to_products(
                    marketplace_ids=payload.get("marketplace_ids") or None,
                    job_id=job_id,
                )
            )
            totals = result.get("totals", {}) if isinstance(result, dict) else {}
            set_job_success(
                job_id,
                records_processed=int(totals.get("created", 0) or 0) + int(totals.get("enriched", 0) or 0),
                message=(
                    f"Listings synced created={totals.get('created', 0)}, "
                    f"enriched={totals.get('enriched', 0)}, errors={totals.get('errors', 0)}"
                ),
            )
        elif job_type == "sync_ads":
            from app.services.ads_sync import run_full_ads_sync

            result = _run_async_job(run_full_ads_sync(days_back=int(payload.get("days_back", 3) or 3)))
            rows = int(result.get("daily_rows_upserted", 0) or 0)
            campaigns = int(result.get("campaigns_upserted", 0) or 0)
            set_job_success(
                job_id,
                records_processed=rows,
                message=(
                    f"Ads sync {result.get('status', 'ok')}: campaigns={campaigns}, "
                    f"daily_rows={rows}"
                ),
            )
        elif job_type == "sync_taxonomy":
            from app.services.taxonomy import refresh_taxonomy_predictions

            progress_state: dict[str, int] = {"last_processed": -1}

            def _progress_callback(processed: int, total: int, generated: int, stage: str) -> None:
                if processed == progress_state["last_processed"] and stage == "predicting":
                    return
                progress_state["last_processed"] = processed
                total_safe = max(int(total or 0), 1)
                pct = 10 + int(min(85, (processed / total_safe) * 80))
                remaining = max(0, int(total or 0) - int(processed or 0))
                set_job_progress(
                    job_id,
                    progress_pct=pct,
                    records_processed=int(processed or 0),
                    message=(
                        f"Taxonomy {stage}: processed={processed}/{total}, "
                        f"remaining={remaining}, generated={generated}"
                    ),
                )

            result = refresh_taxonomy_predictions(
                limit=int(payload.get("limit", 40000) or 40000),
                min_auto_confidence=float(payload.get("min_auto_confidence", 0.90) or 0.90),
                auto_apply=bool(payload.get("auto_apply", True)),
                progress_hook=_progress_callback,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("generated", 0) or 0),
                message=(
                    f"Taxonomy refreshed generated={result.get('generated', 0)}, "
                    f"auto_applied={result.get('auto_applied', 0)}, "
                    f"candidates={result.get('candidates', 0)}"
                ),
            )
        elif job_type == "inventory_taxonomy_refresh":
            from app.services.taxonomy import refresh_taxonomy_predictions

            result = refresh_taxonomy_predictions(
                limit=int(payload.get("limit", 40000) or 40000),
                min_auto_confidence=float(payload.get("min_auto_confidence", 0.90) or 0.90),
                auto_apply=bool(payload.get("auto_apply", True)),
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("generated", 0) or 0),
                message=(
                    f"Inventory taxonomy refreshed generated={result.get('generated', 0)}, "
                    f"auto_applied={result.get('auto_applied', 0)}, "
                    f"candidates={result.get('candidates', 0)}"
                ),
            )
        elif job_type == "inventory_sync_listings":
            from app.services.family_mapper.marketplace_sync import sync_marketplace_listings

            result = asyncio.run(sync_marketplace_listings())
            set_job_success(
                job_id,
                records_processed=int(result.get("synced", 0) or 0),
                message=f"Inventory listings synced={result.get('synced', 0)}, marketplaces={result.get('marketplaces', 0)}, errors={result.get('errors', 0)}",
            )
        elif job_type == "inventory_sync_snapshots":
            from app.services.fba_ops import sync_inventory_cache

            set_job_progress(
                job_id,
                progress_pct=25,
                message="Syncing FBA inventory snapshots",
                records_processed=0,
            )
            result = sync_inventory_cache(return_meta=True)
            rows = int(result.get("rows", 0) or 0)
            set_job_success(
                job_id,
                records_processed=rows,
                message=f"Inventory snapshots synced={rows}",
            )
        elif job_type == "inventory_sync_sales_traffic":
            from app.services.manage_inventory import sync_inventory_sales_traffic

            result = sync_inventory_sales_traffic(
                days_back=int(payload.get("days_back", 90)),
                job_id=job_id,
                marketplace_ids=payload.get("marketplace_ids"),
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("rows", 0) or 0),
                message=(
                    f"Inventory sales traffic sku_rows={int(result.get('rows', 0) or 0)} "
                    f"asin_rows={int(result.get('asin_rows', 0) or 0)}"
                ),
            )
        elif job_type == "inventory_compute_rollups":
            from app.services.manage_inventory import compute_inventory_rollups

            result = compute_inventory_rollups(job_id=job_id)
            set_job_success(
                job_id,
                records_processed=int(result.get("rows", 0) or 0),
                message=f"Inventory rollups {result.get('status', 'ok')} rows={result.get('rows', 0)}",
            )
        elif job_type == "inventory_run_alerts":
            from app.services.manage_inventory import evaluate_inventory_alerts

            result = evaluate_inventory_alerts()
            set_job_success(
                job_id,
                records_processed=int(result.get("candidates", 0) or 0),
                message=f"Inventory alerts evaluated candidates={result.get('candidates', 0)}",
            )
        elif job_type == "inventory_apply_draft":
            from app.services.manage_inventory import apply_inventory_draft

            draft_id = str(payload.get("draft_id") or "").strip()
            if not draft_id:
                raise ValueError("draft_id is required for inventory_apply_draft")
            result = apply_inventory_draft(draft_id, _DEFAULT_ACTOR)
            draft = result.get("draft") if isinstance(result, dict) else {}
            set_job_success(
                job_id,
                records_processed=1,
                message=f"Inventory draft applied status={(draft or {}).get('apply_status', 'unknown')}",
            )
        elif job_type == "inventory_rollback_draft":
            from app.services.manage_inventory import rollback_inventory_draft

            result = rollback_inventory_draft(str(payload.get("draft_id") or ""), _DEFAULT_ACTOR)
            draft = result.get("draft") if isinstance(result, dict) else {}
            set_job_success(
                job_id,
                records_processed=1,
                message=f"Inventory draft rolled back status={(draft or {}).get('apply_status', 'unknown')}",
            )
        elif job_type == "order_pipeline":
            from app.services.order_pipeline import run_order_pipeline as _rop
            result = asyncio.run(_rop())
            set_job_success(job_id, records_processed=0, message=f"Order pipeline completed: {result}")
        elif job_type == "family_sync_marketplace_listings":
            from app.services.family_mapper.marketplace_sync import sync_marketplace_listings

            result = _run_async_job(
                sync_marketplace_listings(
                    marketplace_ids=payload.get("marketplace_ids") or None,
                    family_ids=payload.get("family_ids") or None,
                )
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("synced", 0) or 0),
                message=(
                    f"Family sync marketplaces={result.get('marketplaces', 0)}, "
                    f"synced={result.get('synced', 0)}, errors={result.get('errors', 0)}"
                ),
            )
        elif job_type == "family_matching_pipeline":
            from app.services.family_mapper.coverage import recompute_coverage
            from app.services.family_mapper.matching import run_matching

            match_result = _run_async_job(
                run_matching(
                    marketplace_ids=payload.get("marketplace_ids") or None,
                    family_ids=payload.get("family_ids") or None,
                )
            )
            set_job_progress(
                job_id,
                progress_pct=85,
                records_processed=int(match_result.get("matched", 0) or 0),
                message="Family matching done, recomputing coverage",
            )
            coverage_result = _run_async_job(recompute_coverage())
            set_job_success(
                job_id,
                records_processed=int(match_result.get("matched", 0) or 0),
                message=(
                    f"Family matching matched={match_result.get('matched', 0)}, "
                    f"unmatched={match_result.get('unmatched', 0)}, "
                    f"coverage_updated={coverage_result.get('updated', 0)}"
                ),
            )
        elif job_type == "family_recompute_coverage":
            from app.services.family_mapper.coverage import recompute_coverage

            result = _run_async_job(recompute_coverage())
            set_job_success(
                job_id,
                records_processed=int(result.get("updated", 0) or 0),
                message=f"Family coverage recomputed updated={result.get('updated', 0)}",
            )
        elif job_type == "sync_fba_inventory":
            from app.services.fba_ops import sync_inventory_cache

            result = sync_inventory_cache(return_meta=True)
            rows = int(result.get("rows", 0))
            diag = str(result.get("report_diagnostics_summary") or "reports=n/a")
            set_job_success(job_id, records_processed=rows, message=f"FBA inventory cache synced={rows}; {diag}")
        elif job_type == "sync_fba_inbound":
            from app.services.fba_ops import sync_inbound_stub

            rows = sync_inbound_stub()
            set_job_success(job_id, records_processed=rows, message=f"FBA inbound synced={rows}")
        elif job_type == "run_fba_alerts":
            from app.services.fba_ops import run_alert_scan

            created = run_alert_scan()
            set_job_success(job_id, records_processed=created, message=f"FBA alerts created={created}")
        elif job_type == "recompute_fba_replenishment":
            from app.services.fba_ops import get_replenishment_suggestions

            result = get_replenishment_suggestions()
            set_job_success(job_id, records_processed=result.get("total", 0), message=f"FBA replenishment suggestions={result.get('total', 0)}")
        elif job_type == "finance_sync_transactions":
            from app.services.finance_center import import_amazon_transactions

            result = import_amazon_transactions(
                days_back=int(payload.get("days_back", 30)),
                marketplace_id=job.get("marketplace_id"),
                job_id=job_id,
            )
            processed = int(result.get("fee_rows", 0) or result.get("transactions", 0) or 0)
            bridged = int(result.get("bridged_lines", 0) or 0)
            completeness = (result.get("completeness_alert") or {}).get("status") or "unknown"
            set_job_success(
                job_id,
                records_processed=processed,
                message=f"Finance transactions synced={processed}, bridged={bridged}, completeness={completeness}",
            )
        elif job_type == "finance_prepare_settlements":
            from app.services.finance_center import build_settlement_summaries

            result = build_settlement_summaries(job_id=job_id)
            set_job_success(job_id, records_processed=int(result.get("settlements", 0)), message=f"Finance settlements prepared={result.get('settlements', 0)}")
        elif job_type == "finance_generate_ledger":
            from app.services.finance_center import generate_ledger_from_amazon

            result = generate_ledger_from_amazon(days_back=int(payload.get("days_back", 90)), job_id=job_id)
            set_job_success(job_id, records_processed=int(result.get("inserted", 0)), message=f"Finance ledger generated={result.get('inserted', 0)}")
        elif job_type == "finance_reconcile_payouts":
            from app.services.finance_center import auto_match_payouts

            result = auto_match_payouts()
            set_job_success(job_id, records_processed=int(result.get("matched", 0)), message=f"Finance payouts matched={result.get('matched', 0)}")
        elif job_type == "returns_seed_items":
            from app.services.return_tracker import seed_return_items_from_orders

            from_date = date.fromisoformat(payload["date_from"]) if payload.get("date_from") else (today - timedelta(days=90))
            to_date = date.fromisoformat(payload["date_to"]) if payload.get("date_to") else today
            result = seed_return_items_from_orders(date_from=from_date, date_to=to_date)
            set_job_success(
                job_id,
                records_processed=int(result.get("inserted", 0) or 0) + int(result.get("skipped_existing", 0) or 0),
                message=(
                    f"Returns seeded inserted={result.get('inserted', 0)}, "
                    f"skipped_existing={result.get('skipped_existing', 0)}"
                ),
            )
        elif job_type == "returns_reconcile":
            from app.services.return_tracker import reconcile_returns

            result = reconcile_returns()
            set_job_success(
                job_id,
                records_processed=int(result.get("matched", 0) or 0),
                message=(
                    f"Returns reconciled matched={result.get('matched', 0)}, "
                    f"pending_marked_lost={result.get('pending_marked_lost', 0)}"
                ),
            )
        elif job_type == "returns_rebuild_summary":
            from app.services.return_tracker import rebuild_daily_summary

            from_date = date.fromisoformat(payload["date_from"]) if payload.get("date_from") else (today - timedelta(days=90))
            to_date = date.fromisoformat(payload["date_to"]) if payload.get("date_to") else today
            rows = rebuild_daily_summary(date_from=from_date, date_to=to_date)
            set_job_success(
                job_id,
                records_processed=int(rows or 0),
                message=f"Returns daily summary rebuilt rows={int(rows or 0)}",
            )
        elif job_type == "returns_sync_fba":
            from app.services.return_tracker import sync_fba_returns

            result = _run_async_job(
                sync_fba_returns(
                    days_back=int(payload.get("days_back", 30) or 30),
                    marketplace_ids=payload.get("marketplace_ids") or None,
                    use_watermark=bool(payload.get("use_watermark", True)),
                )
            )
            totals = result.get("totals", {}) if isinstance(result, dict) else {}
            set_job_success(
                job_id,
                records_processed=int(totals.get("rows", 0) or 0),
                message=(
                    f"Returns sync reports={totals.get('reports', 0)}, "
                    f"rows={totals.get('rows', 0)}, errors={totals.get('errors', 0)}"
                ),
            )
        elif job_type == "returns_backfill_fba":
            from app.services.return_tracker import backfill_fba_returns

            result = _run_async_job(
                backfill_fba_returns(
                    days_back=int(payload.get("days_back", 90) or 90),
                    marketplace_ids=payload.get("marketplace_ids") or None,
                    chunk_days=int(payload.get("chunk_days", 30) or 30),
                )
            )
            totals = result.get("totals", {}) if isinstance(result, dict) else {}
            set_job_success(
                job_id,
                records_processed=int(totals.get("rows", 0) or 0),
                message=(
                    f"Returns backfill reports={totals.get('reports', 0)}, "
                    f"rows={totals.get('rows', 0)}, errors={totals.get('errors', 0)}"
                ),
            )
        elif job_type == "fee_gap_watch_seed":
            from app.services.profit_engine import seed_fee_gap_watch

            from_date = date.fromisoformat(payload["date_from"]) if payload.get("date_from") else (today - timedelta(days=30))
            to_date = date.fromisoformat(payload["date_to"]) if payload.get("date_to") else today
            result = seed_fee_gap_watch(
                date_from=from_date,
                date_to=to_date,
                marketplace_id=job.get("marketplace_id"),
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("inserted", 0) or 0) + int(result.get("updated", 0) or 0),
                message=(
                    f"Fee-gap watch seeded inserted={result.get('inserted', 0)}, "
                    f"updated={result.get('updated', 0)}, open_total={result.get('open_total', 0)}"
                ),
            )
        elif job_type == "fee_gap_watch_recheck":
            from app.services.profit_engine import recheck_fee_gap_watch

            result = recheck_fee_gap_watch(
                limit=int(payload.get("limit", 50) or 50),
                marketplace_id=job.get("marketplace_id"),
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("checked", 0) or 0),
                message=(
                    f"Fee-gap watch rechecked checked={result.get('checked', 0)}, "
                    f"resolved={result.get('resolved', 0)}, "
                    f"amazon_events_available={result.get('amazon_events_available', 0)}, "
                    f"still_missing={result.get('still_missing', 0)}, "
                    f"api_errors={result.get('api_errors', 0)}"
                ),
            )
        elif job_type == "dhl_backfill_shipments":
            from app.services.dhl_registry_sync import backfill_dhl_shipments

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else (today - timedelta(days=30))
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else today
            result = backfill_dhl_shipments(
                created_from=from_date,
                created_to=to_date,
                include_events=bool(payload.get("include_events", True)),
                limit_shipments=int(payload.get("limit_shipments") or 0) or None,
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("shipments_processed", 0) or 0),
                message=(
                    f"DHL backfill processed={result.get('shipments_processed', 0)}, "
                    f"linked={result.get('shipments_linked', 0)}, "
                    f"events={result.get('events_written', 0)}"
                ),
            )
        elif job_type == "dhl_sync_tracking_events":
            from app.services.dhl_registry_sync import sync_dhl_tracking_events

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = sync_dhl_tracking_events(
                created_from=from_date,
                created_to=to_date,
                limit_shipments=int(payload.get("limit_shipments", 500) or 500),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("shipments_processed", 0) or 0),
                message=(
                    f"DHL tracking sync processed={result.get('shipments_processed', 0)}, "
                    f"events={result.get('events_written', 0)}, "
                    f"delivered={result.get('delivered_shipments', 0)}"
                ),
            )
        elif job_type == "dhl_sync_costs":
            from app.services.dhl_cost_sync import sync_dhl_shipment_costs

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = sync_dhl_shipment_costs(
                created_from=from_date,
                created_to=to_date,
                limit_shipments=int(payload.get("limit_shipments", 500) or 500),
                allow_estimated=bool(payload.get("allow_estimated", True)),
                refresh_existing=bool(payload.get("refresh_existing", False)),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("shipments_processed", 0) or 0),
                message=(
                    f"DHL cost sync processed={result.get('shipments_processed', 0)}, "
                    f"actual={result.get('actual_costs_written', 0)}, "
                    f"estimated={result.get('estimated_costs_written', 0)}"
                ),
            )
        elif job_type == "dhl_import_billing_files":
            from app.services.dhl_billing_import import import_dhl_billing_files

            result = import_dhl_billing_files(
                invoice_root=payload.get("invoice_root"),
                jj_root=payload.get("jj_root"),
                manifest_path=payload.get("manifest_path"),
                include_shipment_seed=bool(payload.get("include_shipment_seed", True)),
                seed_all_existing=bool(payload.get("seed_all_existing", False)),
                force_reimport=bool(payload.get("force_reimport", False)),
                limit_invoice_files=int(payload.get("limit_invoice_files", 0) or 0) or None,
                limit_jj_files=int(payload.get("limit_jj_files", 0) or 0) or None,
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(
                    (result.get("invoice_line_rows_imported", 0) or 0)
                    + (result.get("jj_rows_imported", 0) or 0)
                ),
                message=(
                    f"DHL billing import invoice_rows={result.get('invoice_line_rows_imported', 0)}, "
                    f"jj_rows={result.get('jj_rows_imported', 0)}, "
                    f"shipments_seeded={result.get('shipments_seeded', 0)}"
                ),
            )
        elif job_type == "dhl_seed_shipments_from_staging":
            from app.services.dhl_billing_import import seed_dhl_shipments_from_staging

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = seed_dhl_shipments_from_staging(
                created_from=from_date,
                created_to=to_date,
                seed_all_existing=bool(payload.get("seed_all_existing", True)),
                limit_parcels=int(payload.get("limit_parcels", 0) or 0) or None,
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("shipments_seeded", 0) or 0),
                message=(
                    f"DHL seed shipments seeded={result.get('shipments_seeded', 0)}, "
                    f"linked={result.get('shipments_linked', 0)}, "
                    f"unlinked={result.get('shipments_unlinked', 0)}"
                ),
            )
        elif job_type == "gls_import_billing_files":
            from app.services.gls_billing_import import import_gls_billing_files

            result = import_gls_billing_files(
                invoice_root=payload.get("invoice_root"),
                bl_map_path=payload.get("bl_map_path"),
                include_shipment_seed=bool(payload.get("include_shipment_seed", True)),
                seed_all_existing=bool(payload.get("seed_all_existing", False)),
                force_reimport=bool(payload.get("force_reimport", False)),
                limit_invoice_files=int(payload.get("limit_invoice_files", 0) or 0) or None,
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(
                    (result.get("invoice_line_rows_imported", 0) or 0)
                    + (result.get("bl_map_rows_imported", 0) or 0)
                ),
                message=(
                    f"GLS billing import invoice_rows={result.get('invoice_line_rows_imported', 0)}, "
                    f"bl_rows={result.get('bl_map_rows_imported', 0)}, "
                    f"shipments_seeded={result.get('shipments_seeded', 0)}, "
                    f"errors={result.get('errors', 0)}"
                ),
            )
        elif job_type == "gls_seed_shipments_from_staging":
            from app.services.gls_billing_import import seed_gls_shipments_from_staging

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = seed_gls_shipments_from_staging(
                created_from=from_date,
                created_to=to_date,
                seed_all_existing=bool(payload.get("seed_all_existing", True)),
                limit_parcels=int(payload.get("limit_parcels", 0) or 0) or None,
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("shipments_seeded", 0) or 0),
                message=(
                    f"GLS seed shipments seeded={result.get('shipments_seeded', 0)}, "
                    f"linked={result.get('shipments_linked', 0)}, "
                    f"unlinked={result.get('shipments_unlinked', 0)}"
                ),
            )
        elif job_type == "gls_sync_costs":
            from app.services.gls_cost_sync import sync_gls_shipment_costs

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = sync_gls_shipment_costs(
                created_from=from_date,
                created_to=to_date,
                limit_shipments=int(payload.get("limit_shipments", 5000) or 5000),
                refresh_existing=bool(payload.get("refresh_existing", False)),
                billing_periods=payload.get("billing_periods") or None,
                seeded_only=bool(payload.get("seeded_only", False)),
                only_primary_linked=bool(payload.get("only_primary_linked", False)),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("shipments_processed", 0) or 0),
                message=(
                    f"GLS cost sync processed={result.get('shipments_processed', 0)}, "
                    f"actual={result.get('actual_costs_written', 0)}, "
                    f"no_cost={result.get('no_cost_match', 0)}"
                ),
            )
        elif job_type == "gls_aggregate_logistics":
            from app.services.gls_logistics_aggregation import aggregate_gls_order_logistics

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = aggregate_gls_order_logistics(
                created_from=from_date,
                created_to=to_date,
                limit_orders=int(payload.get("limit_orders", 5000) or 5000),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("orders_aggregated", 0) or 0),
                message=(
                    f"GLS logistics aggregated orders={result.get('orders_aggregated', 0)}, "
                    f"shipments={result.get('shipments_aggregated', 0)}, "
                    f"actual_shipments={result.get('actual_shipments_count', 0)}"
                ),
            )
        elif job_type == "gls_shadow_logistics":
            from app.services.gls_logistics_aggregation import build_gls_logistics_shadow

            from_date = date.fromisoformat(payload["purchase_from"]) if payload.get("purchase_from") else None
            to_date = date.fromisoformat(payload["purchase_to"]) if payload.get("purchase_to") else None
            result = build_gls_logistics_shadow(
                purchase_from=from_date,
                purchase_to=to_date,
                limit_orders=int(payload.get("limit_orders", 10000) or 10000),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("orders_compared", 0) or 0),
                message=(
                    f"GLS shadow compared={result.get('orders_compared', 0)}, "
                    f"delta={result.get('delta', 0)}, "
                    f"shadow_only={result.get('shadow_only', 0)}, "
                    f"legacy_only={result.get('legacy_only', 0)}"
                ),
            )
        elif job_type == "sync_bl_distribution_order_cache":
            from app.services.bl_distribution_cache import sync_bl_distribution_order_cache

            from_date = date.fromisoformat(payload["date_confirmed_from"]) if payload.get("date_confirmed_from") else None
            to_date = date.fromisoformat(payload["date_confirmed_to"]) if payload.get("date_confirmed_to") else None
            result = sync_bl_distribution_order_cache(
                date_confirmed_from=from_date,
                date_confirmed_to=to_date,
                source_ids=_to_int_list(payload.get("source_ids")) or None,
                tracking_numbers=_to_text_list(payload.get("tracking_numbers")) or None,
                include_packages=bool(payload.get("include_packages", True)),
                limit_orders=int(payload.get("limit_orders", 0) or 0) or None,
                job_id=job_id,
            )
            tracking_targets = int(result.get("tracking_targets", 0) or 0)
            tracking_matched = int(result.get("tracking_targets_matched", 0) or 0)
            success_message = (
                f"BL distribution cache orders={result.get('orders_synced', 0)}, "
                f"packages={result.get('packages_synced', 0)}, "
                f"with_delivery_package_nr={result.get('orders_with_delivery_package_nr', 0)}, "
                f"with_external_order_id={result.get('orders_with_external_order_id', 0)}"
            )
            if tracking_targets > 0:
                success_message += f", tracking_matched={tracking_matched}/{tracking_targets}"
            set_job_success(
                job_id,
                records_processed=int(result.get("orders_synced", 0) or 0),
                message=success_message,
            )
        elif job_type == "courier_backfill_identifier_sources":
            from app.services.courier_identifier_backfill import run_courier_identifier_backfill

            result = run_courier_identifier_backfill(
                mode=str(payload.get("mode") or ""),
                months=_to_text_list(payload.get("months")) or None,
                created_to_buffer_days=int(payload.get("created_to_buffer_days", 31) or 31),
                limit_values=int(payload.get("limit_values", 200) or 200),
                include_packages=bool(payload.get("include_packages", True)),
                include_bl_orders=bool(payload.get("include_bl_orders", True)),
                include_dis_map=bool(payload.get("include_dis_map", True)),
                include_dhl_parcel_map=bool(payload.get("include_dhl_parcel_map", True)),
                job_id=job_id,
            )
            totals = result.get("totals", {}) if isinstance(result, dict) else {}
            set_job_success(
                job_id,
                records_processed=int(totals.get("candidate_values", 0) or 0),
                message=(
                    f"Courier identifier backfill mode={result.get('mode')}, "
                    f"candidates={totals.get('candidate_values', 0)}, "
                    f"resolved_orders={totals.get('resolved_order_ids', 0)}, "
                    f"packages={totals.get('acc_package_rows_written', 0)}, "
                    f"bl_orders={totals.get('acc_bl_order_rows_written', 0)}, "
                    f"dis_map={totals.get('acc_dis_map_rows_written', 0)}, "
                    f"dhl_parcel_map={totals.get('acc_dhl_parcel_map_rows_written', 0)}"
                ),
            )
        elif job_type == "courier_refresh_order_relations":
            from app.services.courier_order_relations import refresh_courier_order_relations

            months = _to_text_list(payload.get("months")) or None
            carriers = [item.upper() for item in (_to_text_list(payload.get("carriers")) or [])] or None
            set_job_progress(
                job_id,
                progress_pct=5,
                records_processed=0,
                message="Courier order relation refresh start",
            )
            result = refresh_courier_order_relations(
                months=months,
                carriers=carriers,
                lookahead_days=max(1, int(payload.get("lookahead_days", 30) or 1)),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("rows_written", 0) or 0),
                message=(
                    f"Courier order relations rows={result.get('rows_written', 0)}, "
                    f"months={len(result.get('months') or [])}, "
                    f"carriers={len(result.get('carriers') or [])}"
                ),
            )
        elif job_type == "courier_evaluate_alerts":
            from app.services.courier_alerts import evaluate_courier_alerts

            result = evaluate_courier_alerts(
                window_days=int(payload.get("window_days", 7) or 7),
                cost_coverage_min_pct=float(payload.get("cost_coverage_min_pct", 95.0) or 95.0),
                link_coverage_min_pct=float(payload.get("link_coverage_min_pct", 95.0) or 95.0),
                shadow_delta_max_pct=float(payload.get("shadow_delta_max_pct", 10.0) or 10.0),
                estimation_mape_max_pct=float(payload.get("estimation_mape_max_pct", 25.0) or 25.0),
                estimation_mae_max_pln=float(payload.get("estimation_mae_max_pln", 3.0) or 3.0),
                estimation_min_samples=int(payload.get("estimation_min_samples", 30) or 30),
                estimation_days_back=int(payload.get("estimation_days_back", 30) or 30),
            )
            set_job_success(
                job_id,
                records_processed=int(len(result.get("items") or [])),
                message=(
                    f"Courier alerts created={result.get('created', 0)}, "
                    f"updated={result.get('updated', 0)}, "
                    f"resolved={result.get('resolved', 0)}"
                ),
            )
        elif job_type == "courier_order_universe_linking":
            from app.services.courier_order_universe_pipeline import run_courier_order_universe_pipeline

            months = _to_text_list(payload.get("months")) or ["2025-11", "2025-12", "2026-01"]
            carriers = [item.upper() for item in (_to_text_list(payload.get("carriers")) or ["DHL", "GLS"])]
            set_job_progress(
                job_id,
                progress_pct=10,
                records_processed=0,
                message=f"Courier order-universe linking start months={len(months)} carriers={len(carriers)}",
            )

            def _progress(message: str, completed_steps: int, total_steps: int) -> None:
                pct = 10 + int((max(0, completed_steps) / max(1, total_steps)) * 80)
                set_job_progress(
                    job_id,
                    progress_pct=min(90, max(10, pct)),
                    records_processed=completed_steps,
                    message=message,
                )

            result = run_courier_order_universe_pipeline(
                months=months,
                carriers=carriers,
                reset_existing_in_scope=bool(payload.get("reset_existing_in_scope", False)),
                run_aggregate_shadow=bool(payload.get("run_aggregate_shadow", False)),
                limit_orders=max(1, int(payload.get("limit_orders", 3_000_000) or 1)),
                created_to_buffer_days=max(0, int(payload.get("created_to_buffer_days", 31) or 0)),
                progress_callback=_progress,
            )
            orders_with_fact = 0
            for by_carrier in result.values():
                for details in by_carrier.values():
                    coverage = details.get("coverage") or {}
                    orders_with_fact += int(coverage.get("orders_with_fact", 0) or 0)
            success_message = (
                f"Courier order-universe linking done months={len(months)}, carriers={len(carriers)}, "
                f"orders_with_fact={orders_with_fact}"
            )
            if len(months) == 1 and len(carriers) == 1:
                month_token = months[0]
                carrier_key = carriers[0]
                details = (result.get(month_token) or {}).get(carrier_key) or {}
                linking = details.get("linking") or {}
                coverage = details.get("coverage") or {}
                shipments_in_scope = int(linking.get("shipments_in_scope", 0) or 0)
                shipments_with_primary_link = int(linking.get("shipments_with_primary_link", 0) or 0)
                shipments_unlinked = int(linking.get("shipments_unlinked", 0) or 0)
                orders_linked_primary = int(coverage.get("orders_linked_primary", 0) or 0)
                success_message = (
                    f"Courier link {month_token} {carrier_key} "
                    f"ship={shipments_in_scope} primary={shipments_with_primary_link} "
                    f"unlinked={shipments_unlinked} orders_linked={orders_linked_primary} "
                    f"orders_fact={int(coverage.get('orders_with_fact', 0) or 0)}"
                )
                if carrier_key == "GLS":
                    success_message += (
                        f" note1_map={int(linking.get('unlinked_shipments_note1_mapped_to_order_universe', 0) or 0)} "
                        f"gls_map={int(linking.get('unlinked_shipments_present_in_gls_bl_map', 0) or 0)}"
                    )
                else:
                    success_message += (
                        f" core={int(linking.get('unlinked_shipments_with_core_token', 0) or 0)} "
                        f"pkg={int(linking.get('unlinked_shipments_with_any_package_token_match', 0) or 0)}"
                    )
            set_job_success(
                job_id,
                records_processed=orders_with_fact,
                message=success_message,
            )
        elif job_type == "courier_verify_billing_completeness":
            from app.services.courier_verification import verify_courier_billing_completeness

            result = verify_courier_billing_completeness(
                carrier=str(payload.get("carrier") or "").strip() or None,
                billing_period=str(payload.get("billing_period") or "").strip() or None,
                trigger_source=str(job.get("trigger_source") or "job"),
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("audits_written", 0) or 0),
                message=(
                    f"Courier billing verification audits={result.get('audits_written', 0)}, "
                    f"status={result.get('status', 'unknown')}"
                ),
            )
        elif job_type == "courier_refresh_monthly_kpis":
            from app.services.courier_monthly_kpi import refresh_courier_monthly_kpi_snapshot

            months_raw = _to_text_list(payload.get("months"))
            carriers_raw = _to_text_list(payload.get("carriers"))
            set_job_progress(
                job_id,
                progress_pct=5,
                records_processed=0,
                message="Courier monthly KPI snapshot start",
            )
            result = refresh_courier_monthly_kpi_snapshot(
                months=months_raw or None,
                carriers=[item.upper() for item in carriers_raw] if carriers_raw else None,
                buffer_days=max(0, int(payload.get("buffer_days", 45) or 0)),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("rows_upserted", 0) or 0),
                message=(
                    f"Courier monthly KPI snapshot rows={result.get('rows_upserted', 0)}, "
                    f"months={len(result.get('months') or [])}, "
                    f"carriers={len(result.get('carriers') or [])}"
                ),
            )
        elif job_type == "courier_refresh_shipment_outcomes":
            from app.services.courier_shipment_semantics import refresh_courier_shipment_outcomes

            months_raw = _to_text_list(payload.get("months"))
            carriers_raw = _to_text_list(payload.get("carriers"))
            set_job_progress(
                job_id,
                progress_pct=5,
                records_processed=0,
                message="Courier shipment outcome refresh start",
            )
            result = refresh_courier_shipment_outcomes(
                months=months_raw or None,
                carriers=[item.upper() for item in carriers_raw] if carriers_raw else None,
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("rows_written", 0) or 0),
                message=(
                    f"Courier shipment outcomes rows={result.get('rows_written', 0)}, "
                    f"months={len(result.get('months') or [])}, "
                    f"carriers={len(result.get('carriers') or [])}"
                ),
            )
        elif job_type == "courier_estimate_preinvoice_costs":
            from app.services.courier_cost_estimation import estimate_preinvoice_courier_costs

            carriers = [str(item).strip().upper() for item in (_to_text_list(payload.get("carriers")) or ["DHL", "GLS"])]
            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = estimate_preinvoice_courier_costs(
                carriers=carriers,
                created_from=from_date,
                created_to=to_date,
                horizon_days=max(30, int(payload.get("horizon_days", 180) or 180)),
                min_samples=max(1, int(payload.get("min_samples", 10) or 10)),
                limit_shipments=max(1, int(payload.get("limit_shipments", 20000) or 1)),
                refresh_existing=bool(payload.get("refresh_existing", False)),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("estimated_written", 0) or 0),
                message=(
                    f"Courier preinvoice estimated={result.get('estimated_written', 0)}, "
                    f"targets={result.get('shipments_selected', 0)}, "
                    f"history={result.get('historical_samples', 0)}"
                ),
            )
        elif job_type == "courier_reconcile_estimated_costs":
            from app.services.courier_cost_estimation import reconcile_estimated_costs

            carriers = [str(item).strip().upper() for item in (_to_text_list(payload.get("carriers")) or ["DHL", "GLS"])]
            result = reconcile_estimated_costs(
                carriers=carriers,
                limit_shipments=max(1, int(payload.get("limit_shipments", 50000) or 1)),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("reconciled_rows", 0) or 0),
                message=(
                    f"Courier reconcile reconciled={result.get('reconciled_rows', 0)}, "
                    f"deleted_estimates={result.get('estimated_cost_rows_deleted', 0)}"
                ),
            )
        elif job_type == "courier_compute_estimation_kpis":
            from app.services.courier_cost_estimation import compute_courier_estimation_kpis

            carriers = [str(item).strip().upper() for item in (_to_text_list(payload.get("carriers")) or ["DHL", "GLS"])]
            result = compute_courier_estimation_kpis(
                days_back=max(1, int(payload.get("days_back", 30) or 30)),
                carriers=carriers,
                model_version=str(payload.get("model_version") or "courier_hist_v1"),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("kpi_rows_upserted", 0) or 0),
                message=(
                    f"Courier KPI rows={result.get('kpi_rows_upserted', 0)}, "
                    f"source_rows={result.get('rows_source', 0)}"
                ),
            )
        elif job_type == "import_products_upload":
            from app.services.import_products import parse_import_excel, upsert_import_products

            file_path = str(payload.get("file_path") or "").strip()
            if not file_path:
                raise RuntimeError("Missing file_path for import_products_upload")
            set_job_progress(job_id, progress_pct=15, message="Reading staged import-products file")
            file_bytes = Path(file_path).read_bytes()
            set_job_progress(job_id, progress_pct=45, message="Parsing import-products Excel")
            rows = parse_import_excel(file_bytes)
            set_job_progress(
                job_id,
                progress_pct=75,
                records_processed=len(rows),
                message=f"Upserting import-products rows={len(rows)}",
            )
            result = upsert_import_products(rows)
            set_job_success(
                job_id,
                records_processed=int(result.get("total", 0) or 0),
                message=(
                    f"Import products processed={result.get('total', 0)}, "
                    f"inserted={result.get('inserted', 0)}, updated={result.get('updated', 0)}"
                ),
            )
            _cleanup_staged_job_file(file_path)
        elif job_type == "planning_refresh_actuals":
            result = refresh_plan_actuals(
                plan_id=int(payload.get("plan_id")) if payload.get("plan_id") is not None else None,
                year=int(payload.get("year")) if payload.get("year") is not None else None,
            )
            set_job_success(
                job_id,
                records_processed=int(result or 0),
                message=f"Planning actuals refreshed updated={int(result or 0)}",
            )
        elif job_type == "profit_ai_match_run":
            from app.services.ai_product_matcher import run_ai_matching as _run_ai_matching

            result = _run_async_job(_run_ai_matching())
            set_job_success(
                job_id,
                records_processed=int(result.get("suggestions_saved", 0) or 0),
                message=(
                    f"AI match status={result.get('status', 'ok')}, "
                    f"suggestions_saved={result.get('suggestions_saved', 0)}, "
                    f"errors={result.get('errors_count', 0)}"
                ),
            )
        elif job_type == "content_apply_publish_mapping_suggestions":
            from app.services.content_ops import apply_publish_mapping_suggestions

            result = apply_publish_mapping_suggestions(payload=payload)
            set_job_success(
                job_id,
                records_processed=int(result.get("created", 0) or 0),
                message=(
                    f"Content mapping apply created={result.get('created', 0)}, "
                    f"skipped={result.get('skipped', 0)}, dry_run={result.get('dry_run', False)}"
                ),
            )
        elif job_type == "content_refresh_product_type_definition":
            from app.services.content_ops import refresh_product_type_definition

            result = refresh_product_type_definition(payload=payload)
            set_job_success(
                job_id,
                records_processed=1,
                message=(
                    f"Content product type definition refreshed marketplace={result.get('marketplace_code') or result.get('marketplace')}, "
                    f"product_type={result.get('product_type')}"
                ),
            )
        elif job_type == "cogs_import":
            from app.services.cogs_importer import scan_and_import

            result = scan_and_import()
            processed = int(result.get("files_processed", 0) or 0)
            set_job_success(
                job_id,
                records_processed=processed,
                message=(
                    f"COGS import files_processed={processed}, "
                    f"files_skipped={result.get('files_skipped', 0)}, "
                    f"new={result.get('total_new', 0)}, updated={result.get('total_updated', 0)}"
                ),
            )
        elif job_type == "dhl_aggregate_logistics":
            from app.services.dhl_logistics_aggregation import aggregate_dhl_order_logistics

            from_date = date.fromisoformat(payload["created_from"]) if payload.get("created_from") else None
            to_date = date.fromisoformat(payload["created_to"]) if payload.get("created_to") else None
            result = aggregate_dhl_order_logistics(
                created_from=from_date,
                created_to=to_date,
                limit_orders=int(payload.get("limit_orders", 5000) or 5000),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("orders_aggregated", 0) or 0),
                message=(
                    f"DHL logistics aggregated orders={result.get('orders_aggregated', 0)}, "
                    f"shipments={result.get('shipments_aggregated', 0)}, "
                    f"estimated_shipments={result.get('estimated_shipments_count', 0)}"
                ),
            )
        elif job_type == "dhl_shadow_logistics":
            from app.services.dhl_logistics_aggregation import build_dhl_logistics_shadow

            from_date = date.fromisoformat(payload["purchase_from"]) if payload.get("purchase_from") else None
            to_date = date.fromisoformat(payload["purchase_to"]) if payload.get("purchase_to") else None
            result = build_dhl_logistics_shadow(
                purchase_from=from_date,
                purchase_to=to_date,
                limit_orders=int(payload.get("limit_orders", 10000) or 10000),
                job_id=job_id,
            )
            set_job_success(
                job_id,
                records_processed=int(result.get("orders_compared", 0) or 0),
                message=(
                    f"DHL shadow compared={result.get('orders_compared', 0)}, "
                    f"delta={result.get('delta', 0)}, "
                    f"shadow_only={result.get('shadow_only', 0)}, "
                    f"legacy_only={result.get('legacy_only', 0)}"
                ),
            )
        else:
            raise RuntimeError(f"Unsupported job type '{job_type}'")
    except Exception as exc:
        failure_result = handle_job_failure(job_id, exc, job_type=job_type)
        if job_type == "import_products_upload" and failure_result.get("status") == "failure":
            _cleanup_staged_job_file(str(payload.get("file_path") or ""))
    finally:
        if has_heavy_slot:
            release_db_heavy_slot(job_id)

    return get_job(job_id) or {}
