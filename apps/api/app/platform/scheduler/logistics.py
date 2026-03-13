"""Logistics domain — GLS, DHL, courier estimation, billing verification, BL distribution."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


async def _sync_gls_logistics_pipeline() -> None:
    """Nightly — import GLS billing files, sync costs, aggregate, build shadow."""
    from datetime import date as _date, timedelta as _td

    lookback_days = max(1, int(settings.GLS_LOGISTICS_SYNC_LOOKBACK_DAYS or 60))
    limit_shipments = max(1, int(settings.GLS_LOGISTICS_SYNC_LIMIT_SHIPMENTS or 50000))
    limit_orders = max(1, int(settings.GLS_LOGISTICS_SYNC_LIMIT_ORDERS or 50000))
    date_from = (_date.today() - _td(days=lookback_days)).isoformat()
    date_to = _date.today().isoformat()

    log.info("scheduler.gls_logistics_pipeline.start",
             lookback_days=lookback_days, limit_shipments=limit_shipments, limit_orders=limit_orders)
    try:
        for job_type, params in [
            ("gls_import_billing_files", {"include_shipment_seed": False, "force_reimport": False}),
            ("gls_seed_shipments_from_staging", {"created_from": date_from, "created_to": date_to, "seed_all_existing": False}),
            ("gls_sync_costs", {"created_from": date_from, "created_to": date_to, "limit_shipments": limit_shipments, "refresh_existing": False}),
            ("gls_aggregate_logistics", {"created_from": date_from, "created_to": date_to, "limit_orders": limit_orders}),
            ("gls_shadow_logistics", {"purchase_from": date_from, "purchase_to": date_to, "limit_orders": limit_orders}),
        ]:
            await asyncio.to_thread(run_scheduled_job_type, job_type, params)
        log.info("scheduler.gls_logistics_pipeline.done", date_from=date_from, date_to=date_to)
    except Exception as exc:
        log.error("scheduler.gls_logistics_pipeline.error", error=str(exc))


async def _sync_dhl_logistics_pipeline() -> None:
    """Nightly — import DHL billing files, sync costs, aggregate, build shadow."""
    from datetime import date as _date, timedelta as _td

    lookback_days = max(1, int(settings.DHL_LOGISTICS_SYNC_LOOKBACK_DAYS or 60))
    limit_shipments = max(1, int(settings.DHL_LOGISTICS_SYNC_LIMIT_SHIPMENTS or 50000))
    limit_orders = max(1, int(settings.DHL_LOGISTICS_SYNC_LIMIT_ORDERS or 50000))
    date_from = (_date.today() - _td(days=lookback_days)).isoformat()
    date_to = _date.today().isoformat()

    log.info("scheduler.dhl_logistics_pipeline.start",
             lookback_days=lookback_days, limit_shipments=limit_shipments, limit_orders=limit_orders,
             allow_estimated=bool(settings.DHL_LOGISTICS_SYNC_ALLOW_ESTIMATED))
    try:
        for job_type, params in [
            ("dhl_import_billing_files", {"include_shipment_seed": False, "force_reimport": False}),
            ("dhl_seed_shipments_from_staging", {"created_from": date_from, "created_to": date_to, "seed_all_existing": False}),
            ("dhl_sync_costs", {"created_from": date_from, "created_to": date_to, "limit_shipments": limit_shipments,
                                "allow_estimated": bool(settings.DHL_LOGISTICS_SYNC_ALLOW_ESTIMATED), "refresh_existing": False}),
            ("dhl_aggregate_logistics", {"created_from": date_from, "created_to": date_to, "limit_orders": limit_orders}),
            ("dhl_shadow_logistics", {"purchase_from": date_from, "purchase_to": date_to, "limit_orders": limit_orders}),
        ]:
            await asyncio.to_thread(run_scheduled_job_type, job_type, params)
        log.info("scheduler.dhl_logistics_pipeline.done", date_from=date_from, date_to=date_to)
    except Exception as exc:
        log.error("scheduler.dhl_logistics_pipeline.error", error=str(exc))


async def _run_courier_estimation_pipeline() -> None:
    """Nightly — estimate preinvoice courier costs, reconcile, refresh KPI table."""
    from datetime import date as _date, timedelta as _td

    lookback_days = max(1, int(settings.COURIER_ESTIMATION_SYNC_LOOKBACK_DAYS or 45))
    date_from = (_date.today() - _td(days=lookback_days)).isoformat()
    date_to = _date.today().isoformat()
    carriers = ["DHL", "GLS"]

    log.info("scheduler.courier_estimation_pipeline.start", lookback_days=lookback_days)
    try:
        for job_type, params in [
            ("courier_estimate_preinvoice_costs", {
                "carriers": carriers, "created_from": date_from, "created_to": date_to,
                "horizon_days": max(30, int(settings.COURIER_ESTIMATION_HORIZON_DAYS or 180)),
                "min_samples": max(1, int(settings.COURIER_ESTIMATION_MIN_SAMPLES or 10)),
                "limit_shipments": max(1, int(settings.COURIER_ESTIMATION_LIMIT_SHIPMENTS or 20000)),
                "refresh_existing": bool(settings.COURIER_ESTIMATION_REFRESH_EXISTING),
            }),
            ("courier_reconcile_estimated_costs", {
                "carriers": carriers,
                "limit_shipments": max(1, int(settings.COURIER_RECONCILE_LIMIT_SHIPMENTS or 50000)),
            }),
            ("courier_compute_estimation_kpis", {
                "carriers": carriers, "days_back": max(1, lookback_days),
            }),
        ]:
            await asyncio.to_thread(run_scheduled_job_type, job_type, params)
        log.info("scheduler.courier_estimation_pipeline.done", date_from=date_from, date_to=date_to)
    except Exception as exc:
        log.error("scheduler.courier_estimation_pipeline.error", error=str(exc))


async def _verify_courier_billing_completeness() -> None:
    """Daily — verify expected courier billing files vs imported staging state."""
    job_id = create_job_record("courier_verify_billing_completeness")
    log.info("scheduler.courier_verify_billing.start", job_id=job_id)
    try:
        from app.services.courier_verification import verify_courier_billing_completeness
        result = verify_courier_billing_completeness(trigger_source="scheduler")
        set_job_success(
            job_id, records_processed=int(result.get("audits_written", 0) or 0),
            message=f"courier_billing_verification_status={result.get('status', 'unknown')}, audits={result.get('audits_written', 0)}",
        )
        log.info("scheduler.courier_verify_billing.done", **result)
        try:
            refresh_job = await asyncio.to_thread(run_scheduled_job_type, "courier_refresh_monthly_kpis", {})
            log.info("scheduler.courier_refresh_monthly_kpis.enqueued", job_id=refresh_job.get("id"))
        except Exception as refresh_exc:
            log.warning("scheduler.courier_refresh_monthly_kpis.enqueue_failed", error=str(refresh_exc))
    except Exception as exc:
        log.error("scheduler.courier_verify_billing.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_bl_distribution_cache() -> None:
    """Nightly — incrementally sync BaseLinker Distribution order/package cache."""
    from datetime import date as _date, timedelta as _td

    lookback_days = max(1, int(settings.BASELINKER_DISTRIBUTION_SYNC_LOOKBACK_DAYS or 2))
    limit_orders = max(1, int(settings.BASELINKER_DISTRIBUTION_SYNC_LIMIT_ORDERS or 500))
    date_from = (_date.today() - _td(days=lookback_days)).isoformat()
    date_to = _date.today().isoformat()

    log.info("scheduler.bl_distribution_cache.start",
             lookback_days=lookback_days, limit_orders=limit_orders)
    try:
        await asyncio.to_thread(
            run_scheduled_job_type, "sync_bl_distribution_order_cache",
            {
                "date_confirmed_from": date_from, "date_confirmed_to": date_to,
                "include_packages": bool(settings.BASELINKER_DISTRIBUTION_SYNC_INCLUDE_PACKAGES),
                "limit_orders": limit_orders,
            },
        )
        log.info("scheduler.bl_distribution_cache.done", date_from=date_from, date_to=date_to)
    except Exception as exc:
        log.error("scheduler.bl_distribution_cache.error", error=str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    if settings.GLS_LOGISTICS_SYNC_ENABLED:
        scheduler.add_job(
            _sync_gls_logistics_pipeline,
            trigger=CronTrigger(
                hour=int(settings.GLS_LOGISTICS_SYNC_HOUR),
                minute=int(settings.GLS_LOGISTICS_SYNC_MINUTE),
            ),
            id="sync-gls-logistics-nightly",
            name=f"Sync GLS Logistics ({int(settings.GLS_LOGISTICS_SYNC_HOUR):02d}:{int(settings.GLS_LOGISTICS_SYNC_MINUTE):02d})",
            replace_existing=True, max_instances=1, misfire_grace_time=900,
        )
    else:
        log.info("scheduler.sync_gls_logistics.disabled")

    if settings.DHL_LOGISTICS_SYNC_ENABLED:
        scheduler.add_job(
            _sync_dhl_logistics_pipeline,
            trigger=CronTrigger(
                hour=int(settings.DHL_LOGISTICS_SYNC_HOUR),
                minute=int(settings.DHL_LOGISTICS_SYNC_MINUTE),
            ),
            id="sync-dhl-logistics-nightly",
            name=f"Sync DHL Logistics ({int(settings.DHL_LOGISTICS_SYNC_HOUR):02d}:{int(settings.DHL_LOGISTICS_SYNC_MINUTE):02d})",
            replace_existing=True, max_instances=1, misfire_grace_time=900,
        )
    else:
        log.info("scheduler.sync_dhl_logistics.disabled")

    if settings.COURIER_ESTIMATION_SYNC_ENABLED:
        scheduler.add_job(
            _run_courier_estimation_pipeline,
            trigger=CronTrigger(
                hour=int(settings.COURIER_ESTIMATION_SYNC_HOUR),
                minute=int(settings.COURIER_ESTIMATION_SYNC_MINUTE),
            ),
            id="courier-estimation-nightly",
            name=f"Courier Estimation Pipeline ({int(settings.COURIER_ESTIMATION_SYNC_HOUR):02d}:{int(settings.COURIER_ESTIMATION_SYNC_MINUTE):02d})",
            replace_existing=True, max_instances=1, misfire_grace_time=900,
        )
    else:
        log.info("scheduler.courier_estimation_pipeline.disabled")

    if settings.COURIER_BILLING_VERIFY_ENABLED:
        scheduler.add_job(
            _verify_courier_billing_completeness,
            trigger=CronTrigger(
                hour=int(settings.COURIER_BILLING_VERIFY_HOUR),
                minute=int(settings.COURIER_BILLING_VERIFY_MINUTE),
            ),
            id="verify-courier-billing-daily",
            name=f"Verify Courier Billing ({int(settings.COURIER_BILLING_VERIFY_HOUR):02d}:{int(settings.COURIER_BILLING_VERIFY_MINUTE):02d})",
            replace_existing=True, max_instances=1, misfire_grace_time=900,
        )
    else:
        log.info("scheduler.verify_courier_billing.disabled")

    if settings.BASELINKER_DISTRIBUTION_SYNC_ENABLED:
        scheduler.add_job(
            _sync_bl_distribution_cache,
            trigger=CronTrigger(
                hour=int(settings.BASELINKER_DISTRIBUTION_SYNC_HOUR),
                minute=int(settings.BASELINKER_DISTRIBUTION_SYNC_MINUTE),
            ),
            id="sync-bl-distribution-cache-nightly",
            name=f"Sync BL Distribution Cache ({int(settings.BASELINKER_DISTRIBUTION_SYNC_HOUR):02d}:{int(settings.BASELINKER_DISTRIBUTION_SYNC_MINUTE):02d})",
            replace_existing=True, max_instances=1, misfire_grace_time=900,
        )
    else:
        log.info("scheduler.sync_bl_distribution_cache.disabled")
