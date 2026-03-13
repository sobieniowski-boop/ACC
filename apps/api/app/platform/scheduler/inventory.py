"""Inventory & FBA domain — inventory, sales traffic, FBA inventory/inbound/reconciliation/alerts, returns."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


async def _sync_inventory() -> None:
    """04:00 — SP-API inventory snapshots."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_inventory", {})
        return
    job_id = create_job_record("sync_inventory")
    log.info("scheduler.sync_inventory.start", job_id=job_id)
    try:
        from app.services.sync_service import sync_inventory
        count = await sync_inventory(job_id=job_id)
        set_job_success(job_id, records_processed=count)
        log.info("scheduler.sync_inventory.done", count=count)
    except Exception as exc:
        log.error("scheduler.sync_inventory.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_sales_traffic() -> None:
    """04:30 — SP-API sales & traffic report -> rollup."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(
            run_scheduled_job_type, "inventory_sync_sales_traffic", {"days_back": 90},
        )
        return
    job_id = create_job_record("inventory_sync_sales_traffic", params={"days_back": 90})
    log.info("scheduler.sync_sales_traffic.start", job_id=job_id)
    try:
        from app.services.manage_inventory import sync_inventory_sales_traffic
        result = await asyncio.to_thread(
            sync_inventory_sales_traffic, days_back=90, job_id=job_id,
        )
        rows = result.get("rows", 0) + result.get("asin_rows", 0)
        set_job_success(
            job_id, records_processed=rows,
            message=f"sku={result.get('rows',0)} asin={result.get('asin_rows',0)}",
        )
        log.info("scheduler.sync_sales_traffic.done", result=result)
    except Exception as exc:
        log.error("scheduler.sync_sales_traffic.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_fba_inventory() -> None:
    """Every 8h — build canonical FBA inventory snapshot from planning + stranded feeds."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_fba_inventory", {})
        return
    job_id = create_job_record("sync_fba_inventory")
    log.info("scheduler.sync_fba_inventory.start", job_id=job_id)
    try:
        from app.services.fba_ops import sync_inventory_cache
        result = await asyncio.to_thread(sync_inventory_cache, return_meta=True)
        rows = int(result.get("rows", 0))
        diag = str(result.get("report_diagnostics_summary") or "reports=n/a")
        set_job_success(job_id, records_processed=rows, message=f"fba_inventory_rows={rows}; {diag}")
        log.info("scheduler.sync_fba_inventory.done", rows=rows, report_diagnostics=diag)
    except Exception as exc:
        log.error("scheduler.sync_fba_inventory.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_fba_inbound() -> None:
    """Every 2h — refresh inbound shipment headers and line items from SP-API."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_fba_inbound", {})
        return
    job_id = create_job_record("sync_fba_inbound")
    log.info("scheduler.sync_fba_inbound.start", job_id=job_id)
    try:
        from app.services.fba_ops import sync_inbound_stub
        rows = await asyncio.to_thread(sync_inbound_stub)
        set_job_success(job_id, records_processed=rows, message=f"fba_inbound_rows={rows}")
        log.info("scheduler.sync_fba_inbound.done", rows=rows)
    except Exception as exc:
        log.error("scheduler.sync_fba_inbound.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_fba_reconciliation() -> None:
    """Daily — receiving reconciliation + auto-fill shipment plan actuals."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_fba_reconciliation", {})
        return
    job_id = create_job_record("sync_fba_reconciliation")
    log.info("scheduler.sync_fba_reconciliation.start", job_id=job_id)
    try:
        from app.services.fba_ops import sync_receiving_reconciliation, auto_fill_shipment_plan_actuals
        recon_rows = await asyncio.to_thread(sync_receiving_reconciliation)
        plan_rows = await asyncio.to_thread(auto_fill_shipment_plan_actuals)
        total = recon_rows + plan_rows
        set_job_success(
            job_id, records_processed=total,
            message=f"reconciliation={recon_rows}, plan_autofill={plan_rows}",
        )
        log.info("scheduler.sync_fba_reconciliation.done", reconciliation=recon_rows, plan_autofill=plan_rows)
    except Exception as exc:
        log.error("scheduler.sync_fba_reconciliation.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _run_fba_alerts() -> None:
    """Every 2h — evaluate FBA stockout, inbound, aging and stranded rules."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "run_fba_alerts", {})
        return
    job_id = create_job_record("run_fba_alerts")
    log.info("scheduler.run_fba_alerts.start", job_id=job_id)
    try:
        from app.services.fba_ops import run_alert_scan
        created = await asyncio.to_thread(run_alert_scan)
        set_job_success(job_id, records_processed=created, message=f"fba_alerts_created={created}")
        log.info("scheduler.run_fba_alerts.done", created=created)
    except Exception as exc:
        log.error("scheduler.run_fba_alerts.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _return_tracker_pipeline() -> None:
    """06:30 — FBA returns sync: fetch reports, seed refunds, reconcile, rebuild summary."""
    job_id = create_job_record("return_tracker")
    log.info("scheduler.return_tracker.start", job_id=job_id)
    try:
        from app.services.return_tracker import sync_fba_returns
        result = await sync_fba_returns(days_back=30, use_watermark=True)
        totals = result.get("totals", {})
        reconcile = result.get("reconcile", {})
        log.info(
            "scheduler.return_tracker.done", job_id=job_id,
            reports=totals.get("reports", 0), rows=totals.get("rows", 0),
            errors=totals.get("errors", 0), matched=reconcile.get("matched", 0),
            sellable=reconcile.get("sellable", 0), damaged=reconcile.get("damaged", 0),
        )
        set_job_success(job_id)
    except Exception as exc:
        log.error("scheduler.return_tracker.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _sync_inventory,
        trigger=CronTrigger(hour=4, minute=0),
        id="sync-inventory-daily",
        name="Sync Inventory (04:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _sync_sales_traffic,
        trigger=CronTrigger(hour=4, minute=30),
        id="sync-sales-traffic-daily",
        name="Sync Sales Traffic (04:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _sync_fba_inventory,
        trigger=IntervalTrigger(hours=8),
        id="sync-fba-inventory-8h",
        name="Sync FBA Inventory (8h, real-time via SQS)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _sync_fba_inbound,
        trigger=IntervalTrigger(hours=2),
        id="sync-fba-inbound-2h",
        name="Sync FBA Inbound (2h)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _sync_fba_reconciliation,
        trigger=CronTrigger(hour=6, minute=0),
        id="sync-fba-reconciliation-daily",
        name="FBA Reconciliation + Plan Auto-fill (06:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _run_fba_alerts,
        trigger=IntervalTrigger(hours=2, start_date=datetime.now(timezone.utc)),
        id="run-fba-alerts-2h",
        name="Run FBA Alerts (2h)",
        replace_existing=True, max_instances=1, misfire_grace_time=180,
    )
    scheduler.add_job(
        _return_tracker_pipeline,
        trigger=CronTrigger(hour=6, minute=30),
        id="return-tracker-daily",
        name="Return Tracker Pipeline (06:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
