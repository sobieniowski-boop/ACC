"""Finance domain — purchase prices, ECB rates, finances, fee-gap, COGS import."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


async def _sync_purchase_prices() -> None:
    """02:00 — Holding FIFO + XLSX fallback."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_purchase_prices", {})
        return
    job_id = create_job_record("sync_purchase_prices")
    log.info("scheduler.sync_purchase_prices.start", job_id=job_id)
    try:
        from app.services.sync_service import sync_purchase_prices
        count = await sync_purchase_prices(job_id=job_id)
        log.info("scheduler.sync_purchase_prices.done", count=count)
    except Exception as exc:
        log.error("scheduler.sync_purchase_prices.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_ecb_exchange_rates() -> None:
    """02:30 — ECB exchange rates (backup source)."""
    log.info("scheduler.sync_ecb.start")
    try:
        from app.services.sync_service import sync_ecb_exchange_rates
        inserted = await sync_ecb_exchange_rates(days_back=90)
        log.info("scheduler.sync_ecb.done", inserted=inserted)
    except Exception as exc:
        log.error("scheduler.sync_ecb.error", error=str(exc))


async def _sync_finances() -> None:
    """03:00 — SP-API financial events (v2024-06-19 with dedup)."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_finances", {"days_back": 3})
        return
    job_id = create_job_record("sync_finances")
    log.info("scheduler.sync_finances.start", job_id=job_id)
    try:
        from app.services.order_pipeline import step_sync_finances
        result = await step_sync_finances(days_back=3, job_id=job_id)
        count = result.get("fee_rows", 0)
        set_job_success(job_id, records_processed=count)
        log.info("scheduler.sync_finances.done", count=count)

        # Emit domain event for downstream triggers (profitability chain)
        from app.services.event_backbone import emit_domain_event
        from datetime import date
        emit_domain_event(
            "finance", "synced",
            {"fee_rows": count, "days_back": 3},
            idempotency_key=f"finance_sync_{date.today().isoformat()}",
        )
    except Exception as exc:
        log.error("scheduler.sync_finances.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _recheck_fee_gaps() -> None:
    """03:20 - refresh fee-gap watchlist and probe Amazon for newly available events."""
    job_id = create_job_record("fee_gap_watch_recheck")
    log.info("scheduler.fee_gap_recheck.start", job_id=job_id)
    try:
        from datetime import date as _date, timedelta as _td
        from app.services.profit_engine import seed_fee_gap_watch, recheck_fee_gap_watch

        seed = await asyncio.to_thread(
            seed_fee_gap_watch,
            date_from=_date.today() - _td(days=30),
            date_to=_date.today(),
            marketplace_id=None,
        )
        recheck = await asyncio.to_thread(
            recheck_fee_gap_watch, limit=100, marketplace_id=None,
        )
        set_job_success(
            job_id,
            records_processed=int(recheck.get("checked", 0) or 0),
            message=(
                f"seeded_inserted={seed.get('inserted', 0)}, seeded_updated={seed.get('updated', 0)}, "
                f"checked={recheck.get('checked', 0)}, resolved={recheck.get('resolved', 0)}, "
                f"amazon_events_available={recheck.get('amazon_events_available', 0)}, "
                f"still_missing={recheck.get('still_missing', 0)}"
            ),
        )
        log.info("scheduler.fee_gap_recheck.done", seed=seed, recheck=recheck)
    except Exception as exc:
        log.error("scheduler.fee_gap_recheck.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _cogs_import_scan() -> None:
    """06:00 — scan 'cogs from sell' folder for new/changed XLSX files."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "cogs_import", {})
        return
    job_id = create_job_record("cogs_import")
    log.info("scheduler.cogs_import.start", job_id=job_id)
    try:
        from app.services.cogs_importer import scan_and_import
        result = await asyncio.to_thread(scan_and_import)
        processed = result.get("files_processed", 0)
        new_prices = result.get("total_new", 0)
        updated = result.get("total_updated", 0)
        set_job_success(
            job_id,
            records_processed=new_prices + updated,
            message=f"files={processed}, new={new_prices}, updated={updated}",
        )
        log.info("scheduler.cogs_import.done", **{k: v for k, v in result.items() if k != 'errors'})
    except Exception as exc:
        log.error("scheduler.cogs_import.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _sync_purchase_prices,
        trigger=CronTrigger(hour=2, minute=0),
        id="sync-purchase-prices-nightly",
        name="Sync Purchase Prices (02:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _sync_ecb_exchange_rates,
        trigger=CronTrigger(hour=2, minute=30),
        id="sync-ecb-exchange-rates-daily",
        name="Sync ECB Exchange Rates (02:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _sync_finances,
        trigger=CronTrigger(hour=3, minute=0),
        id="sync-finances-daily",
        name="Sync Finances (03:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _recheck_fee_gaps,
        trigger=CronTrigger(hour=3, minute=20),
        id="fee-gap-recheck-daily",
        name="Fee Gap Recheck (03:20)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _cogs_import_scan,
        trigger=CronTrigger(hour=6, minute=0),
        id="cogs-import-daily",
        name="COGS Import Scan (daily 06:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
