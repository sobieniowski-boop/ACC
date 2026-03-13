"""Orders domain — order pipeline, listings sync, listing registry."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------

async def _order_pipeline() -> None:
    """Every 30 min — 5-step pipeline (sync orders, backfill, link, map, stamp)."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "order_pipeline", {"days_back": 1})
        return
    job_id = create_job_record("sync_orders")
    log.info("scheduler.order_pipeline.start", job_id=job_id)
    try:
        from app.services.order_pipeline import run_order_pipeline
        result = await run_order_pipeline(days_back=1)
        set_job_success(
            job_id,
            records_processed=result.get("sync_orders", {}).get("orders", 0),
            message=str(result),
        )
        log.info("scheduler.order_pipeline.done", result=result)
    except Exception as exc:
        log.error("scheduler.order_pipeline.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_listings_to_products() -> None:
    """01:00 — fetch GET_MERCHANT_LISTINGS_ALL_DATA for all marketplaces → upsert acc_product."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_listings_to_products", {})
        return
    job_id = create_job_record("sync_listings_to_products")
    log.info("scheduler.sync_listings_to_products.start", job_id=job_id)
    try:
        from app.services.sync_listings_to_products import sync_listings_to_products

        result = await sync_listings_to_products(job_id=job_id)
        totals = result.get("totals", {})
        set_job_success(
            job_id,
            records_processed=totals.get("created", 0) + totals.get("enriched", 0),
            message=f"created={totals.get('created',0)}, enriched={totals.get('enriched',0)}, errors={totals.get('errors',0)}",
        )
        log.info("scheduler.sync_listings_to_products.done", **totals)
    except Exception as exc:
        log.error("scheduler.sync_listings_to_products.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_amazon_listing_registry() -> None:
    """01:30 - sync Google Sheet Amazon listing registry to ACC staging."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_amazon_listing_registry", {})
        return
    job_id = create_job_record("sync_amazon_listing_registry")
    log.info("scheduler.sync_amazon_listing_registry.start", job_id=job_id)
    try:
        from app.services.amazon_listing_registry import sync_amazon_listing_registry

        result = await asyncio.to_thread(sync_amazon_listing_registry, False, job_id)
        set_job_success(
            job_id,
            records_processed=int(result.get("row_count", 0) or 0),
            message=f"Amazon listing registry {result.get('status')} rows={result.get('row_count', 0)}",
        )
        log.info("scheduler.sync_amazon_listing_registry.done", **result)
    except Exception as exc:
        log.error("scheduler.sync_amazon_listing_registry.error", error=str(exc))
        set_job_failure(job_id, str(exc))


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _order_pipeline,
        trigger=IntervalTrigger(minutes=30),
        id="order-pipeline-30m",
        name="Order Pipeline (30 min, real-time via SQS)",
        replace_existing=True, max_instances=1, misfire_grace_time=120,
    )
    scheduler.add_job(
        _sync_listings_to_products,
        trigger=CronTrigger(hour=1, minute=0),
        id="sync-listings-to-products-daily",
        name="Sync Listings → Products (01:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _sync_amazon_listing_registry,
        trigger=CronTrigger(hour=1, minute=30),
        id="sync-amazon-listing-registry",
        name="Sync Amazon Listing Registry (01:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
