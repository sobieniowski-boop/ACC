"""
Standalone Amazon SP-API sync runner.

Run from command line or Windows Task Scheduler — no Redis/Celery needed.

Usage:
    python -m app.sync_runner --all
    python -m app.sync_runner --orders --days-back 7
    python -m app.sync_runner --inventory
    python -m app.sync_runner --pricing
    python -m app.sync_runner --catalog
    python -m app.sync_runner --finances --days-back 14
    python -m app.sync_runner --exchange-rates --days-back 60
    python -m app.sync_runner --profit --days-back 1
    python -m app.sync_runner --traffic --days-back 90
    python -m app.sync_runner --marketplace A1PA6795UKMFR9 --orders

Windows Task Scheduler:
    Program:   C:\\ACC\\.venv\\Scripts\\python.exe
    Arguments: -m app.sync_runner --all
    Start in:  C:\\ACC\\apps\\api
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import os
import time

# Ensure correct sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
api_dir = os.path.dirname(script_dir)  # apps/api
if api_dir not in sys.path:
    sys.path.insert(0, api_dir)

import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
)

log = structlog.get_logger("sync_runner")


async def main():
    parser = argparse.ArgumentParser(
        description="Amazon SP-API → MSSQL sync runner"
    )
    parser.add_argument("--all", action="store_true",
                        help="Run full sync (all types)")
    parser.add_argument("--orders", action="store_true",
                        help="Sync orders + order items")
    parser.add_argument("--inventory", action="store_true",
                        help="Sync FBA inventory snapshots")
    parser.add_argument("--pricing", action="store_true",
                        help="Sync pricing & BuyBox data")
    parser.add_argument("--catalog", action="store_true",
                        help="Sync product catalog details")
    parser.add_argument("--finances", action="store_true",
                        help="Sync financial events")
    parser.add_argument("--exchange-rates", action="store_true",
                        help="Sync NBP exchange rates")
    parser.add_argument("--profit", action="store_true",
                        help="Recalculate profit margins")
    parser.add_argument("--traffic", action="store_true",
                        help="Sync sales & traffic reports + rollup")
    parser.add_argument("--ecb-rates", action="store_true",
                        help="Sync ECB exchange rates (backup)")
    parser.add_argument("--days-back", type=int, default=7,
                        help="Number of days to look back (default: 7)")
    parser.add_argument("--marketplace", type=str, default=None,
                        help="Marketplace ID to sync (default: all active)")

    args = parser.parse_args()

    # Load env from .env file
    env_path = os.path.join(os.path.dirname(api_dir), ".env")
    if os.path.exists(env_path):
        _load_dotenv(env_path)

    # Import sync service (after env is loaded)
    from app.services.sync_service import (
        sync_orders,
        sync_inventory,
        sync_pricing,
        sync_catalog,
        sync_exchange_rates,
        sync_ecb_exchange_rates,
        calc_profit,
        run_full_sync,
    )
    from app.services.order_pipeline import step_sync_finances
    from app.services.manage_inventory import sync_inventory_sales_traffic
    from app.connectors.mssql.mssql_store import (
        create_job,
        set_job_running,
        set_job_success,
        set_job_failure,
    )

    start_time = time.time()
    results = {}

    def _job(job_type: str) -> str:
        """Create a job record and mark it running. Returns job_id."""
        try:
            job = create_job(
                job_type=job_type,
                marketplace_id=args.marketplace,
                trigger_source="cli",
                triggered_by="sync_runner",
            )
            jid = job["id"]
            set_job_running(jid, f"Running via sync_runner --{job_type.replace('_', '-')}")
            return jid
        except Exception as exc:
            log.warning("sync_runner.create_job_failed", job_type=job_type, error=str(exc))
            return ""

    def _ok(job_id: str, records: int = 0):
        if job_id:
            set_job_success(job_id, records_processed=records)

    def _fail(job_id: str, exc: BaseException):
        if job_id:
            set_job_failure(job_id, exc)

    if args.all:
        jid = _job("full_sync")
        log.info("sync_runner.full_sync_start", days_back=args.days_back,
                 marketplace=args.marketplace)
        try:
            results = await run_full_sync(
                marketplace_id=args.marketplace,
                days_back=args.days_back,
            )
            _ok(jid, sum(v for v in results.values() if isinstance(v, int)))
        except Exception as exc:
            _fail(jid, exc)
            raise
    else:
        if args.exchange_rates:
            jid = _job("sync_exchange_rates")
            log.info("sync_runner.exchange_rates_start",
                     days_back=args.days_back)
            try:
                count = await sync_exchange_rates(days_back=args.days_back)
                results["exchange_rates"] = count
                _ok(jid, count)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.orders:
            jid = _job("sync_orders")
            log.info("sync_runner.orders_start", days_back=args.days_back,
                     marketplace=args.marketplace)
            try:
                count = await sync_orders(
                    marketplace_id=args.marketplace,
                    days_back=args.days_back,
                )
                results["orders"] = count
                _ok(jid, count)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.inventory:
            jid = _job("sync_inventory")
            log.info("sync_runner.inventory_start",
                     marketplace=args.marketplace)
            try:
                count = await sync_inventory(marketplace_id=args.marketplace)
                results["inventory"] = count
                _ok(jid, count)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.pricing:
            jid = _job("sync_pricing")
            log.info("sync_runner.pricing_start",
                     marketplace=args.marketplace)
            try:
                count = await sync_pricing(marketplace_id=args.marketplace)
                results["pricing"] = count
                _ok(jid, count)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.catalog:
            jid = _job("sync_catalog")
            log.info("sync_runner.catalog_start",
                     marketplace=args.marketplace)
            try:
                count = await sync_catalog(marketplace_id=args.marketplace)
                results["catalog"] = count
                _ok(jid, count)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.finances:
            jid = _job("sync_finances")
            log.info("sync_runner.finances_start",
                     days_back=args.days_back,
                     marketplace=args.marketplace)
            try:
                result = await step_sync_finances(
                    marketplace_id=args.marketplace,
                    days_back=args.days_back,
                )
                cnt = result.get("fee_rows", 0)
                results["finances"] = cnt
                _ok(jid, cnt)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.profit:
            jid = _job("calc_profit")
            log.info("sync_runner.profit_start",
                     days_back=args.days_back)
            try:
                count = await calc_profit(
                    days_back=args.days_back,
                    marketplace_id=args.marketplace,
                )
                results["profit"] = count
                _ok(jid, count)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.traffic:
            jid = _job("inventory_sync_sales_traffic")
            log.info("sync_runner.traffic_start",
                     days_back=args.days_back)
            try:
                import asyncio as _aio
                result = await _aio.to_thread(
                    sync_inventory_sales_traffic,
                    days_back=args.days_back,
                )
                cnt = result.get("rows", 0) + result.get("asin_rows", 0)
                results["traffic"] = cnt
                log.info("sync_runner.traffic_done", result=result)
                _ok(jid, cnt)
            except Exception as exc:
                _fail(jid, exc); raise

        if args.ecb_rates:
            jid = _job("sync_ecb_exchange_rates")
            log.info("sync_runner.ecb_rates_start",
                     days_back=args.days_back)
            try:
                count = await sync_ecb_exchange_rates(days_back=args.days_back)
                results["ecb_rates"] = count
                _ok(jid, count)
            except Exception as exc:
                _fail(jid, exc); raise

    elapsed = round(time.time() - start_time, 1)
    log.info("sync_runner.complete", elapsed_sec=elapsed, results=results)

    if not any([args.all, args.orders, args.inventory, args.pricing,
                args.catalog, args.finances, args.exchange_rates, args.profit,
                args.traffic, args.ecb_rates]):
        parser.print_help()


def _load_dotenv(path: str):
    """Simple .env file loader (no python-dotenv needed)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    # Remove surrounding quotes
                    if (value.startswith('"') and value.endswith('"')) or \
                       (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    os.environ.setdefault(key, value)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
