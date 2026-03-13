from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta

from app.services.gls_billing_import import seed_gls_shipments_from_staging
from app.services.gls_cost_sync import sync_gls_shipment_costs
from app.services.gls_logistics_aggregation import (
    aggregate_gls_order_logistics,
    build_gls_logistics_shadow,
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _dump(label: str, payload: dict) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    sys.stdout.flush()


def _month_windows(start: date, end: date) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        if cursor.month == 12:
            next_month = date(cursor.year + 1, 1, 1)
        else:
            next_month = date(cursor.year, cursor.month + 1, 1)
        window_start = max(start, cursor)
        window_end = min(end, next_month - timedelta(days=1))
        if window_start <= window_end:
            windows.append((window_start, window_end))
        cursor = next_month
    return windows


def _day_windows(start: date, end: date, days: int) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    safe_days = max(1, int(days))
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(days=safe_days - 1))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def main() -> int:
    parser = argparse.ArgumentParser(description="Run GLS seed/cost/aggregate/shadow from current staging.")
    parser.add_argument("--created-from", required=True)
    parser.add_argument("--created-to", required=True)
    parser.add_argument("--purchase-from", required=True)
    parser.add_argument("--purchase-to", required=True)
    parser.add_argument("--limit-shipments", type=int, default=300000)
    parser.add_argument("--limit-orders", type=int, default=300000)
    parser.add_argument("--limit-parcels", type=int, default=300000)
    parser.add_argument("--batch-days", type=int, default=0)
    parser.add_argument("--refresh-existing", action="store_true", default=False)
    parser.add_argument("--monthly-batches", action="store_true", default=False)
    args = parser.parse_args()

    created_from = _parse_date(args.created_from)
    created_to = _parse_date(args.created_to)
    purchase_from = _parse_date(args.purchase_from)
    purchase_to = _parse_date(args.purchase_to)

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting GLS staging pipeline")
    print(
        json.dumps(
            {
                "created_from": created_from.isoformat(),
                "created_to": created_to.isoformat(),
                "purchase_from": purchase_from.isoformat(),
                "purchase_to": purchase_to.isoformat(),
                "limit_shipments": args.limit_shipments,
                "limit_orders": args.limit_orders,
                "limit_parcels": args.limit_parcels,
                "batch_days": args.batch_days,
                "refresh_existing": args.refresh_existing,
                "monthly_batches": args.monthly_batches,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    sys.stdout.flush()

    started = time.time()
    if args.batch_days and args.batch_days > 0:
        windows = _day_windows(created_from, created_to, args.batch_days)
    elif args.monthly_batches:
        windows = _month_windows(created_from, created_to)
    else:
        windows = [(created_from, created_to)]

    total_seed: dict[str, int] = {}
    total_cost: dict[str, int] = {}
    total_aggregate: dict[str, int] = {}

    for window_start, window_end in windows:
        seed_result = seed_gls_shipments_from_staging(
            created_from=window_start,
            created_to=window_end,
            seed_all_existing=False,
            limit_parcels=args.limit_parcels,
        )
        _dump(f"seed_shipments {window_start}..{window_end}", seed_result)
        for key, value in seed_result.items():
            if isinstance(value, bool):
                continue
            try:
                total_seed[key] = total_seed.get(key, 0) + int(value or 0)
            except Exception:
                continue

        cost_result = sync_gls_shipment_costs(
            created_from=window_start,
            created_to=window_end,
            limit_shipments=args.limit_shipments,
            refresh_existing=args.refresh_existing,
        )
        _dump(f"sync_costs {window_start}..{window_end}", cost_result)
        for key, value in cost_result.items():
            try:
                total_cost[key] = total_cost.get(key, 0) + int(value or 0)
            except Exception:
                continue

        aggregate_result = aggregate_gls_order_logistics(
            created_from=window_start,
            created_to=window_end,
            limit_orders=args.limit_orders,
        )
        _dump(f"aggregate_logistics {window_start}..{window_end}", aggregate_result)
        for key, value in aggregate_result.items():
            try:
                total_aggregate[key] = total_aggregate.get(key, 0) + int(value or 0)
            except Exception:
                continue

    if len(windows) > 1:
        _dump("seed_shipments_total", total_seed)
        _dump("sync_costs_total", total_cost)
        _dump("aggregate_logistics_total", total_aggregate)

    shadow_result = build_gls_logistics_shadow(
        purchase_from=purchase_from,
        purchase_to=purchase_to,
        limit_orders=args.limit_orders,
    )
    _dump("shadow_logistics", shadow_result)

    duration = round(time.time() - started, 2)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Finished in {duration}s")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
