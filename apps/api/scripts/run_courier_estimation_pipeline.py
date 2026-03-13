from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date

from app.services.courier_cost_estimation import (
    compute_courier_estimation_kpis,
    estimate_preinvoice_courier_costs,
    reconcile_estimated_costs,
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _dump(label: str, payload: dict) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    sys.stdout.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run courier preinvoice estimate + reconcile + KPI pipeline.")
    parser.add_argument("--carriers", nargs="+", default=["DHL", "GLS"])
    parser.add_argument("--created-from", default="")
    parser.add_argument("--created-to", default="")
    parser.add_argument("--horizon-days", type=int, default=180)
    parser.add_argument("--min-samples", type=int, default=10)
    parser.add_argument("--limit-shipments", type=int, default=20000)
    parser.add_argument("--refresh-existing", action="store_true", default=False)
    parser.add_argument("--kpi-days-back", type=int, default=30)
    args = parser.parse_args()

    carriers = [str(item).strip().upper() for item in args.carriers if str(item).strip()]
    created_from = _parse_date(args.created_from) if str(args.created_from or "").strip() else None
    created_to = _parse_date(args.created_to) if str(args.created_to or "").strip() else None

    estimate_result = estimate_preinvoice_courier_costs(
        carriers=carriers,
        created_from=created_from,
        created_to=created_to,
        horizon_days=max(30, int(args.horizon_days)),
        min_samples=max(1, int(args.min_samples)),
        limit_shipments=max(1, int(args.limit_shipments)),
        refresh_existing=bool(args.refresh_existing),
    )
    _dump("estimate_preinvoice", estimate_result)

    reconcile_result = reconcile_estimated_costs(
        carriers=carriers,
        limit_shipments=max(1, int(args.limit_shipments)),
    )
    _dump("reconcile_estimates", reconcile_result)

    kpi_result = compute_courier_estimation_kpis(
        days_back=max(1, int(args.kpi_days_back)),
        carriers=carriers,
    )
    _dump("compute_kpis", kpi_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
