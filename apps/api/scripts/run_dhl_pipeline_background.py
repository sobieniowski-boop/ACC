from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

from app.services.dhl_billing_import import import_dhl_billing_files
from app.services.dhl_cost_sync import sync_dhl_shipment_costs
from app.services.dhl_logistics_aggregation import (
    aggregate_dhl_order_logistics,
    build_dhl_logistics_shadow,
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _dump(label: str, payload: dict) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    sys.stdout.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DHL billing import + cost/shadow pipeline.")
    parser.add_argument("--invoice-root", required=True)
    parser.add_argument("--jj-root", required=True)
    parser.add_argument("--manifest-path", default="")
    parser.add_argument("--created-from", required=True)
    parser.add_argument("--created-to", required=True)
    parser.add_argument("--purchase-from", required=True)
    parser.add_argument("--purchase-to", required=True)
    parser.add_argument("--limit-shipments", type=int, default=50000)
    parser.add_argument("--limit-orders", type=int, default=50000)
    parser.add_argument("--include-shipment-seed", action="store_true", default=True)
    parser.add_argument("--seed-all-existing", action="store_true", default=False)
    parser.add_argument("--force-reimport", action="store_true", default=False)
    args = parser.parse_args()

    invoice_root = str(Path(args.invoice_root))
    jj_root = str(Path(args.jj_root))
    manifest_path = str(Path(args.manifest_path)) if args.manifest_path else None
    created_from = _parse_date(args.created_from)
    created_to = _parse_date(args.created_to)
    purchase_from = _parse_date(args.purchase_from)
    purchase_to = _parse_date(args.purchase_to)

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting DHL pipeline")
    print(
        json.dumps(
            {
                "invoice_root": invoice_root,
                "jj_root": jj_root,
                "manifest_path": manifest_path,
                "created_from": created_from.isoformat(),
                "created_to": created_to.isoformat(),
                "purchase_from": purchase_from.isoformat(),
                "purchase_to": purchase_to.isoformat(),
                "limit_shipments": args.limit_shipments,
                "limit_orders": args.limit_orders,
                "seed_all_existing": args.seed_all_existing,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    sys.stdout.flush()

    started = time.time()
    import_result = import_dhl_billing_files(
        invoice_root=invoice_root,
        jj_root=jj_root,
        manifest_path=manifest_path,
        include_shipment_seed=args.include_shipment_seed,
        seed_all_existing=args.seed_all_existing,
        force_reimport=args.force_reimport,
    )
    _dump("import_billing_files", import_result)

    cost_result = sync_dhl_shipment_costs(
        created_from=created_from,
        created_to=created_to,
        limit_shipments=args.limit_shipments,
        allow_estimated=True,
        refresh_existing=False,
    )
    _dump("sync_costs", cost_result)

    aggregate_result = aggregate_dhl_order_logistics(
        created_from=created_from,
        created_to=created_to,
        limit_orders=args.limit_orders,
    )
    _dump("aggregate_logistics", aggregate_result)

    shadow_result = build_dhl_logistics_shadow(
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
