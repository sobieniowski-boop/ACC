from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from app.services.gls_billing_import import import_gls_billing_files
from app.services.gls_cost_sync import sync_gls_shipment_costs
from app.services.gls_logistics_aggregation import (
    aggregate_gls_order_logistics,
    build_gls_logistics_shadow,
)


def _dump(label: str, payload: dict) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    sys.stdout.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run GLS billing import + cost/shadow pipeline.")
    parser.add_argument("--invoice-root", required=True)
    parser.add_argument("--bl-map-path", required=True)
    parser.add_argument("--limit-invoice-files", type=int, default=None)
    parser.add_argument("--limit-shipments", type=int, default=200000)
    parser.add_argument("--limit-orders", type=int, default=300000)
    parser.add_argument("--include-shipment-seed", action="store_true", default=True)
    parser.add_argument("--force-reimport", action="store_true", default=False)
    args = parser.parse_args()

    invoice_root = str(Path(args.invoice_root))
    bl_map_path = str(Path(args.bl_map_path))

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting GLS pipeline")
    print(
        json.dumps(
            {
                "invoice_root": invoice_root,
                "bl_map_path": bl_map_path,
                "limit_invoice_files": args.limit_invoice_files,
                "limit_shipments": args.limit_shipments,
                "limit_orders": args.limit_orders,
                "include_shipment_seed": args.include_shipment_seed,
                "force_reimport": args.force_reimport,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    sys.stdout.flush()

    started = time.time()
    import_result = import_gls_billing_files(
        invoice_root=invoice_root,
        bl_map_path=bl_map_path,
        include_shipment_seed=args.include_shipment_seed,
        force_reimport=args.force_reimport,
        limit_invoice_files=args.limit_invoice_files,
    )
    _dump("import_billing_files", import_result)

    cost_result = sync_gls_shipment_costs(
        created_from=None,
        created_to=None,
        limit_shipments=args.limit_shipments,
        refresh_existing=True,
    )
    _dump("sync_costs", cost_result)

    aggregate_result = aggregate_gls_order_logistics(
        created_from=None,
        created_to=None,
        limit_orders=args.limit_orders,
    )
    _dump("aggregate_logistics", aggregate_result)

    shadow_result = build_gls_logistics_shadow(
        purchase_from=None,
        purchase_to=None,
        limit_orders=args.limit_orders,
    )
    _dump("shadow_logistics", shadow_result)

    duration = round(time.time() - started, 2)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Finished in {duration}s")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
