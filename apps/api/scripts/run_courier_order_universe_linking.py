from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.courier_order_universe_pipeline import run_courier_order_universe_pipeline


def _print_event(event: str, payload: dict) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run order-universe-first courier linking and coverage snapshot.")
    parser.add_argument("--months", nargs="+", default=["2025-11", "2025-12", "2026-01"])
    parser.add_argument("--carriers", nargs="+", default=["DHL", "GLS"])
    parser.add_argument("--reset-existing-in-scope", action="store_true")
    parser.add_argument("--run-aggregate-shadow", action="store_true")
    parser.add_argument("--limit-orders", type=int, default=3_000_000)
    parser.add_argument("--created-to-buffer-days", type=int, default=31)
    parser.add_argument("--heartbeat-sec", type=int, default=30)
    args = parser.parse_args()

    started = time.time()
    stop_flag = threading.Event()

    def _heartbeat() -> None:
        while not stop_flag.wait(timeout=max(5, int(args.heartbeat_sec or 30))):
            _print_event(
                "heartbeat",
                {
                    "elapsed_sec": int(time.time() - started),
                    "months": [str(m) for m in args.months],
                    "carriers": [str(c).upper() for c in args.carriers],
                },
            )

    def _progress(message: str, completed_steps: int, total_steps: int) -> None:
        pct = int((completed_steps / max(total_steps, 1)) * 100)
        _print_event(
            "progress",
            {
                "message": message,
                "completed_steps": int(completed_steps),
                "total_steps": int(total_steps),
                "progress_pct": pct,
                "elapsed_sec": int(time.time() - started),
            },
        )

    _print_event(
        "run_start",
        {
            "months": [str(m) for m in args.months],
            "carriers": [str(c).upper() for c in args.carriers],
            "run_aggregate_shadow": bool(args.run_aggregate_shadow),
            "limit_orders": int(args.limit_orders or 1),
            "created_to_buffer_days": max(0, int(args.created_to_buffer_days or 0)),
        },
    )
    hb = threading.Thread(target=_heartbeat, daemon=True, name="courier-linking-heartbeat")
    hb.start()

    report = run_courier_order_universe_pipeline(
        months=args.months,
        carriers=args.carriers,
        reset_existing_in_scope=bool(args.reset_existing_in_scope),
        run_aggregate_shadow=bool(args.run_aggregate_shadow),
        limit_orders=max(1, int(args.limit_orders or 1)),
        created_to_buffer_days=max(0, int(args.created_to_buffer_days or 0)),
        progress_callback=_progress,
    )
    stop_flag.set()
    _print_event("run_done", {"elapsed_sec": int(time.time() - started)})
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
