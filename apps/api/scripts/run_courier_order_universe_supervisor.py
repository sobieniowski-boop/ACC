from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.connectors.mssql import enqueue_job, get_job, set_job_failure
from app.services.courier_order_universe_pipeline import _coverage_snapshot


@dataclass(frozen=True)
class Scope:
    month: str
    carrier: str


def _month_bounds(token: str) -> tuple[date, date]:
    year_str, month_str = token.split("-", 1)
    start = date(int(year_str), int(month_str), 1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start, end


def _print_event(event: str, payload: dict) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))


def _monitor_job(
    *,
    job_id: str,
    stale_timeout_sec: int,
    hard_timeout_sec: int,
    poll_sec: int = 10,
) -> dict:
    started = time.time()
    last_change = started
    last_key = None
    while True:
        job = get_job(job_id) or {}
        status = str(job.get("status") or "").lower()
        progress_pct = int(job.get("progress_pct") or 0)
        progress_message = str(job.get("progress_message") or "")
        records_processed = int(job.get("records_processed") or 0)
        key = (status, progress_pct, progress_message, records_processed)
        if key != last_key:
            last_key = key
            last_change = time.time()
            _print_event(
                "job_progress",
                {
                    "job_id": job_id,
                    "status": status,
                    "progress_pct": progress_pct,
                    "records_processed": records_processed,
                    "progress_message": progress_message,
                },
            )
        if status in {"completed", "failure"}:
            return job
        if (time.time() - last_change) > stale_timeout_sec:
            set_job_failure(
                job_id,
                f"Supervisor stale-timeout after {stale_timeout_sec}s without progress change",
                allow_retry=False,
            )
            return get_job(job_id) or {}
        if (time.time() - started) > hard_timeout_sec:
            set_job_failure(
                job_id,
                f"Supervisor hard-timeout after {hard_timeout_sec}s",
                allow_retry=False,
            )
            return get_job(job_id) or {}
        time.sleep(max(1, poll_sec))


def _is_transient_failure(error_message: str) -> bool:
    text = str(error_message or "").lower()
    transient_tokens = (
        "dbprocess is dead",
        "connection timed out",
        "adaptive server connection timed out",
        "transport-level error",
        "connection reset",
        "connection aborted",
        "timeout",
    )
    return any(token in text for token in transient_tokens)


def _run_scope(
    *,
    scope: Scope,
    limit_orders: int,
    created_to_buffer_days: int,
    stale_timeout_sec: int,
    hard_timeout_sec: int,
    transient_retries: int,
) -> dict:
    _print_event("scope_start", {"month": scope.month, "carrier": scope.carrier})
    attempts = max(0, int(transient_retries)) + 1
    final_job: dict = {}
    job_id = ""
    for attempt in range(1, attempts + 1):
        job = enqueue_job(
            job_type="courier_order_universe_linking",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by="supervisor",
            params={
                "months": [scope.month],
                "carriers": [scope.carrier],
                "run_aggregate_shadow": True,
                "reset_existing_in_scope": False,
                "limit_orders": int(limit_orders),
                "created_to_buffer_days": max(0, int(created_to_buffer_days)),
            },
        )
        job_id = str(job.get("id") or "")
        _print_event(
            "scope_job_enqueued",
            {"month": scope.month, "carrier": scope.carrier, "job_id": job_id, "attempt": attempt, "max_attempts": attempts},
        )
        final_job = _monitor_job(
            job_id=job_id,
            stale_timeout_sec=stale_timeout_sec,
            hard_timeout_sec=hard_timeout_sec,
        )
        status = str(final_job.get("status") or "").lower()
        if status == "completed":
            break
        if attempt < attempts and _is_transient_failure(str(final_job.get("error_message") or "")):
            _print_event(
                "scope_retry_transient",
                {
                    "month": scope.month,
                    "carrier": scope.carrier,
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "error_message": str(final_job.get("error_message") or ""),
                },
            )
            time.sleep(10)
            continue
        break
    month_start, month_end = _month_bounds(scope.month)
    coverage = _coverage_snapshot(
        carrier=scope.carrier,
        purchase_from=month_start,
        purchase_to_exclusive=month_end,
    )
    result = {
        "scope": {"month": scope.month, "carrier": scope.carrier},
        "job": {
            "id": str(final_job.get("id") or job_id),
            "status": str(final_job.get("status") or ""),
            "progress_pct": int(final_job.get("progress_pct") or 0),
            "progress_message": str(final_job.get("progress_message") or ""),
            "error_message": str(final_job.get("error_message") or ""),
        },
        "coverage": coverage,
    }
    _print_event("scope_end", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Transparent supervisor for courier order-universe runs.")
    parser.add_argument("--months", nargs="+", default=["2025-11", "2025-12", "2026-01"])
    parser.add_argument("--carriers", nargs="+", default=["DHL", "GLS"])
    parser.add_argument("--limit-orders", type=int, default=3_000_000)
    parser.add_argument("--created-to-buffer-days", type=int, default=31)
    parser.add_argument("--stale-timeout-sec", type=int, default=900)
    parser.add_argument("--hard-timeout-sec", type=int, default=7200)
    parser.add_argument("--transient-retries", type=int, default=2)
    parser.add_argument("--stop-on-failure", action="store_true")
    parser.add_argument(
        "--checkpoint-file",
        default="C:/ACC/apps/api/scripts/courier_order_universe_supervisor_checkpoint.json",
    )
    args = parser.parse_args()

    scopes: list[Scope] = []
    for month in [str(item).strip() for item in args.months if str(item).strip()]:
        _month_bounds(month)
        for carrier in [str(item).strip().upper() for item in args.carriers if str(item).strip()]:
            if carrier not in {"DHL", "GLS"}:
                raise ValueError(f"Unsupported carrier '{carrier}'")
            scopes.append(Scope(month=month, carrier=carrier))

    checkpoint_path = Path(args.checkpoint_file)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    report: dict[str, list[dict]] = {"runs": []}

    for scope in scopes:
        result = _run_scope(
            scope=scope,
            limit_orders=max(1, int(args.limit_orders)),
            created_to_buffer_days=max(0, int(args.created_to_buffer_days)),
            stale_timeout_sec=max(60, int(args.stale_timeout_sec)),
            hard_timeout_sec=max(300, int(args.hard_timeout_sec)),
            transient_retries=max(0, int(args.transient_retries)),
        )
        report["runs"].append(result)
        checkpoint_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        status = str(result["job"]["status"]).lower()
        if args.stop_on_failure and status != "completed":
            _print_event("supervisor_stop_on_failure", {"failed_scope": result["scope"], "checkpoint_file": str(checkpoint_path)})
            break

    _print_event("supervisor_finished", {"checkpoint_file": str(checkpoint_path), "runs": len(report["runs"])})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
