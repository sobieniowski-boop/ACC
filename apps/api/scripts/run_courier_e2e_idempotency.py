from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from app.connectors.mssql import enqueue_job, get_job
from app.services.courier_readiness import get_courier_readiness_snapshot


def _wait_job(job_id: str, timeout_sec: int = 7200, poll_sec: int = 10) -> dict:
    started = time.time()
    while True:
        job = get_job(job_id) or {}
        status = str(job.get("status") or "").lower()
        if status in {"completed", "failure"}:
            return job
        if time.time() - started > timeout_sec:
            return job
        time.sleep(max(1, poll_sec))


def _run_once(*, months: list[str], carriers: list[str], limit_orders: int) -> dict:
    job = enqueue_job(
        job_type="courier_order_universe_linking",
        marketplace_id=None,
        trigger_source="manual",
        triggered_by="e2e-idempotency",
        params={
            "months": months,
            "carriers": carriers,
            "run_aggregate_shadow": True,
            "reset_existing_in_scope": False,
            "limit_orders": int(limit_orders),
        },
    )
    job_id = str(job.get("id") or "")
    final_job = _wait_job(job_id)
    return {
        "job_id": job_id,
        "status": str(final_job.get("status") or ""),
        "progress_pct": int(final_job.get("progress_pct") or 0),
        "progress_message": str(final_job.get("progress_message") or ""),
        "error_message": str(final_job.get("error_message") or ""),
        "records_processed": int(final_job.get("records_processed") or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run courier E2E idempotency check with artifacts.")
    parser.add_argument("--months", nargs="+", default=["2025-11", "2025-12", "2026-01"])
    parser.add_argument("--carriers", nargs="+", default=["DHL", "GLS"])
    parser.add_argument("--limit-orders", type=int, default=3_000_000)
    parser.add_argument(
        "--artifact-file",
        default=f"C:/ACC/apps/api/scripts/courier_e2e_artifact_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    args = parser.parse_args()

    months = [str(item).strip() for item in args.months if str(item).strip()]
    carriers = [str(item).strip().upper() for item in args.carriers if str(item).strip()]

    before = get_courier_readiness_snapshot(months=months, carriers=carriers)
    first = _run_once(months=months, carriers=carriers, limit_orders=max(1, int(args.limit_orders)))
    mid = get_courier_readiness_snapshot(months=months, carriers=carriers)
    second = _run_once(months=months, carriers=carriers, limit_orders=max(1, int(args.limit_orders)))
    after = get_courier_readiness_snapshot(months=months, carriers=carriers)

    artifact = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "months": months,
        "carriers": carriers,
        "before": before,
        "first_run": first,
        "after_first": mid,
        "second_run": second,
        "after_second": after,
        "idempotency_pass": (mid.get("matrix") == after.get("matrix")),
    }
    path = Path(args.artifact_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"artifact_file": str(path), "idempotency_pass": artifact["idempotency_pass"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
