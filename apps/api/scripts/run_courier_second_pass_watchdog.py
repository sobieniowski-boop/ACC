from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.connectors.mssql import list_jobs

import run_courier_order_universe_supervisor as supervisor


@dataclass(frozen=True)
class ScopeWithCoverage:
    month: str
    carrier: str
    fact_coverage_pct: float


def _print_event(event: str, payload: dict) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False), flush=True)


def _load_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {"runs": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"runs": []}


def _active_courier_jobs_count() -> int:
    items = list_jobs(job_type="courier_order_universe_linking", page=1, page_size=50).get("items", [])
    active = [
        item
        for item in items
        if str(item.get("status") or "").lower() in {"pending", "running", "retrying"}
    ]
    return len(active)


def _month_key(token: str) -> tuple[int, int]:
    y, m = token.split("-", 1)
    return int(y), int(m)


def _extract_low_coverage_scopes(checkpoint: dict, threshold_pct: float) -> list[ScopeWithCoverage]:
    out: list[ScopeWithCoverage] = []
    for run in checkpoint.get("runs", []):
        scope = run.get("scope") or {}
        coverage = run.get("coverage") or {}
        month = str(scope.get("month") or "").strip()
        carrier = str(scope.get("carrier") or "").strip().upper()
        if not month or carrier not in {"DHL", "GLS"}:
            continue
        pct = float(coverage.get("fact_coverage_pct") or 0.0)
        if pct < threshold_pct:
            out.append(ScopeWithCoverage(month=month, carrier=carrier, fact_coverage_pct=pct))
    out.sort(key=lambda item: (_month_key(item.month), item.carrier), reverse=True)
    return out


def _wait_primary_finish(primary_checkpoint: Path, expected_scopes: int, poll_sec: int) -> dict:
    while True:
        checkpoint = _load_checkpoint(primary_checkpoint)
        runs_count = len(checkpoint.get("runs", []))
        active_jobs = _active_courier_jobs_count()
        _print_event(
            "watch_status",
            {
                "primary_checkpoint": str(primary_checkpoint),
                "runs_in_checkpoint": runs_count,
                "expected_scopes": expected_scopes,
                "active_courier_jobs": active_jobs,
            },
        )
        if runs_count >= expected_scopes and active_jobs == 0:
            return checkpoint
        time.sleep(max(5, int(poll_sec)))


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for pass-1 completion and run selective pass-2.")
    parser.add_argument("--primary-checkpoint", required=True)
    parser.add_argument("--pass2-checkpoint", required=True)
    parser.add_argument("--expected-scopes", type=int, required=True)
    parser.add_argument("--fact-coverage-threshold-pct", type=float, default=85.0)
    parser.add_argument("--poll-sec", type=int, default=30)
    parser.add_argument("--limit-orders", type=int, default=3_000_000)
    parser.add_argument("--created-to-buffer-days", type=int, default=31)
    parser.add_argument("--stale-timeout-sec", type=int, default=1200)
    parser.add_argument("--hard-timeout-sec", type=int, default=21600)
    parser.add_argument("--transient-retries", type=int, default=6)
    args = parser.parse_args()

    primary_checkpoint = Path(args.primary_checkpoint)
    pass2_checkpoint = Path(args.pass2_checkpoint)
    pass2_checkpoint.parent.mkdir(parents=True, exist_ok=True)

    _print_event(
        "watch_start",
        {
            "primary_checkpoint": str(primary_checkpoint),
            "pass2_checkpoint": str(pass2_checkpoint),
            "expected_scopes": int(args.expected_scopes),
            "fact_coverage_threshold_pct": float(args.fact_coverage_threshold_pct),
        },
    )
    primary_data = _wait_primary_finish(
        primary_checkpoint=primary_checkpoint,
        expected_scopes=max(1, int(args.expected_scopes)),
        poll_sec=max(5, int(args.poll_sec)),
    )
    low_scopes = _extract_low_coverage_scopes(primary_data, float(args.fact_coverage_threshold_pct))
    _print_event(
        "pass2_plan",
        {
            "low_coverage_scopes": len(low_scopes),
            "scopes": [
                {"month": item.month, "carrier": item.carrier, "fact_coverage_pct": item.fact_coverage_pct}
                for item in low_scopes
            ],
        },
    )

    report: dict[str, list[dict]] = {"runs": []}
    for item in low_scopes:
        result = supervisor._run_scope(
            scope=supervisor.Scope(month=item.month, carrier=item.carrier),
            limit_orders=max(1, int(args.limit_orders)),
            created_to_buffer_days=max(0, int(args.created_to_buffer_days)),
            stale_timeout_sec=max(60, int(args.stale_timeout_sec)),
            hard_timeout_sec=max(300, int(args.hard_timeout_sec)),
            transient_retries=max(0, int(args.transient_retries)),
        )
        report["runs"].append(result)
        pass2_checkpoint.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    _print_event(
        "pass2_done",
        {
            "scopes_rerun": len(report["runs"]),
            "pass2_checkpoint": str(pass2_checkpoint),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
