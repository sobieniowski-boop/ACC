from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.courier_order_universe_linking import backfill_order_links_order_universe
from app.services.courier_order_universe_pipeline import _coverage_snapshot
from app.services.gls_logistics_aggregation import aggregate_gls_order_logistics, build_gls_logistics_shadow


def _dump_event(event: str, payload: dict) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))


def _week_ranges() -> list[tuple[date, date]]:
    return [
        (date(2025, 12, 1), date(2025, 12, 7)),
        (date(2025, 12, 8), date(2025, 12, 14)),
        (date(2025, 12, 15), date(2025, 12, 21)),
        (date(2025, 12, 22), date(2025, 12, 31)),
    ]


def main() -> int:
    checkpoint = Path("C:/ACC/apps/api/scripts/gls_dec2025_weekly_recovery_checkpoint.json")
    report: dict[str, list[dict]] = {"weeks": []}

    for start, end in _week_ranges():
        _dump_event("week_start", {"from": start.isoformat(), "to": end.isoformat()})
        week_started = time.time()
        try:
            link_result = backfill_order_links_order_universe(
                carrier="GLS",
                purchase_from=start,
                purchase_to=end,
                created_from=start,
                created_to=end,
                reset_existing_in_scope=False,
            )
            status = "completed"
            error = ""
        except Exception as exc:
            link_result = {}
            status = "failure"
            error = str(exc)

        duration = round(time.time() - week_started, 2)
        week_payload = {
            "from": start.isoformat(),
            "to": end.isoformat(),
            "status": status,
            "duration_sec": duration,
            "linking": link_result,
            "error": error,
        }
        report["weeks"].append(week_payload)
        checkpoint.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        _dump_event("week_end", week_payload)
        if status != "completed":
            _dump_event("stop_on_failure", {"failed_week": week_payload})
            return 1

    _dump_event("aggregate_start", {"carrier": "GLS", "from": "2025-12-01", "to": "2025-12-31"})
    agg = aggregate_gls_order_logistics(created_from=date(2025, 12, 1), created_to=date(2025, 12, 31), limit_orders=3_000_000)
    _dump_event("aggregate_end", agg)

    _dump_event("shadow_start", {"carrier": "GLS", "from": "2025-12-01", "to": "2025-12-31"})
    shadow = build_gls_logistics_shadow(purchase_from=date(2025, 12, 1), purchase_to=date(2025, 12, 31), limit_orders=3_000_000)
    _dump_event("shadow_end", shadow)

    cov = _coverage_snapshot(carrier="GLS", purchase_from=date(2025, 12, 1), purchase_to_exclusive=date(2026, 1, 1))
    _dump_event("coverage_end", {"month": "2025-12", "carrier": "GLS", "coverage": cov})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
