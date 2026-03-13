from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.connectors.mssql.mssql_store import list_jobs


def running_courier_jobs() -> list[dict]:
    items = list_jobs(job_type="courier_order_universe_linking", page=1, page_size=50).get("items", [])
    return [j for j in items if str(j.get("status") or "").lower() == "running"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch courier jobs until idle.")
    parser.add_argument("--poll-sec", type=int, default=30)
    parser.add_argument("--max-minutes", type=int, default=720)
    args = parser.parse_args()

    started = time.time()
    deadline = started + (args.max_minutes * 60)

    while True:
        now = datetime.now(timezone.utc).isoformat()
        running = running_courier_jobs()
        if not running:
            print(f"[{now}] COURIER_IDLE=1")
            return
        first = running[0]
        print(
            f"[{now}] COURIER_IDLE=0 running={len(running)} "
            f"job_id={first.get('id')} progress={first.get('progress_pct')} "
            f"msg={first.get('progress_message')}"
        )
        if time.time() > deadline:
            print(f"[{now}] TIMEOUT max_minutes={args.max_minutes}")
            return
        time.sleep(max(5, int(args.poll_sec)))


if __name__ == "__main__":
    main()
