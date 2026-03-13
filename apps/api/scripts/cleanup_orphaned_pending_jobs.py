from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from app.core.db_connection import connect_acc


def cleanup(minutes_old: int, dry_run: bool) -> dict:
    sql_candidates = """
    SELECT
      CAST(id AS NVARCHAR(40)) AS id,
      job_type,
      status,
      trigger_source,
      triggered_by,
      created_at,
      started_at
    FROM dbo.acc_al_jobs WITH (NOLOCK)
    WHERE status = 'pending'
      AND started_at IS NULL
      AND created_at < DATEADD(minute, -?, SYSUTCDATETIME())
    ORDER BY created_at ASC
    """
    with connect_acc() as conn:
        cur = conn.cursor()
        cur.execute(sql_candidates, (max(1, int(minutes_old)),))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        if not dry_run and rows:
            ids = [r["id"] for r in rows]
            placeholders = ", ".join(["CAST(? AS UNIQUEIDENTIFIER)"] * len(ids))
            cur.execute(
                f"""
                UPDATE dbo.acc_al_jobs
                SET
                  status = 'failure',
                  progress_pct = 100,
                  progress_message = 'Failed',
                  error_message = 'orphaned_pending_pre_cutover',
                  finished_at = SYSUTCDATETIME(),
                  duration_seconds = CASE
                    WHEN started_at IS NULL THEN NULL
                    ELSE DATEDIFF(second, started_at, SYSUTCDATETIME())
                  END
                WHERE id IN ({placeholders})
                """,
                ids,
            )
            conn.commit()
    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "minutes_old": minutes_old,
        "dry_run": dry_run,
        "candidates": len(rows),
        "jobs": rows[:100],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Mark orphaned pending jobs as failure before worker cutover.")
    parser.add_argument("--minutes-old", type=int, default=30)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = cleanup(minutes_old=max(1, int(args.minutes_old)), dry_run=not args.apply)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
