from __future__ import annotations

import json
from datetime import datetime, timezone

from app.core.db_connection import connect_acc


def _query_rows(sql: str) -> list[dict]:
    with connect_acc() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def build_report() -> dict:
    queue_depth = _query_rows(
        """
        SELECT job_type, status, COUNT(*) AS count_jobs
        FROM dbo.acc_al_jobs WITH (NOLOCK)
        WHERE status IN ('pending', 'running', 'retry_scheduled')
        GROUP BY job_type, status
        ORDER BY count_jobs DESC, job_type
        """
    )
    oldest_pending = _query_rows(
        """
        SELECT TOP 20
          CAST(id AS NVARCHAR(40)) AS id,
          job_type,
          status,
          created_at,
          DATEDIFF(minute, created_at, SYSUTCDATETIME()) AS age_min
        FROM dbo.acc_al_jobs WITH (NOLOCK)
        WHERE status='pending'
        ORDER BY created_at ASC
        """
    )
    failure_rate_1h = _query_rows(
        """
        WITH last_hour AS (
          SELECT job_type, status
          FROM dbo.acc_al_jobs WITH (NOLOCK)
          WHERE created_at >= DATEADD(hour, -1, SYSUTCDATETIME())
        )
        SELECT
          job_type,
          SUM(CASE WHEN status='failure' THEN 1 ELSE 0 END) AS failure_count,
          COUNT(*) AS total_count,
          CAST(100.0 * SUM(CASE WHEN status='failure' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(6,2)) AS failure_rate_pct
        FROM last_hour
        GROUP BY job_type
        ORDER BY failure_rate_pct DESC, total_count DESC
        """
    )
    runtime_stats = _query_rows(
        """
        SELECT TOP 30
          job_type,
          COUNT(*) AS completed_count,
          CAST(AVG(CAST(duration_seconds AS FLOAT)) AS DECIMAL(12,2)) AS avg_duration_sec,
          CAST(MAX(CAST(duration_seconds AS FLOAT)) AS DECIMAL(12,2)) AS max_duration_sec
        FROM dbo.acc_al_jobs WITH (NOLOCK)
        WHERE status='completed'
          AND finished_at >= DATEADD(day, -1, SYSUTCDATETIME())
        GROUP BY job_type
        ORDER BY completed_count DESC
        """
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "queue_depth": queue_depth,
        "oldest_pending": oldest_pending,
        "failure_rate_1h": failure_rate_1h,
        "runtime_stats_24h_completed": runtime_stats,
    }


def main() -> int:
    print(json.dumps(build_report(), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
