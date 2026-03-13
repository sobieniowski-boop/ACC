from __future__ import annotations

import json
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.api.v1.routes_health import _check_order_sync
from app.connectors.mssql.mssql_store import list_jobs
from app.core.db_connection import connect_acc
from app.services.executive_service import (
    get_exec_marketplaces,
    get_exec_overview,
    get_exec_products,
)
from app.services.profitability_service import (
    get_profitability_orders,
    get_profitability_overview,
    get_profitability_products,
)
from app.services.strategy_service import (
    get_opportunities_page,
    get_strategy_overview,
)


def _iso(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _bench(name, fn):
    t0 = time.perf_counter()
    out = fn()
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
    return {
        "name": name,
        "elapsed_ms": elapsed_ms,
        "ok": True,
        "meta": {
            "has_kpi": bool(isinstance(out, dict) and out.get("kpi")),
            "items": len(out.get("items", [])) if isinstance(out, dict) and isinstance(out.get("items"), list) else None,
            "total": out.get("total") if isinstance(out, dict) else None,
        },
    }


def _collect_jobs():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    items = list_jobs(page=1, page_size=300).get("items", [])
    counts = Counter(j.get("status") for j in items)
    running = []
    pending = []
    for j in items:
        status = j.get("status")
        if status == "running":
            started = j.get("started_at") or j.get("created_at")
            age_min = round((now - started).total_seconds() / 60.0, 1) if started else None
            running.append(
                {
                    "id": j.get("id"),
                    "job_type": j.get("job_type"),
                    "progress_pct": j.get("progress_pct"),
                    "progress_message": j.get("progress_message"),
                    "age_min": age_min,
                }
            )
        elif status == "pending":
            created = j.get("created_at")
            age_min = round((now - created).total_seconds() / 60.0, 1) if created else None
            pending.append(
                {
                    "id": j.get("id"),
                    "job_type": j.get("job_type"),
                    "trigger_source": j.get("trigger_source"),
                    "age_min": age_min,
                }
            )
    pending = sorted(pending, key=lambda x: x.get("age_min") or -1, reverse=True)
    return {
        "counts": dict(counts),
        "running": running,
        "pending_top10_oldest": pending[:10],
    }


def _collect_sql():
    sql = {}
    with connect_acc(timeout=30) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            WITH s AS (
                SELECT TOP 40 end_time, avg_cpu_percent, avg_data_io_percent, avg_log_write_percent, avg_memory_usage_percent
                FROM sys.dm_db_resource_stats
                ORDER BY end_time DESC
            )
            SELECT
                AVG(CAST(avg_cpu_percent AS float)),
                MAX(CAST(avg_cpu_percent AS float)),
                AVG(CAST(avg_data_io_percent AS float)),
                MAX(CAST(avg_data_io_percent AS float)),
                AVG(CAST(avg_log_write_percent AS float)),
                MAX(CAST(avg_log_write_percent AS float)),
                AVG(CAST(avg_memory_usage_percent AS float))
            FROM s
            """
        )
        r = cur.fetchone()
        sql["resource_40samples"] = {
            "avg_cpu_pct": float(r[0] or 0),
            "max_cpu_pct": float(r[1] or 0),
            "avg_data_io_pct": float(r[2] or 0),
            "max_data_io_pct": float(r[3] or 0),
            "avg_log_io_pct": float(r[4] or 0),
            "max_log_io_pct": float(r[5] or 0),
            "avg_memory_pct": float(r[6] or 0),
        }

        cur.execute(
            """
            SELECT TOP 10
              CAST(qs.total_elapsed_time/1000.0 AS DECIMAL(18,1)) AS total_elapsed_ms,
              CAST((qs.total_elapsed_time*1.0/NULLIF(qs.execution_count,0))/1000.0 AS DECIMAL(18,1)) AS avg_elapsed_ms,
              qs.execution_count,
              qs.last_execution_time,
              LEFT(REPLACE(REPLACE(st.text, CHAR(10), ' '), CHAR(13), ' '), 220) AS sql_text
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            ORDER BY qs.total_elapsed_time DESC
            """
        )
        top = []
        for row in cur.fetchall():
            top.append(
                {
                    "total_elapsed_ms": float(row[0] or 0),
                    "avg_elapsed_ms": float(row[1] or 0),
                    "execution_count": int(row[2] or 0),
                    "last_execution_time": _iso(row[3]),
                    "sql_text": row[4],
                }
            )
        sql["top_query_stats"] = top
    return sql


def main():
    from_d = date.today() - timedelta(days=30)
    to_d = date.today()

    benchmarks = []
    for name, fn in [
        ("executive.overview", lambda: get_exec_overview(from_d, to_d, None)),
        ("executive.products", lambda: get_exec_products(from_d, to_d, None, None, "profit_pln", "desc", 1, 50)),
        ("executive.marketplaces", lambda: get_exec_marketplaces(from_d, to_d)),
        ("strategy.overview", get_strategy_overview),
        ("strategy.opportunities", lambda: get_opportunities_page(page=1, page_size=50)),
        ("profitability.overview", lambda: get_profitability_overview(from_d, to_d, None)),
        ("profitability.orders", lambda: get_profitability_orders(from_d, to_d, None, None, False, None, None, 1, 50)),
        ("profitability.products", lambda: get_profitability_products(from_d, to_d, None, None, "profit_pln", "desc", 1, 50)),
    ]:
        try:
            benchmarks.append(_bench(name, fn))
        except Exception as exc:
            benchmarks.append({"name": name, "ok": False, "error": str(exc)})

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "order_sync_health": _check_order_sync(),
        "jobs": _collect_jobs(),
        "benchmarks": benchmarks,
        "sql": _collect_sql(),
    }

    out = Path(__file__).resolve().parent / f"acc_prod_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=True, indent=2, default=_iso), encoding="utf-8")
    print(str(out))
    print(json.dumps(report["jobs"]["counts"], ensure_ascii=True))


if __name__ == "__main__":
    main()
