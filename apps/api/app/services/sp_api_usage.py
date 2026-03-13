"""SP-API usage telemetry (daily aggregated).

Stores call volume and latency per endpoint/marketplace/profile so we can:
- measure API pressure (rate-limit risk),
- compare sync profiles (core/ops/pii),
- estimate operational API cost trends.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import threading
from typing import Any

import pyodbc
import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

_SCHEMA_READY = False
_SCHEMA_LOCK = threading.Lock()


def _connect() -> pyodbc.Connection:
    return connect_acc(autocommit=False, timeout=10)


def ensure_sp_api_usage_schema() -> None:
    """No-op — schema managed by Alembic migration eb018."""


def record_sp_api_usage(
    *,
    endpoint_name: str,
    http_method: str,
    status_code: int,
    marketplace_id: str | None,
    sync_profile: str | None,
    duration_ms: int,
    rows_returned: int = 0,
    error_text: str | None = None,
) -> None:
    """Best-effort upsert into daily usage telemetry."""
    if not endpoint_name:
        endpoint_name = "unknown"
    method = (http_method or "GET").upper()[:10]
    profile = (sync_profile or "default")[:40]
    mkt = (marketplace_id or "")[:32]
    status = int(status_code or 0)
    is_success = 1 if 200 <= status < 400 else 0
    is_error = 0 if is_success else 1
    is_throttled = 1 if status == 429 else 0
    err = (error_text or "")[:500] or None
    usage_date = date.today()
    dur = max(int(duration_ms or 0), 0)
    rows = max(int(rows_returned or 0), 0)

    try:
        ensure_sp_api_usage_schema()
    except Exception as exc:
        log.warning("spapi.usage.ensure_schema_failed", error=str(exc))
        return

    conn = None
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_sp_api_usage_daily
            SET calls_count = calls_count + 1,
                success_count = success_count + ?,
                error_count = error_count + ?,
                throttled_count = throttled_count + ?,
                total_duration_ms = total_duration_ms + ?,
                rows_returned = rows_returned + ?,
                last_error = COALESCE(?, last_error),
                updated_at = SYSUTCDATETIME()
            WHERE usage_date = ?
              AND endpoint_name = ?
              AND http_method = ?
              AND marketplace_id = ?
              AND sync_profile = ?
              AND status_code = ?
            """,
            (
                is_success,
                is_error,
                is_throttled,
                dur,
                rows,
                err,
                usage_date,
                endpoint_name,
                method,
                mkt,
                profile,
                status,
            ),
        )
        cur.execute("SELECT @@ROWCOUNT")
        updated = int(cur.fetchone()[0] or 0)
        if updated == 0:
            try:
                cur.execute(
                    """
                    INSERT INTO dbo.acc_sp_api_usage_daily
                    (
                        usage_date, endpoint_name, http_method, marketplace_id, sync_profile, status_code,
                        calls_count, success_count, error_count, throttled_count, total_duration_ms, rows_returned, last_error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        usage_date,
                        endpoint_name,
                        method,
                        mkt,
                        profile,
                        status,
                        is_success,
                        is_error,
                        is_throttled,
                        dur,
                        rows,
                        err,
                    ),
                )
            except pyodbc.IntegrityError:
                # Parallel insert race; second update is enough.
                cur.execute(
                    """
                    UPDATE dbo.acc_sp_api_usage_daily
                    SET calls_count = calls_count + 1,
                        success_count = success_count + ?,
                        error_count = error_count + ?,
                        throttled_count = throttled_count + ?,
                        total_duration_ms = total_duration_ms + ?,
                        rows_returned = rows_returned + ?,
                        last_error = COALESCE(?, last_error),
                        updated_at = SYSUTCDATETIME()
                    WHERE usage_date = ?
                      AND endpoint_name = ?
                      AND http_method = ?
                      AND marketplace_id = ?
                      AND sync_profile = ?
                      AND status_code = ?
                    """,
                    (
                        is_success,
                        is_error,
                        is_throttled,
                        dur,
                        rows,
                        err,
                        usage_date,
                        endpoint_name,
                        method,
                        mkt,
                        profile,
                        status,
                    ),
                )
        conn.commit()
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        log.warning(
            "spapi.usage.record_failed",
            endpoint=endpoint_name,
            status_code=status,
            marketplace_id=mkt,
            sync_profile=profile,
            error=str(exc),
        )
    finally:
        if conn:
            conn.close()


def list_sp_api_usage_daily(
    *,
    days: int = 7,
    endpoint_name: str | None = None,
    marketplace_id: str | None = None,
    sync_profile: str | None = None,
) -> list[dict[str, Any]]:
    """Return aggregated usage rows for diagnostics UI/API."""
    ensure_sp_api_usage_schema()
    days_safe = max(1, min(int(days or 7), 90))
    conn = _connect()
    try:
        cur = conn.cursor()
        where = [
            "usage_date >= DATEADD(day, -?, CAST(GETUTCDATE() AS DATE))",
        ]
        params: list[Any] = [days_safe - 1]
        if endpoint_name:
            where.append("endpoint_name = ?")
            params.append(endpoint_name)
        if marketplace_id:
            where.append("marketplace_id = ?")
            params.append(marketplace_id)
        if sync_profile:
            where.append("sync_profile = ?")
            params.append(sync_profile)
        sql = f"""
            SELECT
                usage_date,
                endpoint_name,
                http_method,
                marketplace_id,
                sync_profile,
                SUM(calls_count) AS calls_count,
                SUM(success_count) AS success_count,
                SUM(error_count) AS error_count,
                SUM(throttled_count) AS throttled_count,
                SUM(total_duration_ms) AS total_duration_ms,
                SUM(rows_returned) AS rows_returned,
                MAX(updated_at) AS updated_at
            FROM dbo.acc_sp_api_usage_daily WITH (NOLOCK)
            WHERE {" AND ".join(where)}
            GROUP BY usage_date, endpoint_name, http_method, marketplace_id, sync_profile
            ORDER BY usage_date DESC, calls_count DESC, endpoint_name ASC
        """
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            calls = int(r[5] or 0)
            total_ms = int(r[9] or 0)
            out.append(
                {
                    "usage_date": r[0].isoformat() if isinstance(r[0], (datetime, date)) else str(r[0]),
                    "endpoint_name": str(r[1] or ""),
                    "http_method": str(r[2] or ""),
                    "marketplace_id": str(r[3] or ""),
                    "sync_profile": str(r[4] or ""),
                    "calls_count": calls,
                    "success_count": int(r[6] or 0),
                    "error_count": int(r[7] or 0),
                    "throttled_count": int(r[8] or 0),
                    "total_duration_ms": total_ms,
                    "avg_duration_ms": round(total_ms / calls, 2) if calls > 0 else 0.0,
                    "rows_returned": int(r[10] or 0),
                    "updated_at": r[11].isoformat() if hasattr(r[11], "isoformat") else None,
                }
            )
        return out
    finally:
        conn.close()
