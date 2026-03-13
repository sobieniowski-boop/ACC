"""Action Center — unified gateway for Amazon write operations.

Every outbound write to Amazon (content publish, price change, family
restructure, etc.) flows through ``execute_action()``.  The gateway
provides:

* **Audit trail** — every action logged to ``acc_action_log``
* **Circuit breaker** — per action_type, Redis-backed sliding window
* **Rate limiting** — per marketplace, Redis-backed
* **Structured result** — callers get a consistent envelope

Tables managed (auto-created via ``ensure_action_center_schema``):

* ``acc_action_log`` — audit log of all write operations
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Awaitable, Callable

from app.core.config import settings
from app.core.db_connection import connect_acc
from app.core.redis_client import get_redis

log = logging.getLogger("amazon-acc")

# ── Configuration ───────────────────────────────────────────────────────────

# Circuit breaker defaults (per action_type)
CB_FAILURE_THRESHOLD: int = 10
CB_WINDOW_SECONDS: int = 3600       # 1-hour sliding window
CB_COOLDOWN_SECONDS: int = 1800     # 30-min open state

# Rate limit defaults (per marketplace)
RL_MAX_ACTIONS: int = 30
RL_WINDOW_SECONDS: int = 60         # 30 actions per minute per marketplace


# ── Schema DDL ──────────────────────────────────────────────────────────────

_SCHEMA_STATEMENTS: list[str] = [
    """
    IF OBJECT_ID('dbo.acc_action_log', 'U') IS NULL
    CREATE TABLE dbo.acc_action_log (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        action_id           VARCHAR(64)   NOT NULL UNIQUE,
        action_type         VARCHAR(80)   NOT NULL,
        marketplace_id      VARCHAR(20)   NULL,
        correlation_id      VARCHAR(64)   NOT NULL,
        status              VARCHAR(20)   NOT NULL DEFAULT 'pending',
        payload             NVARCHAR(MAX) NULL,
        result_summary      NVARCHAR(MAX) NULL,
        error_message       NVARCHAR(500) NULL,
        duration_ms         INT           NULL,
        circuit_breaker     BIT           NOT NULL DEFAULT 0,
        rate_limited        BIT           NOT NULL DEFAULT 0,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        completed_at        DATETIME2     NULL
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_action_log_type_status')
    CREATE INDEX ix_action_log_type_status
        ON dbo.acc_action_log (action_type, status)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_action_log_marketplace')
    CREATE INDEX ix_action_log_marketplace
        ON dbo.acc_action_log (marketplace_id, created_at)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_action_log_correlation')
    CREATE INDEX ix_action_log_correlation
        ON dbo.acc_action_log (correlation_id)
    """,
]


def ensure_action_center_schema() -> None:
    """Create action center tables if they don't exist (called on startup)."""
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
        log.info("action_center.schema_ensured")
    finally:
        conn.close()


# ── Circuit Breaker (generic, per action_type) ──────────────────────────────


class ActionCircuitOpen(Exception):
    """Raised when the circuit breaker for an action type is open."""

    def __init__(self, action_type: str, cooldown_remaining: int):
        self.action_type = action_type
        self.cooldown_remaining = cooldown_remaining
        super().__init__(
            f"Circuit breaker open for '{action_type}', "
            f"cooldown {cooldown_remaining}s remaining"
        )


def _cb_keys(action_type: str) -> tuple[str, str]:
    return (
        f"ac:cb:{action_type}:failures",
        f"ac:cb:{action_type}:open_until",
    )


async def is_action_circuit_open(action_type: str) -> bool:
    """Return True if the breaker for *action_type* is open."""
    try:
        redis = await get_redis()
        _, key_open = _cb_keys(action_type)
        raw = await redis.get(key_open)
    except Exception:
        return False  # fail-open
    if raw is None:
        return False
    try:
        open_until = float(raw)
    except (ValueError, TypeError):
        return False
    if time.time() < open_until:
        return True
    # Cooldown expired — clean up
    key_fail, key_open = _cb_keys(action_type)
    pipe = redis.pipeline()
    pipe.delete(key_open)
    pipe.delete(key_fail)
    await pipe.execute()
    return False


async def _cb_record_failure(action_type: str) -> bool:
    """Record a failure; returns True if the breaker just tripped."""
    try:
        redis = await get_redis()
    except Exception:
        return False
    key_fail, key_open = _cb_keys(action_type)
    now = time.time()
    window_start = now - CB_WINDOW_SECONDS

    pipe = redis.pipeline()
    pipe.zadd(key_fail, {str(now): now})
    pipe.zremrangebyscore(key_fail, "-inf", window_start)
    pipe.zcard(key_fail)
    results = await pipe.execute()

    failure_count = int(results[2])
    if failure_count >= CB_FAILURE_THRESHOLD:
        open_until = now + CB_COOLDOWN_SECONDS
        await redis.set(key_open, str(open_until), ex=CB_COOLDOWN_SECONDS + 60)
        log.critical(
            "action_center.circuit_breaker.OPEN  action_type=%s failures=%d",
            action_type, failure_count,
        )
        return True
    return False


async def _cb_record_success(action_type: str) -> None:
    """Record success; resets breaker if it was half-open."""
    try:
        redis = await get_redis()
    except Exception:
        return
    _, key_open = _cb_keys(action_type)
    raw = await redis.get(key_open)
    if raw is not None:
        key_fail, _ = _cb_keys(action_type)
        pipe = redis.pipeline()
        pipe.delete(key_open)
        pipe.delete(key_fail)
        await pipe.execute()
        log.info("action_center.circuit_breaker.CLOSED  action_type=%s", action_type)


async def get_circuit_breaker_state(action_type: str) -> dict:
    """Return current breaker state for observability."""
    try:
        redis = await get_redis()
    except Exception:
        return {"state": "unknown", "action_type": action_type}
    key_fail, key_open = _cb_keys(action_type)
    now = time.time()
    raw_open = await redis.get(key_open)
    window_start = now - CB_WINDOW_SECONDS
    failure_count = await redis.zcount(key_fail, window_start, "+inf")

    if raw_open is not None:
        try:
            open_until = float(raw_open)
        except (ValueError, TypeError):
            open_until = 0.0
        if now < open_until:
            return {
                "state": "open",
                "action_type": action_type,
                "failures_in_window": failure_count,
                "threshold": CB_FAILURE_THRESHOLD,
                "cooldown_remaining_seconds": int(open_until - now),
            }

    return {
        "state": "closed",
        "action_type": action_type,
        "failures_in_window": failure_count,
        "threshold": CB_FAILURE_THRESHOLD,
        "cooldown_remaining_seconds": 0,
    }


# ── Rate Limiting (per marketplace) ─────────────────────────────────────────


class ActionRateLimited(Exception):
    """Raised when the per-marketplace rate limit is exceeded."""

    def __init__(self, marketplace_id: str, retry_after: int):
        self.marketplace_id = marketplace_id
        self.retry_after = retry_after
        super().__init__(
            f"Rate limit exceeded for marketplace '{marketplace_id}', "
            f"retry after {retry_after}s"
        )


async def _check_rate_limit(marketplace_id: str | None) -> None:
    """Raise ``ActionRateLimited`` if the marketplace is over its limit."""
    if not marketplace_id:
        return
    try:
        redis = await get_redis()
    except Exception:
        return  # fail-open
    key = f"ac:rl:{marketplace_id}"
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, RL_WINDOW_SECONDS)
    if count > RL_MAX_ACTIONS:
        ttl = await redis.ttl(key)
        raise ActionRateLimited(marketplace_id, max(ttl, 1))


# ── Audit Logging ───────────────────────────────────────────────────────────

def _log_action(
    action_id: str,
    action_type: str,
    marketplace_id: str | None,
    correlation_id: str,
    status: str,
    payload: dict | None,
    result_summary: str | None = None,
    error_message: str | None = None,
    duration_ms: int | None = None,
    circuit_breaker: bool = False,
    rate_limited: bool = False,
) -> None:
    """Insert or update a row in acc_action_log."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        payload_str = json.dumps(payload, default=str, ensure_ascii=False)[:4000] if payload else None
        cur.execute(
            """
            MERGE dbo.acc_action_log AS tgt
            USING (SELECT ? AS action_id) AS src
                ON tgt.action_id = src.action_id
            WHEN MATCHED THEN
                UPDATE SET
                    status         = ?,
                    result_summary = ?,
                    error_message  = ?,
                    duration_ms    = ?,
                    circuit_breaker = ?,
                    rate_limited   = ?,
                    completed_at   = CASE WHEN ? IN ('completed', 'failed', 'blocked')
                                         THEN SYSUTCDATETIME() ELSE completed_at END
            WHEN NOT MATCHED THEN
                INSERT (action_id, action_type, marketplace_id, correlation_id,
                        status, payload, result_summary, error_message,
                        duration_ms, circuit_breaker, rate_limited)
                VALUES (?, ?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?);
            """,
            (
                action_id,
                # UPDATE params
                status, result_summary, error_message, duration_ms,
                1 if circuit_breaker else 0, 1 if rate_limited else 0,
                status,
                # INSERT params
                action_id, action_type, marketplace_id, correlation_id,
                status, payload_str, result_summary, error_message,
                duration_ms, 1 if circuit_breaker else 0, 1 if rate_limited else 0,
            ),
        )
        conn.commit()
        cur.close()
    except Exception as exc:
        log.warning("action_center.audit_log_failed", error=str(exc))
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


# ── Main Gateway ────────────────────────────────────────────────────────────


async def execute_action(
    action_type: str,
    callback: Callable[..., Awaitable[dict]],
    *,
    marketplace_id: str | None = None,
    payload: dict | None = None,
    correlation_id: str | None = None,
    skip_circuit_breaker: bool = False,
    skip_rate_limit: bool = False,
) -> dict:
    """Execute an Amazon write operation through the action center.

    Parameters:
        action_type:   identifies the operation, e.g. "content_publish"
        callback:      async callable that performs the actual write;
                       receives ``payload`` and must return a result dict
        marketplace_id: for rate limiting + audit grouping
        payload:       data passed to callback
        correlation_id: optional trace ID
        skip_circuit_breaker: bypass breaker (e.g. for retries after manual reset)
        skip_rate_limit: bypass rate limit

    Returns:
        ``{"status": "completed"|"failed"|"blocked"|"rate_limited",
           "action_id": ..., "result": ..., "error": ...}``
    """
    action_id = uuid.uuid4().hex
    correlation_id = correlation_id or uuid.uuid4().hex
    t0 = time.time()

    # Audit: record pending action
    _log_action(action_id, action_type, marketplace_id, correlation_id,
                "pending", payload)

    # ── Circuit breaker gate ────────────────────────────────────────────
    if not skip_circuit_breaker and await is_action_circuit_open(action_type):
        state = await get_circuit_breaker_state(action_type)
        remaining = state.get("cooldown_remaining_seconds", 0)
        _log_action(action_id, action_type, marketplace_id, correlation_id,
                    "blocked", payload, error_message="circuit_breaker_open",
                    circuit_breaker=True)
        return {
            "status": "blocked",
            "action_id": action_id,
            "reason": "circuit_breaker_open",
            "cooldown_remaining_seconds": remaining,
        }

    # ── Rate limit gate ─────────────────────────────────────────────────
    if not skip_rate_limit:
        try:
            await _check_rate_limit(marketplace_id)
        except ActionRateLimited as rl:
            _log_action(action_id, action_type, marketplace_id, correlation_id,
                        "rate_limited", payload, error_message=str(rl),
                        rate_limited=True)
            return {
                "status": "rate_limited",
                "action_id": action_id,
                "reason": "marketplace_rate_limit",
                "retry_after_seconds": rl.retry_after,
            }

    # ── Execute callback ────────────────────────────────────────────────
    try:
        result = await callback(payload or {})
        elapsed_ms = int((time.time() - t0) * 1000)

        await _cb_record_success(action_type)

        summary = json.dumps(result, default=str, ensure_ascii=False)[:4000] if result else None
        _log_action(action_id, action_type, marketplace_id, correlation_id,
                    "completed", payload, result_summary=summary,
                    duration_ms=elapsed_ms)

        log.info("action_center.completed",
                 action_type=action_type, action_id=action_id[:16],
                 marketplace=marketplace_id, duration_ms=elapsed_ms)
        return {
            "status": "completed",
            "action_id": action_id,
            "result": result,
            "duration_ms": elapsed_ms,
        }

    except Exception as exc:
        elapsed_ms = int((time.time() - t0) * 1000)
        tripped = await _cb_record_failure(action_type)

        error_msg = str(exc)[:500]
        _log_action(action_id, action_type, marketplace_id, correlation_id,
                    "failed", payload, error_message=error_msg,
                    duration_ms=elapsed_ms)

        log.error("action_center.failed",
                  action_type=action_type, action_id=action_id[:16],
                  error=error_msg, circuit_tripped=tripped)
        return {
            "status": "failed",
            "action_id": action_id,
            "error": error_msg,
            "duration_ms": elapsed_ms,
            "circuit_breaker_tripped": tripped,
        }


# ── Observability ───────────────────────────────────────────────────────────


def get_recent_actions(
    action_type: str | None = None,
    marketplace_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return recent action_log rows for dashboard/API consumption."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        conditions = ["1=1"]
        params: list[Any] = []
        if action_type:
            conditions.append("action_type = ?")
            params.append(action_type)
        if marketplace_id:
            conditions.append("marketplace_id = ?")
            params.append(marketplace_id)
        if status:
            conditions.append("status = ?")
            params.append(status)
        where = " AND ".join(conditions)
        cur.execute(
            f"""
            SELECT TOP (?) action_id, action_type, marketplace_id,
                   correlation_id, status, result_summary, error_message,
                   duration_ms, circuit_breaker, rate_limited,
                   created_at, completed_at
            FROM dbo.acc_action_log WITH (NOLOCK)
            WHERE {where}
            ORDER BY created_at DESC
            """,
            (limit, *params),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()
