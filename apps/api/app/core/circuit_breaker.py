"""Circuit breaker for Content Studio publish pipeline.

Tracks SP-API publish failures in a Redis-backed sliding window.
When *FAILURE_THRESHOLD* failures occur within *WINDOW_SECONDS*, the
breaker **opens** and rejects new publish attempts for *COOLDOWN_SECONDS*.

State model (stored in Redis — shared across all workers):
  - **closed**  → normal operation, failures are counted
  - **open**    → publishing blocked, waiting for cooldown
  - **half-open** (implicit) → cooldown expired, next call is allowed

Redis keys:
  ``cb:content_publish:failures``  – sorted set of failure timestamps
  ``cb:content_publish:open_until`` – epoch timestamp when the breaker re-closes
"""
from __future__ import annotations

import logging
import time

from app.core.redis_client import get_redis

log = logging.getLogger("amazon-acc")

# ── Configuration ───────────────────────────────────────────────────────────

FAILURE_THRESHOLD: int = 10       # failures within window to trip
WINDOW_SECONDS: int = 3600        # 1 hour sliding window
COOLDOWN_SECONDS: int = 1800      # 30 minutes open state

_KEY_FAILURES = "cb:content_publish:failures"
_KEY_OPEN_UNTIL = "cb:content_publish:open_until"


class ContentPublishCircuitOpen(Exception):
    """Raised when the circuit breaker is open and publishing is blocked."""


# ── Public API (all async — call from async context or via asyncio.run) ─────


async def is_circuit_open() -> bool:
    """Return True if the breaker is open (publishing should be blocked).

    Defaults to closed (False) if Redis is unavailable — fail-open to avoid
    blocking publishing when the monitoring layer is down.
    """
    try:
        redis = await get_redis()
        raw = await redis.get(_KEY_OPEN_UNTIL)
    except Exception:
        return False
    if raw is None:
        return False
    try:
        open_until = float(raw)
    except (ValueError, TypeError):
        return False
    if time.time() < open_until:
        return True
    # Cooldown expired → half-open / closed.  Clean up.
    await _reset(redis)
    return False


async def record_failure() -> bool:
    """Record a publish failure.  Returns True if the breaker just tripped."""
    try:
        redis = await get_redis()
    except Exception:
        return False
    now = time.time()
    window_start = now - WINDOW_SECONDS

    pipe = redis.pipeline()
    pipe.zadd(_KEY_FAILURES, {str(now): now})
    pipe.zremrangebyscore(_KEY_FAILURES, "-inf", window_start)
    pipe.zcard(_KEY_FAILURES)
    results = await pipe.execute()

    failure_count = int(results[2])

    if failure_count >= FAILURE_THRESHOLD:
        open_until = now + COOLDOWN_SECONDS
        await redis.set(_KEY_OPEN_UNTIL, str(open_until), ex=COOLDOWN_SECONDS + 60)
        log.critical(
            "circuit_breaker.content_publish.OPEN  "
            "failures=%d threshold=%d cooldown_minutes=%d",
            failure_count, FAILURE_THRESHOLD, COOLDOWN_SECONDS // 60,
        )
        return True
    return False


async def record_success() -> None:
    """Record a successful publish (no-op when closed, resets when half-open)."""
    try:
        redis = await get_redis()
    except Exception:
        return
    raw = await redis.get(_KEY_OPEN_UNTIL)
    if raw is not None:
        # Was open/half-open → successful call confirms recovery.
        await _reset(redis)
        log.info("circuit_breaker.content_publish.CLOSED  recovered after cooldown")


async def get_state() -> dict:
    """Return current breaker state for observability / health endpoints."""
    redis = await get_redis()
    now = time.time()

    raw_open = await redis.get(_KEY_OPEN_UNTIL)
    window_start = now - WINDOW_SECONDS
    failure_count = await redis.zcount(_KEY_FAILURES, window_start, "+inf")

    if raw_open is not None:
        try:
            open_until = float(raw_open)
        except (ValueError, TypeError):
            open_until = 0.0
        if now < open_until:
            remaining = int(open_until - now)
            return {
                "state": "open",
                "failures_in_window": failure_count,
                "threshold": FAILURE_THRESHOLD,
                "cooldown_remaining_seconds": remaining,
            }

    return {
        "state": "closed",
        "failures_in_window": failure_count,
        "threshold": FAILURE_THRESHOLD,
        "cooldown_remaining_seconds": 0,
    }


async def force_reset() -> None:
    """Manually reset the breaker (ops escape hatch)."""
    redis = await get_redis()
    await _reset(redis)
    log.info("circuit_breaker.content_publish.FORCE_RESET")


# ── Internal ────────────────────────────────────────────────────────────────


async def _reset(redis) -> None:
    """Clear all breaker state."""
    pipe = redis.pipeline()
    pipe.delete(_KEY_OPEN_UNTIL)
    pipe.delete(_KEY_FAILURES)
    await pipe.execute()
