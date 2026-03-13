"""Per-IP rate limiting backed by Redis."""
from __future__ import annotations

import logging
from fastapi import HTTPException, Request, status

from app.core.redis_client import get_redis

log = logging.getLogger("amazon-acc")

# Redis key layout:
#   auth:attempts:{ip}   – counter, TTL 60 s (sliding window)
#   auth:block:{ip}      – flag, TTL 300 s (5-min block)

_ATTEMPT_LIMIT = 10
_ATTEMPT_WINDOW = 60       # seconds
_BLOCK_DURATION = 300      # seconds


async def check_login_rate_limit(request: Request) -> None:
    """Raise 429 if the caller exceeds the login attempt threshold.

    Call this at the *top* of the login handler, before touching the DB.
    Gracefully degrades (skips rate check) when Redis is unavailable.
    """
    ip = request.client.host if request.client else "unknown"
    try:
        redis = await get_redis()
    except Exception:
        log.warning("rate_limit.redis_unavailable — skipping rate check for ip=%s", ip)
        return

    try:
        block_key = f"auth:block:{ip}"
        attempt_key = f"auth:attempts:{ip}"

        # ── already blocked? ──
        if await redis.exists(block_key):
            log.warning("rate_limit.blocked ip=%s", ip)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts. Try again later.",
            )

        # ── increment attempt counter (atomic INCR + conditional EXPIRE) ──
        count = await redis.incr(attempt_key)
        if count == 1:
            await redis.expire(attempt_key, _ATTEMPT_WINDOW)

        if count > _ATTEMPT_LIMIT:
            # Set the block flag and reset the attempt counter
            await redis.set(block_key, "1", ex=_BLOCK_DURATION)
            await redis.delete(attempt_key)
            log.warning("rate_limit.threshold_exceeded ip=%s attempts=%s", ip, count)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts. Try again later.",
            )
    except HTTPException:
        raise
    except Exception:
        log.warning("rate_limit.redis_error — skipping rate check for ip=%s", ip)
