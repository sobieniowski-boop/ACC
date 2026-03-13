"""
Redis-based distributed scheduler lock.

Ensures only ONE worker process runs APScheduler jobs across all
replicas / machines.  Uses atomic SET NX EX for leader election
with background renewal and safe release on shutdown.

Protocol
--------
1. Worker tries ``SET acc:scheduler:leader <worker_id> NX EX 60``
2. If acquired → this worker is the leader; it starts a renewal
   task that re-sets the TTL every RENEW_INTERVAL seconds.
3. If NOT acquired → this worker skips scheduler startup.
4. On shutdown the leader deletes the key **only if it still owns it**
   (Lua compare-and-delete to avoid releasing someone else's lock).
5. If the leader crashes, the key expires after LOCK_TTL seconds and
   another worker can take over.
"""
from __future__ import annotations

import asyncio
import os
import uuid

import structlog

from app.core.redis_client import get_redis

log = structlog.get_logger(__name__)

LOCK_KEY = "acc:scheduler:leader"
LOCK_TTL = 60          # seconds — key auto-expires if not renewed
RENEW_INTERVAL = 20    # seconds — renew at 1/3 of TTL

# Lua script: delete key only if its value matches our worker_id.
# Prevents releasing a lock that was already taken over by another worker.
_RELEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

# Lua script: extend TTL only if we still own the lock.
_RENEW_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""


class SchedulerLock:
    """Distributed leader lock backed by Redis."""

    def __init__(self) -> None:
        self._worker_id: str = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._owned: bool = False
        self._renew_task: asyncio.Task | None = None

    # -- public API ----------------------------------------------------------

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def is_leader(self) -> bool:
        return self._owned

    async def acquire(self) -> bool:
        """Try to become the scheduler leader.  Returns True on success.

        Falls back to local-only leadership when Redis is unreachable
        (single-worker dev mode).
        """
        try:
            redis = await get_redis()
            acquired = await redis.set(
                LOCK_KEY, self._worker_id, nx=True, ex=LOCK_TTL,
            )
        except Exception as exc:
            # Redis unavailable — assume single-worker dev mode.
            log.warning(
                "scheduler_lock.redis_unavailable_assuming_leader",
                worker_id=self._worker_id,
                error=str(exc)[:120],
            )
            self._owned = True
            return True

        if acquired:
            self._owned = True
            self._renew_task = asyncio.create_task(
                self._renewal_loop(), name="scheduler-lock-renewal",
            )
            log.info(
                "scheduler_lock.acquired",
                worker_id=self._worker_id,
                ttl=LOCK_TTL,
            )
            return True

        # Someone else holds the lock — log who.
        current_holder = await redis.get(LOCK_KEY)
        log.info(
            "scheduler_lock.not_acquired",
            worker_id=self._worker_id,
            current_leader=current_holder,
        )
        return False

    async def release(self) -> None:
        """Release the lock if we still own it.  Safe to call multiple times."""
        if not self._owned:
            return

        # Cancel renewal first so we don't race against ourselves.
        if self._renew_task and not self._renew_task.done():
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None

        try:
            redis = await get_redis()
            freed = await redis.eval(
                _RELEASE_LUA, 1, LOCK_KEY, self._worker_id,
            )
            if freed:
                log.info("scheduler_lock.released", worker_id=self._worker_id)
            else:
                log.warning(
                    "scheduler_lock.release_skipped",
                    worker_id=self._worker_id,
                    reason="lock owned by another worker or already expired",
                )
        except Exception as exc:
            log.warning("scheduler_lock.release_error", error=str(exc))
        finally:
            self._owned = False

    # -- internal ------------------------------------------------------------

    async def _renewal_loop(self) -> None:
        """Periodically extend the TTL while we're still the leader."""
        try:
            while True:
                await asyncio.sleep(RENEW_INTERVAL)
                try:
                    redis = await get_redis()
                    renewed = await redis.eval(
                        _RENEW_LUA, 1, LOCK_KEY, self._worker_id, str(LOCK_TTL),
                    )
                    if renewed:
                        log.debug(
                            "scheduler_lock.renewed",
                            worker_id=self._worker_id,
                            ttl=LOCK_TTL,
                        )
                    else:
                        log.error(
                            "scheduler_lock.renewal_failed",
                            worker_id=self._worker_id,
                            reason="lock lost — another worker may have taken over",
                        )
                        self._owned = False
                        return
                except Exception as exc:
                    log.warning(
                        "scheduler_lock.renewal_error",
                        worker_id=self._worker_id,
                        error=str(exc),
                    )
        except asyncio.CancelledError:
            return


# Module-level singleton.
scheduler_lock = SchedulerLock()
