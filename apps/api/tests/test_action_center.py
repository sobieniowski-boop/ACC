"""Unit tests for action_center gateway.

Tests the Redis-backed circuit breaker and rate limiter:
  - _cb_keys: key generation
  - is_action_circuit_open: open/closed/expired detection
  - _cb_record_failure: failure counting + breaker trip
  - _cb_record_success: half-open → closed transition
  - _check_rate_limit: per-marketplace limiting
  - execute_action: full orchestration (CB → RL → callback → audit)

Sprint 8 – S8.5
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from app.platform.action_center import (
    _cb_keys,
    is_action_circuit_open,
    _cb_record_failure,
    _cb_record_success,
    _check_rate_limit,
    execute_action,
    ActionCircuitOpen,
    ActionRateLimited,
    CB_FAILURE_THRESHOLD,
    CB_COOLDOWN_SECONDS,
    RL_MAX_ACTIONS,
)


# ── In-memory FakeRedis ────────────────────────────────────────────────────

class _FakePipeline:
    """Queues commands, executes them in order."""

    def __init__(self, store: "_FakeRedis"):
        self._store = store
        self._ops: list = []

    def zadd(self, key, mapping):
        self._ops.append(("zadd", key, mapping))
        return self

    def zremrangebyscore(self, key, mn, mx):
        self._ops.append(("zremrangebyscore", key, mn, mx))
        return self

    def zcard(self, key):
        self._ops.append(("zcard", key))
        return self

    def delete(self, *keys):
        for k in keys:
            self._ops.append(("delete", k))
        return self

    async def execute(self):
        results = []
        for op in self._ops:
            cmd = op[0]
            if cmd == "zadd":
                await self._store.zadd(op[1], op[2])
                results.append(len(op[2]))
            elif cmd == "zremrangebyscore":
                await self._store.zremrangebyscore(op[1], op[2], op[3])
                results.append(0)
            elif cmd == "zcard":
                results.append(await self._store.zcard(op[1]))
            elif cmd == "delete":
                await self._store.delete(op[1])
                results.append(1)
        self._ops.clear()
        return results


class _FakeRedis:
    """Minimal async Redis stub supporting string, sorted-set, incr, expire, ttl."""

    def __init__(self):
        self._data: dict = {}
        self._zsets: dict[str, dict[str, float]] = {}
        self._ttls: dict[str, float] = {}  # key → expire-at timestamp

    async def get(self, key):
        return self._data.get(key)

    async def set(self, key, value, ex=None):
        self._data[key] = value
        if ex:
            self._ttls[key] = time.time() + ex

    async def delete(self, *keys):
        for k in keys:
            self._data.pop(k, None)
            self._zsets.pop(k, None)
            self._ttls.pop(k, None)

    async def zadd(self, key, mapping):
        if key not in self._zsets:
            self._zsets[key] = {}
        self._zsets[key].update(mapping)

    async def zremrangebyscore(self, key, mn, mx):
        zset = self._zsets.get(key, {})
        mn_f = float("-inf") if mn == "-inf" else float(mn)
        mx_f = float("inf") if mx == "+inf" else float(mx)
        to_rm = [m for m, s in zset.items() if mn_f <= s <= mx_f]
        for m in to_rm:
            del zset[m]

    async def zcard(self, key):
        return len(self._zsets.get(key, {}))

    async def zcount(self, key, mn, mx):
        zset = self._zsets.get(key, {})
        mn_f = float("-inf") if mn == "-inf" else float(mn)
        mx_f = float("inf") if mx == "+inf" else float(mx)
        return sum(1 for s in zset.values() if mn_f <= s <= mx_f)

    async def incr(self, key):
        val = int(self._data.get(key, 0)) + 1
        self._data[key] = str(val)
        return val

    async def expire(self, key, seconds):
        self._ttls[key] = time.time() + seconds

    async def ttl(self, key):
        exp = self._ttls.get(key)
        if exp is None:
            return -1
        rem = int(exp - time.time())
        return max(rem, 0)

    def pipeline(self):
        return _FakePipeline(self)


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def fake_redis():
    r = _FakeRedis()

    async def _get_redis():
        return r

    with patch("app.platform.action_center.get_redis", new=_get_redis):
        yield r


# ═══════════════════════════════════════════════════════════════════════════
#  _cb_keys
# ═══════════════════════════════════════════════════════════════════════════

class TestCbKeys:
    def test_format(self):
        fail_key, open_key = _cb_keys("content_publish")
        assert fail_key == "ac:cb:content_publish:failures"
        assert open_key == "ac:cb:content_publish:open_until"


# ═══════════════════════════════════════════════════════════════════════════
#  is_action_circuit_open
# ═══════════════════════════════════════════════════════════════════════════

class TestIsActionCircuitOpen:
    @pytest.mark.asyncio
    async def test_closed_by_default(self, fake_redis):
        assert await is_action_circuit_open("test") is False

    @pytest.mark.asyncio
    async def test_open_when_future_timestamp(self, fake_redis):
        _, key_open = _cb_keys("test")
        await fake_redis.set(key_open, str(time.time() + 600))
        assert await is_action_circuit_open("test") is True

    @pytest.mark.asyncio
    async def test_expired_cleans_up(self, fake_redis):
        key_fail, key_open = _cb_keys("test")
        await fake_redis.set(key_open, str(time.time() - 10))
        await fake_redis.zadd(key_fail, {"x": time.time()})
        result = await is_action_circuit_open("test")
        assert result is False
        # Keys should be cleaned up
        assert await fake_redis.get(key_open) is None


# ═══════════════════════════════════════════════════════════════════════════
#  _cb_record_failure
# ═══════════════════════════════════════════════════════════════════════════

class TestCbRecordFailure:
    @pytest.mark.asyncio
    async def test_single_failure_below_threshold(self, fake_redis):
        tripped = await _cb_record_failure("test")
        assert tripped is False

    @pytest.mark.asyncio
    async def test_trips_at_threshold(self, fake_redis):
        # time.time() may return identical values in a tight loop on Windows,
        # so we patch it to produce unique timestamps for each ZADD member.
        call_count = 0
        base = time.time()

        def _advancing_time():
            nonlocal call_count
            call_count += 1
            return base + call_count

        with patch("app.platform.action_center.time") as mock_time:
            mock_time.time = _advancing_time
            for i in range(CB_FAILURE_THRESHOLD - 1):
                await _cb_record_failure("test")
            tripped = await _cb_record_failure("test")
        assert tripped is True
        # open_until key should be set
        _, key_open = _cb_keys("test")
        raw = await fake_redis.get(key_open)
        assert raw is not None
        assert float(raw) > time.time()


# ═══════════════════════════════════════════════════════════════════════════
#  _cb_record_success
# ═══════════════════════════════════════════════════════════════════════════

class TestCbRecordSuccess:
    @pytest.mark.asyncio
    async def test_resets_open_breaker(self, fake_redis):
        key_fail, key_open = _cb_keys("test")
        await fake_redis.set(key_open, str(time.time() + 600))
        await fake_redis.zadd(key_fail, {"x": time.time()})
        await _cb_record_success("test")
        assert await fake_redis.get(key_open) is None

    @pytest.mark.asyncio
    async def test_noop_when_already_closed(self, fake_redis):
        # Should not raise
        await _cb_record_success("test")


# ═══════════════════════════════════════════════════════════════════════════
#  _check_rate_limit
# ═══════════════════════════════════════════════════════════════════════════

class TestCheckRateLimit:
    @pytest.mark.asyncio
    async def test_none_marketplace_skips(self, fake_redis):
        # Should not raise
        await _check_rate_limit(None)

    @pytest.mark.asyncio
    async def test_under_limit_passes(self, fake_redis):
        for _ in range(RL_MAX_ACTIONS):
            await _check_rate_limit("A1PA")

    @pytest.mark.asyncio
    async def test_over_limit_raises(self, fake_redis):
        for _ in range(RL_MAX_ACTIONS):
            await _check_rate_limit("A1PA")
        with pytest.raises(ActionRateLimited) as exc_info:
            await _check_rate_limit("A1PA")
        assert exc_info.value.marketplace_id == "A1PA"


# ═══════════════════════════════════════════════════════════════════════════
#  execute_action
# ═══════════════════════════════════════════════════════════════════════════

class TestExecuteAction:
    @pytest.mark.asyncio
    @patch("app.platform.action_center._log_action")
    async def test_success_path(self, mock_log, fake_redis):
        async def callback(payload):
            return {"ok": True}

        result = await execute_action("test_type", callback, payload={"x": 1})
        assert result["status"] == "completed"
        assert result["result"] == {"ok": True}

    @pytest.mark.asyncio
    @patch("app.platform.action_center._log_action")
    @patch("app.platform.action_center.log")
    async def test_callback_failure(self, mock_logger, mock_log, fake_redis):
        async def callback(payload):
            raise ValueError("boom")

        result = await execute_action("test_type", callback)
        assert result["status"] == "failed"
        assert "boom" in result["error"]

    @pytest.mark.asyncio
    @patch("app.platform.action_center._log_action")
    async def test_blocked_by_circuit_breaker(self, mock_log, fake_redis):
        _, key_open = _cb_keys("test_type")
        await fake_redis.set(key_open, str(time.time() + 600))
        callback = AsyncMock()
        result = await execute_action("test_type", callback)
        assert result["status"] == "blocked"
        callback.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("app.platform.action_center._log_action")
    async def test_skip_circuit_breaker(self, mock_log, fake_redis):
        _, key_open = _cb_keys("test_type")
        await fake_redis.set(key_open, str(time.time() + 600))

        async def callback(payload):
            return {"ok": True}

        result = await execute_action(
            "test_type", callback, skip_circuit_breaker=True
        )
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    @patch("app.platform.action_center._log_action")
    async def test_rate_limited(self, mock_log, fake_redis):
        for _ in range(RL_MAX_ACTIONS):
            await _check_rate_limit("M1")
        callback = AsyncMock()
        result = await execute_action("t", callback, marketplace_id="M1")
        assert result["status"] == "rate_limited"
        callback.assert_not_awaited()
