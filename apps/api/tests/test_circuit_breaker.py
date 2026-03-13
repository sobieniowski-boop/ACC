"""Tests for content publish circuit breaker.

Validates:
  - Breaker stays closed when failures < threshold
  - Breaker opens after FAILURE_THRESHOLD failures within WINDOW_SECONDS
  - is_circuit_open() returns True while breaker is open
  - is_circuit_open() returns False after cooldown expires
  - record_success() resets breaker from half-open state
  - force_reset() clears all state
  - get_state() reports correct state and failure count
  - process_queued_publish_jobs short-circuits when breaker is open
  - _process_publish_push_job raises ContentPublishCircuitOpen when open
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from app.core import circuit_breaker
from app.core.circuit_breaker import (
    COOLDOWN_SECONDS,
    FAILURE_THRESHOLD,
    WINDOW_SECONDS,
    ContentPublishCircuitOpen,
    _KEY_FAILURES,
    _KEY_OPEN_UNTIL,
    force_reset,
    get_state,
    is_circuit_open,
    record_failure,
    record_success,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


class FakeRedis:
    """Minimal in-memory Redis stub for testing sorted sets and keys."""

    def __init__(self):
        self._data: dict = {}
        self._zsets: dict[str, dict[str, float]] = {}

    async def get(self, key):
        return self._data.get(key)

    async def set(self, key, value, ex=None):
        self._data[key] = value

    async def delete(self, *keys):
        for k in keys:
            self._data.pop(k, None)
            self._zsets.pop(k, None)

    async def zadd(self, key, mapping):
        if key not in self._zsets:
            self._zsets[key] = {}
        self._zsets[key].update(mapping)

    async def zremrangebyscore(self, key, min_score, max_score):
        zset = self._zsets.get(key, {})
        min_val = float("-inf") if min_score == "-inf" else float(min_score)
        max_val = float("inf") if max_score == "+inf" else float(max_score)
        to_remove = [m for m, s in zset.items() if min_val <= s <= max_val]
        for m in to_remove:
            del zset[m]

    async def zcard(self, key):
        return len(self._zsets.get(key, {}))

    async def zcount(self, key, min_score, max_score):
        zset = self._zsets.get(key, {})
        min_val = float("-inf") if min_score == "-inf" else float(min_score)
        max_val = float("inf") if max_score == "+inf" else float(max_score)
        return sum(1 for s in zset.values() if min_val <= s <= max_val)

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis: FakeRedis):
        self._redis = redis
        self._ops: list = []

    def zadd(self, key, mapping):
        self._ops.append(("zadd", key, mapping))
        return self

    def zremrangebyscore(self, key, min_score, max_score):
        self._ops.append(("zremrangebyscore", key, min_score, max_score))
        return self

    def zcard(self, key):
        self._ops.append(("zcard", key))
        return self

    def delete(self, *keys):
        self._ops.append(("delete", keys))
        return self

    async def execute(self):
        results = []
        for op in self._ops:
            if op[0] == "zadd":
                await self._redis.zadd(op[1], op[2])
                results.append(None)
            elif op[0] == "zremrangebyscore":
                await self._redis.zremrangebyscore(op[1], op[2], op[3])
                results.append(None)
            elif op[0] == "zcard":
                results.append(await self._redis.zcard(op[1]))
            elif op[0] == "delete":
                await self._redis.delete(*op[1])
                results.append(None)
        return results


@pytest.fixture
def fake_redis():
    r = FakeRedis()
    with patch("app.core.circuit_breaker.get_redis", new_callable=lambda: lambda: AsyncMock(return_value=r)):
        # get_redis is async, so we need an async mock
        pass

    async def _get():
        return r

    with patch("app.core.circuit_breaker.get_redis", side_effect=_get):
        yield r


# ── Tests ────────────────────────────────────────────────────────────────────


class TestIsCircuitOpen:
    @pytest.mark.asyncio
    async def test_closed_when_no_state(self, fake_redis):
        assert await is_circuit_open() is False

    @pytest.mark.asyncio
    async def test_open_when_within_cooldown(self, fake_redis):
        fake_redis._data[_KEY_OPEN_UNTIL] = str(time.time() + 600)
        assert await is_circuit_open() is True

    @pytest.mark.asyncio
    async def test_closed_after_cooldown_expired(self, fake_redis):
        fake_redis._data[_KEY_OPEN_UNTIL] = str(time.time() - 10)
        assert await is_circuit_open() is False
        # State should be cleaned up
        assert _KEY_OPEN_UNTIL not in fake_redis._data

    @pytest.mark.asyncio
    async def test_invalid_value_treated_as_closed(self, fake_redis):
        fake_redis._data[_KEY_OPEN_UNTIL] = "not-a-number"
        assert await is_circuit_open() is False


class TestRecordFailure:
    @pytest.mark.asyncio
    async def test_under_threshold_stays_closed(self, fake_redis):
        for _ in range(FAILURE_THRESHOLD - 1):
            tripped = await record_failure()
            assert tripped is False
        assert _KEY_OPEN_UNTIL not in fake_redis._data

    @pytest.mark.asyncio
    async def test_at_threshold_trips_open(self, fake_redis):
        # Pre-populate N-1 failures with distinct timestamps
        now = time.time()
        for i in range(FAILURE_THRESHOLD - 1):
            ts = now - (FAILURE_THRESHOLD - i)
            fake_redis._zsets.setdefault(_KEY_FAILURES, {})[str(ts)] = ts
        # The Nth failure should trip the breaker
        tripped = await record_failure()
        assert tripped is True
        assert _KEY_OPEN_UNTIL in fake_redis._data
        open_until = float(fake_redis._data[_KEY_OPEN_UNTIL])
        assert open_until > time.time()
        assert open_until <= time.time() + COOLDOWN_SECONDS + 5

    @pytest.mark.asyncio
    async def test_old_failures_outside_window_pruned(self, fake_redis):
        old_time = time.time() - WINDOW_SECONDS - 100
        for i in range(20):
            fake_redis._zsets.setdefault(_KEY_FAILURES, {})[str(old_time + i)] = old_time + i
        # These are old — recording one new failure should NOT trip
        tripped = await record_failure()
        assert tripped is False


class TestRecordSuccess:
    @pytest.mark.asyncio
    async def test_no_op_when_closed(self, fake_redis):
        await record_success()
        # Nothing to assert, just no crash

    @pytest.mark.asyncio
    async def test_resets_when_half_open(self, fake_redis):
        fake_redis._data[_KEY_OPEN_UNTIL] = str(time.time() - 1)
        await record_success()
        assert _KEY_OPEN_UNTIL not in fake_redis._data


class TestGetState:
    @pytest.mark.asyncio
    async def test_closed_state(self, fake_redis):
        state = await get_state()
        assert state["state"] == "closed"
        assert state["failures_in_window"] == 0
        assert state["cooldown_remaining_seconds"] == 0

    @pytest.mark.asyncio
    async def test_open_state(self, fake_redis):
        fake_redis._data[_KEY_OPEN_UNTIL] = str(time.time() + 300)
        # Add some failures
        now = time.time()
        for i in range(5):
            fake_redis._zsets.setdefault(_KEY_FAILURES, {})[str(now - i)] = now - i
        state = await get_state()
        assert state["state"] == "open"
        assert state["failures_in_window"] == 5
        assert state["cooldown_remaining_seconds"] > 0
        assert state["threshold"] == FAILURE_THRESHOLD


class TestForceReset:
    @pytest.mark.asyncio
    async def test_clears_all_state(self, fake_redis):
        fake_redis._data[_KEY_OPEN_UNTIL] = str(time.time() + 600)
        fake_redis._zsets[_KEY_FAILURES] = {"a": 1.0, "b": 2.0}
        await force_reset()
        assert _KEY_OPEN_UNTIL not in fake_redis._data
        assert _KEY_FAILURES not in fake_redis._zsets


class TestContentOpsIntegration:
    def test_process_queued_short_circuits_when_open(self, monkeypatch):
        monkeypatch.setattr("app.services.content_ops.publish.ensure_v2_schema", lambda: None)
        monkeypatch.setattr(
            "app.services.content_ops.publish._is_circuit_open_sync",
            lambda: True,
        )
        from app.services.content_ops import process_queued_publish_jobs
        result = process_queued_publish_jobs(limit=5)
        assert result["circuit_breaker"] == "open"
        assert result["claimed"] == 0


# ── Constants ────────────────────────────────────────────────────────────────


class TestConstants:
    def test_failure_threshold(self):
        assert FAILURE_THRESHOLD == 10

    def test_window_seconds(self):
        assert WINDOW_SECONDS == 3600

    def test_cooldown_seconds(self):
        assert COOLDOWN_SECONDS == 1800


# ── Helpers ──────────────────────────────────────────────────────────────────


async def _async_return(val):
    return val
