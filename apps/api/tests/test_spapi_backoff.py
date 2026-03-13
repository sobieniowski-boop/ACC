"""Tests for SP-API exponential backoff in client.py.

Validates:
  - _backoff_delay() produces correct exponential progression
  - Jitter stays within ±25% bounds
  - Retry-After header is respected (delay ≥ Retry-After)
  - Max delay cap works
  - _parse_retry_after() extracts header values correctly
  - SPAPIClient._request() retries on 429, 500, 502, 503, 504
  - SPAPIClient._request() retries on transient connection errors
  - SPAPIThrottledError raised after all retries exhausted
  - Successful response returned on first attempt
  - Successful response returned after N-1 failures then success
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.connectors.amazon_sp_api.client import (
    BACKOFF_BASE,
    BACKOFF_JITTER,
    BACKOFF_MAX,
    DEFAULT_RETRIES,
    SPAPIClient,
    SPAPIThrottledError,
    _backoff_delay,
    _parse_retry_after,
    _RETRYABLE_STATUS,
)


# ── _backoff_delay() ────────────────────────────────────────────────────────


class TestBackoffDelay:
    def test_exponential_progression(self):
        """delay ≈ base * 2^attempt (before jitter)."""
        for attempt in range(6):
            delay = _backoff_delay(attempt, jitter=0.0)
            expected = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_MAX)
            assert delay == pytest.approx(expected, rel=1e-9)

    def test_sequence_1_2_4_8_16_32(self):
        """With base=1 and no jitter: 1, 2, 4, 8, 16, 32."""
        seq = [_backoff_delay(a, base=1.0, jitter=0.0) for a in range(6)]
        assert seq == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0]

    def test_jitter_within_bounds(self):
        """With default jitter, delay is within ±25% of base value."""
        for attempt in range(5):
            base_val = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_MAX)
            for _ in range(100):
                delay = _backoff_delay(attempt)
                assert delay >= max(base_val * (1 - BACKOFF_JITTER), 0.1)
                assert delay <= base_val * (1 + BACKOFF_JITTER)

    def test_max_cap(self):
        """Delay is capped at BACKOFF_MAX even for huge attempt numbers."""
        delay = _backoff_delay(20, jitter=0.0)
        assert delay == BACKOFF_MAX

    def test_custom_max(self):
        delay = _backoff_delay(10, max_delay=5.0, jitter=0.0)
        assert delay == 5.0

    def test_minimum_floor(self):
        """Delay never goes below 0.1s even with negative jitter luck."""
        for _ in range(200):
            delay = _backoff_delay(0, base=0.01, jitter=0.5)
            assert delay >= 0.1

    def test_retry_after_respected(self):
        """Server-supplied Retry-After takes precedence when larger."""
        delay = _backoff_delay(0, jitter=0.0, retry_after=10.0)
        assert delay == 10.0

    def test_retry_after_does_not_reduce(self):
        """Retry-After doesn't reduce an already-large computed delay."""
        delay = _backoff_delay(5, jitter=0.0, retry_after=1.0)
        assert delay == 32.0  # base*2^5 = 32

    def test_retry_after_none_ignored(self):
        d1 = _backoff_delay(0, jitter=0.0, retry_after=None)
        d2 = _backoff_delay(0, jitter=0.0)
        assert d1 == d2


# ── _parse_retry_after() ────────────────────────────────────────────────────


class TestParseRetryAfter:
    def _resp(self, headers: dict) -> httpx.Response:
        r = httpx.Response(429, headers=headers)
        return r

    def test_retry_after_integer(self):
        val = _parse_retry_after(self._resp({"Retry-After": "5"}))
        assert val == 5.0

    def test_retry_after_float(self):
        val = _parse_retry_after(self._resp({"Retry-After": "2.5"}))
        assert val == 2.5

    def test_amzn_rate_limit_header(self):
        val = _parse_retry_after(self._resp({"x-amzn-RateLimit-Limit": "0.5"}))
        assert val == 0.5

    def test_no_header_returns_none(self):
        val = _parse_retry_after(self._resp({}))
        assert val is None

    def test_invalid_value_returns_none(self):
        val = _parse_retry_after(self._resp({"Retry-After": "not-a-number"}))
        assert val is None


# ── SPAPIClient._request() behaviour ────────────────────────────────────────


def _make_response(status: int, body: dict | None = None, headers: dict | None = None):
    """Create a mock httpx.Response."""
    resp = httpx.Response(
        status,
        json=body or {},
        headers=headers or {},
        request=httpx.Request("GET", "https://test"),
    )
    return resp


@pytest.fixture
def client():
    """SPAPIClient with mocked auth so no real token refresh occurs."""
    with patch("app.connectors.amazon_sp_api.client._auth") as auth_mock:
        auth_mock.get_access_token = AsyncMock(return_value="fake-token")
        c = SPAPIClient.__new__(SPAPIClient)
        c.marketplace_id = "A1PA6795UKMFR9"
        c.seller_id = "SELLER123"
        c.sync_profile = "test"
        c._use_grantless = False
        c._grantless_scope = ""
        yield c


class TestRequestRetries:
    @pytest.mark.asyncio
    async def test_success_first_attempt(self, client):
        resp = _make_response(200, {"items": [1, 2, 3]})
        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=resp), \
             patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"):
            data = await client._request("GET", "/test")
            assert data == {"items": [1, 2, 3]}

    @pytest.mark.asyncio
    async def test_429_then_success(self, client):
        """Retries on 429 and succeeds on second attempt."""
        resp_429 = _make_response(429, headers={"Retry-After": "0"})
        resp_ok = _make_response(200, {"ok": True})

        call_count = 0

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return resp_429 if call_count == 1 else resp_ok

        with patch("httpx.AsyncClient.request", side_effect=mock_request), \
             patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            data = await client._request("GET", "/test", retries=3)
            assert data == {"ok": True}
            assert call_count == 2

    @pytest.mark.asyncio
    async def test_500_then_success(self, client):
        resp_500 = _make_response(500)
        resp_ok = _make_response(200, {"recovered": True})
        call_count = 0

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return resp_500
            return resp_ok

        with patch("httpx.AsyncClient.request", side_effect=mock_request), \
             patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            data = await client._request("GET", "/test", retries=3)
            assert data == {"recovered": True}

    @pytest.mark.asyncio
    async def test_all_retryable_statuses(self, client):
        for status in _RETRYABLE_STATUS:
            resp_fail = _make_response(status)
            resp_ok = _make_response(200, {"status": status})
            _call = 0

            async def mock_req(*a, **k):
                nonlocal _call
                _call += 1
                if _call == 1:
                    return resp_fail
                return resp_ok

            with patch("httpx.AsyncClient.request", side_effect=mock_req), \
                 patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"), \
                 patch("asyncio.sleep", new_callable=AsyncMock):
                data = await client._request("GET", f"/test-{status}", retries=2)
                assert data == {"status": status}

    @pytest.mark.asyncio
    async def test_exhausted_retries_raises_throttled(self, client):
        resp_429 = _make_response(429)

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=resp_429), \
             patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(SPAPIThrottledError, match="throttled after 3 retries"):
                await client._request("POST", "/test", retries=3)

    @pytest.mark.asyncio
    async def test_transient_connect_error_retries(self, client):
        call_count = 0
        resp_ok = _make_response(200, {"ok": True})

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("Connection refused")
            return resp_ok

        with patch("httpx.AsyncClient.request", side_effect=mock_request), \
             patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            data = await client._request("GET", "/test", retries=3)
            assert data == {"ok": True}
            assert call_count == 2

    @pytest.mark.asyncio
    async def test_transient_read_timeout_retries(self, client):
        call_count = 0
        resp_ok = _make_response(200, {"ok": True})

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ReadTimeout("Read timed out")
            return resp_ok

        with patch("httpx.AsyncClient.request", side_effect=mock_request), \
             patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            data = await client._request("GET", "/test", retries=3)
            assert data == {"ok": True}

    @pytest.mark.asyncio
    async def test_non_retryable_4xx_raises_immediately(self, client):
        resp_403 = _make_response(403)
        call_count = 0

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return resp_403

        with patch("httpx.AsyncClient.request", side_effect=mock_request), \
             patch("app.connectors.amazon_sp_api.client.record_sp_api_usage"):
            with pytest.raises(httpx.HTTPStatusError):
                await client._request("GET", "/test", retries=3)
            assert call_count == 1  # No retry on 403

    @pytest.mark.asyncio
    async def test_post_delegates_to_request(self, client):
        resp = _make_response(200, {"created": True})
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={"created": True}) as mock:
            data = await client.post("/items", {"name": "test"})
            mock.assert_called_once()
            assert data == {"created": True}

    @pytest.mark.asyncio
    async def test_put_delegates_to_request(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={"updated": True}) as mock:
            data = await client.put("/items/1", {"name": "new"})
            mock.assert_called_once()
            assert data == {"updated": True}

    @pytest.mark.asyncio
    async def test_delete_delegates_to_request(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={}) as mock:
            data = await client.delete("/items/1")
            mock.assert_called_once()
            assert data == {}

    @pytest.mark.asyncio
    async def test_patch_delegates_to_request(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={"patched": True}) as mock:
            data = await client.patch("/items/1", {"field": "val"})
            mock.assert_called_once()
            assert data == {"patched": True}


# ── Constants ────────────────────────────────────────────────────────────────


class TestConstants:
    def test_backoff_base(self):
        assert BACKOFF_BASE == 1.0

    def test_backoff_max(self):
        assert BACKOFF_MAX == 60.0

    def test_backoff_jitter(self):
        assert BACKOFF_JITTER == 0.25

    def test_default_retries(self):
        assert DEFAULT_RETRIES == 6

    def test_retryable_status_codes(self):
        assert _RETRYABLE_STATUS == frozenset({429, 500, 502, 503, 504})
