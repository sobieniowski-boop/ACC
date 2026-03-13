"""Amazon SP-API base client: auth + HTTP + usage telemetry + exponential backoff."""
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Optional

import httpx
import structlog

from app.core.config import settings
from app.platform.otel import get_tracer
from app.services.sp_api_usage import record_sp_api_usage

log = structlog.get_logger(__name__)
_tracer = get_tracer(__name__)


# ── Retry / back-off configuration ──────────────────────────────────────────

# SP-API Catalog rate: 2 req/s burst, 2 req/s restore.
# Other endpoints vary between 0.5 req/s and 15 req/s.
# These defaults are safe for all endpoints; callers can override per-call.

BACKOFF_BASE: float = 1.0        # base delay in seconds
BACKOFF_MAX: float = 60.0        # cap on computed delay
BACKOFF_JITTER: float = 0.25     # ±25% random jitter
DEFAULT_RETRIES: int = 6         # enough for 1 + 2 + 4 + 8 + 16 + 32 = 63 s

# HTTP status codes that warrant an automatic retry
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class SPAPIThrottledError(Exception):
    """Raised when all retries are exhausted on 429 responses."""


def _backoff_delay(attempt: int, base: float = BACKOFF_BASE,
                   max_delay: float = BACKOFF_MAX,
                   jitter: float = BACKOFF_JITTER,
                   retry_after: float | None = None) -> float:
    """Compute exponential back-off: ``base * 2^attempt`` with jitter.

    If the server sent a ``Retry-After`` header the returned delay is at
    least that value (Amazon sometimes asks for up to 30 s).
    """
    delay = min(base * (2 ** attempt), max_delay)
    # Apply ±jitter
    delta = delay * jitter
    delay = delay + random.uniform(-delta, delta)  # noqa: S311 – not security-sensitive
    delay = max(delay, 0.1)
    if retry_after is not None and retry_after > 0:
        delay = max(delay, retry_after)
    return delay

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP_API_HOST = "https://sellingpartnerapi-eu.amazon.com"


class SPAPIAuth:
    """LWA access token cache (in-process, single-instance)."""

    def __init__(self):
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    async def get_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at - 60:
            return self._access_token

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                LWA_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": settings.SP_API_REFRESH_TOKEN,
                    "client_id": settings.SP_API_CLIENT_ID,
                    "client_secret": settings.SP_API_CLIENT_SECRET,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._expires_at = time.time() + data.get("expires_in", 3600)
            log.info("spapi.token_refreshed", expires_in=data.get("expires_in"))
            return self._access_token


_auth = SPAPIAuth()


class SPAPIGrantlessAuth:
    """LWA access token for grantless operations (client_credentials grant)."""

    def __init__(self):
        self._tokens: dict[str, tuple[str, float]] = {}  # scope -> (token, expires_at)

    async def get_access_token(self, scope: str = "sellingpartnerapi::notifications") -> str:
        cached = self._tokens.get(scope)
        if cached and time.time() < cached[1] - 60:
            return cached[0]

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                LWA_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "scope": scope,
                    "client_id": settings.SP_API_CLIENT_ID,
                    "client_secret": settings.SP_API_CLIENT_SECRET,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            token = data["access_token"]
            expires_at = time.time() + data.get("expires_in", 3600)
            self._tokens[scope] = (token, expires_at)
            log.info("spapi.grantless_token_refreshed", scope=scope, expires_in=data.get("expires_in"))
            return token


_grantless_auth = SPAPIGrantlessAuth()


class SPAPIClient:
    """Thin async HTTP client for SP-API calls."""

    def __init__(self, marketplace_id: Optional[str] = None, sync_profile: str | None = None, *, use_grantless: bool = False, grantless_scope: str = "sellingpartnerapi::notifications"):
        self.marketplace_id = marketplace_id or settings.SP_API_PRIMARY_MARKETPLACE
        self.seller_id = settings.SP_API_SELLER_ID
        self.sync_profile = (sync_profile or "default")[:40]
        self._use_grantless = use_grantless
        self._grantless_scope = grantless_scope

    async def _headers(self) -> dict:
        if self._use_grantless:
            token = await _grantless_auth.get_access_token(self._grantless_scope)
        else:
            token = await _auth.get_access_token()
        return {
            "x-amz-access-token": token,
            "x-amz-seller-id": self.seller_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @staticmethod
    def _estimate_rows(payload: Any) -> int:
        if isinstance(payload, list):
            return len(payload)
        if isinstance(payload, dict):
            inner = payload.get("payload")
            if isinstance(inner, dict):
                for key in (
                    "Orders",
                    "OrderItems",
                    "Transactions",
                    "FinancialEvents",
                    "Reports",
                    "Shipments",
                    "items",
                    "data",
                ):
                    value = inner.get(key)
                    if isinstance(value, list):
                        return len(value)
            for key in ("items", "data", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return len(value)
        return 0

    def _record_usage(
        self,
        *,
        endpoint_name: str,
        http_method: str,
        status_code: int,
        duration_ms: int,
        rows_returned: int = 0,
        error_text: str | None = None,
        marketplace_id: str | None = None,
        sync_profile: str | None = None,
    ) -> None:
        try:
            record_sp_api_usage(
                endpoint_name=endpoint_name,
                http_method=http_method,
                status_code=status_code,
                marketplace_id=marketplace_id or self.marketplace_id,
                sync_profile=sync_profile or self.sync_profile,
                duration_ms=duration_ms,
                rows_returned=rows_returned,
                error_text=error_text,
            )
        except Exception:
            # Telemetry is best-effort only.
            pass

    async def get(
        self,
        path: str,
        params: Optional[dict] = None,
        retries: int = DEFAULT_RETRIES,
        *,
        endpoint_name: str | None = None,
        marketplace_id: str | None = None,
        sync_profile: str | None = None,
    ) -> Any:
        return await self._request(
            "GET", path, retries=retries, params=params,
            endpoint_name=endpoint_name, marketplace_id=marketplace_id,
            sync_profile=sync_profile,
        )

    async def post(
        self,
        path: str,
        body: dict,
        retries: int = DEFAULT_RETRIES,
        *,
        endpoint_name: str | None = None,
        marketplace_id: str | None = None,
        sync_profile: str | None = None,
    ) -> Any:
        return await self._request(
            "POST", path, retries=retries, json_body=body,
            endpoint_name=endpoint_name, marketplace_id=marketplace_id,
            sync_profile=sync_profile,
        )

    async def put(
        self,
        path: str,
        body: dict,
        retries: int = DEFAULT_RETRIES,
        *,
        endpoint_name: str | None = None,
        marketplace_id: str | None = None,
        sync_profile: str | None = None,
    ) -> Any:
        return await self._request(
            "PUT", path, retries=retries, json_body=body,
            endpoint_name=endpoint_name, marketplace_id=marketplace_id,
            sync_profile=sync_profile,
        )

    async def delete(
        self,
        path: str,
        params: Optional[dict] = None,
        retries: int = DEFAULT_RETRIES,
        *,
        endpoint_name: str | None = None,
        marketplace_id: str | None = None,
        sync_profile: str | None = None,
    ) -> Any:
        return await self._request(
            "DELETE", path, retries=retries, params=params,
            endpoint_name=endpoint_name, marketplace_id=marketplace_id,
            sync_profile=sync_profile,
        )

    async def patch(
        self,
        path: str,
        body: dict,
        retries: int = DEFAULT_RETRIES,
        *,
        endpoint_name: str | None = None,
        marketplace_id: str | None = None,
        sync_profile: str | None = None,
    ) -> Any:
        return await self._request(
            "PATCH", path, retries=retries, json_body=body,
            endpoint_name=endpoint_name, marketplace_id=marketplace_id,
            sync_profile=sync_profile,
        )

    # ── Unified request with exponential backoff ────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        *,
        retries: int = DEFAULT_RETRIES,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        endpoint_name: str | None = None,
        marketplace_id: str | None = None,
        sync_profile: str | None = None,
    ) -> Any:
        """Execute an HTTP request with exponential backoff on retryable errors.

        Retryable: 429 (throttle), 500, 502, 503, 504, connection/timeout errors.
        Back-off: ``base * 2^attempt`` with ±25 % jitter, capped at 60 s.
        Respects ``Retry-After`` header from Amazon.
        """
        from opentelemetry import trace as _trace

        headers = await self._headers()
        url = f"{SP_API_HOST}{path}"
        tag = endpoint_name or path
        mkt = marketplace_id or self.marketplace_id

        with _tracer.start_as_current_span(
            f"sp-api {method} {tag}",
            kind=_trace.SpanKind.CLIENT,
            attributes={
                "http.method": method,
                "http.url": url,
                "sp_api.endpoint": tag,
                "sp_api.marketplace_id": mkt,
                "sp_api.sync_profile": sync_profile or self.sync_profile,
            },
        ) as span:
            return await self._request_inner(
                method, path, url=url, headers=headers, tag=tag,
                retries=retries, params=params, json_body=json_body,
                marketplace_id=marketplace_id, sync_profile=sync_profile,
                span=span,
            )

    async def _request_inner(
        self,
        method: str,
        path: str,
        *,
        url: str,
        headers: dict,
        tag: str,
        retries: int,
        params: Optional[dict],
        json_body: Optional[dict],
        marketplace_id: str | None,
        sync_profile: str | None,
        span: Any,
    ) -> Any:

        for attempt in range(retries):
            started = time.perf_counter()
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    resp = await client.request(
                        method, url, headers=headers,
                        params=params, json=json_body,
                    )
                    elapsed_ms = int((time.perf_counter() - started) * 1000)

                    if resp.status_code in _RETRYABLE_STATUS:
                        retry_after = _parse_retry_after(resp)
                        self._record_usage(
                            endpoint_name=tag, http_method=method,
                            status_code=int(resp.status_code),
                            duration_ms=elapsed_ms,
                            error_text=f"retryable_{resp.status_code}",
                            marketplace_id=marketplace_id,
                            sync_profile=sync_profile,
                        )
                        wait = _backoff_delay(attempt, retry_after=retry_after)
                        log.warning(
                            "spapi.retryable",
                            method=method, path=path,
                            status=resp.status_code,
                            wait=round(wait, 2),
                            attempt=attempt + 1,
                            retries=retries,
                        )
                        if attempt < retries - 1:
                            await asyncio.sleep(wait)
                            headers = await self._headers()
                            continue
                        raise SPAPIThrottledError(
                            f"SP-API {method} {path} throttled after {retries} "
                            f"retries (last status {resp.status_code})"
                        )

                    resp.raise_for_status()
                    data = resp.json()
                    self._record_usage(
                        endpoint_name=tag, http_method=method,
                        status_code=int(resp.status_code),
                        duration_ms=elapsed_ms,
                        rows_returned=self._estimate_rows(data),
                        marketplace_id=marketplace_id,
                        sync_profile=sync_profile,
                    )
                    span.set_attribute("http.status_code", int(resp.status_code))
                    span.set_attribute("sp_api.duration_ms", elapsed_ms)
                    span.set_attribute("sp_api.retries", attempt)
                    return data

                except (SPAPIThrottledError, httpx.HTTPStatusError) as exc:
                    span.set_attribute("http.status_code", getattr(getattr(exc, 'response', None), 'status_code', 0))
                    span.set_attribute("error", True)
                    span.record_exception(exc)
                    raise
                except (httpx.ConnectError, httpx.ReadTimeout,
                        httpx.WriteTimeout, httpx.PoolTimeout) as exc:
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    self._record_usage(
                        endpoint_name=tag, http_method=method,
                        status_code=0, duration_ms=elapsed_ms,
                        error_text=str(exc),
                        marketplace_id=marketplace_id,
                        sync_profile=sync_profile,
                    )
                    if attempt < retries - 1:
                        wait = _backoff_delay(attempt)
                        log.warning(
                            "spapi.transient_error",
                            method=method, path=path, error=str(exc),
                            wait=round(wait, 2), attempt=attempt + 1,
                        )
                        await asyncio.sleep(wait)
                        headers = await self._headers()
                        continue
                    raise
                except Exception as exc:
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    self._record_usage(
                        endpoint_name=tag, http_method=method,
                        status_code=0, duration_ms=elapsed_ms,
                        error_text=str(exc),
                        marketplace_id=marketplace_id,
                        sync_profile=sync_profile,
                    )
                    span.set_attribute("error", True)
                    span.record_exception(exc)
                    raise

        raise RuntimeError(f"SP-API {method} {path} failed after {retries} retries")


# ── Module-level helpers ────────────────────────────────────────────────────


def _parse_retry_after(resp: httpx.Response) -> float | None:
    """Extract Retry-After header value as float seconds, or None."""
    raw = resp.headers.get("Retry-After") or resp.headers.get("x-amzn-RateLimit-Limit")
    if not raw:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None
