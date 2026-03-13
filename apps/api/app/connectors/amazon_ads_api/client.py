"""Amazon Ads API — auth (LWA) + async HTTP client.

Follows the same pattern as SP-API client (app/connectors/amazon_sp_api/client.py)
but uses separate credentials and the Advertising API endpoint.

API docs: https://advertising.amazon.com/API/docs/en-us/
EU endpoint: https://advertising-api-eu.amazon.com
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

import httpx
import structlog

from app.core.config import settings
from app.platform.otel import get_tracer

log = structlog.get_logger(__name__)
_tracer = get_tracer(__name__)

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

# Regional base URLs for Amazon Ads API
ADS_API_HOSTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}


class AdsAPIAuth:
    """LWA access token cache for Amazon Ads API (separate from SP-API)."""

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
                    "refresh_token": settings.AMAZON_ADS_REFRESH_TOKEN,
                    "client_id": settings.AMAZON_ADS_CLIENT_ID,
                    "client_secret": settings.AMAZON_ADS_CLIENT_SECRET,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._expires_at = time.time() + data.get("expires_in", 3600)
            log.info("ads_api.token_refreshed", expires_in=data.get("expires_in"))
            return self._access_token


# Module-level singleton
_auth = AdsAPIAuth()


class AdsAPIClient:
    """Async HTTP client for Amazon Advertising API.

    Usage:
        client = AdsAPIClient(profile_id=123456)
        campaigns = await client.get("/sp/campaigns", params={"stateFilter": "enabled"})
    """

    def __init__(self, profile_id: Optional[int] = None):
        self.profile_id = profile_id
        region = settings.AMAZON_ADS_REGION.upper()
        self.base_url = ADS_API_HOSTS.get(region, ADS_API_HOSTS["EU"])

    async def _headers(self) -> dict[str, str]:
        token = await _auth.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Amazon-Advertising-API-ClientId": settings.AMAZON_ADS_CLIENT_ID,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.profile_id:
            headers["Amazon-Advertising-API-Scope"] = str(self.profile_id)
        return headers

    async def get(
        self,
        path: str,
        params: Optional[dict] = None,
        retries: int = 5,
        timeout: float = 30,
    ) -> Any:
        from opentelemetry import trace as _trace

        headers = await self._headers()
        url = f"{self.base_url}{path}"
        with _tracer.start_as_current_span(
            f"ads-api GET {path}",
            kind=_trace.SpanKind.CLIENT,
            attributes={
                "http.method": "GET",
                "http.url": url,
                "ads_api.profile_id": str(self.profile_id or ""),
            },
        ) as span:
            for attempt in range(retries):
                started = time.perf_counter()
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.get(url, headers=headers, params=params)
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    if resp.status_code == 429:
                        import random
                        base_wait = 3 * (2 ** attempt)
                        jitter = random.uniform(0, min(base_wait * 0.3, 5))
                        wait = float(resp.headers.get("Retry-After", base_wait)) + jitter
                        log.warning("ads_api.rate_limited", path=path, wait=round(wait, 1), attempt=attempt + 1)
                        await asyncio.sleep(wait)
                        headers = await self._headers()
                        continue
                    if resp.status_code == 401:
                        _auth._access_token = None
                        headers = await self._headers()
                        continue
                    resp.raise_for_status()
                    span.set_attribute("http.status_code", resp.status_code)
                    span.set_attribute("ads_api.duration_ms", elapsed_ms)
                    span.set_attribute("ads_api.retries", attempt)
                    return resp.json()
            span.set_attribute("error", True)
            raise RuntimeError(f"Ads API GET {path} failed after {retries} retries")

    async def post(
        self,
        path: str,
        body: Optional[dict] = None,
        retries: int = 6,
        timeout: float = 60,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> Any:
        from opentelemetry import trace as _trace

        headers = await self._headers()
        if extra_headers:
            headers.update(extra_headers)
        url = f"{self.base_url}{path}"
        with _tracer.start_as_current_span(
            f"ads-api POST {path}",
            kind=_trace.SpanKind.CLIENT,
            attributes={
                "http.method": "POST",
                "http.url": url,
                "ads_api.profile_id": str(self.profile_id or ""),
            },
        ) as span:
            for attempt in range(retries):
                started = time.perf_counter()
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.post(url, headers=headers, json=body)
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    if resp.status_code == 429:
                        # Exponential backoff: 3, 6, 12, 24, 48, 96 seconds + jitter
                        import random
                        base_wait = 3 * (2 ** attempt)
                        jitter = random.uniform(0, min(base_wait * 0.3, 5))
                        wait = float(resp.headers.get("Retry-After", base_wait)) + jitter
                        log.warning("ads_api.rate_limited", path=path, wait=round(wait, 1), attempt=attempt + 1, max_retries=retries)
                        await asyncio.sleep(wait)
                        headers = await self._headers()
                        if extra_headers:
                            headers.update(extra_headers)
                        continue
                    if resp.status_code == 401:
                        _auth._access_token = None
                        headers = await self._headers()
                        if extra_headers:
                            headers.update(extra_headers)
                        continue
                    resp.raise_for_status()
                    span.set_attribute("http.status_code", resp.status_code)
                    span.set_attribute("ads_api.duration_ms", elapsed_ms)
                    span.set_attribute("ads_api.retries", attempt)
                    return resp.json()
            span.set_attribute("error", True)
            raise RuntimeError(f"Ads API POST {path} failed after {retries} retries")

    async def download(self, url: str, retries: int = 5) -> bytes:
        """Download a report file (gzip JSON) from a pre-signed S3 URL.

        Retries on transient network errors (DNS, connection reset, timeout).
        """
        import random
        from opentelemetry import trace as _trace

        with _tracer.start_as_current_span(
            "ads-api download",
            kind=_trace.SpanKind.CLIENT,
            attributes={
                "http.method": "GET",
                "ads_api.profile_id": str(self.profile_id or ""),
                "ads_api.operation": "download",
            },
        ) as span:
            for attempt in range(retries):
                started = time.perf_counter()
                try:
                    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                        resp = await client.get(url)
                        resp.raise_for_status()
                        elapsed_ms = int((time.perf_counter() - started) * 1000)
                        span.set_attribute("http.status_code", resp.status_code)
                        span.set_attribute("ads_api.duration_ms", elapsed_ms)
                        span.set_attribute("ads_api.download_bytes", len(resp.content))
                        span.set_attribute("ads_api.retries", attempt)
                        return resp.content
                except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as exc:
                    if attempt == retries - 1:
                        span.set_attribute("error", True)
                        span.record_exception(exc)
                        raise
                    wait = 5 * (2 ** attempt) + random.uniform(0, 3)
                    log.warning(
                        "ads_api.download.retry",
                        attempt=attempt + 1,
                        max_retries=retries,
                        wait=round(wait, 1),
                        error=str(exc),
                    )
                    await asyncio.sleep(wait)
            span.set_attribute("error", True)
            raise RuntimeError(f"Download failed after {retries} retries")
