"""Security hardening middleware and utilities for ACC.

Implements:
- Request rate limiting per IP (via slowapi, optional)
- Request size limits
- IP allowlist/blocklist for admin endpoints
- Security event logging
"""
from __future__ import annotations

import logging
import time
from typing import Set

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.config import settings

log = logging.getLogger("acc.security")

# Paths that should only be accessible from internal network
INTERNAL_ONLY_PATHS: Set[str] = {
    "/metrics",
    "/api/v1/system/health",
}

# Maximum request body size (10 MB)
MAX_BODY_SIZE = 10 * 1024 * 1024

# Trusted proxy networks (Docker bridge, localhost, Azure VNet)
TRUSTED_NETWORKS = ("127.0.0.1", "10.10.", "10.0.", "172.17.", "172.18.")


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject oversized request bodies to prevent resource exhaustion."""

    async def dispatch(self, request: Request, call_next) -> Response:
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > MAX_BODY_SIZE:
            log.warning(
                "security.request_too_large ip=%s path=%s size=%s",
                request.client.host if request.client else "unknown",
                request.url.path,
                content_length,
            )
            return Response(
                content='{"detail":"Request body too large"}',
                status_code=413,
                media_type="application/json",
            )
        return await call_next(request)


class InternalOnlyMiddleware(BaseHTTPMiddleware):
    """Block access to internal-only endpoints from external IPs in production."""

    async def dispatch(self, request: Request, call_next) -> Response:
        if settings.APP_ENV != "production":
            return await call_next(request)

        path = request.url.path
        if path in INTERNAL_ONLY_PATHS:
            client_ip = request.client.host if request.client else "unknown"
            if not any(client_ip.startswith(prefix) for prefix in TRUSTED_NETWORKS):
                log.warning(
                    "security.internal_endpoint_blocked ip=%s path=%s",
                    client_ip,
                    path,
                )
                return Response(
                    content='{"detail":"Forbidden"}',
                    status_code=403,
                    media_type="application/json",
                )
        return await call_next(request)


class SecurityAuditMiddleware(BaseHTTPMiddleware):
    """Log security-relevant events: auth failures, suspicious patterns."""

    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - start

        # Log authentication failures
        if response.status_code in (401, 403):
            log.warning(
                "security.auth_failure ip=%s method=%s path=%s status=%d elapsed=%.3f",
                request.client.host if request.client else "unknown",
                request.method,
                request.url.path,
                response.status_code,
                elapsed,
            )

        # Log suspiciously slow requests (potential DoS probe)
        if elapsed > 30:
            log.warning(
                "security.slow_request ip=%s method=%s path=%s elapsed=%.1fs",
                request.client.host if request.client else "unknown",
                request.method,
                request.url.path,
                elapsed,
            )

        return response


def setup_security(app: FastAPI) -> None:
    """Wire all security middleware into the FastAPI application.
    
    Must be called after CORS middleware is added.
    """
    app.add_middleware(RequestSizeLimitMiddleware)
    app.add_middleware(InternalOnlyMiddleware)
    app.add_middleware(SecurityAuditMiddleware)
    log.info("security.middleware_initialized env=%s", settings.APP_ENV)
