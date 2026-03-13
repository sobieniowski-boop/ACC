"""Middleware logujący żądania HTTP — metoda, ścieżka, status, czas trwania.

Pomija endpointy health-check (/health, /metrics, /readiness),
żeby nie zaśmiecać logów.

Montować PO CorrelationIdMiddleware, aby correlation_id był dostępny.
"""
from __future__ import annotations

import time

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

log = structlog.get_logger("acc.request")

# Ścieżki pomijane w logowaniu (health probes, metryki)
_SKIP_PATHS: frozenset[str] = frozenset({
    "/health",
    "/healthz",
    "/readiness",
    "/metrics",
    "/api/v1/health",
    "/api/v1/health/",
})


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Loguje każde żądanie HTTP z czasem odpowiedzi w ms."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        # Pomijamy health-checki i metryki
        if path in _SKIP_PATHS or path.startswith("/health/"):
            return await call_next(request)

        start = time.perf_counter()
        response: Response | None = None
        try:
            response = await call_next(request)
            return response
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 1)
            status = response.status_code if response else 500
            log.info(
                "http.request",
                method=request.method,
                path=path,
                status=status,
                duration_ms=duration_ms,
                client=request.client.host if request.client else None,
            )
