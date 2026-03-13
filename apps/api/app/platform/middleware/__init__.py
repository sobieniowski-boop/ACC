"""Correlation-ID middleware — traces every request across logs and responses.

Reads ``X-Correlation-ID`` from incoming request headers (or generates a
UUID4 if absent), binds it to the structlog context for the duration of
the request, and echoes it back in the response headers.

Usage (in main.py, after CORS middleware):
    from app.platform.middleware.correlation_id import CorrelationIdMiddleware
    app.add_middleware(CorrelationIdMiddleware)
"""
from __future__ import annotations

import uuid
from contextvars import ContextVar

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

HEADER_NAME = "X-Correlation-ID"

# ContextVar accessible from anywhere in the same async task
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        cid = request.headers.get(HEADER_NAME) or str(uuid.uuid4())
        correlation_id_var.set(cid)

        structlog.contextvars.bind_contextvars(correlation_id=cid)
        try:
            response = await call_next(request)
            response.headers[HEADER_NAME] = cid
            return response
        finally:
            structlog.contextvars.unbind_contextvars("correlation_id")
