"""Prometheus metrics middleware for FastAPI.

Exposes /metrics endpoint with:
- HTTP request count, latency histogram, in-flight gauge
- Business metrics: active DB connections, scheduler job status
- System metrics: Python GC, process RSS

Usage: already wired in main.py via ``setup_metrics(app)``.
Scrape endpoint: GET /metrics (no auth, internal network only).
"""
from __future__ import annotations

import time
from typing import Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client import multiprocess  # noqa: F401 — side-effect import
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Match

# ── Registry ─────────────────────────────────────────────────────
REGISTRY = CollectorRegistry()

# ── HTTP Metrics ─────────────────────────────────────────────────
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
    registry=REGISTRY,
)

HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=REGISTRY,
)

HTTP_REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "Number of HTTP requests currently being processed",
    ["method"],
    registry=REGISTRY,
)

# ── Business Metrics ─────────────────────────────────────────────
DB_CONNECTIONS_ACTIVE = Gauge(
    "db_connections_active",
    "Active database connections",
    registry=REGISTRY,
)

SCHEDULER_JOBS_TOTAL = Counter(
    "scheduler_jobs_total",
    "Total scheduler job executions",
    ["job_name", "status"],
    registry=REGISTRY,
)

SCHEDULER_JOB_DURATION = Histogram(
    "scheduler_job_duration_seconds",
    "Scheduler job execution duration",
    ["job_name"],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0),
    registry=REGISTRY,
)

ORDER_SYNC_TOTAL = Counter(
    "order_sync_total",
    "Orders synced from Amazon SP-API",
    ["marketplace", "status"],
    registry=REGISTRY,
)

PROFIT_CALC_DURATION = Histogram(
    "profit_calculation_duration_seconds",
    "Profit engine calculation time",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
    registry=REGISTRY,
)

ADS_SYNC_CAMPAIGNS = Gauge(
    "ads_sync_campaigns_total",
    "Total Amazon Ads campaigns tracked",
    registry=REGISTRY,
)

# ── App Info ─────────────────────────────────────────────────────
APP_INFO = Gauge(
    "app_info",
    "Application metadata",
    ["version", "environment"],
    registry=REGISTRY,
)


def _get_route_name(request: Request) -> str:
    """Resolve the matched route template (e.g., /api/v1/orders/{order_id})."""
    for route in request.app.routes:
        match, _ = route.matches(request.scope)
        if match == Match.FULL:
            return getattr(route, "path", request.url.path)
    return request.url.path


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Collect per-request HTTP metrics."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip metrics endpoint itself
        if request.url.path == "/metrics":
            return await call_next(request)

        method = request.method
        HTTP_REQUESTS_IN_PROGRESS.labels(method=method).inc()
        start = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            endpoint = _get_route_name(request)
            HTTP_REQUESTS_TOTAL.labels(
                method=method, endpoint=endpoint, status="500"
            ).inc()
            HTTP_REQUEST_DURATION.labels(
                method=method, endpoint=endpoint
            ).observe(time.perf_counter() - start)
            raise
        finally:
            HTTP_REQUESTS_IN_PROGRESS.labels(method=method).dec()

        endpoint = _get_route_name(request)
        status = str(response.status_code)
        HTTP_REQUESTS_TOTAL.labels(
            method=method, endpoint=endpoint, status=status
        ).inc()
        HTTP_REQUEST_DURATION.labels(
            method=method, endpoint=endpoint
        ).observe(time.perf_counter() - start)

        return response


def setup_metrics(app: FastAPI) -> None:
    """Wire Prometheus middleware and /metrics endpoint into the FastAPI app."""
    from app.core.config import settings

    APP_INFO.labels(version="1.0.0", environment=settings.APP_ENV).set(1)

    app.add_middleware(PrometheusMiddleware)

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint():
        return Response(
            content=generate_latest(REGISTRY),
            media_type=CONTENT_TYPE_LATEST,
        )
