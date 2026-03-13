"""Lightweight OpenTelemetry bootstrap for ACC API.

Provides a single ``get_tracer(name)`` helper that returns an OTEL tracer.
The exporter is configured once at first import:

* If ``APPLICATIONINSIGHTS_CONNECTION_STRING`` is set → Azure Monitor exporter.
* If ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set → OTLP/gRPC exporter.
* Otherwise → NoOp (zero overhead, spans are silently discarded).
"""
from __future__ import annotations

import os
from functools import lru_cache

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider


def _build_provider() -> TracerProvider:
    resource = Resource.create({"service.name": "acc-api"})
    provider = TracerProvider(resource=resource)

    ai_conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if ai_conn:
        from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider.add_span_processor(
            BatchSpanProcessor(AzureMonitorTraceExporter(connection_string=ai_conn))
        )
        return provider

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if otlp_endpoint:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
        )
        return provider

    # No exporter configured → NoOp (zero overhead)
    return provider


@lru_cache(maxsize=1)
def _init_tracing() -> None:
    provider = _build_provider()
    trace.set_tracer_provider(provider)


def get_tracer(name: str) -> trace.Tracer:
    """Return an OTEL tracer, initialising the provider on first call."""
    _init_tracing()
    return trace.get_tracer(name)
