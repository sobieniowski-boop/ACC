"""Structured logging configuration — structlog + stdlib bridge.

Call ``setup_logging()`` once at startup (before any log calls).
- development: coloured console output
- production / test: JSON lines (one JSON object per log event)
"""
from __future__ import annotations

import logging
import sys

import structlog

from app.core.config import settings


def setup_logging() -> None:
    """Configure structlog and stdlib logging for the entire application."""

    is_json = settings.APP_ENV in ("production", "staging", "test")
    log_level = getattr(logging, settings.LOG_LEVEL, logging.INFO)

    # Shared processors (used by both structlog and stdlib bridge)
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if is_json:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Bridge stdlib logging so uvicorn / third-party libs also output structured
    root = logging.getLogger()
    root.setLevel(log_level)

    # Remove existing handlers to avoid duplicates on reload
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    if is_json:
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
            foreign_pre_chain=shared_processors,
        )
    else:
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.dev.ConsoleRenderer(),
            ],
            foreign_pre_chain=shared_processors,
        )

    handler.setFormatter(formatter)
    root.addHandler(handler)

    # Quiet noisy loggers
    for name in ("uvicorn.access", "httpx", "httpcore", "azure"):
        logging.getLogger(name).setLevel(logging.WARNING)
