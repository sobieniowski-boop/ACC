"""Wspólne schematy odpowiedzi API — koperty, paginacja, health.

Użycie:
    from app.schemas.common import PaginatedResponse, ApiResponse

    @router.get("/items", response_model=PaginatedResponse[ItemOut])
    async def list_items(...):
        ...
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

from app.platform.middleware import correlation_id_var

T = TypeVar("T")


class ResponseMeta(BaseModel):
    """Metadane dołączane do każdej odpowiedzi API."""
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: str | None = Field(default_factory=lambda: correlation_id_var.get() or None)


class ApiResponse(BaseModel, Generic[T]):
    """Standardowa koperta dla pojedynczego obiektu."""
    data: T
    meta: ResponseMeta = Field(default_factory=ResponseMeta)


class PaginatedResponse(BaseModel, Generic[T]):
    """Standardowa koperta dla listy z paginacją."""
    data: list[T]
    total: int
    page: int
    page_size: int
    has_next: bool
    meta: ResponseMeta = Field(default_factory=ResponseMeta)


class HealthResponse(BaseModel):
    """Schemat odpowiedzi health-check."""
    status: str = "ok"
    app: str = "amazon-command-center"
    env: str = ""


class DeepHealthResponse(BaseModel):
    """Schemat szczegółowego health-check (Azure SQL, Redis, SP-API)."""
    status: str = "ok"
    azure_sql: str = "unknown"
    redis: str = "unknown"
    sp_api: str = "unknown"
    pool: dict[str, Any] | None = None
