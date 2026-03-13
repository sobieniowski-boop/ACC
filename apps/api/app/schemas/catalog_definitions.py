"""Pydantic schemas for Catalog Definitions (PTD cache) endpoints."""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class PTDCacheEntry(BaseModel):
    product_type: str
    marketplace_id: str
    requirements: str = "LISTING"
    locale: str = "DEFAULT"
    schema_size_bytes: int = 0
    schema_version_hash: str = ""
    property_groups: int = 0
    required_attributes: int = 0
    total_attributes: int = 0
    has_variations: bool = False
    variation_theme: Optional[str] = None
    fetched_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    age_days: Optional[int] = None
    is_stale: bool = False


class PTDListResponse(BaseModel):
    count: int
    entries: list[PTDCacheEntry]


class PTDRefreshRequest(BaseModel):
    product_type: Optional[str] = None
    marketplace_id: Optional[str] = None
    force: bool = False


class PTDRefreshResult(BaseModel):
    marketplace_id: str
    synced: int = 0
    skipped: int = 0
    errors: int = 0
    details: list[dict[str, Any]] = []


class PTDSchemaResponse(BaseModel):
    product_type: str
    marketplace_id: str
    ptd_schema: dict[str, Any] = Field(alias="schema")
    fetched_at: Optional[str] = None
    schema_version_hash: str = ""
    metadata: dict[str, Any] = {}

    model_config = {"populate_by_name": True}


class ValidateRequest(BaseModel):
    product_type: str
    marketplace_id: str
    attributes: dict[str, Any]


class ValidationIssueSchema(BaseModel):
    attribute: str
    severity: str
    code: str
    message: str


class ValidateResponse(BaseModel):
    valid: bool
    product_type: str
    marketplace_id: str
    attributes_checked: int = 0
    required_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    issues: list[ValidationIssueSchema] = []


class VariationInfoResponse(BaseModel):
    has_variations: bool
    product_type: str
    marketplace_id: str
    themes: list[str] = []


class MarketplaceDiffResponse(BaseModel):
    product_type: str
    cached_marketplaces: int = 0
    common_required: list[str] = []
    marketplace_specific: dict[str, list[str]] = {}
    coverage: dict[str, int] = {}
