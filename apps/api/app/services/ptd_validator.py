"""Schema-driven validation for Amazon listing payloads.

Uses cached Product Type Definitions (PTD) to validate attributes
*before* sending writes to SP-API Listings Items API.

Capabilities:
  • ``validate_listing_payload()`` — full validation: required fields, type checks
  • ``get_required_attributes()`` — extract required attribute list from cached PTD
  • ``get_attribute_schema()`` — get full property schema for a single attribute
  • ``get_variation_info()`` — variation theme + available themes
  • ``diff_marketplace_requirements()`` — compare required attrs across marketplaces (foundation)

Design notes:
  - Validation is *advisory* — never blocks SP-API writes silently.
  - Results include severity (error/warning/info) for UI rendering.
  - Lazy-loads PTD from cache; returns error if cache miss.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import structlog

from app.services.ptd_cache import get_cached_ptd

log = structlog.get_logger(__name__)


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    attribute: str
    severity: Severity
    code: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "attribute": self.attribute,
            "severity": self.severity.value,
            "code": self.code,
            "message": self.message,
        }


@dataclass
class ValidationResult:
    valid: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    product_type: str = ""
    marketplace_id: str = ""
    attributes_checked: int = 0
    required_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "product_type": self.product_type,
            "marketplace_id": self.marketplace_id,
            "attributes_checked": self.attributes_checked,
            "required_count": self.required_count,
            "error_count": sum(1 for i in self.issues if i.severity == Severity.ERROR),
            "warning_count": sum(1 for i in self.issues if i.severity == Severity.WARNING),
            "issues": [i.to_dict() for i in self.issues],
        }


def _get_json_schema(cached: dict) -> dict:
    """Extract the embedded JSON Schema from a cached PTD."""
    return cached.get("schema", {}).get("schema", {})


def get_required_attributes(
    product_type: str,
    marketplace_id: str,
) -> list[str] | None:
    """Return sorted list of required attribute names, or None if cache miss."""
    cached = get_cached_ptd(product_type, marketplace_id)
    if not cached:
        return None
    json_schema = _get_json_schema(cached)
    return sorted(json_schema.get("required", []))


def get_attribute_schema(
    product_type: str,
    marketplace_id: str,
    attribute_name: str,
) -> dict[str, Any] | None:
    """Get property schema for a single attribute. Returns None on miss."""
    cached = get_cached_ptd(product_type, marketplace_id)
    if not cached:
        return None
    json_schema = _get_json_schema(cached)
    props = json_schema.get("properties", {})
    return props.get(attribute_name)


def get_variation_info(
    product_type: str,
    marketplace_id: str,
) -> dict[str, Any] | None:
    """Get variation support info from cached PTD schema."""
    cached = get_cached_ptd(product_type, marketplace_id)
    if not cached:
        return None

    json_schema = _get_json_schema(cached)
    props = json_schema.get("properties", {})

    result: dict[str, Any] = {
        "has_variations": "child_parent_sku_relationship" in props,
        "product_type": product_type.upper(),
        "marketplace_id": marketplace_id,
        "themes": [],
    }

    rel_prop = props.get("child_parent_sku_relationship")
    if rel_prop:
        items = rel_prop.get("items", {})
        child_type = items.get("properties", {}).get("child_relationship_type", {})
        result["themes"] = child_type.get("enum", [])

    return result


def validate_listing_payload(
    product_type: str,
    marketplace_id: str,
    attributes: dict[str, Any],
) -> ValidationResult:
    """Validate a listing attributes dict against the cached PTD schema.

    Performs:
      1. Required field presence check
      2. Unknown attribute detection (warning only)
      3. Basic type compatibility where schema specifies type

    Returns ValidationResult with issues list.
    """
    result = ValidationResult(
        valid=True,
        product_type=product_type.upper(),
        marketplace_id=marketplace_id,
    )

    cached = get_cached_ptd(product_type, marketplace_id)
    if not cached:
        result.valid = False
        result.issues.append(ValidationIssue(
            attribute="",
            severity=Severity.ERROR,
            code="PTD_CACHE_MISS",
            message=f"No cached PTD for {product_type} / {marketplace_id}. "
                    f"Refresh cache before validating.",
        ))
        return result

    json_schema = _get_json_schema(cached)
    properties = json_schema.get("properties", {})
    required = set(json_schema.get("required", []))

    result.required_count = len(required)
    result.attributes_checked = len(attributes)

    # 1. Required field check
    for req_attr in sorted(required):
        if req_attr not in attributes:
            result.valid = False
            result.issues.append(ValidationIssue(
                attribute=req_attr,
                severity=Severity.ERROR,
                code="MISSING_REQUIRED",
                message=f"Required attribute '{req_attr}' is missing.",
            ))

    # 2. Unknown attribute detection
    for attr_name in sorted(attributes.keys()):
        if attr_name not in properties:
            result.issues.append(ValidationIssue(
                attribute=attr_name,
                severity=Severity.WARNING,
                code="UNKNOWN_ATTRIBUTE",
                message=f"Attribute '{attr_name}' is not defined in PTD schema.",
            ))

    # 3. Basic type check for provided attributes
    for attr_name, attr_value in attributes.items():
        prop_schema = properties.get(attr_name)
        if not prop_schema:
            continue  # Already flagged as unknown

        expected_type = prop_schema.get("type")
        if not expected_type:
            continue

        _check = _validate_type(attr_name, attr_value, expected_type, prop_schema)
        if _check:
            result.issues.append(_check)
            if _check.severity == Severity.ERROR:
                result.valid = False

    return result


def _validate_type(
    attr_name: str,
    value: Any,
    expected_type: str,
    prop_schema: dict,
) -> ValidationIssue | None:
    """Basic JSON Schema type validation. Returns issue or None."""
    type_map = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
        "object": dict,
    }

    expected_py = type_map.get(expected_type)
    if not expected_py:
        return None

    # SP-API listing attributes are arrays of value objects, e.g.
    # "item_name": [{"value": "My Product", "language_tag": "en_US"}]
    if expected_type == "array" and isinstance(value, list):
        return None  # Array shape is correct; deep validation is future work

    if not isinstance(value, expected_py):
        return ValidationIssue(
            attribute=attr_name,
            severity=Severity.ERROR,
            code="TYPE_MISMATCH",
            message=f"Attribute '{attr_name}' expects {expected_type}, "
                    f"got {type(value).__name__}.",
        )
    return None


def diff_marketplace_requirements(
    product_type: str,
    marketplace_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Compare required attributes for a product type across marketplaces.

    Returns a dict with:
      - common_required: attrs required in ALL marketplaces
      - marketplace_specific: {mkt_id: [attrs only required there]}
      - coverage: {mkt_id: total_required_count}
    """
    from app.core.config import MARKETPLACE_REGISTRY

    if marketplace_ids is None:
        marketplace_ids = list(MARKETPLACE_REGISTRY.keys())

    per_mkt: dict[str, set[str]] = {}
    for mkt_id in marketplace_ids:
        req = get_required_attributes(product_type, mkt_id)
        if req is not None:
            per_mkt[mkt_id] = set(req)

    if not per_mkt:
        return {
            "product_type": product_type.upper(),
            "cached_marketplaces": 0,
            "common_required": [],
            "marketplace_specific": {},
            "coverage": {},
        }

    # Common = intersection of all
    common = set.intersection(*per_mkt.values()) if per_mkt else set()

    marketplace_specific = {}
    for mkt_id, req_set in per_mkt.items():
        unique = req_set - common
        if unique:
            marketplace_specific[mkt_id] = sorted(unique)

    return {
        "product_type": product_type.upper(),
        "cached_marketplaces": len(per_mkt),
        "common_required": sorted(common),
        "marketplace_specific": marketplace_specific,
        "coverage": {mkt_id: len(reqs) for mkt_id, reqs in per_mkt.items()},
    }
