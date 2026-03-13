"""Content Ops - product type resolution, attribute mapping, catalog CRUD."""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import pyodbc

from app.connectors.amazon_sp_api.client import SPAPIClient
from app.connectors.mssql.mssql_store import ensure_v2_schema
from app.core.config import settings, MARKETPLACE_REGISTRY
from ._helpers import (
    _connect, _fetchall_dict, _json_load, _json_list,
    _marketplace_to_id, _marketplace_to_code,
    _normalize_publish_selection,
    _run_async, _spapi_ready,
    _PRODUCT_TYPE_KEYWORDS, _ATTRIBUTE_TRANSFORMS,
)


def _normalize_required_attrs(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
    return []


def _extract_required_attrs_from_definition_payload(payload: Any) -> list[str]:
    required: set[str] = set()

    def _walk(node: Any):
        if isinstance(node, dict):
            req = node.get("required")
            if isinstance(req, list):
                for item in req:
                    text = str(item).strip()
                    if text:
                        required.add(text)
            prop_names = node.get("propertyNames")
            if isinstance(prop_names, list):
                for item in prop_names:
                    text = str(item).strip()
                    if text:
                        required.add(text)
            for val in node.values():
                _walk(val)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(payload)
    return sorted(required)


def _load_attribute_mapping_rules(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id, marketplace_id, product_type, source_field, target_attribute,
            transform, priority, is_active
        FROM dbo.acc_co_attribute_map WITH (NOLOCK)
        WHERE is_active = 1
        ORDER BY priority ASC, created_at ASC
        """
    )
    rows = _fetchall_dict(cur)
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": str(row.get("id")),
                "marketplace_id": str(row.get("marketplace_id") or "").strip() or None,
                "product_type": str(row.get("product_type") or "").strip().upper() or None,
                "source_field": str(row.get("source_field") or "").strip(),
                "target_attribute": str(row.get("target_attribute") or "").strip(),
                "transform": str(row.get("transform") or "identity").strip().lower(),
                "priority": int(row.get("priority") or 100),
            }
        )
    return out


def _get_nested_value(source: dict[str, Any], path: str) -> Any:
    if not path:
        return None
    current: Any = source
    for part in [x for x in path.split(".") if x]:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _transform_mapped_value(value: Any, transform: str) -> Any:
    t = str(transform or "identity").strip().lower()
    if t not in _ATTRIBUTE_TRANSFORMS:
        t = "identity"
    if value is None:
        return None
    if t == "stringify":
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=True)
    if isinstance(value, str):
        if t == "upper":
            return value.upper()
        if t == "lower":
            return value.lower()
        if t == "trim":
            return value.strip()
    return value


def _attrs_missing_required(attrs: dict[str, Any], required_attrs: list[str]) -> list[str]:
    missing: list[str] = []
    for attr_name in required_attrs:
        key = str(attr_name).strip()
        if not key:
            continue
        val = attrs.get(key)
        if _is_missing_value(val):
            missing.append(key)
    return missing


def _apply_attribute_registry(
    *,
    fields: dict[str, Any],
    marketplace: str,
    product_type: str,
    rules: list[dict[str, Any]],
) -> dict[str, Any]:
    attrs = fields.get("attributes_json") if isinstance(fields.get("attributes_json"), dict) else {}
    attrs = dict(attrs or {})
    market_id = _marketplace_to_id(marketplace)
    source = {"fields": fields, "attributes_json": attrs}

    for rule in rules:
        rule_market = rule.get("marketplace_id")
        rule_product_type = str(rule.get("product_type") or "").strip().upper() or None
        if rule_market and rule_market != market_id:
            continue
        if rule_product_type and rule_product_type != str(product_type or "").strip().upper():
            continue
        source_field = str(rule.get("source_field") or "").strip()
        target_attr = str(rule.get("target_attribute") or "").strip()
        if not source_field or not target_attr:
            continue
        if not _is_missing_value(attrs.get(target_attr)):
            continue
        val = _get_nested_value(source, source_field)
        if _is_missing_value(val):
            continue
        transformed = _transform_mapped_value(val, str(rule.get("transform") or "identity"))
        if _is_missing_value(transformed):
            continue
        attrs[target_attr] = transformed
    return attrs


def _suggest_source_fields_for_attr(
    *,
    missing_attr: str,
    fields: dict[str, Any],
    product_type: str,
) -> list[str]:
    attr = str(missing_attr or "").strip().lower()
    source_candidates: list[str] = [f"fields.attributes_json.{attr}", "fields.title", "fields.description", "fields.keywords"]
    if attr in {"color", "colour"}:
        source_candidates.extend(["fields.attributes_json.color", "fields.attributes_json.colour", "fields.bullets"])
    elif attr in {"size", "size_name"}:
        source_candidates.extend(["fields.attributes_json.size", "fields.attributes_json.size_name", "fields.bullets"])
    elif attr in {"material"}:
        source_candidates.extend(["fields.attributes_json.material", "fields.bullets"])
    elif attr in {"brand", "manufacturer"}:
        source_candidates.extend(["fields.attributes_json.brand", "fields.title"])
    elif attr in {"item_name", "product_name"}:
        source_candidates.extend(["fields.title"])
    elif product_type in {"PLANTER", "FURNITURE"}:
        source_candidates.extend(["fields.attributes_json.material", "fields.attributes_json.color"])
    elif product_type in {"KITCHEN", "TOOL"}:
        source_candidates.extend(["fields.attributes_json.material", "fields.attributes_json.model", "fields.bullets"])
    return list(dict.fromkeys(source_candidates))


def _load_required_attrs_state_from_definition(
    cur: pyodbc.Cursor,
    *,
    marketplace: str,
    product_type: str,
) -> tuple[str, list[str]]:
    market_id = _marketplace_to_id(marketplace)
    product_type_value = str(product_type or "").strip().upper()
    if not market_id or not product_type_value:
        return "invalid_input", []
    cur.execute(
        """
        SELECT TOP 1 required_attrs_json
        FROM dbo.acc_co_product_type_defs WITH (NOLOCK)
        WHERE marketplace_id = ?
          AND product_type = ?
        ORDER BY refreshed_at DESC
        """,
        (market_id, product_type_value),
    )
    row = cur.fetchone()
    if not row:
        return "missing_definition", []
    required = _normalize_required_attrs(row[0])
    if not required:
        return "empty_required_attrs", []
    return "ok", required


def _load_required_attrs_from_definition(cur: pyodbc.Cursor, *, marketplace: str, product_type: str) -> list[str]:
    _, required = _load_required_attrs_state_from_definition(
        cur,
        marketplace=marketplace,
        product_type=product_type,
    )
    return required


def _normalize_attribute_values(*, value: Any, lang: str, mp_id: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        out: list[dict[str, Any]] = []
        for item in value:
            text = str(item).strip() if not isinstance(item, (dict, list)) else json.dumps(item, ensure_ascii=True)
            if text:
                out.append({"value": text, "language_tag": lang, "marketplace_id": mp_id})
        return out
    if isinstance(value, dict):
        return [{"value": json.dumps(value, ensure_ascii=True), "language_tag": lang, "marketplace_id": mp_id}]
    text = str(value).strip()
    if not text:
        return []
    return [{"value": text, "language_tag": lang, "marketplace_id": mp_id}]


def _is_reserved_content_attr(attr_name: str) -> bool:
    normalized = str(attr_name or "").strip().lower()
    return normalized in {"product_type", "amazon_product_type", "required_attrs"}


def _load_product_type_map_rules(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id, marketplace_id, brand, category, subcategory, product_type,
            required_attrs_json, priority, is_active
        FROM dbo.acc_co_product_type_map WITH (NOLOCK)
        WHERE is_active = 1
        ORDER BY priority ASC, created_at ASC
        """
    )
    rows = _fetchall_dict(cur)
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": str(row.get("id")),
                "marketplace_id": str(row.get("marketplace_id") or "").strip() or None,
                "brand": str(row.get("brand") or "").strip() or None,
                "category": str(row.get("category") or "").strip() or None,
                "subcategory": str(row.get("subcategory") or "").strip() or None,
                "product_type": str(row.get("product_type") or "").strip().upper(),
                "required_attrs": _normalize_required_attrs(row.get("required_attrs_json")),
                "priority": int(row.get("priority") or 100),
            }
        )
    return out


def _rule_match_value(rule_val: str | None, actual_val: str | None) -> bool:
    if not rule_val:
        return True
    left = str(rule_val).strip().lower()
    right = str(actual_val or "").strip().lower()
    return left == right


def _resolve_native_product_type_and_requirements(
    *,
    sku: str,
    fields: dict[str, Any],
    category_hint: str | None,
    subcategory_hint: str | None,
    brand_hint: str | None,
    marketplace: str | None,
    mapping_rules: list[dict[str, Any]] | None = None,
) -> tuple[str, list[str], str]:
    attrs = fields.get("attributes_json")
    if isinstance(attrs, dict):
        explicit = str(attrs.get("product_type") or attrs.get("amazon_product_type") or "").strip().upper()
        if explicit:
            required = _normalize_required_attrs(attrs.get("required_attrs"))
            return explicit, required, "attributes_json"

    market_id = _marketplace_to_id(str(marketplace or "").strip())
    if mapping_rules:
        for rule in mapping_rules:
            if not _rule_match_value(rule.get("marketplace_id"), market_id):
                continue
            if not _rule_match_value(rule.get("brand"), brand_hint):
                continue
            if not _rule_match_value(rule.get("category"), category_hint):
                continue
            if not _rule_match_value(rule.get("subcategory"), subcategory_hint):
                continue
            product_type = str(rule.get("product_type") or "").strip().upper()
            if product_type:
                return product_type, _normalize_required_attrs(rule.get("required_attrs")), "mapping_rule"

    pieces: list[str] = []
    if category_hint:
        pieces.append(category_hint)
    if subcategory_hint:
        pieces.append(subcategory_hint)
    title = str(fields.get("title") or "").strip()
    if title:
        pieces.append(title)
    text = " ".join(pieces).lower()

    for product_type, keywords in _PRODUCT_TYPE_KEYWORDS:
        if any(k in text for k in keywords):
            return product_type, [], "heuristic"
    return "HOME", [], "default"


def _resolve_native_product_type(
    *,
    sku: str,
    fields: dict[str, Any],
    category_hint: str | None,
    subcategory_hint: str | None,
) -> str:
    product_type, _, _ = _resolve_native_product_type_and_requirements(
        sku=sku,
        fields=fields,
        category_hint=category_hint,
        subcategory_hint=subcategory_hint,
        brand_hint=None,
        marketplace=None,
        mapping_rules=None,
    )
    return product_type


def list_product_type_mappings():
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, marketplace_id, brand, category, subcategory, product_type,
                required_attrs_json, priority, is_active
            FROM dbo.acc_co_product_type_map WITH (NOLOCK)
            ORDER BY priority ASC, created_at ASC
            """
        )
        rows = _fetchall_dict(cur)
        return [
            {
                "id": str(r.get("id")),
                "marketplace_id": r.get("marketplace_id"),
                "brand": r.get("brand"),
                "category": r.get("category"),
                "subcategory": r.get("subcategory"),
                "product_type": str(r.get("product_type") or "").upper(),
                "required_attrs": _normalize_required_attrs(r.get("required_attrs_json")),
                "priority": int(r.get("priority") or 100),
                "is_active": bool(r.get("is_active")),
            }
            for r in rows
        ]
    finally:
        conn.close()


def upsert_product_type_mappings(*, payload: dict):
    ensure_v2_schema()
    rules = payload.get("rules") or []
    if not isinstance(rules, list):
        raise ValueError("rules must be a list")

    conn = _connect()
    try:
        cur = conn.cursor()
        for item in rules:
            if not isinstance(item, dict):
                continue
            product_type = str(item.get("product_type") or "").strip().upper()
            if not product_type:
                raise ValueError("product_type is required")

            rule_id = str(item.get("id") or "").strip()
            if not rule_id:
                rule_id = str(uuid.uuid4())

            marketplace_id = str(item.get("marketplace_id") or "").strip() or None
            if marketplace_id and len(marketplace_id) <= 5:
                marketplace_id = _marketplace_to_id(marketplace_id)
            brand = str(item.get("brand") or "").strip() or None
            category = str(item.get("category") or "").strip() or None
            subcategory = str(item.get("subcategory") or "").strip() or None
            required_attrs = _normalize_required_attrs(item.get("required_attrs"))
            priority = int(item.get("priority") or 100)
            is_active = 1 if bool(item.get("is_active", True)) else 0

            cur.execute(
                """
                UPDATE dbo.acc_co_product_type_map
                SET marketplace_id = ?,
                    brand = ?,
                    category = ?,
                    subcategory = ?,
                    product_type = ?,
                    required_attrs_json = ?,
                    priority = ?,
                    is_active = ?,
                    updated_at = SYSUTCDATETIME()
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                (
                    marketplace_id,
                    brand,
                    category,
                    subcategory,
                    product_type,
                    json.dumps(required_attrs, ensure_ascii=True),
                    priority,
                    is_active,
                    rule_id,
                ),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO dbo.acc_co_product_type_map
                        (id, marketplace_id, brand, category, subcategory, product_type, required_attrs_json, priority, is_active, created_by)
                    VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rule_id,
                        marketplace_id,
                        brand,
                        category,
                        subcategory,
                        product_type,
                        json.dumps(required_attrs, ensure_ascii=True),
                        priority,
                        is_active,
                        settings.DEFAULT_ACTOR,
                    ),
                )

        conn.commit()
        return list_product_type_mappings()
    finally:
        conn.close()


def list_attribute_mappings():
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, marketplace_id, product_type, source_field, target_attribute,
                transform, priority, is_active
            FROM dbo.acc_co_attribute_map WITH (NOLOCK)
            ORDER BY priority ASC, created_at ASC
            """
        )
        rows = _fetchall_dict(cur)
        return [
            {
                "id": str(r.get("id")),
                "marketplace_id": r.get("marketplace_id"),
                "product_type": str(r.get("product_type") or "").upper() or None,
                "source_field": r.get("source_field"),
                "target_attribute": r.get("target_attribute"),
                "transform": str(r.get("transform") or "identity").lower(),
                "priority": int(r.get("priority") or 100),
                "is_active": bool(r.get("is_active")),
            }
            for r in rows
        ]
    finally:
        conn.close()


def upsert_attribute_mappings(*, payload: dict):
    ensure_v2_schema()
    rules = payload.get("rules") or []
    if not isinstance(rules, list):
        raise ValueError("rules must be a list")

    conn = _connect()
    try:
        cur = conn.cursor()
        for item in rules:
            if not isinstance(item, dict):
                continue
            source_field = str(item.get("source_field") or "").strip()
            target_attribute = str(item.get("target_attribute") or "").strip()
            if not source_field or not target_attribute:
                raise ValueError("source_field and target_attribute are required")
            transform = str(item.get("transform") or "identity").strip().lower()
            if transform not in _ATTRIBUTE_TRANSFORMS:
                raise ValueError(f"transform must be one of: {', '.join(sorted(_ATTRIBUTE_TRANSFORMS))}")

            rule_id = str(item.get("id") or "").strip() or str(uuid.uuid4())
            marketplace_id = str(item.get("marketplace_id") or "").strip() or None
            if marketplace_id and len(marketplace_id) <= 5:
                marketplace_id = _marketplace_to_id(marketplace_id)
            product_type = str(item.get("product_type") or "").strip().upper() or None
            priority = int(item.get("priority") or 100)
            is_active = 1 if bool(item.get("is_active", True)) else 0

            cur.execute(
                """
                UPDATE dbo.acc_co_attribute_map
                SET marketplace_id = ?,
                    product_type = ?,
                    source_field = ?,
                    target_attribute = ?,
                    transform = ?,
                    priority = ?,
                    is_active = ?,
                    updated_at = SYSUTCDATETIME()
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                (
                    marketplace_id,
                    product_type,
                    source_field,
                    target_attribute,
                    transform,
                    priority,
                    is_active,
                    rule_id,
                ),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO dbo.acc_co_attribute_map
                        (id, marketplace_id, product_type, source_field, target_attribute, transform, priority, is_active, created_by)
                    VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rule_id,
                        marketplace_id,
                        product_type,
                        source_field,
                        target_attribute,
                        transform,
                        priority,
                        is_active,
                        settings.DEFAULT_ACTOR,
                    ),
                )
        conn.commit()
        return list_attribute_mappings()
    finally:
        conn.close()


def list_product_type_definitions(*, marketplace: str | None = None, product_type: str | None = None):
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where_parts: list[str] = []
        params: list[Any] = []

        if marketplace:
            where_parts.append("marketplace_id = ?")
            params.append(_marketplace_to_id(str(marketplace).strip().upper()))
        if product_type:
            where_parts.append("product_type = ?")
            params.append(str(product_type).strip().upper())

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        cur.execute(
            f"""
            SELECT
                id, marketplace_id, marketplace_code, product_type,
                requirements_json, required_attrs_json, refreshed_at, source
            FROM dbo.acc_co_product_type_defs WITH (NOLOCK)
            {where_sql}
            ORDER BY refreshed_at DESC
            """,
            params,
        )
        rows = _fetchall_dict(cur)
        return [
            {
                "id": str(r.get("id")),
                "marketplace_id": r.get("marketplace_id"),
                "marketplace_code": r.get("marketplace_code") or _marketplace_to_code(str(r.get("marketplace_id") or "")),
                "product_type": str(r.get("product_type") or "").upper(),
                "requirements_json": _json_load(r.get("requirements_json")),
                "required_attrs": _normalize_required_attrs(r.get("required_attrs_json")),
                "refreshed_at": r.get("refreshed_at"),
                "source": r.get("source") or "sp_api_definitions",
            }
            for r in rows
        ]
    finally:
        conn.close()


def refresh_product_type_definition(*, payload: dict):
    ensure_v2_schema()
    market = str(payload.get("marketplace") or "").strip().upper()
    product_type = str(payload.get("product_type") or "").strip().upper()
    if not market:
        raise ValueError("marketplace is required")
    if not product_type:
        raise ValueError("product_type is required")
    market_id = _marketplace_to_id(market)
    if not market_id:
        raise ValueError("invalid marketplace")

    if not _spapi_ready():
        raise RuntimeError("SP-API not configured for product type definitions")

    async def _call():
        client = SPAPIClient(marketplace_id=market_id)
        return await client.get(
            f"/definitions/2020-09-01/productTypes/{product_type}",
            params={"marketplaceIds": market_id, "requirements": "LISTING"},
        )

    raw = _run_async(_call())
    required_attrs = _extract_required_attrs_from_definition_payload(raw)

    conn = _connect()
    try:
        cur = conn.cursor()
        definition_id = str(uuid.uuid4())
        cur.execute(
            """
            UPDATE dbo.acc_co_product_type_defs
            SET requirements_json = ?,
                required_attrs_json = ?,
                refreshed_at = SYSUTCDATETIME(),
                source = 'sp_api_definitions'
            WHERE marketplace_id = ?
              AND product_type = ?
            """,
            (
                json.dumps(raw or {}, ensure_ascii=True),
                json.dumps(required_attrs, ensure_ascii=True),
                market_id,
                product_type,
            ),
        )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO dbo.acc_co_product_type_defs
                    (id, marketplace_id, marketplace_code, product_type, requirements_json, required_attrs_json, source, refreshed_at)
                VALUES
                    (?, ?, ?, ?, ?, ?, 'sp_api_definitions', SYSUTCDATETIME())
                """,
                (
                    definition_id,
                    market_id,
                    market,
                    product_type,
                    json.dumps(raw or {}, ensure_ascii=True),
                    json.dumps(required_attrs, ensure_ascii=True),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    rows = list_product_type_definitions(marketplace=market, product_type=product_type)
    if not rows:
        raise RuntimeError("failed to refresh product type definition")
    return rows[0]


def get_publish_coverage(*, marketplaces: str, selection: str = "approved"):
    ensure_v2_schema()
    selection_value = _normalize_publish_selection(selection)
    markets = [str(x).strip().upper() for x in str(marketplaces or "").split(",") if str(x).strip()]
    if not markets:
        raise ValueError("marketplaces is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        from .publish import _load_push_candidates
        candidates = _load_push_candidates(
            cur,
            marketplaces=markets,
            selection=selection_value,
            sku_filter=[],
            version_ids=[],
        )
        pt_rules = _load_product_type_map_rules(cur)
        attr_rules = _load_attribute_mapping_rules(cur)

        bucket: dict[tuple[str, str, str], dict[str, Any]] = {}
        for r in candidates:
            market = str(r.get("marketplace_id") or "").strip()
            category = str(r.get("category") or "").strip() or None
            subcategory = str(r.get("subcategory") or "").strip() or None
            brand_hint = str(r.get("brand") or "").strip() or None
            fields = _json_load(r.get("fields_json"))
            product_type, required_attrs, _ = _resolve_native_product_type_and_requirements(
                sku=str(r.get("sku") or "").strip(),
                fields=fields,
                category_hint=category,
                subcategory_hint=subcategory,
                brand_hint=brand_hint,
                marketplace=market,
                mapping_rules=pt_rules,
            )
            ptd_state = "not_needed"
            if not required_attrs:
                ptd_state, required_attrs = _load_required_attrs_state_from_definition(
                    cur,
                    marketplace=market,
                    product_type=product_type,
                )
            attrs = _apply_attribute_registry(
                fields=fields,
                marketplace=market,
                product_type=product_type,
                rules=attr_rules,
            )
            missing = _attrs_missing_required(attrs, required_attrs)
            if ptd_state != "ok" and ptd_state != "not_needed":
                missing = [f"__ptd_{ptd_state}__"]

            key = (market, category or "-", product_type)
            if key not in bucket:
                bucket[key] = {
                    "marketplace_id": market,
                    "category": category,
                    "product_type": product_type,
                    "total_candidates": 0,
                    "fully_covered": 0,
                    "missing_counter": {},
                }
            item = bucket[key]
            item["total_candidates"] += 1
            if not missing:
                item["fully_covered"] += 1
            else:
                for attr in missing:
                    item["missing_counter"][attr] = int(item["missing_counter"].get(attr, 0)) + 1

        items: list[dict[str, Any]] = []
        for _, val in bucket.items():
            total = int(val["total_candidates"])
            covered = int(val["fully_covered"])
            missing_counter = val["missing_counter"]
            missing_top = sorted(missing_counter.items(), key=lambda x: (-x[1], x[0]))[:5]
            items.append(
                {
                    "marketplace_id": val["marketplace_id"],
                    "category": val["category"],
                    "product_type": val["product_type"],
                    "total_candidates": total,
                    "fully_covered": covered,
                    "coverage_pct": round((covered * 100.0 / total), 2) if total > 0 else 0.0,
                    "missing_required_top": [x[0] for x in missing_top],
                }
            )

        items.sort(key=lambda x: (x["marketplace_id"], x["category"] or "", x["product_type"]))
        return {"generated_at": datetime.now(timezone.utc), "items": items}
    finally:
        conn.close()


def get_publish_mapping_suggestions(*, marketplaces: str, selection: str = "approved", limit: int = 100):
    ensure_v2_schema()
    selection_value = _normalize_publish_selection(selection)
    markets = [str(x).strip().upper() for x in str(marketplaces or "").split(",") if str(x).strip()]
    if not markets:
        raise ValueError("marketplaces is required")
    safe_limit = max(1, min(int(limit or 100), 500))

    conn = _connect()
    try:
        cur = conn.cursor()
        from .publish import _load_push_candidates
        candidates = _load_push_candidates(
            cur,
            marketplaces=markets,
            selection=selection_value,
            sku_filter=[],
            version_ids=[],
        )
        pt_rules = _load_product_type_map_rules(cur)
        attr_rules = _load_attribute_mapping_rules(cur)

        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in candidates:
            market = str(row.get("marketplace_id") or "").strip()
            sku = str(row.get("sku") or "").strip()
            fields = _json_load(row.get("fields_json"))
            category = str(row.get("category") or "").strip() or None
            subcategory = str(row.get("subcategory") or "").strip() or None
            brand_hint = str(row.get("brand") or "").strip() or None
            product_type, required_attrs, _ = _resolve_native_product_type_and_requirements(
                sku=sku,
                fields=fields,
                category_hint=category,
                subcategory_hint=subcategory,
                brand_hint=brand_hint,
                marketplace=market,
                mapping_rules=pt_rules,
            )
            if not required_attrs:
                ptd_state, required_attrs = _load_required_attrs_state_from_definition(
                    cur,
                    marketplace=market,
                    product_type=product_type,
                )
                if ptd_state != "ok":
                    continue
            mapped_attrs = _apply_attribute_registry(
                fields=fields,
                marketplace=market,
                product_type=product_type,
                rules=attr_rules,
            )
            missing_required = _attrs_missing_required(mapped_attrs, required_attrs)
            if not missing_required:
                continue
            for missing in missing_required:
                key = (market, product_type, missing)
                item = grouped.setdefault(
                    key,
                    {
                        "marketplace_id": market,
                        "product_type": product_type,
                        "missing_attribute": missing,
                        "affected_skus": set(),
                        "source_hits": {},
                    },
                )
                item["affected_skus"].add(sku)
                source_fields = _suggest_source_fields_for_attr(
                    missing_attr=missing,
                    fields=fields,
                    product_type=product_type,
                )
                source_payload = {"fields": fields}
                for src in source_fields:
                    val = _get_nested_value(source_payload, src)
                    if _is_missing_value(val):
                        continue
                    item["source_hits"][src] = int(item["source_hits"].get(src, 0)) + 1

        items: list[dict[str, Any]] = []
        for data in grouped.values():
            affected = max(1, len(data["affected_skus"]))
            ranked = sorted(data["source_hits"].items(), key=lambda x: (-x[1], x[0]))
            best_field = ranked[0][0] if ranked else None
            confidence = round((ranked[0][1] * 100.0 / affected), 2) if ranked else 0.0
            items.append(
                {
                    "marketplace_id": data["marketplace_id"],
                    "product_type": data["product_type"],
                    "missing_attribute": data["missing_attribute"],
                    "suggested_source_field": best_field,
                    "confidence": confidence,
                    "candidates": [x[0] for x in ranked[:5]],
                    "affected_skus": len(data["affected_skus"]),
                }
            )

        items.sort(key=lambda x: (-x["affected_skus"], -x["confidence"], x["marketplace_id"], x["product_type"]))
        return {"generated_at": datetime.now(timezone.utc), "items": items[:safe_limit]}
    finally:
        conn.close()


def apply_publish_mapping_suggestions(*, payload: dict):
    ensure_v2_schema()
    markets = [str(x).strip().upper() for x in (payload.get("marketplaces") or []) if str(x).strip()]
    if not markets:
        raise ValueError("marketplaces is required")
    selection = _normalize_publish_selection(str(payload.get("selection") or "approved"))
    min_confidence = max(0.0, min(float(payload.get("min_confidence") or 70), 100.0))
    limit = max(1, min(int(payload.get("limit") or 100), 500))
    dry_run = bool(payload.get("dry_run", False))

    suggestions = get_publish_mapping_suggestions(
        marketplaces=",".join(markets),
        selection=selection,
        limit=limit,
    )
    items = suggestions.get("items") if isinstance(suggestions, dict) else []
    items = items if isinstance(items, list) else []

    created = 0
    skipped = 0
    out_items: list[dict[str, Any]] = []

    conn = _connect()
    try:
        cur = conn.cursor()
        for item in items:
            if not isinstance(item, dict):
                continue
            confidence = float(item.get("confidence") or 0)
            source_field = str(item.get("suggested_source_field") or "").strip()
            target_attr = str(item.get("missing_attribute") or "").strip()
            product_type = str(item.get("product_type") or "").strip().upper()
            marketplace_id = _marketplace_to_id(str(item.get("marketplace_id") or "").strip())
            if confidence < min_confidence or not source_field or not target_attr or not product_type:
                skipped += 1
                continue
            out_items.append(
                {
                    "marketplace_id": marketplace_id,
                    "product_type": product_type,
                    "source_field": source_field,
                    "target_attribute": target_attr,
                    "confidence": confidence,
                }
            )
            if dry_run:
                continue
            cur.execute(
                """
                SELECT TOP 1 id
                FROM dbo.acc_co_attribute_map WITH (NOLOCK)
                WHERE ISNULL(marketplace_id, '') = ISNULL(?, '')
                  AND ISNULL(product_type, '') = ISNULL(?, '')
                  AND source_field = ?
                  AND target_attribute = ?
                  AND is_active = 1
                """,
                (marketplace_id or None, product_type or None, source_field, target_attr),
            )
            existing = cur.fetchone()
            if existing:
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO dbo.acc_co_attribute_map
                    (id, marketplace_id, product_type, source_field, target_attribute, transform, priority, is_active, created_by)
                VALUES
                    (?, ?, ?, ?, ?, 'identity', 100, 1, ?)
                """,
                (
                    str(uuid.uuid4()),
                    marketplace_id or None,
                    product_type or None,
                    source_field,
                    target_attr,
                    settings.DEFAULT_ACTOR,
                ),
            )
            created += 1
        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    return {
        "generated_at": datetime.now(timezone.utc),
        "dry_run": dry_run,
        "evaluated": len(items),
        "created": created,
        "skipped": skipped,
        "items": out_items[:200],
    }

