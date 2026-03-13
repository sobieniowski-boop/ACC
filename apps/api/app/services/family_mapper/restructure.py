"""
Family Restructure — analyse and (later) execute family structure alignment
on non-DE marketplaces to mirror the DE canonical structure.

Phase 1 (current): ANALYSIS mode — dry-run comparison.
Phase 2 (future):  EXECUTE mode — DELETE foreign parent → CREATE DE parent → ASSIGN children.

Algorithm:
  1. Load DE canonical family (parent ASIN, children, variation_theme, brand, product_type).
  2. Load target MP state from marketplace_listing_child (current parent, theme, attributes).
  3. Compare: identify foreign parents, mismatched themes, missing children.
  4. Return structured analysis report.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

import structlog

from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

DE_MARKETPLACE = settings.SP_API_PRIMARY_MARKETPLACE  # A1PA6795UKMFR9

# Marketplace → language mapping for GPT translations
MARKETPLACE_LANGUAGE = {
    "A1PA6795UKMFR9": "German",
    "A1C3SOZRARQ6R3": "Polish",
    "A1RKKUPIHCS9HS": "Spanish",
    "A13V1IB3VIYZZH": "French",
    "A1805IZSGTT6HS": "Dutch",
    "APJ6JRA9NG5V4":  "Italian",
    "A2NODRKZP88ZB9": "Swedish",
    "AMEN7PMS3EDWL":  "French",      # Belgium → French (primary)
    "A28R8C7NBKEWEA": "English",     # Ireland
}

# Language ISO codes for SP-API locale parameter
MARKETPLACE_LOCALE = {
    "A1PA6795UKMFR9": "de_DE",
    "A1C3SOZRARQ6R3": "pl_PL",
    "A1RKKUPIHCS9HS": "es_ES",
    "A13V1IB3VIYZZH": "fr_FR",
    "A1805IZSGTT6HS": "nl_NL",
    "APJ6JRA9NG5V4":  "it_IT",
    "A2NODRKZP88ZB9": "sv_SE",
    "AMEN7PMS3EDWL":  "fr_BE",
    "A28R8C7NBKEWEA": "en_IE",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect():
    return connect_acc(autocommit=True)


def _ensure_restructure_run_table(cur) -> None:
    cur.execute(
        """
        IF OBJECT_ID('dbo.family_restructure_run', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.family_restructure_run (
                run_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                family_id INT NOT NULL,
                marketplace_id NVARCHAR(32) NOT NULL,
                marketplace_code NVARCHAR(8) NULL,
                dry_run BIT NOT NULL DEFAULT 0,
                status NVARCHAR(32) NOT NULL,
                progress_pct INT NOT NULL DEFAULT 0,
                children_total INT NOT NULL DEFAULT 0,
                children_done INT NOT NULL DEFAULT 0,
                progress_message NVARCHAR(500) NULL,
                result_json NVARCHAR(MAX) NULL,
                error_message NVARCHAR(MAX) NULL,
                created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                finished_at DATETIME2 NULL
            );

            CREATE INDEX IX_family_restructure_run_lookup
                ON dbo.family_restructure_run(family_id, marketplace_id, created_at DESC);
        END;
        """
    )


def create_restructure_run(
    family_id: int,
    marketplace_id: str,
    mp_code: str,
    *,
    dry_run: bool,
) -> str:
    run_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        _ensure_restructure_run_table(cur)
        cur.execute(
            """
            INSERT INTO dbo.family_restructure_run (
                run_id, family_id, marketplace_id, marketplace_code, dry_run,
                status, progress_pct, children_total, children_done, progress_message
            )
            VALUES (?, ?, ?, ?, ?, 'running', 0, 0, 0, 'Starting execute-restructure')
            """,
            run_id,
            family_id,
            marketplace_id,
            mp_code,
            1 if dry_run else 0,
        )
        conn.commit()
        return run_id
    finally:
        conn.close()


def update_restructure_run_progress(
    run_id: str,
    *,
    progress_pct: int,
    children_done: int,
    children_total: int,
    message: str,
) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        _ensure_restructure_run_table(cur)
        cur.execute(
            """
            UPDATE dbo.family_restructure_run
            SET progress_pct = ?,
                children_done = ?,
                children_total = ?,
                progress_message = ?,
                updated_at = SYSUTCDATETIME()
            WHERE run_id = ?
            """,
            max(0, min(int(progress_pct), 100)),
            max(0, int(children_done)),
            max(0, int(children_total)),
            (message or "")[:500],
            run_id,
        )
        conn.commit()
    finally:
        conn.close()


def finish_restructure_run(
    run_id: str,
    *,
    status: str,
    result: Optional[dict] = None,
    error_message: Optional[str] = None,
) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        _ensure_restructure_run_table(cur)
        cur.execute(
            """
            UPDATE dbo.family_restructure_run
            SET status = ?,
                progress_pct = CASE WHEN ? IN ('completed', 'completed_with_errors', 'already_aligned', 'nothing_to_do', 'no_data') THEN 100 ELSE progress_pct END,
                result_json = ?,
                error_message = ?,
                updated_at = SYSUTCDATETIME(),
                finished_at = SYSUTCDATETIME()
            WHERE run_id = ?
            """,
            status,
            status,
            json.dumps(result, ensure_ascii=False) if result is not None else None,
            error_message,
            run_id,
        )
        conn.commit()
    finally:
        conn.close()


def get_restructure_run_status(
    *,
    family_id: int,
    marketplace_id: str,
    run_id: Optional[str] = None,
) -> Optional[dict]:
    conn = _connect()
    try:
        cur = conn.cursor()
        _ensure_restructure_run_table(cur)
        if run_id:
            cur.execute(
                """
                SELECT TOP 1 run_id, family_id, marketplace_id, marketplace_code, dry_run,
                             status, progress_pct, children_total, children_done,
                             progress_message, result_json, error_message,
                             created_at, updated_at, finished_at
                FROM dbo.family_restructure_run WITH (NOLOCK)
                WHERE run_id = ?
                """,
                run_id,
            )
        else:
            cur.execute(
                """
                SELECT TOP 1 run_id, family_id, marketplace_id, marketplace_code, dry_run,
                             status, progress_pct, children_total, children_done,
                             progress_message, result_json, error_message,
                             created_at, updated_at, finished_at
                FROM dbo.family_restructure_run WITH (NOLOCK)
                WHERE family_id = ? AND marketplace_id = ?
                ORDER BY created_at DESC
                """,
                family_id,
                marketplace_id,
            )
        row = cur.fetchone()
        if not row:
            return None
        run_id_value = str(row[0])
        updated_at = row[13]
        if row[5] == "running" and updated_at is not None:
            updated_dt = updated_at
            if updated_dt.tzinfo is None:
                updated_dt = updated_dt.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - updated_dt).total_seconds() > 180:
                finish_restructure_run(
                    run_id_value,
                    status="failed",
                    result=None,
                    error_message="Execution interrupted: backend restarted or worker stopped.",
                )
                return get_restructure_run_status(
                    family_id=family_id,
                    marketplace_id=marketplace_id,
                    run_id=run_id,
                )

        result_json = None
        if row[10]:
            try:
                result_json = json.loads(row[10])
            except Exception:
                result_json = None
        return {
            "run_id": run_id_value,
            "family_id": int(row[1]),
            "marketplace_id": row[2],
            "marketplace": row[3],
            "dry_run": bool(row[4]),
            "status": row[5],
            "progress_pct": int(row[6] or 0),
            "children_total": int(row[7] or 0),
            "children_done": int(row[8] or 0),
            "progress_message": row[9],
            "result": result_json,
            "error_message": row[11],
            "created_at": str(row[12]) if row[12] else None,
            "updated_at": str(updated_at) if updated_at else None,
            "finished_at": str(row[14]) if row[14] else None,
        }
    finally:
        conn.close()


def _rewrite_marketplace_ids(
    attrs: dict, source_mp: str, target_mp: str,
) -> dict:
    """Deep-copy attributes, replacing marketplace_id values from source → target.

    SP-API attribute values are lists of dicts like:
      [{"marketplace_id": "A1PA6795UKMFR9", "value": "...", "language_tag": "de_DE"}]
    When copying DE attributes to FR, we must rewrite marketplace_id AND language_tag.
    """
    import copy
    source_locale = MARKETPLACE_LOCALE.get(source_mp, "de_DE")
    target_locale = MARKETPLACE_LOCALE.get(target_mp, "en_GB")
    result = copy.deepcopy(attrs)
    for attr_values in result.values():
        if not isinstance(attr_values, list):
            continue
        for entry in attr_values:
            if not isinstance(entry, dict):
                continue
            if entry.get("marketplace_id") == source_mp:
                entry["marketplace_id"] = target_mp
            if entry.get("language_tag") == source_locale:
                entry["language_tag"] = target_locale
            # Also fix nested dicts (e.g. unit_count.type has language_tag)
            for v in entry.values():
                if isinstance(v, dict) and v.get("language_tag") == source_locale:
                    v["language_tag"] = target_locale
    return result


def _sanitize_parent_attributes_for_target(attrs: dict, target_marketplace_id: str = None) -> dict:
    """Best-effort normalization to avoid frequent PTD validation errors.

    - Trim item_name to 200 chars (common max for title).
    - Drop unit_count when source locale values are not accepted on target MP.
    - Fix child_parent_sku_relationship to use parent_relationship_type for parent listings.
    """
    sanitized = dict(attrs or {})

    item_name_values = sanitized.get("item_name")
    if isinstance(item_name_values, list):
        for entry in item_name_values:
            if isinstance(entry, dict):
                value = entry.get("value")
                if isinstance(value, str) and len(value) > 200:
                    entry["value"] = value[:200].rstrip()

    # `unit_count.type.value` often carries locale-specific values (e.g. "stück")
    # rejected by target marketplace. It's optional for this flow, so drop it.
    if "unit_count" in sanitized:
        sanitized.pop("unit_count", None)

    # FIX: Parent listings must have parent_relationship_type, not child_relationship_type
    # When copying from DE parent, it may have incorrect child_relationship_type structure.
    # Replace with correct parent structure.
    if target_marketplace_id:
        sanitized["child_parent_sku_relationship"] = [{
            "marketplace_id": target_marketplace_id,
            "parent_relationship_type": "parent"
        }]

    return sanitized


# ---------------------------------------------------------------------------
# GPT Translation — translate parent attributes to target marketplace language
# ---------------------------------------------------------------------------

async def _translate_parent_attributes(
    attrs: dict,
    target_marketplace_id: str,
    product_type: str,
) -> tuple[dict, dict]:
    """Translate item_name, bullet_point, product_description from DE → target language.

    Returns (translated_attrs, translation_report).
    The translation_report contains what was translated and how.
    """
    import copy
    from openai import AsyncOpenAI

    target_lang = MARKETPLACE_LANGUAGE.get(target_marketplace_id, "English")
    mp_code = MARKETPLACE_REGISTRY.get(target_marketplace_id, {}).get("code", "??")

    # If target is also German, no translation needed
    if target_lang == "German":
        return attrs, {"status": "skipped", "reason": "Target is also German"}

    # Extract text values to translate — all German text fields, not just 4
    TEXT_FIELDS = (
        "item_name", "bullet_point", "product_description",
        "generic_keyword", "material", "pattern", "item_shape",
        "included_components", "model_name",
    )
    # Fields that should NOT be translated (brand stays as-is)
    SKIP_TRANSLATE = {"brand", "manufacturer"}
    fields_to_translate = {}
    for key in TEXT_FIELDS:
        if key in SKIP_TRANSLATE:
            continue
        values = attrs.get(key)
        if not values or not isinstance(values, list):
            continue
        texts = []
        for entry in values:
            if isinstance(entry, dict):
                val = entry.get("value")
                if val and isinstance(val, str) and val.strip():
                    texts.append(val)
        if texts:
            fields_to_translate[key] = texts

    if not fields_to_translate:
        return attrs, {"status": "skipped", "reason": "No translatable fields found"}

    # Build GPT prompt
    prompt_parts = [
        f"Translate the following Amazon product listing text from German to {target_lang}.",
        f"This is for marketplace {mp_code} (Amazon), product type: {product_type}.",
        "Keep the translated text professional, SEO-optimized, and suitable for Amazon listings.",
        "Return a JSON object with the same keys, each containing a list of translated strings in the same order.",
        "Do NOT add or remove bullet points. Preserve the number of items in each list.",
        "",
        "Input:",
        json.dumps(fields_to_translate, ensure_ascii=False, indent=2),
    ]

    client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY, timeout=60.0)
    try:
        resp = await client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            max_completion_tokens=settings.OPENAI_MAX_TOKENS,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": (
                    "You are a professional Amazon product listing translator. "
                    "You translate German product titles, bullet points, and descriptions "
                    "into the target language while maintaining SEO quality and Amazon guidelines."
                )},
                {"role": "user", "content": "\n".join(prompt_parts)},
            ],
            temperature=0.2,
        )
        content = resp.choices[0].message.content or "{}"
        translated = json.loads(content)
    except Exception as e:
        log.error("restructure.translation_failed", error=str(e), mp=mp_code)
        return attrs, {"status": "error", "error": str(e)}

    # Apply translations back into attributes
    result = copy.deepcopy(attrs)
    translated_fields = []
    for key, original_texts in fields_to_translate.items():
        new_texts = translated.get(key, [])
        if not new_texts or len(new_texts) != len(original_texts):
            log.warning("restructure.translation_mismatch",
                        key=key, expected=len(original_texts), got=len(new_texts))
            continue
        values = result.get(key, [])
        text_idx = 0
        for entry in values:
            if isinstance(entry, dict) and entry.get("value") and text_idx < len(new_texts):
                entry["value"] = new_texts[text_idx]
                text_idx += 1
        translated_fields.append(key)

    report = {
        "status": "ok",
        "source_language": "German",
        "target_language": target_lang,
        "marketplace": mp_code,
        "translated_fields": translated_fields,
        "field_counts": {k: len(v) for k, v in fields_to_translate.items()},
    }
    log.info("restructure.translation_done", **report)
    return result, report


# ---------------------------------------------------------------------------
# Variation Theme Validation via SP-API Product Type Definitions
# ---------------------------------------------------------------------------

async def _validate_variation_theme(
    target_marketplace_id: str,
    product_type: str,
    desired_theme: str,
) -> dict:
    """Check if desired variation_theme is valid for productType on target MP.

    Uses SP-API Product Type Definitions API.
    Returns a report with: valid, allowed_themes, recommendation.
    """
    from app.connectors.amazon_sp_api.listings import ListingsClient

    mp_code = MARKETPLACE_REGISTRY.get(target_marketplace_id, {}).get("code", "??")
    locale = MARKETPLACE_LOCALE.get(target_marketplace_id, "DEFAULT")
    client = ListingsClient(marketplace_id=target_marketplace_id)

    try:
        ptd = await client.get_product_type_definition(
            product_type, locale=locale,
        )
    except Exception as e:
        log.warning("restructure.ptd_fetch_failed", error=str(e),
                    product_type=product_type, mp=mp_code)
        return {
            "status": "error",
            "error": str(e),
            "recommendation": "use_de_theme",
            "desired_theme": desired_theme,
        }

    # Check if variation_theme is in propertyGroups (confirms it's supported)
    property_groups = ptd.get("propertyGroups", {})
    vt_supported = False
    for group_data in property_groups.values():
        prop_names = group_data.get("propertyNames", [])
        if "variation_theme" in prop_names:
            vt_supported = True
            break

    # Try to find allowed values from JSON schema
    schema = ptd.get("schema", {})
    allowed_themes: list[str] = []

    # Check inline schema properties
    properties = schema.get("properties", {})
    vt_prop = properties.get("variation_theme", {})
    if vt_prop:
        items = vt_prop.get("items", {})
        if items:
            val_prop = items.get("properties", {}).get("value", {})
            enum_vals = val_prop.get("enum", [])
            if enum_vals:
                allowed_themes = enum_vals
        if not allowed_themes:
            enum_vals = vt_prop.get("enum", [])
            if enum_vals:
                allowed_themes = enum_vals

    # If no inline enum but schema links to external schema, try fetching it
    if not allowed_themes:
        meta_schema = ptd.get("metaSchema", {})
        schema_link = meta_schema.get("link", {}).get("resource") if isinstance(meta_schema, dict) else None
        # The linked schema is the meta-schema, not per-product-type.
        # Variation themes are typically validated at submission time by Amazon.
        # We rely on propertyGroups to confirm the concept is supported.

    if allowed_themes:
        # Normalize for comparison (case-insensitive)
        desired_upper = desired_theme.upper().replace(" ", "")
        allowed_upper = {t.upper().replace(" ", ""): t for t in allowed_themes}

        if desired_upper in allowed_upper:
            return {
                "status": "valid",
                "desired_theme": desired_theme,
                "matched_theme": allowed_upper[desired_upper],
                "allowed_themes": allowed_themes,
                "recommendation": "use_desired",
            }

        # Try partial match (e.g., COLOR_NAME/SIZE_NAME vs COLOR/SIZE)
        for au_key, au_val in allowed_upper.items():
            if ("COLOR" in au_key and "SIZE" in au_key and
                    "COLOR" in desired_upper and "SIZE" in desired_upper):
                return {
                    "status": "equivalent_found",
                    "desired_theme": desired_theme,
                    "matched_theme": au_val,
                    "allowed_themes": allowed_themes,
                    "recommendation": "use_equivalent",
                }

        return {
            "status": "invalid",
            "desired_theme": desired_theme,
            "allowed_themes": allowed_themes,
            "recommendation": "use_de_theme",
            "reason": f"{desired_theme} not found in allowed themes for {product_type} on {mp_code}",
        }

    # No enum available but variation_theme is in property groups → assume valid
    if vt_supported:
        return {
            "status": "supported",
            "desired_theme": desired_theme,
            "allowed_themes": [],
            "recommendation": "use_desired",
            "reason": f"variation_theme is supported for {product_type} on {mp_code} (exact values validated at submission)",
        }

    return {
        "status": "unknown",
        "desired_theme": desired_theme,
        "allowed_themes": [],
        "recommendation": "use_de_theme",
        "reason": f"variation_theme not found in PTD for {product_type} on {mp_code}",
    }


# ---------------------------------------------------------------------------
# Child Attribute Audit — check size/color completeness on target MP
# ---------------------------------------------------------------------------

async def _audit_child_attributes(
    target_marketplace_id: str,
    child_skus: list[str],
) -> dict:
    """Audit size/color attributes on ALL children on target MP.

    GETs every child listing on target MP (concurrent, semaphore-limited)
    and checks if color_name/size_name/size attributes are present.

    Returns audit report with: total_checked, missing_color, missing_size,
    details per child.
    """
    import asyncio
    from app.connectors.amazon_sp_api.listings import ListingsClient

    mp_code = MARKETPLACE_REGISTRY.get(target_marketplace_id, {}).get("code", "??")
    target_lang = MARKETPLACE_LANGUAGE.get(target_marketplace_id, "English")
    client = ListingsClient(marketplace_id=target_marketplace_id)
    seller_id = client.seller_id

    children_details: list[dict] = []
    missing_color = 0
    missing_size = 0
    total_checked = 0
    sem = asyncio.Semaphore(5)  # SP-API rate limit ~5 req/s
    lock = asyncio.Lock()

    async def _check_one(sku: str) -> None:
        nonlocal missing_color, missing_size, total_checked
        async with sem:
            try:
                listing = await client.get_listings_item(
                    seller_id, sku, included_data="attributes",
                )
                attrs = listing.get("attributes", {})

                color_val = None
                for color_key in ("color_name", "color", "color_map"):
                    vals = attrs.get(color_key, [])
                    if vals and isinstance(vals, list):
                        for entry in vals:
                            if isinstance(entry, dict) and entry.get("value"):
                                color_val = entry["value"]
                                break
                    if color_val:
                        break

                size_val = None
                for size_key in ("size_name", "size", "size_map"):
                    vals = attrs.get(size_key, [])
                    if vals and isinstance(vals, list):
                        for entry in vals:
                            if isinstance(entry, dict) and entry.get("value"):
                                size_val = entry["value"]
                                break
                    if size_val:
                        break

                has_color = color_val is not None
                has_size = size_val is not None
                detail = {
                    "sku": sku,
                    "color": color_val,
                    "size": size_val,
                    "has_color": has_color,
                    "has_size": has_size,
                }
            except Exception as e:
                has_color = False
                has_size = False
                detail = {
                    "sku": sku,
                    "error": str(e),
                    "has_color": False,
                    "has_size": False,
                }

            async with lock:
                if not has_color:
                    missing_color += 1
                if not has_size:
                    missing_size += 1
                total_checked += 1
                children_details.append(detail)

            await asyncio.sleep(0.2)

    # Process all children in batches of 20
    batch_size = 20
    for i in range(0, len(child_skus), batch_size):
        batch = child_skus[i : i + batch_size]
        await asyncio.gather(*[_check_one(sku) for sku in batch])

    return {
        "status": "ok",
        "marketplace": mp_code,
        "target_language": target_lang,
        "total_skus": len(child_skus),
        "sample_checked": total_checked,
        "missing_color": missing_color,
        "missing_size": missing_size,
        "all_color_ok": missing_color == 0,
        "all_size_ok": missing_size == 0,
        "children": children_details,
    }


def _audit_reason(audit: dict) -> str:
    """Build human-readable audit reason string."""
    checked = audit.get("sample_checked", 0)
    total = audit.get("total_skus", 0)
    mc = audit.get("missing_color", 0)
    ms = audit.get("missing_size", 0)
    color_str = "OK" if mc == 0 else f"{mc} missing"
    size_str = "OK" if ms == 0 else f"{ms} missing"
    return f"Checked {checked}/{total} children: color {color_str}, size {size_str}"


def _first_attr_value(attrs: dict, keys: tuple[str, ...]) -> str | None:
    """Return first non-empty attribute value from candidate keys."""
    for key in keys:
        vals = attrs.get(key, [])
        if not isinstance(vals, list):
            continue
        for item in vals:
            if isinstance(item, dict):
                val = item.get("value")
                if isinstance(val, str) and val.strip():
                    return val.strip()
    return None


def _validate_attr_uniqueness(
    audit_children: list[dict],
    enrichment_children: list[dict] | None = None,
) -> dict:
    """Check for duplicate (color, size) pairs among children.

    Merges audit data with enrichment results to get the latest values,
    then checks that each (color, size) tuple is unique.
    Returns report with duplicate pairs and affected SKUs.
    """
    sku_attrs: dict[str, dict] = {}
    for child in audit_children:
        sku = child.get("sku")
        if sku:
            sku_attrs[sku] = {
                "color": child.get("color"),
                "size": child.get("size"),
            }

    # Overlay enrichment results (parsed from "filled" / "would_fill" field)
    if enrichment_children:
        for child in enrichment_children:
            sku = child.get("sku")
            if not sku or sku not in sku_attrs:
                continue
            filled = child.get("filled") or child.get("would_fill") or []
            for f in filled:
                if f.startswith("color="):
                    sku_attrs[sku]["color"] = f.split("=", 1)[1]
                elif f.startswith("size="):
                    sku_attrs[sku]["size"] = f.split("=", 1)[1]

    # Check for duplicate (color, size) tuples
    pair_to_skus: dict[tuple, list[str]] = {}
    for sku, attrs in sku_attrs.items():
        color = (attrs.get("color") or "").strip().lower()
        size = (attrs.get("size") or "").strip().lower()
        if not color and not size:
            continue
        pair = (color, size)
        pair_to_skus.setdefault(pair, []).append(sku)

    duplicates = {
        f"{pair[0]}|{pair[1]}": skus
        for pair, skus in pair_to_skus.items()
        if len(skus) > 1
    }

    return {
        "status": "warning" if duplicates else "ok",
        "total_checked": len(sku_attrs),
        "unique_pairs": len(pair_to_skus),
        "duplicate_count": len(duplicates),
        "duplicates": {k: v[:5] for k, v in duplicates.items()},
        "reason": (
            f"{len(duplicates)} duplicate (color,size) pairs found"
            if duplicates
            else f"All {len(pair_to_skus)} (color,size) pairs are unique"
        ),
    }


async def _detect_ghost_parents(
    target_marketplace_id: str,
    child_asins: list[str],
    known_parent_asins: set[str],
) -> list[str]:
    """Detect catalog-level parent ASINs not managed by our seller.

    Uses Catalog API relationships on sample children to discover
    parents that exist at catalog level but aren't in our seller listings.
    Returns list of ghost parent ASINs.
    """
    import asyncio
    from app.connectors.amazon_sp_api.catalog import CatalogClient

    catalog_client = CatalogClient(marketplace_id=target_marketplace_id)
    discovered_parents: set[str] = set()

    sample = child_asins[:5]
    for asin in sample:
        try:
            data = await catalog_client.get_item(asin, included_data="relationships")
            relationships = data.get("relationships", [])
            for rel_group in relationships:
                if not isinstance(rel_group, dict):
                    continue
                for rel in rel_group.get("relationships", []):
                    if not isinstance(rel, dict):
                        continue
                    for pa in rel.get("parentAsins", []):
                        if pa and pa not in known_parent_asins:
                            discovered_parents.add(pa)
            await asyncio.sleep(0.2)
        except Exception:
            continue

    return list(discovered_parents)


async def _claim_and_delete_ghost_parent(
    target_marketplace_id: str,
    ghost_asin: str,
    product_type: str,
) -> dict:
    """Claim a ghost catalog parent with a temp SKU, then delete it."""
    import asyncio
    from app.connectors.amazon_sp_api.listings import ListingsClient

    client = ListingsClient(marketplace_id=target_marketplace_id)
    seller_id = client.seller_id
    temp_sku = f"GHOST-CLEANUP-{ghost_asin[:8]}-{uuid.uuid4().hex[:6].upper()}"

    put_body = {
        "productType": product_type,
        "requirements": "LISTING",
        "attributes": {
            "condition_type": [{
                "marketplace_id": target_marketplace_id,
                "value": "new_new",
            }],
            "merchant_suggested_asin": [{
                "marketplace_id": target_marketplace_id,
                "value": ghost_asin,
            }],
        },
    }

    try:
        put_result = await client.put_listings_item(seller_id, temp_sku, put_body)
        put_status = put_result.get("status", "UNKNOWN")
        await asyncio.sleep(1)

        del_result = await client.delete_listings_item(seller_id, temp_sku)
        del_status = del_result.get("status", "UNKNOWN")

        return {
            "ghost_asin": ghost_asin,
            "temp_sku": temp_sku,
            "put_status": put_status,
            "delete_status": del_status,
            "status": "ok" if put_status == "ACCEPTED" and del_status == "ACCEPTED" else "partial",
        }
    except Exception as e:
        return {
            "ghost_asin": ghost_asin,
            "temp_sku": temp_sku,
            "status": "error",
            "error": str(e),
        }


async def _enrich_children_from_de(
    target_marketplace_id: str,
    audit_children: list[dict],
    actionable: list[dict],
    product_type: str,
    *,
    dry_run: bool = True,
) -> dict:
    """Query DE marketplace for missing color/size and optionally PATCH them.

    For children where audit found missing color or size, fetches the
    same SKU from DE marketplace and extracts color/size values.
    Translates from German to target language via GPT.
    In non-dry-run mode, PATCHes the attributes onto the target MP listing.

    Returns enrichment report with per-child details.
    """
    import asyncio
    from app.connectors.amazon_sp_api.listings import ListingsClient

    mp_code = MARKETPLACE_REGISTRY.get(target_marketplace_id, {}).get("code", "??")
    target_lang = MARKETPLACE_LANGUAGE.get(target_marketplace_id, "English")

    # Identify children with missing color or size from audit
    missing_children = [
        c for c in audit_children
        if not c.get("has_color") or not c.get("has_size")
    ]
    if not missing_children:
        return {
            "status": "ok",
            "reason": "All children have color and size — no enrichment needed.",
            "total_missing": 0,
            "de_found": 0,
            "patched": 0,
            "children": [],
        }

    # Build SKU/ASIN maps from actionable
    sku_to_asin = {a["sku"]: a["asin"] for a in actionable}
    asin_to_sku = {a["asin"]: a["sku"] for a in actionable if a.get("asin") and a.get("sku")}
    missing_sku_map = {}  # sku→child_audit_entry with asin
    for c in missing_children:
        asin = sku_to_asin.get(c["sku"])
        if asin:
            missing_sku_map[c["sku"]] = {**c, "asin": asin}

    # Build fallback DE SKU map by ASIN (handles prefixed SKUs like amzn.gr.*)
    asin_to_de_sku: dict[str, str] = {}
    if missing_sku_map:
        asins = [v["asin"] for v in missing_sku_map.values() if v.get("asin")]
        if asins:
            conn = _connect()
            cur = conn.cursor()
            try:
                ph = ",".join(["?"] * len(asins))
                cur.execute(
                    f"""
                    SELECT asin, merchant_sku
                    FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
                    WHERE asin IN ({ph})
                      AND merchant_sku IS NOT NULL
                    """,
                    *asins,
                )
                for asin, sku in cur.fetchall():
                    if asin and sku:
                        asin_to_de_sku[str(asin)] = str(sku)

                unresolved = [a for a in asins if a not in asin_to_de_sku]
                if unresolved:
                    ph2 = ",".join(["?"] * len(unresolved))
                    cur.execute(
                        f"""
                        SELECT asin, sku
                        FROM dbo.acc_product WITH (NOLOCK)
                        WHERE asin IN ({ph2})
                          AND sku IS NOT NULL
                        """,
                        *unresolved,
                    )
                    for asin, sku in cur.fetchall():
                        if asin and sku and str(asin) not in asin_to_de_sku:
                            asin_to_de_sku[str(asin)] = str(sku)
            finally:
                conn.close()

    # Fetch listings from DE marketplace
    de_client = ListingsClient(marketplace_id=DE_MARKETPLACE)
    seller_id = de_client.seller_id
    de_listings = {}

    for sku, child_info in missing_sku_map.items():
        asin = child_info.get("asin")
        fallback_sku = asin_to_de_sku.get(str(asin)) if asin else None
        de_sku_candidates = [sku]
        if fallback_sku and fallback_sku != sku:
            de_sku_candidates.append(fallback_sku)

        attrs = None
        used_de_sku = None
        try:
            for de_sku in de_sku_candidates:
                try:
                    de_listing = await de_client.get_listings_item(
                        seller_id, de_sku, included_data="attributes"
                    )
                    attrs = de_listing.get("attributes", {})
                    used_de_sku = de_sku
                    break
                except Exception:
                    continue
            if not attrs:
                log.warning(
                    "restructure.de_fetch_failed",
                    sku=sku,
                    asin=asin,
                    fallback_sku=fallback_sku,
                )
            de_listings[sku] = {"attrs": attrs, "source_sku": used_de_sku}
        except Exception as e:
            log.warning("restructure.de_fetch_failed", sku=sku, asin=asin, error=str(e))
            de_listings[sku] = {"attrs": None, "source_sku": None}
        await asyncio.sleep(0.2)  # Rate limiting

    log.info("restructure.de_lookup.done", 
             requested=len(missing_sku_map), found=len([v for v in de_listings.values() if v]))

    # Extract color/size values from DE listings
    de_values_to_translate = set()
    for sku, payload in de_listings.items():
        attrs = payload.get("attrs") if isinstance(payload, dict) else None
        if not isinstance(attrs, dict):
            continue
        color_val = _first_attr_value(attrs, ("color_name", "color", "color_map"))
        size_val = _first_attr_value(attrs, ("size_name", "size", "size_map"))
        if color_val:
            de_values_to_translate.add(color_val)
        if size_val:
            de_values_to_translate.add(size_val)

    # Translate DE values to target language via GPT
    translated_map: dict[str, str] = {}
    if de_values_to_translate and target_lang not in ("German", "Polish"):
        try:
            import openai
            oai = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY, timeout=30.0)
            values_list = sorted(de_values_to_translate)
            prompt = (
                f"Translate these German product attribute values (colors, sizes) to {target_lang}. "
                f"Return ONLY a JSON object mapping original→translated. "
                f"Keep numeric values and measurement units as-is (e.g. '16 cm' stays '16 cm'). "
                f"Values: {values_list}"
            )
            resp = await oai.chat.completions.create(
                model=settings.OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=1000,
                temperature=0.1,
            )
            import json as _json
            raw = resp.choices[0].message.content.strip()
            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                if raw.endswith("```"):
                    raw = raw[:-3].strip()
            translated_map = _json.loads(raw)
            log.info("restructure.de_translate", count=len(translated_map), 
                    source="German", target=target_lang)
        except Exception as e:
            log.warning("restructure.de_translate_failed", error=str(e))
            # Fallback: use German values directly
            translated_map = {v: v for v in de_values_to_translate}
    else:
        # No translation needed or German marketplace
        translated_map = {v: v for v in de_values_to_translate}

    # Build enrichment plan and optionally PATCH
    target_client = ListingsClient(marketplace_id=target_marketplace_id)
    target_locale = MARKETPLACE_LOCALE.get(target_marketplace_id, "en_GB")
    enriched_children = []
    patched = 0

    # Catalog API client for fallback when DE has no color/size
    from app.connectors.amazon_sp_api.catalog import CatalogClient
    catalog_client = CatalogClient(marketplace_id=target_marketplace_id)

    for sku, child_info in missing_sku_map.items():
        asin = child_info["asin"]
        de_payload = de_listings.get(sku) or {}
        de_attrs = de_payload.get("attrs") if isinstance(de_payload, dict) else None
        source_sku = de_payload.get("source_sku") if isinstance(de_payload, dict) else None

        patches = []
        filled_fields = []

        # Try DE marketplace first
        if de_attrs:
            # Color: fill if missing in audit AND DE has it
            if not child_info.get("has_color"):
                color_de = _first_attr_value(de_attrs, ("color_name", "color", "color_map"))
                if color_de:
                    color_translated = translated_map.get(color_de, color_de)
                    patches.append({
                        "op": "replace",
                        "path": "/attributes/color",
                        "value": [{
                            "marketplace_id": target_marketplace_id,
                            "language_tag": target_locale,
                            "value": color_translated,
                        }],
                    })
                    filled_fields.append(f"color={color_translated}")

            # Size: fill if missing in audit AND DE has it
            if not child_info.get("has_size"):
                size_de = _first_attr_value(de_attrs, ("size_name", "size", "size_map"))
                if size_de:
                    size_translated = translated_map.get(size_de, size_de)
                    patches.append({
                        "op": "replace",
                        "path": "/attributes/size",
                        "value": [{
                            "marketplace_id": target_marketplace_id,
                            "language_tag": target_locale,
                            "value": size_translated,
                        }],
                    })
                    filled_fields.append(f"size={size_translated}")

        # Catalog API fallback when DE didn't provide all needed values
        still_missing_color = not child_info.get("has_color") and not any(
            f.startswith("color=") for f in filled_fields
        )
        still_missing_size = not child_info.get("has_size") and not any(
            f.startswith("size=") for f in filled_fields
        )
        if still_missing_color or still_missing_size:
            try:
                cat_data = await catalog_client.get_item(asin, included_data="attributes")
                cat_attrs = cat_data.get("attributes", {})
                if still_missing_color:
                    cat_color = _first_attr_value(cat_attrs, ("color_name", "color", "color_map"))
                    if cat_color:
                        patches.append({
                            "op": "replace",
                            "path": "/attributes/color",
                            "value": [{
                                "marketplace_id": target_marketplace_id,
                                "language_tag": target_locale,
                                "value": cat_color,
                            }],
                        })
                        filled_fields.append(f"color={cat_color}")
                if still_missing_size:
                    cat_size = _first_attr_value(cat_attrs, ("size_name", "size", "size_map"))
                    if cat_size:
                        patches.append({
                            "op": "replace",
                            "path": "/attributes/size",
                            "value": [{
                                "marketplace_id": target_marketplace_id,
                                "language_tag": target_locale,
                                "value": cat_size,
                            }],
                        })
                        filled_fields.append(f"size={cat_size}")
                await asyncio.sleep(0.2)
            except Exception as e_cat:
                log.debug("restructure.catalog_fallback_failed", asin=asin, error=str(e_cat))

        if not patches:
            enriched_children.append({
                "sku": sku,
                "asin": asin,
                "status": "no_source",
                "source_de_sku": source_sku,
                "reason": "Neither DE listing nor Catalog API had color/size values",
            })
            continue

        # Execute PATCH if not dry-run
        if not dry_run:
            try:
                result = await target_client.patch_listings_item(
                    seller_id, sku, patches,
                    product_type=product_type,
                )
                patch_status = result.get("status", "UNKNOWN")
                enriched_children.append({
                    "sku": sku,
                    "asin": asin,
                    "source_de_sku": source_sku,
                    "status": patch_status.lower(),
                    "filled": filled_fields,
                    "submission_id": result.get("submissionId"),
                })
                if patch_status == "ACCEPTED":
                    patched += 1
                await asyncio.sleep(0.2)
            except Exception as e:
                enriched_children.append({
                    "sku": sku,
                    "asin": asin,
                    "source_de_sku": source_sku,
                    "status": "error",
                    "error": str(e),
                })
        else:
            # Dry-run: just record what would be filled
            enriched_children.append({
                "sku": sku,
                "asin": asin,
                "source_de_sku": source_sku,
                "status": "dry_run",
                "would_fill": filled_fields,
            })
            patched += 1  # Count as "would patch" in dry run

    return {
        "status": "ok" if patched > 0 else "warning",
        "reason": f"Enriched {patched}/{len(missing_sku_map)} children from DE marketplace",
        "total_missing": len(missing_sku_map),
        "de_found": len([
            v for v in de_listings.values()
            if isinstance(v, dict) and isinstance(v.get("attrs"), dict) and v.get("attrs")
        ]),
        "patched": patched,
        "target_language": target_lang,
        "children": enriched_children[:20],  # Limit output size
    }


async def targeted_repair_missing_child_attrs(
    family_id: int,
    target_marketplace_id: str,
    *,
    dry_run: bool = True,
) -> dict:
    """Repair only missing color/size attrs for target marketplace children.

    This function does NOT reassign parent relations. It only audits children
    and PATCHes missing color_name/size_name values from DE with ASIN fallback.
    """
    from app.connectors.amazon_sp_api.listings import ListingsClient

    analysis = await analyze_restructure(family_id, target_marketplace_id)
    if "error" in analysis:
        return {"status": "error", "error": analysis["error"]}

    mp_code = analysis.get("marketplace", target_marketplace_id)
    target_children = analysis.get("target_state", {}).get("children", [])
    if not target_children:
        return {
            "status": "no_data",
            "family_id": family_id,
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "message": "No target children found.",
        }

    # Resolve SKUs for all target children
    conn = _connect()
    cur = conn.cursor()
    child_asins = [c.get("asin") for c in target_children if c.get("asin")]
    placeholders = ",".join(["?"] * len(child_asins)) if child_asins else ""
    child_sku_map: dict[str, str] = {}
    try:
        if child_asins:
            cur.execute(
                f"SELECT asin, merchant_sku "
                f"FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) "
                f"WHERE asin IN ({placeholders})",
                *child_asins,
            )
            for asin, sku in cur.fetchall():
                if asin and sku:
                    child_sku_map[str(asin)] = str(sku)

            missing = [a for a in child_asins if a not in child_sku_map]
            if missing:
                ph2 = ",".join(["?"] * len(missing))
                cur.execute(
                    f"SELECT asin, sku "
                    f"FROM dbo.acc_product WITH (NOLOCK) "
                    f"WHERE asin IN ({ph2}) AND sku IS NOT NULL",
                    *missing,
                )
                for asin, sku in cur.fetchall():
                    if asin and sku and str(asin) not in child_sku_map:
                        child_sku_map[str(asin)] = str(sku)
    finally:
        conn.close()

    actionable = []
    for child in target_children:
        asin = child.get("asin")
        sku = child_sku_map.get(str(asin)) if asin else None
        if asin and sku and not str(sku).lower().startswith("amzn.gr."):
            actionable.append({"asin": asin, "sku": sku})

    if not actionable:
        return {
            "status": "no_data",
            "family_id": family_id,
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "message": "No actionable child SKUs found.",
        }

    # Detect product type from first actionable child on DE
    de_client = ListingsClient(marketplace_id=DE_MARKETPLACE)
    seller_id = de_client.seller_id
    product_type = "PRODUCT"
    try:
        de_child = await de_client.get_listings_item(
            seller_id, actionable[0]["sku"], included_data="summaries"
        )
        summaries = de_child.get("summaries", [])
        if summaries:
            product_type = summaries[0].get("productType", "PRODUCT")
    except Exception:
        pass

    # Audit and enrich only missing attrs
    attr_audit_before = await _audit_child_attributes(
        target_marketplace_id,
        [a["sku"] for a in actionable],
    )

    enrichment = await _enrich_children_from_de(
        target_marketplace_id,
        attr_audit_before.get("children", []),
        actionable,
        product_type,
        dry_run=dry_run,
    )

    attr_audit_after = None
    if not dry_run:
        attr_audit_after = await _audit_child_attributes(
            target_marketplace_id,
            [a["sku"] for a in actionable],
        )

    return {
        "status": "completed",
        "dry_run": dry_run,
        "family_id": family_id,
        "marketplace": mp_code,
        "marketplace_id": target_marketplace_id,
        "children_total": len(actionable),
        "attr_audit_before": {
            "sample_checked": attr_audit_before.get("sample_checked"),
            "missing_color": attr_audit_before.get("missing_color"),
            "missing_size": attr_audit_before.get("missing_size"),
        },
        "enrichment": enrichment,
        "attr_audit_after": {
            "sample_checked": attr_audit_after.get("sample_checked"),
            "missing_color": attr_audit_after.get("missing_color"),
            "missing_size": attr_audit_after.get("missing_size"),
        } if attr_audit_after else None,
    }


async def _enrich_children_from_pim(
    target_marketplace_id: str,
    audit_children: list[dict],
    actionable: list[dict],
    product_type: str,
    *,
    dry_run: bool = True,
) -> dict:
    """Query Ergonode PIM for missing color/size and optionally PATCH them.

    For children where audit found missing color or size, looks up the
    PIM data and translates values via GPT to the target language.
    In non-dry-run mode, PATCHes the attributes onto the target MP listing.

    Returns enrichment report with per-child details.
    """
    import asyncio
    from app.connectors.ergonode import fetch_ergonode_variant_lookup
    from app.connectors.amazon_sp_api.listings import ListingsClient

    mp_code = MARKETPLACE_REGISTRY.get(target_marketplace_id, {}).get("code", "??")
    target_lang = MARKETPLACE_LANGUAGE.get(target_marketplace_id, "English")

    # Identify children with missing color or size from audit
    missing_children = [
        c for c in audit_children
        if not c.get("has_color") or not c.get("has_size")
    ]
    if not missing_children:
        return {
            "status": "ok",
            "reason": "All children have color and size — no PIM enrichment needed.",
            "total_missing": 0,
            "pim_found": 0,
            "patched": 0,
            "children": [],
        }

    # Build ASIN set from actionable (sku→asin map)
    sku_to_asin = {a["sku"]: a["asin"] for a in actionable}
    missing_asins = set()
    missing_sku_map = {}  # sku→child_audit_entry
    for c in missing_children:
        asin = sku_to_asin.get(c["sku"])
        if asin:
            missing_asins.add(asin)
            missing_sku_map[c["sku"]] = {**c, "asin": asin}

    # Query Ergonode PIM
    pim_lookup = await fetch_ergonode_variant_lookup(missing_asins)

    # Translate PIM values to target language if needed (batch via GPT)
    pim_values_to_translate = set()
    for asin, pim_data in pim_lookup.items():
        for field in ("color", "size", "quantity"):
            val = pim_data.get(field)
            if val:
                pim_values_to_translate.add(val)

    translated_map: dict[str, str] = {}
    if pim_values_to_translate and target_lang != "Polish":
        try:
            import openai
            oai = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
            values_list = sorted(pim_values_to_translate)
            prompt = (
                f"Translate these Polish product attribute values to {target_lang}. "
                f"Return ONLY a JSON object mapping original→translated. "
                f"Keep numeric values and measurement units as-is (e.g. '16 cm' stays '16 cm'). "
                f"Values: {values_list}"
            )
            resp = await oai.chat.completions.create(
                model=settings.OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=1000,
                temperature=0.1,
            )
            import json as _json
            raw = resp.choices[0].message.content.strip()
            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                if raw.endswith("```"):
                    raw = raw[:-3].strip()
            translated_map = _json.loads(raw)
            log.info("restructure.pim_translate", count=len(translated_map), lang=target_lang)
        except Exception as e:
            log.warning("restructure.pim_translate_failed", error=str(e))
            # Fallback: use Polish values directly
            translated_map = {v: v for v in pim_values_to_translate}
    else:
        # Polish marketplace or no values to translate
        translated_map = {v: v for v in pim_values_to_translate}

    # Build enrichment plan
    client = ListingsClient(marketplace_id=target_marketplace_id)
    seller_id = client.seller_id
    enriched_children = []
    patched = 0

    for sku, child_info in missing_sku_map.items():
        asin = child_info["asin"]
        pim = pim_lookup.get(asin)
        if not pim:
            enriched_children.append({
                "sku": sku,
                "asin": asin,
                "status": "not_in_pim",
                "reason": "ASIN not found in Ergonode PIM",
            })
            continue

        patches = []
        filled_fields = []

        target_locale = MARKETPLACE_LOCALE.get(target_marketplace_id, "en_GB")

        # Color: fill if missing in audit AND PIM has it
        if not child_info.get("has_color") and pim.get("color"):
            color_translated = translated_map.get(pim["color"], pim["color"])
            patches.append({
                "op": "replace",
                "path": "/attributes/color_name",
                "value": [{
                    "marketplace_id": target_marketplace_id,
                    "language_tag": target_locale,
                    "value": color_translated,
                }],
            })
            filled_fields.append(f"color_name={color_translated}")

        # Size: fill if missing — prefer size, fallback to quantity
        if not child_info.get("has_size"):
            size_val = pim.get("size") or pim.get("quantity")
            if size_val:
                size_translated = translated_map.get(size_val, size_val)
                patches.append({
                    "op": "replace",
                    "path": "/attributes/size_name",
                    "value": [{
                        "marketplace_id": target_marketplace_id,
                        "language_tag": target_locale,
                        "value": size_translated,
                    }],
                })
                filled_fields.append(f"size_name={size_translated}")

        if not patches:
            enriched_children.append({
                "sku": sku,
                "asin": asin,
                "status": "pim_empty",
                "reason": "PIM has no color/size values for this product",
            })
            continue

        if dry_run:
            enriched_children.append({
                "sku": sku,
                "asin": asin,
                "status": "dry_run",
                "would_patch": filled_fields,
                "pim_source": pim.get("sku_ergonode", ""),
            })
        else:
            try:
                result = await client.patch_listings_item(
                    seller_id, sku, patches,
                    product_type=product_type,
                )
                enriched_children.append({
                    "sku": sku,
                    "asin": asin,
                    "status": result.get("status", "ACCEPTED"),
                    "patched_fields": filled_fields,
                    "submission_id": result.get("submissionId"),
                    "issues": result.get("issues"),
                    "pim_source": pim.get("sku_ergonode", ""),
                })
                patched += 1
            except Exception as e:
                enriched_children.append({
                    "sku": sku,
                    "asin": asin,
                    "status": "error",
                    "error": str(e),
                })
            await asyncio.sleep(0.3)

    return {
        "status": "ok" if patched > 0 or dry_run else "no_data",
        "marketplace": mp_code,
        "target_language": target_lang,
        "total_missing": len(missing_children),
        "pim_found": len([c for c in enriched_children if c["status"] not in ("not_in_pim",)]),
        "patched": patched,
        "dry_run": dry_run,
        "children": enriched_children,
    }


# ---------------------------------------------------------------------------
# Public API — analysis
# ---------------------------------------------------------------------------

async def analyze_restructure(
    family_id: int,
    target_marketplace_id: str,
) -> dict:
    """
    Analyse family structure on target MP vs DE canonical.

    Returns a structured report with:
      - de_canonical: the DE family structure
      - target_state: what exists on the target MP
      - foreign_parents: parent ASINs that differ from DE (to be deleted)
      - children_to_reassign: children under foreign parents
      - missing_children: DE children not found on target MP
      - extra_children: target MP children not in DE canonical
      - verdict: 'aligned' | 'needs_restructure' | 'no_data'
    """
    mp_info = MARKETPLACE_REGISTRY.get(target_marketplace_id)
    if not mp_info:
        return {"error": f"Unknown marketplace: {target_marketplace_id}"}

    mp_code = mp_info["code"]

    # marketplace_listing_child stores country code (PL, FR, etc.), not marketplace ID
    mp_db_key = mp_code

    conn = _connect()
    cur = conn.cursor()

    # ── 1. Load DE canonical family ──────────────────────────────────────
    cur.execute("""
        SELECT id, de_parent_asin, brand, category, product_type,
               variation_theme_de
        FROM dbo.global_family WITH (NOLOCK)
        WHERE id = ?
    """, family_id)
    fam_row = cur.fetchone()
    if not fam_row:
        conn.close()
        return {"error": f"Family {family_id} not found"}

    de_parent_asin = fam_row[1]
    de_canonical = {
        "family_id": fam_row[0],
        "de_parent_asin": de_parent_asin,
        "brand": fam_row[2],
        "category": fam_row[3],
        "product_type": fam_row[4],
        "variation_theme_de": fam_row[5],
        "children": [],
    }

    # ── 2. Load DE canonical children ────────────────────────────────────
    cur.execute("""
        SELECT de_child_asin, master_key, key_type, sku_de, ean_de,
               attributes_json
        FROM dbo.global_family_child WITH (NOLOCK)
        WHERE global_family_id = ?
        ORDER BY de_child_asin
    """, family_id)
    de_children = []
    de_asin_set = set()
    for row in cur.fetchall():
        child = {
            "asin": row[0],
            "master_key": row[1],
            "key_type": row[2],
            "sku_de": row[3],
            "ean_de": row[4],
            "attributes": json.loads(row[5]) if row[5] else None,
        }
        de_children.append(child)
        de_asin_set.add(row[0])
    de_canonical["children"] = de_children
    de_canonical["children_count"] = len(de_children)

    # ── 3. Load target MP state from marketplace_listing_child ───────────
    cur.execute("""
        SELECT asin, sku, ean, current_parent_asin,
               variation_theme, attributes_json
        FROM dbo.marketplace_listing_child WITH (NOLOCK)
        WHERE marketplace = ?
          AND asin IN (
              SELECT de_child_asin
              FROM dbo.global_family_child WITH (NOLOCK)
              WHERE global_family_id = ?
          )
        ORDER BY asin
    """, mp_db_key, family_id)

    target_children = []
    target_asin_set = set()
    parent_asin_counts: dict[str | None, int] = {}
    for row in cur.fetchall():
        child = {
            "asin": row[0],
            "sku": row[1],
            "ean": row[2],
            "current_parent_asin": row[3],
            "variation_theme": row[4],
            "attributes": json.loads(row[5]) if row[5] else None,
        }
        target_children.append(child)
        target_asin_set.add(row[0])
        pa = row[3]
        parent_asin_counts[pa] = parent_asin_counts.get(pa, 0) + 1

    # ── 4. Also load extra children (on target MP, same parents, but NOT in DE) ──
    if parent_asin_counts:
        # Find children on target MP that share the same parents but are not in DE
        parent_list = [p for p in parent_asin_counts if p is not None]
        if parent_list:
            placeholders = ",".join(["?"] * len(parent_list))
            cur.execute(f"""
                SELECT asin, sku, ean, current_parent_asin,
                       variation_theme, attributes_json
                FROM dbo.marketplace_listing_child WITH (NOLOCK)
                WHERE marketplace = ?
                  AND current_parent_asin IN ({placeholders})
                  AND asin NOT IN (
                      SELECT de_child_asin
                      FROM dbo.global_family_child WITH (NOLOCK)
                      WHERE global_family_id = ?
                  )
                ORDER BY asin
            """, mp_db_key, *parent_list, family_id)
            extra_children = []
            for row in cur.fetchall():
                extra_children.append({
                    "asin": row[0],
                    "sku": row[1],
                    "ean": row[2],
                    "current_parent_asin": row[3],
                    "variation_theme": row[4],
                    "attributes": json.loads(row[5]) if row[5] else None,
                })
        else:
            extra_children = []
    else:
        extra_children = []

    conn.close()

    # ── 5. Analysis ──────────────────────────────────────────────────────

    if not target_children:
        return {
            "verdict": "no_data",
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "de_canonical": de_canonical,
            "target_state": {
                "children_found": 0,
                "parent_asins": {},
                "children": [],
            },
            "summary": f"No listings found on {mp_code} for this family's DE children.",
            "actions": [],
        }

    # Identify foreign parents (different from DE canonical parent)
    foreign_parents = {}
    children_to_reassign = []
    children_aligned = []

    for child in target_children:
        pa = child["current_parent_asin"]
        if pa == de_parent_asin:
            children_aligned.append(child)
        elif pa is None:
            # Orphan — no parent on target MP
            children_to_reassign.append({
                **child,
                "reason": "orphan",
                "current_parent": None,
            })
        else:
            # Foreign parent — different from DE
            if pa not in foreign_parents:
                foreign_parents[pa] = {
                    "parent_asin": pa,
                    "children_count": 0,
                    "children_asins": [],
                }
            foreign_parents[pa]["children_count"] += 1
            foreign_parents[pa]["children_asins"].append(child["asin"])
            children_to_reassign.append({
                **child,
                "reason": "foreign_parent",
                "current_parent": pa,
            })

    # Missing children (in DE, not on target MP)
    missing_asins = de_asin_set - target_asin_set
    missing_children = [
        c for c in de_children if c["asin"] in missing_asins
    ]

    # Determine variation theme on target MP
    target_themes = set()
    for ch in target_children:
        if ch.get("variation_theme"):
            target_themes.add(ch["variation_theme"])

    # Determine verdict
    if not foreign_parents and not missing_children:
        verdict = "aligned"
    else:
        verdict = "needs_restructure"

    # Build action plan (analysis only — not executed)
    actions = []
    for fp_asin, fp_info in foreign_parents.items():
        actions.append({
            "action": "DELETE_FOREIGN_PARENT",
            "target": fp_asin,
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "affected_children": fp_info["children_count"],
            "note": f"Delete foreign parent {fp_asin} (has {fp_info['children_count']} children). "
                    f"DE parent is {de_parent_asin}.",
        })

    if foreign_parents or children_to_reassign:
        actions.append({
            "action": "CREATE_PARENT",
            "target": de_parent_asin,
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "note": f"Create/ensure DE parent {de_parent_asin} exists on {mp_code}.",
        })

    for child in children_to_reassign:
        actions.append({
            "action": "REASSIGN_CHILD",
            "child_asin": child["asin"],
            "from_parent": child.get("current_parent"),
            "to_parent": de_parent_asin,
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "note": f"Reassign {child['asin']} from {child.get('current_parent', 'none')} to {de_parent_asin}.",
        })

    return {
        "verdict": verdict,
        "marketplace": mp_code,
        "marketplace_id": target_marketplace_id,
        "de_canonical": de_canonical,
        "target_state": {
            "children_found": len(target_children),
            "children_aligned": len(children_aligned),
            "children_misaligned": len(children_to_reassign),
            "parent_asins": {
                pa: cnt for pa, cnt in parent_asin_counts.items()
            },
            "variation_themes": list(target_themes),
            "children": target_children,
        },
        "foreign_parents": list(foreign_parents.values()),
        "children_to_reassign": children_to_reassign,
        "missing_children": missing_children,
        "extra_children": extra_children,
        "summary": _build_summary(
            mp_code, de_parent_asin, de_children,
            target_children, foreign_parents, children_to_reassign,
            missing_children, extra_children, children_aligned,
        ),
        "actions": actions,
    }


def _build_summary(
    mp_code: str,
    de_parent: str,
    de_children: list,
    target_children: list,
    foreign_parents: dict,
    children_to_reassign: list,
    missing_children: list,
    extra_children: list,
    children_aligned: list,
) -> str:
    """Build human-readable summary of analysis."""
    lines = [
        f"Family Analysis: {mp_code} vs DE (parent: {de_parent})",
        f"DE children: {len(de_children)} | Found on {mp_code}: {len(target_children)}",
    ]
    if children_aligned:
        lines.append(f"  ✓ Already aligned under DE parent: {len(children_aligned)}")
    if foreign_parents:
        for fp_asin, fp_info in foreign_parents.items():
            lines.append(
                f"  ✗ Foreign parent {fp_asin}: {fp_info['children_count']} children → needs DELETE"
            )
    orphans = [c for c in children_to_reassign if c["reason"] == "orphan"]
    if orphans:
        lines.append(f"  ○ Orphans (no parent): {len(orphans)}")
    if missing_children:
        lines.append(f"  ? Missing on {mp_code}: {len(missing_children)} ASINs not found")
    if extra_children:
        lines.append(f"  + Extra children on {mp_code} (not in DE): {len(extra_children)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# EXECUTE — actually perform restructure via SP-API
# ---------------------------------------------------------------------------

async def execute_restructure(
    family_id: int,
    target_marketplace_id: str,
    *,
    dry_run: bool = False,
    progress_hook: Optional[Callable[[int, int, str], Awaitable[None] | None]] = None,
) -> dict:
    """
    Execute family restructure on target MP via SP-API Listings Items API.

    v3 — Live SP-API based (NOT registry-based). Handles:
      - Real DE parent SKU from live child listing
      - Parent existence check + creation on target MP
      - Correct PATCH: productType + parentage_level + marketplace_id
      - Rate limiting between API calls

        Algorithm:
            1. Run analysis (existing) to get children_to_reassign list
            2. Resolve child SKUs from DB registry (ASIN → SKU)
            3. GET one child on DE via SP-API → discover real parent_sku + productType
            4. Delete all detected foreign parent listings on target MP (orphans children automatically)
            5. Ensure DE parent SKU exists on target MP (create when missing)
            6. Reassign orphaned children to DE parent SKU
    """
    import asyncio
    from app.connectors.amazon_sp_api.listings import ListingsClient

    # ── Step 1: Fresh analysis ───────────────────────────────────────────
    analysis = await analyze_restructure(family_id, target_marketplace_id)
    if "error" in analysis:
        return {"status": "error", "error": analysis["error"], "steps": []}
    if analysis["verdict"] == "aligned":
        return {"status": "already_aligned", "steps": [], "analysis": analysis}
    if analysis["verdict"] == "no_data":
        return {"status": "no_data", "steps": [], "analysis": analysis}

    mp_code = analysis["marketplace"]
    de_parent_asin = analysis["de_canonical"]["de_parent_asin"]
    children_to_reassign = analysis.get("children_to_reassign", [])
    foreign_parents = analysis.get("foreign_parents", [])

    if not children_to_reassign:
        return {"status": "nothing_to_do", "steps": [],
                "message": "No children to reassign."}

    async def _notify_progress(done: int, total: int, message: str) -> None:
        if not progress_hook:
            return
        try:
            maybe_coro = progress_hook(done, total, message)
            if hasattr(maybe_coro, "__await__"):
                await maybe_coro
        except Exception:
            # Progress reporting must never break the execution flow.
            pass

    # ── Step 2: Resolve child SKUs from registry + product table ────────
    conn = _connect()
    cur = conn.cursor()
    child_asins = [c["asin"] for c in children_to_reassign]
    placeholders = ",".join(["?"] * len(child_asins))
    # Primary: listing registry
    cur.execute(
        f"SELECT asin, merchant_sku "
        f"FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) "
        f"WHERE asin IN ({placeholders})",
        *child_asins,
    )
    child_sku_map: dict[str, str] = {}
    for r in cur.fetchall():
        if r[0] and r[1]:
            child_sku_map[r[0]] = r[1]
    # Fallback: acc_product table (listings sync)
    missing = [a for a in child_asins if a not in child_sku_map]
    if missing:
        ph2 = ",".join(["?"] * len(missing))
        cur.execute(
            f"SELECT asin, sku "
            f"FROM dbo.acc_product WITH (NOLOCK) "
            f"WHERE asin IN ({ph2}) AND sku IS NOT NULL",
            *missing,
        )
        for r in cur.fetchall():
            if r[0] and r[1] and r[0] not in child_sku_map:
                child_sku_map[r[0]] = r[1]
    conn.close()

    # Split into actionable vs skipped
    actionable: list[dict] = []
    skipped: list[dict] = []
    for child in children_to_reassign:
        c_asin = child["asin"]
        c_sku = child_sku_map.get(c_asin)
        if not c_sku:
            skipped.append({"asin": c_asin, "reason": "Child ASIN not in DE registry (no SKU)."})
        elif str(c_sku).lower().startswith("amzn.gr."):
            skipped.append({
                "asin": c_asin,
                "reason": "Skipped by business rule: amzn.gr.* (FBA return/resale used inventory).",
            })
        else:
            actionable.append({
                "asin": c_asin,
                "sku": c_sku,
                "from_parent": child.get("current_parent"),
            })

    await _notify_progress(0, len(actionable), "Prepared children for reassignment")

    steps: list[dict] = []
    errors = 0

    if not actionable:
        for s in skipped:
            steps.append({"action": "REASSIGN_CHILD", "asin": s["asin"],
                          "status": "skipped", "reason": s["reason"]})
            errors += 1
        return {"status": "completed_with_errors", "dry_run": dry_run,
                "family_id": family_id, "marketplace": mp_code,
                "de_parent_asin": de_parent_asin,
                "total_steps": len(steps), "children_planned": len(children_to_reassign),
                "children_skipped": len(skipped), "errors": errors, "steps": steps}

    # ── Step 2b: Refresh target marketplace data via SP-API ──────────────
    de_client = ListingsClient(marketplace_id=DE_MARKETPLACE)
    target_client = ListingsClient(marketplace_id=target_marketplace_id)
    seller_id = target_client.seller_id

    # Refresh children state from target marketplace
    children_refresh_errors = 0
    children_refreshed = 0
    for plan in actionable[:5]:  # Sample first 5 for performance
        try:
            child_data = await target_client.get_listings_item(seller_id, plan["sku"])
            attrs = child_data.get("attributes", {})
            cpsr = attrs.get("child_parent_sku_relationship", [])
            current_parent = cpsr[0].get("parent_sku") if cpsr else None
            plan["live_parent"] = current_parent
            children_refreshed += 1
            await asyncio.sleep(0.2)
        except Exception as e:
            if "404" not in str(e):
                children_refresh_errors += 1
                log.warning("restructure.child_refresh_failed", sku=plan["sku"], error=str(e))

    steps.append({
        "action": "REFRESH_TARGET_CHILDREN",
        "status": "ok" if children_refresh_errors == 0 else "partial",
        "sampled": min(5, len(actionable)),
        "refreshed": children_refreshed,
        "errors": children_refresh_errors,
        "reason": f"Refreshed {children_refreshed} children from target marketplace",
    })
    log.info("restructure.children_refreshed", 
             sampled=min(5, len(actionable)), 
             refreshed=children_refreshed,
             mp=mp_code)

    # Refresh foreign parents state
    foreign_parents_exist = {}
    for fp in foreign_parents:
        parent_asin = fp.get("parent_asin")
        if not parent_asin:
            continue
        # Try to find SKU for this parent
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) WHERE asin = ?",
            parent_asin,
        )
        row = cur.fetchone()
        conn.close()
        
        if not row or not row[0]:
            foreign_parents_exist[parent_asin] = "no_sku"
            continue
        
        parent_sku = row[0]
        try:
            parent_data = await target_client.get_listings_item(seller_id, parent_sku)
            foreign_parents_exist[parent_asin] = "exists"
            await asyncio.sleep(0.2)
        except Exception as e:
            if "404" in str(e):
                foreign_parents_exist[parent_asin] = "not_found"
            else:
                foreign_parents_exist[parent_asin] = "error"
                log.warning("restructure.parent_refresh_failed", asin=parent_asin, error=str(e))

    existing_parents = sum(1 for v in foreign_parents_exist.values() if v == "exists")
    steps.append({
        "action": "REFRESH_FOREIGN_PARENTS",
        "status": "ok",
        "checked": len(foreign_parents_exist),
        "existing": existing_parents,
        "not_found": sum(1 for v in foreign_parents_exist.values() if v == "not_found"),
        "reason": f"Verified {existing_parents}/{len(foreign_parents_exist)} foreign parents exist on target MP",
    })
    log.info("restructure.parents_refreshed",
             checked=len(foreign_parents_exist),
             existing=existing_parents,
             mp=mp_code)

    # ── Step 3: Live SP-API pre-flight ───────────────────────────────────

    preflight_sku = actionable[0]["sku"]
    product_type = "PRODUCT"
    de_parent_sku: str | None = None

    # 3a: GET child on DE → learn productType + real DE parent SKU
    try:
        de_child = await de_client.get_listings_item(
            seller_id, preflight_sku,
            included_data="summaries,attributes",
        )
        summaries = de_child.get("summaries", [])
        if summaries:
            product_type = summaries[0].get("productType", product_type)

        attrs = de_child.get("attributes", {})
        cpsr = attrs.get("child_parent_sku_relationship", [])
        if cpsr:
            de_parent_sku = cpsr[0].get("parent_sku")

        steps.append({
            "action": "PREFLIGHT_DE_CHILD",
            "sku": preflight_sku,
            "status": "ok",
            "product_type": product_type,
            "de_parent_sku": de_parent_sku,
        })
        log.info("restructure.preflight_de",
                 sku=preflight_sku, product_type=product_type,
                 de_parent_sku=de_parent_sku, mp=mp_code)
    except Exception as e:
        steps.append({"action": "PREFLIGHT_DE_CHILD", "sku": preflight_sku,
                       "status": "error", "error": str(e)})
        log.error("restructure.preflight_de_failed", sku=preflight_sku, error=str(e))
        return {"status": "error", "error": f"Cannot GET child {preflight_sku} on DE: {e}",
                "steps": steps}

    if not de_parent_sku:
        return {"status": "error",
                "error": f"Child {preflight_sku} has no child_parent_sku_relationship on DE.",
                "steps": steps}

    # 3a-bis: Get DE variation_theme
    de_variation_theme = analysis["de_canonical"].get("variation_theme_de") or "COLOR/SIZE"

    # 3a-ter: Validate variation_theme on target MP
    theme_report = await _validate_variation_theme(
        target_marketplace_id, product_type, de_variation_theme,
    )
    effective_theme = de_variation_theme
    if theme_report.get("recommendation") == "use_equivalent":
        effective_theme = theme_report["matched_theme"]
    steps.append({
        "action": "VALIDATE_THEME",
        "status": theme_report["status"],
        "desired_theme": de_variation_theme,
        "effective_theme": effective_theme,
        "allowed_themes": theme_report.get("allowed_themes", []),
        "reason": theme_report.get("reason", f"Theme '{effective_theme}' → {theme_report['status']}"),
    })
    log.info("restructure.theme_validated",
             status=theme_report["status"],
             desired=de_variation_theme, effective=effective_theme,
             mp=mp_code)

    await asyncio.sleep(0.3)

    # 3a-quater-bis: Detect ghost catalog parents via Catalog API relationships
    known_parent_asins = set(foreign_parents_exist.keys()) | {de_parent_asin}
    sample_child_asins = [p["asin"] for p in actionable[:5]]
    ghost_parents = await _detect_ghost_parents(
        target_marketplace_id, sample_child_asins, known_parent_asins,
    )

    if ghost_parents:
        ghost_results = []
        for ghost_asin in ghost_parents:
            if not dry_run:
                result = await _claim_and_delete_ghost_parent(
                    target_marketplace_id, ghost_asin, product_type,
                )
                ghost_results.append(result)
                await asyncio.sleep(0.5)
            else:
                ghost_results.append({
                    "ghost_asin": ghost_asin,
                    "status": "dry_run",
                })

        steps.append({
            "action": "DETECT_GHOST_PARENTS",
            "status": "cleaned",
            "ghost_parents_found": len(ghost_parents),
            "ghost_asins": ghost_parents,
            "cleanup_results": ghost_results,
            "reason": (
                f"Found {len(ghost_parents)} ghost catalog parents, "
                f"{'cleaned' if not dry_run else 'would clean'} {len(ghost_results)}"
            ),
        })
        log.info("restructure.ghost_parents",
                 found=len(ghost_parents), ghosts=ghost_parents, mp=mp_code)
    else:
        steps.append({
            "action": "DETECT_GHOST_PARENTS",
            "status": "none_found",
            "ghost_parents_found": 0,
            "reason": "No ghost catalog parents detected",
        })

    # 3a-quater: Audit child size/color attributes on target MP
    audit_skus = [a["sku"] for a in actionable]
    attr_audit = await _audit_child_attributes(
        target_marketplace_id, audit_skus,
    )
    steps.append({
        "action": "AUDIT_CHILD_ATTRS",
        "status": "ok" if attr_audit["all_color_ok"] and attr_audit["all_size_ok"] else "warning",
        "sample_checked": attr_audit["sample_checked"],
        "missing_color": attr_audit["missing_color"],
        "missing_size": attr_audit["missing_size"],
        "target_language": attr_audit["target_language"],
        "children": attr_audit.get("children", []),
        "reason": _audit_reason(attr_audit),
    })
    log.info("restructure.child_audit",
             sample=attr_audit["sample_checked"],
             missing_color=attr_audit["missing_color"],
             missing_size=attr_audit["missing_size"],
             mp=mp_code)

    # 3a-quinquies: Enrich missing color/size from DE marketplace
    de_enrichment = None
    if not attr_audit["all_color_ok"] or not attr_audit["all_size_ok"]:
        de_enrichment = await _enrich_children_from_de(
            target_marketplace_id,
            attr_audit.get("children", []),
            actionable,
            product_type,
            dry_run=dry_run,
        )
        de_children = de_enrichment.get("children", [])
        patched_count = de_enrichment.get("patched", 0)
        de_found = de_enrichment.get("de_found", 0)
        steps.append({
            "action": "ENRICH_FROM_DE",
            "status": de_enrichment["status"],
            "dry_run": dry_run,
            "total_missing": de_enrichment["total_missing"],
            "de_found": de_found,
            "patched": patched_count,
            "target_language": de_enrichment.get("target_language"),
            "children": de_children,
            "reason": (
                f"DE lookup: {de_found}/{de_enrichment['total_missing']} found on DE marketplace, "
                f"{'would patch' if dry_run else 'patched'} {patched_count if not dry_run else de_found}"
            ),
        })
        log.info("restructure.de_enrich",
                 total_missing=de_enrichment["total_missing"],
                 de_found=de_found, patched=patched_count,
                 dry_run=dry_run, mp=mp_code)

    # 3a-sexies: Validate attribute uniqueness after enrichment
    uniqueness_report = _validate_attr_uniqueness(
        attr_audit.get("children", []),
        de_enrichment.get("children", []) if de_enrichment else None,
    )
    steps.append({
        "action": "VALIDATE_ATTR_UNIQUENESS",
        "status": uniqueness_report["status"],
        "unique_pairs": uniqueness_report["unique_pairs"],
        "duplicate_count": uniqueness_report["duplicate_count"],
        "duplicates": uniqueness_report.get("duplicates", {}),
        "reason": uniqueness_report["reason"],
    })
    if uniqueness_report["duplicate_count"] > 0:
        log.warning("restructure.duplicate_attrs",
                     duplicates=uniqueness_report["duplicate_count"],
                     mp=mp_code)

    # 3b: Check if DE parent exists on target MP
    parent_exists_on_target = False
    try:
        parent_listing = await target_client.get_listings_item(
            seller_id, de_parent_sku, included_data="summaries",
        )
        if parent_listing.get("summaries"):
            parent_exists_on_target = True
            steps.append({
                "action": "CHECK_PARENT_ON_TARGET",
                "sku": de_parent_sku,
                "status": "exists",
                "asin": parent_listing["summaries"][0].get("asin"),
            })
    except Exception:
        steps.append({
            "action": "CHECK_PARENT_ON_TARGET",
            "sku": de_parent_sku,
            "status": "not_found",
            "reason": f"Parent SKU {de_parent_sku} does not exist on {mp_code}.",
        })

    await asyncio.sleep(0.3)

    # 3b2: Check if de_parent_asin already exists under a DIFFERENT SKU on target
    # (safety: prevent creating listing that would conflict with existing ASIN)
    if not parent_exists_on_target and de_parent_asin:
        conn_check = connect_acc()
        try:
            cur = conn_check.cursor()
            
            # Check if ASIN exists as parent (current_parent_asin) on target MP
            cur.execute(
                """
                SELECT DISTINCT sku, current_parent_asin 
                FROM dbo.marketplace_listing_child WITH (NOLOCK)
                WHERE marketplace = ? AND current_parent_asin = ?
                """,
                mp_code, de_parent_asin
            )
            as_parent_asin = cur.fetchall()
            
            # Check if ASIN exists as child listing (asin column) on target MP
            cur.execute(
                """
                SELECT TOP 5 sku, asin
                FROM dbo.marketplace_listing_child WITH (NOLOCK)
                WHERE marketplace = ? AND asin = ?
                """,
                mp_code, de_parent_asin
            )
            as_child_asin = cur.fetchall()
            
            if as_parent_asin and as_child_asin:
                # ASIN exists BOTH as parent AND child - complex conflict
                conflict_child_skus = [row[0] for row in as_child_asin]
                steps.append({
                    "action": "CHECK_ASIN_CONFLICT",
                    "asin": de_parent_asin,
                    "status": "complex_conflict",
                    "parent_references": len(as_parent_asin),
                    "child_skus": conflict_child_skus,
                    "reason": (
                        f"ASIN {de_parent_asin} exists both as parent ({len(as_parent_asin)} refs) "
                        f"AND as child listing. Will create alternative parent SKU."
                    ),
                })
                # Treat as conflict - need alternative SKU
                asin_conflict = True
            elif as_child_asin:
                # ASIN exists ONLY as child listing - clear conflict
                conflict_child_skus = [row[0] for row in as_child_asin]
                steps.append({
                    "action": "CHECK_ASIN_CONFLICT",
                    "asin": de_parent_asin,
                    "status": "child_conflict",
                    "child_skus": conflict_child_skus,
                    "reason": (
                        f"ASIN {de_parent_asin} exists as child listing "
                        f"under SKU(s): {', '.join(conflict_child_skus[:3])}. "
                        f"Will create alternative parent SKU."
                    ),
                })
                asin_conflict = True
            elif as_parent_asin:
                # ASIN exists ONLY as parent - expected state for our family
                steps.append({
                    "action": "CHECK_ASIN_CONFLICT",
                    "asin": de_parent_asin,
                    "status": "parent_exists",
                    "parent_references": len(as_parent_asin),
                    "reason": (
                        f"{len(as_parent_asin)} children on {mp_code} already reference "
                        f"parent ASIN {de_parent_asin}. Will align all to canonical parent SKU."
                    ),
                })
                asin_conflict = False  # Not a conflict - expected state
            else:
                # ASIN doesn't exist at all on target MP
                steps.append({
                    "action": "CHECK_ASIN_CONFLICT",
                    "asin": de_parent_asin,
                    "status": "ok",
                    "reason": f"ASIN {de_parent_asin} does not exist on {mp_code} yet. Safe to create.",
                })
                asin_conflict = False
            
            # If conflict detected, use alternative parent SKU with suffix
            if asin_conflict:
                conflict_child_skus = [row[0] for row in as_child_asin]
                
                # Find available SKU suffix
                alternative_sku = None
                for suffix_num in range(2, 10):  # Try _2v, _3v ... _9v
                    candidate_sku = f"{de_parent_sku}_{suffix_num}v"
                    try:
                        test_listing = await target_client.get_listings_item(
                            seller_id, candidate_sku, included_data="summaries"
                        )
                        if not test_listing.get("summaries"):
                            alternative_sku = candidate_sku
                            break
                    except Exception:
                        # SKU doesn't exist - available
                        alternative_sku = candidate_sku
                        break
                
                if not alternative_sku:
                    # Fallback: use _2v even if occupied (will fail later with clear error)
                    alternative_sku = f"{de_parent_sku}_2v"
                
                # Update de_parent_sku to use alternative
                original_sku = de_parent_sku
                de_parent_sku = alternative_sku
                
                steps.append({
                    "action": "RESOLVE_ASIN_CONFLICT",
                    "asin": de_parent_asin,
                    "status": "resolved",
                    "original_parent_sku": original_sku,
                    "alternative_parent_sku": alternative_sku,
                    "reason": (
                        f"Created alternative parent SKU {alternative_sku} "
                        f"(Amazon will generate new ASIN for parent)."
                    ),
                })
                log.warning("restructure.asin_conflict_resolved",
                           asin=de_parent_asin, mp=mp_code,
                           original_sku=original_sku,
                           alternative_sku=alternative_sku)
                
        finally:
            conn_check.close()
        
        await asyncio.sleep(0.2)

    # 3c: If parent missing → create it on target MP
    if not parent_exists_on_target:
        log.info("restructure.parent_missing", de_parent_sku=de_parent_sku, mp=mp_code)

        # If de_parent_sku was changed to alternative (e.g. _2v suffix due to ASIN conflict),
        # we need to fetch original SKU from DE marketplace
        original_de_parent_sku = de_parent_sku.rsplit("_", 1)[0] if "_" in de_parent_sku and de_parent_sku.endswith(("2v", "3v", "4v", "5v", "6v", "7v", "8v", "9v")) else de_parent_sku

        if dry_run:
            # In dry-run, still show that translation would happen
            target_lang = MARKETPLACE_LANGUAGE.get(target_marketplace_id, "English")
            steps.append({
                "action": "TRANSLATE_PARENT",
                "status": "dry_run",
                "reason": f"Would translate parent attributes from German to {target_lang}.",
            })
            steps.append({
                "action": "CREATE_PARENT",
                "sku": de_parent_sku,
                "status": "dry_run",
                "reason": f"Would create parent {de_parent_sku} on {mp_code} from DE listing.",
            })
            # In dry-run, assume parent would be created successfully
            parent_exists_on_target = True
        else:
            # GET full DE parent listing (use original SKU if we have alternative)
            try:
                de_parent_full = await de_client.get_listings_item(
                    seller_id, original_de_parent_sku,
                    included_data="summaries,attributes",
                )
                de_attrs = de_parent_full.get("attributes", {})

                # Replace marketplace_id in all attribute values: DE → target
                target_attrs = _rewrite_marketplace_ids(
                    de_attrs, DE_MARKETPLACE, target_marketplace_id,
                )
                target_attrs = _sanitize_parent_attributes_for_target(target_attrs, target_marketplace_id)
                # Remove DE-specific attrs that won't transfer
                for skip_attr in ("merchant_shipping_group",):
                    target_attrs.pop(skip_attr, None)

                # Translate text attributes (item_name, bullet_point, etc.) to target language
                target_attrs, translation_report = await _translate_parent_attributes(
                    target_attrs, target_marketplace_id, product_type,
                )
                # Translation can lengthen text (e.g. title), sanitize once more.
                target_attrs = _sanitize_parent_attributes_for_target(target_attrs, target_marketplace_id)
                steps.append({
                    "action": "TRANSLATE_PARENT",
                    "status": translation_report.get("status", "unknown"),
                    "target_language": translation_report.get("target_language"),
                    "translated_fields": translation_report.get("translated_fields", []),
                    "reason": (
                        f"Translated {', '.join(translation_report.get('translated_fields', []))} "
                        f"to {translation_report.get('target_language', '?')}"
                    ) if translation_report.get("translated_fields") else (
                        translation_report.get("reason", "No translation performed")
                    ),
                })

                put_body = {
                    "productType": product_type,
                    "requirements": "LISTING",
                    "attributes": target_attrs,
                }

                result = await target_client.put_listings_item(
                    seller_id, de_parent_sku, put_body,
                )
                put_status = result.get("status", "UNKNOWN")
                steps.append({
                    "action": "CREATE_PARENT",
                    "sku": de_parent_sku,
                    "status": put_status,
                    "submission_id": result.get("submissionId"),
                    "issues": result.get("issues"),
                })
                if put_status == "ACCEPTED":
                    parent_exists_on_target = True
                    log.info("restructure.parent_created",
                             sku=de_parent_sku, mp=mp_code, status=put_status)
                else:
                    log.warning("restructure.parent_create_status",
                                sku=de_parent_sku, mp=mp_code, status=put_status,
                                issues=result.get("issues"))
            except Exception as e:
                steps.append({
                    "action": "CREATE_PARENT",
                    "sku": de_parent_sku,
                    "status": "error",
                    "error": str(e),
                })
                errors += 1
                log.error("restructure.parent_create_failed",
                          sku=de_parent_sku, mp=mp_code, error=str(e))

            await asyncio.sleep(0.3)

    # If parent could not be created/found, stop here and surface actionable error.
    if not parent_exists_on_target:
        create_parent_step = next(
            (s for s in reversed(steps) if s.get("action") == "CREATE_PARENT"),
            None,
        )
        create_issues = create_parent_step.get("issues") if isinstance(create_parent_step, dict) else None
        issue_text = ""
        if isinstance(create_issues, list) and create_issues:
            issue_text = " | ".join(
                f"{i.get('code')}: {i.get('message')}"
                for i in create_issues[:3]
                if isinstance(i, dict)
            )
        return {
            "status": "error",
            "dry_run": dry_run,
            "family_id": family_id,
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "de_parent_asin": de_parent_asin,
            "de_parent_sku": de_parent_sku,
            "error": (
                "Parent creation failed on target marketplace. "
                + (issue_text or "Check CREATE_PARENT step issues.")
            ),
            "total_steps": len(steps),
            "children_planned": len(actionable) + len(skipped),
            "children_actionable": len(actionable),
            "children_skipped": len(skipped),
            "errors": max(errors, 1),
            "steps": steps,
        }

    # ── Step 4: DELETE old/foreign parents on target marketplace ─────────
    # Deleting parent listings automatically orphans all children.
    # No separate ORPHAN step needed - Amazon handles this automatically.
    foreign_parent_asins = [
        fp.get("parent_asin")
        for fp in foreign_parents
        if isinstance(fp, dict) and fp.get("parent_asin")
    ]
    parent_sku_by_asin: dict[str, str] = {}
    if foreign_parent_asins:
        conn = _connect()
        cur = conn.cursor()
        try:
            ph = ",".join(["?"] * len(foreign_parent_asins))
            cur.execute(
                f"""
                SELECT asin, merchant_sku
                FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
                WHERE asin IN ({ph})
                  AND merchant_sku IS NOT NULL
                """,
                *foreign_parent_asins,
            )
            for asin, sku in cur.fetchall():
                if asin and sku and asin not in parent_sku_by_asin:
                    parent_sku_by_asin[str(asin)] = str(sku)

            unresolved = [a for a in foreign_parent_asins if a not in parent_sku_by_asin]
            if unresolved:
                ph2 = ",".join(["?"] * len(unresolved))
                cur.execute(
                    f"""
                    SELECT asin, sku
                    FROM dbo.acc_product WITH (NOLOCK)
                    WHERE asin IN ({ph2})
                      AND sku IS NOT NULL
                    """,
                    *unresolved,
                )
                for asin, sku in cur.fetchall():
                    if asin and sku and asin not in parent_sku_by_asin:
                        parent_sku_by_asin[str(asin)] = str(sku)
        finally:
            conn.close()

    parent_delete_errors = 0
    for parent_asin in foreign_parent_asins:
        parent_sku = parent_sku_by_asin.get(parent_asin)
        step = {
            "action": "DELETE_FOREIGN_PARENT",
            "asin": parent_asin,
            "sku": parent_sku,
        }
        if not parent_sku:
            step["status"] = "skipped"
            step["reason"] = "Parent SKU not found in registry/product tables."
            steps.append(step)
            continue

        if dry_run:
            step["status"] = "dry_run"
            step["reason"] = f"Would delete foreign parent {parent_sku} on {mp_code}."
            steps.append(step)
            continue

        try:
            result = await target_client.delete_listings_item(seller_id, parent_sku)
            status = result.get("status", "ACCEPTED")
            step["status"] = status
            step["submission_id"] = result.get("submissionId")
            if result.get("issues"):
                step["issues"] = result.get("issues")
            steps.append(step)
            if status != "ACCEPTED":
                parent_delete_errors += 1
                log.error(
                    "restructure.foreign_parent_delete_status",
                    asin=parent_asin,
                    sku=parent_sku,
                    mp=mp_code,
                    status=status,
                    issues=result.get("issues"),
                )
            else:
                log.info(
                    "restructure.foreign_parent_deleted",
                    asin=parent_asin,
                    sku=parent_sku,
                    mp=mp_code,
                    status=status,
                )
        except Exception as e:
            msg = str(e)
            # Treat not-found as harmless cleanup result.
            if "404" in msg:
                step["status"] = "already_absent"
                step["reason"] = "Parent listing already absent on target marketplace."
                steps.append(step)
            else:
                step["status"] = "error"
                step["error"] = msg
                steps.append(step)
                parent_delete_errors += 1
                log.error("restructure.foreign_parent_delete_failed", asin=parent_asin, sku=parent_sku, error=msg)

        await asyncio.sleep(0.25)

    if parent_delete_errors > 0:
        return {
            "status": "error",
            "dry_run": dry_run,
            "family_id": family_id,
            "marketplace": mp_code,
            "marketplace_id": target_marketplace_id,
            "de_parent_asin": de_parent_asin,
            "de_parent_sku": de_parent_sku,
            "error": "Failed to delete one or more foreign parents. Execution stopped for safety.",
            "total_steps": len(steps),
            "children_planned": len(actionable) + len(skipped),
            "children_actionable": len(actionable),
            "children_skipped": len(skipped),
            "errors": max(errors + parent_delete_errors, 1),
            "steps": steps,
        }

    # ── Wait for Amazon to process parent deletions ──────────────────────
    if not dry_run and foreign_parent_asins:
        # Give Amazon time to process parent deletions and orphan all children
        # before we reassign them to new parent
        wait_seconds = 5
        log.info(
            "restructure.waiting_after_parent_delete",
            seconds=wait_seconds,
            deleted_parents=len(foreign_parent_asins),
            mp=mp_code,
        )
        await asyncio.sleep(wait_seconds)

    # ── Step 5: PATCH each child under new parent ───────────────────────
    processed = 0
    for plan in actionable:
        step = {
            "action": "REASSIGN_CHILD",
            "asin": plan["asin"],
            "sku": plan["sku"],
            "from_parent": plan.get("from_parent"),
            "to_parent_sku": de_parent_sku,
        }

        if dry_run:
            step["status"] = "dry_run"
            steps.append(step)
            processed += 1
            await _notify_progress(
                processed,
                len(actionable),
                f"Dry-run validated {processed}/{len(actionable)} children",
            )
            continue

        try:
            # Enrichment already done in ENRICH_FROM_DE + Catalog fallback.
            # REASSIGN only sets parent relationship + parentage_level.
            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/child_parent_sku_relationship",
                    "value": [{
                        "marketplace_id": target_marketplace_id,
                        "child_relationship_type": "variation",
                        "parent_sku": de_parent_sku,
                    }],
                },
                {
                    "op": "replace",
                    "path": "/attributes/parentage_level",
                    "value": [{
                        "marketplace_id": target_marketplace_id,
                        "value": "child",
                    }],
                },
            ]

            result = await target_client.patch_listings_item(
                seller_id, plan["sku"], patches,
                product_type=product_type,
            )
            step["status"] = result.get("status", "ACCEPTED")
            step["submission_id"] = result.get("submissionId")
            if result.get("issues"):
                step["issues"] = result["issues"]
            steps.append(step)
            log.info("restructure.child_reassigned",
                     asin=plan["asin"], sku=plan["sku"],
                     to_parent_sku=de_parent_sku, mp=mp_code,
                     status=step["status"])
            processed += 1
            await _notify_progress(
                processed,
                len(actionable),
                f"Reassigned {processed}/{len(actionable)} children",
            )
        except Exception as e:
            step["status"] = "error"
            step["error"] = str(e)
            steps.append(step)
            errors += 1
            log.error("restructure.child_reassign_failed",
                      asin=plan["asin"], sku=plan["sku"], error=str(e))
            processed += 1
            await _notify_progress(
                processed,
                len(actionable),
                f"Processed {processed}/{len(actionable)} children (with errors)",
            )

        await asyncio.sleep(0.3)

    # ── Step 5b: POST_VERIFY — verify parent and sample children ────────
    if not dry_run and processed > 0:
        await asyncio.sleep(2)  # Brief wait for Amazon processing
        verify_results = {"parent": None, "children_checked": 0, "children_ok": 0}

        # Verify parent listing
        try:
            parent_data = await target_client.get_listings_item(
                seller_id, de_parent_sku, included_data="summaries,issues",
            )
            parent_issues = parent_data.get("issues", [])
            parent_summaries = parent_data.get("summaries", [])
            verify_results["parent"] = {
                "sku": de_parent_sku,
                "status": "ok",
                "issues_count": len(parent_issues),
                "asin": parent_summaries[0].get("asin") if parent_summaries else None,
            }
        except Exception as e:
            verify_results["parent"] = {"sku": de_parent_sku, "status": "error", "error": str(e)}

        # Sample up to 5 children to verify parent assignment
        verify_sample = actionable[:min(5, len(actionable))]
        for vplan in verify_sample:
            try:
                child_data = await target_client.get_listings_item(
                    seller_id, vplan["sku"], included_data="attributes",
                )
                v_attrs = child_data.get("attributes", {})
                cpsr = v_attrs.get("child_parent_sku_relationship", [])
                actual_parent = cpsr[0].get("parent_sku") if cpsr else None
                ok = actual_parent == de_parent_sku
                verify_results["children_checked"] += 1
                if ok:
                    verify_results["children_ok"] += 1
                await asyncio.sleep(0.2)
            except Exception:
                verify_results["children_checked"] += 1

        all_children_ok = (
            verify_results["children_ok"] == verify_results["children_checked"]
            and verify_results["children_checked"] > 0
        )
        steps.append({
            "action": "POST_VERIFY",
            "status": "ok" if all_children_ok else "warning",
            "parent": verify_results["parent"],
            "children_checked": verify_results["children_checked"],
            "children_ok": verify_results["children_ok"],
            "reason": (
                f"Verified parent + {verify_results['children_ok']}/"
                f"{verify_results['children_checked']} children OK"
            ),
        })
        log.info("restructure.post_verify",
                 parent_ok=verify_results["parent"].get("status") == "ok"
                 if verify_results["parent"] else False,
                 children_ok=verify_results["children_ok"],
                 children_checked=verify_results["children_checked"],
                 mp=mp_code)

    # ── Step 6: Skipped children ─────────────────────────────────────────
    for s in skipped:
        steps.append({
            "action": "REASSIGN_CHILD",
            "asin": s["asin"],
            "status": "skipped",
            "reason": s["reason"],
        })
        errors += 1

    # ── Step 7: Foreign parents info ─────────────────────────────────────
    for fp in foreign_parents:
        steps.append({
            "action": "FOREIGN_PARENT_INFO",
            "asin": fp["parent_asin"],
            "children_count": fp["children_count"],
            "status": "info",
            "reason": "Foreign parent processed in cleanup step.",
        })

    # ── Step 8: Log to DB ────────────────────────────────────────────────
    if not dry_run:
        _log_execution(family_id, target_marketplace_id, mp_code, steps, errors)

    await _notify_progress(len(actionable), len(actionable), "Execution finished")

    return {
        "status": "completed" if errors == 0 else "completed_with_errors",
        "dry_run": dry_run,
        "family_id": family_id,
        "marketplace": mp_code,
        "marketplace_id": target_marketplace_id,
        "de_parent_asin": de_parent_asin,
        "de_parent_sku": de_parent_sku,
        "parent_on_target": parent_exists_on_target,
        "product_type_detected": product_type,
        "variation_theme": effective_theme,
        "child_attr_audit": {
            "missing_color": attr_audit.get("missing_color", 0),
            "missing_size": attr_audit.get("missing_size", 0),
            "sample_checked": attr_audit.get("sample_checked", 0),
        },
        "de_enrichment": {
            "total_missing": de_enrichment.get("total_missing", 0) if de_enrichment else 0,
            "de_found": de_enrichment.get("de_found", 0) if de_enrichment else 0,
            "patched": de_enrichment.get("patched", 0) if de_enrichment else 0,
        } if de_enrichment else None,
        "total_steps": len(steps),
        "children_planned": len(actionable) + len(skipped),
        "children_actionable": len(actionable),
        "children_skipped": len(skipped),
        "errors": errors,
        "steps": steps,
    }


def _log_execution(
    family_id: int,
    marketplace_id: str,
    mp_code: str,
    steps: list[dict],
    errors: int,
) -> None:
    """Log execution to family_restructure_log table (create if needed)."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = 'family_restructure_log'
            )
            CREATE TABLE dbo.family_restructure_log (
                id INT IDENTITY(1,1) PRIMARY KEY,
                family_id INT NOT NULL,
                marketplace_id VARCHAR(30) NOT NULL,
                marketplace_code VARCHAR(5) NOT NULL,
                total_steps INT NOT NULL,
                errors INT NOT NULL,
                steps_json NVARCHAR(MAX) NOT NULL,
                executed_at DATETIME2 DEFAULT GETUTCDATE()
            )
        """)
        cur.execute(
            "INSERT INTO dbo.family_restructure_log "
            "(family_id, marketplace_id, marketplace_code, total_steps, errors, steps_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            family_id, marketplace_id, mp_code,
            len(steps), errors, json.dumps(steps, ensure_ascii=False),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("restructure.log_failed", error=str(e))


# ---------------------------------------------------------------------------
# Analyze all marketplaces for a family (batch)
# ---------------------------------------------------------------------------

async def analyze_restructure_all_marketplaces(
    family_id: int,
) -> dict:
    """Run analysis for all non-DE marketplaces and return combined report."""
    from app.services.family_mapper.marketplace_sync import NON_DE_MARKETPLACES

    results = {}
    for mp_id in NON_DE_MARKETPLACES:
        mp_code = MARKETPLACE_REGISTRY[mp_id]["code"]
        result = await analyze_restructure(family_id, mp_id)
        results[mp_code] = result

    # Summary stats
    aligned = sum(1 for r in results.values() if r.get("verdict") == "aligned")
    needs_work = sum(1 for r in results.values() if r.get("verdict") == "needs_restructure")
    no_data = sum(1 for r in results.values() if r.get("verdict") == "no_data")

    return {
        "family_id": family_id,
        "total_marketplaces": len(NON_DE_MARKETPLACES),
        "aligned": aligned,
        "needs_restructure": needs_work,
        "no_data": no_data,
        "results": results,
    }
