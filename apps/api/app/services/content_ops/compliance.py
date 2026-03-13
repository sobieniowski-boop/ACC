"""Content Ops - compliance rules, validation, QA, AI generation, content standards."""
from __future__ import annotations

import json
import re
import uuid
import hashlib
from datetime import datetime, date, timedelta, timezone
from typing import Any

import pyodbc
import httpx

from app.connectors.mssql.mssql_store import ensure_v2_schema
from app.core.config import settings, MARKETPLACE_REGISTRY
from ._helpers import (
    _connect, _fetchall_dict, _json_load, _json_list, _is_missing_value,
    _marketplace_to_id, _marketplace_to_code,
    _normalize_policy_severity,
    _run_async, _spapi_ready,
    _native_catalog_search_by_ean, _native_restrictions_check,
    _AI_BANNED_CLAIMS, _POLISH_LEAK_PATTERNS, _DEFAULT_CONTENT_MARKETS,
)
from .catalog import (
    _resolve_native_product_type_and_requirements,
    _attrs_missing_required,
    _apply_attribute_registry,
)


def _sanitize_text(value: str, policy_flags: list[str]) -> str:
    text = value or ""
    # PII: email
    pii_email = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", flags=re.IGNORECASE)
    if pii_email.search(text):
        text = pii_email.sub("[redacted-email]", text)
        policy_flags.append("pii_email_redacted")

    # PII: phone-like sequences
    pii_phone = re.compile(r"(?:(?:\+|00)\d{1,3}[\s-]?)?(?:\d[\s-]?){8,14}\d")
    if pii_phone.search(text):
        text = pii_phone.sub("[redacted-phone]", text)
        policy_flags.append("pii_phone_redacted")

    # Banned claims / superlatives
    for pattern in _AI_BANNED_CLAIMS:
        rgx = re.compile(pattern, flags=re.IGNORECASE)
        if rgx.search(text):
            text = rgx.sub("", text)
            policy_flags.append(f"claim_removed:{pattern}")

    # Normalize spaces after removals
    return re.sub(r"\s{2,}", " ", text).strip()


def _apply_guardrails(fields: dict[str, Any], constraints: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    out = dict(fields or {})
    policy_flags: list[str] = []
    text_fields = ["title", "description", "keywords"]

    for field_name in text_fields:
        if field_name in out and isinstance(out[field_name], str):
            out[field_name] = _sanitize_text(out[field_name], policy_flags)

    bullets = out.get("bullets")
    if isinstance(bullets, list):
        sanitized_bullets: list[str] = []
        for b in bullets:
            sanitized_bullets.append(_sanitize_text(str(b), policy_flags))
        out["bullets"] = sanitized_bullets

    max_title_len = int(constraints.get("max_title_len") or 200)
    if isinstance(out.get("title"), str) and len(out["title"]) > max_title_len:
        out["title"] = out["title"][:max_title_len].rstrip()
        policy_flags.append("title_truncated")

    # Amazon backend keywords limit
    if isinstance(out.get("keywords"), str) and len(out["keywords"]) > 250:
        out["keywords"] = out["keywords"][:250].rstrip()
        policy_flags.append("keywords_truncated")

    # Ensure expected structures exist
    if not isinstance(out.get("bullets"), list):
        out["bullets"] = []
    if not isinstance(out.get("special_features"), list):
        out["special_features"] = []
    if not isinstance(out.get("attributes_json"), dict):
        out["attributes_json"] = {}
    if not isinstance(out.get("aplus_json"), dict):
        out["aplus_json"] = {}

    return out, sorted(set(policy_flags))


def _context_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _read_cached_ai(
    cur: pyodbc.Cursor,
    *,
    ctx_hash: str,
    mode: str,
    marketplace_id: str,
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT TOP 1 output_json
        FROM dbo.acc_co_ai_cache WITH (NOLOCK)
        WHERE context_hash = ?
          AND mode = ?
          AND marketplace_id = ?
          AND (expires_at IS NULL OR expires_at >= SYSUTCDATETIME())
        ORDER BY created_at DESC
        """,
        (ctx_hash, mode, marketplace_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    return _json_load(row[0])


def _store_cached_ai(
    cur: pyodbc.Cursor,
    *,
    ctx_hash: str,
    mode: str,
    marketplace_id: str,
    input_payload: dict[str, Any],
    output_payload: dict[str, Any],
    model: str,
):
    cache_id = str(uuid.uuid4())
    ttl_hours = 24
    cur.execute(
        """
        UPDATE dbo.acc_co_ai_cache
        SET output_json = ?,
            input_json = ?,
            model = ?,
            created_at = SYSUTCDATETIME(),
            expires_at = DATEADD(HOUR, ?, SYSUTCDATETIME())
        WHERE context_hash = ?
          AND mode = ?
          AND marketplace_id = ?
        """,
        (
            json.dumps(output_payload, ensure_ascii=True),
            json.dumps(input_payload, ensure_ascii=True),
            model,
            ttl_hours,
            ctx_hash,
            mode,
            marketplace_id,
        ),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO dbo.acc_co_ai_cache
                (id, context_hash, mode, marketplace_id, input_json, output_json, model, expires_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, DATEADD(HOUR, ?, SYSUTCDATETIME()))
            """,
            (
                cache_id,
                ctx_hash,
                mode,
                marketplace_id,
                json.dumps(input_payload, ensure_ascii=True),
                json.dumps(output_payload, ensure_ascii=True),
                model,
                ttl_hours,
            ),
        )


def _build_ai_fallback_output(payload: dict[str, Any], base_fields: dict[str, Any]) -> dict[str, Any]:
    sku = str(payload.get("sku") or "").strip()
    marketplace = str(payload.get("marketplace_id") or "").strip().upper()
    mode = str(payload.get("mode") or "improve")

    title = base_fields.get("title") or f"{sku} listing {marketplace}".strip()
    description = base_fields.get("description") or f"Optimized content draft for {sku} ({marketplace})."
    bullets = base_fields.get("bullets") or []
    keywords = base_fields.get("keywords") or f"{sku} {marketplace}"

    return {
        "title": title,
        "bullets": bullets[:5],
        "description": description,
        "keywords": keywords,
        "special_features": base_fields.get("special_features") or [],
        "attributes_json": base_fields.get("attributes_json") or {"mode": mode},
        "aplus_json": base_fields.get("aplus_json") or {},
        "compliance_notes": base_fields.get("compliance_notes") or "Generated via fallback template",
    }


def _generate_with_openai(payload: dict[str, Any], base_fields: dict[str, Any], model: str) -> dict[str, Any]:
    api_key = (settings.OPENAI_API_KEY or "").strip()
    if not api_key:
        return _build_ai_fallback_output(payload, base_fields)
    try:
        from openai import OpenAI
    except Exception:
        return _build_ai_fallback_output(payload, base_fields)

    user_prompt = {
        "mode": payload.get("mode"),
        "sku": payload.get("sku"),
        "marketplace_id": payload.get("marketplace_id"),
        "source_market": payload.get("source_market"),
        "constraints": payload.get("constraints_json") or {},
        "base_fields": base_fields,
        "required_output_keys": [
            "title",
            "bullets",
            "description",
            "keywords",
            "special_features",
            "attributes_json",
            "aplus_json",
            "compliance_notes",
        ],
    }
    system_prompt = (
        "You generate Amazon product content JSON. "
        "Return only valid JSON object with required keys. "
        "No PII, no medical or unverifiable claims."
    )

    try:
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            temperature=0.3,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=True)},
            ],
        )
        content = (resp.choices[0].message.content or "{}").strip()
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return _build_ai_fallback_output(payload, base_fields)


def ai_generate(*, payload: dict):
    ensure_v2_schema()
    sku = str(payload.get("sku") or "").strip()
    marketplace_id = str(payload.get("marketplace_id") or "").strip().upper()
    mode = str(payload.get("mode") or "").strip().lower()
    model = str(payload.get("model") or settings.OPENAI_MODEL or "gpt-4o").strip()
    constraints = payload.get("constraints_json") or {}
    if not isinstance(constraints, dict):
        constraints = {}

    if not sku:
        raise ValueError("sku is required")
    if not marketplace_id:
        raise ValueError("marketplace_id is required")
    if mode not in {"new_listing", "improve", "localize"}:
        raise ValueError("mode must be one of: new_listing, improve, localize")

    cache_input = {
        "sku": sku,
        "marketplace_id": marketplace_id,
        "mode": mode,
        "source_market": payload.get("source_market"),
        "fields": payload.get("fields") or [],
        "constraints_json": constraints,
        "model": model,
    }
    ctx_hash = _context_hash(cache_input)

    conn = _connect()
    try:
        cur = conn.cursor()
        cached = _read_cached_ai(cur, ctx_hash=ctx_hash, mode=mode, marketplace_id=marketplace_id)
        if cached:
            return {
                "sku": sku,
                "marketplace_id": marketplace_id,
                "mode": mode,
                "model": model,
                "cache_hit": True,
                "policy_flags": cached.get("policy_flags") if isinstance(cached.get("policy_flags"), list) else [],
                "output": cached.get("output") if isinstance(cached.get("output"), dict) else {},
                "generated_at": datetime.now(timezone.utc),
            }

        base_fields: dict[str, Any] = {}
        source_market = str(payload.get("source_market") or marketplace_id).strip().upper()
        source_row = _get_latest_version_for_market(cur, sku, source_market)
        if source_row:
            base_fields = _json_load(source_row.get("fields_json"))

        generated_raw = _generate_with_openai(payload, base_fields, model)
        safe_output, policy_flags = _apply_guardrails(generated_raw, constraints)
        response_payload = {
            "sku": sku,
            "marketplace_id": marketplace_id,
            "mode": mode,
            "model": model,
            "cache_hit": False,
            "policy_flags": policy_flags,
            "output": safe_output,
            "generated_at": datetime.now(timezone.utc),
        }

        cache_store_payload = {
            "output": safe_output,
            "policy_flags": policy_flags,
        }
        _store_cached_ai(
            cur,
            ctx_hash=ctx_hash,
            mode=mode,
            marketplace_id=marketplace_id,
            input_payload=cache_input,
            output_payload=cache_store_payload,
            model=model,
        )
        conn.commit()
        return response_payload
    finally:
        conn.close()


def _flatten_content_fields(fields: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(fields or {})
    title = str(normalized.get("title") or "").strip()
    description = str(normalized.get("description") or "").strip()
    keywords = str(normalized.get("keywords") or "").strip()
    bullets_raw = normalized.get("bullets")
    bullets = [str(x).strip() for x in bullets_raw] if isinstance(bullets_raw, list) else []
    bullets = [b for b in bullets if b]
    return {
        "title": title,
        "description": description,
        "keywords": keywords,
        "bullets": bullets,
    }


def _family_coverage_pct_for_targets(
    cur: pyodbc.Cursor,
    *,
    asin: str | None,
    parent_asin: str | None,
    target_markets: list[str],
) -> tuple[float, bool]:
    if not target_markets:
        return 0.0, False
    if not asin and not parent_asin:
        return 0.0, False

    family_id = None
    if parent_asin:
        cur.execute(
            "SELECT TOP 1 id FROM dbo.global_family WITH (NOLOCK) WHERE de_parent_asin = ?",
            (parent_asin,),
        )
        row = cur.fetchone()
        if row:
            family_id = int(row[0])

    if family_id is None and asin:
        cur.execute(
            """
            SELECT TOP 1 gfc.global_family_id
            FROM dbo.global_family_child gfc WITH (NOLOCK)
            WHERE gfc.de_child_asin = ?
            """,
            (asin,),
        )
        row = cur.fetchone()
        if row:
            family_id = int(row[0])

    if family_id is None:
        return 0.0, False

    placeholders = ",".join("?" for _ in target_markets)
    cur.execute(
        f"""
        SELECT marketplace, coverage_pct
        FROM dbo.family_coverage_cache WITH (NOLOCK)
        WHERE global_family_id = ?
          AND marketplace IN ({placeholders})
        """,
        (family_id, *target_markets),
    )
    rows = cur.fetchall()
    if not rows:
        return 0.0, True

    by_market = {str(r[0]).upper(): float(r[1] or 0) for r in rows}
    values = [by_market.get(m, 0.0) for m in target_markets]
    avg = sum(values) / len(values) if values else 0.0
    return round(avg, 2), True


def _preflight_recommendations(blockers: list[str], warnings: list[str]) -> list[str]:
    actions: list[str] = []
    if any("missing_pim_data" in b for b in blockers):
        actions.append("Uzupelnij dane PIM (brand/title/ean/category) przed publikacja.")
    if any("family_coverage" in b for b in blockers):
        actions.append("Domknij parent-child na rynkach docelowych na bazie DE canonical.")
    if any("spapi_not_configured" in b for b in blockers):
        actions.append("Skonfiguruj SP-API credentials w ACC, bo gate restrictions/catalog jest natywny.")
    if any("listing_requires_approval" in b or "listing_blocked" in b for b in blockers):
        actions.append("Rozwiaz ograniczenia listingowe (approval lub blokada) przed push.")
    if any("catalog" in b for b in blockers):
        actions.append("Zweryfikuj dopasowanie katalogowe (ASIN/EAN) przed publikacja.")
    if not actions:
        actions.append("Gotowe do produkcji draftu i review QA.")
    return actions


def _create_preflight_task_if_missing(
    cur: pyodbc.Cursor,
    *,
    sku: str,
    marketplace_id: str | None,
    task_type: str,
    title: str,
    note: str,
) -> str | None:
    cur.execute(
        """
        SELECT TOP 1 id
        FROM dbo.acc_co_tasks WITH (NOLOCK)
        WHERE sku = ?
          AND ISNULL(marketplace_id, '') = ISNULL(?, '')
          AND task_type = ?
          AND status IN ('open', 'investigating')
          AND title = ?
          AND source_page = 'content_onboard_preflight'
        ORDER BY created_at DESC
        """,
        (sku, marketplace_id, task_type, title),
    )
    existing = cur.fetchone()
    if existing:
        return str(existing[0])

    task_id = str(uuid.uuid4())
    owner = _auto_assign_owner(cur, task_type=task_type, marketplace_id=marketplace_id, sku=sku)
    cur.execute(
        """
        INSERT INTO dbo.acc_co_tasks
            (id, task_type, sku, marketplace_id, priority, owner, status, title, note, source_page, created_by)
        VALUES
            (?, ?, ?, ?, 'p0', ?, 'open', ?, ?, 'content_onboard_preflight', ?)
        """,
        (task_id, task_type, sku, marketplace_id, owner, title, note, settings.DEFAULT_ACTOR),
    )
    return task_id


def run_onboard_preflight(*, payload: dict):
    ensure_v2_schema()
    sku_list = [str(s).strip() for s in (payload.get("sku_list") or []) if str(s).strip()]
    if not sku_list:
        raise ValueError("sku_list is required")

    main_market = str(payload.get("main_market") or "DE").strip().upper()
    target_markets = [str(m).strip().upper() for m in (payload.get("target_markets") or []) if str(m).strip()]
    if not target_markets:
        target_markets = [m for m in _DEFAULT_CONTENT_MARKETS if m != main_market]

    auto_create_tasks = bool(payload.get("auto_create_tasks"))
    unique_skus = list(dict.fromkeys(sku_list))

    conn = _connect()
    try:
        cur = conn.cursor()
        out_items: list[dict[str, Any]] = []

        for sku in unique_skus:
            cur.execute(
                """
                SELECT TOP 1
                    p.sku, p.asin, p.ean, p.brand, p.title, p.category, p.parent_asin
                FROM dbo.acc_product p WITH (NOLOCK)
                WHERE p.internal_sku = ? OR p.sku = ?
                ORDER BY CASE WHEN p.internal_sku = ? THEN 0 ELSE 1 END, p.updated_at DESC
                """,
                (sku, sku, sku),
            )
            row = cur.fetchone()

            asin = str(row[1]).strip() if row and row[1] else None
            ean = str(row[2]).strip() if row and row[2] else None
            brand = str(row[3]).strip() if row and row[3] else None
            title = str(row[4]).strip() if row and row[4] else None
            category = str(row[5]).strip() if row and row[5] else None
            parent_asin = str(row[6]).strip() if row and row[6] else None

            pim_fields = [asin, ean, brand, title, category]
            pim_score = int(round((sum(1 for v in pim_fields if v) / 5.0) * 100))

            blockers: list[str] = []
            warnings: list[str] = []
            if row is None:
                blockers.append("missing_product_mapping: sku not found in acc_product")
            else:
                if not brand:
                    blockers.append("missing_pim_data: brand")
                if not title:
                    blockers.append("missing_pim_data: title")
                if not ean:
                    warnings.append("missing_pim_data: ean")

            family_coverage_pct, family_known = _family_coverage_pct_for_targets(
                cur,
                asin=asin,
                parent_asin=parent_asin,
                target_markets=target_markets,
            )
            if not family_known:
                warnings.append("family_mapping_missing: no canonical family mapping found")
            elif family_coverage_pct < 80:
                blockers.append(f"family_coverage_low: {family_coverage_pct}%")
            elif family_coverage_pct < 100:
                warnings.append(f"family_coverage_partial: {family_coverage_pct}%")

            # Hard gate: restrictions + catalog must be checked natively in ACC.
            resolved_asin = asin
            if not _spapi_ready():
                blockers.append("restrictions_catalog_spapi_not_configured")
            else:
                try:
                    if not resolved_asin and ean:
                        catalog_payload = _native_catalog_search_by_ean(ean, main_market)
                        matches = catalog_payload.get("matches")
                        if isinstance(matches, list) and matches:
                            first = matches[0] if isinstance(matches[0], dict) else {}
                            candidate = str(first.get("asin") or "").strip()
                            if candidate:
                                resolved_asin = candidate
                                warnings.append("catalog_match_found_by_ean")
                        else:
                            blockers.append("catalog_no_match_for_ean")
                    elif not resolved_asin and not ean:
                        blockers.append("catalog_check_missing_identifier: asin_or_ean_required")
                except Exception as exc:
                    blockers.append(f"catalog_native_error:{exc}")
                    # Optional fallback to ProductOnboard bridge.
                    if _bridge_enabled():
                        try:
                            status_code, catalog_payload = _bridge_get_json(
                                str(getattr(settings, "PRODUCTONBOARD_CATALOG_BY_EAN_PATH", "") or "/api/productonboard/catalog/search-by-ean"),
                                {"ean": ean, "marketplace": main_market},
                            )
                            if status_code < 300:
                                matches = catalog_payload.get("matches")
                                if isinstance(matches, list) and matches:
                                    first = matches[0] if isinstance(matches[0], dict) else {}
                                    candidate = str(first.get("asin") or "").strip()
                                    if candidate:
                                        resolved_asin = candidate
                                        warnings.append("catalog_match_found_by_bridge_fallback")
                        except Exception:
                            pass

                if resolved_asin:
                    for market in target_markets:
                        try:
                            res_payload = _native_restrictions_check(resolved_asin, market)
                            can_list = bool(res_payload.get("can_list"))
                            requires_approval = bool(res_payload.get("requires_approval"))
                            if can_list:
                                continue
                            if requires_approval:
                                blockers.append(f"listing_requires_approval:{market}")
                            else:
                                blockers.append(f"listing_blocked:{market}")
                        except Exception as exc:
                            blockers.append(f"restrictions_native_error:{market}:{exc}")
                            # Optional fallback to ProductOnboard bridge.
                            if _bridge_enabled():
                                try:
                                    status_code, res_payload = _bridge_get_json(
                                        str(getattr(settings, "PRODUCTONBOARD_RESTRICTIONS_PATH", "") or "/api/productonboard/restrictions/check"),
                                        {"asin": resolved_asin, "marketplace": market},
                                    )
                                    if status_code < 300:
                                        can_list = bool(res_payload.get("can_list"))
                                        requires_approval = bool(res_payload.get("requires_approval"))
                                        if not can_list:
                                            if requires_approval:
                                                blockers.append(f"listing_requires_approval:{market}")
                                            else:
                                                blockers.append(f"listing_blocked:{market}")
                                        warnings.append(f"restrictions_checked_by_bridge_fallback:{market}")
                                except Exception:
                                    pass
                else:
                    blockers.append("catalog_no_asin_resolved_for_restrictions")

            if resolved_asin and not asin:
                asin = resolved_asin
            actions = _preflight_recommendations(blockers, warnings)

            tasks_created: list[str] = []
            if auto_create_tasks:
                if any("missing_pim_data" in b or "missing_product_mapping" in b for b in blockers):
                    task_id = _create_preflight_task_if_missing(
                        cur,
                        sku=sku,
                        marketplace_id=None,
                        task_type="create_listing",
                        title="Uzupelnij dane PIM przed listingiem",
                        note="Preflight wykryl braki danych PIM wymagane do publikacji.",
                    )
                    if task_id:
                        tasks_created.append(task_id)
                if any("family_coverage" in b for b in blockers):
                    task_id = _create_preflight_task_if_missing(
                        cur,
                        sku=sku,
                        marketplace_id=None,
                        task_type="expand_marketplaces",
                        title="Domknij relacje family na rynkach docelowych",
                        note=f"Coverage family ponizej progu dla target markets: {', '.join(target_markets)}",
                    )
                    if task_id:
                        tasks_created.append(task_id)

            out_items.append(
                {
                    "sku": sku,
                    "asin": asin,
                    "ean": ean,
                    "brand": brand,
                    "title": title,
                    "pim_score": pim_score,
                    "family_coverage_pct": family_coverage_pct,
                    "blockers": blockers,
                    "warnings": warnings,
                    "recommended_actions": actions,
                    "tasks_created": tasks_created,
                }
            )

        if auto_create_tasks:
            conn.commit()

        return {
            "main_market": main_market,
            "target_markets": target_markets,
            "items": out_items,
            "generated_at": datetime.now(timezone.utc),
        }
    finally:
        conn.close()


def verify_content_quality(*, payload: dict):
    fields_input = payload.get("content") or {}
    fields = _flatten_content_fields(fields_input)
    pim_facts = payload.get("pim_facts_json") or {}
    if not isinstance(pim_facts, dict):
        pim_facts = {}
    target_language = str(payload.get("target_language") or "de_DE").strip().lower()

    findings: list[dict[str, Any]] = []

    def add_finding(category: str, severity: str, field: str, message: str, suggestion: str | None = None):
        findings.append(
            {
                "category": category,
                "severity": _normalize_policy_severity(severity),
                "field": field,
                "message": message,
                "suggestion": suggestion,
            }
        )

    title = fields["title"]
    description = fields["description"]
    keywords = fields["keywords"]
    bullets = fields["bullets"]
    all_text = " ".join([title, description, keywords, *bullets]).strip()

    if title and len(title) < 30:
        add_finding("accuracy", "major", "title", "Title shorter than 30 chars.", "Rozwin tytul o kluczowe cechy i brand.")
    if len(title) > 200:
        add_finding("compliance", "major", "title", "Title exceeds 200 chars.", "Skroc tytul do limitu Amazon.")
    if len(bullets) < 3:
        add_finding("conversion", "major", "bullets", "Less than 3 bullet points.", "Dodaj minimum 3 konkretne benefity.")
    if len(bullets) != len(set(b.lower() for b in bullets)):
        add_finding("seo", "minor", "bullets", "Duplicate bullet points detected.", "Usuń duplikaty i rozdziel benefity.")

    if target_language != "pl_pl":
        for pattern in _POLISH_LEAK_PATTERNS:
            if re.search(pattern, all_text, flags=re.IGNORECASE):
                add_finding(
                    "language",
                    "critical",
                    "title",
                    "Potential Polish leak detected in non-PL content.",
                    "Przetlumacz fragmenty i usuń wycieki jezykowe.",
                )
                break

    for pattern in _AI_BANNED_CLAIMS:
        if re.search(pattern, all_text, flags=re.IGNORECASE):
            add_finding(
                "compliance",
                "critical",
                "description",
                f"Prohibited claim pattern detected: {pattern}",
                "Usuń claimy medyczne/superlatywy i niezweryfikowane obietnice.",
            )

    brand = str(pim_facts.get("brand") or "").strip()
    if brand and brand.lower() not in all_text.lower():
        add_finding(
            "accuracy",
            "major",
            "title",
            f"Brand '{brand}' not found in content.",
            "Dodaj brand do tytulu albo pierwszego bulletu.",
        )

    color = str(pim_facts.get("color") or "").strip()
    if color and color.lower() not in all_text.lower():
        add_finding(
            "accuracy",
            "minor",
            "description",
            f"Color '{color}' from PIM not present in listing copy.",
            "Rozwaz dodanie koloru jesli to wariant kluczowy dla konwersji.",
        )

    critical_count = sum(1 for x in findings if x["severity"] == "critical")
    major_count = sum(1 for x in findings if x["severity"] == "major")
    minor_count = sum(1 for x in findings if x["severity"] == "minor")

    score = 100.0 - (critical_count * 30.0) - (major_count * 10.0) - (minor_count * 3.0)
    score = max(0.0, min(100.0, score))

    if critical_count >= 2 or score < 40:
        status = "rejected"
    elif critical_count > 0 or score < 70:
        status = "needs_revision"
    else:
        status = "passed"

    checks_json = {
        "lengths": {
            "title": len(title),
            "bullets": len(bullets),
            "description": len(description),
            "keywords": len(keywords),
        },
        "target_language": target_language,
        "pim_brand_used": bool(brand and brand.lower() in all_text.lower()),
        "pim_color_used": bool(color and color.lower() in all_text.lower()) if color else None,
    }

    return {
        "status": status,
        "score": round(score, 2),
        "critical_count": critical_count,
        "major_count": major_count,
        "minor_count": minor_count,
        "findings": findings,
        "checks_json": checks_json,
        "checked_at": datetime.now(timezone.utc),
    }


def get_content_ops_health():
    queue_health = get_publish_queue_health(stale_minutes=30)
    compliance_critical = list_compliance_queue(severity="critical", page=1, page_size=1)
    compliance_major = list_compliance_queue(severity="major", page=1, page_size=1)
    data_quality = get_content_data_quality()

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_count,
                SUM(CASE WHEN status = 'investigating' THEN 1 ELSE 0 END) AS investigating_count,
                SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_count,
                SUM(CASE WHEN due_date IS NOT NULL AND due_date < CAST(SYSUTCDATETIME() AS DATE) AND status IN ('open','investigating') THEN 1 ELSE 0 END) AS overdue_count
            FROM dbo.acc_co_tasks WITH (NOLOCK)
            """
        )
        row = cur.fetchone()
        tasks_health = {
            "open": int((row[0] if row else 0) or 0),
            "investigating": int((row[1] if row else 0) or 0),
            "resolved": int((row[2] if row else 0) or 0),
            "overdue": int((row[3] if row else 0) or 0),
        }
    finally:
        conn.close()

    return {
        "generated_at": datetime.now(timezone.utc),
        "queue_health": queue_health,
        "compliance_backlog": {
            "critical": int(compliance_critical.get("total") or 0),
            "major_or_higher": int(compliance_major.get("total") or 0),
        },
        "tasks_health": tasks_health,
        "data_quality_cards": data_quality.get("cards") if isinstance(data_quality.get("cards"), list) else [],
    }


def onboard_catalog_search_by_ean(*, ean: str, marketplace: str = "DE"):
    ean_value = str(ean or "").strip()
    if not ean_value:
        raise ValueError("ean is required")
    market_code = str(marketplace or "DE").strip().upper()
    return _native_catalog_search_by_ean(ean_value, market_code)


def onboard_restrictions_check(*, asin: str, marketplace: str = "DE"):
    asin_value = str(asin or "").strip()
    if not asin_value:
        raise ValueError("asin is required")
    market_code = str(marketplace or "DE").strip().upper()
    return _native_restrictions_check(asin_value, market_code)

