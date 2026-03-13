"""AI Product Matcher — match unmapped Amazon products to internal SKUs using GPT.

Wszystkie sugestie trafiają do tabeli acc_product_match_suggestion ze statusem 'pending'.
Dopasowanie (mapping + price) następuje WYŁĄCZNIE po potwierdzeniu przez użytkownika.
"""
from __future__ import annotations

import json
import math
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Optional

import structlog
from openai import APIConnectionError, APIStatusError, AsyncOpenAI, AuthenticationError, BadRequestError, RateLimitError

from app.core.config import settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

_client: Optional[AsyncOpenAI] = None


def _get_openai() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    return _client


def _connect():
    return connect_acc(autocommit=False, timeout=20)


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _classify_ai_error(exc: Exception) -> tuple[str, str]:
    message = str(exc)
    lowered = message.lower()

    if isinstance(exc, AuthenticationError):
        return ("auth_error", "OpenAI odrzucił autoryzację. Sprawdź klucz API i uprawnienia modelu.")
    if isinstance(exc, RateLimitError):
        if "insufficient_quota" in lowered or "quota" in lowered or "billing" in lowered:
            return ("quota_exceeded", "Brak dostępnego budżetu lub quota na kluczu OpenAI. Doładuj billing albo użyj innego klucza.")
        return ("rate_limited", "OpenAI ograniczył liczbę wywołań. Spróbuj ponownie za chwilę.")
    if isinstance(exc, BadRequestError):
        if "context_length_exceeded" in lowered:
            return ("context_too_large", "Prompt AI był za duży dla modelu. To jest błąd konfiguracji matchera, nie danych użytkownika.")
        if "unsupported parameter" in lowered:
            return ("unsupported_parameter", "Wywołanie OpenAI używa nieobsługiwanego parametru dla tego modelu.")
        return ("bad_request", f"OpenAI odrzucił zapytanie: {message}")
    if isinstance(exc, APIConnectionError):
        return ("connection_error", "Nie udało się połączyć z OpenAI. Sprawdź sieć lub spróbuj ponownie.")
    if isinstance(exc, APIStatusError):
        status_code = getattr(exc, "status_code", None)
        if status_code == 402:
            return ("quota_exceeded", "OpenAI zwrócił błąd rozliczeniowy lub brak środków na koncie.")
        if status_code == 429:
            return ("rate_limited", "OpenAI zwrócił limit 429. Spróbuj ponownie za chwilę.")
        return ("api_status_error", f"OpenAI zwrócił błąd API ({status_code or 'unknown'}).")
    if "quota" in lowered or "billing" in lowered or "insufficient_quota" in lowered:
        return ("quota_exceeded", "Brak dostępnego budżetu lub quota na kluczu OpenAI. Doładuj billing albo użyj innego klucza.")
    return ("unknown_error", message or "Nieznany błąd AI")


# ---------------------------------------------------------------------------
# Data loaders (Azure SQL only — NO Netfox!)
# ---------------------------------------------------------------------------

def _load_unmapped_products() -> list[dict[str, Any]]:
    """Fetch products without internal_sku that have a title."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.id, p.sku, p.asin, p.title, p.parent_asin, p.ean
            FROM dbo.acc_product p WITH (NOLOCK)
            WHERE (p.internal_sku IS NULL OR p.internal_sku = '')
              AND p.title IS NOT NULL AND p.title <> ''
              AND p.sku NOT LIKE 'amzn.gr.%%' AND p.sku NOT LIKE 'amazon.found%%'
              AND NOT EXISTS (
                  SELECT 1 FROM dbo.acc_product_match_suggestion s WITH (NOLOCK)
                  WHERE s.unmapped_sku = p.sku AND s.status IN ('pending', 'approved')
              )
            ORDER BY p.sku
        """)
        return _fetchall_dict(cur)
    finally:
        conn.close()


def _load_mapped_products() -> list[dict[str, Any]]:
    """Fetch mapped products with internal_sku + title + price as reference pool."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                p.internal_sku,
                p.sku,
                p.asin,
                p.title,
                p.ean,
                COALESCE(p.netto_purchase_price_pln,
                    (SELECT TOP 1 pp.netto_price_pln
                     FROM dbo.acc_purchase_price pp WITH (NOLOCK)
                     WHERE pp.internal_sku = p.internal_sku
                     ORDER BY pp.valid_from DESC)
                ) AS price_pln
            FROM dbo.acc_product p WITH (NOLOCK)
            WHERE p.internal_sku IS NOT NULL AND p.internal_sku <> ''
              AND p.title IS NOT NULL AND p.title <> ''
        """)
        return _fetchall_dict(cur)
    finally:
        conn.close()


def _load_listing_registry_exact_map() -> dict[str, set[str]]:
    """Load exact Merchant SKU / ASIN / EAN -> internal_sku hints from ACC staging."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.acc_amazon_listing_registry', 'U') IS NOT NULL
            BEGIN
                SELECT DISTINCT
                    merchant_sku,
                    merchant_sku_alt,
                    asin,
                    ean,
                    internal_sku
                FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
                WHERE ISNULL(internal_sku, '') <> ''
            END
            """
        )
        if not cur.description:
            return {}
        lookup: dict[str, set[str]] = {}
        for row in cur.fetchall():
            internal_sku = str(row[4] or "").strip()
            if not internal_sku:
                continue
            for raw_key in row[:4]:
                key = str(raw_key or "").strip()
                if not key:
                    continue
                lookup.setdefault(key, set()).add(internal_sku)
        return lookup
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GPT prompt builder & caller
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert product data analyst for a Polish household goods manufacturer (KADAX brand).
Your task is to match unmapped Amazon product listings to known internal products (SKUs).

Key domain knowledge:
- Products are mostly kitchenware, jars, containers, household items
- Amazon titles often describe bundles: "20er Set", "10x", "Zestaw 6 szt." = multiple units
- A bundle "KADAX 20er Set Marmeladengläser 350ml" means 20 × single jar (350ml)
- The matched product may be the single unit — you must identify the quantity in the bundle
- Prices: unit_price × quantity_in_bundle = total_price for the unmapped product
- Lid colors, sizes (ml), material matter for matching — they must match!
- If the unmapped product includes lids, each jar needs 1 lid (separate SKU)

CRITICAL RULES:
1. Only match if you are CONFIDENT the products are the same physical item (just different bundle size)
2. If unsure, set confidence < 50 and explain why
3. For bundles: decompose into BOM (Bill of Materials) — list each component with quantity
4. Price each component: total_price = SUM(component_price × component_qty)
5. Output must be valid JSON
6. Return ONLY this JSON object shape: {"results": [...]}

For each unmapped product, output one object inside "results":
{
  "unmapped_sku": "...",
  "matched_internal_sku": "..." or null if no match,
  "matched_sku": "..." (the reference product SKU),
  "confidence": 0-100,
  "reasoning": "short explanation in Polish",
  "quantity_in_bundle": 1,
  "bom": [{"internal_sku": "...", "name": "...", "qty": N, "unit_price_pln": X.XX}],
  "total_price_pln": X.XX
}
If no match found, set matched_internal_sku=null, confidence=0, bom=[]."""


STOPWORDS = {
    "kadax", "set", "zestaw", "szt", "sztuk", "stuck", "stueck", "stk", "pcs", "piece", "pack", "paket",
    "mit", "und", "der", "die", "das", "dla", "with", "for", "the", "ein", "eine", "einer", "do", "de",
    "na", "od", "cm", "mm", "ml", "l", "x", "oraz", "w", "wewnetrzny", "zewnetrzny",
}

TOKEN_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)
NUMBER_RE = re.compile(r"\b\d+[.,]?\d*\b")


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def _tokenize(value: Any) -> set[str]:
    text = _normalize_text(value)
    tokens = {
        token for token in TOKEN_RE.findall(text)
        if token and token not in STOPWORDS and len(token) > 1
    }
    return tokens


def _extract_numbers(value: Any) -> set[str]:
    return {num.replace(",", ".") for num in NUMBER_RE.findall(_normalize_text(value))}


def _title_similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, _normalize_text(left), _normalize_text(right)).ratio()


def _build_reference_index(reference_pool: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen_isk: set[str] = set()
    indexed: list[dict[str, Any]] = []
    for row in reference_pool:
        internal_sku = str(row.get("internal_sku") or "").strip()
        if not internal_sku or internal_sku in seen_isk:
            continue
        seen_isk.add(internal_sku)
        title = str(row.get("title") or "")
        ean = str(row.get("ean") or "").strip()
        sku = str(row.get("sku") or "").strip()
        indexed.append({
            "isk": internal_sku,
            "sku": sku,
            "title": title,
            "ean": ean,
            "price": float(row["price_pln"]) if row.get("price_pln") else None,
            "_tokens": _tokenize(title),
            "_numbers": _extract_numbers(title),
        })
    return indexed


def _select_candidates(
    unmapped_product: dict[str, Any],
    indexed_reference: list[dict[str, Any]],
    *,
    exact_registry_lookup: dict[str, set[str]] | None = None,
    limit: int = 24,
) -> list[dict[str, Any]]:
    title = str(unmapped_product.get("title") or "")
    ean = str(unmapped_product.get("ean") or "").strip()
    sku = str(unmapped_product.get("sku") or "").strip()
    asin = str(unmapped_product.get("asin") or "").strip()
    tokens = _tokenize(title)
    numbers = _extract_numbers(title)
    scored: list[tuple[float, dict[str, Any]]] = []
    preferred_isks: set[str] = set()

    if exact_registry_lookup:
        for key in (sku, asin, ean):
            if key and key in exact_registry_lookup:
                preferred_isks.update(exact_registry_lookup[key])

    for ref in indexed_reference:
        score = 0.0
        ref_tokens = ref["_tokens"]
        ref_numbers = ref["_numbers"]

        if preferred_isks and ref.get("isk") in preferred_isks:
            score += 150.0

        if ean and ref.get("ean") and ean == ref.get("ean"):
            score += 100.0

        overlap = len(tokens & ref_tokens)
        if overlap:
            score += overlap * 6.0

        if numbers and ref_numbers:
            shared_numbers = len(numbers & ref_numbers)
            if shared_numbers:
                score += shared_numbers * 4.0
            elif len(numbers) >= 2:
                score -= 3.0

        similarity = _title_similarity(title, str(ref.get("title") or ""))
        score += similarity * 20.0

        if tokens and overlap == 0 and similarity < 0.18 and not (ean and ref.get("ean") and ean == ref.get("ean")):
            continue

        if score > 0:
            scored.append((score, ref))

    scored.sort(key=lambda item: item[0], reverse=True)
    top = []
    for _, ref in scored[:limit]:
        top.append({
            "isk": ref["isk"],
            "sku": ref.get("sku", ""),
            "title": ref.get("title", ""),
            "ean": ref.get("ean", ""),
            "price": ref.get("price"),
        })
    return top


async def _call_gpt_match(
    unmapped_batch: list[dict],
    reference_pool: list[dict],
) -> list[dict]:
    """Send a batch of unmapped products + reference pool to GPT for matching."""
    client = _get_openai()
    indexed_reference = _build_reference_index(reference_pool)
    exact_registry_lookup = _load_listing_registry_exact_map()
    tasks_payload = []
    candidate_total = 0
    for unmapped in unmapped_batch:
        candidates = _select_candidates(
            unmapped,
            indexed_reference,
            exact_registry_lookup=exact_registry_lookup,
            limit=24,
        )
        candidate_total += len(candidates)
        tasks_payload.append({
            "unmapped": {
                "sku": unmapped["sku"],
                "title": unmapped["title"],
                "asin": unmapped.get("asin", ""),
                "ean": unmapped.get("ean", ""),
            },
            "candidates": candidates,
        })

    user_prompt = f"""Match these unmapped Amazon products to the candidate pools below.

Each product has its own short candidate list already prefiltered locally.
Use ONLY the candidates listed for that product.

INPUT:
{json.dumps({"items": tasks_payload}, ensure_ascii=False, indent=1)}

Return ONLY a JSON object with shape:
{{
  "results": [
    {{
      "unmapped_sku": "...",
      "matched_internal_sku": "..." or null,
      "matched_sku": "...",
      "confidence": 0-100,
      "reasoning": "short explanation in Polish",
      "quantity_in_bundle": 1,
      "bom": [{{"internal_sku": "...", "name": "...", "qty": 1, "unit_price_pln": 0.0}}],
      "total_price_pln": 0.0
    }}
  ]
}}"""

    log.info(
        "ai_matcher.calling_gpt",
        unmapped_count=len(unmapped_batch),
        ref_count=len(indexed_reference),
        candidate_total=candidate_total,
        model=settings.OPENAI_MODEL or "gpt-5.2",
    )

    resp = await client.chat.completions.create(
        model=settings.OPENAI_MODEL or "gpt-5.2",
        max_completion_tokens=4096,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
    )

    content = resp.choices[0].message.content or "{}"
    parsed = json.loads(content)

    # GPT may wrap results in {"results": [...]} or return array directly
    if isinstance(parsed, list):
        results = parsed
    elif isinstance(parsed, dict):
        results = parsed.get("results", parsed.get("matches", []))
        if not isinstance(results, list):
            results = [parsed]
    else:
        results = []

    usage = resp.usage
    log.info(
        "ai_matcher.gpt_response",
        results_count=len(results),
        prompt_tokens=usage.prompt_tokens if usage else None,
        completion_tokens=usage.completion_tokens if usage else None,
    )

    return results


# ---------------------------------------------------------------------------
# Save suggestions to DB
# ---------------------------------------------------------------------------

def _save_suggestions(suggestions: list[dict]) -> int:
    """Save AI suggestions to acc_product_match_suggestion table."""
    if not suggestions:
        return 0

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")
        saved = 0

        for s in suggestions:
            unmapped_sku = s.get("unmapped_sku", "")
            if not unmapped_sku:
                continue

            matched_isk = s.get("matched_internal_sku")
            if not matched_isk:
                continue  # skip no-match results

            confidence = float(s.get("confidence", 0))
            if confidence < 20:
                continue  # skip very low confidence matches

            reasoning = s.get("reasoning", "")[:2000]
            qty = int(s.get("quantity_in_bundle", 1))
            bom = s.get("bom", [])
            bom_json = json.dumps(bom, ensure_ascii=False) if bom else None
            total_price = s.get("total_price_pln")

            # Calculate unit price from BOM if available
            unit_price = None
            if bom and isinstance(bom, list) and len(bom) > 0:
                # Unit price = price of the primary (first) component
                unit_price = bom[0].get("unit_price_pln")
            elif total_price and qty > 0:
                unit_price = round(float(total_price) / qty, 4)

            # Look up unmapped product_id
            cur.execute("""
                SELECT TOP 1 id, asin, title
                FROM dbo.acc_product WITH (NOLOCK)
                WHERE sku = ?
            """, [unmapped_sku])
            row = cur.fetchone()
            product_id = row[0] if row else None
            unmapped_asin = row[1] if row else None
            unmapped_title = row[2] if row else None

            # Get matched title
            matched_title = None
            matched_sku = s.get("matched_sku", "")
            if matched_isk:
                cur.execute("""
                    SELECT TOP 1 title
                    FROM dbo.acc_product WITH (NOLOCK)
                    WHERE internal_sku = ?
                """, [matched_isk])
                mrow = cur.fetchone()
                matched_title = mrow[0] if mrow else None

            cur.execute("""
                INSERT INTO dbo.acc_product_match_suggestion
                    (unmapped_product_id, unmapped_sku, unmapped_asin, unmapped_title,
                     matched_internal_sku, matched_title, matched_sku,
                     confidence, reasoning, quantity_in_bundle,
                     unit_price_pln, total_price_pln, bom_json,
                     status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', SYSUTCDATETIME(), SYSUTCDATETIME())
            """, [
                str(product_id) if product_id else None,
                unmapped_sku,
                unmapped_asin,
                unmapped_title,
                matched_isk,
                matched_title,
                matched_sku,
                confidence,
                reasoning,
                qty,
                float(unit_price) if unit_price else None,
                float(total_price) if total_price else None,
                bom_json,
            ])
            saved += 1

        conn.commit()
        log.info("ai_matcher.saved_suggestions", count=saved)
        return saved
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

async def run_ai_matching() -> dict[str, Any]:
    """Run AI matching pipeline: load data, call GPT, save suggestions.
    
    Returns summary dict with counts.
    """
    # Load data
    unmapped = _load_unmapped_products()
    if not unmapped:
        return {"unmapped_count": 0, "suggestions_saved": 0, "message": "Brak produktów do dopasowania"}

    reference = _load_mapped_products()
    if not reference:
        return {"unmapped_count": len(unmapped), "suggestions_saved": 0, "message": "Brak produktów referencyjnych"}

    log.info("ai_matcher.start", unmapped=len(unmapped), reference=len(reference))

    # Small batches keep prompts predictable and make retries cheaper.
    batch_size = 8
    all_suggestions: list[dict] = []
    batch_errors: list[dict[str, Any]] = []

    for i in range(0, len(unmapped), batch_size):
        batch = unmapped[i:i + batch_size]
        try:
            results = await _call_gpt_match(batch, reference)
            all_suggestions.extend(results)
        except Exception as exc:
            code, friendly_message = _classify_ai_error(exc)
            batch_errors.append({
                "batch_start": i,
                "batch_size": len(batch),
                "code": code,
                "message": friendly_message,
            })
            log.error("ai_matcher.gpt_error", batch_start=i, error=str(exc), error_code=code, friendly_message=friendly_message)
            if code in {"quota_exceeded", "auth_error", "unsupported_parameter", "context_too_large"}:
                break

    # Save to DB
    saved = _save_suggestions(all_suggestions)
    error_summary = "; ".join(
        f"batch {err['batch_start'] + 1}: {err['message']}"
        for err in batch_errors[:3]
    )
    if len(batch_errors) > 3:
        error_summary += f" (+{len(batch_errors) - 3} kolejnych błędów)"

    if batch_errors and not all_suggestions:
        primary = batch_errors[0]
        return {
            "status": "error",
            "unmapped_count": len(unmapped),
            "batches_processed": math.ceil(len(unmapped) / batch_size),
            "gpt_results": 0,
            "suggestions_saved": 0,
            "errors_count": len(batch_errors),
            "error_code": primary["code"],
            "error_summary": error_summary or primary["message"],
            "message": primary["message"],
        }

    status = "partial" if batch_errors else "ok"
    message = f"Zapisano {saved} sugestii do weryfikacji"
    if batch_errors:
        message += f" (część batchy nie przeszła: {len(batch_errors)})"

    return {
        "status": status,
        "unmapped_count": len(unmapped),
        "batches_processed": math.ceil(len(unmapped) / batch_size),
        "gpt_results": len(all_suggestions),
        "suggestions_saved": saved,
        "errors_count": len(batch_errors),
        "error_code": batch_errors[0]["code"] if batch_errors else None,
        "error_summary": error_summary or None,
        "message": message,
    }


def get_match_suggestions(
    status: str = "pending",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Get match suggestions from DB with pagination."""
    conn = _connect()
    try:
        cur = conn.cursor()
        offset = (page - 1) * page_size

        # Count
        cur.execute("""
            SELECT COUNT(*) FROM dbo.acc_product_match_suggestion WITH (NOLOCK)
            WHERE status = ?
        """, [status])
        total = cur.fetchone()[0]

        # Fetch
        cur.execute("""
            SELECT
                id, unmapped_sku, unmapped_asin, unmapped_title,
                matched_internal_sku, matched_title, matched_sku,
                confidence, reasoning, quantity_in_bundle,
                unit_price_pln, total_price_pln, bom_json,
                status, created_at
            FROM dbo.acc_product_match_suggestion WITH (NOLOCK)
            WHERE status = ?
            ORDER BY confidence DESC, id
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, [status, offset, page_size])
        rows = _fetchall_dict(cur)

        # Parse bom_json
        items = []
        for r in rows:
            bom_raw = r.pop("bom_json", None)
            r["bom"] = json.loads(bom_raw) if bom_raw else []
            r["confidence"] = float(r.get("confidence", 0))
            r["unit_price_pln"] = float(r["unit_price_pln"]) if r.get("unit_price_pln") else None
            r["total_price_pln"] = float(r["total_price_pln"]) if r.get("total_price_pln") else None
            r["quantity_in_bundle"] = int(r.get("quantity_in_bundle", 1))
            r["created_at"] = str(r.get("created_at", ""))
            items.append(r)

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": math.ceil(total / page_size) if total > 0 else 0,
            "items": items,
        }
    finally:
        conn.close()


def approve_suggestion(suggestion_id: int) -> dict[str, Any]:
    """Approve a suggestion: map product + set price.
    
    This is the ONLY way a suggestion gets applied — user must explicitly approve.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        # Fetch suggestion
        cur.execute("""
            SELECT id, unmapped_sku, matched_internal_sku, total_price_pln,
                   quantity_in_bundle, unit_price_pln, status
            FROM dbo.acc_product_match_suggestion
            WHERE id = ?
        """, [suggestion_id])
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Suggestion {suggestion_id} not found")

        cols = [c[0] for c in cur.description]
        s = {cols[i]: row[i] for i in range(len(cols))}

        if s["status"] != "pending":
            raise ValueError(f"Suggestion {suggestion_id} already {s['status']}")

        sku = s["unmapped_sku"]
        isk = s["matched_internal_sku"]
        total_price = float(s["total_price_pln"]) if s["total_price_pln"] else None

        if not isk:
            raise ValueError("No matched internal_sku")

        # Step 1: Map acc_product.internal_sku
        # --- Controlling: log mapping change ---
        try:
            from app.services.controlling import log_mapping_change
            cur.execute(
                "SELECT CAST(id AS VARCHAR(36)), internal_sku, mapping_source "
                "FROM dbo.acc_product WHERE sku = ?", [sku]
            )
            for prow in cur.fetchall():
                log_mapping_change(
                    conn,
                    product_id=str(prow[0]),
                    sku=sku,
                    old_internal_sku=str(prow[1]) if prow[1] else None,
                    new_internal_sku=isk,
                    old_source=str(prow[2]) if prow[2] else None,
                    new_source="ai_match",
                    change_type="set" if not prow[1] else "update",
                    reason=f"AI match suggestion #{suggestion_id} approved",
                )
        except Exception:
            pass  # controlling is non-blocking

        cur.execute("""
            UPDATE dbo.acc_product
            SET internal_sku = ?, mapping_source = 'ai_match', updated_at = GETDATE()
            WHERE sku = ? AND (internal_sku IS NULL OR internal_sku = '')
        """, [isk, sku])
        mapped_count = cur.rowcount

        # Step 2: Upsert purchase price (if total_price available)
        price_status = None
        if total_price and total_price > 0:
            from datetime import date as date_cls
            now_str = date_cls.today().isoformat()

            cur.execute("""
                SELECT id FROM dbo.acc_purchase_price
                WHERE internal_sku = ? AND source = 'ai_match'
            """, [isk])
            existing = cur.fetchone()

            if existing:
                cur.execute("""
                    UPDATE dbo.acc_purchase_price
                    SET netto_price_pln = ?, valid_from = ?, updated_at = GETDATE()
                    WHERE id = ?
                """, [total_price, now_str, existing[0]])
                price_status = "updated"
            else:
                # Only insert if no manual price exists
                cur.execute("""
                    SELECT id FROM dbo.acc_purchase_price
                    WHERE internal_sku = ? AND source = 'manual'
                """, [isk])
                manual = cur.fetchone()
                if not manual:
                    cur.execute("""
                        INSERT INTO dbo.acc_purchase_price
                            (internal_sku, netto_price_pln, valid_from, source, source_document, created_at, updated_at)
                        VALUES (?, ?, ?, 'ai_match', 'ai_product_matcher', GETDATE(), GETDATE())
                    """, [isk, total_price, now_str])
                    price_status = "created"
                else:
                    price_status = "manual_exists"

        # Step 3: Mark suggestion as approved
        cur.execute("""
            UPDATE dbo.acc_product_match_suggestion
            SET status = 'approved', reviewed_at = SYSUTCDATETIME(), updated_at = SYSUTCDATETIME()
            WHERE id = ?
        """, [suggestion_id])

        conn.commit()
        log.info("ai_matcher.approved", id=suggestion_id, sku=sku, isk=isk, mapped=mapped_count)

        return {
            "id": suggestion_id,
            "status": "approved",
            "unmapped_sku": sku,
            "matched_internal_sku": isk,
            "mapped_products": mapped_count,
            "price_status": price_status,
        }
    finally:
        conn.close()


def reject_suggestion(suggestion_id: int) -> dict[str, Any]:
    """Reject a suggestion — no changes applied."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        cur.execute("""
            SELECT id, unmapped_sku, status
            FROM dbo.acc_product_match_suggestion
            WHERE id = ?
        """, [suggestion_id])
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Suggestion {suggestion_id} not found")
        if row[2] != "pending":
            raise ValueError(f"Suggestion {suggestion_id} already {row[2]}")

        cur.execute("""
            UPDATE dbo.acc_product_match_suggestion
            SET status = 'rejected', reviewed_at = SYSUTCDATETIME(), updated_at = SYSUTCDATETIME()
            WHERE id = ?
        """, [suggestion_id])
        conn.commit()

        log.info("ai_matcher.rejected", id=suggestion_id, sku=row[1])
        return {
            "id": suggestion_id,
            "status": "rejected",
            "unmapped_sku": row[1],
        }
    finally:
        conn.close()
