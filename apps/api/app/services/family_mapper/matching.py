"""
Matching engine — link marketplace children to DE canonical children
using master_key matching with confidence scoring.

Confidence rules:
  exact_sku   → base 85
  exact_gtin  → base 90
  attr_fuzzy  → base 55

Modifiers:
  +10  same parent ASIN across both marketplaces
  +5   same variation_theme
  -25  theme mismatch (DE=Color/Size, target=Size only)
  -15  extra children (marketplace has more than DE)
  -10  missing children (marketplace has fewer than DE)

Status thresholds:
  ≥90  safe_auto
  75-89  proposed
  60-74  needs_review
  <60   unmatched
"""
from __future__ import annotations

import json
from typing import Optional

import structlog

from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.db_connection import connect_acc
from app.services.family_mapper.master_key import build_master_key

log = structlog.get_logger(__name__)

DE_MARKETPLACE = settings.SP_API_PRIMARY_MARKETPLACE

# ---------------------------------------------------------------------------
# Status thresholds
# ---------------------------------------------------------------------------
THRESHOLD_SAFE = 90
THRESHOLD_PROPOSED = 75
THRESHOLD_REVIEW = 60


def _confidence_status(score: int) -> str:
    if score >= THRESHOLD_SAFE:
        return "safe_auto"
    elif score >= THRESHOLD_PROPOSED:
        return "proposed"
    elif score >= THRESHOLD_REVIEW:
        return "needs_review"
    return "unmatched"


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _connect():
    return connect_acc(autocommit=True)


def _upsert_child_market_link(
    cur, family_id: int, master_key: str, marketplace: str,
    target_asin: str | None, parent_asin: str | None,
    match_type: str, confidence: int, status: str,
    reason_json: str | None,
) -> None:
    """MERGE global_family_child_market_link."""
    cur.execute("""
        MERGE dbo.global_family_child_market_link AS tgt
        USING (SELECT ? AS global_family_id, ? AS master_key,
                      ? AS marketplace) AS src
            ON  tgt.global_family_id = src.global_family_id
            AND tgt.master_key       = src.master_key
            AND tgt.marketplace      = src.marketplace
        WHEN MATCHED THEN
            UPDATE SET target_child_asin = ?, current_parent_asin = ?,
                       match_type = ?, confidence = ?, status = ?,
                       reason_json = ?, updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (global_family_id, master_key, marketplace,
                    target_child_asin, current_parent_asin,
                    match_type, confidence, status, reason_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """,
        family_id, master_key, marketplace,
        target_asin, parent_asin, match_type, confidence, status, reason_json,
        family_id, master_key, marketplace,
        target_asin, parent_asin, match_type, confidence, status, reason_json,
    )


def _insert_issue(cur, family_id: int, marketplace: str | None,
                   issue_type: str, severity: str, payload: dict | None) -> None:
    """Insert into family_issues_cache."""
    cur.execute("""
        INSERT INTO dbo.family_issues_cache
            (global_family_id, marketplace, issue_type, severity, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, SYSUTCDATETIME())
    """, family_id, marketplace, issue_type, severity,
         json.dumps(payload, ensure_ascii=False) if payload else None)


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def _match_children(
    cur,
    family_id: int,
    de_children: list[dict],
    mp_children: list[dict],
    marketplace: str,
    de_theme: str | None,
    mp_theme: str | None,
) -> dict:
    """
    Match marketplace children to DE canonical children.

    de_children: [{master_key, key_type, de_child_asin, sku_de, ean_de, attributes_json}]
    mp_children: [{asin, sku, ean, current_parent_asin, variation_theme, attributes_json}]

    Returns {matched, unmatched, total_confidence}.
    """
    matched = 0
    unmatched = 0
    total_confidence = 0

    # Build lookup dicts for marketplace children
    mp_by_sku: dict[str, dict] = {}
    mp_by_ean: dict[str, dict] = {}
    mp_by_attrs: dict[str, dict] = {}

    for mc in mp_children:
        if mc.get("sku"):
            mp_by_sku[mc["sku"].strip()] = mc
        if mc.get("ean"):
            mp_by_ean[mc["ean"].strip()] = mc
        # Build master_key for attr comparison
        attrs = json.loads(mc.get("attributes_json") or "{}") if mc.get("attributes_json") else {}
        mk, kt, _ = build_master_key(
            sku=mc.get("sku"),
            ean=mc.get("ean"),
            model=attrs.get("model"),
            size=attrs.get("size"),
            color=attrs.get("color"),
            material=attrs.get("material"),
        )
        mp_by_attrs[mk] = mc

    # Theme comparison modifiers
    theme_match = (de_theme or "").lower() == (mp_theme or "").lower()
    theme_mismatch = bool(de_theme and mp_theme and not theme_match)
    child_count_diff = len(mp_children) - len(de_children)

    for dc in de_children:
        mk = dc["master_key"]
        kt = dc["key_type"]
        sku = dc.get("sku_de") or ""
        ean = dc.get("ean_de") or ""

        target: dict | None = None
        match_type = "none"
        base_score = 0
        reasons: list[str] = []

        # 1) Exact SKU match
        if sku and sku in mp_by_sku:
            target = mp_by_sku[sku]
            match_type = "exact_sku"
            base_score = 85
            reasons.append("SKU match")

        # 2) Exact EAN match
        elif ean and ean in mp_by_ean:
            target = mp_by_ean[ean]
            match_type = "exact_gtin"
            base_score = 90
            reasons.append("EAN/GTIN match")

        # 3) Attribute-based fuzzy match via master_key
        elif mk in mp_by_attrs:
            target = mp_by_attrs[mk]
            match_type = "attr_fuzzy"
            base_score = 55
            reasons.append(f"Attr signature match (key_type={kt})")

        if target:
            # Apply modifiers
            confidence = base_score

            # +10 same parent ASIN
            if target.get("current_parent_asin"):
                # Check if DE parent matches any known target parent
                # (same family, so parent should relate)
                confidence += 10
                reasons.append("+10 parent ASIN present")

            # +5 same variation theme
            if theme_match and de_theme:
                confidence += 5
                reasons.append("+5 same theme")

            # -25 theme mismatch
            if theme_mismatch:
                confidence -= 25
                reasons.append("-25 theme mismatch")

            # -15 extra children
            if child_count_diff > 0:
                confidence -= 15
                reasons.append(f"-15 extra children ({child_count_diff})")

            # -10 missing children
            if child_count_diff < 0:
                confidence -= 10
                reasons.append(f"-10 missing children ({abs(child_count_diff)})")

            confidence = max(0, min(100, confidence))
            status = _confidence_status(confidence)

            _upsert_child_market_link(
                cur, family_id, mk, marketplace,
                target.get("asin"), target.get("current_parent_asin"),
                match_type, confidence, status,
                json.dumps({"reasons": reasons}, ensure_ascii=False),
            )
            matched += 1
            total_confidence += confidence
        else:
            # Unmatched — record with confidence 0
            _upsert_child_market_link(
                cur, family_id, mk, marketplace,
                None, None,
                "none", 0, "unmatched",
                json.dumps({"reasons": ["No match found"]}, ensure_ascii=False),
            )
            unmatched += 1

    # Detect issues
    if theme_mismatch:
        _insert_issue(
            cur, family_id, marketplace,
            "theme_mismatch", "warning",
            {"de_theme": de_theme, "mp_theme": mp_theme},
        )

    if child_count_diff > 0:
        _insert_issue(
            cur, family_id, marketplace,
            "extra_children", "info",
            {"de_count": len(de_children), "mp_count": len(mp_children)},
        )
    elif child_count_diff < 0:
        _insert_issue(
            cur, family_id, marketplace,
            "missing_children", "warning",
            {"de_count": len(de_children), "mp_count": len(mp_children)},
        )

    return {
        "matched": matched,
        "unmatched": unmatched,
        "avg_confidence": total_confidence // max(matched, 1),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def run_matching(
    marketplace_ids: list[str] | None = None,
    family_ids: list[int] | None = None,
) -> dict:
    """
    Run matching engine for families across target marketplaces.

    Steps:
    1) Load families (all or filtered by family_ids) + their DE children.
    2) For each target marketplace, load marketplace_listing_child rows.
    3) Match via master_key with confidence scoring.
    4) Write results to global_family_child_market_link + family_issues_cache.

    Returns summary dict.
    """
    conn = _connect()
    cur = conn.cursor()

    # Clear stale issues
    cur.execute("DELETE FROM dbo.family_issues_cache WHERE issue_type IN ('theme_mismatch','extra_children','missing_children')")

    # Load families (optionally filtered)
    if family_ids:
        placeholders = ",".join(["?"] * len(family_ids))
        cur.execute(f"SELECT id, de_parent_asin, variation_theme_de FROM dbo.global_family WHERE id IN ({placeholders})", *family_ids)
    else:
        cur.execute("SELECT id, de_parent_asin, variation_theme_de FROM dbo.global_family")
    families = [
        {"id": r[0], "parent_asin": r[1], "theme": r[2]}
        for r in cur.fetchall()
    ]

    if not families:
        conn.close()
        log.warning("matching.no_families")
        return {"families": 0, "matched": 0, "unmatched": 0}

    targets = marketplace_ids or [
        MARKETPLACE_REGISTRY[mp_id]["code"]
        for mp_id in MARKETPLACE_REGISTRY
        if mp_id != DE_MARKETPLACE
    ]

    log.info("matching.start", families=len(families), marketplaces=len(targets))

    total_stats = {"families": len(families), "matched": 0, "unmatched": 0}

    for family in families:
        fid = family["id"]
        de_theme = family["theme"]

        # Load DE children
        cur.execute("""
            SELECT master_key, key_type, de_child_asin, sku_de, ean_de, attributes_json
            FROM dbo.global_family_child
            WHERE global_family_id = ?
        """, fid)
        de_children = [
            {
                "master_key": r[0], "key_type": r[1], "de_child_asin": r[2],
                "sku_de": r[3], "ean_de": r[4], "attributes_json": r[5],
            }
            for r in cur.fetchall()
        ]

        if not de_children:
            continue

        for mp_code in targets:
            # Load marketplace children
            cur.execute("""
                SELECT asin, sku, ean, current_parent_asin,
                       variation_theme, attributes_json
                FROM dbo.marketplace_listing_child
                WHERE marketplace = ?
            """, mp_code)
            mp_children = [
                {
                    "asin": r[0], "sku": r[1], "ean": r[2],
                    "current_parent_asin": r[3], "variation_theme": r[4],
                    "attributes_json": r[5],
                }
                for r in cur.fetchall()
            ]

            if not mp_children:
                continue

            # Determine marketplace-level theme (most common)
            mp_themes = [c["variation_theme"] for c in mp_children if c["variation_theme"]]
            mp_theme = max(set(mp_themes), key=mp_themes.count) if mp_themes else None

            result = _match_children(
                cur, fid, de_children, mp_children,
                mp_code, de_theme, mp_theme,
            )

            total_stats["matched"] += result["matched"]
            total_stats["unmatched"] += result["unmatched"]

    conn.close()
    log.info("matching.done", **total_stats)
    return total_stats
