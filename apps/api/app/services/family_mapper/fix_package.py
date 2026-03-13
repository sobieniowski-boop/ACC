"""
Fix package generator — produce actionable plans for fixing variation
family structures per marketplace.

A fix package contains ordered steps:
  1) DELETE orphan / extra children from target marketplace listing
  2) CREATE new parent ASIN if target marketplace lacks one
  3) UPDATE child attributes (re-link, add missing variations)

Constraints:
  - Minimum confidence 75 for any child in the package
  - Coverage must be ≥ 90% OR the package requires explicit approve
  - Packages start as 'draft', go through 'pending_approve' → 'approved' → 'applied'
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog

from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

DE_MARKETPLACE = settings.SP_API_PRIMARY_MARKETPLACE

MIN_CONFIDENCE = 75
COVERAGE_AUTO_THRESHOLD = 90


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _connect():
    return connect_acc(autocommit=True)


# ---------------------------------------------------------------------------
# Plan builder
# ---------------------------------------------------------------------------

def _build_action_plan(
    cur,
    family_id: int,
    marketplace: str,
    de_children: list[dict],
    coverage: dict,
) -> dict | None:
    """
    Build the action plan JSON for one (family, marketplace) pair.

    Returns plan dict or None if no actions needed.
    """
    steps: list[dict] = []

    # Load matched links
    cur.execute("""
        SELECT master_key, target_child_asin, current_parent_asin,
               match_type, confidence, status
        FROM dbo.global_family_child_market_link
        WHERE global_family_id = ? AND marketplace = ?
    """, family_id, marketplace)
    links = [
        {
            "master_key": r[0], "target_child_asin": r[1],
            "current_parent_asin": r[2], "match_type": r[3],
            "confidence": r[4], "status": r[5],
        }
        for r in cur.fetchall()
    ]

    link_map = {l["master_key"]: l for l in links}

    # Load marketplace children for this family's target parent
    target_parent = None
    cur.execute("""
        SELECT target_parent_asin
        FROM dbo.global_family_market_link
        WHERE global_family_id = ? AND marketplace = ?
    """, family_id, marketplace)
    tp_row = cur.fetchone()
    if tp_row and tp_row[0]:
        target_parent = tp_row[0]

    # Step 1: DELETE orphans — marketplace children not mapped to any DE child
    if target_parent:
        cur.execute("""
            SELECT asin
            FROM dbo.marketplace_listing_child
            WHERE marketplace = ? AND current_parent_asin = ?
              AND asin NOT IN (
                  SELECT ISNULL(target_child_asin, '')
                  FROM dbo.global_family_child_market_link
                  WHERE global_family_id = ? AND marketplace = ?
              )
        """, marketplace, target_parent, family_id, marketplace)
        orphans = [r[0] for r in cur.fetchall()]
        for orphan_asin in orphans:
            steps.append({
                "order": len(steps) + 1,
                "action": "DELETE",
                "type": "remove_orphan_child",
                "marketplace": marketplace,
                "asin": orphan_asin,
                "reason": f"Orphan child in {marketplace} not matching any DE canonical child",
            })

    # Step 2: CREATE parent if missing
    if not target_parent and coverage.get("matched_children_count", 0) > 0:
        steps.append({
            "order": len(steps) + 1,
            "action": "CREATE",
            "type": "create_parent",
            "marketplace": marketplace,
            "reason": f"No parent ASIN found in {marketplace} for DE family",
        })

    # Step 3: UPDATE — re-link or create missing children
    for dc in de_children:
        mk = dc["master_key"]
        link = link_map.get(mk)

        if not link or link["status"] == "unmatched":
            # Missing child — needs creation
            steps.append({
                "order": len(steps) + 1,
                "action": "CREATE",
                "type": "create_child",
                "marketplace": marketplace,
                "master_key": mk,
                "de_child_asin": dc.get("de_child_asin"),
                "de_sku": dc.get("sku_de"),
                "reason": f"DE child {dc.get('de_child_asin')} has no match in {marketplace}",
            })
        elif link.get("confidence", 0) < MIN_CONFIDENCE:
            # Low confidence — flag for review
            steps.append({
                "order": len(steps) + 1,
                "action": "REVIEW",
                "type": "low_confidence_link",
                "marketplace": marketplace,
                "master_key": mk,
                "de_child_asin": dc.get("de_child_asin"),
                "target_child_asin": link.get("target_child_asin"),
                "confidence": link.get("confidence"),
                "reason": f"Match confidence {link.get('confidence')} below threshold {MIN_CONFIDENCE}",
            })

    if not steps:
        return None

    return {
        "family_id": family_id,
        "marketplace": marketplace,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "steps": steps,
        "summary": {
            "delete_count": sum(1 for s in steps if s["action"] == "DELETE"),
            "create_count": sum(1 for s in steps if s["action"] == "CREATE"),
            "review_count": sum(1 for s in steps if s["action"] == "REVIEW"),
        },
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def generate_fix_package(
    family_id: int | None = None,
    marketplace: str | None = None,
) -> dict:
    """
    Generate fix packages for families/marketplaces.

    If family_id is None, processes all families.
    If marketplace is None, processes all non-DE marketplaces.

    Returns summary dict.
    """
    conn = _connect()
    cur = conn.cursor()

    # Determine scope
    if family_id:
        cur.execute("SELECT id FROM dbo.global_family WHERE id = ?", family_id)
        family_ids = [r[0] for r in cur.fetchall()]
    else:
        cur.execute("SELECT id FROM dbo.global_family")
        family_ids = [r[0] for r in cur.fetchall()]

    mp_codes = (
        [marketplace] if marketplace
        else [
            info["code"]
            for mp_id, info in MARKETPLACE_REGISTRY.items()
            if mp_id != DE_MARKETPLACE
        ]
    )

    log.info("fix_package.start", families=len(family_ids), marketplaces=len(mp_codes))

    stats = {"generated": 0, "skipped": 0, "errors": 0}

    for fid in family_ids:
        # Load DE children
        cur.execute("""
            SELECT master_key, key_type, de_child_asin, sku_de, ean_de
            FROM dbo.global_family_child
            WHERE global_family_id = ?
        """, fid)
        de_children = [
            {"master_key": r[0], "key_type": r[1], "de_child_asin": r[2],
             "sku_de": r[3], "ean_de": r[4]}
            for r in cur.fetchall()
        ]

        if not de_children:
            continue

        for mp_code in mp_codes:
            try:
                # Load coverage
                cur.execute("""
                    SELECT de_children_count, matched_children_count,
                           coverage_pct, missing_children_count,
                           extra_children_count, confidence_avg
                    FROM dbo.family_coverage_cache
                    WHERE global_family_id = ? AND marketplace = ?
                """, fid, mp_code)
                cov_row = cur.fetchone()
                coverage = {
                    "de_children_count": cov_row[0] if cov_row else 0,
                    "matched_children_count": cov_row[1] if cov_row else 0,
                    "coverage_pct": cov_row[2] if cov_row else 0,
                    "missing_children_count": cov_row[3] if cov_row else 0,
                    "extra_children_count": cov_row[4] if cov_row else 0,
                    "confidence_avg": cov_row[5] if cov_row else 0,
                }

                plan = _build_action_plan(cur, fid, mp_code, de_children, coverage)
                if not plan:
                    stats["skipped"] += 1
                    continue

                # Determine initial status
                cov_pct = coverage.get("coverage_pct", 0)
                avg_conf = coverage.get("confidence_avg", 0)
                status = "draft"
                if cov_pct >= COVERAGE_AUTO_THRESHOLD and avg_conf >= MIN_CONFIDENCE:
                    status = "pending_approve"

                # Insert fix package
                cur.execute("""
                    INSERT INTO dbo.family_fix_package
                        (marketplace, global_family_id, action_plan_json,
                         status, generated_at)
                    VALUES (?, ?, ?, ?, SYSUTCDATETIME())
                """, mp_code, fid,
                    json.dumps(plan, ensure_ascii=False), status)

                stats["generated"] += 1

            except Exception as exc:
                log.error("fix_package.error", family=fid, mp=mp_code, error=str(exc))
                stats["errors"] += 1

    conn.close()
    log.info("fix_package.done", **stats)
    return stats
