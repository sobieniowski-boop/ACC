"""
Coverage cache — recompute family_coverage_cache and
global_family_market_link aggregates for all families × marketplaces.

Called after matching completes to give a per-family per-marketplace
snapshot of child coverage, missing/extra children, theme mismatches,
and average confidence.
"""
from __future__ import annotations

import structlog

from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

DE_MARKETPLACE = settings.SP_API_PRIMARY_MARKETPLACE


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _connect():
    return connect_acc(autocommit=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def recompute_coverage() -> dict:
    """
    Recompute family_coverage_cache and global_family_market_link
    for every (family, marketplace) pair.

    Logic:
      - de_children_count = count of global_family_child for that family
      - matched_children_count = count of child_market_link rows
        where status IN ('safe_auto','proposed','needs_review')
      - coverage_pct = matched_children_count / de_children_count * 100
      - missing = de_children_count - matched_children_count
      - extra = marketplace children not mapped to any DE child
      - theme_mismatch = any issue of type 'theme_mismatch'
      - confidence_avg = average confidence across matched links

    Returns summary dict.
    """
    conn = _connect()
    cur = conn.cursor()

    # Get all family IDs + child counts
    cur.execute("""
        SELECT gf.id, gf.variation_theme_de,
               COUNT(gfc.id) AS de_count
        FROM dbo.global_family gf
        LEFT JOIN dbo.global_family_child gfc ON gfc.global_family_id = gf.id
        GROUP BY gf.id, gf.variation_theme_de
    """)
    families = [
        {"id": r[0], "theme": r[1], "de_count": r[2]}
        for r in cur.fetchall()
    ]

    if not families:
        conn.close()
        log.warning("coverage.no_families")
        return {"updated": 0}

    # Get all non-DE marketplace codes
    mp_codes = [
        info["code"]
        for mp_id, info in MARKETPLACE_REGISTRY.items()
        if mp_id != DE_MARKETPLACE
    ]

    updated = 0

    for family in families:
        fid = family["id"]
        de_count = family["de_count"]
        if de_count == 0:
            continue

        for mp_code in mp_codes:
            # Count matched children (status != 'unmatched')
            cur.execute("""
                SELECT
                    COUNT(*)                                        AS matched,
                    ISNULL(AVG(confidence), 0)                      AS avg_conf
                FROM dbo.global_family_child_market_link
                WHERE global_family_id = ?
                  AND marketplace      = ?
                  AND status          <> 'unmatched'
            """, fid, mp_code)
            row = cur.fetchone()
            matched = row[0] if row else 0
            avg_conf = row[1] if row else 0

            # Extra children = marketplace listing children with this parent
            # that are NOT linked to any DE child
            cur.execute("""
                SELECT COUNT(*)
                FROM dbo.marketplace_listing_child mlc
                WHERE mlc.marketplace = ?
                  AND mlc.current_parent_asin IN (
                      SELECT target_parent_asin
                      FROM dbo.global_family_market_link
                      WHERE global_family_id = ? AND marketplace = ?
                  )
                  AND mlc.asin NOT IN (
                      SELECT ISNULL(target_child_asin, '')
                      FROM dbo.global_family_child_market_link
                      WHERE global_family_id = ? AND marketplace = ?
                  )
            """, mp_code, fid, mp_code, fid, mp_code)
            extra_row = cur.fetchone()
            extra_count = extra_row[0] if extra_row else 0

            # Theme mismatch check
            cur.execute("""
                SELECT COUNT(*)
                FROM dbo.family_issues_cache
                WHERE global_family_id = ?
                  AND marketplace = ?
                  AND issue_type = 'theme_mismatch'
            """, fid, mp_code)
            tm_row = cur.fetchone()
            theme_mismatch = (tm_row[0] or 0) > 0

            missing = max(0, de_count - matched)
            coverage_pct = min(100, (matched * 100) // de_count)

            # MERGE family_coverage_cache
            cur.execute("""
                MERGE dbo.family_coverage_cache AS tgt
                USING (SELECT ? AS global_family_id, ? AS marketplace) AS src
                    ON  tgt.global_family_id = src.global_family_id
                    AND tgt.marketplace      = src.marketplace
                WHEN MATCHED THEN
                    UPDATE SET
                        de_children_count      = ?,
                        matched_children_count = ?,
                        coverage_pct           = ?,
                        missing_children_count = ?,
                        extra_children_count   = ?,
                        theme_mismatch         = ?,
                        confidence_avg         = ?,
                        updated_at             = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (global_family_id, marketplace,
                            de_children_count, matched_children_count,
                            coverage_pct, missing_children_count,
                            extra_children_count, theme_mismatch,
                            confidence_avg, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
            """,
                fid, mp_code,
                de_count, matched, coverage_pct, missing, extra_count,
                1 if theme_mismatch else 0, avg_conf,
                fid, mp_code,
                de_count, matched, coverage_pct, missing, extra_count,
                1 if theme_mismatch else 0, avg_conf,
            )

            # Also update global_family_market_link status
            if coverage_pct >= 90 and avg_conf >= 80:
                link_status = "mapped"
            elif coverage_pct > 0:
                link_status = "partial"
            else:
                link_status = "unmapped"

            cur.execute("""
                MERGE dbo.global_family_market_link AS tgt
                USING (SELECT ? AS global_family_id, ? AS marketplace) AS src
                    ON  tgt.global_family_id = src.global_family_id
                    AND tgt.marketplace      = src.marketplace
                WHEN MATCHED THEN
                    UPDATE SET status = ?, confidence_avg = ?,
                               updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (global_family_id, marketplace, status,
                            confidence_avg, updated_at)
                    VALUES (?, ?, ?, ?, SYSUTCDATETIME());
            """,
                fid, mp_code,
                link_status, avg_conf,
                fid, mp_code, link_status, avg_conf,
            )

            updated += 1

    conn.close()
    log.info("coverage.done", updated=updated, families=len(families))
    return {"updated": updated, "families": len(families)}
