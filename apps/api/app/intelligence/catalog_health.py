"""Catalog Health Monitor — unified health scorecard & analytics.

Aggregates listing state, content completeness, and suppression data
into a per-listing health score and a catalog-level scorecard.

Sprint 9 – S9.1 / S9.3 / S9.4
Sprint 10 – S10.2 / S10.3 / S10.4

Scoring model (per listing, 0-100):
  - Listing active        20 pts  (ACTIVE=20, INACTIVE=10, SUPPRESSED/DELETED=0)
  - No issues             15 pts  (no issues=15, warnings only=8, errors=0)
  - Not suppressed        15 pts
  - Has title             10 pts
  - Has image             10 pts
  - Has price             10 pts
  - Content completeness  20 pts  (0-20 from content version coverage)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


# ── Schema DDL ──────────────────────────────────────────────────────────

_SCHEMA_STATEMENTS: list[str] = [
    # Field-level diff tracking for listing changes
    """
    IF OBJECT_ID('dbo.acc_listing_field_diff', 'U') IS NULL
    CREATE TABLE dbo.acc_listing_field_diff (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(100) NOT NULL,
        marketplace_id  VARCHAR(20)   NOT NULL,
        field_name      VARCHAR(50)   NOT NULL,
        old_value       NVARCHAR(500) NULL,
        new_value       NVARCHAR(500) NULL,
        change_source   VARCHAR(50)   NOT NULL DEFAULT 'unknown',
        detected_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_lfd_sku_mkt')
    CREATE INDEX ix_lfd_sku_mkt
        ON dbo.acc_listing_field_diff (seller_sku, marketplace_id, detected_at DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_lfd_detected')
    CREATE INDEX ix_lfd_detected
        ON dbo.acc_listing_field_diff (detected_at DESC)
    """,
    # ── Persisted health score snapshots (S10.4) ──
    """
    IF OBJECT_ID('dbo.acc_listing_health_snapshot', 'U') IS NULL
    CREATE TABLE dbo.acc_listing_health_snapshot (
        id                       BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku               NVARCHAR(100) NOT NULL,
        marketplace_id           VARCHAR(20)   NOT NULL,
        snapshot_date            DATE          NOT NULL,
        health_score             SMALLINT      NOT NULL,
        status_pts               SMALLINT      NOT NULL DEFAULT 0,
        issues_pts               SMALLINT      NOT NULL DEFAULT 0,
        suppression_pts          SMALLINT      NOT NULL DEFAULT 0,
        basic_content_pts        SMALLINT      NOT NULL DEFAULT 0,
        content_completeness_pts SMALLINT      NOT NULL DEFAULT 0,
        listing_status           VARCHAR(30)   NULL,
        is_suppressed            BIT           NOT NULL DEFAULT 0,
        has_issues               BIT           NOT NULL DEFAULT 0,
        computed_at              DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_lhs_sku_mkt_date UNIQUE (seller_sku, marketplace_id, snapshot_date)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_lhs_date')
    CREATE INDEX ix_lhs_date ON dbo.acc_listing_health_snapshot (snapshot_date DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_lhs_score')
    CREATE INDEX ix_lhs_score ON dbo.acc_listing_health_snapshot (snapshot_date, health_score)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_lhs_mkt')
    CREATE INDEX ix_lhs_mkt ON dbo.acc_listing_health_snapshot (marketplace_id, snapshot_date DESC)
    """,
]


def ensure_catalog_health_schema() -> None:
    """Create catalog health tables if they don't exist."""
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
        log.info("catalog_health.schema_ensured")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Per-listing health scoring (pure functions)
# ═══════════════════════════════════════════════════════════════════════════

_STATUS_SCORES: dict[str, int] = {
    "ACTIVE": 20,
    "INACTIVE": 10,
    "INCOMPLETE": 5,
}
# Everything else (SUPPRESSED, DELETED, UNKNOWN) → 0


def score_listing_status(listing_status: str | None) -> int:
    """Score for listing operational status (0-20)."""
    return _STATUS_SCORES.get((listing_status or "").upper(), 0)


def score_issues(has_issues: bool, issues_severity: str | None) -> int:
    """Score for issue-free listing (0-15)."""
    if not has_issues:
        return 15
    if (issues_severity or "").upper() == "ERROR":
        return 0
    return 8  # warnings only


def score_suppression(is_suppressed: bool) -> int:
    """Score for non-suppressed listing (0-15)."""
    return 0 if is_suppressed else 15


def score_basic_content(
    title: str | None,
    image_url: str | None,
    current_price: float | None,
) -> int:
    """Score for basic content presence (0-30)."""
    pts = 0
    if title and len(title.strip()) > 0:
        pts += 10
    if image_url and len(image_url.strip()) > 0:
        pts += 10
    if current_price is not None and current_price > 0:
        pts += 10
    return pts


def score_content_completeness(
    has_title: bool = False,
    bullet_count: int = 0,
    has_description: bool = False,
    has_keywords: bool = False,
    has_images: bool = False,
) -> int:
    """Score for rich content completeness from content_ops (0-20).

    - Title present:     4 pts
    - Bullets (up to 5): 2 pts each, max 10
    - Description:       3 pts
    - Keywords/search:   3 pts
    """
    pts = 0
    if has_title:
        pts += 4
    pts += min(max(bullet_count, 0), 5) * 2
    if has_description:
        pts += 3
    if has_keywords:
        pts += 3
    return min(pts, 20)


def compute_health_score(
    listing_status: str | None,
    has_issues: bool,
    issues_severity: str | None,
    is_suppressed: bool,
    title: str | None,
    image_url: str | None,
    current_price: float | None,
    content_completeness_pts: int = 0,
) -> int:
    """Compute overall listing health score (0-100)."""
    return (
        score_listing_status(listing_status)
        + score_issues(has_issues, issues_severity)
        + score_suppression(is_suppressed)
        + score_basic_content(title, image_url, current_price)
        + min(content_completeness_pts, 20)
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Catalog scorecard (aggregate)
# ═══════════════════════════════════════════════════════════════════════════

def get_catalog_scorecard(
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Compute the full catalog health scorecard.

    Returns per-marketplace stats + overall aggregates + score distribution.
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()

        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "WHERE ls.marketplace_id = ?"
            params.append(marketplace_id)

        # ── Aggregate health metrics ────────────────────────────────
        cur.execute(f"""
            SELECT
                COUNT(*)                                                  AS total,
                SUM(CASE WHEN ls.listing_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active,
                SUM(CASE WHEN ls.listing_status = 'INACTIVE' THEN 1 ELSE 0 END) AS inactive,
                SUM(CASE WHEN ls.listing_status = 'SUPPRESSED' THEN 1 ELSE 0 END) AS suppressed_status,
                SUM(CASE WHEN ls.is_suppressed = 1 THEN 1 ELSE 0 END)   AS suppressed_flag,
                SUM(CASE WHEN ls.has_issues = 1 THEN 1 ELSE 0 END)      AS with_issues,
                SUM(CASE WHEN ls.issues_severity = 'ERROR' THEN 1 ELSE 0 END) AS critical,
                SUM(CASE WHEN ls.title IS NOT NULL AND LEN(ls.title) > 0 THEN 1 ELSE 0 END) AS has_title,
                SUM(CASE WHEN ls.image_url IS NOT NULL AND LEN(ls.image_url) > 0 THEN 1 ELSE 0 END) AS has_image,
                SUM(CASE WHEN ls.current_price IS NOT NULL AND ls.current_price > 0 THEN 1 ELSE 0 END) AS has_price,
                SUM(CASE WHEN ls.last_synced_at < DATEADD(hour, -48, SYSUTCDATETIME()) THEN 1 ELSE 0 END) AS stale_48h
            FROM dbo.acc_listing_state ls WITH (NOLOCK)
            {mkt_filter}
        """, tuple(params))
        row = cur.fetchone()
        total = row[0] or 0

        totals = {
            "total_listings": total,
            "active": row[1] or 0,
            "inactive": row[2] or 0,
            "suppressed_by_status": row[3] or 0,
            "suppressed_by_flag": row[4] or 0,
            "with_issues": row[5] or 0,
            "critical_issues": row[6] or 0,
            "has_title": row[7] or 0,
            "has_image": row[8] or 0,
            "has_price": row[9] or 0,
            "stale_48h": row[10] or 0,
        }

        # Rates
        if total > 0:
            totals["active_rate_pct"] = round(totals["active"] / total * 100, 1)
            totals["suppression_rate_pct"] = round(totals["suppressed_by_flag"] / total * 100, 1)
            totals["issue_rate_pct"] = round(totals["with_issues"] / total * 100, 1)
            totals["title_coverage_pct"] = round(totals["has_title"] / total * 100, 1)
            totals["image_coverage_pct"] = round(totals["has_image"] / total * 100, 1)
            totals["price_coverage_pct"] = round(totals["has_price"] / total * 100, 1)
        else:
            totals.update({k: 0.0 for k in [
                "active_rate_pct", "suppression_rate_pct", "issue_rate_pct",
                "title_coverage_pct", "image_coverage_pct", "price_coverage_pct",
            ]})

        # ── Per-marketplace breakdown ───────────────────────────────
        cur.execute(f"""
            SELECT
                ls.marketplace_id,
                COUNT(*) AS total,
                SUM(CASE WHEN ls.listing_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active,
                SUM(CASE WHEN ls.is_suppressed = 1 THEN 1 ELSE 0 END) AS suppressed,
                SUM(CASE WHEN ls.has_issues = 1 THEN 1 ELSE 0 END) AS with_issues,
                SUM(CASE WHEN ls.issues_severity = 'ERROR' THEN 1 ELSE 0 END) AS critical
            FROM dbo.acc_listing_state ls WITH (NOLOCK)
            {mkt_filter}
            GROUP BY ls.marketplace_id
            ORDER BY COUNT(*) DESC
        """, tuple(params))

        by_marketplace = []
        for r in cur.fetchall():
            mkt = r[0]
            mkt_total = r[1] or 1
            by_marketplace.append({
                "marketplace_id": mkt,
                "marketplace_code": MARKETPLACE_REGISTRY.get(mkt, {}).get("code", mkt[-2:] if mkt else ""),
                "total": r[1] or 0,
                "active": r[2] or 0,
                "suppressed": r[3] or 0,
                "with_issues": r[4] or 0,
                "critical": r[5] or 0,
                "health_pct": round((r[2] or 0) / mkt_total * 100, 1),
            })

        # ── Content completeness from content_ops (aggregate) ───────
        content_stats = _get_content_coverage_stats(cur, marketplace_id)

        cur.close()
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": totals,
            "by_marketplace": by_marketplace,
            "content_coverage": content_stats,
        }
    finally:
        conn.close()


def _get_content_coverage_stats(
    cur: Any,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Compute content field coverage from latest content_ops versions."""
    mkt_filter = ""
    params: list[Any] = []
    if marketplace_id:
        mkt_filter = "AND v.marketplace_id = ?"
        params.append(marketplace_id)

    try:
        cur.execute(f"""
            WITH latest AS (
                SELECT sku, marketplace_id, fields_json,
                       ROW_NUMBER() OVER (
                           PARTITION BY sku, marketplace_id
                           ORDER BY version_no DESC
                       ) AS rn
                FROM dbo.acc_co_versions WITH (NOLOCK)
                WHERE status != 'deleted' {mkt_filter}
            )
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN JSON_VALUE(fields_json, '$.title') IS NOT NULL
                          AND LEN(JSON_VALUE(fields_json, '$.title')) > 0
                     THEN 1 ELSE 0 END) AS has_title,
                SUM(CASE WHEN JSON_QUERY(fields_json, '$.bullets') IS NOT NULL
                     THEN 1 ELSE 0 END) AS has_bullets,
                SUM(CASE WHEN JSON_VALUE(fields_json, '$.description') IS NOT NULL
                          AND LEN(JSON_VALUE(fields_json, '$.description')) > 0
                     THEN 1 ELSE 0 END) AS has_description,
                SUM(CASE WHEN JSON_VALUE(fields_json, '$.keywords') IS NOT NULL
                          AND LEN(JSON_VALUE(fields_json, '$.keywords')) > 0
                     THEN 1 ELSE 0 END) AS has_keywords
            FROM latest
            WHERE rn = 1
        """, tuple(params))
        row = cur.fetchone()
        if not row or not row[0]:
            return {"total_versions": 0, "title_pct": 0, "bullets_pct": 0,
                    "description_pct": 0, "keywords_pct": 0}
        total = row[0]
        return {
            "total_versions": total,
            "title_pct": round((row[1] or 0) / total * 100, 1),
            "bullets_pct": round((row[2] or 0) / total * 100, 1),
            "description_pct": round((row[3] or 0) / total * 100, 1),
            "keywords_pct": round((row[4] or 0) / total * 100, 1),
        }
    except Exception as exc:
        log.warning("catalog_health.content_coverage_failed", error=str(exc))
        return {"total_versions": 0, "title_pct": 0, "bullets_pct": 0,
                "description_pct": 0, "keywords_pct": 0}


# ═══════════════════════════════════════════════════════════════════════════
#  Per-listing health detail
# ═══════════════════════════════════════════════════════════════════════════

def get_listing_health_detail(
    seller_sku: str,
    marketplace_id: str,
) -> dict[str, Any] | None:
    """Return a detailed health breakdown for a single listing."""
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Listing state
        cur.execute("""
            SELECT listing_status, has_issues, issues_severity,
                   is_suppressed, suppression_reasons,
                   title, image_url, current_price,
                   issues_count_error, issues_count_warning,
                   last_synced_at, asin, brand
            FROM dbo.acc_listing_state WITH (NOLOCK)
            WHERE seller_sku = ? AND marketplace_id = ?
        """, (seller_sku, marketplace_id))
        ls = cur.fetchone()
        if not ls:
            cur.close()
            return None

        listing_status = ls[0]
        has_issues = bool(ls[1])
        issues_severity = ls[2]
        is_suppressed = bool(ls[3])
        title = ls[5]
        image_url = ls[6]
        current_price = float(ls[7]) if ls[7] is not None else None

        # Content completeness from latest version
        content_pts = _get_content_completeness_for_sku(cur, seller_sku, marketplace_id)

        health_score = compute_health_score(
            listing_status, has_issues, issues_severity, is_suppressed,
            title, image_url, current_price, content_pts,
        )

        cur.close()
        return {
            "seller_sku": seller_sku,
            "marketplace_id": marketplace_id,
            "asin": ls[11],
            "brand": ls[12],
            "health_score": health_score,
            "breakdown": {
                "status_pts": score_listing_status(listing_status),
                "issues_pts": score_issues(has_issues, issues_severity),
                "suppression_pts": score_suppression(is_suppressed),
                "basic_content_pts": score_basic_content(title, image_url, current_price),
                "content_completeness_pts": content_pts,
            },
            "listing_status": listing_status,
            "has_issues": has_issues,
            "issues_severity": issues_severity,
            "issues_count_error": ls[8] or 0,
            "issues_count_warning": ls[9] or 0,
            "is_suppressed": is_suppressed,
            "suppression_reasons": ls[4],
            "title": title,
            "image_url": image_url,
            "current_price": current_price,
            "last_synced_at": ls[10].isoformat() if ls[10] else None,
        }
    finally:
        conn.close()


def _get_content_completeness_for_sku(
    cur: Any,
    seller_sku: str,
    marketplace_id: str,
) -> int:
    """Return content completeness points (0-20) for a SKU's latest version."""
    try:
        cur.execute("""
            SELECT TOP 1 fields_json
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE sku = ? AND marketplace_id = ? AND status != 'deleted'
            ORDER BY version_no DESC
        """, (seller_sku, marketplace_id))
        row = cur.fetchone()
        if not row or not row[0]:
            return 0
        return _score_fields_json(row[0])
    except Exception:
        return 0


def _score_fields_json(fields_json_str: str) -> int:
    """Parse fields_json and compute content completeness points (0-20)."""
    try:
        fields = json.loads(fields_json_str) if isinstance(fields_json_str, str) else fields_json_str
    except (json.JSONDecodeError, TypeError):
        return 0

    if not isinstance(fields, dict):
        return 0

    has_title = bool(fields.get("title"))
    bullets = fields.get("bullets") or []
    bullet_count = len(bullets) if isinstance(bullets, list) else 0
    has_description = bool(fields.get("description"))
    has_keywords = bool(fields.get("keywords"))

    return score_content_completeness(
        has_title=has_title,
        bullet_count=bullet_count,
        has_description=has_description,
        has_keywords=has_keywords,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Suppression timeline & trends (S9.3)
# ═══════════════════════════════════════════════════════════════════════════

def get_suppression_timeline(
    *,
    days: int = 30,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Daily suppression counts over the last N days.

    Uses acc_listing_state_history to track SUPPRESSED transitions and
    acc_listing_state for current snapshot.
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()

        mkt_filter = ""
        params: list[Any] = [days]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        # ── Daily transition counts (into/out of suppression) ────
        cur.execute(f"""
            SELECT
                CAST(changed_at AS DATE) AS change_date,
                SUM(CASE WHEN new_status = 'SUPPRESSED' THEN 1 ELSE 0 END) AS newly_suppressed,
                SUM(CASE WHEN previous_status = 'SUPPRESSED' AND new_status != 'SUPPRESSED' THEN 1 ELSE 0 END) AS recovered
            FROM dbo.acc_listing_state_history WITH (NOLOCK)
            WHERE changed_at >= DATEADD(day, -?, SYSUTCDATETIME())
              {mkt_filter}
            GROUP BY CAST(changed_at AS DATE)
            ORDER BY change_date
        """, tuple(params))
        timeline = []
        for r in cur.fetchall():
            timeline.append({
                "date": r[0].isoformat() if r[0] else None,
                "newly_suppressed": r[1] or 0,
                "recovered": r[2] or 0,
            })

        # ── Current suppression snapshot ─────────────────────────
        mkt_where = ""
        snap_params: list[Any] = []
        if marketplace_id:
            mkt_where = "AND marketplace_id = ?"
            snap_params.append(marketplace_id)

        cur.execute(f"""
            SELECT COUNT(*) AS total_suppressed
            FROM dbo.acc_listing_state WITH (NOLOCK)
            WHERE is_suppressed = 1 {mkt_where}
        """, tuple(snap_params))
        total_suppressed = (cur.fetchone()[0] or 0)

        # ── Top suppression reasons ──────────────────────────────
        cur.execute(f"""
            SELECT TOP 10 suppression_reasons, COUNT(*) AS cnt
            FROM dbo.acc_listing_state WITH (NOLOCK)
            WHERE is_suppressed = 1 AND suppression_reasons IS NOT NULL
              {mkt_where}
            GROUP BY suppression_reasons
            ORDER BY cnt DESC
        """, tuple(snap_params))
        top_reasons = []
        for r in cur.fetchall():
            top_reasons.append({"reason": r[0], "count": r[1] or 0})

        cur.close()
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "days": days,
            "current_suppressed": total_suppressed,
            "timeline": timeline,
            "top_reasons": top_reasons,
        }
    finally:
        conn.close()


def get_suppression_details(
    *,
    marketplace_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return currently-suppressed listings with details."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [limit]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT TOP (?)
                seller_sku, marketplace_id, asin, title, brand,
                suppression_reasons, issues_severity,
                issues_count_error, issues_count_warning,
                last_status_change, last_synced_at
            FROM dbo.acc_listing_state WITH (NOLOCK)
            WHERE is_suppressed = 1 {mkt_filter}
            ORDER BY last_status_change DESC
        """, tuple(params))

        results = []
        for r in cur.fetchall():
            mkt = r[1]
            results.append({
                "seller_sku": r[0],
                "marketplace_id": mkt,
                "marketplace_code": MARKETPLACE_REGISTRY.get(mkt, {}).get("code", mkt[-2:] if mkt else ""),
                "asin": r[2],
                "title": r[3],
                "brand": r[4],
                "suppression_reasons": r[5],
                "issues_severity": r[6],
                "issues_count_error": r[7] or 0,
                "issues_count_warning": r[8] or 0,
                "last_status_change": r[9].isoformat() if r[9] else None,
                "last_synced_at": r[10].isoformat() if r[10] else None,
            })
        cur.close()
        return results
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Listing field-diff detector (S9.2)
# ═══════════════════════════════════════════════════════════════════════════

# Fields tracked for diffs (field_name → column name in acc_listing_state)
_DIFF_FIELDS: dict[str, str] = {
    "title": "title",
    "listing_status": "listing_status",
    "current_price": "current_price",
    "image_url": "image_url",
    "brand": "brand",
    "is_suppressed": "is_suppressed",
    "has_issues": "has_issues",
    "issues_severity": "issues_severity",
    "fulfillment_channel": "fulfillment_channel",
}


def detect_and_record_diffs(
    cur: Any,
    seller_sku: str,
    marketplace_id: str,
    old_values: dict[str, Any],
    new_values: dict[str, Any],
    *,
    change_source: str = "unknown",
) -> int:
    """Compare old vs new listing values and record diffs.

    Called from upsert_listing_state after reading existing row.
    Returns the number of diffs recorded.
    """
    diffs_recorded = 0
    for field_label, _col in _DIFF_FIELDS.items():
        old_val = old_values.get(field_label)
        new_val = new_values.get(field_label)
        if new_val is None:
            continue  # not provided → no change
        old_str = _to_str(old_val)
        new_str = _to_str(new_val)
        if old_str == new_str:
            continue
        try:
            cur.execute("""
                INSERT INTO dbo.acc_listing_field_diff
                    (seller_sku, marketplace_id, field_name,
                     old_value, new_value, change_source, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
            """, (seller_sku, marketplace_id, field_label,
                  _truncate(old_str, 500), _truncate(new_str, 500), change_source))
            diffs_recorded += 1
        except Exception as exc:
            log.warning("catalog_health.diff_insert_failed",
                        sku=seller_sku, field=field_label, error=str(exc))
    return diffs_recorded


def _to_str(v: Any) -> str:
    """Normalise a value to string for comparison."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    return str(v).strip()


def _truncate(s: str, maxlen: int) -> str | None:
    if not s:
        return None
    return s[:maxlen]


def get_recent_diffs(
    *,
    seller_sku: str | None = None,
    marketplace_id: str | None = None,
    field_name: str | None = None,
    days: int = 7,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return recent field-level diffs, optionally filtered."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        clauses = ["detected_at >= DATEADD(day, -?, SYSUTCDATETIME())"]
        params: list[Any] = [days]

        if seller_sku:
            clauses.append("seller_sku = ?")
            params.append(seller_sku)
        if marketplace_id:
            clauses.append("marketplace_id = ?")
            params.append(marketplace_id)
        if field_name:
            clauses.append("field_name = ?")
            params.append(field_name)

        where = " AND ".join(clauses)
        params.insert(0, limit)  # TOP (?)

        cur.execute(f"""
            SELECT TOP (?)
                id, seller_sku, marketplace_id, field_name,
                old_value, new_value, change_source, detected_at
            FROM dbo.acc_listing_field_diff WITH (NOLOCK)
            WHERE {where}
            ORDER BY detected_at DESC
        """, tuple(params))

        results = []
        for r in cur.fetchall():
            results.append({
                "id": r[0],
                "seller_sku": r[1],
                "marketplace_id": r[2],
                "field_name": r[3],
                "old_value": r[4],
                "new_value": r[5],
                "change_source": r[6],
                "detected_at": r[7].isoformat() if r[7] else None,
            })
        cur.close()
        return results
    finally:
        conn.close()


def get_diff_summary(
    *,
    days: int = 7,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Aggregate field-diff stats over a time window."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [days]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT field_name, COUNT(*) AS cnt
            FROM dbo.acc_listing_field_diff WITH (NOLOCK)
            WHERE detected_at >= DATEADD(day, -?, SYSUTCDATETIME())
              {mkt_filter}
            GROUP BY field_name
            ORDER BY cnt DESC
        """, tuple(params))

        by_field = {}
        total = 0
        for r in cur.fetchall():
            by_field[r[0]] = r[1] or 0
            total += r[1] or 0

        cur.close()
        return {
            "days": days,
            "total_changes": total,
            "by_field": by_field,
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Health snapshot persistence (S10.4)
# ═══════════════════════════════════════════════════════════════════════════

def compute_and_persist_health_snapshots(
    *,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Compute health score for every listing and MERGE into snapshot table.

    Designed to run once per day (scheduler job).  Uses today's date as
    snapshot_date and MERGE to be idempotent on re-runs.
    """
    conn = connect_acc(timeout=300)
    try:
        cur = conn.cursor()

        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "WHERE ls.marketplace_id = ?"
            params.append(marketplace_id)

        # Fetch all listings with fields needed for scoring
        cur.execute(f"""
            SELECT ls.seller_sku, ls.marketplace_id,
                   ls.listing_status, ls.has_issues, ls.issues_severity,
                   ls.is_suppressed, ls.title, ls.image_url, ls.current_price
            FROM dbo.acc_listing_state ls WITH (NOLOCK)
            {mkt_filter}
        """, tuple(params))

        rows = cur.fetchall()
        upserted = 0

        for r in rows:
            sku, mkt = r[0], r[1]
            listing_status = r[2]
            has_issues = bool(r[3])
            issues_severity = r[4]
            is_suppressed = bool(r[5])
            title = r[6]
            image_url = r[7]
            current_price = float(r[8]) if r[8] is not None else None

            s_pts = score_listing_status(listing_status)
            i_pts = score_issues(has_issues, issues_severity)
            sup_pts = score_suppression(is_suppressed)
            bc_pts = score_basic_content(title, image_url, current_price)
            cc_pts = _get_content_completeness_for_sku(cur, sku, mkt)
            total_score = s_pts + i_pts + sup_pts + bc_pts + min(cc_pts, 20)

            cur.execute("""
                MERGE dbo.acc_listing_health_snapshot AS tgt
                USING (SELECT ? AS sku, ? AS mkt, CAST(SYSUTCDATETIME() AS DATE) AS sd) AS src
                ON tgt.seller_sku = src.sku
                   AND tgt.marketplace_id = src.mkt
                   AND tgt.snapshot_date = src.sd
                WHEN MATCHED THEN UPDATE SET
                    health_score = ?, status_pts = ?, issues_pts = ?,
                    suppression_pts = ?, basic_content_pts = ?,
                    content_completeness_pts = ?,
                    listing_status = ?, is_suppressed = ?, has_issues = ?,
                    computed_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN INSERT (
                    seller_sku, marketplace_id, snapshot_date,
                    health_score, status_pts, issues_pts,
                    suppression_pts, basic_content_pts,
                    content_completeness_pts,
                    listing_status, is_suppressed, has_issues
                ) VALUES (
                    src.sku, src.mkt, src.sd,
                    ?, ?, ?,
                    ?, ?,
                    ?,
                    ?, ?, ?
                );
            """, (
                sku, mkt,
                # WHEN MATCHED
                total_score, s_pts, i_pts, sup_pts, bc_pts, cc_pts,
                listing_status, 1 if is_suppressed else 0, 1 if has_issues else 0,
                # WHEN NOT MATCHED
                total_score, s_pts, i_pts, sup_pts, bc_pts, cc_pts,
                listing_status, 1 if is_suppressed else 0, 1 if has_issues else 0,
            ))
            upserted += 1

        conn.commit()
        cur.close()
        log.info("catalog_health.snapshots_persisted", count=upserted)
        return {"upserted": upserted}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Worst performers (S10.3)
# ═══════════════════════════════════════════════════════════════════════════

def get_worst_performers(
    *,
    marketplace_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return bottom-N listings by health score from latest snapshot."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [limit]
        if marketplace_id:
            mkt_filter = "AND s.marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            WITH latest AS (
                SELECT MAX(snapshot_date) AS sd
                FROM dbo.acc_listing_health_snapshot WITH (NOLOCK)
            )
            SELECT TOP (?)
                s.seller_sku, s.marketplace_id, s.health_score,
                s.status_pts, s.issues_pts, s.suppression_pts,
                s.basic_content_pts, s.content_completeness_pts,
                s.listing_status, s.is_suppressed, s.has_issues,
                ls.asin, ls.title, ls.brand, ls.suppression_reasons
            FROM dbo.acc_listing_health_snapshot s WITH (NOLOCK)
            CROSS JOIN latest l
            LEFT JOIN dbo.acc_listing_state ls WITH (NOLOCK)
                ON ls.seller_sku = s.seller_sku AND ls.marketplace_id = s.marketplace_id
            WHERE s.snapshot_date = l.sd {mkt_filter}
            ORDER BY s.health_score ASC, s.seller_sku
        """, tuple(params))

        results = []
        for r in cur.fetchall():
            mkt = r[1]
            results.append({
                "seller_sku": r[0],
                "marketplace_id": mkt,
                "marketplace_code": MARKETPLACE_REGISTRY.get(mkt, {}).get("code", mkt[-2:] if mkt else ""),
                "health_score": r[2],
                "breakdown": {
                    "status_pts": r[3],
                    "issues_pts": r[4],
                    "suppression_pts": r[5],
                    "basic_content_pts": r[6],
                    "content_completeness_pts": r[7],
                },
                "listing_status": r[8],
                "is_suppressed": bool(r[9]),
                "has_issues": bool(r[10]),
                "asin": r[11],
                "title": r[12],
                "brand": r[13],
                "suppression_reasons": r[14],
            })
        cur.close()
        return results
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Health score trends over time (S10.4)
# ═══════════════════════════════════════════════════════════════════════════

def get_health_trends(
    *,
    days: int = 30,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Aggregate daily health score averages from snapshot history."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [days]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT
                snapshot_date,
                COUNT(*) AS listing_count,
                AVG(CAST(health_score AS FLOAT)) AS avg_score,
                MIN(health_score) AS min_score,
                MAX(health_score) AS max_score,
                SUM(CASE WHEN health_score >= 80 THEN 1 ELSE 0 END) AS healthy,
                SUM(CASE WHEN health_score >= 50 AND health_score < 80 THEN 1 ELSE 0 END) AS needs_attention,
                SUM(CASE WHEN health_score < 50 THEN 1 ELSE 0 END) AS critical
            FROM dbo.acc_listing_health_snapshot WITH (NOLOCK)
            WHERE snapshot_date >= DATEADD(day, -?, CAST(SYSUTCDATETIME() AS DATE))
              {mkt_filter}
            GROUP BY snapshot_date
            ORDER BY snapshot_date
        """, tuple(params))

        timeline = []
        for r in cur.fetchall():
            timeline.append({
                "date": r[0].isoformat() if r[0] else None,
                "listing_count": r[1] or 0,
                "avg_score": round(r[2], 1) if r[2] else 0,
                "min_score": r[3] or 0,
                "max_score": r[4] or 0,
                "healthy": r[5] or 0,
                "needs_attention": r[6] or 0,
                "critical": r[7] or 0,
            })
        cur.close()
        return {
            "days": days,
            "timeline": timeline,
        }
    finally:
        conn.close()
