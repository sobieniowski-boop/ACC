"""Content Optimization Engine — Sprint 18: Multi-language generation & A/B testing.

Multi-language generation:
  - Orchestrates ai_generate(mode="localize") per target market
  - Language-quality validation (Polish leak detection, target-language checks)
  - Tracks generation jobs in acc_multilang_job

A/B Content Testing:
  - Experiment lifecycle: draft → running → concluded
  - Variant management (control + challengers)
  - Metric tracking (impressions, clicks, orders, revenue, conversion_rate)
  - Winner declaration based on primary metric

Tables:
  acc_multilang_job        — Multi-language generation job tracking
  acc_content_experiment   — A/B test experiments
  acc_content_variant      — Experiment variant metrics
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any

import structlog

from app.connectors.mssql import connect_acc

log = structlog.get_logger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

VALID_EXPERIMENT_STATUSES = {"draft", "running", "paused", "concluded", "cancelled"}
VALID_METRICS = {"conversion_rate", "ctr", "revenue", "orders"}
VALID_MULTILANG_STATUSES = {"pending", "generating", "completed", "failed", "review"}

LANGUAGE_NAMES: dict[str, str] = {
    "de_DE": "German",
    "fr_FR": "French",
    "it_IT": "Italian",
    "es_ES": "Spanish",
    "nl_NL": "Dutch",
    "pl_PL": "Polish",
    "sv_SE": "Swedish",
    "nl_BE": "Dutch (Belgium)",
    "en_GB": "English",
}

# Polish leak patterns (re-exported for tests)
_POLISH_LEAK_WORDS = [
    r"\bwysoko[śs]ci?\b", r"\bszeroko[śs]ci?\b", r"\bg[łl][ęe]boko[śs]ci?\b",
    r"\bopakowanie\b", r"\bsztuk[ai]?\b", r"\bkolor\b", r"\bmateria[łl]\b",
    r"\bwaga\b", r"\brozmiar\b", r"\bzestaw\b", r"\bbezp[łl]atna\b",
]


# ═══════════════════════════════════════════════════════════════════════════
#  Schema DDL
# ═══════════════════════════════════════════════════════════════════════════

_MULTILANG_AB_SCHEMA: list[str] = [
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_multilang_job')
    CREATE TABLE dbo.acc_multilang_job (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(60)  NOT NULL,
        asin            NVARCHAR(20)  NULL,
        source_marketplace_id NVARCHAR(20) NOT NULL,
        target_marketplace_id NVARCHAR(20) NOT NULL,
        target_language  NVARCHAR(10)  NOT NULL,
        status           NVARCHAR(20)  NOT NULL DEFAULT 'pending',
        source_version_id NVARCHAR(60) NULL,
        target_version_id NVARCHAR(60) NULL,
        model            NVARCHAR(40)  NULL,
        quality_score    INT           NULL,
        quality_issues_json NVARCHAR(MAX) NULL,
        policy_flags_json   NVARCHAR(MAX) NULL,
        error_message    NVARCHAR(500) NULL,
        created_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        completed_at     DATETIME2     NULL,
        CONSTRAINT uq_multilang_job UNIQUE (seller_sku, source_marketplace_id, target_marketplace_id)
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_experiment')
    CREATE TABLE dbo.acc_content_experiment (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        name            NVARCHAR(200) NOT NULL,
        seller_sku      NVARCHAR(60)  NOT NULL,
        marketplace_id  NVARCHAR(20)  NOT NULL,
        status          NVARCHAR(20)  NOT NULL DEFAULT 'draft',
        hypothesis      NVARCHAR(500) NULL,
        metric_primary  NVARCHAR(40)  NOT NULL DEFAULT 'conversion_rate',
        start_date      DATE          NULL,
        end_date        DATE          NULL,
        winner_variant_id INT         NULL,
        created_by      NVARCHAR(60)  NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        concluded_at    DATETIME2     NULL
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_variant')
    CREATE TABLE dbo.acc_content_variant (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        experiment_id   INT           NOT NULL,
        label           NVARCHAR(10)  NOT NULL DEFAULT 'A',
        version_id      NVARCHAR(60)  NULL,
        is_control      BIT           NOT NULL DEFAULT 0,
        impressions     INT           NOT NULL DEFAULT 0,
        clicks          INT           NOT NULL DEFAULT 0,
        orders          INT           NOT NULL DEFAULT 0,
        revenue         DECIMAL(12,2) NOT NULL DEFAULT 0,
        conversion_rate DECIMAL(6,3)  NULL,
        ctr             DECIMAL(6,3)  NULL,
        content_score   INT           NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_variant_exp_label UNIQUE (experiment_id, label)
    );
    """,
]


def ensure_multilang_ab_schema() -> None:
    """Create tables if needed (idempotent)."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        for ddl in _MULTILANG_AB_SCHEMA:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Multi-language generation
# ═══════════════════════════════════════════════════════════════════════════

def _language_tag_for_market(market_code: str) -> str:
    """Map market code → language tag."""
    mapping = {
        "DE": "de_DE", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES",
        "NL": "nl_NL", "PL": "pl_PL", "SE": "sv_SE", "BE": "nl_BE",
    }
    return mapping.get(market_code.upper(), "en_GB")


def validate_language_quality(
    fields: dict[str, Any],
    target_language: str,
) -> tuple[int, list[str]]:
    """Validate generated content for language quality.

    Returns (quality_score 0-100, issues list).
    """
    issues: list[str] = []
    score = 100

    title = fields.get("title", "") or ""
    bullets = fields.get("bullets", []) or []
    description = fields.get("description", "") or ""

    # Check for content presence
    if not title.strip():
        issues.append("missing_title")
        score -= 30
    if not bullets:
        issues.append("missing_bullets")
        score -= 20
    if not description.strip():
        issues.append("missing_description")
        score -= 10

    # Polish leak detection (for non-Polish targets)
    if target_language != "pl_PL":
        all_text = f"{title} {description} {' '.join(str(b) for b in bullets)}".lower()
        for pattern in _POLISH_LEAK_WORDS:
            if re.search(pattern, all_text, re.IGNORECASE):
                issues.append(f"polish_leak:{pattern}")
                score -= 5

    # Source language leak: check for English leaking into non-English targets
    if target_language not in ("en_GB", "en_US"):
        english_markers = [r"\bthe\b", r"\band\b", r"\bwith\b", r"\bfor\b", r"\byour\b"]
        title_lower = title.lower()
        eng_hits = sum(1 for p in english_markers if re.search(p, title_lower))
        if eng_hits >= 3:
            issues.append("english_leak_in_title")
            score -= 15

    # Structural checks
    if len(title) < 30:
        issues.append("title_too_short")
        score -= 10
    if len(bullets) < 3:
        issues.append("too_few_bullets")
        score -= 10

    return max(0, min(100, score)), issues


def generate_multilang_content(
    *,
    seller_sku: str,
    source_marketplace_id: str,
    target_marketplace_id: str,
    target_language: str,
    asin: str | None = None,
) -> dict[str, Any]:
    """Generate localized content for a target marketplace.

    Uses existing ai_generate(mode="localize") and validates quality.
    Tracks job in acc_multilang_job.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        # Upsert job record as 'generating'
        cur.execute("""
            MERGE dbo.acc_multilang_job AS tgt
            USING (SELECT %s AS seller_sku, %s AS source_marketplace_id,
                          %s AS target_marketplace_id) AS src
            ON tgt.seller_sku = src.seller_sku
               AND tgt.source_marketplace_id = src.source_marketplace_id
               AND tgt.target_marketplace_id = src.target_marketplace_id
            WHEN MATCHED THEN
                UPDATE SET status = 'generating', error_message = NULL,
                           completed_at = NULL, created_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (seller_sku, asin, source_marketplace_id, target_marketplace_id,
                        target_language, status)
                VALUES (%s, %s, %s, %s, %s, 'generating');
        """, (
            seller_sku, source_marketplace_id, target_marketplace_id,
            seller_sku, asin, source_marketplace_id, target_marketplace_id,
            target_language,
        ))
        conn.commit()

        # Call AI generation
        try:
            from app.services.content_ops.compliance import ai_generate
            result = ai_generate(payload={
                "sku": seller_sku,
                "marketplace_id": target_marketplace_id,
                "mode": "localize",
                "source_market": source_marketplace_id,
            })
        except Exception as exc:
            # Mark job as failed
            cur.execute("""
                UPDATE dbo.acc_multilang_job
                SET status = 'failed', error_message = %s, completed_at = SYSUTCDATETIME()
                WHERE seller_sku = %s AND source_marketplace_id = %s
                      AND target_marketplace_id = %s
            """, (str(exc)[:500], seller_sku, source_marketplace_id, target_marketplace_id))
            conn.commit()
            return {
                "seller_sku": seller_sku,
                "target_marketplace_id": target_marketplace_id,
                "status": "failed",
                "error": str(exc)[:500],
            }

        output = result.get("output", {})
        policy_flags = result.get("policy_flags", [])
        model = result.get("model", "")

        # Validate language quality
        quality_score, quality_issues = validate_language_quality(output, target_language)

        # Update job with results
        status = "completed" if quality_score >= 50 else "review"
        cur.execute("""
            UPDATE dbo.acc_multilang_job
            SET status = %s, model = %s, quality_score = %s,
                quality_issues_json = %s, policy_flags_json = %s,
                completed_at = SYSUTCDATETIME()
            WHERE seller_sku = %s AND source_marketplace_id = %s
                  AND target_marketplace_id = %s
        """, (
            status, model, quality_score,
            json.dumps(quality_issues), json.dumps(policy_flags),
            seller_sku, source_marketplace_id, target_marketplace_id,
        ))
        conn.commit()

        log.info("multilang.generated",
                 sku=seller_sku, target=target_marketplace_id,
                 quality=quality_score, status=status)

        return {
            "seller_sku": seller_sku,
            "source_marketplace_id": source_marketplace_id,
            "target_marketplace_id": target_marketplace_id,
            "target_language": target_language,
            "status": status,
            "quality_score": quality_score,
            "quality_issues": quality_issues,
            "policy_flags": policy_flags,
            "output": output,
            "model": model,
            "cache_hit": result.get("cache_hit", False),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def generate_all_languages(
    *,
    seller_sku: str,
    source_marketplace_id: str,
    asin: str | None = None,
    target_markets: list[str] | None = None,
) -> dict[str, Any]:
    """Generate content for all target markets from a source listing.

    Returns summary with per-market results.
    """
    from app.services.content_ops._helpers import (
        _DEFAULT_CONTENT_MARKETS,
        _marketplace_to_id,
        _marketplace_to_code,
    )

    source_code = _marketplace_to_code(source_marketplace_id)
    markets = target_markets or list(_DEFAULT_CONTENT_MARKETS)
    # Exclude source market
    markets = [m for m in markets if m.upper() != source_code.upper()]

    results: list[dict] = []
    completed = 0
    failed = 0

    for mkt_code in markets:
        mkt_id = _marketplace_to_id(mkt_code)
        if not mkt_id:
            continue
        lang = _language_tag_for_market(mkt_code)
        try:
            r = generate_multilang_content(
                seller_sku=seller_sku,
                source_marketplace_id=source_marketplace_id,
                target_marketplace_id=mkt_id,
                target_language=lang,
                asin=asin,
            )
            results.append(r)
            if r["status"] in ("completed", "review"):
                completed += 1
            else:
                failed += 1
        except Exception as exc:
            log.error("multilang.market_error", sku=seller_sku, market=mkt_code, error=str(exc))
            results.append({
                "seller_sku": seller_sku,
                "target_marketplace_id": mkt_id,
                "status": "failed",
                "error": str(exc)[:200],
            })
            failed += 1

    return {
        "seller_sku": seller_sku,
        "source_marketplace_id": source_marketplace_id,
        "markets_attempted": len(results),
        "completed": completed,
        "failed": failed,
        "results": results,
    }


def get_multilang_jobs(
    seller_sku: str | None = None,
    *,
    source_marketplace_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Query multi-language generation jobs."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []

        if seller_sku:
            where.append("seller_sku = %s")
            params.append(seller_sku)
        if source_marketplace_id:
            where.append("source_marketplace_id = %s")
            params.append(source_marketplace_id)
        if status:
            where.append("status = %s")
            params.append(status)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_multilang_job WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT id, seller_sku, asin, source_marketplace_id, target_marketplace_id,
                   target_language, status, source_version_id, target_version_id,
                   model, quality_score, quality_issues_json, policy_flags_json,
                   error_message, created_at, completed_at
            FROM dbo.acc_multilang_job WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY created_at DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, params + [offset, limit])

        items = [_multilang_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_multilang_coverage(seller_sku: str, source_marketplace_id: str) -> dict[str, Any]:
    """Get language coverage status for a SKU across all markets."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT target_marketplace_id, target_language, status,
                   quality_score, completed_at
            FROM dbo.acc_multilang_job WITH (NOLOCK)
            WHERE seller_sku = %s AND source_marketplace_id = %s
            ORDER BY target_marketplace_id
        """, (seller_sku, source_marketplace_id))

        markets: list[dict] = []
        for r in cur.fetchall():
            markets.append({
                "target_marketplace_id": r[0],
                "target_language": r[1],
                "language_name": LANGUAGE_NAMES.get(r[1], r[1]),
                "status": r[2],
                "quality_score": r[3],
                "completed_at": str(r[4]) if r[4] else None,
            })

        return {
            "seller_sku": seller_sku,
            "source_marketplace_id": source_marketplace_id,
            "markets_covered": len([m for m in markets if m["status"] in ("completed", "review")]),
            "markets_total": len(markets),
            "markets": markets,
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  A/B Content Testing
# ═══════════════════════════════════════════════════════════════════════════

def create_experiment(
    *,
    name: str,
    seller_sku: str,
    marketplace_id: str,
    hypothesis: str | None = None,
    metric_primary: str = "conversion_rate",
    created_by: str | None = None,
) -> dict[str, Any]:
    """Create a new A/B content experiment."""
    if metric_primary not in VALID_METRICS:
        raise ValueError(f"Invalid metric: {metric_primary}. Must be one of {VALID_METRICS}")

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dbo.acc_content_experiment
                (name, seller_sku, marketplace_id, hypothesis, metric_primary, created_by)
            OUTPUT INSERTED.id
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (name, seller_sku, marketplace_id, hypothesis, metric_primary, created_by))
        row = cur.fetchone()
        experiment_id = row[0]
        conn.commit()

        log.info("experiment.created", id=experiment_id, sku=seller_sku)
        return {
            "id": experiment_id,
            "name": name,
            "seller_sku": seller_sku,
            "marketplace_id": marketplace_id,
            "status": "draft",
            "hypothesis": hypothesis,
            "metric_primary": metric_primary,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def add_variant(
    *,
    experiment_id: int,
    label: str,
    version_id: str | None = None,
    is_control: bool = False,
    content_score: int | None = None,
) -> dict[str, Any]:
    """Add a variant to an experiment."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        # Verify experiment exists and is in draft status
        cur.execute("""
            SELECT status FROM dbo.acc_content_experiment WITH (NOLOCK) WHERE id = %s
        """, (experiment_id,))
        exp_row = cur.fetchone()
        if not exp_row:
            raise ValueError(f"Experiment {experiment_id} not found")
        if exp_row[0] not in ("draft", "running"):
            raise ValueError(f"Cannot add variants to experiment in '{exp_row[0]}' status")

        cur.execute("""
            INSERT INTO dbo.acc_content_variant
                (experiment_id, label, version_id, is_control, content_score)
            OUTPUT INSERTED.id
            VALUES (%s, %s, %s, %s, %s)
        """, (experiment_id, label, version_id, 1 if is_control else 0, content_score))
        row = cur.fetchone()
        variant_id = row[0]
        conn.commit()

        return {
            "id": variant_id,
            "experiment_id": experiment_id,
            "label": label,
            "version_id": version_id,
            "is_control": is_control,
            "content_score": content_score,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def start_experiment(experiment_id: int) -> dict[str, Any]:
    """Move experiment from draft → running."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        cur.execute("SELECT status FROM dbo.acc_content_experiment WHERE id = %s", (experiment_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Experiment {experiment_id} not found")
        if row[0] != "draft":
            raise ValueError(f"Can only start experiments in 'draft' status, current: '{row[0]}'")

        # Must have at least 2 variants
        cur.execute("SELECT COUNT(*) FROM dbo.acc_content_variant WHERE experiment_id = %s", (experiment_id,))
        variant_count = cur.fetchone()[0] or 0
        if variant_count < 2:
            raise ValueError(f"Need at least 2 variants, have {variant_count}")

        cur.execute("""
            UPDATE dbo.acc_content_experiment
            SET status = 'running', start_date = CAST(SYSUTCDATETIME() AS DATE)
            WHERE id = %s
        """, (experiment_id,))
        conn.commit()

        log.info("experiment.started", id=experiment_id)
        return {"id": experiment_id, "status": "running"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def record_variant_metrics(
    *,
    variant_id: int,
    impressions: int | None = None,
    clicks: int | None = None,
    orders: int | None = None,
    revenue: float | None = None,
) -> dict[str, Any]:
    """Update variant performance metrics."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        sets: list[str] = []
        params: list[Any] = []

        if impressions is not None:
            sets.append("impressions = %s")
            params.append(impressions)
        if clicks is not None:
            sets.append("clicks = %s")
            params.append(clicks)
        if orders is not None:
            sets.append("orders = %s")
            params.append(orders)
        if revenue is not None:
            sets.append("revenue = %s")
            params.append(revenue)

        if not sets:
            return {"id": variant_id, "updated": False}

        # Also compute derived metrics
        sets.append("conversion_rate = CASE WHEN impressions > 0 THEN CAST(orders AS DECIMAL(12,3)) / impressions * 100 ELSE NULL END")
        sets.append("ctr = CASE WHEN impressions > 0 THEN CAST(clicks AS DECIMAL(12,3)) / impressions * 100 ELSE NULL END")

        params.append(variant_id)
        cur.execute(
            f"UPDATE dbo.acc_content_variant SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        conn.commit()
        return {"id": variant_id, "updated": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def conclude_experiment(experiment_id: int) -> dict[str, Any]:
    """Conclude experiment and determine winner based on primary metric."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        # Fetch experiment
        cur.execute("""
            SELECT status, metric_primary
            FROM dbo.acc_content_experiment WHERE id = %s
        """, (experiment_id,))
        exp = cur.fetchone()
        if not exp:
            raise ValueError(f"Experiment {experiment_id} not found")
        if exp[0] != "running":
            raise ValueError(f"Can only conclude 'running' experiments, current: '{exp[0]}'")

        metric = exp[1] or "conversion_rate"

        # Fetch variants with metrics
        cur.execute("""
            SELECT id, label, is_control, impressions, clicks, orders,
                   revenue, conversion_rate, ctr, content_score
            FROM dbo.acc_content_variant
            WHERE experiment_id = %s
            ORDER BY label
        """, (experiment_id,))
        variants = cur.fetchall()

        if not variants:
            raise ValueError("No variants found")

        # Determine winner based on primary metric
        metric_col_index = {"conversion_rate": 7, "ctr": 8, "revenue": 6, "orders": 5}
        col_idx = metric_col_index.get(metric, 7)

        best_id = None
        best_val = -1.0
        for v in variants:
            val = float(v[col_idx] or 0)
            if val > best_val:
                best_val = val
                best_id = v[0]

        # Update experiment
        cur.execute("""
            UPDATE dbo.acc_content_experiment
            SET status = 'concluded', winner_variant_id = %s,
                end_date = CAST(SYSUTCDATETIME() AS DATE),
                concluded_at = SYSUTCDATETIME()
            WHERE id = %s
        """, (best_id, experiment_id))
        conn.commit()

        log.info("experiment.concluded", id=experiment_id, winner_variant_id=best_id)
        return {
            "id": experiment_id,
            "status": "concluded",
            "winner_variant_id": best_id,
            "metric": metric,
            "winning_value": best_val,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_experiment(experiment_id: int) -> dict | None:
    """Get experiment with its variants."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, seller_sku, marketplace_id, status, hypothesis,
                   metric_primary, start_date, end_date, winner_variant_id,
                   created_by, created_at, concluded_at
            FROM dbo.acc_content_experiment WITH (NOLOCK)
            WHERE id = %s
        """, (experiment_id,))
        row = cur.fetchone()
        if not row:
            return None

        exp = _experiment_row_to_dict(row)

        cur.execute("""
            SELECT id, experiment_id, label, version_id, is_control,
                   impressions, clicks, orders, revenue,
                   conversion_rate, ctr, content_score, created_at
            FROM dbo.acc_content_variant WITH (NOLOCK)
            WHERE experiment_id = %s
            ORDER BY label
        """, (experiment_id,))
        exp["variants"] = [_variant_row_to_dict(r) for r in cur.fetchall()]
        return exp
    finally:
        conn.close()


def list_experiments(
    marketplace_id: str | None = None,
    *,
    seller_sku: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List experiments with pagination and filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []

        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)
        if seller_sku:
            where.append("seller_sku = %s")
            params.append(seller_sku)
        if status:
            where.append("status = %s")
            params.append(status)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_content_experiment WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT id, name, seller_sku, marketplace_id, status, hypothesis,
                   metric_primary, start_date, end_date, winner_variant_id,
                   created_by, created_at, concluded_at
            FROM dbo.acc_content_experiment WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY created_at DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, params + [offset, limit])

        items = [_experiment_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_experiment_summary(marketplace_id: str | None = None) -> dict[str, Any]:
    """Dashboard summary of experiment activity."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "WHERE marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(CASE WHEN status = 'draft' THEN 1 END) AS draft,
                COUNT(CASE WHEN status = 'running' THEN 1 END) AS running,
                COUNT(CASE WHEN status = 'concluded' THEN 1 END) AS concluded,
                COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled
            FROM dbo.acc_content_experiment WITH (NOLOCK)
            {mkt_filter}
        """, params)
        row = cur.fetchone()

        return {
            "total": row[0] or 0,
            "draft": row[1] or 0,
            "running": row[2] or 0,
            "concluded": row[3] or 0,
            "cancelled": row[4] or 0,
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _safe_json(val: Any) -> Any:
    if val is None:
        return []
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


def _multilang_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "seller_sku": row[1],
        "asin": row[2],
        "source_marketplace_id": row[3],
        "target_marketplace_id": row[4],
        "target_language": row[5],
        "language_name": LANGUAGE_NAMES.get(row[5], row[5]),
        "status": row[6],
        "source_version_id": row[7],
        "target_version_id": row[8],
        "model": row[9],
        "quality_score": row[10],
        "quality_issues": _safe_json(row[11]),
        "policy_flags": _safe_json(row[12]),
        "error_message": row[13],
        "created_at": str(row[14]) if row[14] else None,
        "completed_at": str(row[15]) if row[15] else None,
    }


def _experiment_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "name": row[1],
        "seller_sku": row[2],
        "marketplace_id": row[3],
        "status": row[4],
        "hypothesis": row[5],
        "metric_primary": row[6],
        "start_date": str(row[7]) if row[7] else None,
        "end_date": str(row[8]) if row[8] else None,
        "winner_variant_id": row[9],
        "created_by": row[10],
        "created_at": str(row[11]) if row[11] else None,
        "concluded_at": str(row[12]) if row[12] else None,
    }


def _variant_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "experiment_id": row[1],
        "label": row[2],
        "version_id": row[3],
        "is_control": bool(row[4]),
        "impressions": row[5],
        "clicks": row[6],
        "orders": row[7],
        "revenue": float(row[8]) if row[8] else 0,
        "conversion_rate": float(row[9]) if row[9] else None,
        "ctr": float(row[10]) if row[10] else None,
        "content_score": row[11],
        "created_at": str(row[12]) if row[12] else None,
    }
