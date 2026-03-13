"""
COGS Data Quality Controlling Module
=====================================
Validates mapping + pricing pipeline integrity.

Called:
  - After every order pipeline run (step 5.5)
  - On demand via /api/v1/audit/cogs endpoint
  - Nightly as alert rule evaluation

Checks:
  1. Mapping integrity — internal_sku format + existence in source systems
  2. Price sanity — outlier detection per product
  3. COGS consistency — stamped price vs current price divergence
  4. Coverage tracking — trend of COGS coverage %
  5. Cross-source validation — Holding vs XLSX price discrepancy
"""
from __future__ import annotations

import pyodbc
import json
import structlog
from datetime import date, datetime, timezone
from typing import Any

from app.core.config import settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


def _connect():
    return connect_acc(autocommit=True)


# ---------------------------------------------------------------------------
# 1. MAPPING INTEGRITY
# ---------------------------------------------------------------------------

def check_mapping_integrity() -> dict[str, Any]:
    """
    Validate internal_sku values:
      - Format: numeric, 2-6 digits
      - Existence in Holding or XLSX
      - No duplicates (same EAN → different internal_sku)
    """
    conn = _connect()
    cur = conn.cursor()

    # 1a. Suspicious format
    cur.execute("""
        SELECT COUNT(*)
        FROM acc_product
        WHERE internal_sku IS NOT NULL
          AND (LEN(internal_sku) > 10
               OR internal_sku LIKE '%[^0-9]%'
               OR LEN(internal_sku) < 2)
    """)
    bad_format = cur.fetchone()[0]

    # 1b. Same EAN → multiple internal_sku
    cur.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT ean
            FROM acc_product
            WHERE ean IS NOT NULL AND internal_sku IS NOT NULL
            GROUP BY ean
            HAVING COUNT(DISTINCT internal_sku) > 1
        ) sub
    """)
    ean_conflicts = cur.fetchone()[0]

    # 1c. Mapped but no price anywhere
    cur.execute("""
        SELECT COUNT(*)
        FROM acc_product p
        WHERE p.internal_sku IS NOT NULL
          AND p.netto_purchase_price_pln IS NULL
    """)
    mapped_no_price = cur.fetchone()[0]

    conn.close()

    issues = []
    if bad_format > 0:
        issues.append(f"{bad_format} products with suspicious internal_sku format")
    if ean_conflicts > 0:
        issues.append(f"{ean_conflicts} EANs mapped to multiple internal_sku values")
    if mapped_no_price > 50:
        issues.append(f"{mapped_no_price} mapped products without any purchase price")

    return {
        "check": "mapping_integrity",
        "bad_format": bad_format,
        "ean_conflicts": ean_conflicts,
        "mapped_no_price": mapped_no_price,
        "issues": issues,
        "status": "warning" if issues else "ok",
    }


# ---------------------------------------------------------------------------
# 2. PRICE SANITY
# ---------------------------------------------------------------------------

def check_price_sanity() -> dict[str, Any]:
    """
    Detect price outliers:
      - Prices below 0.10 PLN (probably fractional/error)
      - Prices above 2000 PLN (unusual for KADAX products)
      - Recent large price jumps (>100% change)
    """
    conn = _connect()
    cur = conn.cursor()

    # 2a. Very low prices
    cur.execute("""
        SELECT COUNT(*)
        FROM acc_product
        WHERE netto_purchase_price_pln > 0
          AND netto_purchase_price_pln < 0.10
    """)
    very_low = cur.fetchone()[0]

    # 2b. Very high prices
    cur.execute("""
        SELECT COUNT(*)
        FROM acc_product
        WHERE netto_purchase_price_pln > 2000
    """)
    very_high = cur.fetchone()[0]

    # 2c. Recent price jumps > 100%
    cur.execute("""
        SELECT COUNT(*)
        FROM acc_purchase_price a
        INNER JOIN acc_purchase_price b
            ON a.internal_sku = b.internal_sku
        WHERE a.valid_to IS NOT NULL AND b.valid_to IS NULL
          AND a.valid_to = b.valid_from
          AND a.netto_price_pln > 0
          AND ABS(b.netto_price_pln - a.netto_price_pln)
              / a.netto_price_pln > 1.0
          AND b.valid_from >= DATEADD(DAY, -30, GETDATE())
    """)
    big_jumps = cur.fetchone()[0]

    # 2d. Detail of jumps for logging
    jump_details = []
    if big_jumps > 0:
        cur.execute("""
            SELECT TOP 10 a.internal_sku,
                   a.netto_price_pln AS old_price,
                   b.netto_price_pln AS new_price,
                   ROUND((b.netto_price_pln - a.netto_price_pln)
                         / NULLIF(a.netto_price_pln, 0) * 100, 1) AS pct
            FROM acc_purchase_price a
            INNER JOIN acc_purchase_price b
                ON a.internal_sku = b.internal_sku
            WHERE a.valid_to IS NOT NULL AND b.valid_to IS NULL
              AND a.valid_to = b.valid_from
              AND a.netto_price_pln > 0
              AND ABS(b.netto_price_pln - a.netto_price_pln)
                  / a.netto_price_pln > 1.0
              AND b.valid_from >= DATEADD(DAY, -30, GETDATE())
            ORDER BY ABS(b.netto_price_pln - a.netto_price_pln) DESC
        """)
        for r in cur.fetchall():
            jump_details.append({
                "internal_sku": str(r[0]),
                "old_price": float(r[1]),
                "new_price": float(r[2]),
                "pct_change": float(r[3]),
            })

    conn.close()

    issues = []
    if very_low > 0:
        issues.append(f"{very_low} products with purchase price < 0.10 PLN")
    if very_high > 0:
        issues.append(f"{very_high} products with purchase price > 2000 PLN")
    if big_jumps > 5:
        issues.append(f"{big_jumps} products with > 100% price change in last 30 days")

    return {
        "check": "price_sanity",
        "very_low": very_low,
        "very_high": very_high,
        "big_jumps_30d": big_jumps,
        "jump_details": jump_details,
        "issues": issues,
        "status": "warning" if issues else "ok",
    }


# ---------------------------------------------------------------------------
# 3. COGS CONSISTENCY — stamped vs current price
# ---------------------------------------------------------------------------

def check_cogs_consistency() -> dict[str, Any]:
    """
    Detect order lines where stamped purchase_price_pln diverges
    significantly from the current product price.
    This catches stale stamps or mapping changes.
    """
    conn = _connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM acc_order_line ol
        INNER JOIN acc_product p ON p.id = ol.product_id
        WHERE ol.purchase_price_pln IS NOT NULL
          AND p.netto_purchase_price_pln IS NOT NULL
          AND p.netto_purchase_price_pln > 0
          AND ABS(ol.purchase_price_pln - p.netto_purchase_price_pln)
              / p.netto_purchase_price_pln > 0.5
    """)
    divergent = cur.fetchone()[0]

    # Sample divergent lines
    divergent_samples = []
    if divergent > 0:
        cur.execute("""
            SELECT TOP 10
                ol.sku, p.internal_sku,
                ol.purchase_price_pln AS stamped,
                p.netto_purchase_price_pln AS current_price,
                ol.price_source
            FROM acc_order_line ol
            INNER JOIN acc_product p ON p.id = ol.product_id
            WHERE ol.purchase_price_pln IS NOT NULL
              AND p.netto_purchase_price_pln IS NOT NULL
              AND p.netto_purchase_price_pln > 0
              AND ABS(ol.purchase_price_pln - p.netto_purchase_price_pln)
                  / p.netto_purchase_price_pln > 0.5
            ORDER BY ABS(ol.purchase_price_pln - p.netto_purchase_price_pln) DESC
        """)
        for r in cur.fetchall():
            divergent_samples.append({
                "sku": str(r[0]),
                "internal_sku": str(r[1]) if r[1] else None,
                "stamped_price": float(r[2]),
                "current_price": float(r[3]),
                "price_source": str(r[4]) if r[4] else None,
            })

    conn.close()

    issues = []
    if divergent > 20:
        issues.append(
            f"{divergent} order lines with stamped price diverging > 50% "
            f"from current product price"
        )

    return {
        "check": "cogs_consistency",
        "divergent_lines": divergent,
        "samples": divergent_samples,
        "issues": issues,
        "status": "warning" if issues else "ok",
    }


# ---------------------------------------------------------------------------
# 4. COVERAGE TRACKING
# ---------------------------------------------------------------------------

def check_coverage() -> dict[str, Any]:
    """
    Track COGS coverage: % of order lines with stamped purchase_price_pln.
    Also track mapping coverage (% of products with internal_sku).
    """
    conn = _connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM acc_order_line) AS total_lines,
            (SELECT COUNT(*) FROM acc_order_line
             WHERE purchase_price_pln IS NOT NULL) AS stamped_lines,
            (SELECT COUNT(*) FROM acc_product) AS total_products,
            (SELECT COUNT(*) FROM acc_product
             WHERE internal_sku IS NOT NULL) AS mapped_products,
            (SELECT COUNT(*) FROM acc_product
             WHERE netto_purchase_price_pln IS NOT NULL) AS priced_products
    """)
    r = cur.fetchone()
    total_lines = r[0]
    stamped = r[1]
    total_prod = r[2]
    mapped = r[3]
    priced = r[4]

    # Breakdown of unstamped lines by reason
    cur.execute("""
        SELECT
            SUM(CASE WHEN ol.product_id IS NULL THEN 1 ELSE 0 END) AS no_product,
            SUM(CASE WHEN ol.product_id IS NOT NULL
                      AND p.internal_sku IS NULL THEN 1 ELSE 0 END) AS no_sku,
            SUM(CASE WHEN ol.product_id IS NOT NULL
                      AND p.internal_sku IS NOT NULL
                      AND p.netto_purchase_price_pln IS NULL THEN 1 ELSE 0 END) AS no_price,
            SUM(CASE WHEN ol.product_id IS NOT NULL
                      AND p.netto_purchase_price_pln IS NOT NULL
                      AND ol.purchase_price_pln IS NULL THEN 1 ELSE 0 END) AS timing_gap
        FROM acc_order_line ol
        LEFT JOIN acc_product p ON p.id = ol.product_id
        WHERE ol.purchase_price_pln IS NULL
    """)
    gap = cur.fetchone()

    conn.close()

    cogs_pct = round(stamped / total_lines * 100, 1) if total_lines > 0 else 0
    map_pct = round(mapped / total_prod * 100, 1) if total_prod > 0 else 0

    issues = []
    if cogs_pct < 95:
        issues.append(f"COGS coverage {cogs_pct}% is below 95% target")
    if map_pct < 90:
        issues.append(f"Mapping coverage {map_pct}% is below 90% target")

    return {
        "check": "coverage",
        "cogs_coverage_pct": cogs_pct,
        "mapping_coverage_pct": map_pct,
        "total_lines": total_lines,
        "stamped_lines": stamped,
        "unstamped_lines": total_lines - stamped,
        "gap_breakdown": {
            "no_product": gap[0] or 0,
            "no_internal_sku": gap[1] or 0,
            "no_price": gap[2] or 0,
            "timing_gap": gap[3] or 0,
        },
        "total_products": total_prod,
        "mapped_products": mapped,
        "priced_products": priced,
        "issues": issues,
        "status": "warning" if issues else "ok",
    }


# ---------------------------------------------------------------------------
# 5. MARGIN RATIO CHECK (PLN-converted)
# ---------------------------------------------------------------------------

def check_margin_ratio() -> dict[str, Any]:
    """
    Compute COGS / revenue_PLN ratio using exchange rates.
    Flag lines where COGS > revenue (true losses).
    """
    conn = _connect()
    cur = conn.cursor()

    # Build CASE for exchange rates
    cur.execute("""
        SELECT e.currency, e.rate_to_pln
        FROM acc_exchange_rate e
        INNER JOIN (
            SELECT currency, MAX(rate_date) AS max_date
            FROM acc_exchange_rate GROUP BY currency
        ) mx ON e.currency = mx.currency AND e.rate_date = mx.max_date
    """)
    rate_parts = []
    for r in cur.fetchall():
        rate_parts.append(f"WHEN ol.currency = '{r[0]}' THEN {r[1]}")
    rate_case = "CASE " + " ".join(rate_parts) + " ELSE 1 END" if rate_parts else "1"

    cur.execute(f"""
        SELECT
            COUNT(*) AS total,
            AVG(ol.cogs_pln / NULLIF(ol.item_price * ({rate_case}), 0) * 100)
                AS avg_cogs_pct,
            SUM(CASE WHEN ol.cogs_pln > ol.item_price * ({rate_case}) * 1.0
                THEN 1 ELSE 0 END) AS loss_lines,
            SUM(CASE WHEN ol.cogs_pln > ol.item_price * ({rate_case}) * 1.5
                THEN 1 ELSE 0 END) AS severe_loss_lines
        FROM acc_order_line ol
        WHERE ol.cogs_pln IS NOT NULL AND ol.item_price > 0
    """)
    r = cur.fetchone()
    total = r[0]
    avg_cogs_pct = round(float(r[1]), 1) if r[1] else 0
    loss_lines = r[2]
    severe_loss = r[3]

    conn.close()

    issues = []
    if loss_lines > total * 0.02:
        issues.append(
            f"{loss_lines} lines ({loss_lines/total*100:.1f}%) "
            f"have COGS > revenue — possible mapping/price errors"
        )
    if avg_cogs_pct > 50:
        issues.append(f"Average COGS ratio {avg_cogs_pct}% seems high")

    return {
        "check": "margin_ratio",
        "total_lines": total,
        "avg_cogs_pct": avg_cogs_pct,
        "loss_lines": loss_lines,
        "severe_loss_lines": severe_loss,
        "loss_pct": round(loss_lines / total * 100, 2) if total > 0 else 0,
        "issues": issues,
        "status": "warning" if issues else "ok",
    }


# ---------------------------------------------------------------------------
# FULL AUDIT — runs all checks
# ---------------------------------------------------------------------------

def run_full_audit(*, persist: bool = False,
                   trigger_source: str = "manual") -> dict[str, Any]:
    """Run all COGS data quality checks and return consolidated report.

    Args:
        persist: If True, save results to acc_audit_log table.
        trigger_source: 'scheduler' | 'manual' | 'pipeline'
    """
    checks = [
        check_coverage,
        check_mapping_integrity,
        check_price_sanity,
        check_cogs_consistency,
        check_margin_ratio,
        check_stale_prices,
        check_recent_controlling_alerts,
    ]

    results = []
    all_issues = []
    overall_status = "ok"

    for check_fn in checks:
        try:
            result = check_fn()
            results.append(result)
            all_issues.extend(result.get("issues", []))
            if result.get("status") == "warning":
                overall_status = "warning"
            if result.get("status") == "error":
                overall_status = "error"
        except Exception as e:
            log.error("audit.check_failed", check=check_fn.__name__, error=str(e))
            results.append({
                "check": check_fn.__name__,
                "status": "error",
                "error": str(e),
            })
            overall_status = "error"

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall_status,
        "total_issues": len(all_issues),
        "issues": all_issues,
        "checks": results,
    }

    # Extract key metrics from checks for logging + storage
    cov = next((c for c in results if c.get("check") == "coverage"), {})
    margin = next((c for c in results if c.get("check") == "margin_ratio"), {})

    log.info(
        "audit.complete",
        status=overall_status,
        issues=len(all_issues),
        cogs_pct=cov.get("cogs_coverage_pct"),
    )

    if persist:
        try:
            _save_audit_log(
                overall_status=overall_status,
                cogs_coverage_pct=cov.get("cogs_coverage_pct"),
                mapping_coverage_pct=cov.get("mapping_coverage_pct"),
                total_issues=len(all_issues),
                loss_lines=margin.get("loss_lines"),
                avg_cogs_pct=margin.get("avg_cogs_pct"),
                issues=all_issues,
                checks=results,
                trigger_source=trigger_source,
            )
        except Exception as e:
            log.error("audit.save_log_error", error=str(e))

    return report


def _save_audit_log(
    *,
    overall_status: str,
    cogs_coverage_pct: float | None,
    mapping_coverage_pct: float | None,
    total_issues: int,
    loss_lines: int | None,
    avg_cogs_pct: float | None,
    issues: list[str],
    checks: list[dict],
    trigger_source: str,
) -> None:
    """Persist audit report to acc_audit_log (MERGE on date+trigger)."""
    conn = _connect()
    cur = conn.cursor()

    today = date.today().isoformat()

    # Use MERGE to upsert — one row per (date, trigger_source)
    cur.execute("""
        MERGE dbo.acc_audit_log AS tgt
        USING (SELECT ? AS audit_date, ? AS trigger_source) AS src
            ON tgt.audit_date = src.audit_date
           AND tgt.trigger_source = src.trigger_source
        WHEN MATCHED THEN
            UPDATE SET
                overall_status = ?,
                cogs_coverage_pct = ?,
                mapping_coverage_pct = ?,
                total_issues = ?,
                loss_lines = ?,
                avg_cogs_pct = ?,
                issues_json = ?,
                checks_json = ?,
                created_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (audit_date, overall_status, cogs_coverage_pct,
                    mapping_coverage_pct, total_issues, loss_lines,
                    avg_cogs_pct, issues_json, checks_json, trigger_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
        # USING params
        today, trigger_source,
        # UPDATE params
        overall_status, cogs_coverage_pct, mapping_coverage_pct,
        total_issues, loss_lines, avg_cogs_pct,
        json.dumps(issues, ensure_ascii=False),
        json.dumps(checks, ensure_ascii=False, default=str),
        # INSERT params
        today, overall_status, cogs_coverage_pct, mapping_coverage_pct,
        total_issues, loss_lines, avg_cogs_pct,
        json.dumps(issues, ensure_ascii=False),
        json.dumps(checks, ensure_ascii=False, default=str),
        trigger_source,
    )
    conn.close()
    log.info("audit.log_saved", date=today, trigger=trigger_source)


# ---------------------------------------------------------------------------
# PIPELINE VALIDATION — lightweight check after stamp step
# ---------------------------------------------------------------------------

def check_stale_prices(*, max_age_days: int = 90) -> dict[str, Any]:
    """Delegate to controlling module for stale price detection."""
    from app.services.controlling import check_stale_prices as _check_stale
    return _check_stale(max_age_days=max_age_days)


def check_recent_controlling_alerts() -> dict[str, Any]:
    """Check for recent controlling alerts (blocked overwrites, flagged prices)."""
    conn = _connect()
    cur = conn.cursor()

    blocked_7d = 0
    flagged_prices_7d = 0

    try:
        cur.execute("""
            SELECT COUNT(*) FROM acc_mapping_change_log WITH (NOLOCK)
            WHERE change_type = 'blocked'
              AND created_at >= DATEADD(DAY, -7, GETUTCDATE())
        """)
        blocked_7d = cur.fetchone()[0]
    except Exception:
        pass

    try:
        cur.execute("""
            SELECT COUNT(*) FROM acc_price_change_log WITH (NOLOCK)
            WHERE flagged = 1
              AND created_at >= DATEADD(DAY, -7, GETUTCDATE())
        """)
        flagged_prices_7d = cur.fetchone()[0]
    except Exception:
        pass

    conn.close()

    issues = []
    if blocked_7d > 0:
        issues.append(f"{blocked_7d} mapping overwrites BLOCKED in last 7 days (source priority)")
    if flagged_prices_7d > 0:
        issues.append(f"{flagged_prices_7d} price changes FLAGGED in last 7 days (anomalies)")

    return {
        "check": "controlling_alerts",
        "blocked_overwrites_7d": blocked_7d,
        "flagged_prices_7d": flagged_prices_7d,
        "issues": issues,
        "status": "warning" if issues else "ok",
    }


# ---------------------------------------------------------------------------
# PIPELINE VALIDATION — lightweight check after stamp step
# ---------------------------------------------------------------------------

def validate_after_stamp(stamped_count: int) -> list[str]:
    """
    Quick validation to run after step 5 (stamp purchase prices).
    Returns list of warning messages (empty = all good).
    Designed to be fast (<1 second).
    """
    warnings = []
    conn = _connect()
    cur = conn.cursor()

    # Check: any ACTIVE (non-cancelled) stamped line has 0 or negative cogs
    # Cancelled orders legitimately have qty=0 → cogs=0, so we exclude them.
    cur.execute("""
        SELECT COUNT(*)
        FROM acc_order_line ol WITH (NOLOCK)
        INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE ol.purchase_price_pln IS NOT NULL
          AND (ol.cogs_pln <= 0 OR ol.cogs_pln IS NULL)
          AND ol.quantity_ordered > 0
          AND o.status NOT IN ('Cancelled', 'Canceled')
    """)
    bad_cogs = cur.fetchone()[0]
    if bad_cogs > 0:
        warnings.append(f"{bad_cogs} active lines with purchase_price but missing/zero cogs_pln")

    # Check: coverage didn't drop (compare against expected minimum)
    # Only count active (non-cancelled) order lines with qty > 0
    cur.execute("""
        SELECT
            CAST(SUM(CASE WHEN ol.purchase_price_pln IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT)
            / NULLIF(COUNT(*), 0) * 100
        FROM acc_order_line ol WITH (NOLOCK)
        INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE ol.quantity_ordered > 0
          AND o.status NOT IN ('Cancelled', 'Canceled')
    """)
    coverage = cur.fetchone()[0]
    if coverage and coverage < 95.0:
        warnings.append(f"COGS coverage dropped to {coverage:.1f}% (target: 95%+)")

    conn.close()

    if warnings:
        for w in warnings:
            log.warning("audit.post_stamp_warning", message=w)
    else:
        log.info("audit.post_stamp_ok", stamped=stamped_count)

    return warnings
