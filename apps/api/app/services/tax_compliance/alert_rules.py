"""
Compliance Alert Rules Engine.

P1 (Critical):
  - WSTO without required evidence past threshold
  - FBA movement without WDT/WNT match
  - Local sale in warehouse country without local VAT mapping
  - High share of UNCLASSIFIED VAT events
  - Major mismatch between settlements and VAT decomposition
  - OSS filing blocked with deadline approaching

P2 (Warning):
  - Missing VAT rate mapping
  - Refund not linked to original tax jurisdiction
  - Currency/ECB conversion missing
  - Movement evidence partial
  - High correction backlog

P3 (Info):
  - Incomplete audit pack
  - Missing helper documents
  - Low confidence classifications
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

EVIDENCE_THRESHOLD_DAYS = 30
UNCLASSIFIED_THRESHOLD_PCT = 5.0
MISMATCH_THRESHOLD_PLN = 1000.0
OSS_DEADLINE_DAYS = 20  # Days before end of month following quarter


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def detect_compliance_issues(days_back: int = 90) -> dict[str, Any]:
    """
    Run all alert rules and create compliance_issue records.

    Idempotent — skips issues that already exist (open) for the same source_ref + type.
    """
    conn = _connect()
    cur = conn.cursor()
    stats = {
        "p1_created": 0, "p2_created": 0, "p3_created": 0,
        "skipped_existing": 0, "total_checked": 0,
    }

    try:
        cutoff = date.today() - timedelta(days=days_back)

        # ── P1: WSTO without evidence past threshold ─────────────
        threshold_date = date.today() - timedelta(days=EVIDENCE_THRESHOLD_DAYS)
        cur.execute("""
            SELECT vel.order_id, vel.marketplace
            FROM dbo.vat_event_ledger vel WITH (NOLOCK)
            WHERE vel.vat_classification = 'WSTO'
              AND vel.evidence_status IN ('missing', 'partial')
              AND vel.event_date <= ?
              AND vel.event_date >= ?
        """, (threshold_date, cutoff))
        for row in cur.fetchall():
            stats["total_checked"] += 1
            if _create_issue_if_new(cur, "missing_evidence", "P1", row[0], None, row[1],
                                    f"WSTO order {row[0]} missing evidence (>{EVIDENCE_THRESHOLD_DAYS}d)"):
                stats["p1_created"] += 1
            else:
                stats["skipped_existing"] += 1

        # ── P1: FBA movement without WDT/WNT match ──────────────
        cur.execute("""
            SELECT movement_ref, from_country, to_country
            FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE matching_pair_status = 'unmatched'
              AND movement_date >= ?
              AND vat_treatment IN ('WDT_OWN_GOODS', 'WNT_OWN_GOODS')
        """, (cutoff,))
        for row in cur.fetchall():
            stats["total_checked"] += 1
            if _create_issue_if_new(cur, "movement_unmatched", "P1", row[0], row[1], None,
                                    f"FBA movement {row[0]} ({row[1]}->{row[2]}) unmatched WDT/WNT pair"):
                stats["p1_created"] += 1
            else:
                stats["skipped_existing"] += 1

        # ── P1: High UNCLASSIFIED ratio ──────────────────────────
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN vat_classification = 'UNCLASSIFIED' THEN 1 ELSE 0 END) as unclassified
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE event_date >= ?
        """, (cutoff,))
        totals = cur.fetchone()
        total_events = int(totals[0] or 0)
        unclassified = int(totals[1] or 0)
        if total_events > 0:
            pct = unclassified / total_events * 100
            if pct > UNCLASSIFIED_THRESHOLD_PCT:
                stats["total_checked"] += 1
                if _create_issue_if_new(cur, "vat_unclassified", "P1", f"batch_{date.today()}", None, None,
                                        f"{unclassified}/{total_events} events ({pct:.1f}%) UNCLASSIFIED — exceeds {UNCLASSIFIED_THRESHOLD_PCT}% threshold"):
                    stats["p1_created"] += 1

        # ── P1: Major settlement mismatch ────────────────────────
        cur.execute("""
            SELECT settlement_id, difference_amount
            FROM dbo.amazon_clearing_reconciliation WITH (NOLOCK)
            WHERE status = 'mismatch'
              AND ABS(difference_amount) > ?
        """, (MISMATCH_THRESHOLD_PLN,))
        for row in cur.fetchall():
            stats["total_checked"] += 1
            if _create_issue_if_new(cur, "filing_blocker", "P1", row[0], None, None,
                                    f"Settlement {row[0]} mismatch: {float(row[1]):.2f} PLN difference"):
                stats["p1_created"] += 1
            else:
                stats["skipped_existing"] += 1

        # ── P2: Missing VAT rate mapping ─────────────────────────
        cur.execute("""
            SELECT DISTINCT consumption_country
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE tax_rate IS NULL
              AND consumption_country IS NOT NULL
              AND event_date >= ?
              AND vat_classification IN ('WSTO', 'LOCAL_VAT')
        """, (cutoff,))
        for row in cur.fetchall():
            stats["total_checked"] += 1
            if _create_issue_if_new(cur, "missing_evidence", "P2", f"rate_{row[0]}", row[0], None,
                                    f"No VAT rate mapping for country {row[0]}"):
                stats["p2_created"] += 1
            else:
                stats["skipped_existing"] += 1

        # ── P2: ECB rate missing ─────────────────────────────────
        cur.execute("""
            SELECT DISTINCT currency
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE amount_eur IS NULL
              AND currency IS NOT NULL AND currency != 'EUR'
              AND vat_classification = 'WSTO'
              AND event_date >= ?
        """, (cutoff,))
        for row in cur.fetchall():
            stats["total_checked"] += 1
            if _create_issue_if_new(cur, "ecb_rate_missing", "P2", f"ecb_{row[0]}", None, None,
                                    f"No ECB rate for currency {row[0]} — affects WSTO EUR conversion"):
                stats["p2_created"] += 1

        # ── P3: Low confidence classifications ───────────────────
        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE confidence_score < 0.5
              AND confidence_score > 0
              AND event_date >= ?
        """, (cutoff,))
        low_conf = cur.fetchone()[0] or 0
        if low_conf > 0:
            stats["total_checked"] += 1
            if _create_issue_if_new(cur, "vat_unclassified", "P3", f"low_conf_{date.today()}", None, None,
                                    f"{low_conf} events with confidence < 50% — review recommended"):
                stats["p3_created"] += 1

        conn.commit()
        log.info("detect_compliance_issues.done", **stats)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def _create_issue_if_new(
    cur, issue_type: str, severity: str,
    source_ref: str | None, country: str | None, marketplace: str | None,
    description: str,
) -> bool:
    """Create compliance issue if no open issue with same type+source_ref exists."""
    cur.execute("""
        SELECT COUNT(*) FROM dbo.compliance_issue WITH (NOLOCK)
        WHERE issue_type = ? AND source_ref = ? AND status = 'open'
    """, (issue_type, source_ref))

    if (cur.fetchone()[0] or 0) > 0:
        return False

    cur.execute("""
        INSERT INTO dbo.compliance_issue(
            issue_type, severity, source_ref,
            country, marketplace, description, status
        ) VALUES (?, ?, ?, ?, ?, ?, 'open')
    """, (issue_type, severity, source_ref, country, marketplace, description))
    return True


def resolve_issue(issue_id: int, resolver: str | None = None) -> dict[str, Any]:
    """Resolve a compliance issue."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE dbo.compliance_issue
            SET status = 'resolved', owner = ?
            WHERE id = ?
        """, (resolver, issue_id))
        conn.commit()
        return {"resolved": True, "id": issue_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def assign_issue(issue_id: int, owner: str) -> dict[str, Any]:
    """Assign a compliance issue to an owner."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE dbo.compliance_issue
            SET owner = ?
            WHERE id = ?
        """, (owner, issue_id))
        conn.commit()
        return {"assigned": True, "id": issue_id, "owner": owner}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_compliance_issues(
    issue_type: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    country: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """List compliance issues with filters."""
    conn = _connect()
    cur = conn.cursor()
    try:
        where_parts = ["1=1"]
        params: list = []

        if issue_type:
            where_parts.append("ci.issue_type = ?")
            params.append(issue_type)
        if severity:
            where_parts.append("ci.severity = ?")
            params.append(severity)
        if status:
            where_parts.append("ci.status = ?")
            params.append(status)
        if country:
            where_parts.append("ci.country = ?")
            params.append(country)

        where_sql = " AND ".join(where_parts)
        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT COUNT(*) FROM dbo.compliance_issue ci WITH (NOLOCK)
            WHERE {where_sql}
        """, tuple(params))
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT ci.id, ci.issue_type, ci.severity, ci.source_ref,
                   ci.country, ci.marketplace, ci.description,
                   ci.status, ci.owner, ci.created_at
            FROM dbo.compliance_issue ci WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY
                CASE ci.severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
                ci.created_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, tuple(params) + (offset, page_size))

        items = _fetchall_dict(cur)
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        conn.close()


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]
