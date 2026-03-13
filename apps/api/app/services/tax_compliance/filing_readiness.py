"""
Filing Readiness — dashboard measuring compliance preparedness.

Computes readiness percentages for:
  VIU-DO, JPK, Local VAT, Evidence, Movement matching
Identifies critical blockers.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def build_filing_readiness_snapshot(
    period_type: str = "quarterly",
    period_ref: str | None = None,
) -> dict[str, Any]:
    """
    Build a filing readiness snapshot for a given period.

    Checks:
    1. VIU-DO readiness — all WSTO classified, ECB rates present, OSS period built
    2. JPK readiness — PL transactions mapped, VAT codes assigned
    3. Local VAT readiness — warehouse-country sales classified and ledgered
    4. Evidence completeness — % of WSTO with complete Art. 22a evidence
    5. Movement matching — % of FBA movements matched as WDT/WNT pairs
    """
    conn = _connect()
    cur = conn.cursor()

    try:
        today = date.today()
        if not period_ref:
            if period_type == "quarterly":
                q = (today.month - 1) // 3 + 1
                period_ref = f"{today.year}-Q{q}"
            else:
                period_ref = today.strftime("%Y-%m")

        # Determine date range from period_ref
        if "-Q" in period_ref:
            year = int(period_ref.split("-Q")[0])
            quarter = int(period_ref.split("-Q")[1])
            start_month = (quarter - 1) * 3 + 1
            date_from = date(year, start_month, 1)
            if quarter == 4:
                date_to = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                date_to = date(year, start_month + 3, 1) - timedelta(days=1)
        else:
            year = int(period_ref[:4])
            month = int(period_ref[5:7])
            date_from = date(year, month, 1)
            if month == 12:
                date_to = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                date_to = date(year, month + 1, 1) - timedelta(days=1)

        # 1. VIU-DO readiness
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN vat_classification != 'UNCLASSIFIED' THEN 1 ELSE 0 END) as classified,
                   SUM(CASE WHEN amount_eur IS NOT NULL THEN 1 ELSE 0 END) as has_eur
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE vat_classification = 'WSTO' AND event_date >= ? AND event_date <= ?
        """, (date_from, date_to))
        wsto = cur.fetchone()
        wsto_total = int(wsto[0] or 0)
        wsto_classified = int(wsto[1] or 0)
        wsto_eur = int(wsto[2] or 0)

        # Check if OSS period exists
        if "-Q" in period_ref:
            cur.execute("""
                SELECT status FROM dbo.oss_return_period WITH (NOLOCK)
                WHERE year = ? AND quarter = ?
            """, (year, quarter))
            oss_period = cur.fetchone()
            oss_ready = oss_period is not None and oss_period[0] in ("draft", "ready", "filed")
        else:
            oss_ready = False

        viu_do_pct = 0.0
        if wsto_total > 0:
            classified_pct = wsto_classified / wsto_total * 50
            eur_pct = wsto_eur / wsto_total * 30
            oss_pct = 20 if oss_ready else 0
            viu_do_pct = min(100, classified_pct + eur_pct + oss_pct)

        # 2. JPK readiness — PL transactions
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN vat_classification != 'UNCLASSIFIED' THEN 1 ELSE 0 END) as classified
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE consumption_country = 'PL' AND event_date >= ? AND event_date <= ?
        """, (date_from, date_to))
        jpk = cur.fetchone()
        jpk_total = int(jpk[0] or 0)
        jpk_classified = int(jpk[1] or 0)
        jpk_pct = (jpk_classified / jpk_total * 100) if jpk_total > 0 else 100.0

        # 3. Local VAT readiness
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN vat_classification = 'LOCAL_VAT' THEN 1 ELSE 0 END) as local_count,
                   SUM(CASE WHEN vat_classification = 'UNCLASSIFIED' THEN 1 ELSE 0 END) as unclassified
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE warehouse_country IS NOT NULL AND warehouse_country != 'PL'
              AND event_date >= ? AND event_date <= ?
        """, (date_from, date_to))
        local = cur.fetchone()
        local_total = int(local[0] or 0)
        local_classified = int(local[1] or 0)
        local_unclassified = int(local[2] or 0)
        local_pct = ((local_total - local_unclassified) / local_total * 100) if local_total > 0 else 100.0

        # 4. Evidence completeness
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN evidence_status = 'complete' THEN 1 ELSE 0 END) as complete
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE vat_classification = 'WSTO' AND event_date >= ? AND event_date <= ?
        """, (date_from, date_to))
        ev = cur.fetchone()
        ev_total = int(ev[0] or 0)
        ev_complete = int(ev[1] or 0)
        evidence_pct = (ev_complete / ev_total * 100) if ev_total > 0 else 100.0

        # 5. Movement matching
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN matching_pair_status = 'matched' THEN 1 ELSE 0 END) as matched
            FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE movement_date >= ? AND movement_date <= ?
        """, (date_from, date_to))
        mv = cur.fetchone()
        mv_total = int(mv[0] or 0)
        mv_matched = int(mv[1] or 0)
        movement_pct = (mv_matched / mv_total * 100) if mv_total > 0 else 100.0

        # Critical issues
        cur.execute("""
            SELECT COUNT(*) FROM dbo.compliance_issue WITH (NOLOCK)
            WHERE status = 'open' AND severity = 'P1'
        """)
        critical_count = cur.fetchone()[0] or 0

        # Save snapshot
        cur.execute("""
            MERGE dbo.filing_readiness_snapshot AS tgt
            USING (SELECT ? AS pt, ? AS pr) AS src
            ON tgt.period_type = src.pt AND tgt.period_ref = src.pr
            WHEN MATCHED THEN
                UPDATE SET viu_do_ready_pct = ?, jpk_ready_pct = ?,
                           local_vat_ready_pct = ?, evidence_complete_pct = ?,
                           movement_match_pct = ?, critical_issues_count = ?,
                           created_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (period_type, period_ref, viu_do_ready_pct, jpk_ready_pct,
                        local_vat_ready_pct, evidence_complete_pct,
                        movement_match_pct, critical_issues_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            period_type, period_ref,
            round(viu_do_pct, 4), round(jpk_pct, 4),
            round(local_pct, 4), round(evidence_pct, 4),
            round(movement_pct, 4), critical_count,
            period_type, period_ref,
            round(viu_do_pct, 4), round(jpk_pct, 4),
            round(local_pct, 4), round(evidence_pct, 4),
            round(movement_pct, 4), critical_count,
        ))

        conn.commit()

        result = {
            "period_type": period_type,
            "period_ref": period_ref,
            "viu_do_ready_pct": round(viu_do_pct, 2),
            "jpk_ready_pct": round(jpk_pct, 2),
            "local_vat_ready_pct": round(local_pct, 2),
            "evidence_complete_pct": round(evidence_pct, 2),
            "movement_match_pct": round(movement_pct, 2),
            "critical_issues_count": critical_count,
            "details": {
                "wsto_total": wsto_total,
                "wsto_classified": wsto_classified,
                "wsto_has_eur": wsto_eur,
                "oss_period_exists": oss_ready,
                "jpk_total": jpk_total,
                "local_total": local_total,
                "evidence_total": ev_total,
                "evidence_complete": ev_complete,
                "movements_total": mv_total,
                "movements_matched": mv_matched,
            },
        }

        log.info("filing_readiness.snapshot", **{k: v for k, v in result.items() if k != "details"})
        return result

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_filing_readiness(period_ref: str | None = None) -> dict[str, Any]:
    """Get latest filing readiness snapshot."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if period_ref:
            cur.execute("""
                SELECT TOP 1 * FROM dbo.filing_readiness_snapshot WITH (NOLOCK)
                WHERE period_ref = ?
                ORDER BY created_at DESC
            """, (period_ref,))
        else:
            cur.execute("""
                SELECT TOP 1 * FROM dbo.filing_readiness_snapshot WITH (NOLOCK)
                ORDER BY created_at DESC
            """)

        row = cur.fetchone()
        if not row or not cur.description:
            return {"error": "No readiness snapshot found. Run build first."}

        cols = [c[0] for c in cur.description]
        snapshot = dict(zip(cols, row))

        # Get blockers
        cur.execute("""
            SELECT TOP 20 id, issue_type, severity, source_ref,
                   country, marketplace, description, status
            FROM dbo.compliance_issue WITH (NOLOCK)
            WHERE status = 'open'
            ORDER BY
                CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
                created_at DESC
        """)
        blockers = _fetchall_dict(cur)

        return {
            "snapshot": snapshot,
            "blockers": blockers,
        }
    finally:
        conn.close()


def get_filing_blockers(
    filing_type: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Get compliance issues that block filings."""
    conn = _connect()
    cur = conn.cursor()
    try:
        where_parts = ["ci.status = 'open'"]
        params: list = []

        # Map filing_type to issue_type patterns
        if filing_type == "oss":
            where_parts.append("ci.issue_type IN ('vat_unclassified', 'missing_evidence', 'ecb_rate_missing', 'filing_blocker')")
        elif filing_type == "jpk":
            where_parts.append("ci.issue_type IN ('vat_unclassified', 'filing_blocker')")
            where_parts.append("ci.country = 'PL' OR ci.country IS NULL")
        elif filing_type == "local_vat":
            where_parts.append("ci.issue_type IN ('local_vat_missing', 'vat_unclassified')")

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
