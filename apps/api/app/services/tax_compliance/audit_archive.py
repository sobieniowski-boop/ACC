"""
Audit Archive — compliance data retention and audit pack generation.

Supports 10-year retention of OSS records and associated evidence.
Generates downloadable audit packs per period.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
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


def generate_audit_pack(
    period_type: str = "quarterly",
    period_ref: str = "",
) -> dict[str, Any]:
    """
    Generate a comprehensive audit pack for a given period.

    Includes:
    - VAT event ledger extract
    - OSS return period + lines
    - Local VAT ledger extract
    - Transport evidence summary
    - FBA movement ledger extract
    - Settlement reconciliation extract
    - VAT rate snapshot
    - Filing readiness snapshot
    - Compliance issues log
    """
    conn = _connect()
    cur = conn.cursor()

    try:
        # Determine date range
        if "-Q" in period_ref:
            year = int(period_ref.split("-Q")[0])
            quarter = int(period_ref.split("-Q")[1])
            start_month = (quarter - 1) * 3 + 1
            date_from = date(year, start_month, 1)
            if quarter == 4:
                date_to = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                date_to = date(year, start_month + 3, 1) - timedelta(days=1)
        elif len(period_ref) == 7:  # yyyy-MM
            year = int(period_ref[:4])
            month = int(period_ref[5:7])
            date_from = date(year, month, 1)
            if month == 12:
                date_to = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                date_to = date(year, month + 1, 1) - timedelta(days=1)
        else:
            return {"error": f"Invalid period_ref: {period_ref}. Use yyyy-Qq or yyyy-MM."}

        pack: dict[str, Any] = {
            "period_type": period_type,
            "period_ref": period_ref,
            "date_from": str(date_from),
            "date_to": str(date_to),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sections": {},
        }

        # 1. VAT Event Ledger
        cur.execute("""
            SELECT vat_classification, COUNT(*) as cnt,
                   SUM(tax_base_amount) as total_base,
                   SUM(tax_amount) as total_tax,
                   SUM(gross_amount) as total_gross
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE event_date >= ? AND event_date <= ?
            GROUP BY vat_classification
        """, (date_from, date_to))
        pack["sections"]["vat_events_summary"] = _fetchall_dict(cur)

        cur.execute("""
            SELECT COUNT(*) FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE event_date >= ? AND event_date <= ?
        """, (date_from, date_to))
        pack["sections"]["vat_events_total"] = cur.fetchone()[0] or 0

        # 2. OSS Return
        if "-Q" in period_ref:
            cur.execute("""
                SELECT p.id, p.year, p.quarter, p.status,
                       p.total_base_eur, p.total_tax_eur, p.corrections_count
                FROM dbo.oss_return_period p WITH (NOLOCK)
                WHERE p.year = ? AND p.quarter = ?
            """, (year, quarter))
            oss_period = _fetchall_dict(cur)

            if oss_period:
                pid = oss_period[0]["id"]
                cur.execute("""
                    SELECT consumption_country, vat_rate,
                           tax_base_eur, tax_amount_eur,
                           correction_flag, source_count
                    FROM dbo.oss_return_line WITH (NOLOCK)
                    WHERE oss_period_id = ?
                    ORDER BY consumption_country, vat_rate
                """, (pid,))
                pack["sections"]["oss_period"] = oss_period[0]
                pack["sections"]["oss_lines"] = _fetchall_dict(cur)
            else:
                pack["sections"]["oss_period"] = None
                pack["sections"]["oss_lines"] = []

        # 3. Local VAT
        cur.execute("""
            SELECT country, COUNT(*) as cnt,
                   SUM(tax_base) as total_base,
                   SUM(tax_amount) as total_tax
            FROM dbo.local_vat_ledger WITH (NOLOCK)
            WHERE event_date >= ? AND event_date <= ?
            GROUP BY country
        """, (date_from, date_to))
        pack["sections"]["local_vat_summary"] = _fetchall_dict(cur)

        # 4. Evidence
        cur.execute("""
            SELECT evidence_status, COUNT(*) as cnt
            FROM dbo.transport_evidence_record WITH (NOLOCK)
            WHERE created_at >= ? AND created_at <= ?
            GROUP BY evidence_status
        """, (date_from, date_to))
        pack["sections"]["evidence_summary"] = _fetchall_dict(cur)

        # 5. FBA Movements
        cur.execute("""
            SELECT vat_treatment, matching_pair_status,
                   COUNT(*) as cnt, SUM(quantity) as total_qty
            FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE movement_date >= ? AND movement_date <= ?
            GROUP BY vat_treatment, matching_pair_status
        """, (date_from, date_to))
        pack["sections"]["movements_summary"] = _fetchall_dict(cur)

        # 6. Reconciliation
        cur.execute("""
            SELECT status, COUNT(*) as cnt,
                   SUM(gross_sales) as total_gross,
                   SUM(payout_net) as total_payout,
                   SUM(ABS(difference_amount)) as total_diff
            FROM dbo.amazon_clearing_reconciliation WITH (NOLOCK)
            WHERE settlement_date >= ? AND settlement_date <= ?
            GROUP BY status
        """, (date_from, date_to))
        pack["sections"]["reconciliation_summary"] = _fetchall_dict(cur)

        # 7. VAT Rate Snapshot
        cur.execute("""
            SELECT country, rate, valid_from, valid_to, is_default
            FROM dbo.vat_rate_mapping WITH (NOLOCK)
            WHERE valid_from <= ? AND (valid_to IS NULL OR valid_to >= ?)
            ORDER BY country
        """, (date_to, date_from))
        pack["sections"]["vat_rates_snapshot"] = _fetchall_dict(cur)

        # 8. Filing Readiness
        cur.execute("""
            SELECT TOP 1 * FROM dbo.filing_readiness_snapshot WITH (NOLOCK)
            WHERE period_ref = ?
            ORDER BY created_at DESC
        """, (period_ref,))
        row = cur.fetchone()
        if row and cur.description:
            cols = [c[0] for c in cur.description]
            pack["sections"]["filing_readiness"] = dict(zip(cols, row))
        else:
            pack["sections"]["filing_readiness"] = None

        # 9. Compliance Issues
        cur.execute("""
            SELECT issue_type, severity, COUNT(*) as cnt
            FROM dbo.compliance_issue WITH (NOLOCK)
            WHERE created_at >= ? AND created_at <= ?
            GROUP BY issue_type, severity
            ORDER BY
                CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END
        """, (date_from, date_to))
        pack["sections"]["compliance_issues_summary"] = _fetchall_dict(cur)

        log.info("audit_pack.generated", period_ref=period_ref, sections=len(pack["sections"]))
        return pack

    finally:
        conn.close()


def list_audit_packs() -> dict[str, Any]:
    """List available periods for audit pack generation."""
    conn = _connect()
    cur = conn.cursor()
    try:
        # Available OSS periods
        cur.execute("""
            SELECT year, quarter, status, total_base_eur, total_tax_eur
            FROM dbo.oss_return_period WITH (NOLOCK)
            ORDER BY year DESC, quarter DESC
        """)
        oss_periods = _fetchall_dict(cur)

        # Available months with data
        cur.execute("""
            SELECT FORMAT(event_date, 'yyyy-MM') as month,
                   COUNT(*) as event_count
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            GROUP BY FORMAT(event_date, 'yyyy-MM')
            ORDER BY month DESC
        """)
        months = _fetchall_dict(cur)

        # Filing readiness snapshots
        cur.execute("""
            SELECT period_type, period_ref,
                   viu_do_ready_pct, jpk_ready_pct,
                   created_at
            FROM dbo.filing_readiness_snapshot WITH (NOLOCK)
            ORDER BY created_at DESC
        """)
        snapshots = _fetchall_dict(cur)

        return {
            "oss_periods": oss_periods,
            "months_with_data": months,
            "readiness_snapshots": snapshots,
        }
    finally:
        conn.close()
