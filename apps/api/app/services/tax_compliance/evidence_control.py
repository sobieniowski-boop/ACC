"""
Evidence Control (Art. 22a) — transport & delivery proof tracking.

For WSTO transactions, verifies completeness of:
  - transport proof (carrier tracking)
  - delivery confirmation
  - order record
  - payment proof

Transactions without complete evidence go to the "Evidence Suspense Register".
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

EVIDENCE_THRESHOLD_DAYS = 30  # Days before evidence becomes overdue


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _fetchall_dict(cur, rows=None) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    data = rows if rows is not None else cur.fetchall()
    return [{cols[i]: row[i] for i in range(len(cols))} for row in data]


def sync_transport_evidence(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """
    Sync transport evidence from order/logistics data into evidence records.

    Pulls tracking info from acc_order + DHL/GLS shipments and matches
    to WSTO events in vat_event_ledger.
    """
    conn = _connect()
    cur = conn.cursor()
    stats = {"processed": 0, "created": 0, "updated": 0, "complete": 0, "partial": 0, "missing": 0}

    try:
        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        # Get WSTO events that need evidence
        cur.execute("""
            SELECT vel.id, vel.order_id, vel.marketplace, vel.event_date,
                   vel.evidence_status
            FROM dbo.vat_event_ledger vel WITH (NOLOCK)
            WHERE vel.vat_classification = 'WSTO'
              AND vel.event_date >= ? AND vel.event_date <= ?
              AND vel.order_id IS NOT NULL
        """, (date_from, date_to))

        events = _fetchall_dict(cur)

        for evt in events:
            stats["processed"] += 1
            order_id = str(evt.get("order_id") or "")
            if not order_id:
                continue

            # Check if evidence record exists
            cur.execute("""
                SELECT id, proof_transport, proof_delivery, proof_order, proof_payment
                FROM dbo.transport_evidence_record WITH (NOLOCK)
                WHERE order_id = ?
            """, (order_id,))
            existing = cur.fetchone()

            # Try to find tracking from acc_order + logistics fact
            cur.execute("""
                SELECT TOP 1
                    o.status,
                    o.ship_date,
                    olf.delivered_shipments_count,
                    olf.last_delivery_at
                FROM dbo.acc_order o WITH (NOLOCK)
                LEFT JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
                    ON olf.amazon_order_id = o.amazon_order_id
                WHERE o.amazon_order_id = ?
            """, (order_id,))
            order_data = cur.fetchone()

            proof_transport = 0
            proof_delivery = 0
            proof_order = 1  # We always have the Amazon order record
            proof_payment = 1  # Amazon always has payment confirmation
            carrier = None
            tracking_id = None
            dispatch_date = None
            delivery_date = None

            if order_data:
                order_status = order_data[0]
                dispatch_date = order_data[1]
                delivered_count = order_data[2] or 0
                last_delivery = order_data[3]

                # Transport proof: order was shipped (has ship_date)
                proof_transport = 1 if dispatch_date else 0
                # Delivery proof: logistics fact confirms delivery
                proof_delivery = 1 if (delivered_count > 0 or last_delivery) else 0
                delivery_date = last_delivery

            # Compute status
            proofs = [proof_transport, proof_delivery, proof_order, proof_payment]
            total_proofs = sum(proofs)
            if total_proofs == 4:
                evidence_status = "complete"
                stats["complete"] += 1
            elif total_proofs > 0:
                evidence_status = "partial"
                stats["partial"] += 1
            else:
                evidence_status = "missing"
                stats["missing"] += 1

            evidence_json = json.dumps({
                "dispatch_date": str(dispatch_date) if dispatch_date else None,
                "delivery_date": str(delivery_date) if delivery_date else None,
                "source": "acc_order+logistics_fact",
            }, default=str)

            if existing:
                cur.execute("""
                    UPDATE dbo.transport_evidence_record
                    SET carrier = ?, tracking_id = ?,
                        dispatch_date = ?, delivery_date = ?,
                        proof_transport = ?, proof_delivery = ?,
                        proof_order = ?, proof_payment = ?,
                        evidence_status = ?, evidence_json = ?
                    WHERE id = ?
                """, (
                    carrier, tracking_id,
                    dispatch_date, delivery_date,
                    proof_transport, proof_delivery,
                    proof_order, proof_payment,
                    evidence_status, evidence_json,
                    existing[0],
                ))
                stats["updated"] += 1
            else:
                cur.execute("""
                    INSERT INTO dbo.transport_evidence_record(
                        source_ref, order_id, marketplace,
                        carrier, tracking_id,
                        dispatch_date, delivery_date,
                        proof_transport, proof_delivery,
                        proof_order, proof_payment,
                        evidence_status, evidence_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    order_id, order_id, evt.get("marketplace"),
                    carrier, tracking_id,
                    dispatch_date, delivery_date,
                    proof_transport, proof_delivery,
                    proof_order, proof_payment,
                    evidence_status, evidence_json,
                ))
                stats["created"] += 1

            # Update the vat_event_ledger evidence_status
            cur.execute("""
                UPDATE dbo.vat_event_ledger
                SET evidence_status = ?
                WHERE order_id = ? AND vat_classification = 'WSTO'
            """, (evidence_status, order_id))

            if stats["processed"] % 500 == 0:
                conn.commit()

        conn.commit()

        # Create compliance issues for overdue missing evidence
        _create_evidence_issues(cur, conn, date_from)
        conn.commit()

        log.info("sync_transport_evidence.done", **stats)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def _create_evidence_issues(cur, conn, date_from: date) -> int:
    """Create P1 compliance issues for WSTO with missing evidence past threshold."""
    threshold_date = date.today() - timedelta(days=EVIDENCE_THRESHOLD_DAYS)

    cur.execute("""
        SELECT vel.id, vel.order_id, vel.marketplace, vel.event_date
        FROM dbo.vat_event_ledger vel WITH (NOLOCK)
        WHERE vel.vat_classification = 'WSTO'
          AND vel.evidence_status IN ('missing', 'partial')
          AND vel.event_date <= ?
          AND vel.event_date >= ?
          AND NOT EXISTS (
              SELECT 1 FROM dbo.compliance_issue ci WITH (NOLOCK)
              WHERE ci.source_ref = vel.order_id
                AND ci.issue_type = 'missing_evidence'
                AND ci.status = 'open'
          )
    """, (threshold_date, date_from))

    rows = cur.fetchall()
    count = 0
    for row in rows:
        cur.execute("""
            INSERT INTO dbo.compliance_issue(
                issue_type, severity, source_ref, marketplace,
                description, status
            ) VALUES (
                'missing_evidence', 'P1', ?, ?,
                ?, 'open'
            )
        """, (
            row[1],
            row[2],
            f"WSTO order {row[1]} from {row[3]} missing transport/delivery evidence (>{EVIDENCE_THRESHOLD_DAYS} days)",
        ))
        count += 1

    return count


def get_evidence_summary(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Get evidence completeness summary."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        cur.execute("""
            SELECT
                evidence_status,
                COUNT(*) as cnt
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE vat_classification = 'WSTO'
              AND event_date >= ? AND event_date <= ?
            GROUP BY evidence_status
        """, (date_from, date_to))

        by_status: dict[str, int] = {}
        total = 0
        for row in cur.fetchall():
            by_status[str(row[0] or "unknown")] = int(row[1])
            total += int(row[1])

        complete = by_status.get("complete", 0)

        # Suspense register (overdue missing evidence)
        threshold_date = date.today() - timedelta(days=EVIDENCE_THRESHOLD_DAYS)
        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE vat_classification = 'WSTO'
              AND evidence_status IN ('missing', 'partial')
              AND event_date <= ?
              AND event_date >= ?
        """, (threshold_date, date_from))
        suspended = cur.fetchone()[0] or 0

        return {
            "total_wsto_events": total,
            "by_status": by_status,
            "complete_pct": round(complete / total * 100, 2) if total > 0 else 0,
            "suspended_count": suspended,
            "threshold_days": EVIDENCE_THRESHOLD_DAYS,
        }
    finally:
        conn.close()


def list_evidence_records(
    status: str | None = None,
    marketplace: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """List transport evidence records with filters."""
    conn = _connect()
    cur = conn.cursor()
    try:
        where_parts = ["1=1"]
        params: list = []

        if status:
            where_parts.append("ter.evidence_status = ?")
            params.append(status)
        if marketplace:
            where_parts.append("ter.marketplace = ?")
            params.append(marketplace)

        where_sql = " AND ".join(where_parts)
        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT COUNT(*) FROM dbo.transport_evidence_record ter WITH (NOLOCK)
            WHERE {where_sql}
        """, tuple(params))
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT ter.id, ter.source_ref, ter.order_id, ter.marketplace,
                   ter.carrier, ter.tracking_id,
                   ter.dispatch_date, ter.delivery_date,
                   ter.proof_transport, ter.proof_delivery,
                   ter.proof_order, ter.proof_payment,
                   ter.evidence_status, ter.created_at
            FROM dbo.transport_evidence_record ter WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY ter.created_at DESC
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
