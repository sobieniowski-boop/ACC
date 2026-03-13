"""
Amazon Clearing & Reconciliation — settlement decomposition.

Decomposes Amazon payouts into:
  gross_sales, vat_oss, vat_local, amazon_fees, refunds, ads, payout_net
Highlights mismatches between expected and actual amounts.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog

from app.core.db_connection import connect_acc
from app.core.config import MARKETPLACE_REGISTRY

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


# ═══════════════════════════════════════════════════════════════════
# Reconcile settlements
# ═══════════════════════════════════════════════════════════════════

def reconcile_amazon_clearing(
    days_back: int = 90,
) -> dict[str, Any]:
    """
    Build reconciliation records from acc_finance_transaction grouped by settlement.

    Each settlement is decomposed into revenue, tax, fees, refunds, ads components.
    """
    conn = _connect()
    cur = conn.cursor()
    stats = {"settlements": 0, "matched": 0, "partial": 0, "mismatch": 0, "errors": 0}

    try:
        cutoff = date.today() - timedelta(days=days_back)

        # Group finance transactions by settlement
        cur.execute("""
            SELECT
                ISNULL(ft.settlement_id, ft.financial_event_group_id) as sid,
                MIN(CAST(ft.posted_date AS DATE)) as settlement_date,
                SUM(CASE WHEN ft.charge_type = 'Principal' AND ft.transaction_type = 'ShipmentEventList'
                    THEN ft.amount_pln ELSE 0 END) as gross_sales,
                SUM(CASE WHEN ft.charge_type = 'Tax' AND vel.vat_classification = 'WSTO'
                    THEN ft.amount_pln ELSE 0 END) as vat_oss,
                SUM(CASE WHEN ft.charge_type = 'Tax' AND (vel.vat_classification = 'LOCAL_VAT' OR vel.vat_classification IS NULL)
                    THEN ft.amount_pln ELSE 0 END) as vat_local,
                SUM(CASE WHEN ft.charge_type IN ('Commission', 'FBAPerUnitFulfillmentFee',
                    'FBAStorageFee', 'FBARemovalFee', 'FBADisposalFee',
                    'DigitalServicesFee', 'DigitalServicesFeeFBA',
                    'CustomerReturnHRRUnitFee', 'AmazonForAllFee',
                    'VineFee', 'Subscription', 'PaidServicesFee')
                    THEN ft.amount_pln ELSE 0 END) as amazon_fees,
                SUM(CASE WHEN ft.transaction_type = 'RefundEventList'
                    THEN ft.amount_pln ELSE 0 END) as refunds,
                SUM(CASE WHEN ft.charge_type IN ('ShippingHB', 'ShippingChargeback',
                    'ReturnPostageBilling_Postage', 'ReturnPostageBilling_VAT')
                    THEN ft.amount_pln ELSE 0 END) as shipping_costs,
                SUM(ft.amount_pln) as payout_net
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            LEFT JOIN dbo.vat_event_ledger vel WITH (NOLOCK)
                ON vel.order_id = ft.amazon_order_id
                AND vel.event_type = 'sale'
            WHERE CAST(ft.posted_date AS DATE) >= ?
              AND ISNULL(ft.settlement_id, ft.financial_event_group_id) IS NOT NULL
            GROUP BY ISNULL(ft.settlement_id, ft.financial_event_group_id)
        """, (cutoff,))

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []

        for row in rows:
            rec = dict(zip(cols, row))
            stats["settlements"] += 1

            try:
                sid = str(rec.get("sid") or "")
                gross = _to_float(rec.get("gross_sales"))
                vat_oss = _to_float(rec.get("vat_oss"))
                vat_local = _to_float(rec.get("vat_local"))
                fees = _to_float(rec.get("amazon_fees"))
                refunds_val = _to_float(rec.get("refunds"))
                shipping = _to_float(rec.get("shipping_costs"))
                payout = _to_float(rec.get("payout_net"))

                expected_net = gross + vat_oss + vat_local + fees + refunds_val + shipping
                diff = abs(payout - expected_net)

                if diff < 0.01:
                    status = "matched"
                    stats["matched"] += 1
                elif diff < abs(payout * 0.01):
                    status = "partial"
                    stats["partial"] += 1
                else:
                    status = "mismatch"
                    stats["mismatch"] += 1

                # Upsert
                cur.execute("""
                    MERGE dbo.amazon_clearing_reconciliation AS tgt
                    USING (SELECT ? AS sid) AS src ON tgt.settlement_id = src.sid
                    WHEN MATCHED THEN
                        UPDATE SET
                            settlement_date = ?,
                            gross_sales = ?, vat_oss = ?, vat_local = ?,
                            amazon_fees = ?, refunds = ?, ads = ?,
                            payout_net = ?, expected_net = ?,
                            difference_amount = ?, status = ?
                    WHEN NOT MATCHED THEN
                        INSERT (settlement_id, settlement_date,
                                gross_sales, vat_oss, vat_local,
                                amazon_fees, refunds, ads,
                                payout_net, expected_net,
                                difference_amount, status)
                        VALUES (?, ?,
                                ?, ?, ?,
                                ?, ?, ?,
                                ?, ?,
                                ?, ?);
                """, (
                    sid,
                    rec.get("settlement_date"),
                    gross, vat_oss, vat_local,
                    fees, refunds_val, shipping,
                    payout, expected_net,
                    payout - expected_net, status,
                    sid, rec.get("settlement_date"),
                    gross, vat_oss, vat_local,
                    fees, refunds_val, shipping,
                    payout, expected_net,
                    payout - expected_net, status,
                ))

            except Exception as e:
                stats["errors"] += 1
                log.warning("reconcile_clearing.error", sid=rec.get("sid"), error=str(e))

            if stats["settlements"] % 100 == 0:
                conn.commit()

        conn.commit()
        log.info("reconcile_amazon_clearing.done", **stats)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


# ═══════════════════════════════════════════════════════════════════
# Queries
# ═══════════════════════════════════════════════════════════════════

def get_reconciliation_summary() -> dict[str, Any]:
    """Summary of settlement reconciliation status."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                status,
                COUNT(*) as cnt,
                SUM(ABS(difference_amount)) as total_diff,
                SUM(payout_net) as total_payout
            FROM dbo.amazon_clearing_reconciliation WITH (NOLOCK)
            GROUP BY status
        """)
        by_status = _fetchall_dict(cur)

        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(gross_sales) as total_gross,
                   SUM(amazon_fees) as total_fees,
                   SUM(refunds) as total_refunds,
                   SUM(payout_net) as total_payout,
                   SUM(ABS(difference_amount)) as total_diff
            FROM dbo.amazon_clearing_reconciliation WITH (NOLOCK)
        """)
        totals = _fetchall_dict(cur)

        return {
            "by_status": by_status,
            "totals": totals[0] if totals else {},
        }
    finally:
        conn.close()


def list_reconciliations(
    status: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """List reconciliation records."""
    conn = _connect()
    cur = conn.cursor()
    try:
        where_parts = ["1=1"]
        params: list = []

        if status:
            where_parts.append("r.status = ?")
            params.append(status)

        where_sql = " AND ".join(where_parts)
        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT COUNT(*) FROM dbo.amazon_clearing_reconciliation r WITH (NOLOCK)
            WHERE {where_sql}
        """, tuple(params))
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT r.id, r.settlement_id, r.settlement_date,
                   r.gross_sales, r.vat_oss, r.vat_local,
                   r.amazon_fees, r.refunds, r.ads,
                   r.payout_net, r.expected_net,
                   r.difference_amount, r.status
            FROM dbo.amazon_clearing_reconciliation r WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY r.settlement_date DESC
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
