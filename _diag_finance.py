"""Check finance transaction coverage per marketplace."""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()

# Finance transaction coverage by marketplace vs orders
sql = """
SELECT
    o.marketplace_id,
    COUNT(DISTINCT o.amazon_order_id) AS total_orders,
    COUNT(DISTINCT ft.amazon_order_id) AS orders_with_finance,
    COUNT(DISTINCT CASE WHEN ft.charge_type IN ('Commission','ReferralFee') THEN ft.amazon_order_id END) AS orders_with_referral,
    COUNT(DISTINCT CASE WHEN ft.charge_type LIKE 'FBA%' THEN ft.amazon_order_id END) AS orders_with_fba_fee
FROM dbo.acc_order o WITH (NOLOCK)
LEFT JOIN dbo.acc_finance_transaction ft WITH (NOLOCK)
    ON ft.amazon_order_id = o.amazon_order_id
WHERE o.purchase_date >= '2026-02-09' AND o.purchase_date < '2026-03-11'
  AND o.status = 'Shipped'
  AND o.amazon_order_id NOT LIKE 'S02-%%'
  AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
GROUP BY o.marketplace_id
ORDER BY total_orders DESC
"""
cur.execute(sql)
print("=== FINANCE TRANSACTION COVERAGE BY MARKETPLACE ===")
print(f"  {'MP_ID':20s} | {'Orders':>7s} | {'w/Finance':>9s} | {'%':>5s} | {'w/Referral':>10s} | {'%':>5s} | {'w/FBA':>6s} | {'%':>5s}")
for row in cur.fetchall():
    mp, total, w_fin, w_ref, w_fba = row
    fin_pct = w_fin / total * 100 if total else 0
    ref_pct = w_ref / total * 100 if total else 0
    fba_pct = w_fba / total * 100 if total else 0
    print(f"  {mp:20s} | {total:>7d} | {w_fin:>9d} | {fin_pct:5.1f} | {w_ref:>10d} | {ref_pct:5.1f} | {w_fba:>6d} | {fba_pct:5.1f}")

# Check finance sync state
print()
print("=== FINANCE SYNC STATE ===")
sql2 = """
SELECT marketplace_id, last_updated, last_posted_date, status
FROM dbo.acc_finance_sync_state WITH (NOLOCK)
ORDER BY marketplace_id
"""
try:
    cur.execute(sql2)
    for row in cur.fetchall():
        print(f"  {row[0]:20s} | updated={row[1]} | last_posted={row[2]} | status={row[3]}")
except Exception as e:
    print(f"  Table not found or error: {e}")

# Finance coverage gap diagnostics  
print()
print("=== FINANCE TRANSACTIONS BY MARKETPLACE (30d) ===")
sql3 = """
SELECT
    ft.marketplace_id,
    ft.transaction_type,
    COUNT(*) AS cnt,
    MIN(ft.posted_date) AS min_date,
    MAX(ft.posted_date) AS max_date
FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
WHERE ft.posted_date >= '2026-02-08'
GROUP BY ft.marketplace_id, ft.transaction_type
ORDER BY ft.marketplace_id, ft.transaction_type
"""
cur.execute(sql3)
for row in cur.fetchall():
    print(f"  {str(row[0] or '(null)'):20s} | {str(row[1]):30s} | cnt={row[2]:>6d} | {row[3]} to {row[4]}")

conn.close()
