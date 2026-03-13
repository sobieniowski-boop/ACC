"""Finance date range diagnostics."""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()

# Overall range
cur.execute("SELECT MIN(posted_date), MAX(posted_date), COUNT(*) FROM dbo.acc_finance_transaction WITH (NOLOCK)")
row = cur.fetchone()
print(f"Finance transactions: min={row[0]}, max={row[1]}, count={row[2]}")

# Monthly breakdown
cur.execute("""
    SELECT 
        FORMAT(posted_date, 'yyyy-MM') AS month,
        COUNT(*) AS cnt
    FROM dbo.acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2025-01-01'
    GROUP BY FORMAT(posted_date, 'yyyy-MM')
    ORDER BY month
""")
print("\n=== MONTHLY FINANCE TRANSACTION VOLUME ===")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:>8d} transactions")

# Weekly breakdown for Feb-Mar 2026
cur.execute("""
    SELECT 
        DATEPART(ISO_WEEK, posted_date) AS week,
        MIN(CAST(posted_date AS DATE)) AS min_date,
        MAX(CAST(posted_date AS DATE)) AS max_date,
        COUNT(*) AS cnt,
        COUNT(DISTINCT marketplace_id) AS marketplaces
    FROM dbo.acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2026-02-01'
    GROUP BY DATEPART(ISO_WEEK, posted_date)
    ORDER BY week
""")
print("\n=== WEEKLY FINANCE BREAKDOWN (Feb-Mar 2026) ===")
for row in cur.fetchall():
    print(f"  Week {row[0]:2d}: {row[1]} - {row[2]} | {row[3]:>6d} txns | {row[4]} marketplaces")

# Check marketplace distribution for Feb 2026
cur.execute("""
    SELECT 
        marketplace_id,
        COUNT(*) AS cnt
    FROM dbo.acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2026-02-01' AND posted_date < '2026-03-01'
    GROUP BY marketplace_id
    ORDER BY cnt DESC
""")
print("\n=== FEB 2026 FINANCE TRANSACTIONS BY MARKETPLACE ===")
for row in cur.fetchall():
    print(f"  {str(row[0] or '(null)'):20s}: {row[1]:>6d}")

# Check whether fee stamping worked for February orders
cur.execute("""
    SELECT 
        o.marketplace_id,
        COUNT(*) AS total_lines,
        SUM(CASE WHEN ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln != 0 THEN 1 ELSE 0 END) AS with_ref
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-02-09' AND o.purchase_date < '2026-02-28'
      AND o.status = 'Shipped'
      AND ol.quantity_ordered > 0
    GROUP BY o.marketplace_id
    ORDER BY total_lines DESC
""")
print("\n=== FEB 2026 ORDERS FEE COVERAGE ===")
for row in cur.fetchall():
    mp, total, ref = row
    pct = ref / total * 100 if total else 0
    print(f"  {mp:20s} | lines={total:>5d} | ref_fee={pct:5.1f}%")

conn.close()
