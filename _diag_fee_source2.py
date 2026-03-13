"""Deeper investigation: where do Feb 2026 fees come from?"""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()

# Check if Jan 2025 finance data matches Feb 2026 orders
# The Jan 2025 data (170K) might actually be from Jan 2026!
print("=== Jan 2025 finance data date range check ===")
cur.execute("""
    SELECT MIN(posted_date), MAX(posted_date), COUNT(*) 
    FROM dbo.acc_finance_transaction WITH (NOLOCK) 
    WHERE posted_date >= '2025-01-01' AND posted_date < '2025-02-01'
""")
row = cur.fetchone()
print(f"  Jan 2025: {row[0]} to {row[1]} count={row[2]}")

# Maybe the data is from Jan 2026?
cur.execute("""
    SELECT MIN(posted_date), MAX(posted_date), COUNT(*) 
    FROM dbo.acc_finance_transaction WITH (NOLOCK) 
    WHERE posted_date >= '2026-01-01' AND posted_date < '2026-02-01'
""")
row = cur.fetchone()
print(f"  Jan 2026: {row[0]} to {row[1]} count={row[2]}")

# Full year distribution
cur.execute("""
    SELECT YEAR(posted_date) as yr, MONTH(posted_date) as mo, COUNT(*) as cnt
    FROM dbo.acc_finance_transaction WITH (NOLOCK) 
    GROUP BY YEAR(posted_date), MONTH(posted_date)
    ORDER BY yr, mo
""")
print("\n=== FULL YEAR/MONTH DISTRIBUTION ===")
for row in cur.fetchall():
    print(f"  {row[0]}-{row[1]:02d}: {row[2]:>8d}")

# Check what amazon_order_ids in Jan 2025 finance look like
print("\n=== Sample Jan 2025 finance order IDs ===")
cur.execute("""
    SELECT TOP 5 amazon_order_id, charge_type, posted_date, amount, marketplace_id
    FROM dbo.acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2025-01-01' AND posted_date < '2025-02-01'
      AND amazon_order_id IS NOT NULL
    ORDER BY posted_date
""")
for row in cur.fetchall():
    print(f"  order={row[0]} | charge={row[1]} | date={row[2]} | amount={row[3]} | mp={row[4]}")

# Check how Feb 2026 order fees match
print("\n=== Do Jan 2025 finance events match any Feb 2026 orders? ===")
cur.execute("""
    SELECT COUNT(DISTINCT ft.amazon_order_id) as matched
    FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
    INNER JOIN dbo.acc_order o WITH (NOLOCK) ON ft.amazon_order_id = o.amazon_order_id
    WHERE ft.posted_date >= '2025-01-01' AND ft.posted_date < '2025-02-01'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-01'
""")
row = cur.fetchone()
print(f"  Jan 2025 events matching Feb 2026 orders: {row[0]}")

# The real question: check the order pipeline step 10 source
# Check what finance data was actually used for fee stamping
print("\n=== All finance data matching Feb 2026 DE orders ===")
cur.execute("""
    SELECT COUNT(DISTINCT ft.amazon_order_id) as matched, 
           MIN(ft.posted_date) as min_posted,
           MAX(ft.posted_date) as max_posted
    FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
    INNER JOIN dbo.acc_order o WITH (NOLOCK) ON ft.amazon_order_id = o.amazon_order_id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-01'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
""")
row = cur.fetchone()
print(f"  Matched orders: {row[0]}, posted range: {row[1]} to {row[2]}")

# Check fee_agg temp table source - is it the Finances API or something else?
# Check if there's a separate table for reports-based fees
print("\n=== Check for alternative fee tables ===")
cur.execute("""
    SELECT t.name 
    FROM sys.tables t 
    WHERE t.name LIKE '%fee%' OR t.name LIKE '%referral%' OR t.name LIKE '%commission%'
    ORDER BY t.name
""")
for row in cur.fetchall():
    print(f"  {row[0]}")

# Check if order_line has a fee_source column or similar
print("\n=== acc_order_line columns ===")
cur.execute("""
    SELECT c.name 
    FROM sys.columns c 
    JOIN sys.tables t ON t.object_id = c.object_id 
    WHERE t.name = 'acc_order_line'
    ORDER BY c.column_id
""")
for row in cur.fetchall():
    print(f"  {row[0]}")

conn.close()
