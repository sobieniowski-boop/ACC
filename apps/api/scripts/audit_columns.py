"""Check cost/fee columns in order tables."""
import sys
sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=60)
cur = conn.cursor()

cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'acc_order_line'
      AND (COLUMN_NAME LIKE '%fee%' OR COLUMN_NAME LIKE '%cost%' OR COLUMN_NAME LIKE '%cogs%'
           OR COLUMN_NAME LIKE '%refund%' OR COLUMN_NAME LIKE '%price%' OR COLUMN_NAME LIKE '%tax%'
           OR COLUMN_NAME LIKE '%pln%' OR COLUMN_NAME LIKE '%promo%' OR COLUMN_NAME LIKE '%shipping%'
           OR COLUMN_NAME LIKE '%discount%')
    ORDER BY ORDINAL_POSITION
""")
print("=== acc_order_line cost/fee columns ===")
for r in cur.fetchall():
    print(f"  {r[0]:45s} {r[1]}")

cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'acc_order'
      AND (COLUMN_NAME LIKE '%fee%' OR COLUMN_NAME LIKE '%cost%' OR COLUMN_NAME LIKE '%refund%'
           OR COLUMN_NAME LIKE '%pln%' OR COLUMN_NAME LIKE '%promo%' OR COLUMN_NAME LIKE '%shipping%'
           OR COLUMN_NAME LIKE '%discount%' OR COLUMN_NAME LIKE '%amount%')
    ORDER BY ORDINAL_POSITION
""")
print("\n=== acc_order cost/fee columns ===")
for r in cur.fetchall():
    print(f"  {r[0]:45s} {r[1]}")

# Check how fba_fee_pln and referral_fee_pln are calculated — are they from finance transactions?
cur.execute("""
    SELECT TOP 5 ol.fba_fee_pln, ol.referral_fee_pln, ol.cogs_pln, 
           ol.item_price, ol.item_tax, ol.promotion_discount
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    WHERE ol.fba_fee_pln IS NOT NULL AND ol.fba_fee_pln <> 0
""")
print("\n=== Sample order_line fee data ===")
cols = [c[0] for c in cur.description]
for r in cur.fetchall():
    print(dict(zip(cols, r)))

# Check what transaction_types & charge_types are NOT linked to any order
cur.execute("""
    SELECT ft.charge_type, ft.transaction_type, COUNT(*) as cnt, SUM(ft.amount) as total
    FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
    WHERE ft.amazon_order_id IS NULL OR LTRIM(RTRIM(ft.amazon_order_id)) = ''
    GROUP BY ft.charge_type, ft.transaction_type
    ORDER BY cnt DESC
""")
print("\n=== Finance transactions WITHOUT order_id ===")
for r in cur.fetchall():
    print(f"  {str(r[0] or ''):40s} {str(r[1] or ''):30s} cnt={r[2]:>6,} total={r[3]:>12,.2f}")

conn.close()
