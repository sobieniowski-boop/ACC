"""
FIX: Remove hist_country_v1 logistics costs from FBA (AFN) orders.
FBA orders ship from Amazon warehouses — they should NOT have DHL/GLS costs from our warehouse.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=60)
cur = conn.cursor()

# 1. Count before
cur.execute("""
    SELECT COUNT(*), ISNULL(SUM(lf.total_logistics_pln), 0)
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE lf.calc_version = 'hist_country_v1'
      AND o.fulfillment_channel = 'AFN'
""")
row = cur.fetchone()
print(f"Before: {row[0]:,} AFN rows with hist_country_v1, total = {row[1]:,.0f} PLN")

if row[0] == 0:
    print("Nothing to delete.")
    conn.close()
    sys.exit(0)

# 2. Delete AFN rows
print("Deleting AFN hist_country_v1 rows...")
cur.execute("SET LOCK_TIMEOUT 30000")
cur.execute("""
    DELETE lf
    FROM dbo.acc_order_logistics_fact lf
    JOIN dbo.acc_order o ON o.amazon_order_id = lf.amazon_order_id
    WHERE lf.calc_version = 'hist_country_v1'
      AND o.fulfillment_channel = 'AFN'
""")
deleted = cur.rowcount
conn.commit()
print(f"Deleted: {deleted:,} rows")

# 3. Verify after
cur.execute("""
    SELECT o.fulfillment_channel, COUNT(*) cnt, SUM(lf.total_logistics_pln) total
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE lf.calc_version = 'hist_country_v1'
    GROUP BY o.fulfillment_channel
""")
print("\nAfter deletion:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,} orders, {r[2]:,.0f} PLN")

# 4. Also verify no AFN remain
cur.execute("""
    SELECT COUNT(*)
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE lf.calc_version = 'hist_country_v1'
      AND o.fulfillment_channel = 'AFN'
""")
remain = cur.fetchone()[0]
print(f"\nAFN rows remaining: {remain}")

cur.close()
conn.close()
print("Done.")
