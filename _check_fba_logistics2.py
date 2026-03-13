"""
Check: how many FBA (AFN) orders got hist_country_v1 logistics costs?
FBA orders are fulfilled by Amazon — they should NOT get DHL/GLS shipping costs.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=60)
cur = conn.cursor()

# 0. Get columns of acc_order_logistics_fact
print("=== 0. acc_order_logistics_fact columns ===")
cur.execute("""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'acc_order_logistics_fact'
    ORDER BY ORDINAL_POSITION
""")
cols = [r[0] for r in cur.fetchall()]
print("  ", cols)

# 1. fulfillment_channel distribution (all orders)
print("\n=== 1. fulfillment_channel distribution (all orders) ===")
cur.execute("""
    SELECT fulfillment_channel, COUNT(*) cnt
    FROM dbo.acc_order WITH (NOLOCK)
    GROUP BY fulfillment_channel
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    print(f"  {row[0] or '(NULL)'}: {row[1]:,}")

# 2. hist_country_v1 logistics by fulfillment_channel
print("\n=== 2. hist_country_v1 logistics by fulfillment_channel ===")
cur.execute("""
    SELECT o.fulfillment_channel, COUNT(*) cnt, SUM(lf.total_logistics_pln) total_pln
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE lf.calc_version = 'hist_country_v1'
    GROUP BY o.fulfillment_channel
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    ch = row[0] or '(NULL)'
    print(f"  {ch}: {row[1]:,} orders, total={row[2]:,.0f} PLN")

# 3. Feb-Mar 2026 by fulfillment_channel
print("\n=== 3. Feb-Mar 2026 orders by fulfillment_channel ===")
cur.execute("""
    SELECT fulfillment_channel, COUNT(*) cnt
    FROM dbo.acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-02-01' AND purchase_date < '2026-04-01'
      AND status NOT IN ('Cancelled','Canceled')
    GROUP BY fulfillment_channel
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    print(f"  {row[0] or '(NULL)'}: {row[1]:,}")

# 4. All calc_versions for FBA (AFN) orders
print("\n=== 4. All calc_versions for AFN orders ===")
cur.execute("""
    SELECT lf.calc_version, COUNT(*) cnt, SUM(lf.total_logistics_pln) total_pln
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE o.fulfillment_channel = 'AFN'
    GROUP BY lf.calc_version
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} orders, total={row[2]:,.0f} PLN")

# 5. Top FBA SKUs with highest estimated logistics  
print("\n=== 5. Top 15 FBA SKUs with highest estimated logistics ===")
cur.execute("""
    SELECT TOP 15
        ol.seller_sku,
        o.ship_country,
        COUNT(DISTINCT lf.amazon_order_id) ord_cnt,
        SUM(lf.total_logistics_pln) total_pln
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE lf.calc_version = 'hist_country_v1'
      AND o.fulfillment_channel = 'AFN'
    GROUP BY ol.seller_sku, o.ship_country
    ORDER BY SUM(lf.cost_pln) DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]} ({row[1]}): {row[2]:,} orders, total={row[3]:,.0f} PLN")

# 6. Screenshot data - check the SKUs from user's screenshot
print("\n=== 6. Check screenshot SKUs - are they FBA? ===")
screenshot_skus = [
    'FBA_5902730380030', 'FBA_5903699415252', 'FBA_5902730382157',
    'FBA_5903699425657', 'FBA_5902730382089', 'FBA_5902730382102',
    'FBA_5902730380030', 'FBA_5903699467930', 'FBA_5902730382072',
    'FBA_5902730382126', 'FBA_5903699457245', 'FBA_5902730382003'
]
unique_skus = list(set(screenshot_skus))
placeholders = ','.join(['?' for _ in unique_skus])
cur.execute(f"""
    SELECT ol.seller_sku, o.fulfillment_channel, COUNT(DISTINCT o.amazon_order_id) cnt
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE ol.seller_sku IN ({placeholders})
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
    GROUP BY ol.seller_sku, o.fulfillment_channel
    ORDER BY ol.seller_sku, o.fulfillment_channel
""", unique_skus)
for row in cur.fetchall():
    print(f"  {row[0]} | {row[1]}: {row[2]:,} orders")

cur.close()
conn.close()
print("\nDone.")
