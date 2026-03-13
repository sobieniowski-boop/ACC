"""
Check: how many FBA orders got hist_country_v1 logistics costs?
FBA orders are fulfilled by Amazon — they should NOT get DHL/GLS shipping costs.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=60)
cur = conn.cursor()

# 1. Check how fulfillment_channel is distributed in acc_order
print("=== 1. fulfillment_channel distribution ===")
cur.execute("""
    SELECT fulfillment_channel, COUNT(*) cnt
    FROM dbo.acc_order WITH (NOLOCK)
    GROUP BY fulfillment_channel
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,}")

# 2. How many hist_country_v1 logistics rows are on FBA orders?
print("\n=== 2. hist_country_v1 logistics by fulfillment_channel ===")
cur.execute("""
    SELECT o.fulfillment_channel, COUNT(*) cnt, SUM(lf.total_cost_pln) total_pln
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE lf.calc_version = 'hist_country_v1'
    GROUP BY o.fulfillment_channel
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    ch = row[0] or '(NULL)'
    print(f"  {ch}: {row[1]:,} orders, total={row[2]:,.0f} PLN")

# 3. Check FBA-specific: top SKUs with FBA logistics assigned
print("\n=== 3. Top 10 FBA SKUs with highest estimated logistics ===")
cur.execute("""
    SELECT TOP 10
        ol.seller_sku,
        COUNT(DISTINCT lf.amazon_order_id) ord_cnt,
        SUM(lf.total_cost_pln) total_pln
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE lf.calc_version = 'hist_country_v1'
      AND o.fulfillment_channel = 'AFN'
    GROUP BY ol.seller_sku
    ORDER BY SUM(lf.total_cost_pln) DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} orders, total={row[2]:,.0f} PLN")

# 4. How many FBA vs FBM in Feb-Mar 2026?
print("\n=== 4. Feb-Mar 2026 orders by fulfillment_channel ===")
cur.execute("""
    SELECT fulfillment_channel, COUNT(*) cnt
    FROM dbo.acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-02-01' AND purchase_date < '2026-04-01'
      AND status NOT IN ('Cancelled','Canceled')
    GROUP BY fulfillment_channel
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    ch = row[0] or '(NULL)'
    print(f"  {ch}: {row[1]:,}")

# 5. Check what calc_versions exist for FBA orders (should they have any logistics?)
print("\n=== 5. All calc_versions for FBA orders ===")
cur.execute("""
    SELECT lf.calc_version, COUNT(*) cnt, SUM(lf.total_cost_pln) total
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE o.fulfillment_channel = 'AFN'
    GROUP BY lf.calc_version
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} orders, total={row[2]:,.0f} PLN")

cur.close()
conn.close()
print("\nDone.")
