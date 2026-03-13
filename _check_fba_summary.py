import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
conn = connect_acc(timeout=30)
cur = conn.cursor()

# 1. acc_order_line columns
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_order_line' ORDER BY ORDINAL_POSITION")
cols = [r[0] for r in cur.fetchall()]
print("acc_order_line cols:", cols)

# 2. Summary of the FBA problem
print("\n=== FBA LOGISTICS PROBLEM SUMMARY ===")
cur.execute("""
    SELECT 
        o.fulfillment_channel,
        COUNT(DISTINCT lf.amazon_order_id) ord_cnt,
        SUM(lf.total_logistics_pln) total_pln
    FROM dbo.acc_order_logistics_fact lf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = lf.amazon_order_id
    WHERE lf.calc_version = 'hist_country_v1'
    GROUP BY o.fulfillment_channel
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} orders, {row[2]:,.0f} PLN")

# 3. Verify the screenshot SKUs are all FBA
print("\n=== Screenshot SKUs - prefix FBA_ means AFN? ===")
cur.execute("""
    SELECT TOP 5 o.fulfillment_channel, COUNT(*) cnt
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE ol.seller_sku LIKE 'FBA_%'
    GROUP BY o.fulfillment_channel
""")
for row in cur.fetchall():
    print(f"  fulfillment_channel={row[0]}: {row[1]:,} lines")

conn.close()
