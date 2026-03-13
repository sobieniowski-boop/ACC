import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
conn = connect_acc(timeout=30)
cur = conn.cursor()
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_order_line' ORDER BY ORDINAL_POSITION")
print("acc_order_line columns:", [r[0] for r in cur.fetchall()])

# Also check if screenshot SKUs are AFN
print("\n--- Screenshot SKUs check ---")
skus = ['FBA_5902730380030','FBA_5903699415252','FBA_5902730382157','FBA_5903699425657',
        'FBA_5902730382089','FBA_5902730382102','FBA_5903699467930','FBA_5902730382072',
        'FBA_5902730382126','FBA_5903699457245','FBA_5902730382003']
placeholders = ','.join(['?' for _ in skus])
cur.execute(f"""
    SELECT ol.sku, o.fulfillment_channel, COUNT(DISTINCT o.amazon_order_id) cnt
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE ol.sku IN ({placeholders})
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
    GROUP BY ol.sku, o.fulfillment_channel
    ORDER BY ol.sku, o.fulfillment_channel
""", skus)
for row in cur.fetchall():
    print(f"  {row[0]} | {row[1]}: {row[2]} orders")
conn.close()
