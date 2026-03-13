"""Investigate fee source for February orders - are they from Reports API or Finance API?"""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()

# Check if referral_fee / fba_fee come from Reports API (order line data)
# vs Finance API (stamped from acc_finance_transaction)
print("=== Sample Feb orders with fees vs. their finance match ===")
cur.execute("""
    SELECT TOP 10
        o.amazon_order_id, o.marketplace_id, o.purchase_date,
        ol.sku, ol.referral_fee_pln, ol.fba_fee_pln, ol.item_price,
        (SELECT COUNT(*) FROM dbo.acc_finance_transaction ft WITH (NOLOCK) 
         WHERE ft.amazon_order_id = o.amazon_order_id) AS finance_count
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-02-15' AND o.purchase_date < '2026-02-20'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND o.status = 'Shipped'
      AND ol.quantity_ordered > 0
      AND ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln > 0
    ORDER BY o.purchase_date DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]:20s} | mp={row[1]} | date={row[2]} | sku={row[3]:20s} | ref={row[4]:>8.2f} | fba={row[5]} | price={row[6]} | fin_txns={row[7]}")

# Check order source - did they come from Reports API?
print("\n=== Order data_source field ===")
cur.execute("""
    SELECT DISTINCT
        ISNULL(data_source, '(null)') as src,
        COUNT(*) as cnt
    FROM dbo.acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-02-09' AND purchase_date < '2026-03-11'
    GROUP BY data_source
""")
for row in cur.fetchall():
    print(f"  source={row[0]:20s} | count={row[1]}")

# Check acc_order_line source for fees
print("\n=== Fee stamping check: Feb orders with fees but zero finance txns ===")
cur.execute("""
    SELECT COUNT(*) as with_fees_no_finance
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-02-09' AND o.purchase_date < '2026-03-01'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND o.status = 'Shipped'
      AND ol.quantity_ordered > 0
      AND ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln > 0
      AND NOT EXISTS (
          SELECT 1 FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
          WHERE ft.amazon_order_id = o.amazon_order_id
      )
""")
row = cur.fetchone()
print(f"  Lines with referral_fee but NO finance transactions: {row[0]}")

cur.execute("""
    SELECT COUNT(*) as total_feb
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-02-09' AND o.purchase_date < '2026-03-01'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND o.status = 'Shipped' AND ol.quantity_ordered > 0
""")
print(f"  Total Feb lines DE: {cur.fetchone()[0]}")

# Check backfill_via_reports.py impact - check if orders have commission/referral from Reports
print("\n=== Check if Reports API injects fees directly into order_lines ===")
cur.execute("""
    SELECT TOP 5
        o.amazon_order_id, ol.sku, ol.referral_fee_pln, ol.fba_fee_pln,
        ol.item_price, o.data_source
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-02-15' AND o.purchase_date < '2026-02-20'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND o.status = 'Shipped'
      AND ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln > 0
    ORDER BY NEWID()
""")
for row in cur.fetchall():
    print(f"  {row[0]:20s} | sku={row[1]:20s} | ref={row[2]:>8.2f} | fba={row[3]} | price={row[4]} | src={row[5]}")

# How are Finance API events matched?
print("\n=== Finance API: check events from Jan 2025 batch that match Feb 2026 orders ===")
cur.execute("""
    SELECT TOP 5
        ft.amazon_order_id, ft.charge_type, ft.posted_date, ft.amount, ft.marketplace_id
    FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
    WHERE ft.posted_date >= '2025-01-01' AND ft.posted_date < '2025-02-01'
      AND ft.amazon_order_id IN (
          SELECT TOP 100 o.amazon_order_id
          FROM dbo.acc_order o WITH (NOLOCK)
          WHERE o.purchase_date >= '2026-02-15' AND o.purchase_date < '2026-02-20'
            AND o.marketplace_id = 'A1PA6795UKMFR9'
      )
""")
rows = cur.fetchall()
print(f"  Jan 2025 finance events matching Feb 2026 orders: {len(rows)}")

conn.close()
