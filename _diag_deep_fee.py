"""Deep investigation: where do Feb 2026 order fees come from?"""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()

# 1. Get a specific Feb order with referral_fee
print("=== 1. Sample Feb 2026 DE order with referral_fee ===")
cur.execute("""
    SELECT TOP 1
        o.amazon_order_id, o.marketplace_id, o.purchase_date,
        ol.sku, ol.referral_fee_pln, ol.fba_fee_pln, ol.item_price,
        o.fulfillment_channel, o.status, ol.id as line_id
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-02-15' AND o.purchase_date < '2026-02-20'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln > 0
    ORDER BY o.purchase_date DESC
""")
sample = cur.fetchone()
if sample:
    oid = sample[0]
    print(f"  order_id={oid}, mp={sample[1]}, date={sample[2]}")
    print(f"  sku={sample[3]}, ref_fee={sample[4]}, fba_fee={sample[5]}, price={sample[6]}")
    print(f"  fulfillment={sample[7]}, status={sample[8]}")
    
    # 2. Check ALL finance transactions for this order
    print(f"\n=== 2. ALL finance transactions for {oid} ===")
    cur.execute(f"""
        SELECT charge_type, amount, currency, posted_date, transaction_type, sku, synced_at
        FROM dbo.acc_finance_transaction WITH (NOLOCK)
        WHERE amazon_order_id = '{oid}'
        ORDER BY posted_date
    """)
    rows = cur.fetchall()
    print(f"  Found {len(rows)} finance transactions")
    for r in rows:
        print(f"  type={r[0]:30s} | amt={r[1]:>10.2f} {r[2]} | posted={r[3]} | txn_type={r[4]} | sku={r[5]} | synced={r[6]}")

# 3. Check if there's a pattern - maybe fees were stamped from Jan 2025 finance data?
print("\n=== 3. Finance transactions by posted_date month (all data) ===")
cur.execute("""
    SELECT 
        FORMAT(posted_date, 'yyyy-MM') as month,
        COUNT(*) as cnt,
        COUNT(DISTINCT amazon_order_id) as orders
    FROM dbo.acc_finance_transaction WITH (NOLOCK)
    WHERE amazon_order_id IS NOT NULL
    GROUP BY FORMAT(posted_date, 'yyyy-MM')
    ORDER BY month
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:>8,} txns, {r[2]:>6,} orders")

# 4. Check purchase_date range of orders matched by Jan 2025 finance data
print("\n=== 4. Purchase date range of orders with Jan 2025 finance matches ===")
cur.execute("""
    SELECT TOP 5
        MIN(o.purchase_date) as min_date,
        MAX(o.purchase_date) as max_date,
        COUNT(DISTINCT o.amazon_order_id) as order_count
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_finance_transaction ft WITH (NOLOCK) 
        ON ft.amazon_order_id = o.amazon_order_id
    WHERE ft.posted_date >= '2025-01-01' AND ft.posted_date < '2025-02-01'
""")
r = cur.fetchone()
if r:
    print(f"  Orders matched by Jan 2025 finance: {r[2]} orders, purchase dates {r[0]} to {r[1]}")

# 5. Check purchase_date range of orders with Mar 2026 finance matches
print("\n=== 5. Purchase date range of orders with Mar 2026 finance matches ===")
cur.execute("""
    SELECT
        MIN(o.purchase_date) as min_date,
        MAX(o.purchase_date) as max_date,
        COUNT(DISTINCT o.amazon_order_id) as order_count
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_finance_transaction ft WITH (NOLOCK)
        ON ft.amazon_order_id = o.amazon_order_id
    WHERE ft.posted_date >= '2026-03-01' AND ft.posted_date < '2026-04-01'
""")
r = cur.fetchone()
if r:
    print(f"  Orders matched by Mar 2026 finance: {r[2]} orders, purchase dates {r[0]} to {r[1]}")

# 6. How many Feb 2026 orders have fees?
print("\n=== 6. Feb 2026 fee coverage by marketplace ===")
cur.execute("""
    SELECT 
        o.marketplace_id,
        COUNT(DISTINCT o.amazon_order_id) as total_orders,
        COUNT(DISTINCT CASE WHEN ol.referral_fee_pln > 0 THEN o.amazon_order_id END) as with_ref_fee,
        COUNT(DISTINCT CASE WHEN ol.fba_fee_pln > 0 THEN o.amazon_order_id END) as with_fba_fee,
        COUNT(DISTINCT CASE WHEN EXISTS (
            SELECT 1 FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.amazon_order_id = o.amazon_order_id
        ) THEN o.amazon_order_id END) as with_finance
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-01'
      AND o.status = 'Shipped'
      AND ol.quantity_ordered > 0
    GROUP BY o.marketplace_id
    ORDER BY total_orders DESC
""")
for r in cur.fetchall():
    ref_pct = r[2]*100/max(r[1],1)
    fin_pct = r[4]*100/max(r[1],1)
    print(f"  {r[0]}: total={r[1]:>5}, with_ref={r[2]:>5} ({ref_pct:.1f}%), with_fba={r[3]:>5}, with_finance={r[4]:>5} ({fin_pct:.1f}%)")

conn.close()
