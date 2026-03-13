"""Time individual subquery components."""
import os, sys, time
sys.path.insert(0, r'C:\ACC\apps\api')
os.chdir(r'C:\ACC\apps\api')
from dotenv import load_dotenv
load_dotenv(r'C:\ACC\.env')
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=120)
cur = conn.cursor()

date_from = '2026-02-05'
date_to = '2026-03-07'

# 1. Basic order+line scan
t0 = time.time()
cur.execute("""
    SELECT COUNT(*) AS cnt
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.status = 'Shipped'
      AND o.purchase_date >= CAST(? AS DATE)
      AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
""", (date_from, date_to))
r = cur.fetchone()
print(f"1. Basic scan: {time.time()-t0:.1f}s — {r[0]} lines")

# 2. + registry LEFT JOIN
t0 = time.time()
cur.execute("""
    SELECT COUNT(*) AS cnt
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
    LEFT JOIN (
        SELECT merchant_sku, MAX(parent_asin) AS parent_asin
        FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
        WHERE parent_asin IS NOT NULL AND parent_asin != ''
        GROUP BY merchant_sku
    ) reg ON reg.merchant_sku = ol.sku
    WHERE o.status = 'Shipped'
      AND o.purchase_date >= CAST(? AS DATE)
      AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
""", (date_from, date_to))
r = cur.fetchone()
print(f"2. + reg+product: {time.time()-t0:.1f}s — {r[0]} lines")

# 3. + fx OUTER APPLY
t0 = time.time()
cur.execute("""
    SELECT COUNT(*) AS cnt, SUM(ISNULL(fx.rate_to_pln, 1.0)) AS fx_sum
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    OUTER APPLY (
        SELECT TOP 1 rate_to_pln
        FROM dbo.acc_exchange_rate er WITH (NOLOCK)
        WHERE er.currency = o.currency
          AND er.rate_date <= o.purchase_date
        ORDER BY er.rate_date DESC
    ) fx
    WHERE o.status = 'Shipped'
      AND o.purchase_date >= CAST(? AS DATE)
      AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
""", (date_from, date_to))
r = cur.fetchone()
print(f"3. + fx OUTER APPLY: {time.time()-t0:.1f}s")

# 4. olt subquery alone
t0 = time.time()
cur.execute("""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT ol2.order_id,
            ISNULL(SUM(ISNULL(ol2.item_price, 0)), 0) AS order_line_total
        FROM dbo.acc_order_line ol2 WITH (NOLOCK)
        JOIN dbo.acc_order o2 WITH (NOLOCK) ON o2.id = ol2.order_id
        WHERE o2.status = 'Shipped'
          AND o2.purchase_date >= CAST(? AS DATE)
          AND o2.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
        GROUP BY ol2.order_id
    ) olt
""", (date_from, date_to))
r = cur.fetchone()
print(f"4. olt subquery: {time.time()-t0:.1f}s — {r[0]} orders")

# 5. fin with full scan + hash
t0 = time.time()
cur.execute("""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT ft.amazon_order_id, ft.marketplace_id, COUNT_BIG(1) AS fin_rows
        FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
        JOIN dbo.acc_order o_f WITH (NOLOCK)
            ON ft.amazon_order_id = o_f.amazon_order_id
        WHERE o_f.status = 'Shipped'
          AND o_f.purchase_date >= CAST(? AS DATE)
          AND o_f.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
        GROUP BY ft.amazon_order_id, ft.marketplace_id
    ) fin
    OPTION (HASH JOIN)
""", (date_from, date_to))
r = cur.fetchone()
print(f"5. fin HASH JOIN: {time.time()-t0:.1f}s — {r[0]} groups")

# 5b. fin with separate Python approach
t0 = time.time()
cur.execute("""
    SELECT ft.amazon_order_id, ft.marketplace_id,
        COUNT_BIG(1) AS fin_rows,
        SUM(CASE WHEN ft.charge_type = 'ShippingCharge' AND ft.amount > 0
            THEN ISNULL(ft.amount_pln, ft.amount * 4.25)
            ELSE 0 END) AS shipping_charge_pln
    FROM dbo.acc_finance_transaction ft WITH (NOLOCK, INDEX(0))
    GROUP BY ft.amazon_order_id, ft.marketplace_id
    HAVING SUM(CASE WHEN ft.charge_type = 'ShippingCharge' AND ft.amount > 0 THEN 1 ELSE 0 END) > 0
""")
rows = cur.fetchall()
print(f"5b. fin full-scan shipping: {time.time()-t0:.1f}s — {len(rows)} rows with shipping")

# 6. Full query (simplified SELECT with all JOINs)
t0 = time.time()
cur.execute("""
    SELECT COUNT(*) AS cnt
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
    LEFT JOIN (
        SELECT merchant_sku, MAX(parent_asin) AS parent_asin
        FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
        WHERE parent_asin IS NOT NULL AND parent_asin != ''
        GROUP BY merchant_sku
    ) reg ON reg.merchant_sku = ol.sku
    OUTER APPLY (
        SELECT TOP 1 rate_to_pln
        FROM dbo.acc_exchange_rate er WITH (NOLOCK)
        WHERE er.currency = o.currency
          AND er.rate_date <= o.purchase_date
        ORDER BY er.rate_date DESC
    ) fx
    LEFT JOIN (
        SELECT ol2.order_id,
            ISNULL(SUM(ISNULL(ol2.item_price, 0)), 0) AS order_line_total,
            ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
        FROM dbo.acc_order_line ol2 WITH (NOLOCK)
        JOIN dbo.acc_order o2 WITH (NOLOCK) ON o2.id = ol2.order_id
        WHERE o2.status = 'Shipped'
          AND o2.purchase_date >= CAST(? AS DATE)
          AND o2.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
        GROUP BY ol2.order_id
    ) olt ON olt.order_id = o.id
    LEFT JOIN (
        SELECT ft.amazon_order_id, ft.marketplace_id,
            COUNT_BIG(1) AS fin_rows,
            SUM(CASE WHEN ft.charge_type = 'ShippingCharge' AND ft.amount > 0
                THEN ISNULL(ft.amount_pln, ft.amount * 4.25)
                ELSE 0 END) AS shipping_charge_pln
        FROM dbo.acc_order o_f WITH (NOLOCK)
        JOIN dbo.acc_finance_transaction ft WITH (NOLOCK)
            ON ft.amazon_order_id = o_f.amazon_order_id
        WHERE o_f.status = 'Shipped'
          AND o_f.purchase_date >= CAST(? AS DATE)
          AND o_f.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
        GROUP BY ft.amazon_order_id, ft.marketplace_id
    ) fin ON fin.amazon_order_id = o.amazon_order_id
         AND (fin.marketplace_id = o.marketplace_id OR fin.marketplace_id IS NULL)
    WHERE o.status = 'Shipped'
      AND o.purchase_date >= CAST(? AS DATE)
      AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
""", (date_from, date_to, date_from, date_to, date_from, date_to))
r = cur.fetchone()
print(f"6. Full combined: {time.time()-t0:.1f}s — {r[0]} lines")

conn.close()
