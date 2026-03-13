"""Check SKU coverage in ads product_day — root cause of 43% gap."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=30)
cur = c.cursor()

def q(sql, p=None):
    cur.execute(sql, p or [])
    return cur.fetchall()

# 1. SKU coverage in product_day
print("=== PRODUCT_DAY SKU COVERAGE (Jan 2026) ===")
r = q("""
    SELECT
      SUM(CASE WHEN sku IS NULL OR sku = '' THEN 1 ELSE 0 END) no_sku,
      SUM(CASE WHEN sku IS NOT NULL AND sku <> '' THEN 1 ELSE 0 END) has_sku,
      SUM(CAST(CASE WHEN sku IS NULL OR sku = '' THEN spend_pln ELSE 0 END AS FLOAT)) spend_no_sku,
      SUM(CAST(CASE WHEN sku IS NOT NULL AND sku <> '' THEN spend_pln ELSE 0 END AS FLOAT)) spend_has_sku
    FROM acc_ads_product_day WITH (NOLOCK)
    WHERE report_date >= '2026-01-01' AND report_date < '2026-02-01'
""")[0]
print(f"  no_sku={r[0]:,} rows ({float(r[2] or 0):,.0f} PLN) | has_sku={r[1]:,} rows ({float(r[3] or 0):,.0f} PLN)")

# 2. Same for Feb and Mar
for m_name, d1, d2 in [('Feb', '2026-02-01', '2026-03-01'), ('Mar', '2026-03-01', '2026-04-01')]:
    r = q(f"""
        SELECT
          SUM(CASE WHEN sku IS NULL OR sku = '' THEN 1 ELSE 0 END),
          SUM(CASE WHEN sku IS NOT NULL AND sku <> '' THEN 1 ELSE 0 END),
          SUM(CAST(CASE WHEN sku IS NULL OR sku = '' THEN spend_pln ELSE 0 END AS FLOAT)),
          SUM(CAST(CASE WHEN sku IS NOT NULL AND sku <> '' THEN spend_pln ELSE 0 END AS FLOAT))
        FROM acc_ads_product_day WITH (NOLOCK)
        WHERE report_date >= '{d1}' AND report_date < '{d2}'
    """)[0]
    print(f"  {m_name}: no_sku={int(r[0] or 0):,} ({float(r[2] or 0):,.0f} PLN) | has_sku={int(r[1] or 0):,} ({float(r[3] or 0):,.0f} PLN)")

# 3. SKU-matched rollup join check
print("\n=== ROLLUP MATCH BY SKU (Jan 2026) ===")
r = q("""
    SELECT 
        COUNT(*) total_ads_rows,
        SUM(CAST(a.spend_pln AS FLOAT)) total_spend,
        SUM(CASE WHEN r.sku IS NOT NULL THEN 1 ELSE 0 END) matched,
        SUM(CASE WHEN r.sku IS NOT NULL THEN CAST(a.spend_pln AS FLOAT) ELSE 0 END) matched_spend,
        SUM(CASE WHEN r.sku IS NULL THEN CAST(a.spend_pln AS FLOAT) ELSE 0 END) unmatched_spend
    FROM acc_ads_product_day a WITH (NOLOCK)
    LEFT JOIN acc_sku_profitability_rollup r WITH (NOLOCK)
        ON r.marketplace_id = a.marketplace_id
        AND r.sku = a.sku
        AND r.period_date = a.report_date
    WHERE a.report_date >= '2026-01-01' AND a.report_date < '2026-02-01'
      AND a.sku IS NOT NULL AND a.sku <> ''
""")[0]
print(f"  total={r[0]:,} | spend={float(r[1] or 0):,.0f} | matched={r[2]:,} ({float(r[3] or 0):,.0f} PLN) | unmatched={float(r[4] or 0):,.0f} PLN")

# 4. Sample unmatched SKUs
print("\n=== TOP 10 UNMATCHED SKUs (Jan, by spend) ===")
for r in q("""
    SELECT TOP 10 a.sku, a.marketplace_id,
           SUM(CAST(a.spend_pln AS FLOAT)) total_spend
    FROM acc_ads_product_day a WITH (NOLOCK)
    LEFT JOIN acc_sku_profitability_rollup r WITH (NOLOCK)
        ON r.marketplace_id = a.marketplace_id
        AND r.sku = a.sku
        AND r.period_date = a.report_date
    WHERE a.report_date >= '2026-01-01' AND a.report_date < '2026-02-01'
      AND a.sku IS NOT NULL AND a.sku <> ''
      AND r.sku IS NULL
    GROUP BY a.sku, a.marketplace_id
    ORDER BY SUM(CAST(a.spend_pln AS FLOAT)) DESC
"""):
    print(f"  {r[0]:>30} ({r[1]}): {float(r[2] or 0):>8,.0f} PLN")

# 5. Do these SKUs exist in acc_order_line?
print("\n=== DO UNMATCHED SKUs HAVE ORDERS? ===")
for r in q("""
    SELECT TOP 10 a.sku, a.marketplace_id,
           SUM(CAST(a.spend_pln AS FLOAT)) ads_spend,
           (SELECT COUNT(*) FROM acc_order_line ol WITH (NOLOCK)
            JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE ol.sku = a.sku AND o.marketplace_id = a.marketplace_id
              AND o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-02-01'
              AND o.status = 'Shipped') jan_orders
    FROM acc_ads_product_day a WITH (NOLOCK)
    LEFT JOIN acc_sku_profitability_rollup r WITH (NOLOCK)
        ON r.marketplace_id = a.marketplace_id
        AND r.sku = a.sku
        AND r.period_date = a.report_date
    WHERE a.report_date >= '2026-01-01' AND a.report_date < '2026-02-01'
      AND a.sku IS NOT NULL AND a.sku <> ''
      AND r.sku IS NULL
    GROUP BY a.sku, a.marketplace_id
    ORDER BY SUM(CAST(a.spend_pln AS FLOAT)) DESC
"""):
    print(f"  {r[0]:>30} ({r[1]}): ads={float(r[2] or 0):>6,.0f} PLN | jan_orders={r[3]}")

c.close()
print("\nDONE")
