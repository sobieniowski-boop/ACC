"""Diagnose FBA Fee Audit query performance."""
import sys, time
sys.path.insert(0, r"C:\ACC\apps\api")
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

FBA_TYPES = "FBAPerUnitFulfillmentFee','FBAPerOrderFulfillmentFee','FBAWeightBasedFee','FBAPickAndPackFee"

print("=== Step 1: Suspicious SKUs ===")
t0 = time.time()
cur.execute(f"""
    SELECT ft.sku, ft.currency, COUNT(*) as total_charges,
           AVG(ABS(ft.amount)) as avg_fee,
           MIN(ABS(ft.amount)) as min_fee,
           MAX(ABS(ft.amount)) as max_fee
    FROM acc_finance_transaction ft WITH (NOLOCK)
    WHERE ft.charge_type IN ('{FBA_TYPES}')
      AND ft.sku IS NOT NULL
      AND ft.posted_date >= '2025-12-12'
      AND ft.posted_date <= '2026-03-11 23:59:59'
    GROUP BY ft.sku, ft.currency
    HAVING COUNT(*) >= 5
       AND MAX(ABS(ft.amount)) > MIN(ABS(ft.amount)) * 1.5
""")
rows = cur.fetchall()
print(f"  {len(rows)} suspicious SKUs in {time.time()-t0:.2f}s")

if rows:
    # Test per-SKU query sample
    sku = rows[0][0].replace("'", "''")
    print(f"  Testing per-SKU for: {sku}")
    t0 = time.time()
    cur.execute(f"""
        SELECT ft.amazon_order_id, CAST(ft.posted_date AS DATE) as posted_date,
               ABS(ft.amount) as fee_amount
        FROM acc_finance_transaction ft WITH (NOLOCK)
        WHERE ft.charge_type IN ('{FBA_TYPES}')
          AND ft.sku = '{sku}'
          AND ft.posted_date >= '2025-12-12'
          AND ft.posted_date <= '2026-03-11 23:59:59'
        ORDER BY ft.posted_date
    """)
    r = cur.fetchall()
    per_sku_time = time.time() - t0
    print(f"  {len(r)} charges in {per_sku_time:.2f}s")
    print(f"  Estimated total for {len(rows)} SKUs: {len(rows) * per_sku_time:.0f}s")

conn.close()
