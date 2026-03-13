"""Test the new single-pass overcharge query."""
import sys, time
sys.path.insert(0, r"C:\ACC\apps\api")
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

FBA_TYPES = "'FBAPerUnitFulfillmentFee','FBAPerOrderFulfillmentFee','FBAWeightBasedFee','FBAPickAndPackFee'"

print("=== Single-pass overcharge CTE ===")
t0 = time.time()
cur.execute(f"""
    WITH base AS (
        SELECT
            ft.sku,
            ft.currency,
            ft.amazon_order_id,
            CAST(ft.posted_date AS DATE) AS posted_date,
            ABS(ft.amount)               AS fee_amount,
            COUNT(*) OVER (PARTITION BY ft.sku, ft.currency) AS total_charges,
            MIN(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency)   AS min_fee,
            MAX(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency)   AS max_fee,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(ft.amount))
                OVER (PARTITION BY ft.sku, ft.currency)                   AS median_fee
        FROM acc_finance_transaction ft WITH (NOLOCK)
        WHERE ft.charge_type IN ({FBA_TYPES})
          AND ft.sku IS NOT NULL
          AND ft.posted_date >= '2025-12-12'
          AND ft.posted_date <= '2026-03-11 23:59:59'
    ),
    suspicious AS (
        SELECT *
        FROM base
        WHERE total_charges >= 5
          AND max_fee > min_fee * 1.5
    )
    SELECT
        sku, currency, amazon_order_id, posted_date,
        fee_amount, total_charges, median_fee,
        median_fee * 1.5 AS threshold,
        CASE WHEN fee_amount > median_fee * 1.5
             THEN fee_amount - median_fee ELSE 0 END AS excess
    FROM suspicious
    ORDER BY sku, posted_date
""")
rows = cur.fetchall()
elapsed = time.time() - t0
print(f"  {len(rows)} rows in {elapsed:.2f}s")

# Count unique SKUs
skus = set()
overcharged = 0
for r in rows:
    skus.add(r[0])
    if float(r[8]) > 0:
        overcharged += 1
print(f"  {len(skus)} unique SKUs, {overcharged} overcharged orders")

conn.close()
