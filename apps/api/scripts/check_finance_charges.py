"""Quick diagnostic: check finance charge_type date ranges and totals."""
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
cur.execute("""
SELECT 
  charge_type,
  MIN(posted_date) AS min_dt,
  MAX(posted_date) AS max_dt,
  COUNT(*) AS cnt,
  SUM(ISNULL(amount_pln, 0)) AS total_pln
FROM acc_finance_transaction WITH (NOLOCK)
WHERE charge_type IN ('ShippingCharge','ShippingTax','FBAStorageFee','ShippingHB','ReturnPostage','ServiceFee')
GROUP BY charge_type
ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"{r[0]:25s} {str(r[1]):10s} - {str(r[2]):10s}  cnt={r[3]:>8d}  pln={float(r[4]):>12.2f}")

print("\n--- Jan 2025 ShippingCharge check ---")
cur.execute("""
SELECT 
  ft.marketplace_id,
  COUNT(*) AS cnt,
  SUM(ISNULL(ft.amount_pln, 0)) AS total_pln
FROM acc_finance_transaction ft WITH (NOLOCK)
WHERE ft.charge_type IN ('ShippingCharge','ShippingTax')
  AND ft.posted_date >= '2025-01-01' AND ft.posted_date < '2025-02-01'
GROUP BY ft.marketplace_id
""")
for r in cur.fetchall():
    print(f"  mkt={str(r[0] or 'NULL'):20s}  cnt={r[1]:>6d}  pln={float(r[2]):>10.2f}")

print("\n--- Jan 2025 CM2 charge_types ---")
cur.execute("""
SELECT 
  ft.charge_type,
  COALESCE(ft.marketplace_id, 'NULL') AS mkt,
  COUNT(*) AS cnt,
  SUM(ABS(ISNULL(ft.amount_pln, 0))) AS total_pln
FROM acc_finance_transaction ft WITH (NOLOCK)
WHERE ft.posted_date >= '2025-01-01' AND ft.posted_date < '2025-02-01'
  AND ft.charge_type IN ('FBAStorageFee','FBALongTermStorageFee','FBAInventoryPlacementServiceFee',
    'ShippingHB','ShippingChargeback','ReturnPostage','FBAPerOrderFulfillmentFee',
    'FBAPerUnitFulfillmentFee','RemovalComplete','DisposalComplete','LiquidationsProceeds')
GROUP BY ft.charge_type, COALESCE(ft.marketplace_id, 'NULL')
ORDER BY total_pln DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]:40s} mkt={r[1]:20s}  cnt={r[2]:>6d}  pln={float(r[3]):>10.2f}")
conn.close()
