"""Understand FBAStorageFee distribution per day/marketplace."""
import pymssql, os
from dotenv import load_dotenv
load_dotenv('C:/ACC/.env')

conn = pymssql.connect(
    server=os.getenv('MSSQL_SERVER'),
    user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'),
    database=os.getenv('MSSQL_DATABASE'),
    port=1433, tds_version='7.3',
    login_timeout=10, timeout=60,
)
cur = conn.cursor()

lines = []

# 1. Per day totals
cur.execute("""
    SELECT CONVERT(VARCHAR(10), posted_date, 120) as d,
           COUNT(*) cnt, SUM(amount) s,
           COUNT(DISTINCT financial_event_group_id) grps,
           COUNT(DISTINCT marketplace_id) mps
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY CONVERT(VARCHAR(10), posted_date, 120)
    ORDER BY d
""")
lines.append("FBAStorageFee per day:")
for r in cur.fetchall():
    lines.append(f"  {r[0]}: {r[1]} rows, sum={r[2]:.2f} EUR, {r[3]} groups, {r[4]} mps")

# 2. Per marketplace totals
cur.execute("""
    SELECT marketplace_id, COUNT(*) cnt, SUM(amount) s,
           COUNT(DISTINCT financial_event_group_id) grps
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY marketplace_id
    ORDER BY SUM(amount)
""")
lines.append("\nFBAStorageFee per marketplace:")
for r in cur.fetchall():
    lines.append(f"  {r[0]}: {r[1]} rows, sum={r[2]:.2f} EUR, {r[3]} groups")

# 3. Sample: DE on 2026-03-03 — what do individual rows look like?
cur.execute("""
    SELECT TOP 20 id, marketplace_id, amount, financial_event_group_id,
           CONVERT(VARCHAR(19), synced_at, 120) as sa
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
      AND marketplace_id = 'A1PA6795UKMFR9'
      AND CONVERT(VARCHAR(10), posted_date, 120) = '2026-03-03'
    ORDER BY amount
""")
lines.append("\nSample: DE FBAStorageFee on 2026-03-03:")
for r in cur.fetchall():
    lines.append(f"  amt={r[2]:.4f} group={r[3][:25]}... synced={r[4]}")

conn.close()
result = '\n'.join(lines)
print(result)
with open('C:/ACC/fba_pattern.txt', 'w') as f:
    f.write(result + '\n')
