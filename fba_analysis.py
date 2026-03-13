"""
Analyze FBAStorageFee duplication pattern.
"""
import pymssql, os
from dotenv import load_dotenv
load_dotenv('C:/ACC/.env')

conn = pymssql.connect(
    server=os.getenv('MSSQL_SERVER'),
    user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'),
    database=os.getenv('MSSQL_DATABASE'),
    port=1433, tds_version='7.3',
    login_timeout=10, timeout=120,
)
cur = conn.cursor()

lines = []

# 1. Total FBAStorageFee
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()
lines.append(f"FBAStorageFee total: {r[0]} rows, sum={r[1]}")

# 2. How many distinct synced_at batches contain FBAStorageFee?
cur.execute("""
    SELECT COUNT(DISTINCT CONVERT(VARCHAR(19), synced_at, 120))
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
""")
lines.append(f"Distinct synced_at batches: {cur.fetchone()[0]}")

# 3. FBAStorageFee per financial_event_group_id
cur.execute("""
    SELECT financial_event_group_id, COUNT(*) cnt, SUM(amount) s
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY financial_event_group_id
    ORDER BY cnt DESC
""")
lines.append("\nPer group_id:")
for r in cur.fetchall():
    lines.append(f"  {r[0]}: {r[1]} rows, sum={r[2]}")

# 4. Per group: how many distinct (sku, amount, posted_date) tuples?
cur.execute("""
    SELECT financial_event_group_id,
           COUNT(*) as total,
           COUNT(DISTINCT CONCAT(ISNULL(sku,''), '|', CAST(amount AS VARCHAR), '|', CONVERT(VARCHAR(10),posted_date,120))) as uniq
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY financial_event_group_id
    HAVING COUNT(*) > COUNT(DISTINCT CONCAT(ISNULL(sku,''), '|', CAST(amount AS VARCHAR), '|', CONVERT(VARCHAR(10),posted_date,120)))
    ORDER BY COUNT(*) DESC
""")
lines.append("\nGroups with soft-dupes (same sku+amount+date, different synced_at/posted_date precision):")
for r in cur.fetchall():
    lines.append(f"  {r[0]}: total={r[1]}, unique={r[2]}, dupes={r[1]-r[2]}")

# 5. Overall: unique FBAStorageFee by (group, sku, amount, date-day)
cur.execute("""
    SELECT COUNT(*) as uniq_combos,
           SUM(min_amount) as clean_sum
    FROM (
        SELECT financial_event_group_id, sku, CAST(amount AS DECIMAL(18,4)) as amt,
               CONVERT(VARCHAR(10), posted_date, 120) as pd,
               MIN(amount) as min_amount
        FROM acc_finance_transaction
        WHERE charge_type='FBAStorageFee'
        GROUP BY financial_event_group_id, sku, CAST(amount AS DECIMAL(18,4)),
                 CONVERT(VARCHAR(10), posted_date, 120)
    ) x
""")
r = cur.fetchone()
lines.append(f"\nUnique FBAStorageFee combos (group+sku+amount+date): {r[0]}, clean sum={r[1]}")

conn.close()

result = '\n'.join(lines)
print(result)
with open('C:/ACC/fba_analysis.txt', 'w') as f:
    f.write(result + '\n')
