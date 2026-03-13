"""Check for FBAStorageFee duplicates - same amount/date/marketplace."""
import pymssql, os
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")

conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
    port=1433, login_timeout=60, timeout=180, tds_version="7.3",
)
cur = conn.cursor()

# 1) Check how many duplicate (same amount, date, marketplace) FBAStorageFee exist
print("=== Duplicate analysis: FBAStorageFee ===")
cur.execute("""
SELECT marketplace_id, CAST(posted_date AS DATE) d, 
       CAST(amount AS VARCHAR(30)) amt, COUNT(*) dupes
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY marketplace_id, CAST(posted_date AS DATE), CAST(amount AS VARCHAR(30))
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
""")
rows = cur.fetchall()
if rows:
    print(f"Found {len(rows)} duplicate groups:")
    for r in rows[:20]:
        print(f"  mp={r[0]}, date={r[1]}, amount={r[2]}, count={r[3]}")
else:
    print("No exact duplicates found by (marketplace, date, amount)")

# 2) Unique amounts vs total records
print("\n=== Unique vs total ===")
cur.execute("""
SELECT COUNT(*) total,
       COUNT(DISTINCT CONCAT(marketplace_id, '|', CAST(posted_date AS DATE), '|', CAST(amount AS VARCHAR(30)))) unique_combos
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
""")
r = cur.fetchone()
print(f"Total records: {r[0]}, Unique (mp+date+amount) combos: {r[1]}")

# 3) Check synced_at - how many sync runs added these?
print("\n=== Sync runs for FBAStorageFee ===")
cur.execute("""
SELECT CONVERT(VARCHAR(19), synced_at, 120) sync_time, COUNT(*) cnt,
       SUM(CAST(amount AS FLOAT)) total_eur
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY CONVERT(VARCHAR(19), synced_at, 120)
ORDER BY sync_time
""")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]:>4} recs | {r[2]:>12,.2f} EUR")

# 4) Check settlement_id distribution
print("\n=== Settlement/FinancialEventGroup IDs ===")
cur.execute("""
SELECT financial_event_group_id, COUNT(*) cnt, SUM(CAST(amount AS FLOAT)) eur
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY financial_event_group_id
ORDER BY cnt DESC
""")
for r in cur.fetchall():
    grp = (r[0] or "NULL")[:40]
    print(f"  {grp:40} | {r[1]:>4} recs | {r[2]:>10,.2f} EUR")

conn.close()
print("\nDONE")
