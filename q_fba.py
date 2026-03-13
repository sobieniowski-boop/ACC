import pymssql, os
from dotenv import load_dotenv
load_dotenv('C:/ACC/.env')
c = pymssql.connect(server=os.getenv('MSSQL_SERVER'), user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'), database=os.getenv('MSSQL_DATABASE'),
    port=1433, tds_version='7.3', login_timeout=10, timeout=60)
cur = c.cursor()

print("=== PER DAY ===")
cur.execute("""
    SELECT CONVERT(VARCHAR(10), posted_date, 120) d,
           COUNT(*) cnt, SUM(amount) s,
           COUNT(DISTINCT financial_event_group_id) grps
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY CONVERT(VARCHAR(10), posted_date, 120)
    ORDER BY d
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} rows sum={r[2]:.2f} groups={r[3]}")

print("\n=== PER MARKETPLACE ===")
cur.execute("""
    SELECT marketplace_id, COUNT(*) cnt, SUM(amount) s,
           COUNT(DISTINCT financial_event_group_id) grps
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY marketplace_id
    ORDER BY SUM(amount)
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} rows sum={r[2]:.2f} groups={r[3]}")

print("\n=== SAMPLE DE 2026-03-03 ===")
cur.execute("""
    SELECT TOP 10 amount, LEFT(financial_event_group_id, 20) gid,
           CONVERT(VARCHAR(19), synced_at, 120) sa
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
      AND marketplace_id='A1PA6795UKMFR9'
      AND CONVERT(VARCHAR(10), posted_date, 120)='2026-03-03'
    ORDER BY amount
""")
for r in cur.fetchall():
    print(f"  amt={r[0]:.4f} group={r[1]}... synced={r[2]}")

c.close()
print("\nDONE")
