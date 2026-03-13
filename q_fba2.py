import pymssql, os
from dotenv import load_dotenv
load_dotenv('C:/ACC/.env')
c = pymssql.connect(server=os.getenv('MSSQL_SERVER'), user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'), database=os.getenv('MSSQL_DATABASE'),
    port=1433, tds_version='7.3', login_timeout=10, timeout=60)
cur = c.cursor()

# For each marketplace, get FBA sum if we keep only the LATEST group (by synced_at)
print("=== FBAStorageFee: latest group only per marketplace ===")
cur.execute("""
    SELECT marketplace_id,
           COUNT(*) as total_rows,
           SUM(amount) as total_sum,
           COUNT(DISTINCT financial_event_group_id) as all_groups
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY marketplace_id
""")
mp_data = cur.fetchall()

grand_latest_sum = 0
grand_latest_rows = 0
for mp, total_rows, total_sum, all_groups in mp_data:
    # Find latest group for this mp
    cur.execute("""
        SELECT TOP 1 financial_event_group_id, MAX(synced_at) latest
        FROM acc_finance_transaction
        WHERE charge_type='FBAStorageFee' AND marketplace_id %s
        GROUP BY financial_event_group_id
        ORDER BY MAX(synced_at) DESC
    """ % ("= '%s'" % mp if mp else "IS NULL",))
    latest = cur.fetchone()
    if latest:
        gid = latest[0]
        cur.execute("""
            SELECT COUNT(*), SUM(amount)
            FROM acc_finance_transaction
            WHERE charge_type='FBAStorageFee'
              AND financial_event_group_id = %s
              AND marketplace_id %s
        """ % ("'%s'" % gid, "= '%s'" % mp if mp else "IS NULL"))
        r = cur.fetchone()
        print(f"  {mp}: all={total_rows} rows sum={total_sum:.2f} | latest group: {r[0]} rows sum={r[1]:.2f}")
        grand_latest_sum += float(r[1])
        grand_latest_rows += r[0]

print(f"\nGrand total (latest group per mp): {grand_latest_rows} rows, sum={grand_latest_sum:.2f} EUR")
print(f"Grand total (all groups): sum={sum(float(r[2]) for r in mp_data):.2f} EUR")

# What if we keep only 1 latest group globally?
cur.execute("""
    SELECT TOP 1 financial_event_group_id, COUNT(*) cnt, SUM(amount) s
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY financial_event_group_id
    ORDER BY MAX(synced_at) DESC
""")
r = cur.fetchone()
print(f"\nLatest single group: {r[0][:30]}... -> {r[1]} rows, sum={r[2]:.2f}")

c.close()
