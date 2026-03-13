"""
Deep analysis: are the GROUPS themselves duplicated?
Each settlement group should contain unique FBAStorageFee charges.
If multiple groups cover the same period, they may contain overlapping charges.
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

# 1. Unique FBAStorageFee ignoring group_id (just sku+amount+date)
cur.execute("""
    SELECT COUNT(*) as uniq_no_group, SUM(min_amount) as clean_sum
    FROM (
        SELECT sku, CAST(amount AS DECIMAL(18,4)) as amt,
               CONVERT(VARCHAR(10), posted_date, 120) as pd,
               MIN(amount) as min_amount
        FROM acc_finance_transaction
        WHERE charge_type='FBAStorageFee'
        GROUP BY sku, CAST(amount AS DECIMAL(18,4)),
                 CONVERT(VARCHAR(10), posted_date, 120)
    ) x
""")
r = cur.fetchone()
lines.append(f"Unique FBAStorageFee (sku+amount+date, NO group): {r[0]}, sum={r[1]}")

# 2. Also try with marketplace
cur.execute("""
    SELECT COUNT(*) as uniq, SUM(min_amount) as clean_sum
    FROM (
        SELECT marketplace_id, sku, CAST(amount AS DECIMAL(18,4)) as amt,
               CONVERT(VARCHAR(10), posted_date, 120) as pd,
               MIN(amount) as min_amount
        FROM acc_finance_transaction
        WHERE charge_type='FBAStorageFee'
        GROUP BY marketplace_id, sku, CAST(amount AS DECIMAL(18,4)),
                 CONVERT(VARCHAR(10), posted_date, 120)
    ) x
""")
r = cur.fetchone()
lines.append(f"Unique FBAStorageFee (mp+sku+amount+date, NO group): {r[0]}, sum={r[1]}")

# 3. Check: are different groups covering the same FBA charges?
cur.execute("""
    SELECT marketplace_id, sku, CAST(amount AS DECIMAL(18,4)) as amt,
           CONVERT(VARCHAR(10), posted_date, 120) as pd,
           COUNT(DISTINCT financial_event_group_id) as num_groups,
           COUNT(*) as total_rows
    FROM acc_finance_transaction
    WHERE charge_type='FBAStorageFee'
    GROUP BY marketplace_id, sku, CAST(amount AS DECIMAL(18,4)),
             CONVERT(VARCHAR(10), posted_date, 120)
    HAVING COUNT(DISTINCT financial_event_group_id) > 1
    ORDER BY COUNT(*) DESC
""")
rows = cur.fetchall()
lines.append(f"\nFBAStorageFee combos appearing in MULTIPLE groups: {len(rows)}")
for r in rows[:20]:
    lines.append(f"  mp={r[0]} sku={r[1]} amt={r[2]} date={r[3]} -> {r[4]} groups, {r[5]} rows")

# 4. If we dedup across groups (keep 1 row per mp+sku+amount+date), what's the sum?
cur.execute("""
    SELECT COUNT(*) as cnt, SUM(amt) as total
    FROM (
        SELECT marketplace_id, sku, CAST(amount AS DECIMAL(18,4)) as amt,
               CONVERT(VARCHAR(10), posted_date, 120) as pd
        FROM acc_finance_transaction
        WHERE charge_type='FBAStorageFee'
        GROUP BY marketplace_id, sku, CAST(amount AS DECIMAL(18,4)),
                 CONVERT(VARCHAR(10), posted_date, 120)
    ) x
""")
r = cur.fetchone()
lines.append(f"\nAfter cross-group dedup: {r[0]} unique FBAStorageFee, sum={r[1]}")

conn.close()

result = '\n'.join(lines)
print(result)
with open('C:/ACC/fba_deep.txt', 'w') as f:
    f.write(result + '\n')
