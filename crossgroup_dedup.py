"""
Cross-group dedup: for each (marketplace_id, charge_type, posted_date-day, amount, sku, amazon_order_id)
keep only ONE row (latest synced_at). This fixes the issue where Amazon reports
the same charge in multiple settlement groups.
"""
import pymssql, os, time
from dotenv import load_dotenv
load_dotenv('C:/ACC/.env')

conn = pymssql.connect(
    server=os.getenv('MSSQL_SERVER'),
    user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'),
    database=os.getenv('MSSQL_DATABASE'),
    port=1433, tds_version='7.3',
    login_timeout=10, timeout=600,
)
cur = conn.cursor()

# Before stats
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
before_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()
fba_before = (r[0], r[1])

print(f"BEFORE: {before_total:,} total rows")
print(f"BEFORE FBAStorageFee: {fba_before[0]} rows, sum={fba_before[1]} EUR")

# Cross-group dedup: partition by the REAL identity of a charge
# (ignoring which group it came from), keep latest synced_at
# Process per marketplace to keep memory/time manageable
cur.execute("SELECT DISTINCT marketplace_id FROM acc_finance_transaction")
mkts = [r[0] for r in cur.fetchall()]

grand_deleted = 0
for mkt in mkts:
    t0 = time.time()
    cur.execute("""
        ;WITH cte AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY ISNULL(charge_type, ''),
                                    CAST(amount AS DECIMAL(18,4)),
                                    CONVERT(VARCHAR(10), posted_date, 120),
                                    ISNULL(sku, ''),
                                    ISNULL(amazon_order_id, '')
                       ORDER BY synced_at DESC
                   ) AS rn
            FROM acc_finance_transaction
            WHERE marketplace_id = %s
        )
        DELETE FROM cte WHERE rn > 1
    """, (mkt,))
    deleted = cur.rowcount
    conn.commit()
    grand_deleted += deleted
    elapsed = time.time() - t0
    print(f"  {mkt}: deleted {deleted:,} cross-group dupes ({elapsed:.1f}s)")

# Also handle marketplace_id IS NULL
t0 = time.time()
cur.execute("""
    ;WITH cte AS (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY ISNULL(charge_type, ''),
                                CAST(amount AS DECIMAL(18,4)),
                                CONVERT(VARCHAR(10), posted_date, 120),
                                ISNULL(sku, ''),
                                ISNULL(amazon_order_id, '')
                   ORDER BY synced_at DESC
               ) AS rn
        FROM acc_finance_transaction
        WHERE marketplace_id IS NULL
    )
    DELETE FROM cte WHERE rn > 1
""")
deleted = cur.rowcount
conn.commit()
grand_deleted += deleted
print(f"  NULL mp: deleted {deleted:,} cross-group dupes ({time.time()-t0:.1f}s)")

# After stats
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
after_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()
fba_after = (r[0], r[1])

summary = f"""
=== CROSS-GROUP DEDUP COMPLETE ===
Before:  {before_total:,} total rows
Deleted: {grand_deleted:,}
After:   {after_total:,} total rows

FBAStorageFee Before: {fba_before[0]} rows, sum={fba_before[1]} EUR
FBAStorageFee After:  {fba_after[0]} rows, sum={fba_after[1]} EUR
"""
print(summary)
with open('C:/ACC/crossgroup_dedup_result.txt', 'w') as f:
    f.write(summary)
conn.close()
