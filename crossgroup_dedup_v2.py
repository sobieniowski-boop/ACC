"""
Cross-group dedup by WEEKLY batches — much faster than per-marketplace
on a 1.6M row table without indexes.
"""
import pymssql, os, time
from datetime import datetime, timedelta
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

# Before
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
before_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()
fba_before = (r[0], r[1])
print(f"BEFORE: {before_total:,} total | FBA: {fba_before[0]} rows sum={fba_before[1]}")

# Get date range
cur.execute("SELECT MIN(posted_date), MAX(posted_date) FROM acc_finance_transaction")
r = cur.fetchone()
min_date = r[0].date() if r[0] else datetime(2025, 9, 1).date()
max_date = r[1].date() if r[1] else datetime(2026, 3, 8).date()

# Process in weekly chunks
grand_deleted = 0
week_start = min_date
while week_start <= max_date:
    week_end = week_start + timedelta(days=7)
    t0 = time.time()
    
    cur.execute("""
        ;WITH cte AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY ISNULL(marketplace_id, ''),
                                    ISNULL(charge_type, ''),
                                    CAST(amount AS DECIMAL(18,4)),
                                    ISNULL(sku, ''),
                                    ISNULL(amazon_order_id, '')
                       ORDER BY synced_at DESC
                   ) AS rn
            FROM acc_finance_transaction
            WHERE posted_date >= %s AND posted_date < %s
        )
        DELETE FROM cte WHERE rn > 1
    """, (str(week_start), str(week_end)))
    
    deleted = cur.rowcount
    conn.commit()
    grand_deleted += deleted
    elapsed = time.time() - t0
    if deleted > 0:
        print(f"  {week_start} -> {week_end}: deleted {deleted:,} ({elapsed:.1f}s)")
    
    week_start = week_end

# Also NULL posted_date
cur.execute("""
    ;WITH cte AS (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY ISNULL(marketplace_id, ''),
                                ISNULL(charge_type, ''),
                                CAST(amount AS DECIMAL(18,4)),
                                ISNULL(sku, ''),
                                ISNULL(amazon_order_id, '')
                   ORDER BY synced_at DESC
               ) AS rn
        FROM acc_finance_transaction
        WHERE posted_date IS NULL
    )
    DELETE FROM cte WHERE rn > 1
""")
nd = cur.rowcount
conn.commit()
grand_deleted += nd
if nd > 0:
    print(f"  NULL date: deleted {nd}")

# After
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
after_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()
fba_after = (r[0], r[1])

summary = f"""
=== CROSS-GROUP DEDUP (WEEKLY) ===
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
print("DONE")
