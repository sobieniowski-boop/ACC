"""
Dedup acc_finance_transaction in batches per marketplace.
For each marketplace, keep only the latest synced_at row per unique key.
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
    login_timeout=10,
    timeout=300,
)
cur = conn.cursor()

# Get marketplaces
cur.execute("SELECT DISTINCT marketplace_id FROM acc_finance_transaction")
mkts = [r[0] for r in cur.fetchall()]

cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
before = cur.fetchone()[0]

total_deleted = 0
results = [f"Before: {before:,} rows\n"]

for mkt in mkts:
    t0 = time.time()
    cur.execute("""
        ;WITH cte AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY CONVERT(VARCHAR(19), posted_date, 120),
                                    ISNULL(charge_type, ''),
                                    CAST(amount AS DECIMAL(18,4)),
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
    elapsed = time.time() - t0
    total_deleted += deleted
    msg = f"  {mkt}: deleted {deleted:,} dupes ({elapsed:.1f}s)"
    print(msg)
    results.append(msg + "\n")

cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
after = cur.fetchone()[0]

cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
fba = cur.fetchone()

summary = f"\nTotal deleted: {total_deleted:,}\nAfter: {after:,}\nFBAStorageFee: {fba[0]} rows, sum={fba[1]} EUR\n"
print(summary)
results.append(summary)

with open('C:/ACC/dedup_result.txt', 'w') as f:
    f.writelines(results)

conn.close()
print("DONE")
