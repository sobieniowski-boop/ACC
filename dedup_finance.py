"""
Dedup acc_finance_transaction: keep ONLY the latest synced_at row
for each unique (marketplace_id, posted_date, charge_type, amount, sku, amazon_order_id).
"""
import pymssql, os
from dotenv import load_dotenv
load_dotenv('C:/ACC/.env')

conn = pymssql.connect(
    server=os.getenv('MSSQL_SERVER'),
    user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'),
    database=os.getenv('MSSQL_DATABASE'),
    port=1433, tds_version='7.3'
)
cur = conn.cursor()

# Step 1: Count before
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
before = cur.fetchone()[0]

# Step 2: Delete duplicates, keeping the row with the MAX(synced_at)
# Using CTE with ROW_NUMBER to identify duplicates
dedup_sql = """
;WITH cte AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY marketplace_id,
                            CONVERT(VARCHAR(19), posted_date, 120),
                            ISNULL(charge_type, ''),
                            CAST(amount AS DECIMAL(18,4)),
                            ISNULL(sku, ''),
                            ISNULL(amazon_order_id, '')
               ORDER BY synced_at DESC
           ) AS rn
    FROM acc_finance_transaction
)
DELETE FROM cte WHERE rn > 1
"""

print(f"Before: {before:,} rows")
print("Running dedup DELETE...")
cur.execute(dedup_sql)
deleted = cur.rowcount
conn.commit()

# Step 3: Count after
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
after = cur.fetchone()[0]

# Step 4: Check FBAStorageFee
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()

result = f"""Before: {before:,} rows
Deleted: {deleted:,} duplicates
After: {after:,} rows
FBAStorageFee: {r[0]} rows, sum={r[1]} EUR
"""
print(result)

with open('C:/ACC/dedup_result.txt', 'w') as f:
    f.write(result)

conn.close()
print("DONE - see C:/ACC/dedup_result.txt")
