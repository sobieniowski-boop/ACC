"""Dry-run: count how many duplicates would be removed."""
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

cur.execute("""
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
SELECT COUNT(*) FROM cte WHERE rn > 1
""")
dupes = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
total = cur.fetchone()[0]

result = f"Total: {total:,}\nDuplicates: {dupes:,}\nAfter dedup: {total - dupes:,}\n"
print(result)
with open('C:/ACC/dedup_dryrun.txt', 'w') as f:
    f.write(result)
conn.close()
