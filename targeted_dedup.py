"""
FAST targeted dedup: fix FBAStorageFee and other service fees.
Only 772 FBA rows — trivial to process.
Then create index for future full-table dedup.
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
    login_timeout=10, timeout=300,
)
cur = conn.cursor()

# ── Step 1: FBAStorageFee ──
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()
print(f"FBAStorageFee BEFORE: {r[0]} rows, sum={r[1]}")

# Dedup: keep 1 per (marketplace_id, amount-rounded, posted_date-day)
# FBAStorageFee has sku=NULL, so not part of key
cur.execute("""
    ;WITH cte AS (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY ISNULL(marketplace_id, ''),
                                CAST(amount AS DECIMAL(18,4)),
                                CONVERT(VARCHAR(10), posted_date, 120)
                   ORDER BY synced_at DESC
               ) AS rn
        FROM acc_finance_transaction
        WHERE charge_type = 'FBAStorageFee'
    )
    DELETE FROM cte WHERE rn > 1
""")
fba_deleted = cur.rowcount
conn.commit()
print(f"FBAStorageFee deleted: {fba_deleted}")

cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()
print(f"FBAStorageFee AFTER:  {r[0]} rows, sum={r[1]}")

# ── Step 2: All service fee types (typically low count) ──
# Get charge_types that look like account-level fees (not per-order)
cur.execute("""
    SELECT charge_type, COUNT(*) cnt
    FROM acc_finance_transaction
    WHERE charge_type LIKE '%Storage%'
       OR charge_type LIKE '%Subscription%'
       OR charge_type LIKE '%ServiceFee%'
       OR charge_type LIKE '%Liquidation%'
       OR charge_type LIKE '%Removal%'
       OR charge_type LIKE '%LongTerm%'
       OR charge_type LIKE '%Aged%'
      OR charge_type LIKE '%Disposal%'
    GROUP BY charge_type
    ORDER BY cnt DESC
""")
fee_types = cur.fetchall()
print(f"\nService fee types found: {len(fee_types)}")
for ft in fee_types:
    print(f"  {ft[0]}: {ft[1]} rows")

# Dedup each service fee type
total_svc_deleted = 0
for ft_name, ft_cnt in fee_types:
    if ft_name == 'FBAStorageFee':
        continue  # already done
    cur.execute("""
        ;WITH cte AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY ISNULL(marketplace_id, ''),
                                    CAST(amount AS DECIMAL(18,4)),
                                    CONVERT(VARCHAR(10), posted_date, 120),
                                    ISNULL(sku, ''),
                                    ISNULL(amazon_order_id, '')
                       ORDER BY synced_at DESC
                   ) AS rn
            FROM acc_finance_transaction
            WHERE charge_type = %s
        )
        DELETE FROM cte WHERE rn > 1
    """, (ft_name,))
    d = cur.rowcount
    conn.commit()
    if d > 0:
        print(f"  {ft_name}: deleted {d}")
    total_svc_deleted += d

print(f"\nTotal service fee dupes deleted: {total_svc_deleted}")

# ── Step 3: Create index for future full dedup ──
print("\nCreating dedup index (will take a while)...")
try:
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_finance_dedup_helper' AND object_id = OBJECT_ID('acc_finance_transaction'))
        CREATE NONCLUSTERED INDEX IX_finance_dedup_helper
        ON acc_finance_transaction (marketplace_id, charge_type, posted_date, amount, sku, amazon_order_id)
        INCLUDE (synced_at)
    """)
    conn.commit()
    print("Index created!")
except Exception as e:
    print(f"Index creation error (non-fatal): {e}")
    conn.rollback()

# ── Final summary ──
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
r = cur.fetchone()

summary = f"""
=== TARGETED DEDUP RESULT ===
FBAStorageFee deleted: {fba_deleted}
Service fees deleted: {total_svc_deleted}
Total rows now: {total:,}
FBAStorageFee: {r[0]} rows, sum={r[1]} EUR
"""
print(summary)
with open('C:/ACC/targeted_dedup_result.txt', 'w') as f:
    f.write(summary)
conn.close()
print("DONE")
