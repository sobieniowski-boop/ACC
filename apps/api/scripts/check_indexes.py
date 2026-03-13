"""Check existing indexes and table row counts."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env'))

from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=20)
cur = conn.cursor()

print("=== acc_order INDEXES ===")
cur.execute("""
SELECT i.name AS index_name, 
       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id  
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID('dbo.acc_order')
GROUP BY i.name
ORDER BY i.name
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

print("\n=== acc_order_line INDEXES ===")
cur.execute("""
SELECT i.name AS index_name, 
       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id  
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID('dbo.acc_order_line')
GROUP BY i.name
ORDER BY i.name
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

print("\n=== acc_exchange_rate INDEXES ===")
cur.execute("""
SELECT i.name AS index_name, 
       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id  
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID('dbo.acc_exchange_rate')
GROUP BY i.name
ORDER BY i.name
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

print("\n=== ROW COUNTS ===")
for table in ['acc_order', 'acc_order_line', 'acc_exchange_rate', 'acc_fba_inventory_snapshot', 
              'acc_finance_transaction', 'acc_product', 'acc_amazon_listing_registry']:
    try:
        cur.execute(f"SELECT COUNT(*) FROM dbo.{table} WITH (NOLOCK)")
        cnt = cur.fetchone()[0]
        print(f"  {table}: {cnt:,}")
    except:
        print(f"  {table}: ERROR")

conn.close()
print("\nDone.")
