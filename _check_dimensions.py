import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=30)
cur = conn.cursor()

# Check product catalog columns
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_product_catalog' ORDER BY ORDINAL_POSITION")
print("acc_product_catalog columns:")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Check if any dimension-like columns exist in any table
print("\nDimension/weight columns across all tables:")
cur.execute("""
    SELECT TABLE_NAME, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE COLUMN_NAME LIKE '%weight%' 
       OR COLUMN_NAME LIKE '%dimension%'
       OR COLUMN_NAME LIKE '%length%'
       OR COLUMN_NAME LIKE '%width%'
       OR COLUMN_NAME LIKE '%height%'
       OR COLUMN_NAME LIKE '%volum%'
    ORDER BY TABLE_NAME, COLUMN_NAME
""")
for r in cur.fetchall():
    print(f"  {r[0]}.{r[1]}")

conn.close()
