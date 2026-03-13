"""Quick schema check for price-related tables."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

for table in ["acc_product", "acc_purchase_price"]:
    print(f"\n=== {table} ===")
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"
    )
    for r in cur.fetchall():
        print(f"  {r[0]:30s} {r[1]}")

# count
cur.execute("SELECT COUNT(*) FROM acc_purchase_price WITH (NOLOCK)")
print(f"\nacc_purchase_price rows: {cur.fetchone()[0]}")

# sample
cur.execute("SELECT TOP 3 * FROM acc_purchase_price WITH (NOLOCK)")
cols = [d[0] for d in cur.description]
print(f"Columns: {cols}")
for r in cur.fetchall():
    print(f"  {list(r)}")

conn.close()
