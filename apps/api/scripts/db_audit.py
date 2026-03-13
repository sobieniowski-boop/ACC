"""Quick DB status audit."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"), override=True)

from app.core.db_connection import connect_acc

c = connect_acc()
cur = c.cursor()

# List all tables
cur.execute("""
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
""")
all_tables = [r[0] for r in cur.fetchall()]
print(f"=== {len(all_tables)} tables in Azure SQL ===\n")

for t in all_tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM [{t}]")
        cnt = cur.fetchone()[0]
        marker = " <<<" if cnt > 0 else ""
        print(f"  {t:40s} {cnt:>8,}{marker}")
    except Exception as e:
        print(f"  {t:40s} ERROR")

print("\n=== Orders by marketplace ===")
cur.execute("""
    SELECT m.code, COUNT(o.id) as cnt
    FROM acc_order o
    JOIN acc_marketplace m ON o.marketplace_id = m.id
    GROUP BY m.code
    ORDER BY cnt DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]:5s} {row[1]:>6,}")

print("\n=== Key metrics ===")
cur.execute("SELECT COUNT(*) FROM acc_product WHERE netto_purchase_price_pln IS NOT NULL AND netto_purchase_price_pln > 0")
print(f"  Products with purchase price: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM acc_order_line WHERE purchase_price_pln IS NOT NULL")
print(f"  Order lines with COGS:        {cur.fetchone()[0]}")
cur.execute("SELECT MIN(purchase_date), MAX(purchase_date) FROM acc_order")
row = cur.fetchone()
print(f"  Order date range:             {row[0]} → {row[1]}")
c.close()
