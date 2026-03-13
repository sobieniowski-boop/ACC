"""Quick verification of CSV import."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

print("=== IMPORT VERIFICATION ===")
cur.execute("SELECT COUNT(*) FROM acc_product WITH (NOLOCK)")
print(f"Total products:      {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM acc_product WITH (NOLOCK) WHERE mapping_source = ?", "import_csv")
print(f"  import_csv:        {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM acc_purchase_price WITH (NOLOCK)")
print(f"Total prices:        {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM acc_purchase_price WITH (NOLOCK) WHERE source = ?", "import_csv")
print(f"  import_csv:        {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM acc_product WITH (NOLOCK) WHERE netto_purchase_price_pln > 0")
print(f"Products with price: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM acc_product WITH (NOLOCK) WHERE asin LIKE ?", "_CSV_%")
print(f"Placeholder ASINs:   {cur.fetchone()[0]}")

print("\nProducts by mapping_source:")
cur.execute("SELECT ISNULL(mapping_source, 'NULL'), COUNT(*) FROM acc_product WITH (NOLOCK) GROUP BY mapping_source ORDER BY COUNT(*) DESC")
for r in cur.fetchall():
    print(f"  {str(r[0]):25s} {r[1]}")

print("\nPrices by source:")
cur.execute("SELECT source, COUNT(*) FROM acc_purchase_price WITH (NOLOCK) GROUP BY source ORDER BY COUNT(*) DESC")
for r in cur.fetchall():
    print(f"  {str(r[0]):25s} {r[1]}")

conn.close()
