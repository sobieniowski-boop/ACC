"""Quick DB metrics check."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"), override=True)
from app.core.db_connection import connect_acc
c = connect_acc()
cur = c.cursor()
metrics = [
    ('orders', 'SELECT COUNT(*) FROM acc_order'),
    ('order_lines', 'SELECT COUNT(*) FROM acc_order_line'),
    ('products', 'SELECT COUNT(*) FROM acc_product'),
    ('mapped_products', 'SELECT COUNT(*) FROM acc_product WHERE internal_sku IS NOT NULL'),
    ('products_w_price', 'SELECT COUNT(*) FROM acc_product WHERE netto_purchase_price_pln IS NOT NULL'),
    ('purchase_history', 'SELECT COUNT(*) FROM acc_purchase_price'),
    ('lines_with_cogs', 'SELECT COUNT(*) FROM acc_order_line WHERE purchase_price_pln IS NOT NULL'),
    ('lines_no_cogs', 'SELECT COUNT(*) FROM acc_order_line WHERE purchase_price_pln IS NULL'),
    ('lines_linked', 'SELECT COUNT(*) FROM acc_order_line WHERE product_id IS NOT NULL'),
    ('lines_unlinked', 'SELECT COUNT(*) FROM acc_order_line WHERE product_id IS NULL'),
    ('exchange_rates', 'SELECT COUNT(*) FROM acc_exchange_rate'),
]
print("=" * 50)
print("  AZURE SQL — CURRENT METRICS")
print("=" * 50)
for name, sql in metrics:
    cur.execute(sql)
    print(f"  {name:22s} {cur.fetchone()[0]:>8,}")
c.close()
