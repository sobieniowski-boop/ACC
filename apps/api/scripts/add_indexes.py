"""Add performance indexes for slow API endpoints."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env'))
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True, timeout=120)
cur = conn.cursor()

indexes = [
    # acc_order: covering index for profit queries (status='Shipped' + date range)
    (
        "IX_acc_order_shipped_date",
        "acc_order",
        """CREATE NONCLUSTERED INDEX IX_acc_order_shipped_date
           ON dbo.acc_order (status, purchase_date)
           INCLUDE (marketplace_id, currency, fulfillment_channel, sales_channel, amazon_order_id, is_refund, refund_type, refund_amount_pln)"""
    ),
    # acc_exchange_rate: proper covering index for OUTER APPLY lookups
    (
        "IX_acc_exchange_rate_fx_lookup",
        "acc_exchange_rate",
        """CREATE NONCLUSTERED INDEX IX_acc_exchange_rate_fx_lookup
           ON dbo.acc_exchange_rate (currency, rate_date DESC)
           INCLUDE (rate_to_pln)"""
    ),
    # acc_order_line: covering index for order_id joins
    (
        "IX_acc_order_line_order_cover",
        "acc_order_line",
        """CREATE NONCLUSTERED INDEX IX_acc_order_line_order_cover
           ON dbo.acc_order_line (order_id)
           INCLUDE (sku, asin, title, product_id, quantity_ordered, quantity_shipped, item_price, item_tax, promotion_discount, cogs_pln, fba_fee_pln, referral_fee_pln, purchase_price_pln)"""
    ),
    # acc_finance_transaction: index for amazon_order_id joins
    (
        "IX_acc_finance_txn_order",
        "acc_finance_transaction",
        """CREATE NONCLUSTERED INDEX IX_acc_finance_txn_order
           ON dbo.acc_finance_transaction (amazon_order_id)"""
    ),
    # acc_fba_inventory_snapshot: index for latest snapshot
    (
        "IX_acc_fba_inv_snapshot_date",
        "acc_fba_inventory_snapshot",
        """CREATE NONCLUSTERED INDEX IX_acc_fba_inv_snapshot_date
           ON dbo.acc_fba_inventory_snapshot (snapshot_date)
           INCLUDE (marketplace_id, sku, asin)"""
    ),
]

for name, table, ddl in indexes:
    # Check if index already exists
    cur.execute(f"""
        SELECT 1 FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        WHERE i.name = '{name}' AND o.name = '{table}'
    """)
    if cur.fetchone():
        print(f"  SKIP {name} (already exists)")
        continue
    try:
        print(f"  Creating {name} on {table} ...", end=" ", flush=True)
        cur.execute(ddl)
        print("OK")
    except Exception as e:
        print(f"FAIL: {e}")

conn.close()
print("\nDone.")
