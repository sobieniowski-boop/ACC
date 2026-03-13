"""Create missing finance transaction index."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=300)
cur = conn.cursor()
try:
    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_acc_finance_txn_order'
              AND object_id = OBJECT_ID('dbo.acc_finance_transaction')
        )
        CREATE NONCLUSTERED INDEX IX_acc_finance_txn_order
        ON dbo.acc_finance_transaction (amazon_order_id)
        INCLUDE (marketplace_id, charge_type, amount, amount_pln, currency)
        WITH (ONLINE = ON, MAXDOP = 1)
    """)
    conn.commit()
    print("Index created OK")
except Exception as e:
    print(f"FAILED: {e}")
finally:
    conn.close()
