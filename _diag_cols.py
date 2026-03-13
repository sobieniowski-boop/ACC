"""Check acc_order columns for precomputed fields."""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()
cur.execute("SELECT TOP 1 * FROM dbo.acc_order WITH (NOLOCK) WHERE revenue_pln IS NOT NULL AND revenue_pln > 0")
cols = [c[0] for c in cur.description]
row = cur.fetchone()
print("=== ACC_ORDER columns with data ===")
for i, c in enumerate(cols):
    if any(k in c.lower() for k in ['rev', 'fee', 'amazon', 'logistic', 'currency', 'margin', 'contrib', 'pln']):
        print(f"  {c}: {row[i]}")
conn.close()
