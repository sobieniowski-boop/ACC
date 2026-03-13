"""Audit all charge_type values in acc_finance_transaction."""
import sys
sys.path.insert(0, ".")

from app.core.db_connection import connect_acc

conn = connect_acc(timeout=60)
cur = conn.cursor()

# 1) All distinct charge_types with counts
cur.execute("""
    SELECT charge_type, COUNT(*) as cnt,
           SUM(amount) as total_amount,
           SUM(ISNULL(amount_pln,0)) as total_pln
    FROM dbo.acc_finance_transaction WITH (NOLOCK)
    GROUP BY charge_type
    ORDER BY cnt DESC
""")
rows = cur.fetchall()
cols = [c[0] for c in cur.description]
print(f"=== {len(rows)} distinct charge_types in acc_finance_transaction ===")
for r in rows:
    ct = str(r[0] or "(NULL)")
    print(f"  {ct:55s} | cnt={r[1]:>8,} | amt={r[2]:>15,.2f} | pln={r[3]:>15,.2f}")

# 2) Also check transaction_type (if it exists)
print("\n=== transaction_type values ===")
try:
    cur.execute("""
        SELECT transaction_type, COUNT(*) as cnt
        FROM dbo.acc_finance_transaction WITH (NOLOCK)
        GROUP BY transaction_type
        ORDER BY cnt DESC
    """)
    for r in cur.fetchall():
        print(f"  {str(r[0] or '(NULL)'):40s} | cnt={r[1]:>8,}")
except Exception as e:
    print(f"  (no transaction_type column: {e})")

# 3) Check columns available
print("\n=== Table columns ===")
cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'acc_finance_transaction'
    ORDER BY ORDINAL_POSITION
""")
for r in cur.fetchall():
    print(f"  {r[0]:40s} {r[1]}")

conn.close()
