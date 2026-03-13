"""Check when fee was last stamped on sample Feb order."""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()

# Check acc_order_line columns
print("=== acc_order_line columns (fee-related) ===")
cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'acc_order_line'
      AND (COLUMN_NAME LIKE '%%fee%%' OR COLUMN_NAME LIKE '%%updated%%' OR COLUMN_NAME LIKE '%%created%%' OR COLUMN_NAME LIKE '%%source%%')
    ORDER BY COLUMN_NAME
""")
for r in cur.fetchall():
    print(f"  {r[0]:30s} ({r[1]})")

# Get full details of the sample order line
print("\n=== Full details of sample Feb order line ===")
cur.execute("""
    SELECT TOP 1 ol.*, o.amazon_order_id, o.marketplace_id, o.purchase_date, o.fulfillment_channel
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.amazon_order_id = '306-6165274-2269946'
""")
cols = [d[0] for d in cur.description]
row = cur.fetchone()
if row:
    for c, v in zip(cols, row):
        if v is not None:
            print(f"  {c:30s} = {v}")

# Check if maybe fees come from profit_engine recomputation
# Look at profit_engine's recomputation logic
print("\n=== Check acc_profitability table ===")
cur.execute("""
    SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'acc_profitability'
""")
if cur.fetchone():
    print("  acc_profitability exists")
    cur.execute("""
        SELECT TOP 3 * 
        FROM dbo.acc_profitability_sku WITH (NOLOCK)
        WHERE marketplace_id = 'A1PA6795UKMFR9'
        ORDER BY updated_at DESC
    """)
    pcols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        for c, v in zip(pcols, row):
            if v is not None:
                print(f"  {c:30s} = {v}")
        print()
else:
    print("  NO acc_profitability table")

# Check if there's a step that estimates fees from offer rates
print("\n=== Check order_pipeline steps called ===")
cur.execute("""
    SELECT TOP 20 step_name, started_at, finished_at, status, created_at
    FROM dbo.acc_pipeline_log WITH (NOLOCK)
    WHERE step_name LIKE '%%fee%%' OR step_name LIKE '%%bridge%%'
    ORDER BY created_at DESC
""")
if cur.description:
    for r in cur.fetchall():
        print(f"  step={r[0]:30s} | started={r[1]} | finished={r[2]} | status={r[3]}")

conn.close()
