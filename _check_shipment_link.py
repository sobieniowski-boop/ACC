import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=30)
cur = conn.cursor()

# 1. acc_shipment columns
print("=== acc_shipment columns ===")
cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_shipment' ORDER BY ORDINAL_POSITION")
for r in cur.fetchall():
    print(f"  {r[0]} ({r[1]})")

# 2. acc_shipment_order_link columns
print("\n=== acc_shipment_order_link columns ===")
cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_shipment_order_link' ORDER BY ORDINAL_POSITION")
for r in cur.fetchall():
    print(f"  {r[0]} ({r[1]})")

# 3. acc_shipment_cost columns
print("\n=== acc_shipment_cost columns ===")
cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_shipment_cost' ORDER BY ORDINAL_POSITION")
for r in cur.fetchall():
    print(f"  {r[0]} ({r[1]})")

# 4. Sample shipment → order link
print("\n=== Sample acc_shipment_order_link ===")
cur.execute("SELECT TOP 5 * FROM dbo.acc_shipment_order_link WITH (NOLOCK)")
cols = [d[0] for d in cur.description]
print(f"  {cols}")
for r in cur.fetchall():
    print(f"  {list(r)}")

# 5. Sample shipment
print("\n=== Sample acc_shipment ===")
cur.execute("SELECT TOP 5 * FROM dbo.acc_shipment WITH (NOLOCK)")
cols = [d[0] for d in cur.description]
print(f"  {cols}")
for r in cur.fetchall():
    print(f"  {[str(x)[:40] for x in r]}")

# 6. Count shipments and their link to GLS
print("\n=== Shipment counts ===")
cur.execute("SELECT COUNT(*) FROM dbo.acc_shipment WITH (NOLOCK)")
print(f"  acc_shipment: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM dbo.acc_shipment_order_link WITH (NOLOCK)")
print(f"  acc_shipment_order_link: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM dbo.acc_shipment_cost WITH (NOLOCK)")
print(f"  acc_shipment_cost: {cur.fetchone()[0]:,}")

# 7. Check if shipment has tracking/parcel number that links to GLS billing
print("\n=== acc_shipment carrier distribution ===")
cur.execute("""
    SELECT TOP 10 carrier, COUNT(*) cnt
    FROM dbo.acc_shipment WITH (NOLOCK)
    GROUP BY carrier
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")

conn.close()
