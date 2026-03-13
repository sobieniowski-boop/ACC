import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=30)
cur = conn.cursor()

# 1. GLS billing columns
print("=== GLS billing columns ===")
cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_gls_billing_line' ORDER BY ORDINAL_POSITION")
for r in cur.fetchall():
    print(f"  {r[0]} ({r[1]})")

# 2. Sample GLS billing data
print("\n=== Sample GLS billing rows (with weight data) ===")
cur.execute("""
    SELECT TOP 10 
        parcel_number, dimension_combined, weight, volumetric_weight, 
        billing_weight, declared_weight, net_amount, recipient_country
    FROM dbo.acc_gls_billing_line WITH (NOLOCK)
    WHERE dimension_combined IS NOT NULL
    ORDER BY NEWID()
""")
cols = [desc[0] for desc in cur.description]
print(f"  {cols}")
for r in cur.fetchall():
    print(f"  {list(r)}")

# 3. How many GLS rows have dimension data?
print("\n=== GLS dimension data coverage ===")
cur.execute("""
    SELECT 
        COUNT(*) AS total,
        SUM(CASE WHEN dimension_combined IS NOT NULL THEN 1 ELSE 0 END) AS with_dims,
        SUM(CASE WHEN volumetric_weight IS NOT NULL THEN 1 ELSE 0 END) AS with_vol_weight,
        SUM(CASE WHEN billing_weight IS NOT NULL THEN 1 ELSE 0 END) AS with_bill_weight,
        SUM(CASE WHEN recipient_country IS NOT NULL THEN 1 ELSE 0 END) AS with_country
    FROM dbo.acc_gls_billing_line WITH (NOLOCK)
""")
r = cur.fetchone()
print(f"  Total: {r[0]:,}")
print(f"  With dimensions: {r[1]:,}")
print(f"  With vol. weight: {r[2]:,}")
print(f"  With billing weight: {r[3]:,}")
print(f"  With country: {r[4]:,}")

# 4. Can we link GLS to orders? Check if parcel_number matches anything in acc_order
print("\n=== Check GLS→order linkage ===")
cur.execute("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME='acc_order_logistics_fact' 
    ORDER BY ORDINAL_POSITION
""")
print("acc_order_logistics_fact columns:")
for r in cur.fetchall():
    print(f"  {r[0]}")

# 5. Check if there's a shipment tracking table
print("\n=== Tables with 'shipment' or 'tracking' in name ===")
cur.execute("""
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME LIKE '%ship%' OR TABLE_NAME LIKE '%track%' OR TABLE_NAME LIKE '%parcel%'
    ORDER BY TABLE_NAME
""")
for r in cur.fetchall():
    print(f"  {r[0]}")

# 6. Check DHL billing weight vs net_amount correlation
print("\n=== DHL weight → cost samples by country ===")
cur.execute("""
    SELECT TOP 20
        shipper_receiver, weight, net_amount, base_fee, volumetric_fee
    FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
    WHERE weight > 0 AND net_amount > 0
    ORDER BY NEWID()
""")
for r in cur.fetchall():
    print(f"  recv={r[0][:30] if r[0] else '?'}, wt={r[1]}, net={r[2]}, base={r[3]}, vol_fee={r[4]}")

conn.close()
