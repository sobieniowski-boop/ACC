import sys, json
sys.path.insert(0, r'C:\ACC\apps\api')
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True)
cur = conn.cursor()

# Check global_family_child
cur.execute("SELECT TOP 5 * FROM dbo.global_family_child WITH (NOLOCK) WHERE global_family_id = 1367")
rows = cur.fetchall()
print(f"global_family_child rows for family 1367: {len(rows)}")
if rows:
    # Get column names
    cur.execute("""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_NAME='global_family_child' ORDER BY ORDINAL_POSITION""")
    cols = [r[0] for r in cur.fetchall()]
    print(f"Columns: {cols}")
    for r in rows[:3]:
        print(dict(zip(cols, r)))

print()

# Check marketplace_listing_child for FR
cur.execute("""SELECT TOP 5 sku, asin, current_parent_asin, marketplace 
               FROM dbo.marketplace_listing_child WITH (NOLOCK) 
               WHERE marketplace = 'FR' 
               ORDER BY asin""")
rows2 = cur.fetchall()
print(f"marketplace_listing_child FR rows (sample): {len(rows2)}")
for r in rows2:
    print(f"  sku={r[0]}, asin={r[1]}, parent_asin={r[2]}, mp={r[3]}")

print()

# Check marketplace_listing_child WHERE asin in global_family_child
cur.execute("""
    SELECT COUNT(*) FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
    WHERE mlc.marketplace = 'FR'
      AND mlc.asin IN (
          SELECT gfc.de_child_asin FROM dbo.global_family_child gfc WITH (NOLOCK)
          WHERE gfc.global_family_id = 1367
      )
""")
cnt = cur.fetchone()[0]
print(f"Matched children (FR + family 1367): {cnt}")

# Also check if maybe marketplace uses marketplace_id
cur.execute("""
    SELECT COUNT(*) FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
    WHERE mlc.marketplace = 'A13V1IB3VIYZZH'
      AND mlc.asin IN (
          SELECT gfc.de_child_asin FROM dbo.global_family_child gfc WITH (NOLOCK)
          WHERE gfc.global_family_id = 1367
      )
""")
cnt2 = cur.fetchone()[0]
print(f"Matched children (A13V1IB3VIYZZH + family 1367): {cnt2}")

conn.close()
