import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True)
cur = conn.cursor()

# Check table structure first
cur.execute("""
    SELECT TOP 1 *
    FROM dbo.marketplace_listing_child WITH (NOLOCK)
""")

cols = [desc[0] for desc in cur.description]
print(f"Table columns: {', '.join(cols)}\n")

row = cur.fetchone()
if row:
    print("Sample row:")
    for i, col in enumerate(cols):
        print(f"  {col}: {row[i]}")
print("\n" + "="*80 + "\n")

# Now get children by SKU pattern
cur.execute("""
    SELECT TOP 10 *
    FROM dbo.marketplace_listing_child WITH (NOLOCK)
    WHERE sku LIKE 'FBA_5902730%'
    ORDER BY sku
""")

rows = cur.fetchall()
print(f"Found {len(rows)} children on FR marketplace:\n")

has_parents = 0
for r in rows:
    has_parent = r[2] is not None and r[2] != ''
    if has_parent:
        has_parents += 1
    print(f"SKU: {r[1]}")
    print(f"  ASIN: {r[0]}")
    print(f"  Current Parent: {r[2] if has_parent else '(NONE)'}")
    print(f"  Theme: {r[3]}")
    print()

print(f"Summary: {has_parents}/{len(rows)} children have a parent assigned")

conn.close()
