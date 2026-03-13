import sys
sys.path.insert(0, r'C:\ACC\apps\api')
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True)
cur = conn.cursor()

ghost_asin = "B07YC444C8"

# Check registry
cur.execute(
    "SELECT asin, merchant_sku, parent_asin, product_name "
    "FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) "
    "WHERE asin = ? OR parent_asin = ?",
    ghost_asin, ghost_asin
)
rows = cur.fetchall()
print(f"Registry matches for {ghost_asin}: {len(rows)}")
for r in rows[:5]:
    print(f"  asin={r[0]}, sku={r[1]}, parent_asin={r[2]}, name={str(r[3])[:60]}")

# Check acc_product
cur.execute(
    "SELECT asin, sku, parent_asin, is_parent "
    "FROM dbo.acc_product WITH (NOLOCK) "
    "WHERE asin = ? OR parent_asin = ?",
    ghost_asin, ghost_asin
)
rows2 = cur.fetchall()
print(f"\nacc_product matches for {ghost_asin}: {len(rows2)}")
for r in rows2[:5]:
    print(f"  asin={r[0]}, sku={r[1]}, parent_asin={r[2]}, is_parent={r[3]}")

# Check marketplace_listing_child
cur.execute(
    "SELECT TOP 10 asin, sku, current_parent_asin, marketplace "
    "FROM dbo.marketplace_listing_child WITH (NOLOCK) "
    "WHERE current_parent_asin = ? AND marketplace = 'FR'",
    ghost_asin
)
rows3 = cur.fetchall()
print(f"\nmarketplace_listing_child (FR, parent={ghost_asin}): {len(rows3)}")
for r in rows3[:5]:
    print(f"  asin={r[0]}, sku={r[1]}, parent_asin={r[2]}")

# Also check what parent_asin the children currently report
cur.execute(
    "SELECT TOP 5 asin, current_parent_asin "
    "FROM dbo.marketplace_listing_child WITH (NOLOCK) "
    "WHERE marketplace = 'FR' "
    "  AND asin IN (SELECT de_child_asin FROM dbo.global_family_child WITH (NOLOCK) WHERE global_family_id = 1367)"
)
rows4 = cur.fetchall()
print(f"\nFamily 1367 children on FR - current_parent_asin in DB:")
for r in rows4[:10]:
    print(f"  asin={r[0]}, current_parent_asin={r[1]}")

conn.close()
