"""Check FK references and try to drop UQ_acc_product_asin."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True)
cur = conn.cursor()
cur.execute("SET LOCK_TIMEOUT 30000")

# Check FKs referencing acc_product
print("=== FK referencing acc_product ===")
cur.execute(
    "SELECT fk.name AS fk_name, "
    "OBJECT_NAME(fk.parent_object_id) AS child_table, "
    "COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS child_col, "
    "COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_col "
    "FROM sys.foreign_keys fk "
    "JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id "
    "WHERE fk.referenced_object_id = OBJECT_ID('acc_product')"
)
fks = cur.fetchall()
for r in fks:
    print(f"  {r[0]}: {r[1]}.{r[2]} -> acc_product.{r[3]}")

# Check FKs specifically on asin column
print("\n=== FK on asin column ===")
asin_fks = [r for r in fks if r[3] == 'asin']
print(f"  Found: {len(asin_fks)}")
for r in asin_fks:
    print(f"  {r[0]}: {r[1]}.{r[2]} -> acc_product.asin")

# If asin FKs exist, drop them first, then the constraint
if asin_fks:
    for r in asin_fks:
        fk_name = r[0]
        child_table = r[1]
        print(f"\nDropping FK {fk_name} on {child_table}...")
        cur.execute(f"ALTER TABLE [{child_table}] DROP CONSTRAINT [{fk_name}]")
        print(f"  Dropped.")

# Now try dropping the UNIQUE constraint again
print("\nDropping UQ_acc_product_asin...")
try:
    cur.execute("ALTER TABLE acc_product DROP CONSTRAINT [UQ_acc_product_asin]")
    print("  SUCCESS!")
except Exception as e:
    print(f"  Failed: {e}")

# Also drop the extra filtered index we just created (we'll recreate if needed)
print("\nDropping UQ_acc_product_asin_filtered...")
try:
    cur.execute("DROP INDEX [UQ_acc_product_asin_filtered] ON acc_product")
    print("  Dropped.")
except Exception as e:
    print(f"  Not found or failed: {e}")

# Create proper filtered index
print("\nCreating filtered unique index...")
try:
    cur.execute(
        "CREATE UNIQUE NONCLUSTERED INDEX UQ_acc_product_asin "
        "ON acc_product(asin) "
        "WHERE asin IS NOT NULL"
    )
    print("  Created UQ_acc_product_asin (filtered).")
except Exception as e:
    print(f"  Failed: {e}")

# Verify
print("\n=== Final state ===")
cur.execute(
    "SELECT i.name, i.is_unique, i.filter_definition "
    "FROM sys.indexes i "
    "WHERE i.object_id = OBJECT_ID('acc_product') "
    "AND i.name LIKE '%asin%'"
)
for r in cur.fetchall():
    print(f"  {r[0]}: unique={r[1]}, filter={r[2]}")

conn.close()
