"""Fix UQ_acc_product_asin: allow multiple NULLs by using a filtered unique index."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True)
cur = conn.cursor()
cur.execute("SET LOCK_TIMEOUT 30000")

# 1) Find the constraint/index name
print("Looking for ASIN constraints...")
cur.execute(
    "SELECT name FROM sys.key_constraints "
    "WHERE parent_object_id = OBJECT_ID('acc_product') "
    "AND name LIKE '%asin%'"
)
constraints = [r[0] for r in cur.fetchall()]
print(f"  Key constraints: {constraints}")

cur.execute(
    "SELECT i.name, i.is_unique, i.filter_definition "
    "FROM sys.indexes i "
    "WHERE i.object_id = OBJECT_ID('acc_product') "
    "AND i.name LIKE '%asin%'"
)
indexes = cur.fetchall()
print(f"  Indexes: {indexes}")

# 2) Drop the UNIQUE constraint (try both methods)
for c_name in constraints:
    print(f"\nDropping constraint: {c_name}")
    try:
        cur.execute(f"ALTER TABLE acc_product DROP CONSTRAINT [{c_name}]")
        print(f"  Dropped via ALTER TABLE.")
    except Exception as e1:
        print(f"  ALTER TABLE failed: {e1}")
        try:
            cur.execute(f"DROP INDEX [{c_name}] ON acc_product")
            print(f"  Dropped via DROP INDEX.")
        except Exception as e2:
            print(f"  DROP INDEX also failed: {e2}")
            # Try with IF EXISTS
            try:
                cur.execute(
                    f"IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='{c_name}' AND object_id=OBJECT_ID('acc_product')) "
                    f"DROP INDEX [{c_name}] ON acc_product"
                )
                print(f"  Dropped via conditional DROP INDEX.")
            except Exception as e3:
                print(f"  All methods failed: {e3}")

# 3) Create filtered unique index (NULL allowed to be duplicated)
print("\nCreating filtered unique index UQ_acc_product_asin_filtered...")
cur.execute(
    "CREATE UNIQUE NONCLUSTERED INDEX UQ_acc_product_asin_filtered "
    "ON acc_product(asin) "
    "WHERE asin IS NOT NULL"
)
print("  Created.")

# 4) Verify
cur.execute(
    "SELECT i.name, i.is_unique, i.filter_definition "
    "FROM sys.indexes i "
    "WHERE i.object_id = OBJECT_ID('acc_product') "
    "AND i.name LIKE '%asin%'"
)
for r in cur.fetchall():
    print(f"  Index: {r[0]}, unique={r[1]}, filter={r[2]}")

conn.close()
print("\nDone. Multiple NULL asins now allowed.")
