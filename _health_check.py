"""One-time health check script — safe to delete after use."""
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

cur.execute("SELECT 1 AS health_check")
row = cur.fetchone()
print(f"Azure SQL: OK (result={row[0]})")

cur.execute(
    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
    "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME LIKE 'acc_%'"
)
tables = cur.fetchone()[0]
print(f"ACC tables in DB: {tables}")

cur.execute("SELECT COUNT(*) FROM dbo.acc_order WITH (NOLOCK)")
orders = cur.fetchone()[0]
print(f"Orders in DB: {orders:,}")

cur.execute("SELECT COUNT(*) FROM dbo.acc_user WITH (NOLOCK)")
users = cur.fetchone()[0]
print(f"Users in DB: {users}")

conn.close()
print("DB connection: CLOSED cleanly")
