"""Phase 4 DDL: add cm1_pln / cm2_pln to executive_daily_metrics."""
from apps.api.app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True, timeout=30)
cur = conn.cursor()

cur.execute(
    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
    "WHERE TABLE_NAME = 'executive_daily_metrics' AND COLUMN_NAME IN ('cm1_pln','cm2_pln')"
)
existing = [r[0] for r in cur.fetchall()]
print("Existing columns:", existing)

if "cm1_pln" not in existing:
    cur.execute("ALTER TABLE dbo.executive_daily_metrics ADD cm1_pln DECIMAL(18,2) NULL")
    print("Added cm1_pln")
else:
    print("cm1_pln already exists")

if "cm2_pln" not in existing:
    cur.execute("ALTER TABLE dbo.executive_daily_metrics ADD cm2_pln DECIMAL(18,2) NULL")
    print("Added cm2_pln")
else:
    print("cm2_pln already exists")

conn.close()
print("DDL migration done")
