"""Check and fix marketplace column, then populate."""
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

# Check width
cur.execute(
    "SELECT c.max_length FROM sys.columns c "
    "JOIN sys.tables t ON c.object_id=t.object_id "
    "WHERE t.name='seasonality_monthly_metrics' AND c.name='marketplace'"
)
row = cur.fetchone()
width = row[0] if row else 0
print(f"marketplace max_length = {width} (NVARCHAR bytes, /2 = {width//2} chars)")

if width < 40:  # 40 bytes = 20 NVARCHAR chars
    print("Widening to NVARCHAR(20)...")
    # Drop indexes that reference marketplace
    cur.execute(
        "SELECT i.name FROM sys.indexes i "
        "JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id "
        "JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id "
        "WHERE OBJECT_NAME(i.object_id)='seasonality_monthly_metrics' "
        "AND c.name='marketplace' AND i.is_primary_key=0"
    )
    for (idx_name,) in cur.fetchall():
        print(f"  Dropping index {idx_name}")
        cur.execute(f"DROP INDEX [{idx_name}] ON [seasonality_monthly_metrics]")
        conn.commit()

    cur.execute("ALTER TABLE seasonality_monthly_metrics ALTER COLUMN marketplace NVARCHAR(20) NOT NULL")
    conn.commit()
    print("  Column widened.")

    # Recreate indexes
    cur.execute("CREATE UNIQUE INDEX UX_season_monthly ON seasonality_monthly_metrics (marketplace, entity_type, entity_id, year, month)")
    cur.execute("CREATE INDEX IX_season_monthly_entity ON seasonality_monthly_metrics (entity_type, entity_id, marketplace)")
    conn.commit()
    print("  Indexes recreated.")
else:
    print("Already wide enough, skipping.")

conn.close()
print("Done.")
