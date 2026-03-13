"""Recreate indexes that were dropped during column widening."""
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

indexes = [
    ("UX_season_profile", "seasonality_profile", "(marketplace, entity_type, entity_id)", True),
    ("IX_season_profile_class", "seasonality_profile", "(seasonality_class, marketplace)", False),
    ("IX_season_opp_status", "seasonality_opportunity", "(status, marketplace)", False),
    ("IX_season_opp_entity", "seasonality_opportunity", "(marketplace, entity_type, entity_id)", False),
    ("IX_season_opp_type", "seasonality_opportunity", "(opportunity_type, marketplace)", False),
]

for name, tbl, cols, unique in indexes:
    u = "UNIQUE " if unique else ""
    try:
        cur.execute(f"CREATE {u}INDEX [{name}] ON [{tbl}] {cols}")
        conn.commit()
        print(f"  Created {name}")
    except Exception as e:
        conn.rollback()
        if "already exists" in str(e):
            print(f"  {name} already exists, skip")
        else:
            print(f"  {name} ERROR: {e}")

conn.close()
print("Index recreation done.")
