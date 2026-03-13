from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

# Table counts
for t in ['seasonality_monthly_metrics', 'seasonality_index_cache', 'seasonality_profile', 'seasonality_opportunity']:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"{t}: {cur.fetchone()[0]} rows")

# Index cache - how many entities and how many months per entity
cur.execute("""
    SELECT entity_type, COUNT(DISTINCT marketplace + '|' + entity_id) AS entities,
           AVG(cnt) AS avg_months, MIN(cnt) AS min_months, MAX(cnt) AS max_months
    FROM (
        SELECT marketplace, entity_type, entity_id, COUNT(*) cnt
        FROM seasonality_index_cache
        GROUP BY marketplace, entity_type, entity_id
    ) x
    GROUP BY entity_type
""")
for row in cur.fetchall():
    print(f"Index cache: type={row[0]}, entities={row[1]}, avg_months={row[2]}, min={row[3]}, max={row[4]}")

# How many have >= 6 months
cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT marketplace, entity_type, entity_id
        FROM seasonality_index_cache
        GROUP BY marketplace, entity_type, entity_id
        HAVING COUNT(*) >= 6
    ) x
""")
print(f"Entities with >=6 months in index cache: {cur.fetchone()[0]}")

# Sample profiles
cur.execute("SELECT TOP 5 marketplace, entity_type, entity_id, seasonality_class, demand_strength_score FROM seasonality_profile")
for row in cur.fetchall():
    print(f"Profile: {row}")

conn.close()
