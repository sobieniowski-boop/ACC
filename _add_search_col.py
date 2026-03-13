"""Add search_demand_index column to seasonality_index_cache."""
import sys
sys.path.insert(0, r"C:\ACC\apps\api")

from app.core.db_connection import connect_acc
conn = connect_acc()
cur = conn.cursor()
cur.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'seasonality_index_cache'
          AND COLUMN_NAME = 'search_demand_index'
    )
    ALTER TABLE seasonality_index_cache
        ADD search_demand_index FLOAT NULL;
""")
conn.commit()

cur.execute("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'seasonality_index_cache' AND COLUMN_NAME = 'search_demand_index'
""")
print("Column exists:", len(cur.fetchall()) > 0)
conn.close()
