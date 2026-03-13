"""Verify search term tables creation."""
import sys
sys.path.insert(0, r"C:\ACC\apps\api")

from app.services.search_term_sync import ensure_tables
ensure_tables()
print("Tables ensured OK")

from app.core.db_connection import connect_acc
conn = connect_acc()
cur = conn.cursor()
cur.execute("""
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME IN ('acc_search_term_weekly', 'acc_search_term_monthly')
""")
tables = [r[0] for r in cur.fetchall()]
conn.close()
print("Tables found:", tables)

# Verify seasonality_index_cache has search_demand_index column
conn = connect_acc()
cur = conn.cursor()
cur.execute("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'seasonality_index_cache' AND COLUMN_NAME = 'search_demand_index'
""")
cols = [r[0] for r in cur.fetchall()]
conn.close()
print("search_demand_index column exists:", len(cols) > 0)
