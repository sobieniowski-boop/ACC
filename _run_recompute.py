"""Run recompute_indices to blend Brand Analytics search demand into seasonality."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

import structlog, logging
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

from app.services.seasonality_service import recompute_indices
from app.core.db_connection import connect_acc

print("=== Recompute Seasonality Indices (with search demand blending) ===")
result = recompute_indices()
print(f"\nResult: {result}")

# Check search_demand_index values
conn = connect_acc()
cur = conn.cursor()
cur.execute("""
    SELECT
        COUNT(*) as total_cached,
        SUM(CASE WHEN search_demand_index IS NOT NULL AND search_demand_index > 0 THEN 1 ELSE 0 END) as with_search,
        AVG(CASE WHEN search_demand_index > 0 THEN search_demand_index END) as avg_search_idx,
        MIN(CASE WHEN search_demand_index > 0 THEN search_demand_index END) as min_search_idx,
        MAX(CASE WHEN search_demand_index > 0 THEN search_demand_index END) as max_search_idx
    FROM seasonality_index_cache
""")
row = cur.fetchone()
print(f"\nIndex cache stats:")
print(f"  Total rows: {row[0]:,}")
print(f"  With search_demand_index: {row[1]:,}")
print(f"  Avg search_demand_index: {row[2]}")
print(f"  Min: {row[3]}, Max: {row[4]}")

# Check demand_index distribution
cur.execute("""
    SELECT TOP 10 marketplace, entity_id, month,
           demand_index, search_demand_index
    FROM seasonality_index_cache
    WHERE search_demand_index > 0
    ORDER BY search_demand_index DESC
""")
print(f"\nTop 10 SKUs by search_demand_index:")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]} | month={r[2]} | demand={r[3]:.4f} | search={r[4]:.4f}")
conn.close()
