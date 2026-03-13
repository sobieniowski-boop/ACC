"""Diagnostic: calc_version breakdown."""
import sys, os
sys.path.insert(0, os.path.join("apps", "api"))

from app.connectors.mssql import connect_acc

conn = connect_acc()
cur = conn.cursor()

# DE: cost by calc_version
cur.execute("""
    SELECT 
        olf.calc_version,
        COUNT(*) AS cnt,
        AVG(olf.total_logistics_pln) AS avg_cost,
        MIN(olf.total_logistics_pln) AS min_cost,
        MAX(olf.total_logistics_pln) AS max_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND o.purchase_date >= DATEADD(day, -30, GETDATE())
      AND olf.total_logistics_pln > 0
    GROUP BY olf.calc_version
    ORDER BY COUNT(*) DESC
""")
print("=== DE: cost by calc_version (last 30d) ===")
for r in cur.fetchall():
    print(f"  {r[0]:>12}: {r[1]:>6} orders, avg={r[2]:.2f}, min={r[3]:.2f}, max={r[4]:.2f}")

# FR: cost by calc_version
cur.execute("""
    SELECT 
        olf.calc_version,
        COUNT(*) AS cnt,
        AVG(olf.total_logistics_pln) AS avg_cost,
        MIN(olf.total_logistics_pln) AS min_cost,
        MAX(olf.total_logistics_pln) AS max_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.marketplace_id = 'A13V1IB3VIYBER'
      AND o.purchase_date >= DATEADD(day, -30, GETDATE())
      AND olf.total_logistics_pln > 0
    GROUP BY olf.calc_version
    ORDER BY COUNT(*) DESC
""")
print()
print("=== FR: cost by calc_version (last 30d) ===")
for r in cur.fetchall():
    print(f"  {r[0]:>12}: {r[1]:>6} orders, avg={r[2]:.2f}, min={r[3]:.2f}, max={r[4]:.2f}")

# PL: cost by calc_version
cur.execute("""
    SELECT 
        olf.calc_version,
        COUNT(*) AS cnt,
        AVG(olf.total_logistics_pln) AS avg_cost,
        MIN(olf.total_logistics_pln) AS min_cost,
        MAX(olf.total_logistics_pln) AS max_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.marketplace_id = 'A1C3SOZRARQ6R3'
      AND o.purchase_date >= DATEADD(day, -30, GETDATE())
      AND olf.total_logistics_pln > 0
    GROUP BY olf.calc_version
    ORDER BY COUNT(*) DESC
""")
print()
print("=== PL: cost by calc_version (last 30d) ===")
for r in cur.fetchall():
    print(f"  {r[0]:>12}: {r[1]:>6} orders, avg={r[2]:.2f}, min={r[3]:.2f}, max={r[4]:.2f}")

# Orders where weight_v3 AND gls_v1 both exist — compare
cur.execute("""
    SELECT TOP 5
        o.amazon_order_id,
        olf_w.total_logistics_pln AS weight_v3_cost,
        olf_g.total_logistics_pln AS gls_v1_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_order_logistics_fact olf_w WITH (NOLOCK)
        ON olf_w.amazon_order_id = o.amazon_order_id AND olf_w.calc_version = 'weight_v3'
    INNER JOIN dbo.acc_order_logistics_fact olf_g WITH (NOLOCK)
        ON olf_g.amazon_order_id = o.amazon_order_id AND olf_g.calc_version = 'gls_v1'
    WHERE o.fulfillment_channel = 'MFN'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
    ORDER BY ABS(olf_w.total_logistics_pln - olf_g.total_logistics_pln) DESC
""")
print()
print("=== DE: weight_v3 vs gls_v1 (orders having both) ===")
for r in cur.fetchall():
    print(f"  {r[0]}: weight_v3={r[1]:.2f}, gls_v1={r[2]:.2f}")

# How the current KPI query handles it (LEFT JOIN no dedup)
cur.execute("""
    SELECT 
        olf.calc_version,
        COUNT(*) AS rows_seen_in_join
    FROM dbo.acc_order o WITH (NOLOCK)
    LEFT JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND o.purchase_date >= DATEADD(day, -30, GETDATE())
      AND olf.total_logistics_pln > 0
    GROUP BY olf.calc_version
    ORDER BY COUNT(*) DESC
""")
print()
print("=== DE: rows in KPI LEFT JOIN by version (30d) ===")
for r in cur.fetchall():
    print(f"  {r[0]:>12}: {r[1]:>6} rows")

conn.close()
