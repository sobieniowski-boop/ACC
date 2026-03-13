"""Diagnostic: FBM logistics cost analysis."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from app.connectors.mssql import connect_acc

conn = connect_acc()
cur = conn.cursor()

# 1. Dedup check
cur.execute("""
    SELECT 
        versions_per_order,
        COUNT(*) AS order_count
    FROM (
        SELECT amazon_order_id, COUNT(DISTINCT calc_version) AS versions_per_order
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        GROUP BY amazon_order_id
    ) x
    GROUP BY versions_per_order
    ORDER BY versions_per_order
""")
print("=== Calc versions per order ===")
for r in cur.fetchall():
    print(f"  {r[0]} version(s): {r[1]:,} orders")

# 2. Avg cost by marketplace (last 30d)
cur.execute("""
    SELECT 
        m.code,
        COUNT(DISTINCT o.id) AS fbm_orders,
        COUNT(DISTINCT CASE WHEN olf.total_logistics_pln > 0 THEN olf.amazon_order_id END) AS has_cost,
        COUNT(DISTINCT CASE WHEN ISNULL(olf.total_logistics_pln, 0) = 0 THEN olf.amazon_order_id END) AS zero_cost,
        CASE WHEN COUNT(DISTINCT CASE WHEN olf.total_logistics_pln > 0 THEN olf.amazon_order_id END) > 0
            THEN SUM(CASE WHEN olf.total_logistics_pln > 0 THEN olf.total_logistics_pln ELSE 0 END)
                 / COUNT(DISTINCT CASE WHEN olf.total_logistics_pln > 0 THEN olf.amazon_order_id END)
            ELSE 0 END AS avg_cost_nonzero,
        CASE WHEN COUNT(DISTINCT olf.amazon_order_id) > 0
            THEN SUM(ISNULL(olf.total_logistics_pln, 0)) / COUNT(DISTINCT olf.amazon_order_id)
            ELSE 0 END AS avg_cost_all
    FROM dbo.acc_order o WITH (NOLOCK)
    LEFT JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    LEFT JOIN dbo.acc_marketplace m WITH (NOLOCK)
        ON m.id = o.marketplace_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.status IN ('Shipped', 'Unshipped')
      AND o.purchase_date >= DATEADD(day, -30, GETDATE())
    GROUP BY o.marketplace_id, m.code
    ORDER BY COUNT(DISTINCT o.id) DESC
""")
print()
print("=== FBM avg cost (last 30d) ===")
fmt = "{:>4} {:>6} {:>6} {:>6} {:>10} {:>10}"
print(fmt.format("MKT", "FBM", "w/cost", "zero", "avg(>0)", "avg(all)"))
for r in cur.fetchall():
    print(fmt.format(r[0] or "?", r[1], r[2], r[3], f"{r[4]:.2f}", f"{r[5]:.2f}"))

# 3. Actual vs estimated
cur.execute("""
    SELECT 
        m.code,
        SUM(CASE WHEN olf.actual_shipments_count > 0 THEN 1 ELSE 0 END) AS actual_rows,
        SUM(CASE WHEN olf.estimated_shipments_count > 0 AND ISNULL(olf.actual_shipments_count,0) = 0 THEN 1 ELSE 0 END) AS estimated_rows,
        AVG(CASE WHEN olf.actual_shipments_count > 0 THEN olf.total_logistics_pln END) AS avg_actual,
        AVG(CASE WHEN olf.estimated_shipments_count > 0 AND ISNULL(olf.actual_shipments_count,0) = 0 THEN olf.total_logistics_pln END) AS avg_estimated
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    LEFT JOIN dbo.acc_marketplace m WITH (NOLOCK)
        ON m.id = o.marketplace_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.status IN ('Shipped', 'Unshipped')
      AND o.purchase_date >= DATEADD(day, -30, GETDATE())
      AND olf.total_logistics_pln > 0
    GROUP BY o.marketplace_id, m.code
    ORDER BY COUNT(*) DESC
""")
print()
print("=== Actual vs Estimated costs (last 30d, nonzero) ===")
fmt2 = "{:>4} {:>8} {:>8} {:>10} {:>10}"
print(fmt2.format("MKT", "actual", "estim", "avg_act", "avg_est"))
for r in cur.fetchall():
    print(fmt2.format(r[0] or "?", r[1], r[2], f"{(r[3] or 0):.2f}", f"{(r[4] or 0):.2f}"))

# 4. Check total cost in acc_shipment_cost (actual billing) vs logistics fact
cur.execute("""
    SELECT TOP 10
        m.code AS mkt,
        o.amazon_order_id,
        olf.calc_version,
        olf.total_logistics_pln AS fact_cost,
        olf.actual_shipments_count,
        olf.estimated_shipments_count,
        (SELECT SUM(sc.net_amount + ISNULL(sc.fuel_amount,0) + ISNULL(sc.toll_amount,0))
         FROM dbo.acc_shipment_order_link sol WITH (NOLOCK)
         JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
         WHERE sol.amazon_order_id = o.amazon_order_id AND sc.net_amount > 0
        ) AS real_cost_sum
    FROM dbo.acc_order o WITH (NOLOCK)
    INNER JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    LEFT JOIN dbo.acc_marketplace m WITH (NOLOCK)
        ON m.id = o.marketplace_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.marketplace_id = 'A1PA6795UKMFR9'  -- DE
      AND o.purchase_date >= DATEADD(day, -30, GETDATE())
      AND olf.total_logistics_pln > 0
    ORDER BY olf.total_logistics_pln ASC
""")
print()
print("=== Sample DE orders (lowest cost first) ===")
fmt3 = "{:>4} {:>22} {:>8} {:>10} {:>6} {:>6} {:>10}"
print(fmt3.format("MKT", "order_id", "version", "fact_cost", "act", "est", "real_cost"))
for r in cur.fetchall():
    print(fmt3.format(r[0] or "?", r[1][:22], r[2] or "?", f"{r[3]:.2f}", r[4], r[5], f"{(r[6] or 0):.2f}"))

conn.close()
