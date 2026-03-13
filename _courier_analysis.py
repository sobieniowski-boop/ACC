"""Courier cost per order analysis — 2026 only."""
import sys, decimal, io
sys.path.insert(0, "apps/api")
from app.connectors.mssql import connect_acc
from datetime import date

# Redirect all output to file
_out = io.StringIO()

conn = connect_acc(autocommit=False, timeout=60)
cur = conn.cursor()

# 1. acc_order_logistics_fact schema
cur.execute("SELECT TOP 1 * FROM dbo.acc_order_logistics_fact WITH (NOLOCK)")
cols_olf = [d[0] for d in cur.description]
print("acc_order_logistics_fact cols:", cols_olf)

# 2. acc_shipment_cost schema
cur.execute("SELECT TOP 1 * FROM dbo.acc_shipment_cost WITH (NOLOCK)")
cols_sc = [d[0] for d in cur.description]
print("acc_shipment_cost cols:", cols_sc)

# 3. Monthly summary: courier costs per order from FACT table — 2026
print("\n=== COURIER COSTS PER ORDER (acc_order_logistics_fact) — 2026 ===")
cur.execute("""
    SELECT
        FORMAT(o.purchase_date, 'yyyy-MM') AS month,
        o.marketplace_id,
        COUNT(DISTINCT o.amazon_order_id) AS orders,
        COUNT(DISTINCT f.amazon_order_id) AS orders_with_fact,
        ROUND(SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS total_logistics_pln,
        ROUND(AVG(ISNULL(f.total_logistics_pln, 0)), 2) AS avg_logistics_pln,
        SUM(CASE WHEN f.total_logistics_pln > 0 THEN 1 ELSE 0 END) AS orders_nonzero,
        SUM(CASE WHEN f.is_estimated = 1 THEN 1 ELSE 0 END) AS orders_estimated,
        SUM(CASE WHEN f.is_estimated = 0 OR f.is_estimated IS NULL THEN 1 ELSE 0 END) AS orders_actual
    FROM dbo.acc_order o WITH (NOLOCK)
    LEFT JOIN (
        SELECT olf.amazon_order_id, olf.total_logistics_pln, olf.is_estimated,
               ROW_NUMBER() OVER (PARTITION BY olf.amazon_order_id ORDER BY olf.calculated_at DESC) AS rn
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
    ) f ON f.amazon_order_id = o.amazon_order_id AND f.rn = 1
    WHERE o.purchase_date >= '2026-01-01'
      AND o.status IN ('Shipped', 'Unshipped')
    GROUP BY FORMAT(o.purchase_date, 'yyyy-MM'), o.marketplace_id
    ORDER BY 1, 2
""")
rows = cur.fetchall()
print(f"{'Month':<10} {'MKT':<20} {'Orders':>8} {'W/Fact':>8} {'TotalPLN':>12} {'AvgPLN':>10} {'NonZero':>8} {'Estim':>8} {'Actual':>8}")
print("-" * 105)
for r in rows:
    vals = [float(v) if isinstance(v, decimal.Decimal) else v for v in r]
    print(f"{vals[0]:<10} {vals[1]:<20} {vals[2]:>8} {vals[3]:>8} {vals[4]:>12.2f} {vals[5]:>10.2f} {vals[6]:>8} {vals[7]:>8} {vals[8]:>8}")

# 4. Compare: acc_order.logistics_pln (legacy) vs acc_order_logistics_fact
print("\n=== COMPARISON: Legacy (acc_order.logistics_pln) vs Fact table — 2026 ===")
cur.execute("""
    SELECT
        FORMAT(o.purchase_date, 'yyyy-MM') AS month,
        COUNT(*) AS orders,
        ROUND(SUM(ISNULL(o.logistics_pln, 0)), 2) AS legacy_logistics_pln,
        ROUND(SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS fact_logistics_pln,
        ROUND(SUM(ISNULL(o.logistics_pln, 0)) - SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS delta,
        SUM(CASE WHEN ISNULL(o.logistics_pln, 0) > 0 AND ISNULL(f.total_logistics_pln, 0) = 0 THEN 1 ELSE 0 END) AS legacy_only,
        SUM(CASE WHEN ISNULL(o.logistics_pln, 0) = 0 AND ISNULL(f.total_logistics_pln, 0) > 0 THEN 1 ELSE 0 END) AS fact_only,
        SUM(CASE WHEN ISNULL(o.logistics_pln, 0) > 0 AND ISNULL(f.total_logistics_pln, 0) > 0
                  AND ABS(o.logistics_pln - f.total_logistics_pln) > 0.01 THEN 1 ELSE 0 END) AS both_differ
    FROM dbo.acc_order o WITH (NOLOCK)
    LEFT JOIN (
        SELECT olf.amazon_order_id, olf.total_logistics_pln,
               ROW_NUMBER() OVER (PARTITION BY olf.amazon_order_id ORDER BY olf.calculated_at DESC) AS rn
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
    ) f ON f.amazon_order_id = o.amazon_order_id AND f.rn = 1
    WHERE o.purchase_date >= '2026-01-01'
      AND o.status IN ('Shipped', 'Unshipped')
    GROUP BY FORMAT(o.purchase_date, 'yyyy-MM')
    ORDER BY 1
""")
rows2 = cur.fetchall()
print(f"{'Month':<10} {'Orders':>8} {'Legacy PLN':>12} {'Fact PLN':>12} {'Delta':>10} {'LegOnly':>8} {'FactOnly':>8} {'Differ':>8}")
print("-" * 85)
for r in rows2:
    vals = [float(v) if isinstance(v, decimal.Decimal) else v for v in r]
    print(f"{vals[0]:<10} {vals[1]:>8} {vals[2]:>12.2f} {vals[3]:>12.2f} {vals[4]:>10.2f} {vals[5]:>8} {vals[6]:>8} {vals[7]:>8}")

# 5. Carrier breakdown from acc_shipment_cost — 2026
print("\n=== CARRIER BREAKDOWN (acc_shipment_cost) — 2026 ===")
cur.execute("""
    SELECT
        FORMAT(s.ship_date, 'yyyy-MM') AS month,
        s.carrier,
        COUNT(DISTINCT s.id) AS shipments,
        ROUND(SUM(ISNULL(c.net_amount, 0) + ISNULL(c.fuel_amount, 0) + ISNULL(c.toll_amount, 0)), 2) AS total_cost,
        ROUND(AVG(ISNULL(c.net_amount, 0) + ISNULL(c.fuel_amount, 0) + ISNULL(c.toll_amount, 0)), 2) AS avg_cost,
        SUM(CASE WHEN c.is_estimated = 1 THEN 1 ELSE 0 END) AS estimated_rows,
        SUM(CASE WHEN c.is_estimated = 0 THEN 1 ELSE 0 END) AS actual_rows
    FROM dbo.acc_shipment s WITH (NOLOCK)
    LEFT JOIN dbo.acc_shipment_cost c WITH (NOLOCK) ON c.shipment_id = s.id
    WHERE s.ship_date >= '2026-01-01'
    GROUP BY FORMAT(s.ship_date, 'yyyy-MM'), s.carrier
    ORDER BY 1, 2
""")
rows3 = cur.fetchall()
print(f"{'Month':<10} {'Carrier':<10} {'Shipments':>10} {'TotalCost':>12} {'AvgCost':>10} {'Estimated':>10} {'Actual':>8}")
print("-" * 80)
for r in rows3:
    vals = [float(v) if isinstance(v, decimal.Decimal) else v for v in r]
    print(f"{vals[0]:<10} {str(vals[1] or 'N/A'):<10} {vals[2]:>10} {vals[3]:>12.2f} {vals[4]:>10.2f} {vals[5]:>10} {vals[6]:>8}")

# 6. Rollup: acc_sku_profitability_rollup — logistics totals 2026
print("\n=== ROLLUP TABLE: logistics_pln per month — 2026 ===")
cur.execute("""
    SELECT
        FORMAT(r.period_date, 'yyyy-MM') AS month,
        r.marketplace_id,
        COUNT(*) AS sku_days,
        ROUND(SUM(r.logistics_pln), 2) AS total_logistics_pln,
        ROUND(AVG(r.logistics_pln), 4) AS avg_per_sku_day,
        SUM(CASE WHEN r.logistics_pln > 0 THEN 1 ELSE 0 END) AS nonzero_rows,
        SUM(CASE WHEN r.logistics_pln = 0 OR r.logistics_pln IS NULL THEN 1 ELSE 0 END) AS zero_rows
    FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
    WHERE r.period_date >= '2026-01-01'
    GROUP BY FORMAT(r.period_date, 'yyyy-MM'), r.marketplace_id
    ORDER BY 1, 2
""")
rows4 = cur.fetchall()
print(f"{'Month':<10} {'MKT':<20} {'SKU-Days':>10} {'TotalPLN':>12} {'AvgPerDay':>10} {'NonZero':>8} {'Zero':>8}")
print("-" * 85)
for r in rows4:
    vals = [float(v) if isinstance(v, decimal.Decimal) else v for v in r]
    print(f"{vals[0]:<10} {vals[1]:<20} {vals[2]:>10} {vals[3]:>12.2f} {vals[4]:>10.4f} {vals[5]:>8} {vals[6]:>8}")

# 7. Check: profit_engine live query — how it gets logistics (sample)
print("\n=== PROFIT_ENGINE LIVE: logistics for sample orders — 2026 Q1 ===")
cur.execute("""
    SELECT TOP 20
        o.amazon_order_id,
        o.marketplace_id,
        CAST(o.purchase_date AS DATE) AS order_date,
        o.fulfillment_channel,
        ISNULL(o.logistics_pln, 0) AS legacy_logistics,
        ISNULL(f.total_logistics_pln, 0) AS fact_logistics,
        CASE WHEN f.total_logistics_pln IS NOT NULL THEN f.total_logistics_pln
             ELSE ISNULL(o.logistics_pln, 0) END AS resolved_logistics,
        f.calc_version,
        f.is_estimated
    FROM dbo.acc_order o WITH (NOLOCK)
    OUTER APPLY (
        SELECT TOP 1 olf.total_logistics_pln, olf.calc_version, olf.is_estimated
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        WHERE olf.amazon_order_id = o.amazon_order_id
        ORDER BY olf.calculated_at DESC
    ) f
    WHERE o.purchase_date >= '2026-01-01'
      AND o.status = 'Shipped'
      AND o.fulfillment_channel = 'MFN'
    ORDER BY o.purchase_date DESC
""")
rows5 = cur.fetchall()
print(f"{'OrderID':<22} {'MKT':<18} {'Date':<12} {'FC':<5} {'Legacy':>8} {'Fact':>8} {'Resolved':>9} {'Version':<15} {'Est':>4}")
print("-" * 120)
for r in rows5:
    vals = [float(v) if isinstance(v, decimal.Decimal) else (str(v) if v else '') for v in r]
    print(f"{vals[0]:<22} {vals[1]:<18} {vals[2]:<12} {vals[3]:<5} {vals[4]:>8} {vals[5]:>8} {vals[6]:>9} {vals[7]:<15} {vals[8]:>4}")

conn.close()
print("\nDone.")
