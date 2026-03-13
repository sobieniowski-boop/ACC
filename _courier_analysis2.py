"""Courier cost per order analysis — 2026 only. Output to file."""
import sys, decimal
sys.path.insert(0, "apps/api")
from app.connectors.mssql import connect_acc

OUT = "C:/ACC/_courier_data.txt"

def main():
    conn = connect_acc(autocommit=False, timeout=60)
    cur = conn.cursor()
    lines = []
    P = lines.append

    # 1. Schemas
    cur.execute("SELECT TOP 1 * FROM dbo.acc_order_logistics_fact WITH (NOLOCK)")
    P("acc_order_logistics_fact cols: " + str([d[0] for d in cur.description]))
    cur.execute("SELECT TOP 1 * FROM dbo.acc_shipment_cost WITH (NOLOCK)")
    P("acc_shipment_cost cols: " + str([d[0] for d in cur.description]))

    # 2. Monthly courier cost per order from FACT table
    P("")
    P("=== COURIER COSTS PER ORDER (acc_order_logistics_fact) — 2026 ===")
    cur.execute("""
        SELECT
            FORMAT(o.purchase_date, 'yyyy-MM') AS month,
            o.marketplace_id,
            COUNT(DISTINCT o.amazon_order_id) AS orders,
            COUNT(DISTINCT f.amazon_order_id) AS orders_with_fact,
            ROUND(SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS total_logistics_pln,
            ROUND(AVG(CASE WHEN f.total_logistics_pln > 0 THEN f.total_logistics_pln END), 2) AS avg_nonzero_logistics,
            SUM(CASE WHEN f.total_logistics_pln > 0 THEN 1 ELSE 0 END) AS orders_nonzero,
            SUM(CASE WHEN f.is_estimated = 1 THEN 1 ELSE 0 END) AS estimated,
            SUM(CASE WHEN ISNULL(f.is_estimated, 0) = 0 AND f.total_logistics_pln IS NOT NULL THEN 1 ELSE 0 END) AS actual
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
    P(f"{'Month':<10} {'MKT':<20} {'Orders':>8} {'W/Fact':>8} {'TotalPLN':>12} {'AvgNZ':>10} {'NonZero':>8} {'Estim':>8} {'Actual':>8}")
    P("-" * 105)
    for r in cur.fetchall():
        v = [float(x) if isinstance(x, decimal.Decimal) else x for x in r]
        avg_v = v[5] if v[5] is not None else 0.0
        P(f"{v[0]:<10} {v[1]:<20} {v[2]:>8} {v[3]:>8} {v[4]:>12.2f} {avg_v:>10.2f} {v[6]:>8} {v[7]:>8} {v[8]:>8}")

    # 3. Legacy vs Fact comparison 
    P("")
    P("=== LEGACY (acc_order.logistics_pln) vs FACT TABLE — 2026 ===")
    cur.execute("""
        SELECT
            FORMAT(o.purchase_date, 'yyyy-MM') AS month,
            COUNT(*) AS orders,
            ROUND(SUM(ISNULL(o.logistics_pln, 0)), 2) AS legacy_total,
            ROUND(SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS fact_total,
            ROUND(SUM(ISNULL(o.logistics_pln, 0)) - SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS delta,
            SUM(CASE WHEN ISNULL(o.logistics_pln,0) > 0 AND ISNULL(f.total_logistics_pln,0) = 0 THEN 1 ELSE 0 END) AS legacy_only,
            SUM(CASE WHEN ISNULL(o.logistics_pln,0) = 0 AND ISNULL(f.total_logistics_pln,0) > 0 THEN 1 ELSE 0 END) AS fact_only,
            SUM(CASE WHEN ISNULL(o.logistics_pln,0) > 0 AND ISNULL(f.total_logistics_pln,0) > 0
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
    P(f"{'Month':<10} {'Orders':>8} {'LegacyPLN':>12} {'FactPLN':>12} {'Delta':>10} {'LegOnly':>8} {'FctOnly':>8} {'Differ':>8}")
    P("-" * 85)
    for r in cur.fetchall():
        v = [float(x) if isinstance(x, decimal.Decimal) else x for x in r]
        P(f"{v[0]:<10} {v[1]:>8} {v[2]:>12.2f} {v[3]:>12.2f} {v[4]:>10.2f} {v[5]:>8} {v[6]:>8} {v[7]:>8}")

    # 4. Carrier breakdown from shipment_cost
    P("")
    P("=== CARRIER BREAKDOWN (acc_shipment_cost) — 2026 ===")
    cur.execute("""
        SELECT
            FORMAT(s.ship_date, 'yyyy-MM') AS month,
            ISNULL(s.carrier, 'N/A') AS carrier,
            COUNT(DISTINCT s.id) AS shipments,
            ROUND(SUM(ISNULL(c.net_amount, 0) + ISNULL(c.fuel_amount, 0) + ISNULL(c.toll_amount, 0)), 2) AS total_cost,
            ROUND(AVG(CASE WHEN (ISNULL(c.net_amount,0)+ISNULL(c.fuel_amount,0)+ISNULL(c.toll_amount,0)) > 0
                THEN ISNULL(c.net_amount,0)+ISNULL(c.fuel_amount,0)+ISNULL(c.toll_amount,0) END), 2) AS avg_cost,
            SUM(CASE WHEN c.is_estimated = 1 THEN 1 ELSE 0 END) AS est,
            SUM(CASE WHEN ISNULL(c.is_estimated,0) = 0 THEN 1 ELSE 0 END) AS actual
        FROM dbo.acc_shipment s WITH (NOLOCK)
        LEFT JOIN dbo.acc_shipment_cost c WITH (NOLOCK) ON c.shipment_id = s.id
        WHERE s.ship_date >= '2026-01-01'
        GROUP BY FORMAT(s.ship_date, 'yyyy-MM'), ISNULL(s.carrier, 'N/A')
        ORDER BY 1, 2
    """)
    P(f"{'Month':<10} {'Carrier':<10} {'Shipments':>10} {'TotalCost':>12} {'AvgCost':>10} {'Estimated':>10} {'Actual':>8}")
    P("-" * 80)
    for r in cur.fetchall():
        v = [float(x) if isinstance(x, decimal.Decimal) else x for x in r]
        avg_v = v[4] if v[4] is not None else 0.0
        P(f"{v[0]:<10} {str(v[1]):<10} {v[2]:>10} {v[3]:>12.2f} {avg_v:>10.2f} {v[5]:>10} {v[6]:>8}")

    # 5. Rollup table logistics
    P("")
    P("=== ROLLUP TABLE (acc_sku_profitability_rollup) logistics — 2026 ===")
    cur.execute("""
        SELECT
            FORMAT(r.period_date, 'yyyy-MM') AS month,
            r.marketplace_id,
            COUNT(*) AS sku_days,
            ROUND(SUM(ISNULL(r.logistics_pln, 0)), 2) AS total_logistics,
            SUM(CASE WHEN ISNULL(r.logistics_pln,0) > 0 THEN 1 ELSE 0 END) AS nonzero,
            SUM(CASE WHEN ISNULL(r.logistics_pln,0) = 0 THEN 1 ELSE 0 END) AS zero
        FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
        WHERE r.period_date >= '2026-01-01'
        GROUP BY FORMAT(r.period_date, 'yyyy-MM'), r.marketplace_id
        ORDER BY 1, 2
    """)
    P(f"{'Month':<10} {'MKT':<20} {'SKUDays':>10} {'TotalPLN':>12} {'NonZero':>8} {'Zero':>8}")
    P("-" * 75)
    for r in cur.fetchall():
        v = [float(x) if isinstance(x, decimal.Decimal) else x for x in r]
        P(f"{v[0]:<10} {v[1]:<20} {v[2]:>10} {v[3]:>12.2f} {v[4]:>8} {v[5]:>8}")

    # 6. Sample MFN orders with logistics detail
    P("")
    P("=== SAMPLE MFN ORDERS — logistics detail (recent 20) ===")
    cur.execute("""
        SELECT TOP 20
            o.amazon_order_id,
            o.marketplace_id,
            CAST(o.purchase_date AS DATE) AS d,
            o.fulfillment_channel,
            ROUND(ISNULL(o.logistics_pln, 0), 2) AS legacy,
            ROUND(ISNULL(f.total_logistics_pln, 0), 2) AS fact,
            f.calc_version,
            ISNULL(f.is_estimated, -1) AS is_est
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
    P(f"{'OrderID':<22} {'MKT':<18} {'Date':<12} {'FC':<5} {'Legacy':>8} {'Fact':>8} {'Version':<16} {'Est':>4}")
    P("-" * 100)
    for r in cur.fetchall():
        v = [float(x) if isinstance(x, decimal.Decimal) else x for x in r]
        P(f"{v[0]:<22} {v[1]:<18} {str(v[2]):<12} {v[3]:<5} {v[4]:>8.2f} {v[5]:>8.2f} {str(v[6] or ''):<16} {v[7]:>4}")

    # 7. AFN orders — do they have logistics?
    P("")
    P("=== AFN (FBA) ORDERS — logistics check — 2026 ===")
    cur.execute("""
        SELECT
            FORMAT(o.purchase_date, 'yyyy-MM') AS month,
            COUNT(*) AS afn_orders,
            SUM(CASE WHEN ISNULL(o.logistics_pln, 0) > 0 THEN 1 ELSE 0 END) AS legacy_nonzero,
            SUM(CASE WHEN f.total_logistics_pln > 0 THEN 1 ELSE 0 END) AS fact_nonzero,
            ROUND(SUM(ISNULL(o.logistics_pln, 0)), 2) AS legacy_total,
            ROUND(SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS fact_total
        FROM dbo.acc_order o WITH (NOLOCK)
        LEFT JOIN (
            SELECT olf.amazon_order_id, olf.total_logistics_pln,
                   ROW_NUMBER() OVER (PARTITION BY olf.amazon_order_id ORDER BY olf.calculated_at DESC) AS rn
            FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ) f ON f.amazon_order_id = o.amazon_order_id AND f.rn = 1
        WHERE o.purchase_date >= '2026-01-01'
          AND o.status IN ('Shipped', 'Unshipped')
          AND o.fulfillment_channel = 'AFN'
        GROUP BY FORMAT(o.purchase_date, 'yyyy-MM')
        ORDER BY 1
    """)
    P(f"{'Month':<10} {'AFN Ord':>8} {'LegNZ':>8} {'FctNZ':>8} {'LegTotal':>12} {'FctTotal':>12}")
    P("-" * 65)
    for r in cur.fetchall():
        v = [float(x) if isinstance(x, decimal.Decimal) else x for x in r]
        P(f"{v[0]:<10} {v[1]:>8} {v[2]:>8} {v[3]:>8} {v[4]:>12.2f} {v[5]:>12.2f}")

    # 8. Grand totals 2026
    P("")
    P("=== GRAND TOTALS 2026 ===")
    cur.execute("""
        SELECT
            COUNT(DISTINCT o.amazon_order_id) AS total_orders,
            COUNT(DISTINCT f.amazon_order_id) AS orders_with_fact,
            ROUND(SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS fact_total_pln,
            ROUND(SUM(ISNULL(o.logistics_pln, 0)), 2) AS legacy_total_pln,
            ROUND(AVG(CASE WHEN f.total_logistics_pln > 0 THEN f.total_logistics_pln END), 2) AS avg_nonzero_fact
        FROM dbo.acc_order o WITH (NOLOCK)
        LEFT JOIN (
            SELECT olf.amazon_order_id, olf.total_logistics_pln,
                   ROW_NUMBER() OVER (PARTITION BY olf.amazon_order_id ORDER BY olf.calculated_at DESC) AS rn
            FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ) f ON f.amazon_order_id = o.amazon_order_id AND f.rn = 1
        WHERE o.purchase_date >= '2026-01-01'
          AND o.status IN ('Shipped', 'Unshipped')
    """)
    r = cur.fetchone()
    v = [float(x) if isinstance(x, decimal.Decimal) else x for x in r]
    P(f"Total orders: {v[0]}")
    P(f"Orders with fact: {v[1]}")
    P(f"Fact total logistics PLN: {v[2]:,.2f}")
    P(f"Legacy total logistics PLN: {v[3]:,.2f}")
    P(f"Avg non-zero fact: {v[4]:.2f} PLN")

    conn.close()
    P("")
    P("Done.")

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"Output written to {OUT} ({len(lines)} lines)")

if __name__ == "__main__":
    main()
