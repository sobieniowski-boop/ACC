import sys, decimal
sys.path.insert(0, r"C:\ACC\apps\api")
from app.connectors.mssql import connect_acc

conn = connect_acc(autocommit=False, timeout=300)
cur = conn.cursor()
lines = []
P = lines.append

P("acc_order_logistics_fact cols:")
cur.execute("SELECT TOP 1 * FROM dbo.acc_order_logistics_fact WITH (NOLOCK)")
P(str([d[0] for d in cur.description]))
P("")

P("acc_shipment_cost cols:")
cur.execute("SELECT TOP 1 * FROM dbo.acc_shipment_cost WITH (NOLOCK)")
P(str([d[0] for d in cur.description]))
P("")

P("=== FACT TABLE: monthly courier per order — 2026 ===")
cur.execute("""
    SELECT FORMAT(o.purchase_date,'yyyy-MM') m, o.marketplace_id,
        COUNT(DISTINCT o.amazon_order_id) orders,
        COUNT(DISTINCT f.amazon_order_id) w_fact,
        ROUND(SUM(ISNULL(f.total_logistics_pln,0)),2) total_pln,
        ROUND(AVG(CASE WHEN f.total_logistics_pln>0 THEN f.total_logistics_pln END),2) avg_nz,
        SUM(CASE WHEN f.total_logistics_pln>0 THEN 1 ELSE 0 END) nz
    FROM dbo.acc_order o WITH(NOLOCK)
    LEFT JOIN (
        SELECT amazon_order_id, total_logistics_pln,
               ROW_NUMBER() OVER(PARTITION BY amazon_order_id ORDER BY calculated_at DESC) rn
        FROM dbo.acc_order_logistics_fact WITH(NOLOCK)
    ) f ON f.amazon_order_id=o.amazon_order_id AND f.rn=1
    WHERE o.purchase_date>='2026-01-01' AND o.status IN('Shipped','Unshipped')
    GROUP BY FORMAT(o.purchase_date,'yyyy-MM'), o.marketplace_id ORDER BY 1,2
""")
hdr = f"{'Month':<10}{'MKT':<20}{'Orders':>8}{'W/Fact':>8}{'TotalPLN':>12}{'AvgNZ':>10}{'NonZero':>8}"
P(hdr); P("-"*len(hdr))
for r in cur.fetchall():
    v=[float(x) if isinstance(x,decimal.Decimal) else x for x in r]
    a=v[5] if v[5] else 0.0
    P(f"{v[0]:<10}{v[1]:<20}{v[2]:>8}{v[3]:>8}{v[4]:>12.2f}{a:>10.2f}{v[6]:>8}")
P("")

P("=== LEGACY vs FACT — 2026 ===")
cur.execute("""
    SELECT FORMAT(o.purchase_date,'yyyy-MM') m, COUNT(*) orders,
        ROUND(SUM(ISNULL(o.logistics_pln,0)),2) leg,
        ROUND(SUM(ISNULL(f.total_logistics_pln,0)),2) fct,
        ROUND(SUM(ISNULL(o.logistics_pln,0))-SUM(ISNULL(f.total_logistics_pln,0)),2) delta,
        SUM(CASE WHEN ISNULL(o.logistics_pln,0)>0 AND ISNULL(f.total_logistics_pln,0)=0 THEN 1 ELSE 0 END) lo,
        SUM(CASE WHEN ISNULL(o.logistics_pln,0)=0 AND ISNULL(f.total_logistics_pln,0)>0 THEN 1 ELSE 0 END) fo,
        SUM(CASE WHEN ISNULL(o.logistics_pln,0)>0 AND ISNULL(f.total_logistics_pln,0)>0
                  AND ABS(o.logistics_pln-f.total_logistics_pln)>0.01 THEN 1 ELSE 0 END) diff
    FROM dbo.acc_order o WITH(NOLOCK)
    LEFT JOIN (
        SELECT amazon_order_id, total_logistics_pln,
               ROW_NUMBER() OVER(PARTITION BY amazon_order_id ORDER BY calculated_at DESC) rn
        FROM dbo.acc_order_logistics_fact WITH(NOLOCK)
    ) f ON f.amazon_order_id=o.amazon_order_id AND f.rn=1
    WHERE o.purchase_date>='2026-01-01' AND o.status IN('Shipped','Unshipped')
    GROUP BY FORMAT(o.purchase_date,'yyyy-MM') ORDER BY 1
""")
P(f"{'Month':<10}{'Orders':>8}{'LegacyPLN':>12}{'FactPLN':>12}{'Delta':>10}{'LegOnly':>8}{'FctOnly':>8}{'Differ':>8}")
P("-"*80)
for r in cur.fetchall():
    v=[float(x) if isinstance(x,decimal.Decimal) else x for x in r]
    P(f"{v[0]:<10}{v[1]:>8}{v[2]:>12.2f}{v[3]:>12.2f}{v[4]:>10.2f}{v[5]:>8}{v[6]:>8}{v[7]:>8}")
P("")

P("=== CARRIER BREAKDOWN — 2026 ===")
cur.execute("""
    SELECT FORMAT(s.ship_date,'yyyy-MM') m, ISNULL(s.carrier,'N/A') c,
        COUNT(DISTINCT s.id) shp,
        ROUND(SUM(ISNULL(c.net_amount,0)+ISNULL(c.fuel_amount,0)+ISNULL(c.toll_amount,0)),2) tot,
        ROUND(AVG(CASE WHEN(ISNULL(c.net_amount,0)+ISNULL(c.fuel_amount,0)+ISNULL(c.toll_amount,0))>0
            THEN ISNULL(c.net_amount,0)+ISNULL(c.fuel_amount,0)+ISNULL(c.toll_amount,0) END),2) av,
        SUM(CASE WHEN c.is_estimated=1 THEN 1 ELSE 0 END) est,
        SUM(CASE WHEN ISNULL(c.is_estimated,0)=0 THEN 1 ELSE 0 END) act
    FROM dbo.acc_shipment s WITH(NOLOCK)
    LEFT JOIN dbo.acc_shipment_cost c WITH(NOLOCK) ON c.shipment_id=s.id
    WHERE s.ship_date>='2026-01-01'
    GROUP BY FORMAT(s.ship_date,'yyyy-MM'), ISNULL(s.carrier,'N/A') ORDER BY 1,2
""")
P(f"{'Month':<10}{'Carrier':<10}{'Shipments':>10}{'TotalCost':>12}{'AvgCost':>10}{'Est':>6}{'Act':>6}")
P("-"*70)
for r in cur.fetchall():
    v=[float(x) if isinstance(x,decimal.Decimal) else x for x in r]
    a=v[4] if v[4] else 0.0
    P(f"{v[0]:<10}{str(v[1]):<10}{v[2]:>10}{v[3]:>12.2f}{a:>10.2f}{v[5]:>6}{v[6]:>6}")
P("")

P("=== ROLLUP TABLE logistics — 2026 ===")
cur.execute("""
    SELECT FORMAT(r.period_date,'yyyy-MM') m, r.marketplace_id,
        COUNT(*) sd, ROUND(SUM(ISNULL(r.logistics_pln,0)),2) tot,
        SUM(CASE WHEN ISNULL(r.logistics_pln,0)>0 THEN 1 ELSE 0 END) nz,
        SUM(CASE WHEN ISNULL(r.logistics_pln,0)=0 THEN 1 ELSE 0 END) z
    FROM dbo.acc_sku_profitability_rollup r WITH(NOLOCK)
    WHERE r.period_date>='2026-01-01'
    GROUP BY FORMAT(r.period_date,'yyyy-MM'), r.marketplace_id ORDER BY 1,2
""")
P(f"{'Month':<10}{'MKT':<20}{'SKUDays':>10}{'TotalPLN':>12}{'NonZero':>8}{'Zero':>8}")
P("-"*72)
for r in cur.fetchall():
    v=[float(x) if isinstance(x,decimal.Decimal) else x for x in r]
    P(f"{v[0]:<10}{v[1]:<20}{v[2]:>10}{v[3]:>12.2f}{v[4]:>8}{v[5]:>8}")
P("")

P("=== SAMPLE MFN ORDERS — recent 20 ===")
cur.execute("""
    SELECT TOP 20 o.amazon_order_id, o.marketplace_id, CAST(o.purchase_date AS DATE) d,
        o.fulfillment_channel, ROUND(ISNULL(o.logistics_pln,0),2) leg,
        ROUND(ISNULL(f.total_logistics_pln,0),2) fct, f.calc_version
    FROM dbo.acc_order o WITH(NOLOCK)
    OUTER APPLY(
        SELECT TOP 1 olf.total_logistics_pln, olf.calc_version
        FROM dbo.acc_order_logistics_fact olf WITH(NOLOCK)
        WHERE olf.amazon_order_id=o.amazon_order_id ORDER BY olf.calculated_at DESC
    ) f
    WHERE o.purchase_date>='2026-01-01' AND o.status='Shipped' AND o.fulfillment_channel='MFN'
    ORDER BY o.purchase_date DESC
""")
P(f"{'OrderID':<22}{'MKT':<18}{'Date':<12}{'FC':<5}{'Legacy':>8}{'Fact':>8}{'Version':<16}")
P("-"*90)
for r in cur.fetchall():
    v=[float(x) if isinstance(x,decimal.Decimal) else x for x in r]
    P(f"{v[0]:<22}{v[1]:<18}{str(v[2]):<12}{v[3]:<5}{v[4]:>8.2f}{v[5]:>8.2f}{str(v[6] or ''):<16}")
P("")

P("=== AFN (FBA) — logistics check 2026 ===")
cur.execute("""
    SELECT FORMAT(o.purchase_date,'yyyy-MM') m, COUNT(*) ord,
        SUM(CASE WHEN ISNULL(o.logistics_pln,0)>0 THEN 1 ELSE 0 END) leg_nz,
        SUM(CASE WHEN f.total_logistics_pln>0 THEN 1 ELSE 0 END) fct_nz,
        ROUND(SUM(ISNULL(o.logistics_pln,0)),2) leg_t,
        ROUND(SUM(ISNULL(f.total_logistics_pln,0)),2) fct_t
    FROM dbo.acc_order o WITH(NOLOCK)
    LEFT JOIN(
        SELECT amazon_order_id, total_logistics_pln,
               ROW_NUMBER() OVER(PARTITION BY amazon_order_id ORDER BY calculated_at DESC) rn
        FROM dbo.acc_order_logistics_fact WITH(NOLOCK)
    ) f ON f.amazon_order_id=o.amazon_order_id AND f.rn=1
    WHERE o.purchase_date>='2026-01-01' AND o.status IN('Shipped','Unshipped') AND o.fulfillment_channel='AFN'
    GROUP BY FORMAT(o.purchase_date,'yyyy-MM') ORDER BY 1
""")
P(f"{'Month':<10}{'AFN':>8}{'LegNZ':>8}{'FctNZ':>8}{'LegTotal':>12}{'FctTotal':>12}")
P("-"*62)
for r in cur.fetchall():
    v=[float(x) if isinstance(x,decimal.Decimal) else x for x in r]
    P(f"{v[0]:<10}{v[1]:>8}{v[2]:>8}{v[3]:>8}{v[4]:>12.2f}{v[5]:>12.2f}")
P("")

P("=== GRAND TOTALS 2026 ===")
cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id) tot, COUNT(DISTINCT f.amazon_order_id) wf,
        ROUND(SUM(ISNULL(f.total_logistics_pln,0)),2) ft, ROUND(SUM(ISNULL(o.logistics_pln,0)),2) lt,
        ROUND(AVG(CASE WHEN f.total_logistics_pln>0 THEN f.total_logistics_pln END),2) av
    FROM dbo.acc_order o WITH(NOLOCK)
    LEFT JOIN(
        SELECT amazon_order_id, total_logistics_pln,
               ROW_NUMBER() OVER(PARTITION BY amazon_order_id ORDER BY calculated_at DESC) rn
        FROM dbo.acc_order_logistics_fact WITH(NOLOCK)
    ) f ON f.amazon_order_id=o.amazon_order_id AND f.rn=1
    WHERE o.purchase_date>='2026-01-01' AND o.status IN('Shipped','Unshipped')
""")
r=cur.fetchone()
v=[float(x) if isinstance(x,decimal.Decimal) else x for x in r]
P(f"Total orders: {v[0]}")
P(f"Orders with fact: {v[1]}")
P(f"Fact total PLN: {v[2]:,.2f}")
P(f"Legacy total PLN: {v[3]:,.2f}")
av=v[4] if v[4] else 0
P(f"Avg non-zero fact: {av:.2f} PLN")

conn.close()

with open(r"C:\ACC\docs\courier_data_2026.txt","w",encoding="utf-8") as fh:
    fh.write("\n".join(lines))
print("OK:", len(lines), "lines written")
