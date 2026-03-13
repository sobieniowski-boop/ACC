"""Final validation of logistics cost fill for Feb-Mar 2026."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=30)
cur = conn.cursor()

print("=" * 70)
print("FINAL VALIDATION: Logistics Cost Fill for Feb-Mar 2026")
print("=" * 70)

# 1. Coverage check
print("\n1. LOGISTICS FACT COVERAGE (Feb-Mar 2026)")
cur.execute("""
    SELECT
        MONTH(o.purchase_date) AS m,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN f.amazon_order_id IS NOT NULL THEN 1 ELSE 0 END) AS has_fact,
        SUM(CASE WHEN f.amazon_order_id IS NULL THEN 1 ELSE 0 END) AS missing
    FROM dbo.acc_order o WITH (NOLOCK)
    LEFT JOIN (
        SELECT DISTINCT amazon_order_id
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
    ) f ON f.amazon_order_id = o.amazon_order_id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
      AND o.status NOT IN ('Cancelled', 'Canceled')
    GROUP BY MONTH(o.purchase_date)
    ORDER BY 1
""")
for m, total, has, miss in cur.fetchall():
    pct = has / total * 100 if total else 0
    mname = {2: "Feb", 3: "Mar"}.get(m, str(m))
    print(f"  {mname} 2026: {has:,}/{total:,} ({pct:.1f}%) covered, {miss:,} missing")

# 2. Fact breakdown by calc_version
print("\n2. FACTS BY CALC_VERSION (Feb-Mar orders)")
cur.execute("""
    SELECT f.calc_version, COUNT(*) AS cnt, AVG(f.total_logistics_pln) AS avg_cost
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = f.amazon_order_id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
    GROUP BY f.calc_version
    ORDER BY cnt DESC
""")
for ver, cnt, avg in cur.fetchall():
    print(f"  {ver}: {cnt:,} rows, avg={float(avg):.2f} PLN")

# 3. Rollup logistics check
print("\n3. SKU ROLLUP LOGISTICS (Feb-Mar)")
cur.execute("""
    SELECT
        MONTH(period_date) AS m,
        COUNT(*) AS sku_days,
        SUM(revenue_pln) AS rev,
        SUM(logistics_pln) AS logistics,
        SUM(cm1_pln) AS cm1,
        SUM(cm2_pln) AS cm2,
        SUM(profit_pln) AS np,
        CASE WHEN SUM(revenue_pln) > 0
             THEN SUM(cm1_pln) / SUM(revenue_pln) * 100
             ELSE 0 END AS cm1_pct
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= '2026-02-01' AND period_date <= '2026-03-11'
    GROUP BY MONTH(period_date)
    ORDER BY 1
""")
for m, days, rev, log, cm1, cm2, np, cm1p in cur.fetchall():
    mname = {2: "Feb", 3: "Mar"}.get(m, str(m))
    print(f"  {mname}: {days:,} SKU-days | rev={float(rev):>10,.0f} | logistics={float(log):>10,.0f} | cm1={float(cm1):>10,.0f} ({float(cm1p):.1f}%) | np={float(np):>10,.0f}")

# 4. EDM check
print("\n4. EXECUTIVE DAILY METRICS (Feb-Mar)")
cur.execute("""
    SELECT
        CONVERT(VARCHAR(7), period_date, 120) AS m,
        SUM(revenue_pln) AS rev,
        SUM(cm1_pln) AS cm1,
        SUM(profit_pln) AS np
    FROM dbo.executive_daily_metrics WITH (NOLOCK)
    WHERE period_date >= '2026-02-01' AND period_date <= '2026-03-11'
    GROUP BY CONVERT(VARCHAR(7), period_date, 120)
    ORDER BY 1
""")
for m, rev, cm1, np in cur.fetchall():
    print(f"  {m}: rev={float(rev):>10,.0f} | cm1={float(cm1):>10,.0f} | np={float(np):>10,.0f}")

# 5. Sample estimates by country
print("\n5. ESTIMATED COST SAMPLES BY COUNTRY (hist_country_v1)")
cur.execute("""
    SELECT TOP 10
        o.ship_country,
        COUNT(*) AS orders,
        AVG(f.total_logistics_pln) AS avg_cost,
        MIN(f.total_logistics_pln) AS min_cost,
        MAX(f.total_logistics_pln) AS max_cost
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = f.amazon_order_id
    WHERE f.calc_version = 'hist_country_v1'
    GROUP BY o.ship_country
    ORDER BY COUNT(*) DESC
""")
for country, orders, avg, mn, mx in cur.fetchall():
    print(f"  {country}: {orders:,} orders, avg={float(avg):.2f}, min={float(mn):.2f}, max={float(mx):.2f}")

conn.close()
print("\n" + "=" * 70)
print("VALIDATION COMPLETE")
print("=" * 70)
