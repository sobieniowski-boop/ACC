"""Feb-Mar rollup in 1-day batches after v3 model apply."""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from datetime import date, timedelta

t0 = time.time()
start = date(2026, 2, 1)
end = date(2026, 3, 11)

from app.services.profitability_service import recompute_rollups

d = start
total_sku = 0
while d <= end:
    print(f"  {d} ...", end=" ", flush=True)
    bt = time.time()
    r = recompute_rollups(date_from=d, date_to=d)
    sku = r.get('sku_rows_upserted', 0)
    total_sku += sku
    print(f"SKU={sku} ({time.time()-bt:.0f}s)")
    d += timedelta(days=1)

print(f"\nRollup done: {total_sku} SKU rows in {time.time()-t0:.0f}s")

print("EDM...", flush=True)
from app.services.executive_service import recompute_executive_metrics
er = recompute_executive_metrics(days_back=40)
print(f"  EDM rows={er.get('metrics_rows','?')}")

# Verify
from app.core.db_connection import connect_acc
conn = connect_acc(timeout=30)
cur = conn.cursor()
cur.execute("""
    SELECT 
        CASE WHEN period_date < '2026-03-01' THEN 'FEB' ELSE 'MAR' END AS mth,
        COUNT(*) rows, SUM(revenue_pln) rev, SUM(logistics_pln) log,
        SUM(cm1_pln) cm1, SUM(profit_pln) np
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= '2026-02-01' AND period_date <= '2026-03-11'
    GROUP BY CASE WHEN period_date < '2026-03-01' THEN 'FEB' ELSE 'MAR' END
""")
for row in cur.fetchall():
    mth, rows, rev, log, cm1, np = row
    print(f"{mth}: {rows:,} SKU-days | rev={float(rev):,.0f} | log={float(log):,.0f} | cm1={float(cm1):,.0f} | np={float(np):,.0f}")
conn.close()
print(f"Total: {time.time()-t0:.0f}s")
