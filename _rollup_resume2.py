"""Resume rollup for Mar 10-11 with retry logic."""
import sys, os, time
sys.path.insert(0, os.path.join("c:\\ACC", "apps", "api"))
from datetime import date
from app.services.profitability_service import recompute_rollups

remaining = [date(2026, 3, 10), date(2026, 3, 11)]
MAX_RETRIES = 3

for d in remaining:
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"  {d} (attempt {attempt})...", end=" ", flush=True)
        bt = time.time()
        try:
            r = recompute_rollups(date_from=d, date_to=d)
            sku = r.get('sku_rows_upserted', 0)
            print(f"SKU={sku} ({time.time()-bt:.0f}s)")
            break
        except Exception as e:
            print(f"FAIL ({time.time()-bt:.0f}s): {e}")
            if attempt < MAX_RETRIES:
                print(f"    Waiting 30s before retry...", flush=True)
                time.sleep(30)
            else:
                print(f"    GIVING UP on {d}")

print("\nEDM...", flush=True)
from app.services.executive_service import recompute_executive_metrics
er = recompute_executive_metrics(days_back=40)
print(f"  EDM rows={er.get('metrics_rows','?')}")

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
print("Done!")
