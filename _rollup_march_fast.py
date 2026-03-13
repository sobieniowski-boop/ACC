"""Quick March-only rollup + EDM refresh."""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from datetime import date

t0 = time.time()
print("[1/2] Rollup Mar 1-11...", flush=True)
from app.services.profitability_service import recompute_rollups
r = recompute_rollups(date_from=date(2026, 3, 1), date_to=date(2026, 3, 11))
print(f"  SKU={r.get('sku_rows_upserted','?')}, MKT={r.get('marketplace_rows_upserted','?')}, elapsed={time.time()-t0:.0f}s")

print("[2/2] EDM...", flush=True)
from app.services.executive_service import recompute_executive_metrics
er = recompute_executive_metrics(days_back=11)
print(f"  EDM rows={er.get('metrics_rows','?')}")

# Verify
print("\nVerification:")
from app.core.db_connection import connect_acc
conn = connect_acc(timeout=30)
cur = conn.cursor()
cur.execute("""
    SELECT COUNT(*) rows, SUM(revenue_pln) rev, SUM(logistics_pln) log, SUM(cm1_pln) cm1, SUM(profit_pln) np
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= '2026-03-01' AND period_date <= '2026-03-11'
""")
rows, rev, log, cm1, np = cur.fetchone()
print(f"  Mar 1-11: {rows:,} SKU-days | rev={float(rev):,.0f} | logistics={float(log):,.0f} | cm1={float(cm1):,.0f} | np={float(np):,.0f}")
conn.close()
print(f"\nDone in {time.time()-t0:.0f}s")
