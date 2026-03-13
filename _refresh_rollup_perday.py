"""
Refresh rollup per-day to avoid connection timeout on large date ranges.
After FBA logistics removal.
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from datetime import date, timedelta

def main():
    t0 = time.time()
    start = date(2026, 2, 1)
    end = date(2026, 3, 11)

    from app.services.profitability_service import recompute_rollups

    d = start
    total_sku = 0
    total_mkt = 0
    while d <= end:
        print(f"  Rollup {d} ...", end=" ", flush=True)
        try:
            result = recompute_rollups(date_from=d, date_to=d)
            sku = result.get('sku_rows_upserted', 0)
            mkt = result.get('marketplace_rows_upserted', 0)
            total_sku += sku
            total_mkt += mkt
            print(f"SKU={sku}, MKT={mkt}")
        except Exception as e:
            print(f"ERROR: {e}")
        d += timedelta(days=1)

    print(f"\nTotal: SKU={total_sku}, MKT={total_mkt}")

    # EDM
    print("\nRecomputing executive_daily_metrics...")
    from app.services.executive_service import recompute_executive_metrics
    days_back = (end - start).days + 1
    exec_result = recompute_executive_metrics(days_back=days_back)
    print(f"  EDM rows: {exec_result.get('metrics_rows', '?')}")

    # Verify
    print("\nVerification:")
    from app.core.db_connection import connect_acc
    conn = connect_acc(timeout=30)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            MONTH(period_date) AS m,
            COUNT(*) AS rows,
            SUM(revenue_pln) AS rev,
            SUM(logistics_pln) AS logistics,
            SUM(cm1_pln) AS cm1,
            SUM(profit_pln) AS np
        FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
        WHERE period_date >= '2026-02-01' AND period_date <= '2026-03-11'
        GROUP BY MONTH(period_date)
        ORDER BY 1
    """)
    for m, rows, rev, log_cost, cm1, np in cur.fetchall():
        print(f"  Month {m}: {rows:,} SKU-days | rev={float(rev):,.0f} | logistics={float(log_cost):,.0f} | cm1={float(cm1):,.0f} | np={float(np):,.0f}")
    conn.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")

if __name__ == "__main__":
    main()
