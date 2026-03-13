"""
Refresh rollup + EDM for Feb-Mar 2026 after logistics cost fill.
"""
import sys, os, time, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from datetime import date

def main():
    t0 = time.time()
    date_from = date(2026, 2, 1)
    date_to = date(2026, 3, 11)

    # Step 1: Recompute SKU + marketplace rollup
    print(f"[1/2] Recomputing rollups for {date_from} → {date_to}...")
    from app.services.profitability_service import recompute_rollups
    result = recompute_rollups(date_from=date_from, date_to=date_to)
    print(f"  SKU rows: {result.get('sku_rows_upserted', '?')}")
    print(f"  MKT rows: {result.get('marketplace_rows_upserted', '?')}")
    print(f"  Enrichment: {result.get('enrichment', '?')}")

    # Step 2: Recompute executive_daily_metrics
    print(f"\n[2/2] Recomputing executive_daily_metrics...")
    from app.services.executive_service import recompute_executive_metrics
    days_back = (date_to - date_from).days + 1
    exec_result = recompute_executive_metrics(days_back=days_back)
    print(f"  Rows: {exec_result.get('metrics_rows', '?')}")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")

    # Verify: check logistics propagation
    print("\nVerification — Feb-Mar 2026 rollup logistics:")
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

if __name__ == "__main__":
    main()
