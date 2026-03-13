"""
Test: run ONLY the new step 4b2 (catch-all ads allocation) for Jan 2026 first.
Then check if the gap closes.
"""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

from datetime import date
from app.core.db_connection import connect_acc

months = [
    ("2026-01", date(2026, 1, 1), date(2026, 1, 31)),
    ("2026-02", date(2026, 2, 1), date(2026, 2, 28)),
    ("2026-03", date(2026, 3, 1), date(2026, 3, 10)),
]

for label, date_from, date_to in months:
    print(f"\n{'='*60}")
    print(f"Processing {label}: {date_from} -> {date_to}")
    print('='*60)

    conn = connect_acc(autocommit=False, timeout=120)
    cur = conn.cursor()

    # ── Before: gap for this month ──
    cur.execute("""
        SELECT SUM(ISNULL(spend_pln, 0))
        FROM dbo.acc_ads_product_day WITH (NOLOCK)
        WHERE report_date >= ? AND report_date <= ?
          AND sku IS NOT NULL AND sku != ''
    """, (date_from, date_to))
    pd_total = float(cur.fetchone()[0] or 0)

    cur.execute("""
        SELECT SUM(ISNULL(ad_spend_pln, 0))
        FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
        WHERE period_date >= ? AND period_date <= ?
    """, (date_from, date_to))
    rollup_before = float(cur.fetchone()[0] or 0)

    gap_before = pd_total - rollup_before
    print(f"  product_day total : {pd_total:>12,.2f}")
    print(f"  rollup BEFORE     : {rollup_before:>12,.2f}")
    print(f"  gap BEFORE        : {gap_before:>12,.2f} ({gap_before/max(pd_total,1)*100:.1f}%)")

    # ── Run catch-all SQL ──
    print("  Running catch-all step 4b2...")
    cur.execute("""
        ;WITH monthly_ads AS (
            SELECT
                a.marketplace_id,
                a.sku,
                DATEFROMPARTS(YEAR(a.report_date), MONTH(a.report_date), 1) AS month_start,
                SUM(ISNULL(a.spend_pln, 0)) AS total_spend
            FROM dbo.acc_ads_product_day a WITH (NOLOCK)
            WHERE a.report_date >= ? AND a.report_date <= ?
              AND a.sku IS NOT NULL AND a.sku != ''
            GROUP BY a.marketplace_id, a.sku,
                     DATEFROMPARTS(YEAR(a.report_date), MONTH(a.report_date), 1)
        ),
        allocated AS (
            SELECT
                r.marketplace_id,
                r.sku,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS month_start,
                SUM(ISNULL(r.ad_spend_pln, 0)) AS already_allocated
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.ad_spend_pln > 0
            GROUP BY r.marketplace_id, r.sku,
                     DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
        ),
        unmatched AS (
            SELECT
                ma.marketplace_id,
                ma.sku,
                ma.month_start,
                ma.total_spend - ISNULL(al.already_allocated, 0) AS unmatched_spend
            FROM monthly_ads ma
            LEFT JOIN allocated al
              ON al.marketplace_id = ma.marketplace_id
             AND al.sku = ma.sku
             AND al.month_start = ma.month_start
            WHERE ma.total_spend - ISNULL(al.already_allocated, 0) > 0.01
        ),
        sku_month_rev AS (
            SELECT
                r.marketplace_id,
                r.sku,
                DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1) AS month_start,
                r.period_date,
                r.revenue_pln,
                SUM(r.revenue_pln) OVER (
                    PARTITION BY r.marketplace_id, r.sku,
                                 DATEFROMPARTS(YEAR(r.period_date), MONTH(r.period_date), 1)
                ) AS month_revenue
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.revenue_pln > 0
        )
        UPDATE r SET
            r.ad_spend_pln = ISNULL(r.ad_spend_pln, 0)
                + ROUND(u.unmatched_spend * smr.revenue_pln / smr.month_revenue, 2),
            r.acos_pct = CASE WHEN r.revenue_pln > 0
                THEN ROUND(
                    (ISNULL(r.ad_spend_pln, 0)
                     + ROUND(u.unmatched_spend * smr.revenue_pln / smr.month_revenue, 2))
                    / r.revenue_pln * 100, 2)
                ELSE NULL END
        FROM dbo.acc_sku_profitability_rollup r
        JOIN sku_month_rev smr
          ON smr.marketplace_id = r.marketplace_id
         AND smr.sku = r.sku
         AND smr.period_date = r.period_date
        JOIN unmatched u
          ON u.marketplace_id = smr.marketplace_id
         AND u.sku = smr.sku
         AND u.month_start = smr.month_start
        WHERE r.period_date >= ? AND r.period_date <= ?
          AND smr.month_revenue > 0
    """, (date_from, date_to, date_from, date_to,
          date_from, date_to, date_from, date_to))
    catchall_rows = cur.rowcount
    print(f"  catch-all rows updated: {catchall_rows}")
    conn.commit()

    # ── After ──
    cur.execute("""
        SELECT SUM(ISNULL(ad_spend_pln, 0))
        FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
        WHERE period_date >= ? AND period_date <= ?
    """, (date_from, date_to))
    rollup_after = float(cur.fetchone()[0] or 0)
    gap_after = pd_total - rollup_after
    recovered = rollup_after - rollup_before

    print(f"  rollup AFTER      : {rollup_after:>12,.2f}")
    print(f"  gap AFTER         : {gap_after:>12,.2f} ({gap_after/max(pd_total,1)*100:.1f}%)")
    print(f"  RECOVERED         : {recovered:>+12,.2f}")

    cur.close()
    conn.close()

print("\nDONE")
