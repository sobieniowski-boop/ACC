"""Deep dive into G01 (revenue stamps) and G11 (CM divergence) to understand root causes."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from datetime import date
from app.core.db_connection import connect_acc

DATE_FROM = date(2026, 1, 1)
DATE_TO   = date(2026, 3, 9)

conn = connect_acc(autocommit=False, timeout=60)
cur = conn.cursor()

# --- G01 breakdown: revenue_pln NULL or 0 vs computed ---
print("=== G01 DEEP DIVE: Revenue stamps ===")
cur.execute("""
    SELECT
        SUM(CASE WHEN o.revenue_pln IS NULL THEN 1 ELSE 0 END) AS null_rev,
        SUM(CASE WHEN o.revenue_pln = 0 THEN 1 ELSE 0 END) AS zero_rev,
        SUM(CASE WHEN o.revenue_pln > 0 THEN 1 ELSE 0 END) AS pos_rev,
        COUNT(*) AS total
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.purchase_date >= ? AND o.purchase_date < DATEADD(day,1,CAST(? AS DATE))
      AND o.status IN ('Shipped','Unshipped')
""", (DATE_FROM, DATE_TO))
r = cur.fetchone()
print(f"  NULL revenue: {r[0]}")
print(f"  Zero revenue: {r[1]}")
print(f"  Positive revenue: {r[2]}")
print(f"  Total: {r[3]}")

# --- G11 breakdown: understand the CM divergence pattern ---
print("\n=== G11 DEEP DIVE: Stored CM vs Inline CM ===")
# CM inline = revenue_pln - amazon_fees_pln - cogs_pln - logistics_pln (CM1, no ads)
# V2 engine writes contribution_margin_pln = same formula
cur.execute("""
    SELECT
        SUM(CASE WHEN o.contribution_margin_pln IS NULL THEN 1 ELSE 0 END) AS null_cm,
        SUM(CASE WHEN o.contribution_margin_pln IS NOT NULL AND o.revenue_pln IS NULL THEN 1 ELSE 0 END) AS cm_but_no_rev,
        SUM(CASE WHEN o.contribution_margin_pln IS NOT NULL AND o.revenue_pln IS NOT NULL
            AND ABS(o.contribution_margin_pln - (o.revenue_pln - ISNULL(o.amazon_fees_pln,0) - ISNULL(o.cogs_pln,0) - ISNULL(o.logistics_pln,0))) <= 5.0
            THEN 1 ELSE 0 END) AS consistent,
        SUM(CASE WHEN o.contribution_margin_pln IS NOT NULL AND o.revenue_pln IS NOT NULL
            AND ABS(o.contribution_margin_pln - (o.revenue_pln - ISNULL(o.amazon_fees_pln,0) - ISNULL(o.cogs_pln,0) - ISNULL(o.logistics_pln,0))) > 5.0
            THEN 1 ELSE 0 END) AS divergent,
        COUNT(*) AS total
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.purchase_date >= ? AND o.purchase_date < DATEADD(day,1,CAST(? AS DATE))
      AND o.status IN ('Shipped','Unshipped')
""", (DATE_FROM, DATE_TO))
r = cur.fetchone()
print(f"  NULL CM: {r[0]}")
print(f"  CM exists but no revenue: {r[1]}")
print(f"  CM consistent with inline (tol 5 PLN): {r[2]}")
print(f"  CM divergent from inline (> 5 PLN): {r[3]}")
print(f"  Total: {r[4]}")

# --- What does V2 recalc actually set? ---
# V2 = recalc_profit_orders computes:
#   revenue_pln = SUM((item_price - item_tax - promo) * fx)  per order
#   contribution_margin_pln = revenue_pln - fees - cogs - logistics
# Both should agree. If CM diverges, it means CM was stamped by V1 GROSS formula
# and revenue was never updated, or vice versa.
print("\n=== Divergent orders: is CM from V1 GROSS or just stale? ===")
cur.execute("""
    SELECT TOP 10
        o.amazon_order_id,
        o.marketplace_id,
        o.purchase_date,
        o.order_total,
        o.currency,
        o.revenue_pln,
        o.cogs_pln,
        o.amazon_fees_pln,
        o.logistics_pln,
        o.contribution_margin_pln,
        (ISNULL(o.revenue_pln,0) - ISNULL(o.amazon_fees_pln,0) - ISNULL(o.cogs_pln,0) - ISNULL(o.logistics_pln,0)) AS inline_cm
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.purchase_date >= ? AND o.purchase_date < DATEADD(day,1,CAST(? AS DATE))
      AND o.status IN ('Shipped','Unshipped')
      AND o.contribution_margin_pln IS NOT NULL
      AND o.revenue_pln IS NOT NULL
      AND ABS(o.contribution_margin_pln - (o.revenue_pln - ISNULL(o.amazon_fees_pln,0) - ISNULL(o.cogs_pln,0) - ISNULL(o.logistics_pln,0))) > 5.0
    ORDER BY ABS(o.contribution_margin_pln - (o.revenue_pln - ISNULL(o.amazon_fees_pln,0) - ISNULL(o.cogs_pln,0) - ISNULL(o.logistics_pln,0))) DESC
""", (DATE_FROM, DATE_TO))
print("  order_id | mkt | rev | cogs | fees | logistics | stored_cm | inline_cm | delta")
for r in cur.fetchall():
    stored_cm = r[9]
    inline_cm = r[10]
    delta = round(stored_cm - inline_cm, 2)
    print(f"  {r[0]} | {r[1]} | rev={r[5]} | cogs={r[6]} | fees={r[7]} | log={r[8]} | cm={stored_cm} | inline={round(inline_cm,2)} | d={delta}")

# --- Check if the divergence is explained by V1 GROSS vs V2 NETTO ---
print("\n=== V1 GROSS revenue pattern check ===")
cur.execute("""
    SELECT TOP 5
        o.amazon_order_id,
        o.order_total,
        o.currency,
        ISNULL(fx.rate_to_pln,
            CASE o.currency
                WHEN 'PLN' THEN 1.0 WHEN 'EUR' THEN 4.30 WHEN 'GBP' THEN 5.10
                WHEN 'SEK' THEN 0.40 WHEN 'CZK' THEN 0.18 ELSE 4.30 END
        ) AS fx_rate,
        o.order_total * ISNULL(fx.rate_to_pln,
            CASE o.currency
                WHEN 'PLN' THEN 1.0 WHEN 'EUR' THEN 4.30 WHEN 'GBP' THEN 5.10
                WHEN 'SEK' THEN 0.40 WHEN 'CZK' THEN 0.18 ELSE 4.30 END
        ) AS gross_rev_pln,
        o.revenue_pln AS netto_rev_pln,
        o.contribution_margin_pln
    FROM dbo.acc_order o WITH (NOLOCK)
    OUTER APPLY (
        SELECT TOP 1 er.rate_to_pln
        FROM dbo.acc_exchange_rate er WITH (NOLOCK)
        WHERE er.currency = o.currency AND er.rate_date <= o.purchase_date
        ORDER BY er.rate_date DESC
    ) fx
    WHERE o.purchase_date >= ? AND o.purchase_date < DATEADD(day,1,CAST(? AS DATE))
      AND o.status IN ('Shipped','Unshipped')
      AND o.contribution_margin_pln IS NOT NULL
      AND o.revenue_pln IS NOT NULL
      AND ABS(o.contribution_margin_pln - (o.revenue_pln - ISNULL(o.amazon_fees_pln,0) - ISNULL(o.cogs_pln,0) - ISNULL(o.logistics_pln,0))) > 5.0
    ORDER BY NEWID()
""", (DATE_FROM, DATE_TO))
for r in cur.fetchall():
    gross = round(float(r[4] or 0), 2)
    netto = round(float(r[5] or 0), 2)
    cm = round(float(r[6] or 0), 2)
    print(f"  {r[0]} | {r[2]} | order_total={r[1]} | fx={round(float(r[3]),4)} | gross={gross} | netto={netto} | cm={cm}")

# --- Check: how many orders have revenue_pln = 0 but contribution_margin_pln <> 0 ---
print("\n=== Revenue=0 but CM<>0 pattern ===")
cur.execute("""
    SELECT COUNT(*)
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.purchase_date >= ? AND o.purchase_date < DATEADD(day,1,CAST(? AS DATE))
      AND o.status IN ('Shipped','Unshipped')
      AND (o.revenue_pln IS NULL OR o.revenue_pln = 0)
      AND o.contribution_margin_pln IS NOT NULL
      AND o.contribution_margin_pln <> 0
""", (DATE_FROM, DATE_TO))
print(f"  Orders with revenue=0/NULL but CM<>0: {cur.fetchone()[0]}")

# --- Check: how many orders have revenue_pln = NULL entirely ---
cur.execute("""
    SELECT COUNT(*)
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.purchase_date >= ? AND o.purchase_date < DATEADD(day,1,CAST(? AS DATE))
      AND o.status IN ('Shipped','Unshipped')
      AND o.revenue_pln IS NULL
""", (DATE_FROM, DATE_TO))
print(f"  Orders with revenue_pln IS NULL: {cur.fetchone()[0]}")

# --- Check: V2 recalc last run date ---
print("\n=== Last V2 recalc timestamp ===")
cur.execute("""
    SELECT MAX(o.updated_at)
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.contribution_margin_pln IS NOT NULL
""")
last_recalc = cur.fetchone()[0]
print(f"  Last order updated_at with CM stamp: {last_recalc}")

conn.close()
print("\nDone.")
