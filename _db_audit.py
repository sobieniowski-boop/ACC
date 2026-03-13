"""DB audit script for Reality Checker / Data Analytics Reporter."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

from app.core.db_connection import connect_acc
c = connect_acc(autocommit=False, timeout=15)
cur = c.cursor()

out = []
def p(s): out.append(s); print(s)

# 1. CM1 order columns
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'acc_order' AND COLUMN_NAME IN ('shipping_surcharge_pln','promo_order_fee_pln','refund_commission_pln','amazon_fees_pln') ORDER BY COLUMN_NAME")
p("=== acc_order financial columns ===")
for r in cur.fetchall():
    p(f"  {r[0]}")

# 2. Counts
cur.execute("SELECT COUNT(*) FROM acc_order WITH (NOLOCK)")
p(f"Orders: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM acc_order_line WITH (NOLOCK)")
p(f"Order lines: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*), MIN(posted_date), MAX(posted_date) FROM acc_finance_transaction WITH (NOLOCK)")
r = cur.fetchone()
p(f"Finance txns: {r[0]:,} | {r[1]} -> {r[2]}")

# 3. CM1 black hole charges
cur.execute("SELECT charge_type, COUNT(*) cnt, SUM(amount_pln) total_pln FROM acc_finance_transaction WITH (NOLOCK) WHERE charge_type IN ('ShippingHB','ShippingChargeback','FBAOverSizeSurcharge','CouponRedemptionFee','PrimeExclusiveDiscountFee','SubscribeAndSavePerformanceFee','RefundCommission') GROUP BY charge_type ORDER BY cnt DESC")
p("=== CM1 Black Hole charge types ===")
rows = cur.fetchall()
if not rows: p("  (none found)")
for r in rows:
    p(f"  {r[0]}: {r[1]:,} txns, {float(r[2]):.2f} PLN")

# 4. Revenue vs CM1 gap
cur.execute("""
SELECT TOP 5 period_date, marketplace_id, revenue_pln, cm1_pln, cm2_pln, profit_pln
FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK) 
WHERE revenue_pln IS NOT NULL AND revenue_pln > 0
ORDER BY period_date DESC
""")
p("=== Latest SKU profitability rollup ===")
for r in cur.fetchall():
    cm1pct = float(r[3])/float(r[2])*100 if float(r[2]) else 0
    p(f"  {r[0]} | mkt={str(r[1])[:10]} | rev={float(r[2]):.0f} | cm1={float(r[3]):.0f} ({cm1pct:.1f}%)")

# 5. Executive daily metrics
cur.execute("""
SELECT TOP 5 period_date, marketplace_id, revenue_pln, cm1_pln, cm2_pln, profit_pln
FROM dbo.executive_daily_metrics WITH (NOLOCK) 
WHERE cm1_pln IS NOT NULL AND cm1_pln <> 0
ORDER BY period_date DESC
""")
p("=== Executive daily metrics (latest) ===")
for r in cur.fetchall():
    p(f"  {r[0]} | mkt={str(r[1])[:10]} | rev={float(r[2]):.0f} cm1={float(r[3]):.0f} cm2={float(r[4]):.0f} np={float(r[5]):.0f}")

# 6. Silent exception count in codebase
p("=== DONE ===")
c.close()

with open(r"C:\ACC\_db_audit_result.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
