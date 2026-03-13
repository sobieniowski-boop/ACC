"""Diagnose all 4 board issues."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=30)
cur = c.cursor()

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

# 1. Bridge columns status
print("=== BRIDGE COLUMNS STATUS ===")
r = q("""
    SELECT COUNT(*) total,
           SUM(CASE WHEN shipping_surcharge_pln IS NOT NULL AND shipping_surcharge_pln <> 0 THEN 1 ELSE 0 END) surcharge,
           SUM(CASE WHEN promo_order_fee_pln IS NOT NULL AND promo_order_fee_pln <> 0 THEN 1 ELSE 0 END) promo,
           SUM(CASE WHEN refund_commission_pln IS NOT NULL AND refund_commission_pln <> 0 THEN 1 ELSE 0 END) refcomm
    FROM acc_order WITH (NOLOCK) WHERE purchase_date >= '2026-01-01'
""")[0]
print(f"  total={r[0]:,} | surcharge={r[1]:,} | promo={r[2]:,} | refcomm={r[3]:,}")

# 2. Finance bridgeable data
print("\n=== FINANCE ROWS FOR BRIDGE ===")
for r in q("""
    SELECT transaction_type, COUNT(*) cnt,
           SUM(CASE WHEN amazon_order_id IS NOT NULL AND LEN(amazon_order_id) > 0 THEN 1 ELSE 0 END) has_order
    FROM acc_finance_transaction WITH (NOLOCK)
    WHERE transaction_type IN ('ShippingHB','ShippingChargeback','FBAOverSizeSurcharge',
                               'CouponRedemptionFee','PrimeExclusiveDiscountFee',
                               'SubscribeAndSavePerformanceFee','RefundCommission')
    GROUP BY transaction_type
"""):
    print(f"  {r[0]:>35}: {r[1]:>6,} total | {r[2]:>6,} with order_id")

# 3. Ads by type
print("\n=== ADS BY TYPE (Jan-Mar 2026) ===")
for r in q("""
    SELECT ad_type, CONVERT(VARCHAR(7), report_date, 120) m,
           COUNT(*) rows, SUM(CAST(spend_pln AS FLOAT)) spend
    FROM acc_ads_campaign_day WITH (NOLOCK)
    WHERE report_date >= '2026-01-01' AND report_date < '2026-04-01'
    GROUP BY ad_type, CONVERT(VARCHAR(7), report_date, 120)
    ORDER BY CONVERT(VARCHAR(7), report_date, 120), ad_type
"""):
    print(f"  {r[1]} {r[0]:>3}: {r[2]:>6,} rows | spend_pln={float(r[3] or 0):>12,.2f}")

# 4. Ads product_day by type (what's actually allocated)
print("\n=== ADS PRODUCT DAY BY TYPE ===")
for r in q("""
    SELECT ad_type, CONVERT(VARCHAR(7), report_date, 120) m,
           COUNT(*) rows, SUM(CAST(spend_pln AS FLOAT)) spend
    FROM acc_ads_product_day WITH (NOLOCK)
    WHERE report_date >= '2026-01-01' AND report_date < '2026-04-01'
    GROUP BY ad_type, CONVERT(VARCHAR(7), report_date, 120)
    ORDER BY CONVERT(VARCHAR(7), report_date, 120), ad_type
"""):
    print(f"  {r[1]} {r[0]:>3}: {r[2]:>6,} rows | spend_pln={float(r[3] or 0):>12,.2f}")

# 5. step_bridge_fees - does it exist and what does it do?
print("\n=== BRIDGE FEES FUNCTION CHECK ===")
try:
    from app.services.order_pipeline import step_bridge_fees
    print("  step_bridge_fees: FOUND")
    import inspect
    sig = inspect.signature(step_bridge_fees)
    print(f"  signature: {sig}")
except ImportError as e:
    print(f"  step_bridge_fees: NOT FOUND - {e}")

# 6. Check charge_type column name (might differ)
print("\n=== ACC_FINANCE_TRANSACTION columns ===")
for r in q("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME='acc_finance_transaction' 
    AND COLUMN_NAME LIKE '%type%'
"""):
    print(f"  {r[0]}")

# 7. All unique transaction_type values 
print("\n=== ALL TRANSACTION TYPES (sample) ===")
for r in q("""
    SELECT TOP 30 transaction_type, COUNT(*) c
    FROM acc_finance_transaction WITH (NOLOCK)
    GROUP BY transaction_type ORDER BY COUNT(*) DESC
"""):
    print(f"  {r[0]:>40}: {r[1]:>8,}")

c.close()
print("\nDONE")
