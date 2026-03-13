"""Board Meeting P&L - Final Consolidated Report."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=60)
cur = c.cursor()

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

# 1. Order-level aggregates
print("=== ORDER-LEVEL REVENUE & COGS (Jan-Mar 2026, Shipped) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), purchase_date, 120) m,
           COUNT(*) orders,
           SUM(CAST(ISNULL(revenue_pln, 0) AS FLOAT)) rev,
           SUM(CAST(ISNULL(cogs_pln, 0) AS FLOAT)) cogs
    FROM acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-01-01' AND purchase_date < '2026-04-01'
      AND status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), purchase_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: orders={r[1]:>6,} | revenue_pln={float(r[2] or 0):>12,.0f} | cogs_pln={float(r[3] or 0):>12,.0f}")

# 4. Ads reconciliation - exec metrics vs raw ads table
print("\n=== ADS RECONCILIATION ===")
edm_ads = {}
for r in q("""
    SELECT CONVERT(VARCHAR(7), period_date, 120) m,
           SUM(CAST(ad_spend_pln AS FLOAT)) ads
    FROM executive_daily_metrics WITH (NOLOCK)
    WHERE period_date >= '2026-01-01' AND period_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), period_date, 120) ORDER BY m
"""):
    edm_ads[r[0]] = float(r[1] or 0)
    print(f"  EDM {r[0]}: {float(r[1] or 0):>12,.2f}")

raw_ads = {}
for r in q("""
    SELECT CONVERT(VARCHAR(7), report_date, 120) m,
           SUM(CAST(spend_pln AS FLOAT)) spend
    FROM acc_ads_campaign_day WITH (NOLOCK)
    WHERE report_date >= '2026-01-01' AND report_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), report_date, 120) ORDER BY m
"""):
    raw_ads[r[0]] = float(r[1] or 0)
    print(f"  RAW {r[0]}: {float(r[1] or 0):>12,.2f}")

for m in sorted(set(list(edm_ads.keys()) + list(raw_ads.keys()))):
    diff = raw_ads.get(m, 0) - edm_ads.get(m, 0)
    print(f"  DIFF {m}: {diff:>12,.2f} (raw - edm)")

# 5. Logistics reconciliation
print("\n=== LOGISTICS DETAIL ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
           COUNT(DISTINCT olf.amazon_order_id) matched_orders,
           COUNT(DISTINCT o.amazon_order_id) total_shipped,
           SUM(CAST(olf.total_logistics_pln AS FLOAT)) logi_pln
    FROM acc_order o WITH (NOLOCK)
    LEFT JOIN acc_order_logistics_fact olf WITH (NOLOCK) ON o.amazon_order_id = olf.amazon_order_id
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
      AND o.status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
"""):
    coverage = float(r[1] or 0) / max(float(r[2] or 1), 1) * 100
    print(f"  {r[0]}: matched={r[1]:>6,}/{r[2]:>6,} ({coverage:.0f}%) | logistics_pln={float(r[3] or 0):>12,.2f}")

# 6. CM2 components deep dive
print("\n=== CM2 COMPONENTS from EXEC METRICS ===")
# Check what other metrics exist
print("  Checking if storage/return columns exist in exec_daily_metrics...")
edm_cols = [r[0] for r in q("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='executive_daily_metrics' ORDER BY ORDINAL_POSITION")]
print(f"  All columns: {edm_cols}")

# 7. Summary table for board
print("\n" + "="*80)
print("  BOARD MEETING P&L SUMMARY (PLN)")
print("="*80)

months = [
    ('2026-01', 'Styczeń 2026', 31),
    ('2026-02', 'Luty 2026', 28),
    ('2026-03', 'Marzec 1-10', 10),
]

for m_key, m_name, days in months:
    edm = q(f"""
        SELECT SUM(CAST(revenue_pln AS FLOAT)),
               SUM(CAST(cogs_pln AS FLOAT)),
               SUM(CAST(cm1_pln AS FLOAT)),
               SUM(CAST(ad_spend_pln AS FLOAT)),
               SUM(CAST(refund_pln AS FLOAT)),
               SUM(CAST(cm2_pln AS FLOAT)),
               SUM(CAST(profit_pln AS FLOAT)),
               COUNT(DISTINCT period_date)
        FROM executive_daily_metrics WITH (NOLOCK)
        WHERE period_date >= '{m_key}-01'
          AND period_date < DATEADD(DAY, {days}, '{m_key}-01')
    """)[0]
    
    rev = float(edm[0] or 0)
    cogs = float(edm[1] or 0)
    cm1 = float(edm[2] or 0)
    ads = float(edm[3] or 0)
    refund = float(edm[4] or 0)
    cm2 = float(edm[5] or 0)
    np_val = float(edm[6] or 0)
    d = int(edm[7] or 0)
    
    amazon_fees = rev - cogs - cm1  # derived: fees = rev - cogs - cm1
    cm2_deductions = cm1 - cm2  # what was deducted to get CM2
    overhead = cm2 - np_val
    
    print(f"\n  --- {m_name} ({d} dni) ---")
    print(f"  Przychód (Revenue):         {rev:>12,.0f}")
    print(f"  COGS:                       {cogs:>12,.0f}")
    print(f"  Marża brutto:               {rev - cogs:>12,.0f}  ({(rev-cogs)/max(rev,1)*100:.1f}%)")
    print(f"  Opłaty Amazon (fee+logi):   {amazon_fees:>12,.0f}")
    print(f"  CM1:                        {cm1:>12,.0f}  ({cm1/max(rev,1)*100:.1f}%)")
    print(f"  Reklama (Ads):              {ads:>12,.0f}")
    print(f"  Refund:                     {refund:>12,.0f}")
    print(f"  Inne CM2 odliczenia:        {cm2_deductions - ads - refund:>12,.0f}")
    print(f"  CM2:                        {cm2:>12,.0f}  ({cm2/max(rev,1)*100:.1f}%)")
    print(f"  Overhead:                   {overhead:>12,.0f}")
    print(f"  WYNIK NETTO (NP):           {np_val:>12,.0f}  ({np_val/max(rev,1)*100:.1f}%)")

# 8. March extrapolation
print("\n" + "="*80)
print("  ESTYMACJA MARZEC 2026 (pełny miesiąc)")
print("="*80)

mar_edm = q("""
    SELECT SUM(CAST(revenue_pln AS FLOAT)),
           SUM(CAST(cogs_pln AS FLOAT)),
           SUM(CAST(cm1_pln AS FLOAT)),
           SUM(CAST(ad_spend_pln AS FLOAT)),
           SUM(CAST(refund_pln AS FLOAT)),
           SUM(CAST(cm2_pln AS FLOAT)),
           SUM(CAST(profit_pln AS FLOAT)),
           COUNT(DISTINCT period_date)
    FROM executive_daily_metrics WITH (NOLOCK)
    WHERE period_date >= '2026-03-01' AND period_date < '2026-03-11'
""")[0]

mar_days = int(mar_edm[7] or 0)
total_mar_days = 31
factor = total_mar_days / max(mar_days, 1)

mar_items = [
    ('Przychód (Revenue)', float(mar_edm[0] or 0)),
    ('COGS', float(mar_edm[1] or 0)),
    ('CM1', float(mar_edm[2] or 0)),
    ('Reklama (Ads)', float(mar_edm[3] or 0)),
    ('Refund', float(mar_edm[4] or 0)),
    ('CM2', float(mar_edm[5] or 0)),
    ('WYNIK NETTO (NP)', float(mar_edm[6] or 0)),
]

print(f"  Bazowe: {mar_days} dni -> ekstrapolacja x{factor:.2f} na {total_mar_days} dni")
print()
for name, val in mar_items:
    est = val * factor
    print(f"  {name:30}: aktual={val:>12,.0f} | estymacja={est:>12,.0f}")

# 9. Q1 2026 total
print("\n" + "="*80)
print("  Q1 2026 ŁĄCZNIE (Sty + Lut + Mar estymacja)")
print("="*80)

jan = q("""SELECT SUM(CAST(revenue_pln AS FLOAT)), SUM(CAST(cogs_pln AS FLOAT)), SUM(CAST(cm1_pln AS FLOAT)), SUM(CAST(ad_spend_pln AS FLOAT)), SUM(CAST(cm2_pln AS FLOAT)), SUM(CAST(profit_pln AS FLOAT))
    FROM executive_daily_metrics WITH (NOLOCK) WHERE period_date >= '2026-01-01' AND period_date < '2026-02-01'""")[0]
feb = q("""SELECT SUM(CAST(revenue_pln AS FLOAT)), SUM(CAST(cogs_pln AS FLOAT)), SUM(CAST(cm1_pln AS FLOAT)), SUM(CAST(ad_spend_pln AS FLOAT)), SUM(CAST(cm2_pln AS FLOAT)), SUM(CAST(profit_pln AS FLOAT))
    FROM executive_daily_metrics WITH (NOLOCK) WHERE period_date >= '2026-02-01' AND period_date < '2026-03-01'""")[0]

mar_est = [float(mar_edm[i] or 0) * factor for i in range(7)]  # full month estimate

q1_items = [
    ('Przychód', float(jan[0] or 0) + float(feb[0] or 0) + mar_est[0]),
    ('COGS', float(jan[1] or 0) + float(feb[1] or 0) + mar_est[1]),
    ('CM1', float(jan[2] or 0) + float(feb[2] or 0) + mar_est[2]),
    ('Ads', float(jan[3] or 0) + float(feb[3] or 0) + mar_est[3]),
    ('CM2', float(jan[4] or 0) + float(feb[4] or 0) + mar_est[5]),
    ('NP', float(jan[5] or 0) + float(feb[5] or 0) + mar_est[6]),
]

for name, val in q1_items:
    print(f"  {name:20}: {val:>14,.0f} PLN")

# 10. Ads discrepancy note
print("\n" + "="*80)
print("  UWAGI / CAVEATS")
print("="*80)
print("  1. Styczen: brak danych finance (acc_finance_transaction) -> CM2 nie zawiera zwrotow/refundow")
print("  2. Ads w EDM (184K sty) vs raw ads table (325K sty) - rozbieznosc wynika z alokacji per-produkt")
print("     Profit engine alokuje ads tylko do matchowanych produktow, reszta jest nieprzypisana")
print("  3. Logistics (courier) w marcu: brak danych -> CM1 marca nie zawiera kosztow kuriera")
print("  4. shipping_surcharge_pln, promo_order_fee_pln, refund_commission_pln - jeszcze puste (bridge nie populuje)")
print("  5. Estymacja marca: liniowa ekstrapolacja z 10 dni (uwzglednia weekendy ale nie sezonowosc)")

c.close()
print("\nDONE")
