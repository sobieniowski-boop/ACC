"""Smoke test: verify CM1/CM2/NP breakdown for January 2026."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

from datetime import date
from app.services.profit_engine import get_product_profit_table

result = get_product_profit_table(
    date_from=date(2026, 1, 1),
    date_to=date(2026, 1, 31),
    profit_mode="cm1",
    sort_by="revenue",
    sort_dir="desc",
    page_size=3,
    page=1,
    group_by="sku",
)
s = result["summary"]
print("=== SUMMARY Jan 2026 ===")
for k in ["total_revenue_pln", "total_cogs_pln", "total_fees_pln",
          "total_logistics_pln", "total_cm1_pln", "total_cm1_pct",
          "total_ads_cost_pln", "total_cm2_pln", "total_cm2_pct",
          "total_np_pln", "total_np_pct"]:
    print(f"  {k:30s} = {s[k]}")

print("\n=== TOP 3 SKUs ===")
for item in result["items"][:3]:
    sku = item["sku"]
    print(f"\n  SKU: {sku}")
    print(f"    revenue        = {item['revenue_pln']:,.2f}")
    print(f"    cogs           = {item['cogs_pln']:,.2f}")
    print(f"    amazon_fees    = {item['amazon_fees_pln']:,.2f}")
    print(f"      fba_fee      = {item['fba_fee_pln']:,.2f}")
    print(f"      referral_fee = {item['referral_fee_pln']:,.2f}")
    print(f"    logistics      = {item.get('logistics_pln', 0):,.2f}")
    print(f"    CM1            = {item['cm1_profit']:,.2f} ({item['cm1_percent']}%)")
    print(f"    ads_cost       = {item.get('ads_cost_pln', 0):,.2f}")
    print(f"    returns_net    = {item.get('returns_net_pln', 0):,.2f}")
    print(f"    CM2            = {item['cm2_profit']:,.2f} ({item['cm2_percent']}%)")
    print(f"    NP             = {item['np_profit']:,.2f} ({item['np_percent']}%)")
