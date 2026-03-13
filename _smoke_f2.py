"""Smoke test F2 fix: primary_sku, sku_count, all_skus in profitability orders."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from datetime import date
from app.services.profitability_service import get_profitability_orders

# 1. Basic query
result = get_profitability_orders(date_from=date(2026, 1, 1), date_to=date(2026, 6, 1), page_size=5)
print(f"total={result['total']}, pages={result['pages']}")
for item in result["items"]:
    oid = item["amazon_order_id"]
    sku = item["sku"]
    cnt = item["sku_count"]
    all_s = item.get("all_skus") or "—"
    margin = item["margin_pct"]
    print(f"  {oid}  sku={sku}  cnt={cnt}  all_skus={all_s}  margin={margin}%")

# 2. Loss-only filter (uses inline CM)
loss = get_profitability_orders(date_from=date(2026, 1, 1), date_to=date(2026, 6, 1), loss_only=True, page_size=3)
print(f"\nloss_only: total={loss['total']}")
for item in loss["items"]:
    print(f"  {item['amazon_order_id']}  profit={item['profit_pln']}  margin={item['margin_pct']}%")

# 3. Margin filter
margin_f = get_profitability_orders(date_from=date(2026, 1, 1), date_to=date(2026, 6, 1), min_margin=-10, max_margin=5, page_size=3)
print(f"\nmargin [-10%, 5%]: total={margin_f['total']}")
for item in margin_f["items"]:
    print(f"  {item['amazon_order_id']}  margin={item['margin_pct']}%")

# 4. Overview (loss_orders)
from app.services.profitability_service import get_profitability_overview
overview = get_profitability_overview(date_from=date(2026, 1, 1), date_to=date(2026, 6, 1))
loss_orders = overview.get("loss_orders", [])
print(f"\noverview loss_orders: {len(loss_orders)} items")
for lo in loss_orders[:3]:
    print(f"  {lo['amazon_order_id']}  sku={lo['sku']}  profit={lo['profit_pln']}")

print("\n✓ All smoke checks passed")
