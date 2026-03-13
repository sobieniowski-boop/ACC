"""Quick smoke test: call profit_v2 products endpoint logic directly."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")

from datetime import date
from app.services.profit_engine import get_product_profit_table

try:
    result = get_product_profit_table(
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 31),
        marketplace_id=None,
        group_by="sku",
        profit_mode="cm1",
        sort_by="revenue",
        sort_dir="desc",
        page_size=5,
        page=1,
    )
    print(f"OK! items={len(result.get('items', []))}, summary keys={list(result.get('summary', {}).keys())[:5]}")
    for item in result.get("items", [])[:3]:
        print(f"  SKU={item.get('sku','?')[:20]} rev={item.get('revenue_pln',0):.0f} cm1={item.get('cm1_profit_pln',0):.0f}")
    print(f"data_source={result.get('data_source','?')}")
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")
