"""Quick parent status check."""
import asyncio, os, sys
from collections import Counter
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'
from app.connectors.amazon_sp_api.listings import ListingsClient

async def main():
    c = ListingsClient(marketplace_id="A13V1IB3VIYZZH")
    p = await c.get_listings_item(c.seller_id, "FR-PARENT-1367-CEA8F738", included_data="summaries,issues")
    s = (p.get("summaries") or [{}])[0]
    issues = p.get("issues", [])
    print(f"Status: {s.get('status', [])}")
    print(f"Issues: {len(issues)}")
    codes = Counter(i.get("code") for i in issues)
    for code, cnt in codes.most_common():
        print(f"  {code}: {cnt}x")

asyncio.run(main())
