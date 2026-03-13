import asyncio, json, sys
sys.path.insert(0, r'C:\ACC\apps\api')
from app.connectors.amazon_sp_api.listings import ListingsClient

async def check():
    c = ListingsClient(marketplace_id='A13V1IB3VIYZZH')
    r = await c.get_listings_item(c.seller_id, 'FR-PARENT-1367-2A0A63DE', included_data='summaries,attributes')
    asin = None
    for s in r.get('summaries', []):
        asin = s.get('asin')
        print(f"Summary: status={s.get('status')}, marketplace={s.get('marketplaceId')}")
        break
    print(f"ASIN: {asin}")
    cpsr = r.get('attributes', {}).get('child_parent_sku_relationship', [])
    print(f"child_parent_sku_relationship: {json.dumps(cpsr, indent=2)}")
    if not asin:
        print("ASIN not yet assigned. Amazon still processing...")

asyncio.run(check())
