"""Check if B07YL989KJ ghost parent still has catalog relationships.
Also try claim+delete on it too for completeness."""
import asyncio, json, os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.catalog import CatalogClient
from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MP = "A13V1IB3VIYZZH"

async def main():
    catalog = CatalogClient(marketplace_id=FR_MP)
    listings = ListingsClient(marketplace_id=FR_MP)
    sid = listings.seller_id

    # 1. Check catalog relationships for B07YL989KJ
    print("=== B07YL989KJ catalog relationships ===")
    item = await catalog.get_item(
        "B07YL989KJ",
        included_data="relationships",
    )
    rels = item.get("relationships", [])
    for r in rels:
        mp = r.get("marketplaceId", "?")
        children = r.get("childAsins", [])
        rel_type = r.get("type", "?")
        print(f"  MP: {mp}, type: {rel_type}, children: {len(children)}")
        if children:
            print(f"  Sample: {children[:5]}...")

    # 2. Try claim+delete anyway (FO-EK0P-A0FI was deleted, but maybe we can
    #    do a fresh claim to be sure)
    TEMP_SKU = "GHOST-DEL-B07YL989KJ"
    print(f"\n>>> PUT (claim B07YL989KJ)...")
    put_body = {
        "productType": "CONTAINER_LID",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "merchant_suggested_asin": [{"value": "B07YL989KJ", "marketplace_id": FR_MP}],
            "condition_type": [{"value": "new_new", "marketplace_id": FR_MP}],
        },
    }
    try:
        r = await listings.put_listings_item(sid, TEMP_SKU, put_body)
        print(f"    Status: {r.get('status')}")
        for iss in r.get("issues", [])[:3]:
            print(f"    {iss.get('code')}: {iss.get('message','')[:150]}")
    except Exception as e:
        print(f"    ERROR: {e}")

    await asyncio.sleep(2)

    print(f"\n>>> DELETE...")
    try:
        r = await listings.delete_listings_item(sid, TEMP_SKU)
        print(f"    Status: {r.get('status')}")
    except Exception as e:
        print(f"    ERROR: {e}")


asyncio.run(main())
