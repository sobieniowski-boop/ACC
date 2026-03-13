"""Step 1: Delete old parent FR-PARENT-1367-2A0A63DE"""
import asyncio, json, os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MP = "A13V1IB3VIYZZH"
OLD_PARENT_SKU = "FR-PARENT-1367-2A0A63DE"

async def main():
    client = ListingsClient(marketplace_id=FR_MP)
    print(f"Deleting parent: {OLD_PARENT_SKU}")
    result = await client.delete_listings_item(client.seller_id, OLD_PARENT_SKU)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"\nStatus: {result.get('status', 'UNKNOWN')}")

asyncio.run(main())
