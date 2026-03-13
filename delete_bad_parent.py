"""Delete bad parent B0GRJDYDD5 from FR marketplace."""
import asyncio
import structlog
from app.connectors.amazon_sp_api.listings import ListingsClient

log = structlog.get_logger()

async def main():
    """Delete bad parent B0GRJDYDD5 (SKU: 7P-HO4I-IM4E) from FR marketplace."""
    
    parent_sku = "7P-HO4I-IM4E"
    parent_asin = "B0GRJDYDD5"
    target_marketplace_id = "A13V1IB3VIYZZH"  # FR
    
    print(f"🗑️  Deleting parent: ASIN {parent_asin}, SKU {parent_sku} from FR marketplace...")
    
    client = ListingsClient(marketplace_id=target_marketplace_id)
    seller_id = client.seller_id
    
    try:
        result = await client.delete_listings_item(seller_id, parent_sku)
        print(f"✅ DELETE result: {result}")
        print(f"\n⚠️  All children will be automatically orphaned by Amazon")
        print(f"⚠️  Now run restructure again to create parent with correct structure")
    except Exception as e:
        print(f"❌ DELETE failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
