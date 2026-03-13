"""Check if parent still exists on DE marketplace."""
import asyncio
from app.connectors.amazon_sp_api.listings import ListingsClient

async def main():
    """Check parent 7P-HO4I-IM4E on DE marketplace."""
    
    parent_sku = "7P-HO4I-IM4E"
    de_marketplace_id = "A1PA6795UKMFR9"  # DE
    
    print(f"🔍 Checking parent SKU {parent_sku} on DE marketplace...")
    
    de_client = ListingsClient(marketplace_id=de_marketplace_id)
    seller_id = de_client.seller_id
    
    try:
        result = await de_client.get_listings_item(
            seller_id, parent_sku,
            included_data="summaries"
        )
        summaries = result.get("summaries", [])
        if summaries:
            asin = summaries[0].get("asin")
            title = summaries[0].get("itemName", "N/A")
            status = summaries[0].get("status", [])
            print(f"✅ Parent EXISTS on DE:")
            print(f"   ASIN: {asin}")
            print(f"   Title: {title}")
            print(f"   Status: {status}")
        else:
            print(f"⚠️  Parent exists but no summaries")
    except Exception as e:
        if "404" in str(e):
            print(f"❌ Parent NOT FOUND on DE marketplace")
            print(f"❌❌❌ PARENT WAS DELETED FROM DE! THIS IS CRITICAL!")
        else:
            print(f"❌ Error checking parent: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
