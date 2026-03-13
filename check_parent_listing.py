import asyncio
import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.connectors.amazon_sp_api.listings import ListingsClient

async def check_parent():
    client = ListingsClient('A13V1IB3VIYZZH')  # FR marketplace
    seller_id = client.seller_id
    
    # Get parent listing
    try:
        parent_data = await client.get_listings_item(
            seller_id,
            '7P-HO4I-IM4E',
            included_data='summaries,attributes,offers'
        )
        
        print("=== PARENT LISTING B0GRJDYDD5 ===\n")
        
        # Summaries
        summaries = parent_data.get('summaries', [])
        if summaries:
            print("SUMMARIES:")
            for s in summaries:
                print(f"  Title: {s.get('title', 'N/A')[:80]}")
                print(f"  Status: {s.get('status', 'N/A')}")
                print(f"  Product Type: {s.get('productType', 'N/A')}")
        
        # Attributes
        attrs = parent_data.get('attributes', {})
        print(f"\nATTRIBUTES (count: {len(attrs)}):")
        for key in sorted(attrs.keys())[:20]:
            val = attrs[key]
            if isinstance(val, list) and len(val) > 0:
                print(f"  - {key}: {str(val[0])[:100]}")
            else:
                print(f"  - {key}: {str(val)[:100]}")
        
        # Offers
        offers = parent_data.get('offers', [])
        print(f"\nOFFERS (count: {len(offers)}):")
        for i, offer in enumerate(offers[:3]):
            print(f"  Offer {i+1}: {offer}")
            
        print("\n✓ Parent exists and is retrievable")
        
    except Exception as e:
        print(f"Error GET parent: {str(e)[:500]}")

asyncio.run(check_parent())
