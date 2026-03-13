"""Check catalog relationships for all 3 ghost parents after claim+delete."""
import asyncio, json, os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.catalog import CatalogClient

FR_MP = "A13V1IB3VIYZZH"

GHOSTS = ["B07YL989KJ", "B08KH6GCMW", "B08KKTKHHG"]

async def main():
    catalog = CatalogClient(marketplace_id=FR_MP)

    for asin in GHOSTS:
        item = await catalog.get_item(asin, included_data="relationships,summaries")
        rels = item.get("relationships", [])
        sums = item.get("summaries", [])
        
        title = sums[0].get("itemName", "?")[:60] if sums else "?"
        
        print(f"\n{asin}: {title}")
        for r in rels:
            children = r.get("childAsins", [])
            rel_type = r.get("type", "?")
            print(f"  type={rel_type}, children={len(children)}")
            if children:
                for c in children[:10]:
                    print(f"    - {c}")
                if len(children) > 10:
                    print(f"    ... +{len(children)-10} more")

asyncio.run(main())
