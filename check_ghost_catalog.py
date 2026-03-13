"""
Inspect ghost/shadow ASIN B07YL989KJ via Catalog Items API on FR marketplace.
Also check the new seller parent B0GRK9SRMP and the old one B0GRJTY66D for comparison.
"""
import asyncio
import json
import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.connectors.amazon_sp_api.catalog import CatalogClient

FR_MARKETPLACE = "A13V1IB3VIYZZH"

ASINS_TO_CHECK = {
    "B07YL989KJ": "Ghost/Shadow parent (from marketplace_listing_child)",
    "B0GRK9SRMP": "New seller-level parent (just created)",
    "B0GRJTY66D": "Old seller-level parent (deleted)",
    "B07YC444C8": "Child ASIN (shown on screenshot)",
}


async def main():
    catalog = CatalogClient(marketplace_id=FR_MARKETPLACE)

    for asin, label in ASINS_TO_CHECK.items():
        print(f"\n{'='*70}")
        print(f"ASIN: {asin} — {label}")
        print('='*70)

        try:
            data = await catalog.get_item(
                asin,
                included_data="summaries,relationships,attributes",
            )

            # Summaries
            summaries = data.get("summaries", [])
            for s in summaries:
                print(f"  Title: {s.get('itemName', '?')}")
                print(f"  Brand: {s.get('brandName', '?')}")
                print(f"  Classification: {s.get('classificationId', '?')}")
                print(f"  Item classification: {s.get('itemClassification', '?')}")
                print(f"  Status: {s.get('status', '?')}")
                print(f"  Product type: {s.get('productType', '?')}")

            # Relationships
            relationships = data.get("relationships", [])
            if relationships:
                print(f"\n  --- Relationships ---")
                for rel_group in relationships:
                    mp = rel_group.get("marketplaceId", "?")
                    rels = rel_group.get("relationships", [])
                    print(f"  Marketplace: {mp}")
                    for r in rels[:10]:
                        child_asins = r.get("childAsins", [])
                        parent_asins = r.get("parentAsins", [])
                        rel_type = r.get("type", "?")
                        print(f"    type={rel_type}")
                        if child_asins:
                            print(f"    childAsins: {child_asins[:5]}{'...' if len(child_asins) > 5 else ''} (total: {len(child_asins)})")
                        if parent_asins:
                            print(f"    parentAsins: {parent_asins}")
            else:
                print(f"\n  No relationships data.")

        except Exception as e:
            print(f"  ERROR: {e}")

        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
