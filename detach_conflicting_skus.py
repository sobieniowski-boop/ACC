"""
Remove child-parent relationship from 4 conflicting "Acier" (steel handle) FBA_ SKUs
that have duplicate color_name+size_name vs the standard variant SKUs.
"""
import asyncio
import json
import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MARKETPLACE = "A13V1IB3VIYZZH"

# These 4 SKUs conflict with existing children — same color+size, different product
CONFLICT_SKUS = [
    "FBA_5903699409312",  # B08JVL9DVD - Acier 14cm (conflicts with FBA_5902730382072)
    "FBA_5903699409329",  # B08JVJSP2L - Acier 16cm (conflicts with FBA_5902730382089)
    "FBA_5903699409336",  # B08JVKSN2M - Acier 18cm (conflicts with FBA_5902730382096)
    "FBA_5903699409343",  # B08JVNDCFK - Acier 20cm (conflicts with FBA_5902730382102)
]

PRODUCT_TYPE = "CONTAINER_LID"

async def main():
    fr = ListingsClient(marketplace_id=FR_MARKETPLACE)
    seller_id = fr.seller_id
    
    for sku in CONFLICT_SKUS:
        print(f"\n=== Detaching {sku} ===")
        # Remove child_parent_sku_relationship and set parentage_level to "child" without parent
        # We PATCH to delete the relationship attributes
        patches = [
            {
                "op": "delete",
                "path": "/attributes/child_parent_sku_relationship",
            },
            {
                "op": "delete",
                "path": "/attributes/parentage_level",
            },
        ]
        try:
            result = await fr.patch_listings_item(
                seller_id, sku, patches, PRODUCT_TYPE,
            )
            status = result.get("status")
            issues = result.get("issues", [])
            print(f"  Status: {status}")
            if issues:
                for iss in issues[:3]:
                    print(f"  [{iss.get('severity')}] {iss.get('code')}: {iss.get('message', '')[:120]}")
            if status == "ACCEPTED":
                print(f"  ✅ Detached")
            else:
                print(f"  ⚠️  {status}")
        except Exception as e:
            print(f"  ERROR: {e}")

    # Verify new parent issues after detach
    print("\n\n=== CHECKING NEW PARENT ISSUES (after detach) ===")
    new_sku = "FR-PARENT-1367-2A0A63DE"
    listing = await fr.get_listings_item(
        fr.seller_id, new_sku,
        included_data="issues",
    )
    issues = listing.get("issues", [])
    issue_counts = {}
    for iss in issues:
        code = iss.get("code", "?")
        issue_counts[code] = issue_counts.get(code, 0) + 1
    print(f"Total issues: {len(issues)}")
    print(f"Issue counts: {json.dumps(issue_counts)}")

if __name__ == "__main__":
    asyncio.run(main())
