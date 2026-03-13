"""Step 5: Verify the restructure — check new parent status & sample children."""
import asyncio, json, os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MP = "A13V1IB3VIYZZH"
NEW_PARENT_SKU = "FR-PARENT-1367-CEA8F738"

# Sample children: one from each color group
SAMPLE_CHILDREN = [
    ("FBA_5903699409312", "Acier 14cm"),
    ("FBA_5902730382072", "Plastique 14cm"),
    ("FBA_5902730382126", "Plastique 24cm (was empty)"),
    ("FBA_5903699409480", "Beige 30cm (was missing size)"),
    ("MAG_5903699470473", "Brillant 14cm"),
    ("MAG_5903699470381", "Noir argent 14cm"),
    ("MAG_5903699470299", "noir mat 14cm"),
    ("MAG_5903699470206", "argentée 14cm"),
    ("FBA_5903699409350", "Acier 22cm (was missing size)"),
]


async def main():
    client = ListingsClient(marketplace_id=FR_MP)
    sid = client.seller_id

    # 1. Check parent
    print("=== NEW PARENT STATUS ===")
    parent = await client.get_listings_item(
        sid, NEW_PARENT_SKU, included_data="summaries,attributes,issues",
    )
    summaries = parent.get("summaries", [{}])
    if summaries:
        s = summaries[0]
        print(f"  ASIN: {s.get('asin')}")
        print(f"  Status: {s.get('status', [])}")
        print(f"  Item name: {s.get('itemName','')[:100]}")

    issues = parent.get("issues", [])
    print(f"  Issues: {len(issues)}")
    if issues:
        # Group by code
        from collections import Counter
        codes = Counter(i.get("code") for i in issues)
        for code, cnt in codes.most_common(10):
            print(f"    {code}: {cnt}x")

    attrs = parent.get("attributes", {})
    theme = attrs.get("variation_theme", [])
    plevel = attrs.get("parentage_level", [])
    print(f"  variation_theme: {json.dumps(theme)}")
    print(f"  parentage_level: {json.dumps(plevel)}")

    # 2. Check sample children
    print("\n=== SAMPLE CHILDREN ===")
    for sku, label in SAMPLE_CHILDREN:
        child = await client.get_listings_item(
            sid, sku, included_data="summaries,attributes",
        )
        s = (child.get("summaries") or [{}])[0]
        a = child.get("attributes", {})
        color = a.get("color", [{}])
        size = a.get("size", [{}])
        parent_rel = a.get("child_parent_sku_relationship", [{}])
        color_val = color[0].get("value", "?") if color else "?"
        size_val = size[0].get("value", "?") if size else "?"
        parent_sku = parent_rel[0].get("parent_sku", "?") if parent_rel else "?"

        status_ok = "✅" if parent_sku == NEW_PARENT_SKU else "❌"
        print(f"  {status_ok} {label}: color={color_val}, size={size_val}, parent={parent_sku}")
        await asyncio.sleep(0.3)

    print("\nDone ✅")


asyncio.run(main())
