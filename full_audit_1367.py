"""
FULL AUDIT: Get ALL children for family 1367 on FR.
For each: ASIN, SKU, title, current color, current size, catalog parent ASIN.
Identify ALL duplicate (color, size) pairs.
Also identify product line from title to recommend correct color.
"""
import asyncio, json, sys
sys.path.insert(0, r'C:\ACC\apps\api')
from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.db_connection import connect_acc

FR_MARKETPLACE = "A13V1IB3VIYZZH"

async def main():
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    # Get ALL children for family 1367 with their FR marketplace data
    cur.execute("""
        SELECT gfc.de_child_asin, gfc.ean_de
        FROM dbo.global_family_child gfc WITH (NOLOCK)
        WHERE gfc.global_family_id = 1367
    """)
    all_children = [(r[0], r[1]) for r in cur.fetchall()]
    print(f"Total children in family 1367: {len(all_children)}")

    # Find FR SKUs for each ASIN
    asin_to_sku = {}
    for asin, ean in all_children:
        cur.execute(
            "SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WHERE asin = ?",
            [asin]
        )
        rows = cur.fetchall()
        if rows:
            asin_to_sku[asin] = rows[0][0]

    # Also find SKUs that are FBA_ based on EAN
    for asin, ean in all_children:
        if asin not in asin_to_sku and ean:
            # Try FBA_{ean} pattern
            fba_sku = f"FBA_{ean}"
            asin_to_sku[asin] = fba_sku  # Assume FBA pattern

    # Get marketplace_listing_child for catalog parent info
    asin_to_catalog_parent = {}
    for asin, _ in all_children:
        cur.execute(
            "SELECT current_parent_asin FROM dbo.marketplace_listing_child WITH (NOLOCK) WHERE asin = ? AND marketplace = 'FR'",
            [asin]
        )
        rows = cur.fetchall()
        if rows:
            asin_to_catalog_parent[asin] = rows[0][0]

    conn.close()

    # Now get listing details from SP-API for each child
    fr = ListingsClient(marketplace_id=FR_MARKETPLACE)
    
    results = []
    skipped = []
    
    for asin, ean in all_children:
        sku = asin_to_sku.get(asin)
        if not sku:
            skipped.append(asin)
            continue
        
        # Skip amzn.gr.* SKUs
        if sku.startswith("amzn.gr."):
            skipped.append(f"{asin} ({sku} - amzn.gr)")
            continue
        
        try:
            listing = await fr.get_listings_item(
                fr.seller_id, sku,
                included_data="summaries,attributes",
            )
            attrs = listing.get("attributes", {})
            color_list = attrs.get("color", [])
            size_list = attrs.get("size", [])
            color_val = color_list[0].get("value", "") if color_list else ""
            size_val = size_list[0].get("value", "") if size_list else ""
            
            title = ""
            status = []
            for s in listing.get("summaries", []):
                title = s.get("itemName", "")
                status = s.get("status", [])
            
            parent_rel = attrs.get("child_parent_sku_relationship", [])
            current_parent_sku = parent_rel[0].get("parent_sku", "") if parent_rel else ""
            
            catalog_parent = asin_to_catalog_parent.get(asin, "")
            
            results.append({
                "asin": asin,
                "sku": sku,
                "ean": ean,
                "title": title[:100],
                "color": color_val,
                "size": size_val,
                "current_parent_sku": current_parent_sku,
                "catalog_parent": catalog_parent,
                "status": status,
            })
        except Exception as e:
            err = str(e)
            if "404" in err:
                skipped.append(f"{asin} ({sku} - 404)")
            else:
                skipped.append(f"{asin} ({sku} - {err[:60]})")

    # Sort by color+size for easy duplicate spotting
    results.sort(key=lambda r: (r["color"], r["size"]))

    # Print all results
    print(f"\n{'='*120}")
    print(f"{'ASIN':<14} {'SKU':<28} {'COLOR':<25} {'SIZE':<10} {'CAT_PARENT':<14} {'TITLE':<50}")
    print(f"{'='*120}")
    for r in results:
        print(f"{r['asin'] or '':<14} {r['sku'] or '':<28} {r['color'] or '':<25} {r['size'] or '':<10} {r['catalog_parent'] or '':<14} {(r['title'] or '')[:50]}")

    # Find duplicates
    print(f"\n{'='*80}")
    print("DUPLICATE (color, size) PAIRS:")
    seen = {}
    for r in results:
        key = (r["color"], r["size"])
        if key not in seen:
            seen[key] = []
        seen[key].append(r)
    
    for key, items in sorted(seen.items()):
        if len(items) > 1:
            print(f"\n  COLOR='{key[0]}', SIZE='{key[1]}' — {len(items)} children:")
            for r in items:
                print(f"    {r['asin']} {r['sku']:<28} | {r['title'][:60]}")

    # Missing color/size
    print(f"\n{'='*80}")
    print("MISSING COLOR OR SIZE:")
    for r in results:
        if not r["color"] or not r["size"]:
            print(f"  {r['asin']} {r['sku']}: color='{r['color']}' size='{r['size']}' | {r['title'][:60]}")

    # Summary by catalog parent
    print(f"\n{'='*80}")
    print("BY CATALOG PARENT:")
    by_parent = {}
    for r in results:
        p = r["catalog_parent"] or "ORPHAN"
        if p not in by_parent:
            by_parent[p] = []
        by_parent[p].append(r)
    for p, items in sorted(by_parent.items()):
        sample_title = items[0]["title"][:60]
        colors = set(r["color"] for r in items)
        print(f"  {p}: {len(items)} children, colors={colors}")
        print(f"    Sample: {sample_title}")

    print(f"\n{'='*80}")
    print(f"Total listed: {len(results)}")
    print(f"Skipped: {len(skipped)}")
    for s in skipped:
        print(f"  {s}")

    # Save full data for next script
    with open("fr_family_1367_audit.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nFull data saved to fr_family_1367_audit.json")

if __name__ == "__main__":
    asyncio.run(main())
