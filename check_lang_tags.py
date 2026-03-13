"""
Check language_tag on FR children for color/size attrs.
Also check what the parent has and what variation_theme is set.
"""
import asyncio, json, sys
sys.path.insert(0, r'C:\ACC\apps\api')
from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MARKETPLACE = "A13V1IB3VIYZZH"
DE_MARKETPLACE = "A1PA6795UKMFR9"

# One pair of conflicting SKUs + a few non-conflicting ones for comparison
CHECK_FR = [
    # Conflicting pair
    ("FBA_5903699409312", "Acier 14cm"),
    ("FBA_5902730382072", "Standard 14cm"),
    # Non-conflicting children (different sizes, no dupes)
    ("FBA_5902730382133", "Standard 24cm?"),
    ("FBA_5902730382157", "Standard 28cm?"),
]

# Also check the parent itself 
PARENT_SKU = "FR-PARENT-1367-2A0A63DE"

async def main():
    fr = ListingsClient(marketplace_id=FR_MARKETPLACE)
    de = ListingsClient(marketplace_id=DE_MARKETPLACE)
    
    # 1. New parent on FR - full attrs dump for variation_theme, color, size
    print("=== NEW PARENT ON FR ===")
    try:
        listing = await fr.get_listings_item(fr.seller_id, PARENT_SKU, included_data="attributes")
        attrs = listing.get("attributes", {})
        for key in sorted(attrs.keys()):
            val = attrs[key]
            if any(k in key.lower() for k in ["color", "size", "variation", "theme", "parentage", "parent", "child"]):
                print(f"  {key}: {json.dumps(val, ensure_ascii=False)}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # 2. Check FR children - full color/size/color_name/size_name with language_tag
    print("\n=== FR CHILDREN - FULL ATTR DUMP ===")
    for sku, label in CHECK_FR:
        print(f"\n--- {label}: {sku} ---")
        try:
            listing = await fr.get_listings_item(fr.seller_id, sku, included_data="attributes")
            attrs = listing.get("attributes", {})
            for key in sorted(attrs.keys()):
                val = attrs[key]
                if any(k in key.lower() for k in ["color", "size", "variation", "theme"]):
                    print(f"  {key}: {json.dumps(val, ensure_ascii=False)}")
        except Exception as e:
            print(f"  ERROR: {e}")

    # 3. Check DE parent for family 1367 - what lang tags does it use?
    print("\n=== DE PARENT (reference) ===")
    de_parent_sku = "DE-PARENT-1367"  # Guess - let me find it
    # Actually let me look it up from the restructure log or DB
    from app.core.db_connection import connect_acc
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT TOP 1 steps_json FROM dbo.family_restructure_log WITH (NOLOCK)
        WHERE family_id = 1367 AND marketplace_id = ?
        ORDER BY executed_at DESC
    """, [FR_MARKETPLACE])
    r = cur.fetchone()
    if r and r[0]:
        steps = json.loads(r[0])
        for s in steps:
            if s.get("action") == "CHECK_PARENT_ON_TARGET":
                de_sku = s.get("de_parent_sku") or s.get("source_sku", "")
                print(f"  DE parent SKU from log: {de_sku}")
                if de_sku:
                    de_parent_sku = de_sku
            if s.get("action") == "TRANSLATE_PARENT":
                print(f"  Translated attrs: {json.dumps(s.get('translated_attrs', s.get('body', {})), ensure_ascii=False)[:1000]}")
    
    # Try getting DE parent directly
    # Scan for parent in global_family_child
    cur.execute("""
        SELECT DISTINCT sku_de FROM dbo.global_family_child WITH (NOLOCK)
        WHERE global_family_id = 1367 AND sku_de IS NOT NULL
    """)
    de_skus = [r[0] for r in cur.fetchall()]
    print(f"\n  DE child SKUs found: {len(de_skus)}")
    
    # Get one DE child to see attrs
    if de_skus:
        sample_de = de_skus[0]
        print(f"\n--- DE child sample: {sample_de} ---")
        try:
            listing = await de.get_listings_item(de.seller_id, sample_de, included_data="attributes")
            attrs = listing.get("attributes", {})
            for key in sorted(attrs.keys()):
                val = attrs[key]
                if any(k in key.lower() for k in ["color", "size", "variation", "theme"]):
                    print(f"  {key}: {json.dumps(val, ensure_ascii=False)}")
            # Show parent_sku
            parent_rel = attrs.get("child_parent_sku_relationship", [])
            if parent_rel:
                de_parent_sku = parent_rel[0].get("parent_sku", "")
                print(f"  parent_sku: {de_parent_sku}")
        except Exception as e:
            print(f"  ERROR: {e}")
        
        # Now get the DE parent
        if de_parent_sku and de_parent_sku != "DE-PARENT-1367":
            print(f"\n--- DE PARENT: {de_parent_sku} ---")
            try:
                listing = await de.get_listings_item(de.seller_id, de_parent_sku, included_data="attributes")
                attrs = listing.get("attributes", {})
                for key in sorted(attrs.keys()):
                    val = attrs[key]
                    if any(k in key.lower() for k in ["color", "size", "variation", "theme", "parentage", "parent", "child", "item_name", "brand"]):
                        print(f"  {key}: {json.dumps(val, ensure_ascii=False)}")
            except Exception as e:
                print(f"  ERROR: {e}")

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
