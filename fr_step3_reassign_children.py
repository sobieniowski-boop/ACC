"""
KROK 3: Podpięcie osieroconych dzieci do nowego parenta na FR.
         Dzieci zostaną zaktualizowane PATCHem z nowym parent_sku.
         Pomijamy amzn.gr.* (FBA returned/used inventory).

PRZED URUCHOMIENIEM: ustaw NEW_PARENT_SKU poniżej!
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.db_connection import connect_acc

FR_MARKETPLACE = "A13V1IB3VIYZZH"
DE_MARKETPLACE = "A1PA6795UKMFR9"
PRODUCT_TYPE = "CONTAINER_LID"
FAMILY_ID = 1367

# ↓↓↓ USTAW TEN SKU po uruchomieniu step2! ↓↓↓
NEW_PARENT_SKU = "FR-PARENT-1367-2A0A63DE"
# ↑↑↑ USTAW TEN SKU po uruchomieniu step2! ↑↑↑


async def get_children_for_family() -> list[dict]:
    """Get children ASINs from marketplace_listing_child + global_family_child,
       then resolve SKUs from acc_amazon_listing_registry / acc_product."""
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    # 1. Get all child ASINs for family 1367 on FR
    cur.execute("""
        SELECT mlc.asin
        FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
        WHERE mlc.marketplace = 'FR'
          AND mlc.asin IN (
              SELECT gfc.de_child_asin
              FROM dbo.global_family_child gfc WITH (NOLOCK)
              WHERE gfc.global_family_id = ?
          )
    """, FAMILY_ID)
    child_asins = [r[0] for r in cur.fetchall() if r[0]]

    if not child_asins:
        conn.close()
        return []

    # 2. Resolve ASIN → SKU from acc_amazon_listing_registry
    ph = ",".join(["?"] * len(child_asins))
    cur.execute(
        f"SELECT asin, merchant_sku FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) "
        f"WHERE asin IN ({ph})",
        *child_asins,
    )
    sku_map = {}
    for asin, sku in cur.fetchall():
        if asin and sku:
            sku_map[str(asin)] = str(sku)

    # 3. Fallback: acc_product
    missing = [a for a in child_asins if a not in sku_map]
    if missing:
        ph2 = ",".join(["?"] * len(missing))
        cur.execute(
            f"SELECT asin, sku FROM dbo.acc_product WITH (NOLOCK) "
            f"WHERE asin IN ({ph2}) AND sku IS NOT NULL",
            *missing,
        )
        for asin, sku in cur.fetchall():
            if asin and sku and str(asin) not in sku_map:
                sku_map[str(asin)] = str(sku)

    conn.close()

    result = []
    for asin in child_asins:
        sku = sku_map.get(str(asin))
        if sku:
            result.append({"sku": sku, "asin": asin})
    return result


async def main():
    if NEW_PARENT_SKU == "CHANGE_ME":
        print("❌ USTAW NEW_PARENT_SKU w skrypcie przed uruchomieniem!")
        return

    print(f"=== KROK 3: Podpięcie dzieci pod nowego parenta ===")
    print(f"New Parent SKU: {NEW_PARENT_SKU}")
    print(f"Marketplace: FR ({FR_MARKETPLACE})")
    print(f"Product type: {PRODUCT_TYPE}")
    print()

    fr_client = ListingsClient(marketplace_id=FR_MARKETPLACE)

    # Verify new parent exists
    print(">>> Verifying new parent exists on FR...")
    try:
        new_parent = await fr_client.get_listings_item(
            fr_client.seller_id, NEW_PARENT_SKU,
            included_data="summaries,attributes",
        )
        parentage = new_parent.get("attributes", {}).get("parentage_level", [])
        new_asin = None
        for s in new_parent.get("summaries", []):
            new_asin = s.get("asin")
            break
        print(f"    ✅ New parent found. ASIN={new_asin}, parentage={json.dumps(parentage)}")
    except Exception as e:
        print(f"    ❌ New parent not found: {e}")
        print("    Być może Amazon jeszcze przetwarza PUT. Spróbuj ponownie za chwilę.")
        return

    # Get children
    print("\n>>> Fetching children list...")
    children = await get_children_for_family()
    print(f"    Found {len(children)} total children in DB")

    # Filter out amzn.gr.*
    actionable = [c for c in children if not str(c["sku"]).lower().startswith("amzn.gr.")]
    skipped = len(children) - len(actionable)
    print(f"    Actionable: {len(actionable)} (skipped {skipped} amzn.gr.* SKUs)")

    # Reassign children
    print(f"\n>>> Reassigning {len(actionable)} children to {NEW_PARENT_SKU}...")
    accepted = 0
    failed = 0
    errors = []

    for i, child in enumerate(actionable, 1):
        sku = child["sku"]
        patches = [
            {
                "op": "replace",
                "path": "/attributes/child_parent_sku_relationship",
                "value": [{
                    "marketplace_id": FR_MARKETPLACE,
                    "child_relationship_type": "variation",
                    "parent_sku": NEW_PARENT_SKU,
                }],
            },
            {
                "op": "replace",
                "path": "/attributes/parentage_level",
                "value": [{
                    "marketplace_id": FR_MARKETPLACE,
                    "value": "child",
                }],
            },
        ]

        try:
            result = await fr_client.patch_listings_item(
                fr_client.seller_id, sku, patches,
                product_type=PRODUCT_TYPE,
            )
            status = result.get("status", "UNKNOWN")
            if status == "ACCEPTED":
                accepted += 1
            else:
                failed += 1
                errors.append({"sku": sku, "status": status, "issues": result.get("issues", [])})

            if i % 10 == 0 or i == len(actionable):
                print(f"    [{i}/{len(actionable)}] accepted={accepted}, failed={failed}")

        except Exception as e:
            failed += 1
            errors.append({"sku": sku, "error": str(e)})
            print(f"    ❌ {sku}: {e}")

        await asyncio.sleep(0.25)  # Rate limit

    print(f"\n=== RESULTS ===")
    print(f"Total:    {len(actionable)}")
    print(f"Accepted: {accepted}")
    print(f"Failed:   {failed}")

    if errors:
        print(f"\n=== ERRORS ({len(errors)}) ===")
        for err in errors[:10]:
            print(json.dumps(err, indent=2, ensure_ascii=False))

    if accepted == len(actionable):
        print(f"\n✅ All {accepted} children reassigned to {NEW_PARENT_SKU}")
    else:
        print(f"\n⚠️  {failed} children failed. Check errors above.")


if __name__ == "__main__":
    asyncio.run(main())
