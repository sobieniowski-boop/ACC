"""Step 2: Fix color/size attributes on all children that need corrections.

Fixes:
  - 9x Acier children:  color Transparent → Acier  (+1 needs size=22 cm)
  - 4x Standard 14-20cm: color Transparent → Plastique
  - 1x Standard 24cm:    color '' → Plastique, size '' → 24 cm
  - 8x Beige (silicone): size '' → correct size from catalog
"""
import asyncio, json, os, sys, time
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MP = "A13V1IB3VIYZZH"
PRODUCT_TYPE = "CONTAINER_LID"

# ── Children needing fixes ──────────────────────────────────────────
FIXES = [
    # Acier line: Transparent → Acier (+ fill size on one)
    {"sku": "FBA_5903699409350", "asin": "B08JVKH5B5", "color": "Acier",      "size": "22 cm"},
    {"sku": "FBA_5903699409312", "asin": "B08JVL9DVD", "color": "Acier",      "size": None},
    {"sku": "FBA_5903699409329", "asin": "B08JVJSP2L", "color": "Acier",      "size": None},
    {"sku": "FBA_5903699409336", "asin": "B08JVKSN2M", "color": "Acier",      "size": None},
    {"sku": "FBA_5903699409343", "asin": "B08JVNDCFK", "color": "Acier",      "size": None},
    {"sku": "FBA_5903699409367", "asin": "B08JVMF8CD", "color": "Acier",      "size": None},
    {"sku": "FBA_5903699409374", "asin": "B08JVN8BT5", "color": "Acier",      "size": None},
    {"sku": "FBA_5903699409381", "asin": "B08JVKZMCX", "color": "Acier",      "size": None},
    {"sku": "FBA_5903699409398", "asin": "B08JVKSH4G", "color": "Acier",      "size": None},

    # Standard plastic 14-20cm: Transparent → Plastique
    {"sku": "FBA_5902730382072", "asin": "B07YC4RQ9N", "color": "Plastique",  "size": None},
    {"sku": "FBA_5902730382089", "asin": "B07YC4Z6QY", "color": "Plastique",  "size": None},
    {"sku": "FBA_5902730382096", "asin": "B07YC4PGQW", "color": "Plastique",  "size": None},
    {"sku": "FBA_5902730382102", "asin": "B07YC444C8", "color": "Plastique",  "size": None},

    # Standard 24cm: both empty
    {"sku": "FBA_5902730382126", "asin": "B07YC4SWHK", "color": "Plastique",  "size": "24 cm"},

    # Beige (silicone): fill missing size
    {"sku": "FBA_5903699409480", "asin": "B08JZ25MNT", "color": None,         "size": "30 cm"},
    {"sku": "FBA_5903699409473", "asin": "B08JYGM9BK", "color": None,         "size": "28 cm"},
    {"sku": "FBA_5903699409466", "asin": "B08JY5QH4D", "color": None,         "size": "26 cm"},
    {"sku": "FBA_5903699409459", "asin": "B08JYPKBMY", "color": None,         "size": "24 cm"},
    {"sku": "FBA_5903699409442", "asin": "B08JYYCMHZ", "color": None,         "size": "22 cm"},
    {"sku": "FBA_5903699409435", "asin": "B08JYYJFBM", "color": None,         "size": "20 cm"},
    {"sku": "FBA_5903699409428", "asin": "B08JXHBDC5", "color": None,         "size": "18 cm"},
    {"sku": "FBA_5903699409404", "asin": "B08JYVC8MH", "color": None,         "size": "14 cm"},
]


def _build_patches(fix: dict) -> list[dict]:
    patches = []
    if fix["color"] is not None:
        patches.append({
            "op": "replace",
            "path": "/attributes/color",
            "value": [{"value": fix["color"], "language_tag": "fr_FR", "marketplace_id": FR_MP}],
        })
    if fix["size"] is not None:
        patches.append({
            "op": "replace",
            "path": "/attributes/size",
            "value": [{"value": fix["size"], "language_tag": "fr_FR", "marketplace_id": FR_MP}],
        })
    return patches


async def main():
    client = ListingsClient(marketplace_id=FR_MP)
    seller_id = client.seller_id

    accepted = 0
    failed = 0

    for i, fix in enumerate(FIXES, 1):
        patches = _build_patches(fix)
        label = f"[{i}/{len(FIXES)}] {fix['sku']} ({fix['asin']})"
        color_label = f"color→{fix['color']}" if fix['color'] else ""
        size_label  = f"size→{fix['size']}" if fix['size'] else ""
        print(f"{label}: {color_label} {size_label}".strip())

        try:
            result = await client.patch_listings_item(
                seller_id, fix["sku"], patches, PRODUCT_TYPE,
            )
            status = result.get("status", "UNKNOWN")
            issues = result.get("issues", [])
            if status == "ACCEPTED":
                accepted += 1
                print(f"  ✅ ACCEPTED")
            else:
                failed += 1
                print(f"  ❌ {status}")
                for iss in issues[:3]:
                    print(f"     {iss.get('code')}: {iss.get('message','')[:120]}")
        except Exception as e:
            failed += 1
            print(f"  ❌ ERROR: {e}")

        # Small delay to avoid throttling
        if i < len(FIXES):
            await asyncio.sleep(0.3)

    print(f"\n=== SUMMARY: {accepted} ACCEPTED, {failed} FAILED out of {len(FIXES)} ===")


asyncio.run(main())
