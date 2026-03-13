"""Step 3: Create a brand-new parent on FR marketplace.

Fetches attributes from DE parent (7P-HO4I-IM4E, which still exists on DE),
rewrites marketplace/locale, translates text fields DE→FR, then PUTs with
a fresh UUID-based SKU so Amazon assigns a new ASIN.
"""
import asyncio, copy, json, os, sys, uuid
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.config import settings
from openai import AsyncOpenAI

FR_MP = "A13V1IB3VIYZZH"
DE_MP = "A1PA6795UKMFR9"
DE_PARENT_SKU = "7P-HO4I-IM4E"
PRODUCT_TYPE = "CONTAINER_LID"

NEW_PARENT_SKU = f"FR-PARENT-1367-{uuid.uuid4().hex[:8].upper()}"


def _rewrite_marketplace(attrs: dict) -> dict:
    result = copy.deepcopy(attrs)
    for attr_values in result.values():
        if not isinstance(attr_values, list):
            continue
        for entry in attr_values:
            if not isinstance(entry, dict):
                continue
            if entry.get("marketplace_id") == DE_MP:
                entry["marketplace_id"] = FR_MP
            if entry.get("language_tag") == "de_DE":
                entry["language_tag"] = "fr_FR"
            for v in entry.values():
                if isinstance(v, dict) and v.get("language_tag") == "de_DE":
                    v["language_tag"] = "fr_FR"
    return result


async def _translate(gpt, text: str) -> str:
    if not text or len(text.strip()) < 2:
        return text
    try:
        resp = await gpt.chat.completions.create(
            model=settings.OPENAI_MODEL,
            max_completion_tokens=500,
            messages=[{
                "role": "user",
                "content": (
                    "Translate the following German product text to French. "
                    "Keep product codes, numbers, and brand names (KADAX) as-is. "
                    "Return ONLY the translation.\n\n" + text
                ),
            }],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"  ⚠️  Translation failed: {e}")
        return text


async def main():
    print(f"=== STEP 3: Create new parent on FR ===")
    print(f"New SKU: {NEW_PARENT_SKU}")

    de_client = ListingsClient(marketplace_id=DE_MP)
    fr_client = ListingsClient(marketplace_id=FR_MP)

    # 1. Fetch DE parent
    print("Fetching DE parent attrs...")
    de_listing = await de_client.get_listings_item(
        de_client.seller_id, DE_PARENT_SKU,
        included_data="summaries,attributes",
    )
    de_attrs = de_listing.get("attributes", {})
    print(f"  Got {len(de_attrs)} attribute groups")

    # 2. Rewrite MP/locale
    fr_attrs = _rewrite_marketplace(de_attrs)

    # 3. Remove DE-specific
    for skip in ("merchant_shipping_group", "unit_count", "fulfillment_availability"):
        fr_attrs.pop(skip, None)

    # 4. Set parent structure
    fr_attrs["child_parent_sku_relationship"] = [{
        "marketplace_id": FR_MP,
        "parent_relationship_type": "variation",
    }]
    fr_attrs["parentage_level"] = [{
        "marketplace_id": FR_MP,
        "value": "parent",
    }]
    fr_attrs["variation_theme"] = [{"name": "COLOR/SIZE"}]

    # 5. Translate text fields
    print("Translating DE → FR...")
    gpt = AsyncOpenAI(api_key=settings.OPENAI_API_KEY, timeout=30.0)

    for field in ("item_name", "bullet_point", "product_description", "generic_keyword"):
        values = fr_attrs.get(field)
        if not isinstance(values, list):
            continue
        for entry in values:
            if not isinstance(entry, dict):
                continue
            if "value" in entry and isinstance(entry["value"], str):
                orig = entry["value"]
                entry["value"] = await _translate(gpt, orig)
                print(f"  {field}: {orig[:50]}… → {entry['value'][:50]}…")
            if "values" in entry and isinstance(entry["values"], list):
                for i, val in enumerate(entry["values"]):
                    if isinstance(val, str):
                        entry["values"][i] = await _translate(gpt, val)
                        print(f"  {field}[{i}]: {val[:40]}… → {entry['values'][i][:40]}…")

    # 6. Trim item_name
    for entry in fr_attrs.get("item_name", []):
        if isinstance(entry, dict) and isinstance(entry.get("value"), str):
            if len(entry["value"]) > 200:
                entry["value"] = entry["value"][:200].rstrip()
                print("  ✂️  Trimmed item_name to 200 chars")

    # 7. PUT new parent
    put_body = {
        "productType": PRODUCT_TYPE,
        "requirements": "LISTING",
        "attributes": fr_attrs,
    }

    print(f"\nCreating parent {NEW_PARENT_SKU}...")
    result = await fr_client.put_listings_item(fr_client.seller_id, NEW_PARENT_SKU, put_body)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    status = result.get("status", "UNKNOWN")
    if status == "ACCEPTED":
        print(f"\n✅ Parent created: {NEW_PARENT_SKU}")
        print("Save this SKU for step 4!")
    else:
        print(f"\n❌ Status: {status}")
        for iss in result.get("issues", [])[:5]:
            print(f"  {iss.get('code')}: {iss.get('message','')[:150]}")

asyncio.run(main())
