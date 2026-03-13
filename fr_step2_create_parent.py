"""
KROK 2: Utwórz nowego parenta na FR z atrybutami skopiowanymi z DE,
         przetłumaczonymi na FR. Użyjemy NOWEGO SKU (uuid-based)
         żeby dostać nowy ASIN. variation_theme = COLOR/SIZE
"""
import asyncio
import copy
import json
import os
import sys
import uuid

sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.config import settings
from openai import AsyncOpenAI

FR_MARKETPLACE = "A13V1IB3VIYZZH"
DE_MARKETPLACE = "A1PA6795UKMFR9"
DE_PARENT_SKU = "7P-HO4I-IM4E"
PRODUCT_TYPE = "CONTAINER_LID"

# New unique SKU so Amazon assigns a fresh ASIN
NEW_PARENT_SKU = f"FR-PARENT-1367-{uuid.uuid4().hex[:8].upper()}"


def _rewrite_marketplace_ids(attrs: dict, source_mp: str, target_mp: str) -> dict:
    """Deep-copy attrs, replacing marketplace_id and language_tag from DE → FR."""
    result = copy.deepcopy(attrs)
    source_locale = "de_DE"
    target_locale = "fr_FR"
    for attr_values in result.values():
        if not isinstance(attr_values, list):
            continue
        for entry in attr_values:
            if not isinstance(entry, dict):
                continue
            if entry.get("marketplace_id") == source_mp:
                entry["marketplace_id"] = target_mp
            if entry.get("language_tag") == source_locale:
                entry["language_tag"] = target_locale
            for v in entry.values():
                if isinstance(v, dict) and v.get("language_tag") == source_locale:
                    v["language_tag"] = target_locale
    return result


async def _translate_text(gpt_client, text: str) -> str:
    """Translate DE → FR using GPT."""
    if not text or len(text.strip()) < 2:
        return text
    try:
        resp = await gpt_client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            max_completion_tokens=500,
            messages=[{
                "role": "user",
                "content": (
                    f"Translate the following German product text to French. "
                    f"Keep product codes, numbers, and brand names as-is. "
                    f"Return ONLY the translation, no explanations.\n\n{text}"
                ),
            }]
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"  ⚠️  Translation failed for '{text[:50]}...': {e}")
        return text


async def main():
    print(f"=== KROK 2: Tworzenie nowego parenta na FR ===")
    print(f"DE Parent SKU: {DE_PARENT_SKU}")
    print(f"New FR Parent SKU: {NEW_PARENT_SKU}")
    print(f"Product type: {PRODUCT_TYPE}")
    print()

    de_client = ListingsClient(marketplace_id=DE_MARKETPLACE)
    fr_client = ListingsClient(marketplace_id=FR_MARKETPLACE)

    # 1. Fetch DE parent attributes
    print(">>> Fetching DE parent listing...")
    de_listing = await de_client.get_listings_item(
        de_client.seller_id, DE_PARENT_SKU,
        included_data="summaries,attributes",
    )
    de_attrs = de_listing.get("attributes", {})
    print(f"    Got {len(de_attrs)} attribute groups from DE")

    # 2. Rewrite marketplace_id and locale
    fr_attrs = _rewrite_marketplace_ids(de_attrs, DE_MARKETPLACE, FR_MARKETPLACE)

    # 3. Remove DE-specific attrs
    for skip in ("merchant_shipping_group", "unit_count"):
        fr_attrs.pop(skip, None)

    # 4. Set correct parent relationship structure
    fr_attrs["child_parent_sku_relationship"] = [{
        "marketplace_id": FR_MARKETPLACE,
        "parent_relationship_type": "variation",
    }]
    fr_attrs["parentage_level"] = [{
        "marketplace_id": FR_MARKETPLACE,
        "value": "parent",
    }]
    fr_attrs["variation_theme"] = [{
        "name": "COLOR/SIZE",
    }]

    # 5. Translate text fields
    print(">>> Translating DE → FR...")
    gpt_client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY, timeout=30.0)

    text_fields = ["item_name", "bullet_point", "product_description", "generic_keyword"]
    for field in text_fields:
        values = fr_attrs.get(field)
        if not isinstance(values, list):
            continue
        for entry in values:
            if isinstance(entry, dict) and "value" in entry:
                original = entry["value"]
                translated = await _translate_text(gpt_client, original)
                entry["value"] = translated
                print(f"    {field}: '{original[:60]}...' → '{translated[:60]}...'")
            elif isinstance(entry, dict) and "values" in entry:
                # bullet_point uses nested "values" key (list of strings)
                for i, val in enumerate(entry.get("values", [])):
                    if isinstance(val, str):
                        translated = await _translate_text(gpt_client, val)
                        entry["values"][i] = translated
                        print(f"    {field}[{i}]: '{val[:50]}...' → '{translated[:50]}...'")

    # 6. Trim item_name to 200 chars
    item_name_vals = fr_attrs.get("item_name", [])
    for entry in item_name_vals:
        if isinstance(entry, dict):
            v = entry.get("value", "")
            if isinstance(v, str) and len(v) > 200:
                entry["value"] = v[:200].rstrip()
                print(f"    ✂️  Trimmed item_name to 200 chars")

    # 7. Create the new parent via PUT
    put_body = {
        "productType": PRODUCT_TYPE,
        "requirements": "LISTING",
        "attributes": fr_attrs,
    }

    print()
    print(f">>> Creating new parent: {NEW_PARENT_SKU}")
    result = await fr_client.put_listings_item(
        fr_client.seller_id, NEW_PARENT_SKU, put_body,
    )
    print(f"\n=== PUT RESULT ===")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    status = result.get("status", "UNKNOWN")
    if status == "ACCEPTED":
        print(f"\n✅ New parent created: SKU={NEW_PARENT_SKU}")
        print(f"   Submission ID: {result.get('submissionId')}")
        print(f"   Amazon will assign a new ASIN shortly.")
        print(f"\n   >>> SAVE THIS SKU for step 3: {NEW_PARENT_SKU}")
    else:
        print(f"\n❌ Failed: {status}")
        for i in result.get("issues", []):
            print(f"   - {i.get('code')}: {i.get('message')}")


if __name__ == "__main__":
    asyncio.run(main())
