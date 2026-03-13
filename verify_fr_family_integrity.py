import asyncio
import json
from collections import Counter

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.db_connection import connect_acc

FR_MP = "A13V1IB3VIYZZH"
DE_MP = "A1PA6795UKMFR9"
PARENT_SKU = "7P-HO4I-IM4E"
EXPECTED_PARENT_ASIN_FR = "B0GRJTY66D"


def _extract_parentage(attrs: dict) -> str | None:
    vals = attrs.get("parentage_level", [])
    if isinstance(vals, list):
        for v in vals:
            if isinstance(v, dict) and v.get("value"):
                return str(v["value"])
    return None


def _extract_cpsr(attrs: dict) -> dict | None:
    vals = attrs.get("child_parent_sku_relationship", [])
    if isinstance(vals, list) and vals:
        for v in vals:
            if isinstance(v, dict):
                return v
    return None


def _first_value(attrs: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        vals = attrs.get(key, [])
        if isinstance(vals, list):
            for v in vals:
                if isinstance(v, dict) and v.get("value"):
                    return str(v["value"])
    return None


def _load_latest_children_from_run() -> list[dict]:
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT TOP 1 result_json
            FROM dbo.family_restructure_run WITH (NOLOCK)
            WHERE family_id = ? AND marketplace_id = ? AND status = 'completed' AND dry_run = 0
            ORDER BY created_at DESC
            """,
            1367,
            FR_MP,
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return []
        result = json.loads(row[0])
        steps = result.get("steps", [])
        children = []
        for s in steps:
            if s.get("action") == "REASSIGN_CHILD" and s.get("status") == "ACCEPTED":
                if s.get("sku") and s.get("asin"):
                    children.append({"sku": s["sku"], "asin": s["asin"]})
        # Deduplicate by SKU
        uniq = {}
        for c in children:
            uniq[c["sku"]] = c
        return list(uniq.values())
    finally:
        conn.close()


async def main() -> None:
    fr = ListingsClient(marketplace_id=FR_MP)
    de = ListingsClient(marketplace_id=DE_MP)
    seller_id = fr.seller_id

    # Parent check FR
    fr_parent = await fr.get_listings_item(seller_id, PARENT_SKU, included_data="summaries,attributes")
    fr_summ = (fr_parent.get("summaries") or [{}])[0]
    fr_attrs = fr_parent.get("attributes", {})
    fr_parent_asin = fr_summ.get("asin")
    fr_parentage = _extract_parentage(fr_attrs)
    fr_cpsr = _extract_cpsr(fr_attrs)

    # Parent check DE for similarity
    de_parent = await de.get_listings_item(seller_id, PARENT_SKU, included_data="summaries,attributes")
    de_summ = (de_parent.get("summaries") or [{}])[0]
    de_attrs = de_parent.get("attributes", {})

    keyset_fr = set(fr_attrs.keys())
    keyset_de = set(de_attrs.keys())
    overlap = sorted(list(keyset_fr & keyset_de))

    # Children integrity check
    children = _load_latest_children_from_run()
    bad = []
    relationship_counter = Counter()
    parentage_counter = Counter()

    for child in children:
        sku = child["sku"]
        asin = child["asin"]
        try:
            item = await fr.get_listings_item(seller_id, sku, included_data="attributes")
            attrs = item.get("attributes", {})
            cpsr = _extract_cpsr(attrs) or {}
            parentage = _extract_parentage(attrs)

            actual_parent_sku = cpsr.get("parent_sku")
            rel_type = cpsr.get("child_relationship_type") or cpsr.get("parent_relationship_type")

            relationship_counter[str(rel_type)] += 1
            parentage_counter[str(parentage)] += 1

            ok_parent = actual_parent_sku == PARENT_SKU
            ok_parentage = (parentage or "").lower() == "child"
            ok_rel_type = str(rel_type).lower() == "variation"

            if not (ok_parent and ok_parentage and ok_rel_type):
                bad.append(
                    {
                        "sku": sku,
                        "asin": asin,
                        "parent_sku": actual_parent_sku,
                        "child_relationship_type": rel_type,
                        "parentage_level": parentage,
                    }
                )
        except Exception as e:
            bad.append({"sku": sku, "asin": asin, "error": str(e)})

        await asyncio.sleep(0.15)

    report = {
        "parent_fr": {
            "sku": PARENT_SKU,
            "asin_live": fr_parent_asin,
            "asin_expected": EXPECTED_PARENT_ASIN_FR,
            "asin_match": fr_parent_asin == EXPECTED_PARENT_ASIN_FR,
            "product_type": fr_summ.get("productType"),
            "parentage_level": fr_parentage,
            "child_parent_sku_relationship": fr_cpsr,
            "variation_theme": fr_attrs.get("variation_theme"),
            "item_name": _first_value(fr_attrs, ("item_name",)),
        },
        "parent_de_reference": {
            "sku": PARENT_SKU,
            "asin": de_summ.get("asin"),
            "product_type": de_summ.get("productType"),
            "variation_theme": de_attrs.get("variation_theme"),
            "item_name": _first_value(de_attrs, ("item_name",)),
        },
        "parent_similarity": {
            "fr_attr_count": len(keyset_fr),
            "de_attr_count": len(keyset_de),
            "shared_attr_count": len(overlap),
            "shared_attr_sample": overlap[:20],
        },
        "children_integrity": {
            "checked": len(children),
            "invalid_count": len(bad),
            "relationship_types": dict(relationship_counter),
            "parentage_levels": dict(parentage_counter),
            "invalid_sample": bad[:15],
        },
    }

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
