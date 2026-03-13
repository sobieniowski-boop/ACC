"""SP-API Catalog Items 2022-04-01 connector.

Fetches product details: title, images, BSR rank, category, brand.
Rate limits: 2 req/s burst, 2 req/s restore.

Reference: https://developer-docs.amazon.com/sp-api/docs/catalog-items-api-v2022-04-01-reference
"""
from __future__ import annotations

import asyncio
from typing import Optional

import structlog

from app.connectors.amazon_sp_api.client import SPAPIClient

log = structlog.get_logger(__name__)

# Catalog Items API v2022-04-01
CATALOG_API_VERSION = "2022-04-01"
CATALOG_BASE = f"/catalog/{CATALOG_API_VERSION}/items"

# includedData options: summaries, images, salesRanks, attributes, dimensions, identifiers
DEFAULT_INCLUDE = "summaries,images,salesRanks"


class CatalogClient(SPAPIClient):
    """Fetch product catalog data from Amazon SP-API."""

    async def get_item(
        self,
        asin: str,
        included_data: str = DEFAULT_INCLUDE,
    ) -> dict:
        """
        Get single catalog item by ASIN.

        Returns dict with keys depending on includedData:
          - summaries[]: [{marketplaceId, brandName, itemName, ...}]
          - images[]: [{marketplaceId, images: [{variant, link, width, height}]}]
          - salesRanks[]: [{marketplaceId, classificationRanks: [{classificationId, title, rank}]}]
        """
        params = {
            "marketplaceIds": self.marketplace_id,
            "includedData": included_data,
        }
        data = await self.get(f"{CATALOG_BASE}/{asin}", params)
        return data

    async def search_items(
        self,
        keywords: Optional[str] = None,
        identifiers: Optional[list[str]] = None,
        identifiers_type: str = "ASIN",
        included_data: str = DEFAULT_INCLUDE,
        page_size: int = 20,
    ) -> list[dict]:
        """
        Search catalog items by keywords or identifiers (ASIN, EAN, UPC, SKU).

        Returns list of item dicts with summaries, images, salesRanks.
        Max 20 items per page, auto-paginates.
        """
        params: dict = {
            "marketplaceIds": self.marketplace_id,
            "includedData": included_data,
            "pageSize": min(page_size, 20),
        }

        if identifiers:
            params["identifiers"] = ",".join(identifiers)
            params["identifiersType"] = identifiers_type
        elif keywords:
            params["keywords"] = keywords
        else:
            raise ValueError("Provide either keywords or identifiers")

        all_items: list[dict] = []
        page_token: Optional[str] = None

        for _ in range(50):  # max 50 pages
            if page_token:
                params["pageToken"] = page_token

            data = await self.get(CATALOG_BASE, params)
            items = data.get("items", [])
            all_items.extend(items)

            pagination = data.get("pagination", {})
            page_token = pagination.get("nextToken")
            if not page_token:
                break

            # Rate limit: stay within 2 req/s (0.6s provides safety margin)
            await asyncio.sleep(0.6)

        log.info("catalog.search_complete", count=len(all_items))
        return all_items

    async def get_items_batch(
        self,
        asins: list[str],
        included_data: str = DEFAULT_INCLUDE,
        batch_size: int = 20,
    ) -> list[dict]:
        """
        Fetch multiple items by ASIN in batches of 20.
        Uses searchItems with identifiers (max 20 per call).
        """
        all_items: list[dict] = []

        for i in range(0, len(asins), batch_size):
            batch = asins[i: i + batch_size]
            try:
                items = await self.search_items(
                    identifiers=batch,
                    identifiers_type="ASIN",
                    included_data=included_data,
                )
                all_items.extend(items)
                log.info("catalog.batch_ok", batch=i // batch_size + 1, count=len(items))
            except Exception as e:
                log.error("catalog.batch_error", batch=i // batch_size + 1, error=str(e))

            # Rate limit: stay within 2 req/s (0.6s provides safety margin)
            if i + batch_size < len(asins):
                await asyncio.sleep(0.6)

        return all_items


def parse_catalog_item(item: dict, marketplace_id: str) -> dict:
    """
    Extract structured product data from catalog API response.

    Returns dict ready for acc_product upsert:
        asin, title, brand, category, subcategory, image_url, bsr_rank, bsr_category
    """
    result: dict = {"asin": item.get("asin", "")}

    # Summaries — find matching marketplace
    for summary_group in item.get("summaries", []):
        if summary_group.get("marketplaceId") == marketplace_id:
            result["title"] = summary_group.get("itemName", "")
            result["brand"] = summary_group.get("brandName", "")

            # Classification (category path)
            classifications = summary_group.get("classifications", [])
            if classifications:
                result["category"] = classifications[0].get("displayName", "")
            break

    # Images — find main image
    for img_group in item.get("images", []):
        if img_group.get("marketplaceId") == marketplace_id:
            images = img_group.get("images", [])
            # Prefer MAIN variant, fallback to first
            main_imgs = [i for i in images if i.get("variant") == "MAIN"]
            if main_imgs:
                result["image_url"] = main_imgs[0].get("link", "")
            elif images:
                result["image_url"] = images[0].get("link", "")
            break

    # Sales Ranks — BSR
    for rank_group in item.get("salesRanks", []):
        if rank_group.get("marketplaceId") == marketplace_id:
            # classificationRanks = specific category rank
            class_ranks = rank_group.get("classificationRanks", [])
            if class_ranks:
                result["bsr_rank"] = class_ranks[0].get("rank")
                result["bsr_category"] = class_ranks[0].get("title", "")
            # displayGroupRanks = broad category rank (fallback)
            elif rank_group.get("displayGroupRanks"):
                dg = rank_group["displayGroupRanks"][0]
                result["bsr_rank"] = dg.get("rank")
                result["bsr_category"] = dg.get("title", "")
            break

    # Identifiers — EAN, UPC, etc.
    for id_group in item.get("identifiers", []):
        if id_group.get("marketplaceId") == marketplace_id or not result.get("ean"):
            for ident in id_group.get("identifiers", []):
                id_type = ident.get("identifierType", "")
                id_val = ident.get("identifier", "")
                if id_type == "EAN" and id_val:
                    result["ean"] = id_val
                elif id_type == "UPC" and id_val and "ean" not in result:
                    result["upc"] = id_val

    return result
