"""SP-API Listings Items 2021-08-01 connector.

Manages listings: get, put (create/update), patch (partial update), delete.
Used for family restructuring — deleting foreign parents and assigning children.

Rate limits: 5 req/s burst, 10 req/s restore.

Reference: https://developer-docs.amazon.com/sp-api/docs/listings-items-api-v2021-08-01-reference
"""
from __future__ import annotations

from typing import Optional

import structlog

from app.connectors.amazon_sp_api.client import SPAPIClient

log = structlog.get_logger(__name__)

LISTINGS_API_VERSION = "2021-08-01"
LISTINGS_BASE = f"/listings/{LISTINGS_API_VERSION}"


class ListingsClient(SPAPIClient):
    """Manage Amazon listings via SP-API Listings Items API."""

    async def get_listings_item(
        self,
        seller_id: str,
        sku: str,
        *,
        included_data: str = "summaries,attributes,issues",
    ) -> dict:
        """
        Get a listing item by seller ID and SKU.

        Returns dict with summaries, attributes, issues, etc.
        """
        path = f"{LISTINGS_BASE}/items/{seller_id}/{_url_encode(sku)}"
        params = {
            "marketplaceIds": self.marketplace_id,
            "includedData": included_data,
        }
        return await self.get(
            path, params,
            endpoint_name="listings.getListingsItem",
        )

    async def put_listings_item(
        self,
        seller_id: str,
        sku: str,
        body: dict,
    ) -> dict:
        """
        Create or fully replace a listing item (putListingsItem).

        body should contain:
          - productType: str
          - requirements: "LISTING" or "LISTING_OFFER_ONLY"
          - attributes: dict of attribute values
        """
        path = f"{LISTINGS_BASE}/items/{seller_id}/{_url_encode(sku)}"
        params_in_url = f"?marketplaceIds={self.marketplace_id}"
        return await self.put(
            f"{path}{params_in_url}", body,
            endpoint_name="listings.putListingsItem",
        )

    async def patch_listings_item(
        self,
        seller_id: str,
        sku: str,
        patches: list[dict],
        product_type: str = "PRODUCT",
    ) -> dict:
        """
        Partially update a listing item (patchListingsItem).

        patches: list of JSON Patch operations, e.g.:
          [{"op": "replace", "path": "/attributes/child_parent_sku_relationship", "value": [...]}]
        """
        path = f"{LISTINGS_BASE}/items/{seller_id}/{_url_encode(sku)}"
        params_in_url = f"?marketplaceIds={self.marketplace_id}"
        body = {
            "productType": product_type,
            "patches": patches,
        }
        return await self.patch(
            f"{path}{params_in_url}", body,
            endpoint_name="listings.patchListingsItem",
        )

    async def delete_listings_item(
        self,
        seller_id: str,
        sku: str,
    ) -> dict:
        """
        Delete a listing item (deleteListingsItem).

        Used to remove foreign parent ASINs from target marketplaces.
        """
        path = f"{LISTINGS_BASE}/items/{seller_id}/{_url_encode(sku)}"
        params = {"marketplaceIds": self.marketplace_id}
        return await self.delete(
            path, params,
            endpoint_name="listings.deleteListingsItem",
        )


    async def get_product_type_definition(
        self,
        product_type: str,
        *,
        requirements: str = "LISTING",
        locale: str = "DEFAULT",
    ) -> dict:
        """
        Get product type definition (allowed attributes, variation themes).

        Uses /definitions/2020-09-01/productTypes/{productType}.
        """
        path = f"/definitions/2020-09-01/productTypes/{product_type}"
        params = {
            "marketplaceIds": self.marketplace_id,
            "requirements": requirements,
            "locale": locale,
        }
        return await self.get(
            path, params,
            endpoint_name="definitions.getProductTypeDefinition",
        )


def _url_encode(sku: str) -> str:
    """URL-encode SKU for path segment (spaces, slashes, etc.)."""
    import urllib.parse
    return urllib.parse.quote(sku, safe="")
