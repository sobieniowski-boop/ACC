"""SP-API FBA Inventory connector."""
from __future__ import annotations

import structlog
from app.connectors.amazon_sp_api.client import SPAPIClient

log = structlog.get_logger(__name__)


class InventoryClient(SPAPIClient):

    async def get_inventory_summaries(self, granularity: str = "Marketplace") -> list[dict]:
        """Fetch all FBA inventory summaries for the marketplace."""
        all_items: list[dict] = []
        next_token = None

        for _ in range(100):
            params = {
                "granularityType": granularity,
                "granularityId": self.marketplace_id,
                "marketplaceIds": self.marketplace_id,
                "details": "true",
            }
            if next_token:
                params["nextToken"] = next_token

            data = await self.get("/fba/inventory/v1/summaries", params)
            summaries = data.get("payload", {}).get("inventorySummaries", [])
            all_items.extend(summaries)
            log.info("inventory.fetched_page", count=len(summaries), total=len(all_items))

            next_token = data.get("payload", {}).get("pagination", {}).get("nextToken")
            if not next_token:
                break

        return all_items
