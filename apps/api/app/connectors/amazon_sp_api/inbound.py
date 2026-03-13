"""SP-API Fulfillment Inbound v0 connector."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.connectors.amazon_sp_api.client import SPAPIClient


class InboundClient(SPAPIClient):
    async def get_shipments(
        self,
        *,
        statuses: Optional[list[str]] = None,
        last_updated_after: Optional[datetime] = None,
        last_updated_before: Optional[datetime] = None,
    ) -> list[dict]:
        params: dict[str, str] = {}
        if statuses:
            params["ShipmentStatusList"] = ",".join(statuses)
        if last_updated_after:
            params["LastUpdatedAfter"] = last_updated_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        if last_updated_before:
            params["LastUpdatedBefore"] = last_updated_before.strftime("%Y-%m-%dT%H:%M:%SZ")
        data = await self.get("/fba/inbound/v0/shipments", params=params)
        payload = data.get("payload", {}) if isinstance(data, dict) else {}
        return payload.get("ShipmentData", []) if isinstance(payload, dict) else []

    async def get_shipment_items(self, shipment_id: str) -> list[dict]:
        data = await self.get(f"/fba/inbound/v0/shipments/{shipment_id}/items")
        payload = data.get("payload", {}) if isinstance(data, dict) else {}
        return payload.get("ItemData", []) if isinstance(payload, dict) else []
