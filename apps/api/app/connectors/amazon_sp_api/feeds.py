from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from app.connectors.amazon_sp_api.client import SPAPIClient

FEEDS_BASE = "/feeds/2021-06-30"


class FeedsClient(SPAPIClient):
    async def create_feed_document(self, content_type: str = "application/json; charset=UTF-8") -> dict[str, Any]:
        return await self.post(f"{FEEDS_BASE}/documents", {"contentType": content_type})

    async def upload_feed_document(self, url: str, content: bytes, content_type: str = "application/json; charset=UTF-8") -> None:
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.put(url, content=content, headers={"Content-Type": content_type})
            response.raise_for_status()

    async def create_feed(
        self,
        *,
        feed_type: str,
        input_feed_document_id: str,
        marketplace_ids: list[str],
    ) -> dict[str, Any]:
        return await self.post(
            f"{FEEDS_BASE}/feeds",
            {
                "feedType": feed_type,
                "marketplaceIds": marketplace_ids,
                "inputFeedDocumentId": input_feed_document_id,
            },
        )

    async def get_feed(self, feed_id: str) -> dict[str, Any]:
        return await self.get(f"{FEEDS_BASE}/feeds/{feed_id}")

    async def submit_json_listings_feed(
        self,
        *,
        marketplace_ids: list[str],
        feed_payload: dict[str, Any],
    ) -> dict[str, Any]:
        document = await self.create_feed_document()
        document_id = str(document.get("feedDocumentId") or "")
        url = str(document.get("url") or "")
        if not document_id or not url:
            raise RuntimeError("create_feed_document did not return upload target")
        body = json.dumps(feed_payload, ensure_ascii=False).encode("utf-8")
        await self.upload_feed_document(url, body)
        feed = await self.create_feed(
            feed_type="JSON_LISTINGS_FEED",
            input_feed_document_id=document_id,
            marketplace_ids=marketplace_ids,
        )
        return {
            "feedDocumentId": document_id,
            "feedId": feed.get("feedId"),
        }

    async def wait_for_feed(self, feed_id: str, *, poll_interval: float = 15.0, max_wait: float = 180.0) -> dict[str, Any]:
        elapsed = 0.0
        while elapsed < max_wait:
            feed = await self.get_feed(feed_id)
            status = str(feed.get("processingStatus") or "")
            if status in {"DONE", "CANCELLED", "FATAL"}:
                return feed
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        latest = await self.get_feed(feed_id)
        latest["timed_out"] = True
        return latest
