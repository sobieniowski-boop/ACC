"""Ergonode PIM API connector — fetches product master data for SKU mapping.

Auth: POST /api/v1/login → JWT token (15-min expiry).
Products grid: GET /api/v1/en_GB/products?columns=...&limit=1000&offset=N

Reference: https://docs.ergonode.com/
"""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx
import structlog

from app.core.config import settings

log = structlog.get_logger(__name__)


class ErgonodeClient:
    """Async Ergonode PIM API client with token refresh."""

    def __init__(self):
        self.base_url = settings.ERGONODE_API_URL.rstrip("/")
        self.username = settings.ERGONODE_USERNAME
        self.password = settings.ERGONODE_PASSWORD
        self.api_key = settings.ERGONODE_API_KEY
        self._token: Optional[str] = None

    async def _authenticate(self) -> str:
        """Get JWT token from Ergonode login endpoint."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/login",
                json={"username": self.username, "password": self.password},
            )
            resp.raise_for_status()
            self._token = resp.json()["token"]
            log.debug("ergonode.auth_ok")
            return self._token

    async def _get_headers(self) -> dict:
        """Get auth headers, refreshing token if needed."""
        if not self._token:
            await self._authenticate()
        return {
            "Authorization": f"Bearer {self._token}",
            "JWTAuthorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make an API request with automatic token refresh on 401."""
        headers = await self._get_headers()
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                method, f"{self.base_url}{path}", headers=headers, **kwargs,
            )
            if resp.status_code == 401:
                log.debug("ergonode.token_expired, refreshing")
                await self._authenticate()
                headers = await self._get_headers()
                resp = await client.request(
                    method, f"{self.base_url}{path}", headers=headers, **kwargs,
                )
            resp.raise_for_status()
            return resp.json()

    async def fetch_all_products(
        self,
        columns: str = "sku,ean,asin_child,asin_parent,parent_sku,kod_producenta",
        page_size: int = 1000,
    ) -> list[dict]:
        """
        Download all products from Ergonode PIM grid API (paginated).

        Returns list of dicts with keys matching requested columns + 'id' (UUID).
        """
        all_products: list[dict] = []
        offset = 0

        while True:
            data = await self._request(
                "GET",
                f"/api/v1/en_GB/products",
                params={
                    "columns": columns,
                    "limit": page_size,
                    "offset": offset,
                },
            )

            rows = data.get("collection", [])
            if not rows:
                break

            for raw_row in rows:
                row = raw_row.get("data", raw_row) if isinstance(raw_row, dict) else raw_row
                product = {"ergonode_id": row.get("id", "")}
                for col in columns.split(","):
                    col = col.strip()
                    val = row.get(col)
                    if isinstance(val, dict):
                        val = val.get("value", val.get("en_GB", ""))
                    product[col] = str(val).strip() if val else ""
                all_products.append(product)

            info = data.get("info", {})
            total = int(info.get("filtered", info.get("count", 0)))
            offset += page_size
            if offset >= total:
                break

            await asyncio.sleep(0.1)

        log.info("ergonode.fetched_all", count=len(all_products))
        return all_products


async def fetch_ergonode_ean_lookup() -> dict[str, dict]:
    """
    Build EAN→product lookup from Ergonode PIM.

    Returns: {ean: {internal_sku, k_number, ergonode_id, asin_child, ...}}
    """
    client = ErgonodeClient()
    products = await client.fetch_all_products()

    lookup: dict[str, dict] = {}
    for p in products:
        ean = str(p.get("ean", "")).strip()
        if ean and ean.isdigit() and len(ean) >= 8:
            lookup[ean] = {
                "internal_sku": str(p.get("sku", "")),
                "k_number": str(p.get("kod_producenta", "")),
                "ergonode_id": str(p.get("id", p.get("ergonode_id", ""))),
                "asin_child": p.get("asin_child", ""),
                "asin_parent": p.get("asin_parent", ""),
                "parent_sku": p.get("parent_sku", ""),
            }

    log.info("ergonode.ean_lookup_built", entries=len(lookup))
    return lookup


async def fetch_ergonode_title_lookup(target_skus: set[str] | None = None) -> dict[str, dict]:
    """
    Build SKU→title lookup from Ergonode PIM (per-product endpoint).

    Fetches product IDs from grid, then individual product details
    for title attributes: tytul (PL), amazon_title (multi-lang), tytul_bl (PL).

    Args:
        target_skus: If provided, only fetch titles for these SKUs (internal_sku).
                     Dramatically reduces API calls when only subset needed.

    Returns: {sku: {title_pl, title_de, title_en, title_source}}
    """
    client = ErgonodeClient()

    # Step 1: Get all product IDs + SKUs from grid
    log.info("ergonode.title_lookup.start", target_count=len(target_skus) if target_skus else "all")
    all_ids: list[tuple[str, str]] = []  # (product_id, sku)
    offset = 0
    page_size = 1000

    while True:
        data = await client._request(
            "GET",
            "/api/v1/en_GB/products",
            params={"columns": "sku", "limit": page_size, "offset": offset},
        )
        rows = data.get("collection", [])
        if not rows:
            break

        for raw_row in rows:
            row = raw_row.get("data", raw_row) if isinstance(raw_row, dict) else raw_row
            pid = row.get("id", "")
            sku_val = row.get("sku")
            if isinstance(sku_val, dict):
                sku_val = sku_val.get("value", sku_val.get("en_GB", ""))
            sku_str = str(sku_val).strip() if sku_val else ""
            if pid and sku_str:
                # Filter by target SKUs if provided
                if target_skus is None or sku_str in target_skus:
                    all_ids.append((pid, sku_str))

        info = data.get("info", {})
        total = int(info.get("filtered", info.get("count", 0)))
        offset += page_size
        if offset >= total:
            break
        await asyncio.sleep(0.05)

    log.info("ergonode.title_lookup.ids_fetched", count=len(all_ids))

    # Step 2: Fetch individual product details (batch with semaphore)
    lookup: dict[str, dict] = {}
    sem = asyncio.Semaphore(10)  # max 10 concurrent requests
    errors = 0

    async def _fetch_one(product_id: str, sku: str) -> None:
        nonlocal errors
        async with sem:
            try:
                detail = await client._request(
                    "GET", f"/api/v1/en_GB/products/{product_id}",
                )
                attrs = detail.get("attributes", {})

                # Extract titles with language fallback
                tytul = attrs.get("tytul", {})
                amazon_title = attrs.get("amazon_title", {})
                tytul_bl = attrs.get("tytul_bl", {})

                title_pl = (
                    _extract_lang(tytul, "pl_PL")
                    or _extract_lang(tytul_bl, "pl_PL")
                    or _extract_lang(amazon_title, "pl_PL")
                )
                title_de = (
                    _extract_lang(amazon_title, "de_DE")
                    or _extract_lang(attrs.get("kaufland_title", {}), "de_DE")
                )
                title_en = _extract_lang(amazon_title, "en_GB")

                # Pick best title: PL > DE > EN
                best_title = title_pl or title_de or title_en

                if best_title:
                    lookup[sku] = {
                        "title_pl": title_pl,
                        "title_de": title_de,
                        "title_en": title_en,
                        "title_best": best_title,
                        "ergonode_id": product_id,
                    }
            except Exception:
                errors += 1
            await asyncio.sleep(0.02)  # rate limiting

    # Process in batches of 50
    batch_size = 50
    for i in range(0, len(all_ids), batch_size):
        batch = all_ids[i : i + batch_size]
        await asyncio.gather(*[_fetch_one(pid, sku) for pid, sku in batch])
        if (i + batch_size) % 500 == 0:
            log.debug(
                "ergonode.title_lookup.progress",
                done=min(i + batch_size, len(all_ids)),
                total=len(all_ids),
                found=len(lookup),
            )

    log.info(
        "ergonode.title_lookup.done",
        total=len(all_ids),
        found=len(lookup),
        errors=errors,
    )
    return lookup


def _extract_lang(val: dict | str | None, lang: str) -> str | None:
    """Extract a localized string from Ergonode attribute value."""
    if not val:
        return None
    if isinstance(val, str):
        return val.strip() or None
    if isinstance(val, dict):
        text = val.get(lang) or val.get("value")
        if isinstance(text, str):
            return text.strip() or None
    return None


# ---------------------------------------------------------------------------
# Variant attribute lookup (color / size) with option resolution
# ---------------------------------------------------------------------------

# Ergonode attribute codes for Amazon variant fields
_VARIANT_COLOR_ATTR = "wariant_kolor_tekst"   # MULTI_SELECT → UUIDs
_VARIANT_SIZE_ATTR = "wariant_text_rozmiar"   # SELECT → UUID
_VARIANT_QTY_ATTR = "wariant_text_ilosc"      # SELECT → UUID


async def _build_option_map(client: ErgonodeClient, attr_code: str) -> dict[str, str]:
    """Fetch options for an attribute and return {uuid: code_text} map.

    Ergonode SELECT/MULTI_SELECT attributes store UUIDs.
    The options endpoint returns [{id, code, label}, ...].
    We use 'code' as the human-readable text value.
    """
    attr_id = None
    offset = 0
    while True:
        data = await client._request(
            "GET", "/api/v1/en_GB/attributes",
            params={"limit": 200, "offset": offset},
        )
        coll = data.get("collection", [])
        if not coll:
            break
        for item in coll:
            d = item.get("data", item)
            if d.get("code") == attr_code:
                attr_id = d.get("id")
                break
        if attr_id:
            break
        info = data.get("info", {})
        total = int(info.get("filtered", info.get("count", 0)))
        offset += 200
        if offset >= total:
            break

    if not attr_id:
        log.warning("ergonode.option_map.attr_not_found", code=attr_code)
        return {}

    try:
        opts = await client._request(
            "GET", f"/api/v1/en_GB/attributes/{attr_id}/options",
        )
    except Exception as e:
        log.warning("ergonode.option_map.fetch_failed", code=attr_code, error=str(e))
        return {}

    option_map: dict[str, str] = {}
    if isinstance(opts, list):
        for o in opts:
            oid = o.get("id", "")
            code_text = o.get("code", "")
            if oid and code_text:
                option_map[oid] = code_text
    log.debug("ergonode.option_map.built", attr=attr_code, count=len(option_map))
    return option_map


async def fetch_ergonode_variant_lookup(
    target_asins: set[str],
) -> dict[str, dict]:
    """
    Build ASIN → variant attributes lookup from Ergonode PIM.

    For each child ASIN found in PIM, resolves wariant_kolor_tekst and
    wariant_text_rozmiar / wariant_text_ilosc to human-readable text.

    Returns: {asin: {color: "Złoty", size: "16 cm", quantity: "10",
                     sku_ergonode: "27219", ergonode_id: "..."}}
    """
    client = ErgonodeClient()

    # Step 1: Build option UUID→text maps (cached for all products)
    color_opts, size_opts, qty_opts = await asyncio.gather(
        _build_option_map(client, _VARIANT_COLOR_ATTR),
        _build_option_map(client, _VARIANT_SIZE_ATTR),
        _build_option_map(client, _VARIANT_QTY_ATTR),
    )

    # Step 2: Fetch product IDs with ASIN filter from grid
    product_ids: list[tuple[str, str, str]] = []  # (product_id, asin, sku)
    offset = 0
    page_size = 1000

    while True:
        data = await client._request(
            "GET", "/api/v1/en_GB/products",
            params={"columns": "sku,asin_child", "limit": page_size, "offset": offset},
        )
        rows = data.get("collection", [])
        if not rows:
            break
        for raw_row in rows:
            row = raw_row.get("data", raw_row) if isinstance(raw_row, dict) else raw_row
            pid = row.get("id", "")
            asin_val = row.get("asin_child")
            if isinstance(asin_val, dict):
                asin_val = asin_val.get("value", asin_val.get("en_GB", ""))
            asin_str = str(asin_val).strip() if asin_val else ""
            sku_val = row.get("sku")
            if isinstance(sku_val, dict):
                sku_val = sku_val.get("value", sku_val.get("en_GB", ""))
            sku_str = str(sku_val).strip() if sku_val else ""
            if pid and asin_str and asin_str in target_asins:
                product_ids.append((pid, asin_str, sku_str))

        info = data.get("info", {})
        total = int(info.get("filtered", info.get("count", 0)))
        offset += page_size
        if offset >= total:
            break
        await asyncio.sleep(0.05)

    log.info("ergonode.variant_lookup.matched", count=len(product_ids), requested=len(target_asins))

    if not product_ids:
        return {}

    # Step 3: Fetch individual product details for variant attributes
    lookup: dict[str, dict] = {}
    sem = asyncio.Semaphore(10)
    errors = 0

    def _resolve_select(attr_val, option_map: dict[str, str]) -> str | None:
        """Resolve a SELECT/MULTI_SELECT UUID value to text."""
        if not attr_val:
            return None
        if isinstance(attr_val, dict):
            uuid_str = next(iter(attr_val.values()), "")
        else:
            uuid_str = str(attr_val)
        if not uuid_str:
            return None
        # MULTI_SELECT may have comma-separated UUIDs — take first
        first_uuid = uuid_str.split(",")[0].strip()
        return option_map.get(first_uuid)

    async def _fetch_one(product_id: str, asin: str, sku: str) -> None:
        nonlocal errors
        async with sem:
            try:
                detail = await client._request(
                    "GET", f"/api/v1/en_GB/products/{product_id}",
                )
                attrs = detail.get("attributes", {})

                color = _resolve_select(attrs.get(_VARIANT_COLOR_ATTR), color_opts)
                size = _resolve_select(attrs.get(_VARIANT_SIZE_ATTR), size_opts)
                qty = _resolve_select(attrs.get(_VARIANT_QTY_ATTR), qty_opts)

                lookup[asin] = {
                    "color": color,
                    "size": size,
                    "quantity": qty,
                    "sku_ergonode": sku,
                    "ergonode_id": product_id,
                }
            except Exception:
                errors += 1
            await asyncio.sleep(0.02)

    batch_size = 50
    for i in range(0, len(product_ids), batch_size):
        batch = product_ids[i : i + batch_size]
        await asyncio.gather(*[_fetch_one(pid, asin, sku) for pid, asin, sku in batch])

    log.info("ergonode.variant_lookup.done", found=len(lookup), errors=errors)
    return lookup