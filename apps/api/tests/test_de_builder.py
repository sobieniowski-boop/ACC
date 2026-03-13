"""
Smoke tests — DE Canonical Builder (mock SP-API + mock DB).

Validates:
  - Helper extraction functions (pure, no network/DB)
  - rebuild_de_canonical end-to-end with mocked CatalogClient + pyodbc
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import FakeConnection
from app.services.family_mapper.de_builder import (
    _extract_attributes,
    _extract_brand,
    _extract_category,
    _extract_child_asins,
    _extract_identifiers,
    _extract_product_type,
    _extract_variation_theme,
    rebuild_de_canonical,
)


DE_MP = "A1PA6795UKMFR9"


# ---------------------------------------------------------------------------
# helpers — pure extraction tests
# ---------------------------------------------------------------------------

SAMPLE_ITEM = {
    "asin": "B0PARENT01",
    "summaries": [
        {
            "marketplaceId": DE_MP,
            "brandName": "KADAX",
            "itemName": "Test Product",
            "classifications": [{"displayName": "Home & Garden"}],
            "productType": "KITCHENWARE",
            "variationTheme": None,
        }
    ],
    "relationships": [
        {
            "marketplaceId": DE_MP,
            "relationships": [
                {
                    "childAsins": ["B0CHILD01", "B0CHILD02"],
                    "variationTheme": {"attributes": ["Color", "Size"]},
                }
            ],
        }
    ],
    "identifiers": [
        {
            "marketplaceId": DE_MP,
            "identifiers": [
                {"identifierType": "EAN", "identifier": "4066991234567"},
                {"identifierType": "SKU", "identifier": "KDX-TEST-01"},
            ],
        }
    ],
    "attributes": {
        "color": [{"value": "Schwarz"}],
        "size": [{"value": "XL"}],
        "material": [{"value": "Edelstahl"}],
        "model_number": [{"value": "KDX-123"}],
    },
}


class TestExtractBrand:
    def test_found(self):
        assert _extract_brand(SAMPLE_ITEM, DE_MP) == "KADAX"

    def test_wrong_marketplace(self):
        assert _extract_brand(SAMPLE_ITEM, "A1RKKUPIHCS9HS") is None

    def test_missing(self):
        assert _extract_brand({"summaries": []}, DE_MP) is None


class TestExtractCategory:
    def test_found(self):
        assert _extract_category(SAMPLE_ITEM, DE_MP) == "Home & Garden"

    def test_empty_classifications(self):
        item = {
            "summaries": [{"marketplaceId": DE_MP, "classifications": []}],
        }
        assert _extract_category(item, DE_MP) is None


class TestExtractProductType:
    def test_found(self):
        assert _extract_product_type(SAMPLE_ITEM, DE_MP) == "KITCHENWARE"


class TestExtractVariationTheme:
    def test_from_relationships(self):
        assert _extract_variation_theme(SAMPLE_ITEM, DE_MP) == "Color/Size"

    def test_string_theme(self):
        item = {
            "relationships": [
                {
                    "marketplaceId": DE_MP,
                    "relationships": [{"variationTheme": "Color"}],
                }
            ],
            "summaries": [],
        }
        assert _extract_variation_theme(item, DE_MP) == "Color"

    def test_fallback_to_summary(self):
        item = {
            "relationships": [],
            "summaries": [
                {"marketplaceId": DE_MP, "variationTheme": "Size"},
            ],
        }
        assert _extract_variation_theme(item, DE_MP) == "Size"

    def test_none(self):
        assert _extract_variation_theme({"relationships": [], "summaries": []}, DE_MP) is None


class TestExtractChildAsins:
    def test_list(self):
        asins = _extract_child_asins(SAMPLE_ITEM, DE_MP)
        assert "B0CHILD01" in asins
        assert "B0CHILD02" in asins

    def test_empty(self):
        assert _extract_child_asins({"relationships": []}, DE_MP) == []

    def test_deduplicates(self):
        item = {
            "relationships": [
                {
                    "marketplaceId": DE_MP,
                    "relationships": [
                        {"childAsins": ["B0DUP", "B0DUP"]},
                    ],
                }
            ],
        }
        assert _extract_child_asins(item, DE_MP) == ["B0DUP"]


class TestExtractIdentifiers:
    def test_ean_and_sku(self):
        ids = _extract_identifiers(SAMPLE_ITEM, DE_MP)
        assert ids["ean"] == "4066991234567"
        assert ids["sku"] == "KDX-TEST-01"

    def test_upc_fallback(self):
        item = {
            "identifiers": [
                {
                    "marketplaceId": DE_MP,
                    "identifiers": [
                        {"identifierType": "UPC", "identifier": "0123456789012"},
                    ],
                }
            ],
        }
        ids = _extract_identifiers(item, DE_MP)
        assert ids["ean"] == "0123456789012"

    def test_empty(self):
        assert _extract_identifiers({"identifiers": []}, DE_MP) == {}


class TestExtractAttributes:
    def test_all_fields(self):
        attrs = _extract_attributes(SAMPLE_ITEM, DE_MP)
        assert attrs["color"] == "Schwarz"
        assert attrs["size"] == "XL"
        assert attrs["material"] == "Edelstahl"
        assert attrs["model"] == "KDX-123"

    def test_empty(self):
        assert _extract_attributes({"attributes": {}}, DE_MP) == {}

    def test_colour_variant(self):
        item = {"attributes": {"colour_name": [{"value": "Red"}]}}
        attrs = _extract_attributes(item, DE_MP)
        assert attrs["color"] == "Red"


# ---------------------------------------------------------------------------
# rebuild_de_canonical smoke (mocked everything)
# ---------------------------------------------------------------------------

class TestRebuildDeCanonical:
    @pytest.mark.asyncio
    async def test_smoke_with_parent_asins(self, mock_db, mock_catalog):
        """Full pipeline with canned parent + children from conftest."""
        fake_conn = FakeConnection()
        with patch("app.services.family_mapper.de_builder._connect", return_value=fake_conn):
            result = await rebuild_de_canonical(
                parent_asins=["B0TEST01DE"], max_parents=5,
            )
        assert result["families"] >= 1
        assert result["children"] >= 1
        assert result["errors"] == 0
        # Verify CatalogClient.get_item was called
        mock_catalog["get_item"].assert_called_once()

    @pytest.mark.asyncio
    async def test_smoke_no_parents(self, mock_db, mock_catalog):
        """When acc_product returns no parent ASINs."""
        fake_conn = FakeConnection()
        with patch("app.services.family_mapper.de_builder._connect", return_value=fake_conn):
            result = await rebuild_de_canonical(max_parents=5)
        # FakeCursor returns [] for SELECT → no parent ASINs → skip
        assert result["families"] == 0
        assert result["children"] == 0
