"""
Smoke tests — master_key builder (pure logic, no DB / no network).

Validates the 4-level priority system, colour/size normalisation,
and edge cases.
"""
from app.services.family_mapper.master_key import build_master_key


# ── Priority 1: SKU ──────────────────────────────────────────────────────────

class TestMasterKeyPrioritySKU:
    def test_sku_wins_over_ean(self):
        mk, kt, _ = build_master_key(sku="KDX-TEST-01", ean="4066991234567")
        assert kt == "sku"
        assert mk == "KDX-TEST-01"

    def test_sku_trimmed(self):
        mk, kt, _ = build_master_key(sku="  ABC-123  ")
        assert mk == "ABC-123"
        assert kt == "sku"

    def test_short_sku_rejected(self):
        """SKU < 3 chars should fall through to EAN."""
        mk, kt, _ = build_master_key(sku="AB", ean="4066991234567")
        assert kt == "gtin"

    def test_empty_sku_falls_through(self):
        mk, kt, _ = build_master_key(sku="", ean="4066991234567")
        assert kt == "gtin"


# ── Priority 2: EAN / GTIN ──────────────────────────────────────────────────

class TestMasterKeyPriorityGTIN:
    def test_valid_ean13(self):
        mk, kt, _ = build_master_key(ean="4066991234567")
        assert kt == "gtin"
        assert mk == "4066991234567"

    def test_valid_ean8(self):
        mk, kt, _ = build_master_key(ean="12345678")
        assert kt == "gtin"

    def test_invalid_ean_letters(self):
        """Non-numeric EAN falls through."""
        mk, kt, _ = build_master_key(ean="ABC123")
        assert kt != "gtin"

    def test_ean_too_short(self):
        mk, kt, _ = build_master_key(ean="1234567")
        assert kt != "gtin"


# ── Priority 3: brand + MPN ─────────────────────────────────────────────────

class TestMasterKeyPriorityBrandMPN:
    def test_brand_mpn_combined(self):
        mk, kt, _ = build_master_key(brand="KADAX", mpn="KDX-5500")
        assert kt == "brand_mpn"
        assert "KADAX" in mk
        assert "KDX-5500" in mk

    def test_brand_only_not_enough(self):
        mk, kt, _ = build_master_key(brand="KADAX", color="Black")
        assert kt != "brand_mpn"

    def test_mpn_only_not_enough(self):
        mk, kt, _ = build_master_key(mpn="KDX-5500", color="Black")
        assert kt != "brand_mpn"


# ── Priority 4: Attribute signature ─────────────────────────────────────────

class TestMasterKeyPriorityAttr:
    def test_color_only(self):
        mk, kt, _ = build_master_key(color="Schwarz")
        assert kt == "attr_sig"
        assert "BLACK" in mk  # normalised

    def test_size_only(self):
        mk, kt, _ = build_master_key(size="extra large")
        assert kt == "attr_sig"
        assert "XL" in mk

    def test_all_attrs(self):
        mk, kt, _ = build_master_key(
            model="ProChef 3000", size="M", color="Rot", material="Steel"
        )
        assert kt == "attr_sig"
        assert "RED" in mk   # normalised from "Rot"

    def test_no_attrs_returns_unknown(self):
        mk, kt, _ = build_master_key()
        assert mk == "UNKNOWN"


# ── Colour normalisation across languages ───────────────────────────────────

class TestColorNormalisation:
    def test_german_schwarz(self):
        mk, _, _ = build_master_key(color="schwarz")
        assert "BLACK" in mk

    def test_french_blanc(self):
        mk, _, _ = build_master_key(color="blanc")
        assert "WHITE" in mk

    def test_italian_rosso(self):
        mk, _, _ = build_master_key(color="rosso")
        assert "RED" in mk

    def test_polish_niebieski(self):
        mk, _, _ = build_master_key(color="niebieski")
        assert "BLUE" in mk

    def test_spanish_verde(self):
        mk, _, _ = build_master_key(color="verde")
        assert "GREEN" in mk

    def test_swedish_svart(self):
        mk, _, _ = build_master_key(color="svart")
        assert "BLACK" in mk

    def test_dutch_wit(self):
        mk, _, _ = build_master_key(color="wit")
        assert "WHITE" in mk


# ── Size normalisation ──────────────────────────────────────────────────────

class TestSizeNormalisation:
    def test_small_to_s(self):
        mk, _, _ = build_master_key(size="small")
        assert "S" in mk

    def test_german_klein(self):
        mk, _, _ = build_master_key(size="klein")
        assert "S" in mk

    def test_dimension_format(self):
        mk, _, _ = build_master_key(size="30 x 40 cm")
        assert "30" in mk and "40" in mk

    def test_xxl_alias(self):
        mk, _, _ = build_master_key(size="2xl")
        assert "XXL" in mk


# ── JSON attributes output ─────────────────────────────────────────────────

class TestAttributesJSON:
    def test_json_contains_only_present_attrs(self):
        import json
        _, _, attrs_json = build_master_key(color="Red", size="M")
        attrs = json.loads(attrs_json)
        assert "color" in attrs
        assert "size" in attrs
        assert "material" not in attrs  # wasn't provided

    def test_json_empty_when_no_attrs(self):
        import json
        _, _, attrs_json = build_master_key(sku="TEST-SKU")
        attrs = json.loads(attrs_json)
        assert attrs == {}
