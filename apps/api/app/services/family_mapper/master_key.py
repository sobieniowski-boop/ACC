"""
Master key builder — deterministic child identity across marketplaces.

Priority:
  1) merchant_sku / seller_sku (if stable)
  2) EAN / GTIN (if present)
  3) brand + mpn (if present)
  4) attribute_signature: model|size|color|material (normalised)

Returns (master_key, key_type, attributes_json).
"""
from __future__ import annotations

import json
import re
from typing import Optional

# ---------------------------------------------------------------------------
# Normalisation maps
# ---------------------------------------------------------------------------

_COLOR_MAP: dict[str, str] = {
    "schwarz": "BLACK", "black": "BLACK", "noir": "BLACK", "nero": "BLACK",
    "negro": "BLACK", "czarny": "BLACK", "zwart": "BLACK", "svart": "BLACK",
    "weiss": "WHITE", "weiß": "WHITE", "white": "WHITE", "blanc": "WHITE",
    "bianco": "WHITE", "blanco": "WHITE", "bialy": "WHITE", "wit": "WHITE",
    "vit": "WHITE",
    "rot": "RED", "red": "RED", "rouge": "RED", "rosso": "RED", "rojo": "RED",
    "grau": "GREY", "gray": "GREY", "grey": "GREY", "gris": "GREY",
    "grigio": "GREY", "szary": "GREY",
    "blau": "BLUE", "blue": "BLUE", "bleu": "BLUE", "blu": "BLUE",
    "azul": "BLUE", "niebieski": "BLUE",
    "gruen": "GREEN", "grün": "GREEN", "green": "GREEN", "vert": "GREEN",
    "verde": "GREEN", "zielony": "GREEN",
    "gelb": "YELLOW", "yellow": "YELLOW", "jaune": "YELLOW", "giallo": "YELLOW",
    "amarillo": "YELLOW", "zolty": "YELLOW",
    "braun": "BROWN", "brown": "BROWN", "brun": "BROWN", "marrone": "BROWN",
    "marron": "BROWN", "brazowy": "BROWN",
    "rosa": "PINK", "pink": "PINK", "rose": "PINK",
    "silber": "SILVER", "silver": "SILVER", "argent": "SILVER",
    "gold": "GOLD", "or": "GOLD", "oro": "GOLD",
    "beige": "BEIGE",
}

_SIZE_ALIASES: dict[str, str] = {
    "extra small": "XS", "xs": "XS",
    "small": "S", "s": "S", "klein": "S", "petit": "S",
    "medium": "M", "m": "M", "mittel": "M", "moyen": "M",
    "large": "L", "l": "L", "gross": "L", "groß": "L", "grand": "L",
    "extra large": "XL", "xl": "XL",
    "xxl": "XXL", "2xl": "XXL",
    "3xl": "3XL", "xxxl": "3XL",
}


def _norm(val: str | None) -> str:
    """Trim + uppercase + collapse whitespace."""
    if not val:
        return ""
    return re.sub(r"\s+", " ", val.strip()).upper()


def _norm_color(val: str | None) -> str:
    if not val:
        return ""
    key = val.strip().lower()
    # Try multi-word first, then single-word
    for token in [key] + key.split():
        if token in _COLOR_MAP:
            return _COLOR_MAP[token]
    return _norm(val)


def _norm_size(val: str | None) -> str:
    if not val:
        return ""
    key = val.strip().lower()
    if key in _SIZE_ALIASES:
        return _SIZE_ALIASES[key]
    # Normalise "30 x 40 cm" → "30X40CM"
    out = re.sub(r"\s*(x|×)\s*", "X", _norm(val))
    out = re.sub(r"\s*(cm|mm|m|in|inch|zoll)\b", lambda m: m.group(1).upper(), out, flags=re.IGNORECASE)
    return out


def _build_attr_signature(attrs: dict) -> str:
    """Build deterministic signature from normalised attributes."""
    parts = []
    for key in ("model", "size", "color", "material"):
        val = attrs.get(key) or ""
        if key == "color":
            val = _norm_color(val)
        elif key == "size":
            val = _norm_size(val)
        else:
            val = _norm(val)
        parts.append(val)
    return "|".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_master_key(
    *,
    sku: str | None = None,
    ean: str | None = None,
    brand: str | None = None,
    mpn: str | None = None,
    model: str | None = None,
    size: str | None = None,
    color: str | None = None,
    material: str | None = None,
) -> tuple[str, str, str]:
    """
    Build a master key for child identification.

    Returns:
        (master_key, key_type, attributes_json)
    """
    attrs = {
        "model": model,
        "size": size,
        "color": color,
        "material": material,
    }
    attrs_json = json.dumps(
        {k: v for k, v in attrs.items() if v}, ensure_ascii=False
    )

    # 1) SKU (most stable if seller controls it)
    if sku and len(sku.strip()) >= 3:
        return (sku.strip(), "sku", attrs_json)

    # 2) EAN / GTIN
    if ean and re.match(r"^\d{8,14}$", ean.strip()):
        return (ean.strip(), "gtin", attrs_json)

    # 3) brand + MPN
    if brand and mpn:
        key = f"{_norm(brand)}::{_norm(mpn)}"
        return (key[:120], "brand_mpn", attrs_json)

    # 4) Attribute signature
    sig = _build_attr_signature(attrs)
    if sig.replace("|", ""):
        return (sig[:120], "attr_sig", attrs_json)

    # Fallback — should not happen with valid data
    return ("UNKNOWN", "attr_sig", attrs_json)
