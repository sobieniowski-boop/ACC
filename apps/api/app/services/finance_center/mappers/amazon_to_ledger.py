from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from app.core.fee_taxonomy import FEE_REGISTRY, classify_fee


@dataclass(slots=True)
class LedgerMappingRule:
    account_code: str
    tax_code: str | None = None
    sign_multiplier: float = 1.0


# ── DEFAULT_RULES — auto-generated from fee_taxonomy.FEE_REGISTRY ──────────
# Kept as a dict[str, LedgerMappingRule] for backward-compatibility with
# callers that import DEFAULT_RULES directly.

DEFAULT_RULES: dict[str, LedgerMappingRule] = {
    key: LedgerMappingRule(
        entry.gl_account,
        entry.gl_tax_code,
        float(entry.sign) if entry.sign != 0 else 1.0,
    )
    for key, entry in FEE_REGISTRY.items()
}


def resolve_mapping_rule(charge_type: str | None, configured_rules: dict[str, LedgerMappingRule] | None = None) -> LedgerMappingRule:
    charge_key = str(charge_type or "").strip()
    if configured_rules and charge_key in configured_rules:
        return configured_rules[charge_key]
    if charge_key in DEFAULT_RULES:
        return DEFAULT_RULES[charge_key]
    # Taxonomy-backed fallback — covers fuzzy matches and unknown alerting
    entry = classify_fee(charge_key)
    return LedgerMappingRule(
        entry.gl_account,
        entry.gl_tax_code,
        float(entry.sign) if entry.sign != 0 else 1.0,
    )


def build_entry_hash(*parts: Any) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return sha256(raw.encode("utf-8")).hexdigest()
