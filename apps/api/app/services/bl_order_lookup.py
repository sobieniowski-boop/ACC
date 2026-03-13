from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_BATCH_SIZE = 300


@dataclass
class ResolvedBlOrder:
    bl_order_id: int
    amazon_order_id: str
    acc_order_id: str | None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_int(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _chunks(values: list[int], size: int) -> list[list[int]]:
    return [values[idx : idx + size] for idx in range(0, len(values), size)]


def _load_cached_external_orders(cur, *, bl_order_ids: list[int]) -> dict[int, str]:
    result: dict[int, str] = {}
    if not bl_order_ids:
        return result
    for batch in _chunks(bl_order_ids, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
SELECT order_id, external_order_id
FROM dbo.acc_cache_bl_orders WITH (NOLOCK)
WHERE order_id IN ({placeholders})
  AND external_order_id IS NOT NULL
  AND LTRIM(RTRIM(external_order_id)) <> ''
            """,
            batch,
        )
        for row in cur.fetchall():
            bl_order_id = _normalize_int(row[0])
            external_order_id = _normalize_text(row[1])
            if bl_order_id is None or not external_order_id:
                continue
            result[bl_order_id] = external_order_id
    return result


def _load_distribution_external_orders(cur, *, bl_order_ids: list[int]) -> dict[int, str]:
    result: dict[int, str] = {}
    if not bl_order_ids:
        return result
    for batch in _chunks(bl_order_ids, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
SELECT order_id, external_order_id
FROM dbo.acc_bl_distribution_order_cache WITH (NOLOCK)
WHERE order_id IN ({placeholders})
  AND external_order_id IS NOT NULL
  AND LTRIM(RTRIM(external_order_id)) <> ''
            """,
            batch,
        )
        for row in cur.fetchall():
            bl_order_id = _normalize_int(row[0])
            external_order_id = _normalize_text(row[1])
            if bl_order_id is None or not external_order_id:
                continue
            result[bl_order_id] = external_order_id
    return result


def _load_distribution_to_holding_map(cur, *, bl_order_ids: list[int]) -> dict[int, int]:
    result: dict[int, int] = {}
    if not bl_order_ids:
        return result
    for batch in _chunks(bl_order_ids, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
SELECT dis_order_id, holding_order_id
FROM dbo.acc_cache_dis_map WITH (NOLOCK)
WHERE dis_order_id IN ({placeholders})
  AND holding_order_id IS NOT NULL
            """,
            batch,
        )
        for row in cur.fetchall():
            dis_order_id = _normalize_int(row[0])
            holding_order_id = _normalize_int(row[1])
            if dis_order_id is None or holding_order_id is None:
                continue
            result[dis_order_id] = holding_order_id
    return result


def _load_acc_orders(cur, *, external_order_ids: list[str]) -> dict[str, tuple[str, str | None]]:
    result: dict[str, tuple[str, str | None]] = {}
    if not external_order_ids:
        return result
    for idx in range(0, len(external_order_ids), _BATCH_SIZE):
        batch = external_order_ids[idx : idx + _BATCH_SIZE]
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
SELECT amazon_order_id, CAST(id AS NVARCHAR(40)) AS acc_order_id
FROM dbo.acc_order WITH (NOLOCK)
WHERE fulfillment_channel = 'MFN'
  AND amazon_order_id IN ({placeholders})
            """,
            batch,
        )
        for row in cur.fetchall():
            amazon_order_id = _normalize_text(row[0])
            if not amazon_order_id:
                continue
            result[amazon_order_id] = (
                amazon_order_id,
                _normalize_text(row[1]) or None,
            )
    return result


def resolve_bl_orders_to_acc_orders(
    cur,
    *,
    bl_order_ids: list[int],
    allow_netfox_fallback: bool = False,
) -> dict[int, ResolvedBlOrder]:
    normalized_ids = sorted({_normalize_int(value) for value in bl_order_ids if _normalize_int(value) is not None})
    if not normalized_ids:
        return {}

    distribution_to_holding = _load_distribution_to_holding_map(cur, bl_order_ids=normalized_ids)
    canonical_by_input = {
        input_id: distribution_to_holding.get(input_id, input_id)
        for input_id in normalized_ids
    }
    canonical_ids = sorted(set(canonical_by_input.values()))

    external_by_bl: dict[int, str] = {}
    external_by_bl.update(_load_cached_external_orders(cur, bl_order_ids=normalized_ids))
    missing_original_ids = [order_id for order_id in normalized_ids if order_id not in external_by_bl]
    if missing_original_ids:
        external_by_bl.update(_load_distribution_external_orders(cur, bl_order_ids=missing_original_ids))

    external_by_canonical = _load_cached_external_orders(cur, bl_order_ids=canonical_ids)
    if allow_netfox_fallback:
        # Netfox access is intentionally disabled in the hot path to protect production load.
        log.info(
            "bl_order_lookup.netfox_fallback_ignored",
            reason="disabled_to_protect_netfox_hot_path",
            requested_missing=len([order_id for order_id in canonical_ids if order_id not in external_by_canonical]),
        )

    orders_by_external = _load_acc_orders(
        cur,
        external_order_ids=sorted(
            {
                value
                for value in [*external_by_bl.values(), *external_by_canonical.values()]
                if value
            }
        ),
    )

    resolved: dict[int, ResolvedBlOrder] = {}
    for input_order_id in normalized_ids:
        canonical_order_id = canonical_by_input.get(input_order_id, input_order_id)
        external_order_id = external_by_bl.get(input_order_id) or external_by_canonical.get(canonical_order_id)
        if not external_order_id:
            continue
        order = orders_by_external.get(external_order_id)
        if not order:
            continue
        resolved[input_order_id] = ResolvedBlOrder(
            bl_order_id=canonical_order_id,
            amazon_order_id=order[0],
            acc_order_id=order[1],
        )
    return resolved


def reset_bl_order_lookup_state_for_tests() -> None:
    return None
