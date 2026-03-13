"""Taxonomy enrichment layer for brand/category gaps.

Scope:
- stores canonical taxonomy nodes/aliases,
- predicts brand/category/product_type for products with missing metadata,
- supports review queue (approve/reject),
- provides runtime lookup for Profit/Inventory fallbacks.
"""
from __future__ import annotations

import difflib
import json
import threading
import time
import uuid
from typing import Any, Callable

import pyodbc
import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)
_SCHEMA_READY = False
_SCHEMA_LOCK = threading.Lock()


def _connect():
    return connect_acc(autocommit=False, timeout=20)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _clean_lower(value: Any) -> str:
    return _clean(value).lower()


def _is_missing(value: Any) -> bool:
    return _clean(value) == ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def ensure_taxonomy_schema() -> None:
    """No-op — schema managed by Alembic migration eb016."""


def _insert_default_nodes(cur) -> int:
    rows_added = 0
    defaults = [
        ("unknown", "Nieprzypisane", None),
    ]
    for node_key, label, parent in defaults:
        cur.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dbo.acc_taxonomy_node WITH (NOLOCK) WHERE node_key = ?)
            BEGIN
                INSERT INTO dbo.acc_taxonomy_node (id, node_key, canonical_label_pl, parent_node_key, is_active)
                VALUES (NEWID(), ?, ?, ?, 1)
            END
            """,
            (node_key, node_key, label, parent),
        )
        rows_added += _safe_int(getattr(cur, "rowcount", 0))
    return rows_added


def _load_registry_rows(cur) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            merchant_sku,
            merchant_sku_alt,
            asin,
            ean,
            internal_sku,
            brand,
            category_1,
            category_2,
            product_name
        FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
        WHERE (
            ISNULL(brand, '') <> ''
            OR ISNULL(category_1, '') <> ''
            OR ISNULL(category_2, '') <> ''
        )
        """
    )
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        out.append(
            {
                "merchant_sku": _clean(row[0]),
                "merchant_sku_alt": _clean(row[1]),
                "asin": _clean(row[2]),
                "ean": _clean(row[3]),
                "internal_sku": _clean(row[4]),
                "brand": _clean(row[5]),
                "category_1": _clean(row[6]),
                "category_2": _clean(row[7]),
                "product_name": _clean(row[8]),
            }
        )
    return out


def _load_product_candidates(cur, limit: int = 40000) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT TOP {int(max(1, min(limit, 200000)))}
            CAST(id AS NVARCHAR(40)) AS id,
            sku,
            asin,
            ean,
            internal_sku,
            title,
            brand,
            category,
            subcategory
        FROM dbo.acc_product WITH (NOLOCK)
        WHERE
            ISNULL(sku, '') <> ''
            AND (
                ISNULL(brand, '') = ''
                OR ISNULL(category, '') = ''
                OR ISNULL(subcategory, '') = ''
            )
        ORDER BY updated_at DESC
        """
    )
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        out.append(
            {
                "id": _clean(row[0]),
                "sku": _clean(row[1]),
                "asin": _clean(row[2]),
                "ean": _clean(row[3]),
                "internal_sku": _clean(row[4]),
                "title": _clean(row[5]),
                "brand": _clean(row[6]),
                "category": _clean(row[7]),
                "subcategory": _clean(row[8]),
            }
        )
    return out


def _load_product_reference(cur) -> dict[str, dict[str, dict[str, str]]]:
    cur.execute(
        """
        SELECT
            CAST(id AS NVARCHAR(40)) AS id,
            sku,
            asin,
            ean,
            internal_sku,
            title,
            brand,
            category,
            subcategory
        FROM dbo.acc_product WITH (NOLOCK)
        WHERE
            (
                ISNULL(brand, '') <> ''
                OR ISNULL(category, '') <> ''
                OR ISNULL(subcategory, '') <> ''
            )
            AND (
                ISNULL(sku, '') <> ''
                OR ISNULL(asin, '') <> ''
                OR ISNULL(ean, '') <> ''
                OR ISNULL(internal_sku, '') <> ''
            )
        """
    )
    by_sku: dict[str, dict[str, str]] = {}
    by_asin: dict[str, dict[str, str]] = {}
    by_ean: dict[str, dict[str, str]] = {}
    by_internal: dict[str, dict[str, str]] = {}
    title_pool: list[dict[str, str]] = []
    for row in cur.fetchall():
        item = {
            "id": _clean(row[0]),
            "sku": _clean(row[1]),
            "asin": _clean(row[2]),
            "ean": _clean(row[3]),
            "internal_sku": _clean(row[4]),
            "title": _clean(row[5]),
            "brand": _clean(row[6]),
            "category": _clean(row[7]),
            "subcategory": _clean(row[8]),
        }
        if item["sku"]:
            by_sku[item["sku"]] = item
        if item["asin"]:
            by_asin[item["asin"]] = item
        if item["ean"]:
            by_ean[item["ean"]] = item
        if item["internal_sku"]:
            by_internal[item["internal_sku"]] = item
        if item["title"]:
            title_pool.append(item)
    return {
        "by_sku": by_sku,
        "by_asin": by_asin,
        "by_ean": by_ean,
        "by_internal": by_internal,
        "title_pool": title_pool,
    }


def _choose_prediction(
    *,
    candidate: dict[str, Any],
    ref_maps: dict[str, dict[str, dict[str, str]]],
    registry_by_sku: dict[str, dict[str, Any]],
    registry_by_asin: dict[str, dict[str, Any]],
    registry_by_ean: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    c_id = candidate.get("id")
    c_sku = candidate.get("sku")
    c_asin = candidate.get("asin")
    c_ean = candidate.get("ean")
    c_internal = candidate.get("internal_sku")
    c_title = candidate.get("title") or ""

    # Source 1: exact PIM/product match on internal_sku/ean/asin.
    exact_ref = None
    exact_reason = None
    exact_conf = 0.0
    if c_internal and c_internal in ref_maps["by_internal"]:
        exact_ref = ref_maps["by_internal"][c_internal]
        exact_reason = "same_internal_sku"
        exact_conf = 0.98
    elif c_ean and c_ean in ref_maps["by_ean"]:
        exact_ref = ref_maps["by_ean"][c_ean]
        exact_reason = "same_ean"
        exact_conf = 0.95
    elif c_asin and c_asin in ref_maps["by_asin"]:
        exact_ref = ref_maps["by_asin"][c_asin]
        exact_reason = "same_asin"
        exact_conf = 0.93
    if exact_ref and exact_ref.get("id") != c_id:
        return {
            "sku": c_sku,
            "asin": c_asin,
            "ean": c_ean,
            "suggested_brand": exact_ref.get("brand") or None,
            "suggested_category": exact_ref.get("category") or None,
            "suggested_product_type": exact_ref.get("subcategory") or None,
            "confidence": exact_conf,
            "source": "pim_exact",
            "reason": exact_reason,
            "evidence": {"reference_id": exact_ref.get("id"), "reference_sku": exact_ref.get("sku"), "reference_asin": exact_ref.get("asin")},
        }

    # Source 2: registry/ean mapping.
    reg = None
    reg_reason = None
    reg_conf = 0.0
    if c_sku and c_sku in registry_by_sku:
        reg = registry_by_sku[c_sku]
        reg_reason = "registry_sku"
        reg_conf = 0.95
    elif c_asin and c_asin in registry_by_asin:
        reg = registry_by_asin[c_asin]
        reg_reason = "registry_asin"
        reg_conf = 0.93
    elif c_ean and c_ean in registry_by_ean:
        reg = registry_by_ean[c_ean]
        reg_reason = "registry_ean"
        reg_conf = 0.90
    if reg and (reg.get("brand") or reg.get("category_1") or reg.get("category_2")):
        return {
            "sku": c_sku,
            "asin": c_asin,
            "ean": c_ean,
            "suggested_brand": reg.get("brand") or None,
            "suggested_category": reg.get("category_1") or None,
            "suggested_product_type": reg.get("category_2") or None,
            "confidence": reg_conf,
            "source": "ean_match",
            "reason": reg_reason,
            "evidence": {"registry_sku": reg.get("merchant_sku"), "registry_asin": reg.get("asin"), "registry_ean": reg.get("ean")},
        }

    # Source 3: lightweight "embedding-like" title similarity fallback.
    if c_title:
        best = None
        best_score = 0.0
        title_norm = _clean_lower(c_title)
        for ref in ref_maps["title_pool"][:10000]:
            ref_title = _clean_lower(ref.get("title"))
            if not ref_title:
                continue
            score = difflib.SequenceMatcher(a=title_norm, b=ref_title).ratio()
            if score > best_score:
                best_score = score
                best = ref
        if best and best_score >= 0.86:
            confidence = round(min(0.89, best_score * 0.88), 4)
            return {
                "sku": c_sku,
                "asin": c_asin,
                "ean": c_ean,
                "suggested_brand": best.get("brand") or None,
                "suggested_category": best.get("category") or None,
                "suggested_product_type": best.get("subcategory") or None,
                "confidence": confidence,
                "source": "embedding_match",
                "reason": "title_similarity",
                "evidence": {"reference_id": best.get("id"), "title_similarity": round(best_score, 4), "reference_title": best.get("title")},
            }
    return None


def _upsert_prediction(cur, prediction: dict[str, Any]) -> None:
    # Two-step upsert is safer on Azure SQL than MERGE under concurrency.
    # Keep terminal statuses untouched (approved/rejected/applied/auto_applied).
    cur.execute(
        """
        UPDATE dbo.acc_taxonomy_prediction
        SET
            suggested_brand = ?,
            suggested_category = ?,
            suggested_product_type = ?,
            confidence = ?,
            reason = ?,
            evidence_json = ?,
            status = CASE
                WHEN status IN ('applied', 'auto_applied', 'approved', 'rejected') THEN status
                ELSE 'pending'
            END,
            updated_at = SYSUTCDATETIME()
        WHERE ISNULL(sku, '') = ISNULL(?, '')
          AND ISNULL(asin, '') = ISNULL(?, '')
          AND ISNULL(ean, '') = ISNULL(?, '')
          AND source = ?
        """,
        (
            prediction.get("suggested_brand"),
            prediction.get("suggested_category"),
            prediction.get("suggested_product_type"),
            prediction.get("confidence"),
            prediction.get("reason"),
            json.dumps(prediction.get("evidence") or {}, ensure_ascii=False),
            prediction.get("sku"),
            prediction.get("asin"),
            prediction.get("ean"),
            prediction.get("source"),
        ),
    )
    if _safe_int(cur.rowcount) > 0:
        return

    cur.execute(
        """
        INSERT INTO dbo.acc_taxonomy_prediction (
            id, marketplace_id, sku, asin, ean,
            suggested_brand, suggested_category, suggested_product_type,
            confidence, source, status, reason, evidence_json, created_at, updated_at
        )
        VALUES (
            NEWID(), NULL, ?, ?, ?,
            ?, ?, ?, ?, ?, 'pending', ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME()
        )
        """,
        (
            prediction.get("sku"),
            prediction.get("asin"),
            prediction.get("ean"),
            prediction.get("suggested_brand"),
            prediction.get("suggested_category"),
            prediction.get("suggested_product_type"),
            prediction.get("confidence"),
            prediction.get("source"),
            prediction.get("reason"),
            json.dumps(prediction.get("evidence") or {}, ensure_ascii=False),
        ),
    )


def _auto_apply_predictions(cur, *, min_confidence: float = 0.90, actor: str = "taxonomy_auto") -> int:
    cur.execute(
        """
        SELECT
            CAST(id AS NVARCHAR(40)) AS id,
            sku, asin, ean,
            suggested_brand, suggested_category, suggested_product_type,
            confidence, source
        FROM dbo.acc_taxonomy_prediction WITH (UPDLOCK, ROWLOCK)
        WHERE status = 'pending'
          AND confidence >= ?
        ORDER BY confidence DESC, updated_at DESC
        """,
        (min_confidence,),
    )
    rows = cur.fetchall()
    applied = 0
    for row in rows:
        pred_id = _clean(row[0])
        sku = _clean(row[1])
        asin = _clean(row[2])
        ean = _clean(row[3])
        suggested_brand = _clean(row[4])
        suggested_category = _clean(row[5])
        suggested_product_type = _clean(row[6])

        if not (suggested_brand or suggested_category or suggested_product_type):
            continue
        cur.execute(
            """
            UPDATE dbo.acc_product
            SET
                brand = CASE WHEN ISNULL(brand, '') = '' THEN NULLIF(?, '') ELSE brand END,
                category = CASE WHEN ISNULL(category, '') = '' THEN NULLIF(?, '') ELSE category END,
                subcategory = CASE WHEN ISNULL(subcategory, '') = '' THEN NULLIF(?, '') ELSE subcategory END,
                updated_at = SYSUTCDATETIME()
            WHERE
                (
                    (ISNULL(?, '') <> '' AND sku = ?)
                    OR (ISNULL(?, '') <> '' AND asin = ?)
                    OR (ISNULL(?, '') <> '' AND ean = ?)
                )
            """,
            (
                suggested_brand,
                suggested_category,
                suggested_product_type,
                sku,
                sku,
                asin,
                asin,
                ean,
                ean,
            ),
        )
        if _safe_int(cur.rowcount) > 0:
            cur.execute(
                """
                UPDATE dbo.acc_taxonomy_prediction
                SET status = 'auto_applied',
                    reviewed_by = ?,
                    reviewed_at = SYSUTCDATETIME(),
                    updated_at = SYSUTCDATETIME()
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                (actor, pred_id),
            )
            applied += 1
    return applied


def refresh_taxonomy_predictions(
    *,
    limit: int = 40000,
    min_auto_confidence: float = 0.90,
    auto_apply: bool = True,
    progress_hook: Callable[[int, int, int, str], None] | None = None,
) -> dict[str, Any]:
    ensure_taxonomy_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET DEADLOCK_PRIORITY LOW;")
        cur.execute("SET LOCK_TIMEOUT 15000;")
        _insert_default_nodes(cur)
        candidates = _load_product_candidates(cur, limit=limit)
        refs = _load_product_reference(cur)
        registry_rows = _load_registry_rows(cur)
        registry_by_sku: dict[str, dict[str, Any]] = {}
        registry_by_asin: dict[str, dict[str, Any]] = {}
        registry_by_ean: dict[str, dict[str, Any]] = {}
        for row in registry_rows:
            sku = _clean(row.get("merchant_sku"))
            sku_alt = _clean(row.get("merchant_sku_alt"))
            asin = _clean(row.get("asin"))
            ean = _clean(row.get("ean"))
            if sku and sku not in registry_by_sku:
                registry_by_sku[sku] = row
            if sku_alt and sku_alt not in registry_by_sku:
                registry_by_sku[sku_alt] = row
            if asin and asin not in registry_by_asin:
                registry_by_asin[asin] = row
            if ean and ean not in registry_by_ean:
                registry_by_ean[ean] = row

        total_candidates = len(candidates)
        processed_candidates = 0
        generated = 0
        source_counts: dict[str, int] = {"pim_exact": 0, "ean_match": 0, "embedding_match": 0}
        pending_commit = 0

        def _emit_progress(stage: str) -> None:
            if not progress_hook:
                return
            try:
                progress_hook(processed_candidates, total_candidates, generated, stage)
            except Exception:
                pass

        _emit_progress("building_candidates")
        for candidate in candidates:
            processed_candidates += 1
            prediction = _choose_prediction(
                candidate=candidate,
                ref_maps=refs,
                registry_by_sku=registry_by_sku,
                registry_by_asin=registry_by_asin,
                registry_by_ean=registry_by_ean,
            )
            if not prediction:
                continue
            retry = 0
            while True:
                try:
                    _upsert_prediction(cur, prediction)
                    break
                except Exception as exc:
                    err_text = str(exc).lower()
                    if (
                        "deadlock" in err_text
                        or "1205" in err_text
                        or "lock request time out" in err_text
                        or "1222" in err_text
                    ) and retry < 5:
                        time.sleep(0.15 * (2**retry))
                        retry += 1
                        continue
                    can_reconnect = (
                        retry < 2
                        and ("dbprocess is dead" in err_text or "not connected" in err_text or "connection is closed" in err_text)
                    )
                    if not can_reconnect:
                        raise
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = _connect()
                    cur = conn.cursor()
                    ensure_taxonomy_schema()
                    retry += 1
            generated += 1
            source = str(prediction.get("source") or "")
            if source in source_counts:
                source_counts[source] += 1
            pending_commit += 1
            if pending_commit >= 300:
                conn.commit()
                pending_commit = 0
            if processed_candidates % 50 == 0 or processed_candidates == total_candidates:
                _emit_progress("predicting")
        auto_applied = 0
        _emit_progress("auto_apply")
        if auto_apply:
            auto_applied = _auto_apply_predictions(cur, min_confidence=float(min_auto_confidence))
        conn.commit()
        _emit_progress("done")
        return {
            "status": "ok",
            "candidates": total_candidates,
            "generated": generated,
            "source_counts": source_counts,
            "auto_applied": auto_applied,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def list_taxonomy_predictions(
    *,
    status: str | None = None,
    min_confidence: float = 0.0,
    limit: int = 200,
) -> list[dict[str, Any]]:
    ensure_taxonomy_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        wheres = ["confidence >= ?"]
        params: list[Any] = [float(min_confidence)]
        if status:
            wheres.append("status = ?")
            params.append(status)
        cur.execute(
            f"""
            SELECT TOP {int(max(1, min(limit, 2000)))}
                CAST(id AS NVARCHAR(40)) AS id,
                marketplace_id, sku, asin, ean,
                suggested_brand, suggested_category, suggested_product_type,
                confidence, source, status, reason, evidence_json,
                created_at, updated_at, reviewed_by, reviewed_at
            FROM dbo.acc_taxonomy_prediction WITH (NOLOCK)
            WHERE {" AND ".join(wheres)}
            ORDER BY
                CASE status WHEN 'pending' THEN 0 WHEN 'auto_applied' THEN 1 WHEN 'applied' THEN 2 WHEN 'approved' THEN 3 ELSE 4 END,
                confidence DESC,
                updated_at DESC
            """,
            tuple(params),
        )
        out: list[dict[str, Any]] = []
        for row in cur.fetchall():
            out.append(
                {
                    "id": _clean(row[0]),
                    "marketplace_id": _clean(row[1]) or None,
                    "sku": _clean(row[2]) or None,
                    "asin": _clean(row[3]) or None,
                    "ean": _clean(row[4]) or None,
                    "suggested_brand": _clean(row[5]) or None,
                    "suggested_category": _clean(row[6]) or None,
                    "suggested_product_type": _clean(row[7]) or None,
                    "confidence": _safe_float(row[8]),
                    "source": _clean(row[9]),
                    "status": _clean(row[10]),
                    "reason": _clean(row[11]) or None,
                    "evidence": json.loads(row[12]) if row[12] else {},
                    "created_at": row[13].isoformat() if hasattr(row[13], "isoformat") else None,
                    "updated_at": row[14].isoformat() if hasattr(row[14], "isoformat") else None,
                    "reviewed_by": _clean(row[15]) or None,
                    "reviewed_at": row[16].isoformat() if hasattr(row[16], "isoformat") else None,
                }
            )
        return out
    finally:
        conn.close()


def review_taxonomy_prediction(*, prediction_id: str, action: str, actor: str = "system") -> dict[str, Any]:
    ensure_taxonomy_schema()
    action_norm = _clean_lower(action)
    if action_norm not in {"approve", "reject"}:
        raise ValueError("action must be approve|reject")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                sku, asin, ean,
                suggested_brand, suggested_category, suggested_product_type,
                confidence, source, status
            FROM dbo.acc_taxonomy_prediction WITH (UPDLOCK, ROWLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (prediction_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("prediction not found")
        sku = _clean(row[0])
        asin = _clean(row[1])
        ean = _clean(row[2])
        suggested_brand = _clean(row[3])
        suggested_category = _clean(row[4])
        suggested_product_type = _clean(row[5])

        if action_norm == "approve":
            cur.execute(
                """
                UPDATE dbo.acc_product
                SET
                    brand = CASE WHEN ISNULL(brand, '') = '' THEN NULLIF(?, '') ELSE brand END,
                    category = CASE WHEN ISNULL(category, '') = '' THEN NULLIF(?, '') ELSE category END,
                    subcategory = CASE WHEN ISNULL(subcategory, '') = '' THEN NULLIF(?, '') ELSE subcategory END,
                    updated_at = SYSUTCDATETIME()
                WHERE
                    (
                        (ISNULL(?, '') <> '' AND sku = ?)
                        OR (ISNULL(?, '') <> '' AND asin = ?)
                        OR (ISNULL(?, '') <> '' AND ean = ?)
                    )
                """,
                (
                    suggested_brand,
                    suggested_category,
                    suggested_product_type,
                    sku,
                    sku,
                    asin,
                    asin,
                    ean,
                    ean,
                ),
            )
            status_value = "applied"
        else:
            status_value = "rejected"
        cur.execute(
            """
            UPDATE dbo.acc_taxonomy_prediction
            SET status = ?, reviewed_by = ?, reviewed_at = SYSUTCDATETIME(), updated_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (status_value, actor, prediction_id),
        )
        conn.commit()
        return {"status": "ok", "prediction_id": prediction_id, "action": action_norm, "new_status": status_value}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_taxonomy_lookup(
    cur,
    *,
    skus: list[str] | None = None,
    asins: list[str] | None = None,
    eans: list[str] | None = None,
    min_confidence: float = 0.75,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Lookup approved/applied taxonomy suggestions for runtime fallback."""
    ensure_taxonomy_schema()
    sku_values = sorted({str(v).strip() for v in (skus or []) if str(v or "").strip()})
    asin_values = sorted({str(v).strip() for v in (asins or []) if str(v or "").strip()})
    ean_values = sorted({str(v).strip() for v in (eans or []) if str(v or "").strip()})
    if not sku_values and not asin_values and not ean_values:
        return {}, {}, {}

    by_sku: dict[str, dict[str, Any]] = {}
    by_asin: dict[str, dict[str, Any]] = {}
    by_ean: dict[str, dict[str, Any]] = {}

    def _chunks(values: list[str], size: int = 900):
        for i in range(0, len(values), size):
            yield values[i : i + size]

    def _consume_rows(rows: list[Any]) -> None:
        for row in rows:
            payload = {
                "brand": _clean(row[3]) or None,
                "category": _clean(row[4]) or None,
                "product_type": _clean(row[5]) or None,
                "confidence": _safe_float(row[6]),
                "source": _clean(row[7]) or None,
                "status": _clean(row[8]) or None,
            }
            sku = _clean(row[0])
            asin = _clean(row[1])
            ean = _clean(row[2])
            if sku and sku not in by_sku:
                by_sku[sku] = payload
            if asin and asin not in by_asin:
                by_asin[asin] = payload
            if ean and ean not in by_ean:
                by_ean[ean] = payload

    def _query_by_field(field: str, values: list[str]) -> None:
        if not values:
            return
        for chunk in _chunks(values):
            placeholders = ",".join("?" for _ in chunk)
            sql = f"""
                SELECT
                    sku, asin, ean,
                    suggested_brand, suggested_category, suggested_product_type,
                    confidence, source, status, updated_at
                FROM dbo.acc_taxonomy_prediction WITH (NOLOCK)
                WHERE confidence >= ?
                  AND status IN ('auto_applied', 'applied', 'approved')
                  AND {field} IN ({placeholders})
                ORDER BY confidence DESC, updated_at DESC
            """
            cur.execute(sql, (float(min_confidence), *chunk))
            _consume_rows(cur.fetchall())

    # Chunked IN strategy is significantly faster on pymssql than filling temp tables via executemany.
    _query_by_field("sku", sku_values)
    _query_by_field("asin", asin_values)
    _query_by_field("ean", ean_values)
    return by_sku, by_asin, by_ean
