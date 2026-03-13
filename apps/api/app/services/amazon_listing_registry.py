from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from typing import Any
from urllib.request import urlopen

import structlog

from app.core.config import settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

DEFAULT_AMAZON_LISTING_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1rRBVZUTwqYcGYZRSp28mIWXw7gMfvqes0apEE_hdpjo/export?format=csv&gid=400534387"
)
DEFAULT_AMAZON_LISTING_GID = "400534387"


def _connect():
    return connect_acc(autocommit=False, timeout=20)


def _norm_text(v: Any) -> str:
    return str(v or "").replace("\n", " ").replace("\r", " ").strip()


def _norm_internal_sku(v: Any) -> str:
    txt = _norm_text(v)
    if txt.endswith(".0"):
        txt = txt[:-2]
    return txt


def _merchant_sku_alt(merchant_sku: str) -> str:
    if merchant_sku.startswith("MAG_"):
        return merchant_sku.replace("MAG_", "FBA_", 1)
    if merchant_sku.startswith("FBA_"):
        return merchant_sku.replace("FBA_", "MAG_", 1)
    return ""


def ensure_amazon_listing_registry_schema() -> None:
    """No-op — schema managed by Alembic migration eb021."""


def _fetch_listing_registry_rows() -> tuple[list[dict[str, Any]], str, str, str]:
    url = getattr(settings, "GSHEET_AMAZON_LISTING_CSV_URL", "") or DEFAULT_AMAZON_LISTING_CSV_URL
    source_gid = DEFAULT_AMAZON_LISTING_GID
    with urlopen(url, timeout=30) as response:
        payload = response.read()
    source_hash = hashlib.sha256(payload).hexdigest()
    text = payload.decode("utf-8-sig", errors="replace")

    rows: list[dict[str, Any]] = []
    for raw in csv.DictReader(io.StringIO(text)):
        merchant_sku = _norm_text(raw.get("Merchant SKU"))
        internal_sku = _norm_internal_sku(raw.get("Nr art."))
        ean = _norm_text(raw.get("EAN"))
        asin = _norm_text(raw.get("ASIN (ADSY)"))
        parent_asin = _norm_text(raw.get("Parent Asin"))
        brand = _norm_text(raw.get("Marka"))
        product_name = _norm_text(raw.get("Nazwa"))
        listing_role = _norm_text(raw.get("Parent/Child"))
        priority_label = _norm_text(raw.get("Priorytet"))
        launch_type = _norm_text(raw.get("Typ wdroż."))
        category_1 = _norm_text(raw.get("Kategoria 1"))
        category_2 = _norm_text(raw.get("Kategoria 2"))

        if not any([merchant_sku, internal_sku, ean, asin, product_name]):
            continue

        clean = {
            "merchant_sku": merchant_sku,
            "merchant_sku_alt": _merchant_sku_alt(merchant_sku),
            "internal_sku": internal_sku,
            "ean": ean,
            "asin": asin,
            "parent_asin": parent_asin,
            "brand": brand,
            "product_name": product_name,
            "listing_role": listing_role,
            "priority_label": priority_label,
            "launch_type": launch_type,
            "category_1": category_1,
            "category_2": category_2,
        }
        row_hash = hashlib.sha256(
            json.dumps(clean, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        clean["row_hash"] = row_hash
        clean["raw_json"] = json.dumps(raw, ensure_ascii=False)
        rows.append(clean)

    return rows, source_hash, url, source_gid


def sync_amazon_listing_registry(force: bool = False, job_id: str | None = None) -> dict[str, Any]:
    ensure_amazon_listing_registry_schema()
    rows, source_hash, source_url, source_gid = _fetch_listing_registry_rows()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT source_hash, row_count, last_synced_at
            FROM dbo.acc_amazon_listing_registry_sync_state WITH (NOLOCK)
            WHERE source_gid = ?
            """,
            [source_gid],
        )
        existing = cur.fetchone()
        if existing and not force and str(existing[0] or "") == source_hash:
            return {
                "status": "skipped",
                "reason": "unchanged_hash",
                "row_count": int(existing[1] or 0),
                "source_gid": source_gid,
            }

        cur.execute("DELETE FROM dbo.acc_amazon_listing_registry WHERE source_gid = ?", [source_gid])
        synced_at = datetime.now(timezone.utc)
        for row in rows:
            cur.execute(
                """
                INSERT INTO dbo.acc_amazon_listing_registry (
                    merchant_sku, merchant_sku_alt, internal_sku, ean, asin, parent_asin,
                    brand, product_name, listing_role, priority_label, launch_type,
                    category_1, category_2, source_gid, row_hash, raw_json, synced_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["merchant_sku"] or None,
                    row["merchant_sku_alt"] or None,
                    row["internal_sku"] or None,
                    row["ean"] or None,
                    row["asin"] or None,
                    row["parent_asin"] or None,
                    row["brand"] or None,
                    row["product_name"] or None,
                    row["listing_role"] or None,
                    row["priority_label"] or None,
                    row["launch_type"] or None,
                    row["category_1"] or None,
                    row["category_2"] or None,
                    source_gid,
                    row["row_hash"],
                    row["raw_json"],
                    synced_at,
                    synced_at,
                ],
            )

        cur.execute(
            """
            MERGE dbo.acc_amazon_listing_registry_sync_state AS target
            USING (SELECT ? AS source_gid) AS src
            ON target.source_gid = src.source_gid
            WHEN MATCHED THEN
                UPDATE SET
                    source_url = ?,
                    source_hash = ?,
                    row_count = ?,
                    last_synced_at = ?,
                    updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (source_gid, source_url, source_hash, row_count, last_synced_at, updated_at)
                VALUES (?, ?, ?, ?, ?, SYSUTCDATETIME());
            """,
            [
                source_gid,
                source_url,
                source_hash,
                len(rows),
                synced_at,
                source_gid,
                source_url,
                source_hash,
                len(rows),
                synced_at,
            ],
        )
        conn.commit()
        return {
            "status": "synced",
            "row_count": len(rows),
            "source_gid": source_gid,
            "source_hash": source_hash,
            "source_url": source_url,
        }
    finally:
        conn.close()


def lookup_listing_registry_context(
    cur,
    *,
    sku: str | None = None,
    asin: str | None = None,
    ean: str | None = None,
    internal_sku: str | None = None,
) -> dict[str, Any]:
    sku_value = _norm_text(sku)
    asin_value = _norm_text(asin)
    ean_value = _norm_text(ean)
    internal_sku_value = _norm_internal_sku(internal_sku)
    if not any([sku_value, asin_value, ean_value, internal_sku_value]):
        return {}

    cur.execute(
        """
        SELECT TOP 1
            merchant_sku,
            merchant_sku_alt,
            internal_sku,
            ean,
            asin,
            parent_asin,
            brand,
            product_name,
            listing_role,
            priority_label,
            launch_type,
            category_1,
            category_2,
            source_gid,
            row_hash
        FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
        WHERE
            (? <> '' AND (merchant_sku = ? OR merchant_sku_alt = ?))
            OR (? <> '' AND asin = ?)
            OR (? <> '' AND ean = ?)
            OR (? <> '' AND internal_sku = ?)
        ORDER BY
            CASE
                WHEN ? <> '' AND merchant_sku = ? THEN 0
                WHEN ? <> '' AND merchant_sku_alt = ? THEN 1
                WHEN ? <> '' AND asin = ? THEN 2
                WHEN ? <> '' AND ean = ? THEN 3
                WHEN ? <> '' AND internal_sku = ? THEN 4
                ELSE 9
            END,
            updated_at DESC
        """,
        (
            sku_value,
            sku_value,
            sku_value,
            asin_value,
            asin_value,
            ean_value,
            ean_value,
            internal_sku_value,
            internal_sku_value,
            sku_value,
            sku_value,
            sku_value,
            sku_value,
            asin_value,
            asin_value,
            ean_value,
            ean_value,
            internal_sku_value,
            internal_sku_value,
        ),
    )
    row = cur.fetchone()
    if not row:
        return {}
    return {
        "merchant_sku": _norm_text(row[0]) or None,
        "merchant_sku_alt": _norm_text(row[1]) or None,
        "internal_sku": _norm_internal_sku(row[2]) or None,
        "ean": _norm_text(row[3]) or None,
        "asin": _norm_text(row[4]) or None,
        "parent_asin": _norm_text(row[5]) or None,
        "brand": _norm_text(row[6]) or None,
        "product_name": _norm_text(row[7]) or None,
        "listing_role": _norm_text(row[8]) or None,
        "priority_label": _norm_text(row[9]) or None,
        "launch_type": _norm_text(row[10]) or None,
        "category_1": _norm_text(row[11]) or None,
        "category_2": _norm_text(row[12]) or None,
        "source_gid": _norm_text(row[13]) or None,
        "row_hash": _norm_text(row[14]) or None,
    }
