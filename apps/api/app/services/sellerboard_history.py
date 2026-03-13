from __future__ import annotations

import csv
import hashlib
import json
from datetime import date
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc


def _fx_case(currency_col: str = "o.currency") -> str:
    """Build SQL CASE expression for FX fallback using DB-sourced rates."""
    from app.core.fx_service import build_fx_case_sql
    return build_fx_case_sql(currency_col)


def _connect():
    return connect_acc(autocommit=False, timeout=60)


def _reverse_marketplaces() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for marketplace_id, meta in MARKETPLACE_REGISTRY.items():
        mapping[str(meta.get("name") or "")] = marketplace_id
    mapping["Amazon.com.be"] = mapping.get("Amazon.be", "")
    return {k: v for k, v in mapping.items() if v}


REVERSE_MARKETPLACES = _reverse_marketplaces()


def _parse_datetime(raw: str) -> datetime | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d/%m/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_decimal(raw: str) -> Decimal | None:
    value = (raw or "").strip().replace(",", ".")
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _parse_bool(raw: str) -> bool | None:
    value = (raw or "").strip().lower()
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    return None


def _normalize_text(raw: Any) -> str:
    return str(raw or "").replace("\r", " ").replace("\n", " ").strip()


def _extract_asin(product_token: str) -> str:
    value = _normalize_text(product_token)
    if not value:
        return ""
    token = value.split(",")[0].split()[0].strip()
    if len(token) == 10 and token.upper().startswith("B"):
        return token.upper()
    return value[:40]


def _source_row_hash(source_name: str, row_number: int, row: dict[str, Any]) -> str:
    payload = {
        "source_name": source_name,
        "row_number": row_number,
        "amazon_order_id": _normalize_text(row.get("AmazonOrderId")),
        "purchase_date": _normalize_text(row.get("PurchaseDate(UTC)")),
        "sales_channel": _normalize_text(row.get("SalesChannel")),
        "fulfillment_channel": _normalize_text(row.get("FulfillmentChannel")),
        "products": _normalize_text(row.get("Products")),
        "order_total": _normalize_text(row.get("OrderTotalAmount")),
        "commission": _normalize_text(row.get("Comission")),
        "fba_fee": _normalize_text(row.get("FBAPerUnitFulfillmentFee")),
        "shipping_cost": _normalize_text(row.get("ShippingCost")),
        "order_status": _normalize_text(row.get("OrderStatus")),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def ensure_sellerboard_history_schema() -> None:
    """No-op — schema managed by Alembic migration eb019."""


def _period_key(date_from: date, date_to: date) -> str:
    return f"{date_from.isoformat()}__{date_to.isoformat()}"


def _upsert_rebuild_state(
    cur,
    *,
    period_key: str,
    date_from: date,
    date_to: date,
    source_name: str | None,
    status: str,
    target_orders: int = 0,
    candidate_lines: int = 0,
    candidate_with_product: int = 0,
    candidate_with_sku: int = 0,
    inserted_lines: int = 0,
    note: str | None = None,
    mark_started: bool = False,
    mark_finished: bool = False,
) -> None:
    cur.execute(
        """
        MERGE dbo.acc_sb_order_line_rebuild_state AS target
        USING (
            SELECT
                ? AS period_key,
                ? AS date_from,
                ? AS date_to,
                ? AS source_name
        ) AS src
        ON target.period_key = src.period_key
        WHEN MATCHED THEN
            UPDATE SET
                date_from = src.date_from,
                date_to = src.date_to,
                source_name = src.source_name,
                status = ?,
                target_orders = ?,
                candidate_lines = ?,
                candidate_with_product = ?,
                candidate_with_sku = ?,
                inserted_lines = ?,
                note = ?,
                started_at = CASE WHEN ? = 1 AND target.started_at IS NULL THEN SYSUTCDATETIME() ELSE target.started_at END,
                finished_at = CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE target.finished_at END,
                updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                period_key, date_from, date_to, source_name, status,
                target_orders, candidate_lines, candidate_with_product, candidate_with_sku,
                inserted_lines, note, started_at, finished_at, updated_at
            )
            VALUES (
                src.period_key, src.date_from, src.date_to, src.source_name, ?,
                ?, ?, ?, ?, ?, ?,
                CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE NULL END,
                CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE NULL END,
                SYSUTCDATETIME()
            );
        """,
        [
            period_key,
            date_from,
            date_to,
            source_name,
            status,
            int(target_orders),
            int(candidate_lines),
            int(candidate_with_product),
            int(candidate_with_sku),
            int(inserted_lines),
            (note or "")[:400] or None,
            1 if mark_started else 0,
            1 if mark_finished else 0,
            status,
            int(target_orders),
            int(candidate_lines),
            int(candidate_with_product),
            int(candidate_with_sku),
            int(inserted_lines),
            (note or "")[:400] or None,
            1 if mark_started else 0,
            1 if mark_finished else 0,
        ],
    )


def stage_sellerboard_orders_2025(
    report_file: str | Path,
    *,
    replace_existing: bool = True,
    batch_size: int = 5000,
) -> dict[str, Any]:
    ensure_sellerboard_history_schema()
    path = Path(report_file)
    source_name = path.name
    source_hash = hashlib.sha256(path.read_bytes()).hexdigest()

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT source_hash, status, row_count_total, row_count_2025
            FROM dbo.acc_sb_order_line_sync_state WITH (NOLOCK)
            WHERE source_name = ?
            """,
            [source_name],
        )
        existing = cur.fetchone()
        if existing and str(existing[0] or "") == source_hash and str(existing[1] or "") == "completed":
            return {
                "status": "skipped",
                "reason": "unchanged_hash",
                "source_name": source_name,
                "row_count_total": int(existing[2] or 0),
                "row_count_2025": int(existing[3] or 0),
            }

        cur.execute(
            """
            MERGE dbo.acc_sb_order_line_sync_state AS target
            USING (SELECT ? AS source_name) AS src
            ON target.source_name = src.source_name
            WHEN MATCHED THEN
                UPDATE SET source_hash = ?, status = 'importing', note = 'preparing import', updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (source_name, source_hash, status, note, updated_at)
                VALUES (?, ?, 'importing', 'preparing import', SYSUTCDATETIME());
            """,
            [source_name, source_hash, source_name, source_hash],
        )
        if replace_existing:
            cur.execute("DELETE FROM dbo.acc_sb_order_line_staging WHERE source_name = ?", [source_name])
        conn.commit()

        insert_sql = """
            INSERT INTO dbo.acc_sb_order_line_staging (
                source_name, source_hash, source_row_number, source_row_hash,
                amazon_order_id, purchase_date, marketplace_id, marketplace_code, sales_channel,
                fulfillment_channel, order_status, currency, asin, product_token, quantity,
                order_total_amount, shipping_amount, gift_wrap_amount, tax_amount,
                item_promotion_amount, ship_promotion_amount, commission_amount,
                fba_fee_amount, coupon_amount, raw_shipping_cost_amount,
                is_premium_order, shipped_by_amazon_tfm, is_replacement_order,
                is_business_order, is_prime, shipment_service_level, raw_json, synced_at, updated_at
            )
            VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME()
            )
        """

        stats = Counter()
        batch: list[tuple[Any, ...]] = []
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=";")
            for row_number, row in enumerate(reader, start=2):
                stats["rows_total"] += 1
                purchase_date = _parse_datetime(row.get("PurchaseDate(UTC)") or "")
                if purchase_date is None or purchase_date.year != 2025:
                    continue
                stats["rows_2025"] += 1
                sales_channel = _normalize_text(row.get("SalesChannel"))
                marketplace_id = REVERSE_MARKETPLACES.get(sales_channel, "")
                marketplace_code = MARKETPLACE_REGISTRY.get(marketplace_id, {}).get("code", "") if marketplace_id else ""
                product_token = _normalize_text(row.get("Products"))
                params = (
                    source_name,
                    source_hash,
                    row_number,
                    _source_row_hash(source_name, row_number, row),
                    _normalize_text(row.get("AmazonOrderId")),
                    purchase_date,
                    marketplace_id or None,
                    marketplace_code or None,
                    sales_channel or None,
                    _normalize_text(row.get("FulfillmentChannel")) or None,
                    _normalize_text(row.get("OrderStatus")) or None,
                    _normalize_text(row.get("OrderTotalCurrencyCode")) or None,
                    _extract_asin(product_token) or None,
                    product_token or None,
                    _parse_decimal(row.get("NumberOfItems") or ""),
                    _parse_decimal(row.get("OrderTotalAmount") or ""),
                    _parse_decimal(row.get("Shipping") or ""),
                    _parse_decimal(row.get("Gift wrap") or ""),
                    _parse_decimal(row.get("Tax") or ""),
                    _parse_decimal(row.get("Item promotion") or ""),
                    _parse_decimal(row.get("Ship Promotion") or ""),
                    _parse_decimal(row.get("Comission") or ""),
                    _parse_decimal(row.get("FBAPerUnitFulfillmentFee") or ""),
                    _parse_decimal(row.get("Coupons") or ""),
                    _parse_decimal(row.get("ShippingCost") or ""),
                    _parse_bool(row.get("IsPremiumOrder") or ""),
                    _parse_bool(row.get("ShippedByAmazonTFM") or ""),
                    _parse_bool(row.get("IsReplacementOrder") or ""),
                    _parse_bool(row.get("IsBusinessOrder") or ""),
                    _parse_bool(row.get("IsPrime") or ""),
                    _normalize_text(row.get("ShipmentServiceLevelCategory")) or None,
                    json.dumps(row, ensure_ascii=False),
                )
                batch.append(params)
                if len(batch) >= batch_size:
                    if hasattr(cur, "fast_executemany"):
                        cur.fast_executemany = True
                    cur.executemany(insert_sql, batch)
                    if hasattr(cur, "fast_executemany"):
                        cur.fast_executemany = False
                    conn.commit()
                    stats["inserted"] += len(batch)
                    batch = []

        if batch:
            if hasattr(cur, "fast_executemany"):
                cur.fast_executemany = True
            cur.executemany(insert_sql, batch)
            if hasattr(cur, "fast_executemany"):
                cur.fast_executemany = False
            conn.commit()
            stats["inserted"] += len(batch)

        cur.execute(
            """
            MERGE dbo.acc_sb_order_line_sync_state AS target
            USING (SELECT ? AS source_name) AS src
            ON target.source_name = src.source_name
            WHEN MATCHED THEN
                UPDATE SET
                    source_hash = ?,
                    row_count_total = ?,
                    row_count_2025 = ?,
                    status = 'completed',
                    note = ?,
                    last_imported_at = SYSUTCDATETIME(),
                    updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (source_name, source_hash, row_count_total, row_count_2025, status, note, last_imported_at, updated_at)
                VALUES (?, ?, ?, ?, 'completed', ?, SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
            [
                source_name,
                source_hash,
                int(stats["rows_total"]),
                int(stats["rows_2025"]),
                f"inserted={int(stats['inserted'])}",
                source_name,
                source_hash,
                int(stats["rows_total"]),
                int(stats["rows_2025"]),
                f"inserted={int(stats['inserted'])}",
            ],
        )
        conn.commit()
        return {
            "status": "completed",
            "source_name": source_name,
            "source_hash": source_hash,
            "row_count_total": int(stats["rows_total"]),
            "row_count_2025": int(stats["rows_2025"]),
            "inserted": int(stats["inserted"]),
        }
    except Exception as exc:
        conn.rollback()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                MERGE dbo.acc_sb_order_line_sync_state AS target
                USING (SELECT ? AS source_name) AS src
                ON target.source_name = src.source_name
                WHEN MATCHED THEN
                    UPDATE SET status = 'failed', note = ?, updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (source_name, source_hash, status, note, updated_at)
                    VALUES (?, ?, 'failed', ?, SYSUTCDATETIME());
                """,
                [source_name, str(exc)[:400], source_name, source_hash, str(exc)[:400]],
            )
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def insert_historical_order_lines_from_sellerboard(
    *,
    date_from: date,
    date_to: date,
    source_name: str | None = None,
    only_orders_without_lines: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    ensure_sellerboard_history_schema()
    period_key = _period_key(date_from, date_to)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET DEADLOCK_PRIORITY LOW")
        cur.execute("SET LOCK_TIMEOUT 10000")
        if not dry_run:
            cur.execute(
                """
                SELECT status, started_at
                FROM dbo.acc_sb_order_line_rebuild_state WITH (NOLOCK)
                WHERE period_key = ?
                """,
                [period_key],
            )
            existing_state = cur.fetchone()
            if existing_state and str(existing_state[0] or "") == "running" and existing_state[1]:
                return {
                    "status": "skipped",
                    "period_key": period_key,
                    "reason": "already_running",
                    "started_at": existing_state[1],
                }
        cur.execute("IF OBJECT_ID('tempdb..#tmp_sb_target_orders') IS NOT NULL DROP TABLE #tmp_sb_target_orders")
        source_filter_sql = ""
        params: list[Any] = [date_from, date_to]
        if source_name:
            source_filter_sql = " AND EXISTS (SELECT 1 FROM dbo.acc_sb_order_line_staging s2 WITH (NOLOCK) WHERE s2.amazon_order_id = o.amazon_order_id AND s2.source_name = ?)"
            params.append(source_name)
        if only_orders_without_lines:
            source_filter_sql += " AND NOT EXISTS (SELECT 1 FROM dbo.acc_order_line ol WITH (NOLOCK) WHERE ol.order_id = o.id)"

        if not dry_run:
            _upsert_rebuild_state(
                cur,
                period_key=period_key,
                date_from=date_from,
                date_to=date_to,
                source_name=source_name,
                status="running",
                note="building temp targets",
                mark_started=True,
            )
            conn.commit()

        cur.execute(
            f"""
            SELECT
                o.id AS order_id,
                o.amazon_order_id,
                o.marketplace_id,
                o.status,
                CAST(o.purchase_date AS DATE) AS purchase_date,
                o.currency
            INTO #tmp_sb_target_orders
            FROM dbo.acc_order o WITH (NOLOCK)
            WHERE o.purchase_date >= ?
              AND o.purchase_date < DATEADD(day, 1, ?)
              AND o.status IN ('Shipped', 'PartiallyShipped', 'Return', 'Refund')
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
              AND o.amazon_order_id NOT LIKE 'S02-%'
              {source_filter_sql}
            """,
            tuple(params),
        )
        cur.execute("CREATE INDEX IX_tmp_sb_target_orders_order ON #tmp_sb_target_orders(amazon_order_id)")
        cur.execute("IF OBJECT_ID('tempdb..#tmp_sb_mapped_lines') IS NOT NULL DROP TABLE #tmp_sb_mapped_lines")
        line_params: list[Any] = []
        mapped_source_where = ""
        if source_name:
            mapped_source_where = " AND s.source_name = ?"
            line_params.append(source_name)
        cur.execute(
            f"""
            SELECT
                tgt.order_id,
                tgt.amazon_order_id,
                CONCAT('SBH|', LEFT(s.source_row_hash, 32)) AS amazon_order_item_id,
                mapped.product_id,
                mapped.sku,
                COALESCE(NULLIF(s.asin, ''), mapped.asin) AS asin,
                mapped.title AS title,
                CASE
                    WHEN TRY_CONVERT(INT, ROUND(ISNULL(s.quantity, 1), 0)) IS NULL OR TRY_CONVERT(INT, ROUND(ISNULL(s.quantity, 1), 0)) <= 0 THEN 1
                    ELSE TRY_CONVERT(INT, ROUND(ISNULL(s.quantity, 1), 0))
                END AS quantity_ordered,
                CASE
                    WHEN tgt.status IN ('Shipped', 'PartiallyShipped', 'Return', 'Refund') THEN
                        CASE
                            WHEN TRY_CONVERT(INT, ROUND(ISNULL(s.quantity, 1), 0)) IS NULL OR TRY_CONVERT(INT, ROUND(ISNULL(s.quantity, 1), 0)) <= 0 THEN 1
                            ELSE TRY_CONVERT(INT, ROUND(ISNULL(s.quantity, 1), 0))
                        END
                    ELSE 0
                END AS quantity_shipped,
                CAST(
                    CASE
                        WHEN s.order_total_amount IS NULL THEN NULL
                        ELSE s.order_total_amount - ISNULL(s.tax_amount, 0)
                    END
                    AS DECIMAL(10,2)
                ) AS item_price,
                CAST(ISNULL(s.tax_amount, 0) AS DECIMAL(10,2)) AS item_tax,
                CAST(ABS(ISNULL(s.item_promotion_amount, 0)) + ABS(ISNULL(s.ship_promotion_amount, 0)) + ABS(ISNULL(s.coupon_amount, 0)) AS DECIMAL(10,2)) AS promotion_discount,
                COALESCE(NULLIF(s.currency, ''), tgt.currency, 'EUR') AS currency,
                CAST(
                    ABS(ISNULL(s.fba_fee_amount, 0)) * ISNULL(
                        fx.rate_to_pln,
                        {_fx_case("COALESCE(NULLIF(s.currency, ''), tgt.currency, 'EUR')")}
                    )
                    AS DECIMAL(10,4)
                ) AS fba_fee_pln,
                CAST(
                    ABS(ISNULL(s.commission_amount, 0)) * ISNULL(
                        fx.rate_to_pln,
                        {_fx_case("COALESCE(NULLIF(s.currency, ''), tgt.currency, 'EUR')")}
                    )
                    AS DECIMAL(10,4)
                ) AS referral_fee_pln
            INTO #tmp_sb_mapped_lines
            FROM #tmp_sb_target_orders tgt
            INNER JOIN dbo.acc_sb_order_line_staging s WITH (NOLOCK)
                ON s.amazon_order_id = tgt.amazon_order_id
            OUTER APPLY (
                SELECT TOP 1
                    r.merchant_sku,
                    r.merchant_sku_alt,
                    r.internal_sku,
                    r.asin,
                    r.product_name
                FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
                WHERE (s.asin IS NOT NULL AND r.asin = s.asin)
                   OR (s.product_token IS NOT NULL AND (r.merchant_sku = s.product_token OR r.merchant_sku_alt = s.product_token))
                ORDER BY
                    CASE
                        WHEN s.asin IS NOT NULL AND r.asin = s.asin THEN 0
                        WHEN s.product_token IS NOT NULL AND r.merchant_sku = s.product_token THEN 1
                        WHEN s.product_token IS NOT NULL AND r.merchant_sku_alt = s.product_token THEN 2
                        ELSE 9
                    END,
                    r.updated_at DESC
            ) rg
            OUTER APPLY (
                SELECT TOP 1 p.id, p.sku, p.asin, p.title
                FROM dbo.acc_product p WITH (NOLOCK)
                WHERE s.asin IS NOT NULL AND p.asin = s.asin
                ORDER BY p.updated_at DESC
            ) p_asin
            OUTER APPLY (
                SELECT TOP 1 p.id, p.sku, p.asin, p.title
                FROM dbo.acc_product p WITH (NOLOCK)
                WHERE rg.merchant_sku IS NOT NULL AND p.sku = rg.merchant_sku
                ORDER BY p.updated_at DESC
            ) p_sku
            OUTER APPLY (
                SELECT TOP 1 p.id, p.sku, p.asin, p.title
                FROM dbo.acc_product p WITH (NOLOCK)
                WHERE rg.internal_sku IS NOT NULL AND p.internal_sku = rg.internal_sku
                ORDER BY p.updated_at DESC
            ) p_internal
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate fx WITH (NOLOCK)
                WHERE fx.currency = COALESCE(NULLIF(s.currency, ''), tgt.currency, 'EUR')
                  AND fx.rate_date <= tgt.purchase_date
                ORDER BY fx.rate_date DESC
            ) fx
            OUTER APPLY (
                SELECT
                    COALESCE(p_sku.id, p_asin.id, p_internal.id) AS product_id,
                    COALESCE(p_sku.sku, p_asin.sku, p_internal.sku, rg.merchant_sku, rg.merchant_sku_alt) AS sku,
                    COALESCE(p_sku.asin, p_asin.asin, p_internal.asin, rg.asin, s.asin) AS asin,
                    COALESCE(p_sku.title, p_asin.title, p_internal.title, rg.product_name, s.product_token, s.asin) AS title
            ) mapped
            WHERE ISNULL(s.quantity, 0) > 0
              {mapped_source_where}
            """,
            tuple(line_params),
        )
        cur.execute("CREATE UNIQUE INDEX UX_tmp_sb_mapped_lines_item ON #tmp_sb_mapped_lines(amazon_order_item_id)")
        cur.execute("SELECT COUNT(*) FROM #tmp_sb_target_orders")
        target_orders = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM #tmp_sb_mapped_lines")
        candidate_lines = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM #tmp_sb_mapped_lines WHERE product_id IS NOT NULL")
        candidate_with_product = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM #tmp_sb_mapped_lines WHERE sku IS NOT NULL")
        candidate_with_sku = int(cur.fetchone()[0] or 0)
        if not dry_run:
            _upsert_rebuild_state(
                cur,
                period_key=period_key,
                date_from=date_from,
                date_to=date_to,
                source_name=source_name,
                status="running",
                target_orders=target_orders,
                candidate_lines=candidate_lines,
                candidate_with_product=candidate_with_product,
                candidate_with_sku=candidate_with_sku,
                note="candidate lines prepared",
            )
            conn.commit()
        if dry_run:
            conn.rollback()
            return {
                "status": "dry_run",
                "period_key": period_key,
                "target_orders": target_orders,
                "candidate_lines": candidate_lines,
                "candidate_with_product": candidate_with_product,
                "candidate_with_sku": candidate_with_sku,
            }

        cur.execute(
            """
            INSERT INTO dbo.acc_order_line (
                id, order_id, product_id, amazon_order_item_id, sku, asin, title,
                quantity_ordered, quantity_shipped, item_price, item_tax,
                promotion_discount, currency, fba_fee_pln, referral_fee_pln
            )
            SELECT
                NEWID(),
                src.order_id,
                src.product_id,
                src.amazon_order_item_id,
                src.sku,
                src.asin,
                LEFT(src.title, 500),
                src.quantity_ordered,
                src.quantity_shipped,
                src.item_price,
                src.item_tax,
                src.promotion_discount,
                src.currency,
                CASE WHEN src.fba_fee_pln > 0 THEN src.fba_fee_pln ELSE NULL END,
                CASE WHEN src.referral_fee_pln > 0 THEN src.referral_fee_pln ELSE NULL END
            FROM #tmp_sb_mapped_lines src
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                WHERE ol.amazon_order_item_id = src.amazon_order_item_id
            )
            """
        )
        inserted_lines = int(cur.rowcount or 0)
        _upsert_rebuild_state(
            cur,
            period_key=period_key,
            date_from=date_from,
            date_to=date_to,
            source_name=source_name,
            status="completed",
            target_orders=target_orders,
            candidate_lines=candidate_lines,
            candidate_with_product=candidate_with_product,
            candidate_with_sku=candidate_with_sku,
            inserted_lines=inserted_lines,
            note="lines inserted",
            mark_finished=True,
        )
        conn.commit()
        return {
            "status": "completed",
            "period_key": period_key,
            "target_orders": target_orders,
            "candidate_lines": candidate_lines,
            "candidate_with_product": candidate_with_product,
            "candidate_with_sku": candidate_with_sku,
            "inserted_lines": inserted_lines,
        }
    except Exception as exc:
        conn.rollback()
        try:
            cur = conn.cursor()
            _upsert_rebuild_state(
                cur,
                period_key=period_key,
                date_from=date_from,
                date_to=date_to,
                source_name=source_name,
                status="failed",
                note=str(exc),
                mark_finished=True,
            )
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def finalize_historical_order_lines_2025(*, date_from: date = date(2025, 1, 1), date_to: date = date(2025, 12, 31)) -> dict[str, Any]:
    from app.connectors.mssql.mssql_store import recalc_profit_orders
    from app.services.order_pipeline import (
        step_backfill_products,
        step_bridge_fees,
        step_enrich_products_from_registry,
        step_link_order_lines,
        step_stamp_purchase_prices,
    )

    enriched_products = step_enrich_products_from_registry()
    backfilled_products = step_backfill_products()
    linked_lines = step_link_order_lines()
    stamped_prices = step_stamp_purchase_prices()
    bridged_lines = step_bridge_fees()
    recalc_orders = recalc_profit_orders(date_from=date_from, date_to=date_to)

    return {
        "status": "completed",
        "enriched_products": int(enriched_products or 0),
        "backfilled_products": int(backfilled_products or 0),
        "linked_lines": int(linked_lines or 0),
        "stamped_prices": int(stamped_prices or 0),
        "bridged_lines": int(bridged_lines or 0),
        "recalc_orders": int(recalc_orders or 0),
    }


def rebuild_historical_order_lines_2025(
    *,
    source_name: str | None = None,
    only_orders_without_lines: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    insert_result = insert_historical_order_lines_from_sellerboard(
        date_from=date(2025, 1, 1),
        date_to=date(2025, 12, 31),
        source_name=source_name,
        only_orders_without_lines=only_orders_without_lines,
        dry_run=dry_run,
    )
    if dry_run:
        return insert_result

    finalize_result = finalize_historical_order_lines_2025()

    return {
        **insert_result,
        **finalize_result,
    }
