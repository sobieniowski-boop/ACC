from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import structlog

from app.core.config import settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

_PAGE_SIZE = 100
_COMMIT_EVERY_ORDERS = 25

class BaseLinkerDistributionAPIError(RuntimeError):
    """Raised when BaseLinker Distribution API returns an error."""


@dataclass
class SyncCursorState:
    source_id: int
    cursor_ts: int
    fetched_orders: int = 0


def _connect():
    return connect_acc(autocommit=False, timeout=60)


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


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _normalize_text(value).lower()
    return text in {"1", "true", "yes", "y"}


def _normalize_tracking(value: Any) -> str:
    return _normalize_text(value).upper().replace(" ", "")


def _payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    ).hexdigest()


def _parse_epoch_datetime(value: Any) -> datetime | None:
    raw = _normalize_int(value)
    if raw is None or raw <= 0:
        return None
    return datetime.fromtimestamp(raw, tz=timezone.utc)


def _to_epoch(value: date | datetime | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    else:
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)  # type: ignore[union-attr]
    return int(dt.timestamp())


def _call_baselinker(method: str, params: dict[str, Any]) -> dict[str, Any]:
    if not settings.baselinker_distribution_enabled:
        raise RuntimeError("BaseLinker Distribution API is not configured")

    body = urlencode(
        {
            "method": method,
            "parameters": json.dumps(params, separators=(",", ":"), ensure_ascii=False),
        }
    ).encode("utf-8")
    req = Request(
        settings.BASELINKER_API,
        data=body,
        headers={"X-BLToken": settings.BASELINKER_DISTRIBUTION_TOKEN},
    )
    try:
        with urlopen(req, timeout=int(settings.BASELINKER_TIMEOUT_SEC or 30)) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise BaseLinkerDistributionAPIError(f"BaseLinker HTTP {exc.code}: {details[:500]}") from exc
    except URLError as exc:
        raise BaseLinkerDistributionAPIError(f"BaseLinker network error: {exc}") from exc
    except TimeoutError as exc:
        raise BaseLinkerDistributionAPIError("BaseLinker timeout") from exc

    if str(payload.get("status") or "").upper() != "SUCCESS":
        error_message = _normalize_text(payload.get("error_message")) or json.dumps(payload, ensure_ascii=False)[:500]
        raise BaseLinkerDistributionAPIError(f"BaseLinker {method} failed: {error_message}")
    return payload


def discover_distribution_source_ids() -> list[int]:
    payload = _call_baselinker("getOrderSources", {})
    sources = payload.get("sources") or {}
    blconnect_sources = sources.get("blconnect") or {}
    result = sorted(
        {
            int(source_id)
            for source_id in blconnect_sources.keys()
            if str(source_id).strip().isdigit()
        }
    )
    return result


def _order_page_sleep_sec() -> float:
    return max(0.0, float(settings.BASELINKER_DISTRIBUTION_PAGE_SLEEP_SEC or 0.0))


def _package_call_sleep_sec() -> float:
    return max(0.0, float(settings.BASELINKER_DISTRIBUTION_PACKAGE_SLEEP_SEC or 0.0))


def ensure_bl_distribution_cache_schema() -> None:
    """No-op: schema managed by Alembic migration eb015."""
    pass


def _upsert_distribution_order(cur, order: dict[str, Any]) -> None:
    order_id = _normalize_int(order.get("order_id"))
    if order_id is None:
        return
    payload_json = json.dumps(order, ensure_ascii=False, sort_keys=True)
    payload_hash = _payload_hash(order)
    cur.execute(
        """
IF EXISTS (SELECT 1 FROM dbo.acc_bl_distribution_order_cache WITH (NOLOCK) WHERE order_id = ?)
BEGIN
    UPDATE dbo.acc_bl_distribution_order_cache
    SET shop_order_id = ?,
        external_order_id = ?,
        order_source = ?,
        order_source_id = ?,
        order_status_id = ?,
        date_add = ?,
        date_confirmed = ?,
        date_in_status = ?,
        confirmed = ?,
        delivery_method = ?,
        delivery_package_module = ?,
        delivery_package_nr = ?,
        delivery_country_code = ?,
        delivery_fullname = ?,
        email = ?,
        phone = ?,
        admin_comments = ?,
        extra_field_1 = ?,
        extra_field_2 = ?,
        order_page = ?,
        pick_state = ?,
        pack_state = ?,
        raw_payload_json = ?,
        source_hash = ?,
        last_synced_at = SYSUTCDATETIME()
    WHERE order_id = ?;
END
ELSE
BEGIN
    INSERT INTO dbo.acc_bl_distribution_order_cache (
        order_id, shop_order_id, external_order_id, order_source, order_source_id,
        order_status_id, date_add, date_confirmed, date_in_status, confirmed,
        delivery_method, delivery_package_module, delivery_package_nr, delivery_country_code,
        delivery_fullname, email, phone, admin_comments, extra_field_1, extra_field_2,
        order_page, pick_state, pack_state, raw_payload_json, source_hash, last_synced_at
    )
    VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, SYSUTCDATETIME()
    );
END
        """,
        [
            order_id,
            _normalize_int(order.get("shop_order_id")),
            _normalize_text(order.get("external_order_id")) or None,
            _normalize_text(order.get("order_source")) or "blconnect",
            _normalize_int(order.get("order_source_id")),
            _normalize_int(order.get("order_status_id")),
            _parse_epoch_datetime(order.get("date_add")),
            _parse_epoch_datetime(order.get("date_confirmed")),
            _parse_epoch_datetime(order.get("date_in_status")),
            1 if _normalize_bool(order.get("confirmed")) else 0,
            _normalize_text(order.get("delivery_method")) or None,
            _normalize_text(order.get("delivery_package_module")) or None,
            _normalize_text(order.get("delivery_package_nr")) or None,
            _normalize_text(order.get("delivery_country_code")) or None,
            _normalize_text(order.get("delivery_fullname")) or None,
            _normalize_text(order.get("email")) or None,
            _normalize_text(order.get("phone")) or None,
            _normalize_text(order.get("admin_comments")) or None,
            _normalize_text(order.get("extra_field_1")) or None,
            _normalize_text(order.get("extra_field_2")) or None,
            _normalize_text(order.get("order_page")) or None,
            _normalize_int(order.get("pick_state")),
            _normalize_int(order.get("pack_state")),
            payload_json,
            payload_hash,
            order_id,
            order_id,
            _normalize_int(order.get("shop_order_id")),
            _normalize_text(order.get("external_order_id")) or None,
            _normalize_text(order.get("order_source")) or "blconnect",
            _normalize_int(order.get("order_source_id")),
            _normalize_int(order.get("order_status_id")),
            _parse_epoch_datetime(order.get("date_add")),
            _parse_epoch_datetime(order.get("date_confirmed")),
            _parse_epoch_datetime(order.get("date_in_status")),
            1 if _normalize_bool(order.get("confirmed")) else 0,
            _normalize_text(order.get("delivery_method")) or None,
            _normalize_text(order.get("delivery_package_module")) or None,
            _normalize_text(order.get("delivery_package_nr")) or None,
            _normalize_text(order.get("delivery_country_code")) or None,
            _normalize_text(order.get("delivery_fullname")) or None,
            _normalize_text(order.get("email")) or None,
            _normalize_text(order.get("phone")) or None,
            _normalize_text(order.get("admin_comments")) or None,
            _normalize_text(order.get("extra_field_1")) or None,
            _normalize_text(order.get("extra_field_2")) or None,
            _normalize_text(order.get("order_page")) or None,
            _normalize_int(order.get("pick_state")),
            _normalize_int(order.get("pack_state")),
            payload_json,
            payload_hash,
        ],
    )


def _upsert_distribution_package(cur, *, order_id: int, package: dict[str, Any]) -> None:
    package_id = _normalize_int(package.get("package_id"))
    if package_id is None:
        return
    payload_json = json.dumps(package, ensure_ascii=False, sort_keys=True)
    payload_hash = _payload_hash(package)
    cur.execute(
        """
IF EXISTS (SELECT 1 FROM dbo.acc_bl_distribution_package_cache WITH (NOLOCK) WHERE package_id = ?)
BEGIN
    UPDATE dbo.acc_bl_distribution_package_cache
    SET order_id = ?,
        courier_package_nr = ?,
        courier_inner_number = ?,
        courier_code = ?,
        courier_other_name = ?,
        account_id = ?,
        tracking_status_date = ?,
        tracking_delivery_days = ?,
        tracking_status = ?,
        tracking_url = ?,
        is_return = ?,
        package_type = ?,
        raw_payload_json = ?,
        source_hash = ?,
        last_synced_at = SYSUTCDATETIME()
    WHERE package_id = ?;
END
ELSE
BEGIN
    INSERT INTO dbo.acc_bl_distribution_package_cache (
        package_id, order_id, courier_package_nr, courier_inner_number, courier_code,
        courier_other_name, account_id, tracking_status_date, tracking_delivery_days,
        tracking_status, tracking_url, is_return, package_type, raw_payload_json,
        source_hash, last_synced_at
    )
    VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, SYSUTCDATETIME()
    );
END
        """,
        [
            package_id,
            order_id,
            _normalize_text(package.get("courier_package_nr")) or None,
            _normalize_text(package.get("courier_inner_number")) or None,
            _normalize_text(package.get("courier_code")) or None,
            _normalize_text(package.get("courier_other_name")) or None,
            _normalize_text(package.get("account_id")) or None,
            _parse_epoch_datetime(package.get("tracking_status_date")),
            _normalize_int(package.get("tracking_delivery_days")),
            _normalize_text(package.get("tracking_status")) or None,
            _normalize_text(package.get("tracking_url")) or None,
            1 if _normalize_bool(package.get("is_return")) else 0,
            _normalize_text(package.get("package_type")) or None,
            payload_json,
            payload_hash,
            package_id,
            package_id,
            order_id,
            _normalize_text(package.get("courier_package_nr")) or None,
            _normalize_text(package.get("courier_inner_number")) or None,
            _normalize_text(package.get("courier_code")) or None,
            _normalize_text(package.get("courier_other_name")) or None,
            _normalize_text(package.get("account_id")) or None,
            _parse_epoch_datetime(package.get("tracking_status_date")),
            _normalize_int(package.get("tracking_delivery_days")),
            _normalize_text(package.get("tracking_status")) or None,
            _normalize_text(package.get("tracking_url")) or None,
            1 if _normalize_bool(package.get("is_return")) else 0,
            _normalize_text(package.get("package_type")) or None,
            payload_json,
            payload_hash,
        ],
    )


def _fetch_distribution_orders_page(
    *,
    source_id: int,
    cursor_ts: int,
) -> list[dict[str, Any]]:
    payload = _call_baselinker(
        "getOrders",
        {
            "date_confirmed_from": int(cursor_ts),
            "get_unconfirmed_orders": False,
            "filter_order_source": "blconnect",
            "filter_order_source_id": int(source_id),
        },
    )
    return list(payload.get("orders") or [])


def _fetch_order_packages(*, order_id: int) -> list[dict[str, Any]]:
    payload = _call_baselinker("getOrderPackages", {"order_id": int(order_id)})
    return list(payload.get("packages") or [])


def sync_bl_distribution_order_cache(
    *,
    date_confirmed_from: date | datetime | None = None,
    date_confirmed_to: date | datetime | None = None,
    source_ids: list[int] | None = None,
    tracking_numbers: list[str] | None = None,
    include_packages: bool = True,
    limit_orders: int | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_bl_distribution_cache_schema()
    if not settings.baselinker_distribution_enabled:
        raise RuntimeError("BaseLinker Distribution API not configured")

    from app.connectors.mssql.mssql_store import set_job_progress

    source_id_list = [int(item) for item in (source_ids or discover_distribution_source_ids())]
    if not source_id_list:
        return {
            "source_ids": [],
            "orders_synced": 0,
            "packages_synced": 0,
            "orders_with_delivery_package_nr": 0,
            "orders_with_external_order_id": 0,
            "api_calls": 0,
        }

    from_ts = _to_epoch(date_confirmed_from or (date.today() if isinstance(date_confirmed_from, date) else None))
    if from_ts is None:
        from_ts = _to_epoch(date.today()) - (2 * 86400)
    to_ts = _to_epoch(date_confirmed_to)
    max_orders = max(1, int(limit_orders or 0)) if limit_orders else None
    tracking_targets = {
        _normalize_tracking(item)
        for item in (tracking_numbers or [])
        if _normalize_tracking(item)
    }
    matched_tracking: set[str] = set()

    stats = {
        "source_ids": source_id_list,
        "orders_synced": 0,
        "packages_synced": 0,
        "orders_with_delivery_package_nr": 0,
        "orders_with_external_order_id": 0,
        "api_calls": 0,
        "tracking_targets": len(tracking_targets),
        "tracking_targets_matched": 0,
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        for source_idx, source_id in enumerate(source_id_list, start=1):
            cursor = SyncCursorState(source_id=source_id, cursor_ts=int(from_ts))
            seen_order_ids: set[int] = set()
            while True:
                if job_id:
                    progress_message = f"BaseLinker distribution source={source_id} cursor={cursor.cursor_ts}"
                    if tracking_targets:
                        progress_message += (
                            f" tracking_matched={stats['tracking_targets_matched']}/{len(tracking_targets)}"
                        )
                    set_job_progress(
                        job_id,
                        progress_pct=min(90, 5 + (source_idx - 1) * 10),
                        records_processed=int(stats["orders_synced"]),
                        message=progress_message,
                    )

                orders = _fetch_distribution_orders_page(source_id=source_id, cursor_ts=cursor.cursor_ts)
                stats["api_calls"] += 1
                if not orders:
                    break

                max_seen_ts = cursor.cursor_ts
                page_processed = 0
                for order in orders:
                    order_id = _normalize_int(order.get("order_id"))
                    if order_id is None or order_id in seen_order_ids:
                        continue
                    seen_order_ids.add(order_id)
                    order_ts = _normalize_int(order.get("date_confirmed")) or _normalize_int(order.get("date_add")) or cursor.cursor_ts
                    max_seen_ts = max(max_seen_ts, order_ts)
                    if to_ts is not None and order_ts > to_ts:
                        continue
                    delivery_package_nr = _normalize_tracking(order.get("delivery_package_nr"))
                    if tracking_targets and delivery_package_nr not in tracking_targets:
                        continue

                    _upsert_distribution_order(cur, order)
                    stats["orders_synced"] += 1
                    page_processed += 1
                    if delivery_package_nr:
                        stats["orders_with_delivery_package_nr"] += 1
                    if delivery_package_nr and delivery_package_nr in tracking_targets and delivery_package_nr not in matched_tracking:
                        matched_tracking.add(delivery_package_nr)
                        stats["tracking_targets_matched"] = len(matched_tracking)
                    if _normalize_text(order.get("external_order_id")):
                        stats["orders_with_external_order_id"] += 1

                    if include_packages:
                        packages = _fetch_order_packages(order_id=order_id)
                        stats["api_calls"] += 1
                        for package in packages:
                            _upsert_distribution_package(cur, order_id=order_id, package=package)
                            stats["packages_synced"] += 1
                        time.sleep(_package_call_sleep_sec())

                    if stats["orders_synced"] % _COMMIT_EVERY_ORDERS == 0:
                        conn.commit()
                        if job_id:
                            progress_message = (
                                f"BaseLinker distribution synced orders={stats['orders_synced']}, "
                                f"packages={stats['packages_synced']}, source={source_id}"
                            )
                            if tracking_targets:
                                progress_message += (
                                    f", tracking_matched={stats['tracking_targets_matched']}/{len(tracking_targets)}"
                                )
                            set_job_progress(
                                job_id,
                                progress_pct=min(95, 10 + int(stats["orders_synced"] / max(1, max_orders or 5000) * 80)),
                                records_processed=int(stats["orders_synced"]),
                                message=progress_message,
                            )

                    if max_orders and stats["orders_synced"] >= max_orders:
                        conn.commit()
                        return stats
                    if tracking_targets and len(matched_tracking) >= len(tracking_targets):
                        conn.commit()
                        return stats

                if len(orders) < _PAGE_SIZE:
                    break
                next_cursor = max_seen_ts + 1
                if to_ts is not None and next_cursor > to_ts:
                    break
                if next_cursor <= cursor.cursor_ts:
                    if page_processed == 0:
                        break
                    next_cursor = cursor.cursor_ts + 1
                cursor.cursor_ts = next_cursor
                time.sleep(_order_page_sleep_sec())

        conn.commit()
        if job_id:
            progress_message = (
                f"BaseLinker distribution cache synced orders={stats['orders_synced']}, "
                f"packages={stats['packages_synced']}"
            )
            if tracking_targets:
                progress_message += f", tracking_matched={stats['tracking_targets_matched']}/{len(tracking_targets)}"
            set_job_progress(
                job_id,
                progress_pct=95,
                records_processed=int(stats["orders_synced"]),
                message=progress_message,
            )
        return stats
    finally:
        conn.close()
