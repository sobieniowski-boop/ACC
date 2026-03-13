"""ACC Event Backbone — normalisation, dedup, persistence, processing hooks.

This is the **core** of the event-driven architecture.  Every SP-API
notification (from SQS polling, direct POST, or replay) flows through:

    raw JSON → ``ingest()`` → normalise → dedup → persist → dispatch

Tables managed (auto-created on startup via ``ensure_event_backbone_schema``):

* ``acc_notification_destination`` — registered SQS/EventBridge destinations
* ``acc_notification_subscription`` — active subscriptions per type
* ``acc_event_log``                — normalised & deduplicated event store
* ``acc_event_processing_log``     — per-handler processing audit trail
* ``acc_event_handler_health``     — per-handler circuit breaker state
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from app.core.config import settings
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Circuit breaker / timeout constants ────────────────────────────────────
HANDLER_TIMEOUT_SECONDS: int = 30
CIRCUIT_BREAKER_THRESHOLD: int = 5
CIRCUIT_BREAKER_COOLDOWN_MINUTES: int = 15

# ── Adaptive SQS polling constants ────────────────────────────────────────
MAX_POLL_LOOPS: int = 5

# ── SQS polling metrics (in-process counters, reset each app restart) ─────
_sqs_metrics: dict[str, int] = {
    "sqs_messages_received": 0,
    "sqs_poll_loops": 0,
    "sqs_empty_polls": 0,
}


# ═══════════════════════════════════════════════════════════════════════════
#  Schema DDL
# ═══════════════════════════════════════════════════════════════════════════

_SCHEMA_STATEMENTS: list[str] = [
    # ── Destinations ──
    """
    IF OBJECT_ID('dbo.acc_notification_destination', 'U') IS NULL
    CREATE TABLE dbo.acc_notification_destination (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        destination_id      VARCHAR(200)  NOT NULL UNIQUE,
        name                VARCHAR(200)  NOT NULL,
        destination_type    VARCHAR(30)   NOT NULL,          -- sqs | eventbridge
        arn                 VARCHAR(300)  NULL,
        account_id          VARCHAR(30)   NULL,
        region              VARCHAR(30)   NULL,
        status              VARCHAR(20)   NOT NULL DEFAULT 'active',
        raw_payload         NVARCHAR(MAX) NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,

    # ── Subscriptions ──
    """
    IF OBJECT_ID('dbo.acc_notification_subscription', 'U') IS NULL
    CREATE TABLE dbo.acc_notification_subscription (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        subscription_id     VARCHAR(200)  NOT NULL UNIQUE,
        notification_type   VARCHAR(100)  NOT NULL,
        destination_id      VARCHAR(200)  NOT NULL,
        event_domain        VARCHAR(50)   NOT NULL,           -- pricing, listing, order, inventory, report, feed
        payload_version     VARCHAR(10)   NOT NULL DEFAULT '1.0',
        status              VARCHAR(20)   NOT NULL DEFAULT 'active',
        raw_payload         NVARCHAR(MAX) NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_sub_type UNIQUE (notification_type)
    )
    """,

    # ── Event log (heart of the backbone) ──
    """
    IF OBJECT_ID('dbo.acc_event_log', 'U') IS NULL
    CREATE TABLE dbo.acc_event_log (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        event_id            VARCHAR(64)   NOT NULL UNIQUE,
        correlation_id      VARCHAR(64)   NOT NULL,
        notification_id     VARCHAR(200)  NULL,
        notification_type   VARCHAR(100)  NOT NULL,
        event_domain        VARCHAR(50)   NOT NULL,
        event_action        VARCHAR(80)   NOT NULL,
        marketplace_id      VARCHAR(20)   NULL,
        seller_id           VARCHAR(30)   NULL,
        asin                VARCHAR(20)   NULL,
        sku                 VARCHAR(100)  NULL,
        amazon_order_id     VARCHAR(50)   NULL,
        payload_raw         NVARCHAR(MAX) NOT NULL,
        payload_normalized  NVARCHAR(MAX) NULL,
        severity            VARCHAR(20)   NOT NULL DEFAULT 'info',
        status              VARCHAR(20)   NOT NULL DEFAULT 'received',
        source              VARCHAR(50)   NOT NULL DEFAULT 'direct',
        received_at         DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        processed_at        DATETIME2     NULL,
        retry_count         INT           NOT NULL DEFAULT 0,
        error_message       NVARCHAR(500) NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,

    # ── Indexes for event_log ──
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_event_log_type_received')
    CREATE INDEX ix_event_log_type_received
        ON dbo.acc_event_log (notification_type, received_at)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_event_log_domain_status')
    CREATE INDEX ix_event_log_domain_status
        ON dbo.acc_event_log (event_domain, status)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_event_log_asin')
    CREATE INDEX ix_event_log_asin
        ON dbo.acc_event_log (asin) WHERE asin IS NOT NULL
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_event_log_correlation')
    CREATE INDEX ix_event_log_correlation
        ON dbo.acc_event_log (correlation_id)
    """,

    # ── Processing log (per-handler audit trail) ──
    """
    IF OBJECT_ID('dbo.acc_event_processing_log', 'U') IS NULL
    CREATE TABLE dbo.acc_event_processing_log (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        event_id            VARCHAR(64)   NOT NULL,
        handler_name        VARCHAR(100)  NOT NULL,
        status              VARCHAR(20)   NOT NULL DEFAULT 'pending',
        started_at          DATETIME2     NULL,
        completed_at        DATETIME2     NULL,
        duration_ms         INT           NULL,
        retry_count         INT           NOT NULL DEFAULT 0,
        error_message       NVARCHAR(500) NULL,
        output_summary      NVARCHAR(MAX) NULL,
        handler_timeout     BIT           NOT NULL DEFAULT 0,
        circuit_open        BIT           NOT NULL DEFAULT 0,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_proc_log_event')
    CREATE INDEX ix_proc_log_event
        ON dbo.acc_event_processing_log (event_id)
    """,

    # ── Handler health / circuit breaker state ──
    """
    IF OBJECT_ID('dbo.acc_event_handler_health', 'U') IS NULL
    CREATE TABLE dbo.acc_event_handler_health (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        handler_name        VARCHAR(100)  NOT NULL UNIQUE,
        failure_count       INT           NOT NULL DEFAULT 0,
        last_failure_at     DATETIME2     NULL,
        circuit_open_until  DATETIME2     NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,

    # ── Backfill new columns (handler_timeout, circuit_open) onto existing table ──
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_event_processing_log')
          AND name = 'handler_timeout'
    )
    ALTER TABLE dbo.acc_event_processing_log
        ADD handler_timeout BIT NOT NULL DEFAULT 0
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_event_processing_log')
          AND name = 'circuit_open'
    )
    ALTER TABLE dbo.acc_event_processing_log
        ADD circuit_open BIT NOT NULL DEFAULT 0
    """,
]


def ensure_event_backbone_schema() -> None:
    """Create backbone tables if they don't exist (called on startup)."""
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
        log.info("event_backbone.schema_ensured")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Event ID generation (deterministic dedup key)
# ═══════════════════════════════════════════════════════════════════════════

def _make_event_id(notification_id: str | None, notification_type: str, event_time: str | None, payload_raw: str) -> str:
    """Deterministic SHA-256 event fingerprint for idempotent ingestion.

    If the notification has an Amazon-assigned ID we use that as the primary
    dedup key.  Otherwise we hash the full payload + type to catch re-sends.
    """
    if notification_id:
        seed = f"{notification_id}:{notification_type}"
    else:
        seed = f"{notification_type}:{event_time or ''}:{payload_raw[:2000]}"
    return hashlib.sha256(seed.encode()).hexdigest()


# ═══════════════════════════════════════════════════════════════════════════
#  Normalisation — raw SP-API notification → normalised event fields
# ═══════════════════════════════════════════════════════════════════════════

def _normalise_event(raw: dict) -> dict:
    """Extract normalised fields from a raw SP-API notification payload.

    Returns a dict with:
        notification_id, notification_type, event_domain, event_action,
        marketplace_id, seller_id, asin, sku, amazon_order_id,
        event_time, severity, payload_normalized
    """
    metadata = raw.get("NotificationMetadata") or {}
    notification_id = metadata.get("NotificationId")
    notification_type = raw.get("NotificationType", "UNKNOWN")
    event_time = raw.get("EventTime")
    payload = raw.get("Payload") or {}

    # Domain mapping
    from app.connectors.amazon_sp_api.notifications import SUPPORTED_NOTIFICATION_TYPES
    event_domain = SUPPORTED_NOTIFICATION_TYPES.get(notification_type, "unknown")

    # Extract common identifiers — each notification type has different shape
    result: dict[str, Any] = {
        "notification_id": notification_id,
        "notification_type": notification_type,
        "event_domain": event_domain,
        "event_action": _derive_action(notification_type, payload),
        "marketplace_id": None,
        "seller_id": None,
        "asin": None,
        "sku": None,
        "amazon_order_id": None,
        "event_time": event_time,
        "severity": "info",
        "payload_normalized": {},
    }

    # ── Type-specific normalisation ─────────────────────────────────────

    if notification_type == "ANY_OFFER_CHANGED":
        offer_change = payload.get("AnyOfferChangedNotification") or {}
        offer_change_trigger = offer_change.get("OfferChangeTrigger") or {}
        result["asin"] = offer_change_trigger.get("ASIN")
        result["marketplace_id"] = offer_change_trigger.get("MarketplaceId")
        result["seller_id"] = settings.SP_API_SELLER_ID

        # Check if our offer is still in BuyBox
        summary = offer_change.get("Summary") or {}
        buy_box = summary.get("BuyBoxPrices") or []
        result["severity"] = "info"
        result["payload_normalized"] = {
            "asin": result["asin"],
            "marketplace_id": result["marketplace_id"],
            "item_condition": offer_change_trigger.get("ItemCondition"),
            "time_of_offer_change": offer_change_trigger.get("TimeOfOfferChange"),
            "buy_box_prices": buy_box,
            "number_of_offers": summary.get("NumberOfOffers"),
        }

    elif notification_type == "LISTINGS_ITEM_STATUS_CHANGE":
        result["seller_id"] = payload.get("SellerId")
        result["marketplace_id"] = payload.get("MarketplaceId")
        result["asin"] = payload.get("Asin")
        result["sku"] = payload.get("Sku")
        status = payload.get("Status")
        result["severity"] = "warning" if status in ("DELETED", "SUPPRESSED") else "info"
        result["payload_normalized"] = {
            "asin": result["asin"],
            "sku": result["sku"],
            "marketplace_id": result["marketplace_id"],
            "status": status,
        }

    elif notification_type == "LISTINGS_ITEM_ISSUES_CHANGE":
        result["seller_id"] = payload.get("SellerId")
        result["marketplace_id"] = payload.get("MarketplaceId")
        result["asin"] = payload.get("Asin")
        result["sku"] = payload.get("Sku")
        severity_counts = payload.get("SeverityCounts") or {}
        has_error = severity_counts.get("ERROR", 0) > 0
        result["severity"] = "critical" if has_error else "warning"
        result["payload_normalized"] = {
            "asin": result["asin"],
            "sku": result["sku"],
            "marketplace_id": result["marketplace_id"],
            "severity_counts": severity_counts,
        }

    elif notification_type == "ORDER_STATUS_CHANGE":
        order_summary = payload.get("OrderStatusChangeNotification") or payload
        result["amazon_order_id"] = order_summary.get("AmazonOrderId")
        result["marketplace_id"] = order_summary.get("MarketplaceId")
        result["seller_id"] = order_summary.get("SellerId") or settings.SP_API_SELLER_ID
        order_status = order_summary.get("OrderStatus")
        result["severity"] = "info"
        result["payload_normalized"] = {
            "amazon_order_id": result["amazon_order_id"],
            "marketplace_id": result["marketplace_id"],
            "order_status": order_status,
        }

    elif notification_type == "FBA_INVENTORY_AVAILABILITY_CHANGES":
        inv = payload.get("FBAInventoryAvailabilityChanges") or payload
        result["seller_id"] = inv.get("SellerId") or settings.SP_API_SELLER_ID
        result["asin"] = inv.get("ASIN")
        result["sku"] = inv.get("SKU") or inv.get("SellerSKU")
        result["marketplace_id"] = inv.get("MarketplaceId")
        result["severity"] = "info"
        result["payload_normalized"] = {
            "asin": result["asin"],
            "sku": result["sku"],
            "fulfillable_quantity": inv.get("FulfillableQuantity"),
            "pending_quantity": inv.get("PendingTransshipmentQuantity"),
        }

    elif notification_type == "REPORT_PROCESSING_FINISHED":
        report_info = payload.get("reportProcessingFinishedNotification") or payload
        result["seller_id"] = report_info.get("sellerId") or settings.SP_API_SELLER_ID
        result["severity"] = "info"
        result["payload_normalized"] = {
            "report_id": report_info.get("reportId"),
            "report_type": report_info.get("reportType"),
            "processing_status": report_info.get("reportProcessingStatus"),
        }

    elif notification_type == "FEED_PROCESSING_FINISHED":
        feed_info = payload.get("feedProcessingFinishedNotification") or payload
        result["seller_id"] = feed_info.get("sellerId") or settings.SP_API_SELLER_ID
        result["severity"] = "info"
        result["payload_normalized"] = {
            "feed_id": feed_info.get("feedId"),
            "feed_type": feed_info.get("feedType"),
            "processing_status": feed_info.get("processingStatus"),
        }

    else:
        # Fallback: store what we can
        result["seller_id"] = payload.get("SellerId") or settings.SP_API_SELLER_ID
        result["payload_normalized"] = payload

    return result


def _derive_action(notification_type: str, payload: dict) -> str:
    """Derive a human-readable action verb from the notification type + payload."""
    action_map: dict[str, str] = {
        "ANY_OFFER_CHANGED": "offer_changed",
        "LISTINGS_ITEM_STATUS_CHANGE": "listing_status_changed",
        "LISTINGS_ITEM_ISSUES_CHANGE": "listing_issues_changed",
        "ORDER_STATUS_CHANGE": "order_status_changed",
        "FBA_INVENTORY_AVAILABILITY_CHANGES": "inventory_availability_changed",
        "REPORT_PROCESSING_FINISHED": "report_ready",
        "FEED_PROCESSING_FINISHED": "feed_ready",
        "ITEM_PRODUCT_TYPE_CHANGE": "product_type_changed",
        "BRANDED_ITEM_CONTENT_CHANGE": "content_changed",
    }
    return action_map.get(notification_type, notification_type.lower())


# ═══════════════════════════════════════════════════════════════════════════
#  Ingestion — the single entry point for all events
# ═══════════════════════════════════════════════════════════════════════════

def ingest(
    raw_payload: dict,
    *,
    source: str = "direct",
    correlation_id: str | None = None,
) -> dict:
    """Ingest a single raw SP-API notification.

    Steps:  normalise → compute event_id → dedup check → persist.
    Returns ``{"status": "created"|"duplicate"|"error", "event_id": ...}``.
    """
    correlation_id = correlation_id or uuid.uuid4().hex

    try:
        norm = _normalise_event(raw_payload)
    except Exception as exc:
        log.error("event_backbone.normalise_failed", error=str(exc))
        return {"status": "error", "event_id": None, "error": str(exc)}

    payload_raw_str = json.dumps(raw_payload, default=str, ensure_ascii=False)
    event_id = _make_event_id(
        norm["notification_id"],
        norm["notification_type"],
        norm.get("event_time"),
        payload_raw_str,
    )

    conn = connect_acc()
    try:
        cur = conn.cursor()

        # ── Dedup check ─────────────────────────────────────────────────
        cur.execute(
            "SELECT id FROM dbo.acc_event_log WITH (NOLOCK) WHERE event_id = ?",
            (event_id,),
        )
        if cur.fetchone():
            cur.close()
            log.debug("event_backbone.duplicate", event_id=event_id[:16])
            return {"status": "duplicate", "event_id": event_id}

        # ── Persist ─────────────────────────────────────────────────────
        normalized_json = json.dumps(norm.get("payload_normalized") or {}, default=str, ensure_ascii=False)

        cur.execute(
            """
            INSERT INTO dbo.acc_event_log (
                event_id, correlation_id, notification_id, notification_type,
                event_domain, event_action, marketplace_id, seller_id,
                asin, sku, amazon_order_id,
                payload_raw, payload_normalized, severity, status, source,
                received_at
            ) VALUES (?, ?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, 'received', ?,  SYSUTCDATETIME())
            """,
            (
                event_id,
                correlation_id,
                norm["notification_id"],
                norm["notification_type"],
                norm["event_domain"],
                norm["event_action"],
                norm.get("marketplace_id"),
                norm.get("seller_id"),
                norm.get("asin"),
                norm.get("sku"),
                norm.get("amazon_order_id"),
                payload_raw_str,
                normalized_json,
                norm.get("severity", "info"),
                source,
            ),
        )
        conn.commit()
        cur.close()

        log.info(
            "event_backbone.ingested",
            event_id=event_id[:16],
            type=norm["notification_type"],
            domain=norm["event_domain"],
            action=norm["event_action"],
            source=source,
        )
        return {"status": "created", "event_id": event_id}

    except Exception as exc:
        conn.rollback()
        log.error("event_backbone.ingest_failed", error=str(exc), event_id=event_id[:16])
        return {"status": "error", "event_id": event_id, "error": str(exc)}
    finally:
        conn.close()


def emit_domain_event(
    domain: str,
    action: str,
    payload: dict | None = None,
    *,
    correlation_id: str | None = None,
    idempotency_key: str | None = None,
    marketplace_id: str | None = None,
    seller_id: str | None = None,
    asin: str | None = None,
    sku: str | None = None,
    amazon_order_id: str | None = None,
    severity: str = "info",
) -> dict:
    """Emit an internal domain event into acc_event_log.

    This is the counterpart to ``ingest()`` for events originating inside
    the application (batch sync completions, state transitions, etc.)
    rather than from SQS / SP-API notifications.

    Parameters:
        domain:          e.g. "orders", "ads", "finance", "profitability"
        action:          e.g. "synced", "captured", "rollup_done"
        payload:         arbitrary data dict
        idempotency_key: if provided, used for deterministic dedup; otherwise
                         a unique event_id is generated per call.
    """
    correlation_id = correlation_id or uuid.uuid4().hex
    payload = payload or {}
    payload_str = json.dumps(payload, default=str, ensure_ascii=False)

    if idempotency_key:
        event_id = hashlib.sha256(
            f"domain:{domain}:{action}:{idempotency_key}".encode()
        ).hexdigest()
    else:
        event_id = hashlib.sha256(
            f"domain:{domain}:{action}:{uuid.uuid4().hex}".encode()
        ).hexdigest()

    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Dedup check
        cur.execute(
            "SELECT id FROM dbo.acc_event_log WITH (NOLOCK) WHERE event_id = ?",
            (event_id,),
        )
        if cur.fetchone():
            cur.close()
            log.debug("event_backbone.domain_event_duplicate", event_id=event_id[:16])
            return {"status": "duplicate", "event_id": event_id}

        cur.execute(
            """
            INSERT INTO dbo.acc_event_log (
                event_id, correlation_id, notification_id, notification_type,
                event_domain, event_action, marketplace_id, seller_id,
                asin, sku, amazon_order_id,
                payload_raw, payload_normalized, severity, status, source,
                received_at
            ) VALUES (?, ?, NULL, 'DOMAIN_EVENT',  ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, 'received', 'internal',  SYSUTCDATETIME())
            """,
            (
                event_id,
                correlation_id,
                domain,
                action,
                marketplace_id,
                seller_id,
                asin,
                sku,
                amazon_order_id,
                payload_str,
                payload_str,
                severity,
            ),
        )
        conn.commit()
        cur.close()

        log.info(
            "event_backbone.domain_event_emitted",
            event_id=event_id[:16],
            domain=domain,
            action=action,
        )
        return {"status": "created", "event_id": event_id}
    except Exception as exc:
        conn.rollback()
        log.error("event_backbone.domain_event_failed", error=str(exc), domain=domain, action=action)
        return {"status": "error", "event_id": event_id, "error": str(exc)}
    finally:
        conn.close()


def check_domain_events_today(domain: str, action: str, *, min_count: int = 1) -> bool:
    """Check whether at least *min_count* domain events of the given type
    were emitted today (UTC).  Used for event-driven dependency gates.
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_event_log WITH (NOLOCK)
            WHERE event_domain = ? AND event_action = ?
              AND source = 'internal'
              AND CAST(received_at AS DATE) = CAST(SYSUTCDATETIME() AS DATE)
            """,
            (domain, action),
        )
        count = cur.fetchone()[0]
        cur.close()
        return count >= min_count
    finally:
        conn.close()


def ingest_batch(
    payloads: list[dict],
    *,
    source: str = "direct",
    correlation_id: str | None = None,
) -> dict:
    """Ingest a batch of raw notifications.  Returns summary stats."""
    cid = correlation_id or uuid.uuid4().hex
    created = 0
    duplicates = 0
    errors = 0
    results = []

    for raw in payloads:
        r = ingest(raw, source=source, correlation_id=cid)
        results.append(r)
        if r["status"] == "created":
            created += 1
        elif r["status"] == "duplicate":
            duplicates += 1
        else:
            errors += 1

    return {
        "correlation_id": cid,
        "total": len(payloads),
        "created": created,
        "duplicates": duplicates,
        "errors": errors,
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Processing hooks — dispatch to domain handlers
# ═══════════════════════════════════════════════════════════════════════════

# Handler registry: maps (event_domain, event_action) → list of handler functions
# Handlers are registered by downstream modules calling ``register_handler()``.
_HANDLER_REGISTRY: dict[str, list[dict]] = {}


def register_handler(
    event_domain: str,
    event_action: str | None = None,
    *,
    handler_name: str,
    handler_fn: Any,
) -> None:
    """Register a processing handler for events of a given domain/action.

    Parameters:
        event_domain:  e.g. "pricing", "listing", "order", "inventory"
        event_action:  specific action or None for all actions in domain
        handler_name:  unique name for audit logging
        handler_fn:    callable(event_row: dict) → dict | None
    """
    key = f"{event_domain}:{event_action or '*'}"
    if key not in _HANDLER_REGISTRY:
        _HANDLER_REGISTRY[key] = []
    _HANDLER_REGISTRY[key].append({"name": handler_name, "fn": handler_fn})
    log.info("event_backbone.handler_registered", key=key, handler=handler_name)


def _get_handlers(event_domain: str, event_action: str) -> list[dict]:
    """Find all handlers matching domain + action (including wildcards)."""
    handlers = []
    # Exact match
    key_exact = f"{event_domain}:{event_action}"
    handlers.extend(_HANDLER_REGISTRY.get(key_exact, []))
    # Wildcard match
    key_wild = f"{event_domain}:*"
    handlers.extend(_HANDLER_REGISTRY.get(key_wild, []))
    return handlers


def process_pending_events(limit: int = 100) -> dict:
    """Process events in 'received' status through registered handlers.

    This is designed to be called from a scheduler job or manually.
    Each event is processed independently; failures don't block others.

    Circuit breaker: if a handler has >= CIRCUIT_BREAKER_THRESHOLD consecutive
    failures, its circuit opens for CIRCUIT_BREAKER_COOLDOWN_MINUTES.  While
    open the handler is *skipped* but the event stays in 'received' so it can
    be retried once the circuit closes.
    """
    conn = connect_acc()
    processed = 0
    failed = 0
    skipped = 0
    circuit_skipped = 0

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (?)
                event_id, notification_type, event_domain, event_action,
                marketplace_id, seller_id, asin, sku, amazon_order_id,
                payload_normalized, severity, retry_count
            FROM dbo.acc_event_log WITH (NOLOCK)
            WHERE status = 'received'
            ORDER BY received_at ASC
            """,
            (limit,),
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        events = [dict(zip(columns, row)) for row in rows]
        cur.close()

        # Pre-load circuit breaker state for all known handlers
        breaker_state = _load_breaker_state(conn)

        for event in events:
            eid = event["event_id"]
            handlers = _get_handlers(event["event_domain"], event["event_action"])

            if not handlers:
                # No handlers registered yet — mark as skipped (can replay later)
                _update_event_status(conn, eid, "skipped")
                skipped += 1
                continue

            all_ok = True
            any_circuit_open = False
            for h in handlers:
                hname = h["name"]
                if _is_circuit_open(breaker_state.get(hname)):
                    # Circuit open — log skip, do NOT mark event as processed
                    _log_circuit_skip(conn, eid, hname)
                    any_circuit_open = True
                    log.debug("event_backbone.circuit_open_skip", handler=hname, event_id=eid[:16])
                    continue

                ok = _run_handler(conn, eid, h, event)
                if ok:
                    _record_breaker_success(conn, hname)
                    # Refresh local cache
                    breaker_state[hname] = _get_breaker_row(conn, hname)
                else:
                    _record_breaker_failure(conn, hname)
                    breaker_state[hname] = _get_breaker_row(conn, hname)
                    all_ok = False

            if any_circuit_open and all_ok:
                # Some handlers were skipped due to open circuit — keep event
                # in 'received' so it can be retried once the circuit closes.
                circuit_skipped += 1
            elif all_ok:
                _update_event_status(conn, eid, "processed")
                processed += 1
            else:
                retry = event.get("retry_count", 0) + 1
                if retry >= 3:
                    _update_event_status(conn, eid, "failed", retry_count=retry)
                else:
                    _update_event_status(conn, eid, "received", retry_count=retry)
                failed += 1

        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error("event_backbone.process_failed", error=str(exc))
    finally:
        conn.close()

    return {
        "processed": processed,
        "failed": failed,
        "skipped": skipped,
        "circuit_skipped": circuit_skipped,
        "total": processed + failed + skipped + circuit_skipped,
    }


def _run_handler(conn: Any, event_id: str, handler: dict, event: dict) -> bool:
    """Run a single handler with timeout, record result in processing log.

    The handler is executed in a thread-pool with a ``HANDLER_TIMEOUT_SECONDS``
    deadline.  If the deadline is exceeded the attempt is recorded as a timeout
    failure.  Returns True on success.
    """
    import time
    handler_name = handler["name"]
    started = time.perf_counter()
    timed_out = False

    cur = conn.cursor()
    try:
        # Run handler with timeout via a thread-pool executor
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(handler["fn"], event)
            try:
                result = future.result(timeout=HANDLER_TIMEOUT_SECONDS)
            except concurrent.futures.TimeoutError:
                timed_out = True
                future.cancel()
                raise TimeoutError(
                    f"Handler '{handler_name}' exceeded {HANDLER_TIMEOUT_SECONDS}s timeout"
                )

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        summary = json.dumps(result, default=str, ensure_ascii=False)[:4000] if result else None
        cur.execute(
            """
            INSERT INTO dbo.acc_event_processing_log
                (event_id, handler_name, status, started_at, completed_at,
                 duration_ms, output_summary, handler_timeout, circuit_open)
            VALUES (?, ?, 'completed', SYSUTCDATETIME(), SYSUTCDATETIME(), ?, ?, 0, 0)
            """,
            (event_id, handler_name, elapsed_ms, summary),
        )
        cur.close()
        return True
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        error_msg = str(exc)[:500]
        cur.execute(
            """
            INSERT INTO dbo.acc_event_processing_log
                (event_id, handler_name, status, started_at, completed_at,
                 duration_ms, error_message, handler_timeout, circuit_open)
            VALUES (?, ?, 'failed', SYSUTCDATETIME(), SYSUTCDATETIME(), ?, ?, ?, 0)
            """,
            (event_id, handler_name, elapsed_ms, error_msg, 1 if timed_out else 0),
        )
        cur.close()
        log.warning(
            "event_backbone.handler_failed",
            handler=handler_name,
            event_id=event_id[:16],
            error=error_msg,
            timed_out=timed_out,
            duration_ms=elapsed_ms,
        )
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  Circuit breaker helpers
# ═══════════════════════════════════════════════════════════════════════════

def _load_breaker_state(conn: Any) -> dict[str, dict]:
    """Load all rows from acc_event_handler_health into a dict keyed by handler_name."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT handler_name, failure_count, last_failure_at, circuit_open_until
        FROM dbo.acc_event_handler_health WITH (NOLOCK)
        """
    )
    state: dict[str, dict] = {}
    for row in cur.fetchall():
        state[row[0]] = {
            "failure_count": row[1],
            "last_failure_at": row[2],
            "circuit_open_until": row[3],
        }
    cur.close()
    return state


def _get_breaker_row(conn: Any, handler_name: str) -> dict | None:
    """Reload a single handler's breaker row after mutation."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT failure_count, last_failure_at, circuit_open_until
        FROM dbo.acc_event_handler_health WITH (NOLOCK)
        WHERE handler_name = ?
        """,
        (handler_name,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return {
        "failure_count": row[0],
        "last_failure_at": row[1],
        "circuit_open_until": row[2],
    }


def _is_circuit_open(breaker_row: dict | None) -> bool:
    """Return True if the circuit is currently open for a handler."""
    if breaker_row is None:
        return False
    open_until = breaker_row.get("circuit_open_until")
    if open_until is None:
        return False
    now = datetime.now(timezone.utc)
    # open_until may be offset-naive (from SQL Server DATETIME2) — treat as UTC
    if open_until.tzinfo is None:
        open_until = open_until.replace(tzinfo=timezone.utc)
    return now < open_until


def _record_breaker_failure(conn: Any, handler_name: str) -> None:
    """Increment failure count; open the circuit if threshold reached."""
    cur = conn.cursor()
    # UPSERT via MERGE
    cur.execute(
        """
        MERGE dbo.acc_event_handler_health AS tgt
        USING (SELECT ? AS handler_name) AS src
            ON tgt.handler_name = src.handler_name
        WHEN MATCHED THEN
            UPDATE SET
                failure_count   = tgt.failure_count + 1,
                last_failure_at = SYSUTCDATETIME(),
                circuit_open_until = CASE
                    WHEN tgt.failure_count + 1 >= ?
                    THEN DATEADD(MINUTE, ?, SYSUTCDATETIME())
                    ELSE tgt.circuit_open_until
                END,
                updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (handler_name, failure_count, last_failure_at, circuit_open_until, created_at, updated_at)
            VALUES (src.handler_name, 1, SYSUTCDATETIME(), NULL, SYSUTCDATETIME(), SYSUTCDATETIME());
        """,
        (handler_name, CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_COOLDOWN_MINUTES),
    )
    cur.close()
    log.debug("event_backbone.breaker_failure_recorded", handler=handler_name)


def _record_breaker_success(conn: Any, handler_name: str) -> None:
    """Reset failure count on successful handler execution."""
    cur = conn.cursor()
    cur.execute(
        """
        MERGE dbo.acc_event_handler_health AS tgt
        USING (SELECT ? AS handler_name) AS src
            ON tgt.handler_name = src.handler_name
        WHEN MATCHED THEN
            UPDATE SET
                failure_count      = 0,
                circuit_open_until = NULL,
                updated_at         = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (handler_name, failure_count, last_failure_at, circuit_open_until, created_at, updated_at)
            VALUES (src.handler_name, 0, NULL, NULL, SYSUTCDATETIME(), SYSUTCDATETIME());
        """,
        (handler_name,),
    )
    cur.close()


def _log_circuit_skip(conn: Any, event_id: str, handler_name: str) -> None:
    """Write a processing-log entry noting the handler was skipped (circuit open)."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dbo.acc_event_processing_log
            (event_id, handler_name, status, started_at, completed_at,
             duration_ms, error_message, handler_timeout, circuit_open)
        VALUES (?, ?, 'skipped', SYSUTCDATETIME(), SYSUTCDATETIME(), 0,
                'Circuit breaker open — handler skipped', 0, 1)
        """,
        (event_id, handler_name),
    )
    cur.close()


def get_handler_health() -> list[dict]:
    """Return current circuit-breaker state for all registered handlers."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT handler_name, failure_count, last_failure_at,
                   circuit_open_until, updated_at
            FROM dbo.acc_event_handler_health WITH (NOLOCK)
            ORDER BY handler_name
            """
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()

        now = datetime.now(timezone.utc)
        for row in rows:
            ou = row.get("circuit_open_until")
            if ou and ou.tzinfo is None:
                ou = ou.replace(tzinfo=timezone.utc)
            row["circuit_open"] = bool(ou and now < ou)
        return rows
    finally:
        conn.close()


def _update_event_status(conn: Any, event_id: str, status: str, *, retry_count: int | None = None) -> None:
    """Update event status in acc_event_log."""
    cur = conn.cursor()
    if retry_count is not None:
        cur.execute(
            """
            UPDATE dbo.acc_event_log
            SET status = ?, retry_count = ?,
                processed_at = CASE WHEN ? IN ('processed', 'failed', 'skipped') THEN SYSUTCDATETIME() ELSE processed_at END
            WHERE event_id = ?
            """,
            (status, retry_count, status, event_id),
        )
    else:
        cur.execute(
            """
            UPDATE dbo.acc_event_log
            SET status = ?,
                processed_at = CASE WHEN ? IN ('processed', 'failed', 'skipped') THEN SYSUTCDATETIME() ELSE processed_at END
            WHERE event_id = ?
            """,
            (status, status, event_id),
        )
    cur.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Replay — re-process events by criteria
# ═══════════════════════════════════════════════════════════════════════════

def replay_events(
    *,
    event_ids: list[str] | None = None,
    event_domain: str | None = None,
    notification_type: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 500,
) -> dict:
    """Reset matching events to 'received' so they'll be re-processed.

    Returns count of events queued for replay.
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()
        conditions = ["1=1"]
        params: list[Any] = []

        if event_ids:
            placeholders = ",".join(["?"] * len(event_ids))
            conditions.append(f"event_id IN ({placeholders})")
            params.extend(event_ids)
        if event_domain:
            conditions.append("event_domain = ?")
            params.append(event_domain)
        if notification_type:
            conditions.append("notification_type = ?")
            params.append(notification_type)
        if since:
            conditions.append("received_at >= ?")
            params.append(since)
        if until:
            conditions.append("received_at <= ?")
            params.append(until)

        where = " AND ".join(conditions)
        cur.execute(
            f"""
            UPDATE TOP (?) dbo.acc_event_log
            SET status = 'received', retry_count = 0, processed_at = NULL, error_message = NULL
            WHERE {where} AND status IN ('processed', 'failed', 'skipped')
            """,
            (limit, *params),
        )
        count = cur.rowcount
        conn.commit()
        cur.close()

        log.info("event_backbone.replay_queued", count=count)
        return {"replayed": count}
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  SQS Polling (optional — requires boto3 + AWS credentials)
# ═══════════════════════════════════════════════════════════════════════════

def poll_sqs(max_messages: int = 10, *, max_loops: int = MAX_POLL_LOOPS, queue_url_override: str | None = None) -> dict:
    """Adaptively poll SQS queue for notifications, ingest each one.

    Polls up to *max_loops* times per cycle.  If a poll returns a full
    batch (``max_messages``), the next iteration fires immediately.
    Stops early when a poll yields fewer messages or zero.

    Deletes messages from SQS only after successful ingestion.

    If *queue_url_override* is given it is used instead of ``settings.SQS_QUEUE_URL``.
    """
    queue_url = queue_url_override or settings.SQS_QUEUE_URL
    if not queue_url:
        return {"status": "disabled", "reason": "SQS_QUEUE_URL not configured"}

    try:
        import boto3
    except ImportError:
        return {"status": "error", "reason": "boto3 not installed"}

    session_kwargs: dict[str, str] = {"region_name": settings.SQS_REGION}
    if settings.AWS_ACCESS_KEY_ID:
        session_kwargs["aws_access_key_id"] = settings.AWS_ACCESS_KEY_ID
        session_kwargs["aws_secret_access_key"] = settings.AWS_SECRET_ACCESS_KEY

    sqs = boto3.client("sqs", **session_kwargs)
    correlation_id = uuid.uuid4().hex
    effective_max = min(max_messages, 10)  # SQS hard cap

    total_received = 0
    total_created = 0
    total_duplicates = 0
    total_errors = 0
    loops_executed = 0
    empty_polls = 0

    for loop_idx in range(max_loops):
        loops_executed += 1
        _sqs_metrics["sqs_poll_loops"] += 1

        try:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=effective_max,
                WaitTimeSeconds=5,
                AttributeNames=["All"],
            )
        except Exception as exc:
            log.error("event_backbone.sqs_poll_failed", error=str(exc), loop=loop_idx)
            return {
                "status": "error",
                "reason": str(exc),
                "loops": loops_executed,
                "received": total_received,
            }

        messages = resp.get("Messages", [])
        batch_size = len(messages)
        total_received += batch_size
        _sqs_metrics["sqs_messages_received"] += batch_size

        if batch_size == 0:
            empty_polls += 1
            _sqs_metrics["sqs_empty_polls"] += 1
            break

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                # SNS-wrapped messages have an inner "Message" field
                if "Message" in body and "TopicArn" in body:
                    body = json.loads(body["Message"])
            except (json.JSONDecodeError, KeyError):
                log.warning("event_backbone.sqs_bad_message", message_id=msg.get("MessageId"))
                total_errors += 1
                continue

            result = ingest(body, source="sqs", correlation_id=correlation_id)

            if result["status"] == "created":
                total_created += 1
            elif result["status"] == "duplicate":
                total_duplicates += 1
            else:
                total_errors += 1
                continue  # don't delete from SQS on error — will retry

            # Delete from SQS only on success or duplicate
            try:
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg["ReceiptHandle"],
                )
            except Exception as exc:
                log.warning("event_backbone.sqs_delete_failed", error=str(exc))

        # Adaptive: if batch was full, there may be more — continue polling
        if batch_size < effective_max:
            break

    log.info(
        "event_backbone.sqs_poll_done",
        total=total_received,
        created=total_created,
        duplicates=total_duplicates,
        errors=total_errors,
        loops=loops_executed,
        empty_polls=empty_polls,
    )
    return {
        "status": "ok",
        "correlation_id": correlation_id,
        "received": total_received,
        "created": total_created,
        "duplicates": total_duplicates,
        "errors": total_errors,
        "loops": loops_executed,
        "empty_polls": empty_polls,
        "max_loops": max_loops,
    }


def get_sqs_metrics() -> dict[str, int]:
    """Return in-process SQS polling metrics (since last app restart)."""
    return dict(_sqs_metrics)


# ═══════════════════════════════════════════════════════════════════════════
#  Query helpers (for the API layer)
# ═══════════════════════════════════════════════════════════════════════════

def get_event_log(
    *,
    event_domain: str | None = None,
    notification_type: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    asin: str | None = None,
    sku: str | None = None,
    since: str | None = None,
    until: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """Query event log with filters + pagination."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        conditions = ["1=1"]
        params: list[Any] = []

        if event_domain:
            conditions.append("event_domain = ?")
            params.append(event_domain)
        if notification_type:
            conditions.append("notification_type = ?")
            params.append(notification_type)
        if status:
            conditions.append("status = ?")
            params.append(status)
        if severity:
            conditions.append("severity = ?")
            params.append(severity)
        if asin:
            conditions.append("asin = ?")
            params.append(asin)
        if sku:
            conditions.append("sku = ?")
            params.append(sku)
        if since:
            conditions.append("received_at >= ?")
            params.append(since)
        if until:
            conditions.append("received_at <= ?")
            params.append(until)

        where = " AND ".join(conditions)

        # Count
        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_event_log WITH (NOLOCK) WHERE {where}", params)
        total = cur.fetchone()[0]

        # Page
        offset = (page - 1) * page_size
        cur.execute(
            f"""
            SELECT
                event_id, correlation_id, notification_id, notification_type,
                event_domain, event_action, marketplace_id, seller_id,
                asin, sku, amazon_order_id,
                severity, status, source, received_at, processed_at,
                retry_count, error_message
            FROM dbo.acc_event_log WITH (NOLOCK)
            WHERE {where}
            ORDER BY received_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            (*params, offset, page_size),
        )
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
        cur.close()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max(1, (total + page_size - 1) // page_size),
            "items": rows,
        }
    finally:
        conn.close()


def get_event_detail(event_id: str) -> dict | None:
    """Get full event including raw + normalised payload."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM dbo.acc_event_log WITH (NOLOCK)
            WHERE event_id = ?
            """,
            (event_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        columns = [d[0] for d in cur.description]
        event = dict(zip(columns, row))

        # Also fetch processing log
        cur.execute(
            """
            SELECT handler_name, status, started_at, completed_at,
                   duration_ms, retry_count, error_message, output_summary
            FROM dbo.acc_event_processing_log WITH (NOLOCK)
            WHERE event_id = ?
            ORDER BY created_at ASC
            """,
            (event_id,),
        )
        proc_cols = [d[0] for d in cur.description]
        event["processing_log"] = [dict(zip(proc_cols, r)) for r in cur.fetchall()]
        cur.close()

        return event
    finally:
        conn.close()


def get_backbone_health() -> dict:
    """Quick health summary of the event backbone."""
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Event counts by status
        cur.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM dbo.acc_event_log WITH (NOLOCK)
            GROUP BY status
            """
        )
        status_counts = {r[0]: r[1] for r in cur.fetchall()}

        # Events in last hour
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_event_log WITH (NOLOCK)
            WHERE received_at >= DATEADD(HOUR, -1, SYSUTCDATETIME())
            """
        )
        last_hour = cur.fetchone()[0]

        # Latest event
        cur.execute(
            """
            SELECT TOP 1 notification_type, event_domain, received_at
            FROM dbo.acc_event_log WITH (NOLOCK)
            ORDER BY received_at DESC
            """
        )
        latest = cur.fetchone()
        latest_event = None
        if latest:
            latest_event = {
                "notification_type": latest[0],
                "event_domain": latest[1],
                "received_at": str(latest[2]) if latest[2] else None,
            }

        # Active subscriptions
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_notification_subscription WITH (NOLOCK)
            WHERE status = 'active'
            """
        )
        active_subs = cur.fetchone()[0]

        # Destinations
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_notification_destination WITH (NOLOCK)
            WHERE status = 'active'
            """
        )
        active_dests = cur.fetchone()[0]

        cur.close()

        total_events = sum(status_counts.values())
        failed = status_counts.get("failed", 0)
        health_status = "healthy"
        if failed > 0:
            health_status = "degraded" if failed / max(total_events, 1) < 0.1 else "critical"

        # Circuit breaker summary
        handler_health = get_handler_health()
        open_circuits = [h["handler_name"] for h in handler_health if h.get("circuit_open")]

        return {
            "status": health_status,
            "total_events": total_events,
            "events_last_hour": last_hour,
            "status_counts": status_counts,
            "active_subscriptions": active_subs,
            "active_destinations": active_dests,
            "latest_event": latest_event,
            "sqs_configured": bool(settings.SQS_QUEUE_URL),
            "sqs_metrics": get_sqs_metrics(),
            "sqs_max_poll_loops": MAX_POLL_LOOPS,
            "registered_handlers": len(_HANDLER_REGISTRY),
            "handler_timeout_seconds": HANDLER_TIMEOUT_SECONDS,
            "circuit_breaker_threshold": CIRCUIT_BREAKER_THRESHOLD,
            "circuit_breaker_cooldown_min": CIRCUIT_BREAKER_COOLDOWN_MINUTES,
            "open_circuits": open_circuits,
            "handler_health": handler_health,
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Destination / Subscription persistence
# ═══════════════════════════════════════════════════════════════════════════

def persist_destination(dest: dict) -> None:
    """Save a destination returned by SP-API to the local DB."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        dest_id = dest.get("destinationId", "")
        name = dest.get("name", "")
        resource = dest.get("resource") or {}
        sqs = resource.get("sqs") or {}
        eb = resource.get("eventBridge") or {}
        dest_type = "sqs" if sqs else "eventbridge"

        cur.execute(
            """
            MERGE dbo.acc_notification_destination AS t
            USING (SELECT ? AS destination_id) AS s
            ON t.destination_id = s.destination_id
            WHEN MATCHED THEN
                UPDATE SET name = ?, status = 'active', raw_payload = ?, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (destination_id, name, destination_type, arn, account_id, region, status, raw_payload)
                VALUES (?, ?, ?, ?, ?, ?, 'active', ?);
            """,
            (
                dest_id,
                name, json.dumps(dest, default=str),
                dest_id, name, dest_type,
                sqs.get("arn"),
                eb.get("accountId"),
                eb.get("region"),
                json.dumps(dest, default=str),
            ),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def persist_subscription(sub: dict, notification_type: str, event_domain: str) -> None:
    """Save a subscription returned by SP-API to the local DB."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        sub_id = sub.get("subscriptionId", "")
        dest_id = sub.get("destinationId", "")
        version = sub.get("payloadVersion", "1.0")

        cur.execute(
            """
            MERGE dbo.acc_notification_subscription AS t
            USING (SELECT ? AS notification_type) AS s
            ON t.notification_type = s.notification_type
            WHEN MATCHED THEN
                UPDATE SET subscription_id = ?, destination_id = ?, event_domain = ?,
                           payload_version = ?, status = 'active', raw_payload = ?, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (subscription_id, notification_type, destination_id, event_domain, payload_version, status, raw_payload)
                VALUES (?, ?, ?, ?, ?, 'active', ?);
            """,
            (
                notification_type,
                sub_id, dest_id, event_domain, version, json.dumps(sub, default=str),
                sub_id, notification_type, dest_id, event_domain, version, json.dumps(sub, default=str),
            ),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def remove_subscription(notification_type: str) -> None:
    """Mark a subscription as deleted in local DB."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE dbo.acc_notification_subscription SET status = 'deleted', updated_at = SYSUTCDATETIME() WHERE notification_type = ?",
            (notification_type,),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def remove_destination(destination_id: str) -> None:
    """Mark a destination as deleted in local DB."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE dbo.acc_notification_destination SET status = 'deleted', updated_at = SYSUTCDATETIME() WHERE destination_id = ?",
            (destination_id,),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
