"""Unit tests for event_backbone pure helpers.

Tests the deterministic, side-effect-free functions in event_backbone:
  - _make_event_id: deterministic SHA-256 event fingerprint
  - _derive_action: notification type → action verb mapping
  - _is_circuit_open: breaker row → bool
  - _normalise_event: raw SP-API payload → normalised fields

Sprint 8 – S8.5
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from app.services.event_backbone import (
    _make_event_id,
    _derive_action,
    _is_circuit_open,
    _normalise_event,
    _get_handlers,
    _HANDLER_REGISTRY,
    register_handler,
)


# ═══════════════════════════════════════════════════════════════════════════
#  _make_event_id
# ═══════════════════════════════════════════════════════════════════════════

class TestMakeEventId:
    def test_deterministic_with_notification_id(self):
        eid1 = _make_event_id("NID-123", "ANY_OFFER_CHANGED", "2025-01-01T00:00:00Z", "{}")
        eid2 = _make_event_id("NID-123", "ANY_OFFER_CHANGED", "2025-01-01T00:00:00Z", "{}")
        assert eid1 == eid2

    def test_different_notification_id_different_hash(self):
        eid1 = _make_event_id("NID-123", "ANY_OFFER_CHANGED", None, "{}")
        eid2 = _make_event_id("NID-999", "ANY_OFFER_CHANGED", None, "{}")
        assert eid1 != eid2

    def test_notification_id_ignores_payload(self):
        """When notification_id is present, payload doesn't affect the hash."""
        eid1 = _make_event_id("NID-1", "TYPE", None, '{"a":1}')
        eid2 = _make_event_id("NID-1", "TYPE", None, '{"b":2}')
        assert eid1 == eid2

    def test_no_notification_id_uses_payload(self):
        eid1 = _make_event_id(None, "TYPE", "T1", '{"a":1}')
        eid2 = _make_event_id(None, "TYPE", "T1", '{"b":2}')
        assert eid1 != eid2

    def test_returns_hex_string(self):
        eid = _make_event_id("X", "T", None, "{}")
        assert len(eid) == 64
        assert all(c in "0123456789abcdef" for c in eid)


# ═══════════════════════════════════════════════════════════════════════════
#  _derive_action
# ═══════════════════════════════════════════════════════════════════════════

class TestDeriveAction:
    def test_known_types_mapped(self):
        assert _derive_action("ANY_OFFER_CHANGED", {}) == "offer_changed"
        assert _derive_action("ORDER_STATUS_CHANGE", {}) == "order_status_changed"
        assert _derive_action("REPORT_PROCESSING_FINISHED", {}) == "report_ready"
        assert _derive_action("FEED_PROCESSING_FINISHED", {}) == "feed_ready"

    def test_unknown_type_lowercased(self):
        assert _derive_action("SOME_NEW_TYPE", {}) == "some_new_type"


# ═══════════════════════════════════════════════════════════════════════════
#  _is_circuit_open
# ═══════════════════════════════════════════════════════════════════════════

class TestIsCircuitOpen:
    def test_none_row_returns_false(self):
        assert _is_circuit_open(None) is False

    def test_no_open_until_returns_false(self):
        assert _is_circuit_open({"circuit_open_until": None}) is False

    def test_future_open_until_returns_true(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        assert _is_circuit_open({"circuit_open_until": future}) is True

    def test_past_open_until_returns_false(self):
        past = datetime.now(timezone.utc) - timedelta(minutes=10)
        assert _is_circuit_open({"circuit_open_until": past}) is False

    def test_naive_datetime_treated_as_utc(self):
        """DATETIME2 from SQL Server may be timezone-naive."""
        future_naive = datetime.utcnow() + timedelta(minutes=10)
        assert _is_circuit_open({"circuit_open_until": future_naive}) is True


# ═══════════════════════════════════════════════════════════════════════════
#  _normalise_event
# ═══════════════════════════════════════════════════════════════════════════

class TestNormaliseEvent:
    def _make_raw(self, notification_type: str, payload: dict | None = None) -> dict:
        return {
            "NotificationType": notification_type,
            "EventTime": "2025-01-15T12:00:00Z",
            "NotificationMetadata": {"NotificationId": "NID-TEST"},
            "Payload": payload or {},
        }

    @patch("app.connectors.amazon_sp_api.notifications.SUPPORTED_NOTIFICATION_TYPES",
           {"ANY_OFFER_CHANGED": "pricing", "ORDER_STATUS_CHANGE": "order", "UNKNOWN": "unknown"})
    def test_any_offer_changed(self):
        raw = self._make_raw("ANY_OFFER_CHANGED", {
            "AnyOfferChangedNotification": {
                "OfferChangeTrigger": {
                    "ASIN": "B0TEST",
                    "MarketplaceId": "A1PA",
                    "ItemCondition": "New",
                    "TimeOfOfferChange": "2025-01-15T12:00:00Z",
                },
                "Summary": {
                    "BuyBoxPrices": [{"price": 9.99}],
                    "NumberOfOffers": 5,
                },
            }
        })
        norm = _normalise_event(raw)
        assert norm["notification_type"] == "ANY_OFFER_CHANGED"
        assert norm["event_domain"] == "pricing"
        assert norm["event_action"] == "offer_changed"
        assert norm["asin"] == "B0TEST"
        assert norm["marketplace_id"] == "A1PA"

    @patch("app.connectors.amazon_sp_api.notifications.SUPPORTED_NOTIFICATION_TYPES",
           {"ORDER_STATUS_CHANGE": "order"})
    def test_order_status_change(self):
        raw = self._make_raw("ORDER_STATUS_CHANGE", {
            "OrderStatusChangeNotification": {
                "AmazonOrderId": "408-123-456",
                "MarketplaceId": "A1PA",
                "OrderStatus": "Shipped",
            }
        })
        norm = _normalise_event(raw)
        assert norm["amazon_order_id"] == "408-123-456"
        assert norm["event_action"] == "order_status_changed"

    @patch("app.connectors.amazon_sp_api.notifications.SUPPORTED_NOTIFICATION_TYPES", {})
    def test_unknown_type_fallback(self):
        raw = self._make_raw("BRAND_NEW_TYPE", {"SellerId": "SEL-1"})
        norm = _normalise_event(raw)
        assert norm["event_domain"] == "unknown"
        assert norm["notification_type"] == "BRAND_NEW_TYPE"


# ═══════════════════════════════════════════════════════════════════════════
#  Handler registry
# ═══════════════════════════════════════════════════════════════════════════

class TestHandlerRegistry:
    def setup_method(self):
        self._saved = dict(_HANDLER_REGISTRY)
        _HANDLER_REGISTRY.clear()

    def teardown_method(self):
        _HANDLER_REGISTRY.clear()
        _HANDLER_REGISTRY.update(self._saved)

    def test_register_and_get_exact(self):
        fn = lambda e: None
        register_handler("pricing", "offer_changed", handler_name="test_h", handler_fn=fn)
        handlers = _get_handlers("pricing", "offer_changed")
        assert len(handlers) == 1
        assert handlers[0]["name"] == "test_h"

    def test_wildcard_handler(self):
        fn = lambda e: None
        register_handler("pricing", None, handler_name="wild_h", handler_fn=fn)
        handlers = _get_handlers("pricing", "offer_changed")
        assert any(h["name"] == "wild_h" for h in handlers)

    def test_no_match_returns_empty(self):
        assert _get_handlers("nonexistent", "action") == []
