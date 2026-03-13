"""Unit tests for Buy Box Radar intelligence module.

Tests the helper functions, offer recording logic, trend computation
patterns, and loss detection thresholds.

Sprint 11 – S11.7
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from app.intelligence.buybox_radar import (
    _extract_amount,
    _safe_float,
    _safe_int,
    SUSTAINED_LOSS_THRESHOLD_DAYS,
    record_competitor_offers,
    get_competitor_landscape,
    get_buybox_trends,
    get_rolling_win_rates,
    get_buybox_dashboard,
    detect_sustained_buybox_losses,
    raise_sustained_loss_alerts,
    get_buybox_alerts,
)


# ═══════════════════════════════════════════════════════════════════════════
#  Helper function tests (pure, no DB)
# ═══════════════════════════════════════════════════════════════════════════


class TestExtractAmount:
    def test_sp_api_nested_dict(self):
        offer = {"ListingPrice": {"Amount": "19.99", "CurrencyCode": "EUR"}}
        assert _extract_amount(offer, "ListingPrice", "listing_price") == 19.99

    def test_sp_api_nested_amount_key(self):
        offer = {"ListingPrice": {"amount": 25.0}}
        assert _extract_amount(offer, "ListingPrice", "listing_price") == 25.0

    def test_flat_key_fallback(self):
        offer = {"listing_price": 12.50}
        assert _extract_amount(offer, "ListingPrice", "listing_price") == 12.50

    def test_none_when_missing(self):
        assert _extract_amount({}, "ListingPrice", "listing_price") is None

    def test_nested_dict_with_none_amount(self):
        offer = {"ListingPrice": {"Amount": None}}
        assert _extract_amount(offer, "ListingPrice", "listing_price") is None


class TestSafeFloat:
    def test_valid_string(self):
        assert _safe_float("3.14") == 3.14

    def test_int_input(self):
        assert _safe_float(5) == 5.0

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_invalid_returns_none(self):
        assert _safe_float("abc") is None


class TestSafeInt:
    def test_valid_string(self):
        assert _safe_int("42") == 42

    def test_float_input(self):
        assert _safe_int(3.9) == 3

    def test_none_returns_none(self):
        assert _safe_int(None) is None

    def test_invalid_returns_none(self):
        assert _safe_int("abc") is None


class TestSustainedLossThreshold:
    def test_default_threshold_is_three(self):
        assert SUSTAINED_LOSS_THRESHOLD_DAYS == 3


# ═══════════════════════════════════════════════════════════════════════════
#  Mock-based tests for DB functions
# ═══════════════════════════════════════════════════════════════════════════


class _FakeConn:
    """Minimal DB connection mock."""

    def __init__(self, rows=None):
        self.cursor_obj = _FakeCursor(rows)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class _FakeCursor:
    """Minimal cursor mock."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.executed = []
        self._fetchall_called = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        self._fetchall_called = True
        return self.rows


class TestRecordCompetitorOffers:
    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_empty_offers_returns_zero(self, mock_conn):
        assert record_competitor_offers("B01ASIN", "AEFP", []) == 0
        mock_conn.assert_not_called()

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_two_offers_inserted(self, mock_conn):
        conn = _FakeConn()
        mock_conn.return_value = conn
        offers = [
            {
                "SellerId": "SELLER1",
                "ListingPrice": {"Amount": "19.99"},
                "Shipping": {"Amount": "3.99"},
                "IsBuyBoxWinner": True,
                "IsFulfilledByAmazon": True,
                "SubCondition": "New",
                "SellerFeedbackRating": {
                    "SellerPositiveFeedbackRating": 98.5,
                    "FeedbackCount": 1200,
                },
            },
            {
                "SellerId": "SELLER2",
                "ListingPrice": {"Amount": "21.00"},
                "Shipping": {"Amount": "0"},
                "IsBuyBoxWinner": False,
                "IsFulfilledByAmazon": False,
                "SubCondition": "New",
            },
        ]
        result = record_competitor_offers(
            "B01ASIN", "AEFP", offers, our_seller_id="SELLER1",
        )
        assert result == 2
        assert conn.committed
        assert conn.closed
        # First insert should have is_our_offer=1 (SELLER1)
        first_params = conn.cursor_obj.executed[0][1]
        assert first_params[3] == 1  # is_our_offer
        # Second insert: is_our_offer=0
        second_params = conn.cursor_obj.executed[1][1]
        assert second_params[3] == 0

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_flat_keys_accepted(self, mock_conn):
        conn = _FakeConn()
        mock_conn.return_value = conn
        offers = [
            {
                "seller_id": "S1",
                "listing_price": 15.0,
                "shipping_price": 2.5,
                "is_buybox_winner": True,
                "is_fba": True,
                "condition_type": "New",
            },
        ]
        result = record_competitor_offers("B02ASIN", "AEFP", offers)
        assert result == 1
        params = conn.cursor_obj.executed[0][1]
        # params = (asin, marketplace_id, seller_id, is_our_offer,
        #   listing_price, shipping_price, landed, currency,
        #   is_buybox_winner, is_fba, condition_type,
        #   seller_feedback_rating, seller_feedback_count)
        assert params[0] == "B02ASIN"
        assert params[1] == "AEFP"
        assert params[2] == "S1"
        assert params[4] == 15.0   # listing_price
        assert params[5] == 2.5    # shipping_price
        assert params[6] == 17.5   # landed_price = 15.0 + 2.5


class TestGetCompetitorLandscape:
    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_empty_returns_zero_sellers(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_competitor_landscape("B01ASIN", "AEFP")
        assert result["total_sellers"] == 0
        assert result["sellers"] == []

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_landscape_with_sellers(self, mock_conn):
        now = datetime(2026, 3, 12, 10, 0, 0)
        rows = [
            # seller_id, is_our_offer, listing_price, shipping_price,
            # landed_price, is_buybox_winner, is_fba, condition_type,
            # seller_feedback_rating, seller_feedback_count, observed_at
            ("S1", 1, 19.99, 0.00, 19.99, 1, 1, "New", 98.5, 1200, now),
            ("S2", 0, 22.50, 3.00, 25.50, 0, 0, "New", 85.0, 300, now),
        ]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn
        result = get_competitor_landscape("B01ASIN", "AEFP")
        assert result["total_sellers"] == 2
        assert result["fba_sellers"] == 1
        assert result["fbm_sellers"] == 1
        assert result["our_position"] == 1
        assert result["buybox_winner"]["seller_id"] == "S1"
        assert result["price_stats"]["min"] == 19.99
        assert result["price_stats"]["max"] == 25.50


class TestGetBuyboxTrends:
    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_returns_trend_rows(self, mock_conn):
        rows = [
            (date(2026, 3, 10), 10, 8, 80.0, 19.99, 19.50, -2.45, 5, 18.00),
            (date(2026, 3, 11), 12, 10, 83.33, 20.10, 19.80, -1.50, 4, 18.50),
        ]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn
        result = get_buybox_trends("SKU1", "AEFP", days=7)
        assert len(result) == 2
        assert result[0]["trend_date"] == "2026-03-10"
        assert result[0]["win_rate"] == 80.0
        assert result[1]["snapshots_won"] == 10


class TestGetRollingWinRates:
    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_returns_three_windows(self, mock_conn):
        conn = _FakeConn(rows=[(85.5, 78.2, 72.0)])
        mock_conn.return_value = conn
        result = get_rolling_win_rates("SKU1", "AEFP")
        assert result["win_rate_7d"] == 85.5
        assert result["win_rate_30d"] == 78.2
        assert result["win_rate_90d"] == 72.0

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_no_data_returns_none(self, mock_conn):
        conn = _FakeConn(rows=[(None, None, None)])
        mock_conn.return_value = conn
        result = get_rolling_win_rates("SKU-NONE", "AEFP")
        assert result["win_rate_7d"] is None
        assert result["win_rate_30d"] is None
        assert result["win_rate_90d"] is None


class TestGetBuyboxDashboard:
    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_empty_dashboard(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_buybox_dashboard()
        assert result["total_skus"] == 0
        assert result["overall_win_rate"] == 0
        assert result["trend_direction"] == "stable"

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_dashboard_with_data(self, mock_conn):
        sku_rows = [
            ("SKU1", "AEFP", "B01", 90.0, 100, 90),
            ("SKU2", "AEFP", "B02", 20.0, 100, 20),
            ("SKU3", "AEFP", "B03", 55.0, 100, 55),
        ]
        trend_row = [(60.0, 62.0)]  # first_half, second_half → stable

        conn = _FakeConn(rows=sku_rows)
        mock_conn.return_value = conn

        # Patch _compute_trend_direction since it makes a second query
        with patch("app.intelligence.buybox_radar._compute_trend_direction", return_value="stable"):
            result = get_buybox_dashboard(marketplace_id="AEFP", days=7)

        assert result["total_skus"] == 3
        assert result["winning_skus"] == 1      # SKU1 ≥ 70
        assert result["losing_skus"] == 1        # SKU2 < 30
        assert result["at_risk_skus"] == 1       # SKU3 30-70
        assert result["overall_win_rate"] == 55.0  # (90+20+55)/3


class TestDetectSustainedLosses:
    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_returns_loss_records(self, mock_conn):
        rows = [
            ("SKU-BAD", "AEFP", "B01BAD", 5, date(2026, 3, 7), date(2026, 3, 11), 1.5),
        ]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn
        result = detect_sustained_buybox_losses("AEFP")
        assert len(result) == 1
        assert result[0]["seller_sku"] == "SKU-BAD"
        assert result[0]["consecutive_loss_days"] == 5
        assert result[0]["loss_start"] == "2026-03-07"

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_no_losses(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = detect_sustained_buybox_losses()
        assert result == []


class TestRaiseSustainedLossAlerts:
    @patch("app.intelligence.buybox_radar.detect_sustained_buybox_losses")
    def test_no_losses_no_alerts(self, mock_detect):
        mock_detect.return_value = []
        assert raise_sustained_loss_alerts() == 0

    @patch("app.intelligence.buybox_radar.connect_acc")
    @patch("app.intelligence.buybox_radar.detect_sustained_buybox_losses")
    def test_new_alert_raised(self, mock_detect, mock_conn):
        mock_detect.return_value = [
            {
                "seller_sku": "SKU-LOST",
                "marketplace_id": "AEFP",
                "asin": "B01LOST",
                "consecutive_loss_days": 4,
                "loss_start": "2026-03-08",
                "loss_end": "2026-03-11",
                "avg_win_rate": 2.0,
            },
        ]
        # No existing alert found (fetchone returns None)
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        count = raise_sustained_loss_alerts()
        assert count == 1
        assert conn.committed
        # Check the INSERT was executed (second execute after SELECT)
        assert len(conn.cursor_obj.executed) == 2

    @patch("app.intelligence.buybox_radar.connect_acc")
    @patch("app.intelligence.buybox_radar.detect_sustained_buybox_losses")
    def test_duplicate_alert_skipped(self, mock_detect, mock_conn):
        mock_detect.return_value = [
            {
                "seller_sku": "SKU-LOST",
                "marketplace_id": "AEFP",
                "asin": "B01LOST",
                "consecutive_loss_days": 3,
                "loss_start": "2026-03-09",
                "loss_end": "2026-03-11",
                "avg_win_rate": 0,
            },
        ]
        # Existing alert found
        conn = _FakeConn(rows=[(12345,)])
        mock_conn.return_value = conn
        count = raise_sustained_loss_alerts()
        assert count == 0  # Skipped due to dedup

    @patch("app.intelligence.buybox_radar.connect_acc")
    @patch("app.intelligence.buybox_radar.detect_sustained_buybox_losses")
    def test_critical_severity_for_five_plus_days(self, mock_detect, mock_conn):
        mock_detect.return_value = [
            {
                "seller_sku": "SKU-CRIT",
                "marketplace_id": "AEFP",
                "asin": "B01CRIT",
                "consecutive_loss_days": 7,
                "loss_start": "2026-03-05",
                "loss_end": "2026-03-11",
                "avg_win_rate": 0,
            },
        ]
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        count = raise_sustained_loss_alerts()
        assert count == 1
        # Check severity is "critical" (7 days ≥ 5)
        insert_params = conn.cursor_obj.executed[1][1]
        assert insert_params[1] == "critical"


class TestGetBuyboxAlerts:
    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_returns_parsed_alerts(self, mock_conn):
        detail_json = json.dumps({"seller_sku": "SKU1", "marketplace_id": "AEFP"})
        now = datetime(2026, 3, 12, 10, 0, 0)
        rows = [
            (1, "buybox_sustained_loss", "warning", "BuyBox lost for 3 days: SKU1", detail_json, now),
        ]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn
        result = get_buybox_alerts()
        assert len(result) == 1
        assert result[0]["severity"] == "warning"
        assert result[0]["details"]["seller_sku"] == "SKU1"


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 12 — Capture service, price history, landscape overview tests
# ═══════════════════════════════════════════════════════════════════════════

from app.intelligence.buybox_radar import (
    record_competitor_offers_from_notification,
    get_competitor_price_history,
    get_landscape_overview,
)


class TestRecordCompetitorOffersFromNotification:

    @patch("app.intelligence.buybox_radar.record_competitor_offers")
    def test_forwards_offers_from_payload(self, mock_record):
        mock_record.return_value = 3
        payload = {
            "Offers": [
                {"SellerId": "S1", "ListingPrice": {"Amount": "19.99"}},
                {"SellerId": "S2", "ListingPrice": {"Amount": "21.50"}},
                {"SellerId": "S3", "ListingPrice": {"Amount": "18.00"}},
            ]
        }
        result = record_competitor_offers_from_notification(
            "B01ASIN", "AEFP", payload, our_seller_id="OURS",
        )
        assert result == 3
        mock_record.assert_called_once_with(
            "B01ASIN", "AEFP", payload["Offers"], our_seller_id="OURS",
        )

    @patch("app.intelligence.buybox_radar.record_competitor_offers")
    def test_lowercase_offers_key(self, mock_record):
        mock_record.return_value = 1
        payload = {
            "offers": [{"SellerId": "S1", "ListingPrice": {"Amount": "10.00"}}]
        }
        result = record_competitor_offers_from_notification("B01", "AEFP", payload)
        assert result == 1

    def test_empty_payload_returns_zero(self):
        result = record_competitor_offers_from_notification("B01", "AEFP", {})
        assert result == 0

    def test_none_offers_returns_zero(self):
        result = record_competitor_offers_from_notification("B01", "AEFP", {"offers": None})
        assert result == 0


class TestCaptureCompetitorOffers:
    """Test the async capture orchestrator."""

    @pytest.mark.asyncio
    @patch("app.intelligence.buybox_radar.record_competitor_offers")
    @patch("app.intelligence.buybox_radar.connect_acc")
    async def test_no_asins_returns_early(self, mock_conn, mock_record):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        from app.intelligence.buybox_radar import capture_competitor_offers
        result = await capture_competitor_offers("AEFP")
        assert result["asins_sampled"] == 0
        assert result["status"] == "no_asins"
        mock_record.assert_not_called()

    @pytest.mark.asyncio
    @patch("app.intelligence.buybox_radar.record_competitor_offers")
    @patch("app.connectors.amazon_sp_api.pricing_api.PricingClient")
    @patch("app.intelligence.buybox_radar.connect_acc")
    async def test_captures_offers_for_asins(self, mock_conn, MockPricingClient, mock_record):
        conn = _FakeConn(rows=[("B01ASIN1", 5), ("B01ASIN2", 3)])
        mock_conn.return_value = conn

        import asyncio

        async def fake_get_offers(asin):
            return {"Offers": [{"SellerId": "S1"}]}

        async def noop_sleep(seconds):
            pass

        mock_client = MagicMock()
        mock_client.get_item_offers = fake_get_offers
        MockPricingClient.return_value = mock_client

        mock_record.return_value = 1

        from app.intelligence.buybox_radar import capture_competitor_offers
        with patch("asyncio.sleep", side_effect=noop_sleep):
            result = await capture_competitor_offers("AEFP", asin_limit=2)

        assert result["asins_sampled"] == 2
        assert result["offers_recorded"] == 2
        assert result["status"] == "ok"


class TestGetCompetitorPriceHistory:

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_returns_daily_aggregates(self, mock_conn):
        rows = [
            (date(2026, 3, 10), 3, 15.00, 22.00, 18.50, 2, 1),
            (date(2026, 3, 11), 4, 14.50, 23.00, 19.00, 3, 1),
        ]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn

        result = get_competitor_price_history("B01ASIN", "AEFP", days=7)

        assert len(result) == 2
        assert result[0]["date"] == "2026-03-10"
        assert result[0]["unique_sellers"] == 3
        assert result[0]["min_price"] == 15.0
        assert result[0]["avg_price"] == 18.5
        assert result[1]["fba_offers"] == 3

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_empty_history(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_competitor_price_history("B01ASIN", "AEFP")
        assert result == []

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_seller_filter_adds_param(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        get_competitor_price_history(
            "B01ASIN", "AEFP", days=14, seller_id="SELLER_X",
        )
        sql = conn.cursor_obj.executed[0][0]
        params = conn.cursor_obj.executed[0][1]
        assert "AND seller_id = %s" in sql
        assert "SELLER_X" in params

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_null_prices_handled(self, mock_conn):
        rows = [(date(2026, 3, 10), 0, None, None, None, 0, 0)]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn
        result = get_competitor_price_history("B01ASIN", "AEFP")
        assert result[0]["min_price"] is None
        assert result[0]["avg_price"] is None


class TestGetLandscapeOverview:

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_returns_sorted_entries(self, mock_conn):
        rows = [
            ("B01ASIN1", "AEFP", 5, 3, 10.00, 25.00, 17.50, "SELLER_A"),
            ("B01ASIN2", "AEFP", 3, 1, 12.00, 20.00, 16.00, None),
        ]
        conn = _FakeConn(rows=rows)
        mock_conn.return_value = conn

        result = get_landscape_overview("AEFP", hours=24)

        assert len(result) == 2
        assert result[0]["asin"] == "B01ASIN1"
        assert result[0]["total_sellers"] == 5
        assert result[0]["fba_sellers"] == 3
        assert result[0]["fbm_sellers"] == 2
        assert result[0]["avg_price"] == 17.5
        assert result[0]["buybox_winner_seller_id"] == "SELLER_A"
        assert result[1]["buybox_winner_seller_id"] is None

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_empty_landscape(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        result = get_landscape_overview()
        assert result == []

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_no_marketplace_filter(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        get_landscape_overview(hours=48)
        sql = conn.cursor_obj.executed[0][0]
        # Without marketplace_id, no WHERE filter on marketplace_id
        assert "AND co.marketplace_id = %s" not in sql

    @patch("app.intelligence.buybox_radar.connect_acc")
    def test_with_marketplace_filter(self, mock_conn):
        conn = _FakeConn(rows=[])
        mock_conn.return_value = conn
        get_landscape_overview("AEFP")
        sql = conn.cursor_obj.executed[0][0]
        assert "co.marketplace_id" in sql


class TestBuyBoxRadarAPIEndpoints:
    """Test the FastAPI router endpoints via TestClient."""

    @pytest.fixture(autouse=True)
    def setup_client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.api.v1.buybox_radar import router

        app = FastAPI()
        app.include_router(router, prefix="/api/v1")
        self.client = TestClient(app)

    @patch("app.intelligence.buybox_radar.get_landscape_overview")
    def test_landscape_endpoint(self, mock_fn):
        mock_fn.return_value = [
            {"asin": "B01", "marketplace_id": "AEFP", "total_sellers": 3,
             "fba_sellers": 2, "fbm_sellers": 1, "min_price": 10.0,
             "max_price": 20.0, "avg_price": 15.0, "buybox_winner_seller_id": "S1"},
        ]
        resp = self.client.get("/api/v1/buybox-radar/landscape?marketplace_id=AEFP")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["landscape"][0]["asin"] == "B01"

    @patch("app.intelligence.buybox_radar.get_competitor_price_history")
    def test_price_history_endpoint(self, mock_fn):
        mock_fn.return_value = [
            {"date": "2026-03-10", "unique_sellers": 3, "min_price": 10.0,
             "max_price": 20.0, "avg_price": 15.0, "fba_offers": 2, "fbm_offers": 1},
        ]
        resp = self.client.get(
            "/api/v1/buybox-radar/competitors/B01ASIN/history?marketplace_id=AEFP&days=14"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["asin"] == "B01ASIN"
        assert len(data["history"]) == 1

    @patch("app.intelligence.buybox_radar.capture_competitor_offers")
    def test_capture_endpoint(self, mock_fn):
        import asyncio
        async def fake_capture(marketplace_id, **kw):
            return {"marketplace_id": marketplace_id, "asins_sampled": 10,
                    "offers_recorded": 25, "errors": 0, "status": "ok"}
        mock_fn.side_effect = fake_capture
        resp = self.client.post(
            "/api/v1/buybox-radar/capture-competitors?marketplace_id=AEFP&asin_limit=10"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["asins_sampled"] == 10
        assert data["offers_recorded"] == 25
