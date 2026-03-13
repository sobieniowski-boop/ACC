"""Unit tests for Content Optimization Engine.

Sprint 17 – Content scoring model, SEO analysis, API endpoints.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from app.intelligence.content_optimization import (
    SCORE_VERSION,
    WEIGHT_TITLE,
    WEIGHT_BULLETS,
    WEIGHT_DESCRIPTION,
    WEIGHT_KEYWORDS,
    WEIGHT_IMAGES,
    WEIGHT_APLUS,
    score_title,
    score_bullets,
    score_description,
    score_keywords,
    score_images,
    compute_content_score,
    analyze_seo,
    _fv,
    _safe_json,
    ensure_content_optimization_schema,
    get_content_scores,
    get_content_score_for_sku,
    get_score_distribution,
    get_top_opportunities,
    get_score_history,
    get_seo_analysis_for_sku,
    save_seo_analysis,
    score_listings_for_marketplace,
)


# ═══════════════════════════════════════════════════════════════════════════
#  DB mock helpers
# ═══════════════════════════════════════════════════════════════════════════

class _FakeConn:
    def __init__(self, rows=None) -> None:
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
    def __init__(self, rows=None) -> None:
        self.rows = rows or []
        self.executed: list[tuple] = []
        self.call_index = 0
        self.multi_rows: list[list] = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        if self.multi_rows:
            r = self.multi_rows[self.call_index] if self.call_index < len(self.multi_rows) else []
            self.call_index += 1
            return r[0] if r else None
        return self.rows[0] if self.rows else None

    def fetchall(self):
        if self.multi_rows:
            r = self.multi_rows[self.call_index] if self.call_index < len(self.multi_rows) else []
            self.call_index += 1
            return r
        return self.rows


# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════


class TestConstants:
    def test_score_version(self):
        assert SCORE_VERSION == 1

    def test_weights_sum_to_100(self):
        total = WEIGHT_TITLE + WEIGHT_BULLETS + WEIGHT_DESCRIPTION + WEIGHT_KEYWORDS + WEIGHT_IMAGES + WEIGHT_APLUS
        assert total == 100

    def test_individual_weights(self):
        assert WEIGHT_TITLE == 25
        assert WEIGHT_BULLETS == 25
        assert WEIGHT_DESCRIPTION == 15
        assert WEIGHT_KEYWORDS == 15
        assert WEIGHT_IMAGES == 10
        assert WEIGHT_APLUS == 10


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════


class TestHelpers:
    def test_fv_float(self):
        assert _fv(3.14159) == 3.14

    def test_fv_int(self):
        assert _fv(42) == 42.0

    def test_fv_string_number(self):
        assert _fv("12.5") == 12.5

    def test_fv_none(self):
        assert _fv(None) is None

    def test_fv_invalid(self):
        assert _fv("abc") is None

    def test_safe_json_string(self):
        assert _safe_json('["a","b"]') == ["a", "b"]

    def test_safe_json_none(self):
        assert _safe_json(None) == []

    def test_safe_json_list_passthrough(self):
        assert _safe_json(["x"]) == ["x"]

    def test_safe_json_invalid(self):
        assert _safe_json("not json{") == []


# ═══════════════════════════════════════════════════════════════════════════
#  Title scoring
# ═══════════════════════════════════════════════════════════════════════════


class TestScoreTitle:
    def test_missing_title(self):
        score, issues, recs = score_title(None)
        assert score == 0
        assert "missing_title" in issues

    def test_empty_title(self):
        score, issues, recs = score_title("")
        assert score == 0

    def test_very_short_title(self):
        score, issues, recs = score_title("Short")
        assert score == 15
        assert "title_too_short" in issues

    def test_ideal_length_title(self):
        title = "Premium Organic Bamboo Cutting Board Set with Juice Groove - Large Kitchen Chopping Board for Meat Vegetables Bread - Natural Eco-Friendly"
        score, issues, recs = score_title(title)
        assert score >= 90

    def test_too_long_title(self):
        title = "A" * 250
        score, issues, recs = score_title(title)
        assert "title_too_long" in issues
        assert score < 100

    def test_all_caps_penalty(self):
        title = "PREMIUM ORGANIC BAMBOO CUTTING BOARD SET WITH JUICE GROOVE AND HANDLE FOR KITCHEN"
        score, issues, recs = score_title(title)
        assert "title_all_caps" in issues

    def test_special_chars_penalty(self):
        title = "Premium Board!!! #1 Best & Amazing Product *** Buy Now"
        score, issues, recs = score_title(title)
        assert "title_special_chars" in issues

    def test_acceptable_length(self):
        title = "A" * 85  # between 80-100: acceptable but not ideal
        score, issues, recs = score_title(title)
        assert 80 <= score <= 100


# ═══════════════════════════════════════════════════════════════════════════
#  Bullet scoring
# ═══════════════════════════════════════════════════════════════════════════


class TestScoreBullets:
    def test_no_bullets(self):
        score, count, avg, issues, recs = score_bullets(None)
        assert score == 0
        assert "no_bullets" in issues

    def test_empty_list(self):
        score, count, avg, issues, recs = score_bullets([])
        assert score == 0

    def test_five_good_bullets(self):
        bullets = [
            "Made from 100% organic bamboo wood, sustainably harvested and eco-friendly for your kitchen preparation needs",
            "Extra large cutting surface 18x12 inches provides ample workspace for chopping vegetables, slicing meat, and preparing meals",
            "Built-in juice groove catches liquids to keep your countertop clean while you prepare seafood, poultry, or fruits",
            "BPA-free and food grade certified, safe for direct contact with all types of food including acidic fruits and vegetables",
            "Easy to clean and maintain — simply hand wash with warm soapy water and dry thoroughly after each use for longevity",
        ]
        score, count, avg, issues, recs = score_bullets(bullets)
        assert score >= 80
        assert count == 5

    def test_too_few_bullets(self):
        bullets = [
            "Good quality product",
            "Fast shipping",
        ]
        score, count, avg, issues, recs = score_bullets(bullets)
        assert "too_few_bullets" in issues
        assert score < 60

    def test_duplicate_bullets_penalty(self):
        bullets = [
            "Great product for home use",
            "Great product for home use",
            "Another feature here",
        ]
        score, count, avg, issues, recs = score_bullets(bullets)
        assert "duplicate_bullets" in issues

    def test_short_bullets_flagged(self):
        bullets = ["A", "B", "C", "D", "E"]
        score, count, avg, issues, recs = score_bullets(bullets)
        assert "short_bullets" in issues


# ═══════════════════════════════════════════════════════════════════════════
#  Description scoring
# ═══════════════════════════════════════════════════════════════════════════


class TestScoreDescription:
    def test_no_description(self):
        score, length, issues, recs = score_description(None)
        assert score == 0
        assert "no_description" in issues

    def test_very_short(self):
        score, length, issues, recs = score_description("Short")
        assert score == 10

    def test_ideal_length(self):
        desc = "A" * 600
        score, length, issues, recs = score_description(desc)
        assert score >= 90

    def test_html_bonus(self):
        desc = "<p>Premium product description with detailed features.</p>" + "A" * 500
        score, length, issues, recs = score_description(desc)
        # HTML formatting gets a small bonus
        assert score >= 95


# ═══════════════════════════════════════════════════════════════════════════
#  Keyword scoring
# ═══════════════════════════════════════════════════════════════════════════


class TestScoreKeywords:
    def test_no_keywords(self):
        score, length, issues, recs = score_keywords(None)
        assert score == 0
        assert "no_keywords" in issues

    def test_very_short(self):
        score, length, issues, recs = score_keywords("bamboo")
        assert score == 10

    def test_ideal_keywords(self):
        kw = "bamboo cutting board chopping block kitchen food preparation organic natural eco friendly wood large heavy duty professional chef cooking slicing"
        score, length, issues, recs = score_keywords(kw)
        assert score >= 80

    def test_commas_penalized(self):
        kw = "bamboo, cutting board, chopping, kitchen, wood"
        score, length, issues, recs = score_keywords(kw)
        assert "keywords_commas" in issues

    def test_too_long_penalized(self):
        kw = " ".join([f"word{i}" for i in range(60)])
        score, length, issues, recs = score_keywords(kw)
        assert "keywords_too_long" in issues

    def test_duplicated_words(self):
        kw = "bamboo bamboo bamboo bamboo cutting bamboo board bamboo bamboo"
        score, length, issues, recs = score_keywords(kw)
        assert "keywords_duplicated" in issues


# ═══════════════════════════════════════════════════════════════════════════
#  Image scoring
# ═══════════════════════════════════════════════════════════════════════════


class TestScoreImages:
    def test_no_images(self):
        score, aplus, issues, recs = score_images(0)
        assert score == 0
        assert "no_images" in issues

    def test_ideal_images(self):
        score, aplus, issues, recs = score_images(7)
        assert score == 100

    def test_few_images(self):
        score, aplus, issues, recs = score_images(2)
        assert "too_few_images" in issues
        assert score < 70

    def test_aplus_bonus(self):
        _, aplus1, _, _ = score_images(3, has_aplus=False)
        _, aplus2, _, _ = score_images(3, has_aplus=True)
        assert aplus1 == 0
        assert aplus2 == 100


# ═══════════════════════════════════════════════════════════════════════════
#  Composite content score
# ═══════════════════════════════════════════════════════════════════════════


class TestComputeContentScore:
    def test_empty_listing(self):
        result = compute_content_score()
        assert result["total_score"] == 0
        assert result["title_score"] == 0
        assert result["bullet_score"] == 0
        assert result["score_version"] == SCORE_VERSION

    def test_full_listing(self):
        result = compute_content_score(
            title="Premium Organic Bamboo Cutting Board Set with Juice Groove for Kitchen Preparation - Large Size Extra Durable Eco-Friendly",
            bullets=[
                "Made from 100% organic bamboo wood that is sustainably harvested from renewable bamboo forests",
                "Extra large cutting surface provides ample workspace for all your food preparation needs in the kitchen",
                "Built-in juice groove catches liquids to keep your countertop clean while preparing meats and fruits",
                "BPA-free and food grade certified material that is safe for all types of food contact and preparation",
                "Easy to clean — simply hand wash with warm water and soap, then dry thoroughly for best maintenance",
            ],
            description="A" * 600,
            keywords="bamboo cutting board chopping block kitchen food preparation organic natural eco " * 3,
            image_count=7,
            has_aplus=True,
        )
        assert result["total_score"] >= 70
        assert result["title_score"] >= 80
        assert result["bullet_score"] >= 80
        assert len(result["issues"]) >= 0
        assert len(result["recommendations"]) >= 0

    def test_score_bounded_0_100(self):
        result = compute_content_score(title="A" * 120)
        assert 0 <= result["total_score"] <= 100

    def test_issues_and_recommendations_populated(self):
        result = compute_content_score()
        assert isinstance(result["issues"], list)
        assert isinstance(result["recommendations"], list)
        assert len(result["issues"]) > 0
        assert len(result["recommendations"]) > 0


# ═══════════════════════════════════════════════════════════════════════════
#  SEO Analysis
# ═══════════════════════════════════════════════════════════════════════════


class TestAnalyzeSeo:
    def test_no_search_terms(self):
        result = analyze_seo(title="Test Title")
        assert result["seo_score"] == 50  # baseline
        assert result["missing_keywords"] == []

    def test_with_search_terms_full_coverage(self):
        result = analyze_seo(
            title="bamboo cutting board kitchen chopping block",
            bullets=["premium bamboo material", "professional cutting board"],
            keywords="bamboo cutting board kitchen",
            brand="BambooKing",
            search_terms=[
                {"search_term": "bamboo cutting board", "search_frequency_rank": 1, "click_share": 15.0, "conversion_share": 10.0},
                {"search_term": "kitchen chopping block", "search_frequency_rank": 2, "click_share": 8.0, "conversion_share": 6.0},
            ],
        )
        assert result["seo_score"] > 0
        assert result["keyword_coverage_pct"] is not None
        assert result["title_has_primary_kw"] is True

    def test_missing_keywords_detected(self):
        result = analyze_seo(
            title="Simple Product",
            search_terms=[
                {"search_term": "bamboo cutting board", "search_frequency_rank": 1, "click_share": 15.0, "conversion_share": 10.0},
                {"search_term": "wooden chopping block", "search_frequency_rank": 2, "click_share": 8.0, "conversion_share": 6.0},
            ],
        )
        assert len(result["missing_keywords"]) > 0
        assert result["keyword_coverage_pct"] is not None

    def test_brand_in_title(self):
        result = analyze_seo(
            title="BambooKing Premium Cutting Board",
            brand="BambooKing",
            search_terms=[
                {"search_term": "cutting board", "search_frequency_rank": 1},
            ],
        )
        assert result["title_has_brand"] is True

    def test_brand_not_in_title(self):
        result = analyze_seo(
            title="Premium Cutting Board",
            brand="BambooKing",
            search_terms=[
                {"search_term": "cutting board", "search_frequency_rank": 1},
            ],
        )
        assert result["title_has_brand"] is False

    def test_seo_score_bounded(self):
        result = analyze_seo(
            title="T",
            search_terms=[{"search_term": f"term{i}", "search_frequency_rank": i} for i in range(25)],
        )
        assert 0 <= result["seo_score"] <= 100

    def test_empty_content(self):
        result = analyze_seo(search_terms=[
            {"search_term": "bamboo board", "search_frequency_rank": 1},
        ])
        assert result["keyword_coverage_pct"] == 0
        assert len(result["missing_keywords"]) > 0


# ═══════════════════════════════════════════════════════════════════════════
#  DB: ensure_content_optimization_schema
# ═══════════════════════════════════════════════════════════════════════════


class TestEnsureSchema:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_runs_ddl(self, mock_conn_fn):
        conn = _FakeConn()
        mock_conn_fn.return_value = conn
        ensure_content_optimization_schema()
        assert conn.committed
        assert conn.closed
        assert len(conn.cursor_obj.executed) == 3  # 3 DDL statements


# ═══════════════════════════════════════════════════════════════════════════
#  DB: get_content_scores
# ═══════════════════════════════════════════════════════════════════════════


class TestGetContentScores:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_returns_paginated(self, mock_conn_fn):
        fake_count = [(5,)]
        fake_rows = [
            (1, "SKU1", "ASIN1", "MKT", 85, 90, 80, 70, 75, 100, 100,
             120, 5, 130, 600, 180, 7, 1,
             '["no_issues"]', '[]', 1, "2025-01-01"),
        ]
        conn = _FakeConn()
        conn.cursor_obj.multi_rows = [fake_count, fake_rows]
        mock_conn_fn.return_value = conn

        result = get_content_scores("MKT", limit=10, offset=0)
        assert result["total"] == 5
        assert len(result["items"]) == 1
        assert result["items"][0]["seller_sku"] == "SKU1"
        assert result["items"][0]["total_score"] == 85
        assert conn.closed

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_empty(self, mock_conn_fn):
        conn = _FakeConn()
        conn.cursor_obj.multi_rows = [[(0,)], []]
        mock_conn_fn.return_value = conn

        result = get_content_scores(limit=10, offset=0)
        assert result["total"] == 0
        assert result["items"] == []


# ═══════════════════════════════════════════════════════════════════════════
#  DB: get_content_score_for_sku
# ═══════════════════════════════════════════════════════════════════════════


class TestGetContentScoreForSku:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_found(self, mock_conn_fn):
        row = (1, "SKU1", "ASIN1", "MKT", 75, 80, 70, 60, 65, 90, 0,
               110, 4, 110, 300, 140, 5, 0,
               '[]', '["add aplus"]', 1, "2025-01-01")
        conn = _FakeConn([row])
        mock_conn_fn.return_value = conn

        result = get_content_score_for_sku("SKU1", "MKT")
        assert result is not None
        assert result["total_score"] == 75
        assert result["has_aplus"] is False

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_not_found(self, mock_conn_fn):
        conn = _FakeConn([])
        mock_conn_fn.return_value = conn

        result = get_content_score_for_sku("MISSING", "MKT")
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
#  DB: get_score_distribution
# ═══════════════════════════════════════════════════════════════════════════


class TestGetScoreDistribution:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_with_data(self, mock_conn_fn):
        row = (2, 3, 10, 8, 5, 28, 62.5, 70.0, 65.0, 55.0, 60.0, 80.0, 30.0)
        conn = _FakeConn([row])
        mock_conn_fn.return_value = conn

        result = get_score_distribution("MKT")
        assert result["total"] == 28
        assert result["distribution"]["poor"] == 2
        assert result["distribution"]["excellent"] == 5
        assert result["avg_score"] == 62.5

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_empty(self, mock_conn_fn):
        row = (0, 0, 0, 0, 0, 0, None, None, None, None, None, None, None)
        conn = _FakeConn([row])
        mock_conn_fn.return_value = conn

        result = get_score_distribution()
        assert result["total"] == 0
        assert result["avg_score"] is None


# ═══════════════════════════════════════════════════════════════════════════
#  DB: get_top_opportunities
# ═══════════════════════════════════════════════════════════════════════════


class TestGetTopOpportunities:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_returns_list(self, mock_conn_fn):
        rows = [
            ("SKU1", "ASIN1", "MKT", 25, 20, 10, 30, 15, 40, 0, '["no_bullets"]', '["Add bullets"]'),
            ("SKU2", "ASIN2", "MKT", 35, 40, 30, 35, 25, 50, 0, '[]', '[]'),
        ]
        conn = _FakeConn(rows)
        mock_conn_fn.return_value = conn

        result = get_top_opportunities("MKT", limit=10)
        assert len(result) == 2
        assert result[0]["seller_sku"] == "SKU1"
        assert result[0]["total_score"] == 25


# ═══════════════════════════════════════════════════════════════════════════
#  DB: get_score_history
# ═══════════════════════════════════════════════════════════════════════════


class TestGetScoreHistory:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_returns_history(self, mock_conn_fn):
        rows = [
            ("2025-03-01", 50, 60, 40, 30, 45, 70, 0),
            ("2025-03-02", 55, 65, 45, 35, 50, 70, 0),
        ]
        conn = _FakeConn(rows)
        mock_conn_fn.return_value = conn

        result = get_score_history("SKU1", "MKT", days=30)
        assert len(result) == 2
        assert result[0]["date"] == "2025-03-01"
        assert result[1]["total_score"] == 55


# ═══════════════════════════════════════════════════════════════════════════
#  DB: SEO analysis persistence
# ═══════════════════════════════════════════════════════════════════════════


class TestSeoAnalysisPersistence:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_seo_found(self, mock_conn_fn):
        row = (1, "SKU1", "ASIN1", "MKT", 72, 85.0,
               '[{"search_term": "board"}]', '[{"search_term": "board", "rank": 1}]',
               '{"board": 3}', 5, 1, 1, "2025-03-10")
        conn = _FakeConn([row])
        mock_conn_fn.return_value = conn

        result = get_seo_analysis_for_sku("SKU1", "MKT")
        assert result is not None
        assert result["seo_score"] == 72
        assert result["title_has_brand"] is True

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_seo_not_found(self, mock_conn_fn):
        conn = _FakeConn([])
        mock_conn_fn.return_value = conn

        result = get_seo_analysis_for_sku("MISSING", "MKT")
        assert result is None

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_save_seo(self, mock_conn_fn):
        conn = _FakeConn()
        mock_conn_fn.return_value = conn

        analysis = {
            "seo_score": 65,
            "keyword_coverage_pct": 70.0,
            "missing_keywords": [{"search_term": "bamboo board"}],
            "top_search_terms": [{"search_term": "cutting board", "rank": 1}],
            "keyword_density": {"cutting": 2},
            "title_keyword_count": 5,
            "title_has_brand": True,
            "title_has_primary_kw": True,
        }
        save_seo_analysis("SKU1", "ASIN1", "MKT", analysis)
        assert conn.committed
        assert conn.closed
        assert len(conn.cursor_obj.executed) == 1


# ═══════════════════════════════════════════════════════════════════════════
#  DB: score_listings_for_marketplace
# ═══════════════════════════════════════════════════════════════════════════


class TestScoreListingsForMarketplace:
    @patch("app.intelligence.content_optimization.connect_acc")
    def test_scores_listings(self, mock_conn_fn):
        # Setup: one listing row, one content version
        listing_row = ("SKU1", "ASIN1", "MKT", "Good Product Title For Testing", "BrandX", "http://img.jpg")
        fields = json.dumps({
            "bullets": ["Bullet one is good", "Bullet two is also good"],
            "description": "Product description text",
            "keywords": "product keyword test",
        })
        version_row = (fields,)

        conn = _FakeConn()
        # Sequence: listings query, version query, MERGE score, MERGE history
        conn.cursor_obj.multi_rows = [
            [listing_row],     # fetchall: listings
            [version_row],     # fetchone: version
            [],                # MERGE score (execute only)
            [],                # MERGE history (execute only)
        ]
        mock_conn_fn.return_value = conn

        result = score_listings_for_marketplace("MKT", limit=1)
        assert result["marketplace_id"] == "MKT"
        assert result["listings_scored"] == 1
        assert result["avg_score"] > 0
        assert conn.committed
        assert conn.closed

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_no_listings(self, mock_conn_fn):
        conn = _FakeConn()
        conn.cursor_obj.multi_rows = [[]]  # no listings
        mock_conn_fn.return_value = conn

        result = score_listings_for_marketplace("MKT")
        assert result["listings_scored"] == 0
        assert result["avg_score"] == 0


# ═══════════════════════════════════════════════════════════════════════════
#  API: Content Optimization endpoints
# ═══════════════════════════════════════════════════════════════════════════


class TestContentOptimizationAPI:
    """Test API endpoints via FastAPI TestClient."""

    @pytest.fixture(autouse=True)
    def _setup_client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.api.v1.content_optimization import router

        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_scores(self, mock_conn_fn):
        conn = _FakeConn()
        conn.cursor_obj.multi_rows = [[(1,)], [
            (1, "SKU1", "ASIN1", "MKT", 80, 85, 75, 70, 65, 90, 100,
             120, 5, 130, 600, 180, 7, 1, '[]', '[]', 1, "2025-01-01"),
        ]]
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/scores?marketplace_id=MKT")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert len(body["items"]) == 1

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_score_detail(self, mock_conn_fn):
        row = (1, "SKU1", "ASIN1", "MKT", 75, 80, 70, 60, 65, 90, 0,
               110, 4, 110, 300, 140, 5, 0, '[]', '[]', 1, "2025-01-01")
        conn = _FakeConn([row])
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/scores/SKU1?marketplace_id=MKT")
        assert resp.status_code == 200
        assert resp.json()["total_score"] == 75

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_score_detail_not_found(self, mock_conn_fn):
        conn = _FakeConn([])
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/scores/MISSING?marketplace_id=MKT")
        assert resp.status_code == 404

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_distribution(self, mock_conn_fn):
        row = (2, 3, 10, 8, 5, 28, 62.5, 70.0, 65.0, 55.0, 60.0, 80.0, 30.0)
        conn = _FakeConn([row])
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/distribution")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 28

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_opportunities(self, mock_conn_fn):
        rows = [
            ("SKU1", "ASIN1", "MKT", 30, 20, 10, 30, 15, 40, 0, '[]', '[]'),
        ]
        conn = _FakeConn(rows)
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/opportunities")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_history(self, mock_conn_fn):
        rows = [("2025-03-01", 50, 60, 40, 30, 45, 70, 0)]
        conn = _FakeConn(rows)
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/history/SKU1?marketplace_id=MKT")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_seo_detail(self, mock_conn_fn):
        row = (1, "SKU1", "ASIN1", "MKT", 72, 85.0,
               '[]', '[]', '{}', 5, 1, 1, "2025-03-10")
        conn = _FakeConn([row])
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/seo/SKU1?marketplace_id=MKT")
        assert resp.status_code == 200
        assert resp.json()["seo_score"] == 72

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_get_seo_not_found(self, mock_conn_fn):
        conn = _FakeConn([])
        mock_conn_fn.return_value = conn

        resp = self.client.get("/content-optimization/seo/MISSING?marketplace_id=MKT")
        assert resp.status_code == 404

    @patch("app.intelligence.content_optimization.connect_acc")
    def test_compute(self, mock_conn_fn):
        conn = _FakeConn()
        conn.cursor_obj.multi_rows = [[]]
        mock_conn_fn.return_value = conn

        resp = self.client.post("/content-optimization/compute?marketplace_id=MKT")
        assert resp.status_code == 200
        body = resp.json()
        assert body["marketplace_id"] == "MKT"
        assert body["listings_scored"] == 0


# ═══════════════════════════════════════════════════════════════════════════
#  Scheduler
# ═══════════════════════════════════════════════════════════════════════════


class TestContentScheduler:
    def test_register_has_scoring_job(self):
        from unittest.mock import MagicMock
        from app.platform.scheduler.content import register

        scheduler = MagicMock()
        register(scheduler)

        job_ids = [c.kwargs.get("id") for c in scheduler.add_job.call_args_list]
        assert "content-scoring-daily" in job_ids
        assert "sync-ptd-cache-daily" in job_ids
        assert "content-publish-queue-1m" in job_ids
