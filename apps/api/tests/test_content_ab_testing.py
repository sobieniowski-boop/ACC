"""Tests for Sprint 18 – Content A/B Testing & Multi-language Generation.

Covers: constants, helpers, language-quality validation, multi-language generation,
A/B experiment lifecycle, DB queries, API endpoints, and edge cases.
"""
from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.intelligence import content_ab_testing as cab

# ── Local fakes ──────────────────────────────────────────────────────


class _FakeCursor:
    """Cursor that feeds rows from a pre-loaded list."""

    def __init__(self):
        self.executed: list[tuple] = []
        self.multi_rows: list = []
        self._idx = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        if self._idx < len(self.multi_rows):
            row = self.multi_rows[self._idx]
            self._idx += 1
            return row
        return None

    def fetchall(self):
        if self._idx < len(self.multi_rows):
            rest = self.multi_rows[self._idx:]
            self._idx = len(self.multi_rows)
            return rest
        return []

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor | None = None):
        self._cursor = cursor or _FakeCursor()

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


# ── S18-01: Constants ────────────────────────────────────────────────


class TestConstants:
    def test_valid_experiment_statuses(self):
        assert "draft" in cab.VALID_EXPERIMENT_STATUSES
        assert "running" in cab.VALID_EXPERIMENT_STATUSES
        assert "concluded" in cab.VALID_EXPERIMENT_STATUSES
        assert "cancelled" in cab.VALID_EXPERIMENT_STATUSES
        assert "paused" in cab.VALID_EXPERIMENT_STATUSES
        assert len(cab.VALID_EXPERIMENT_STATUSES) == 5

    def test_valid_metrics(self):
        assert "conversion_rate" in cab.VALID_METRICS
        assert "ctr" in cab.VALID_METRICS
        assert "revenue" in cab.VALID_METRICS
        assert "orders" in cab.VALID_METRICS
        assert len(cab.VALID_METRICS) == 4

    def test_valid_multilang_statuses(self):
        assert "pending" in cab.VALID_MULTILANG_STATUSES
        assert "generating" in cab.VALID_MULTILANG_STATUSES
        assert "completed" in cab.VALID_MULTILANG_STATUSES
        assert "failed" in cab.VALID_MULTILANG_STATUSES
        assert "review" in cab.VALID_MULTILANG_STATUSES

    def test_language_names_has_default_languages(self):
        for lang in ["de_DE", "fr_FR", "it_IT", "es_ES", "nl_NL", "pl_PL", "sv_SE", "nl_BE"]:
            assert lang in cab.LANGUAGE_NAMES


# ── S18-02: Helpers ──────────────────────────────────────────────────


class TestHelpers:
    def test_safe_json_valid(self):
        assert cab._safe_json('{"a": 1}') == {"a": 1}

    def test_safe_json_list(self):
        assert cab._safe_json("[1, 2]") == [1, 2]

    def test_safe_json_invalid(self):
        assert cab._safe_json("not json") == []

    def test_safe_json_none(self):
        assert cab._safe_json(None) == []

    def test_safe_json_already_dict(self):
        d = {"x": 1}
        assert cab._safe_json(d) == d

    def test_safe_json_already_list(self):
        lst = [1, 2, 3]
        assert cab._safe_json(lst) == lst


class TestLanguageTag:
    def test_de(self):
        assert cab._language_tag_for_market("DE") == "de_DE"

    def test_fr(self):
        assert cab._language_tag_for_market("FR") == "fr_FR"

    def test_it(self):
        assert cab._language_tag_for_market("IT") == "it_IT"

    def test_es(self):
        assert cab._language_tag_for_market("ES") == "es_ES"

    def test_pl(self):
        assert cab._language_tag_for_market("PL") == "pl_PL"

    def test_se(self):
        assert cab._language_tag_for_market("SE") == "sv_SE"

    def test_nl(self):
        assert cab._language_tag_for_market("NL") == "nl_NL"

    def test_be(self):
        assert cab._language_tag_for_market("BE") == "nl_BE"

    def test_unknown_defaults_en(self):
        assert cab._language_tag_for_market("XX") == "en_GB"


# ── S18-03: Language quality validation ──────────────────────────────


class TestValidateLanguageQuality:
    def test_full_quality_de(self):
        fields = {
            "title": "Hochwertige Kaffeemaschine",
            "bullets": ["Punkt eins", "Punkt zwei", "Punkt drei", "Punkt vier", "Punkt fünf"],
            "description": "Lange Beschreibung auf Deutsch " * 20,
            "keywords": "kaffeemaschine espresso milchschäumer",
        }
        score, issues = cab.validate_language_quality(fields, "de_DE")
        assert score >= 70
        assert isinstance(issues, list)

    def test_empty_fields_low_score(self):
        fields = {}
        score, issues = cab.validate_language_quality(fields, "de_DE")
        assert score < 50
        assert len(issues) > 0

    def test_missing_title(self):
        fields = {"bullets": ["a"], "description": "b", "keywords": "c"}
        score, issues = cab.validate_language_quality(fields, "fr_FR")
        assert any("title" in i.lower() for i in issues)

    def test_polish_leak_in_german(self):
        fields = {
            "title": "Produkt opakowanie sztuka zestaw",
            "bullets": ["Gute Qualität"],
            "description": "Beschreibung",
            "keywords": "test",
        }
        score, issues = cab.validate_language_quality(fields, "de_DE")
        assert any("polish_leak" in i for i in issues)

    def test_polish_ok_for_pl(self):
        fields = {
            "title": "Produkt opakowanie sztuka zestaw materiał",
            "bullets": ["Punkt pierwszy", "Punkt drugi", "Punkt trzeci"],
            "description": "Opis produktu w języku polskim " * 10,
            "keywords": "test produkt",
        }
        score, issues = cab.validate_language_quality(fields, "pl_PL")
        # Polish words should NOT be flagged when target is Polish
        assert not any("polish_leak" in i for i in issues)

    def test_english_leak_in_french(self):
        fields = {
            "title": "The best product with your free and amazing",
            "bullets": ["Buy it now", "Free shipping"],
            "description": "This is an amazing product",
            "keywords": "product test",
        }
        score, issues = cab.validate_language_quality(fields, "fr_FR")
        assert any("english" in i.lower() for i in issues)

    def test_short_description_flagged(self):
        fields = {
            "title": "Guter Titel",
            "bullets": ["a", "b"],
            "description": "Kurz",
            "keywords": "test",
        }
        score, issues = cab.validate_language_quality(fields, "de_DE")
        assert any("description" in i.lower() or "short" in i.lower() for i in issues)

    def test_few_bullets_flagged(self):
        fields = {
            "title": "Guter Titel",
            "bullets": ["Eine"],
            "description": "Beschreibung lang genug " * 15,
            "keywords": "test",
        }
        score, issues = cab.validate_language_quality(fields, "de_DE")
        assert any("bullet" in i.lower() for i in issues)


# ── S18-04: Row mappers ─────────────────────────────────────────────


class TestRowMappers:
    def test_multilang_row_to_dict(self):
        row = (
            1, "SKU1", "B00ASIN", "A1PA6795UKMFR9", "A13V1IB3VIYZZH",
            "fr_FR", "completed", "v1", "v2", "gpt-5.2",
            85, '["minor issue"]', '["flag1"]', None,
            datetime(2025, 1, 1), datetime(2025, 1, 2),
        )
        d = cab._multilang_row_to_dict(row)
        assert d["id"] == 1
        assert d["seller_sku"] == "SKU1"
        assert d["target_language"] == "fr_FR"
        assert d["status"] == "completed"
        assert d["quality_score"] == 85
        assert d["quality_issues"] == ["minor issue"]
        assert d["policy_flags"] == ["flag1"]

    def test_experiment_row_to_dict(self):
        row = (
            10, "Test Exp", "SKU2", "A1PA6795UKMFR9", "running",
            "Title matters", "conversion_rate", datetime(2025, 3, 1),
            None, None, "admin", datetime(2025, 2, 28), None,
        )
        d = cab._experiment_row_to_dict(row)
        assert d["id"] == 10
        assert d["name"] == "Test Exp"
        assert d["status"] == "running"
        assert d["metric_primary"] == "conversion_rate"

    def test_variant_row_to_dict(self):
        row = (
            5, 10, "Variant A", "ver-1", 1, 1000, 100,
            50, 2500.0, 5.0, 10.0, 78, datetime(2025, 3, 1),
        )
        d = cab._variant_row_to_dict(row)
        assert d["id"] == 5
        assert d["experiment_id"] == 10
        assert d["label"] == "Variant A"
        assert d["is_control"] is True
        assert d["impressions"] == 1000
        assert d["conversion_rate"] == 5.0


# ── S18-05: Schema DDL ──────────────────────────────────────────────


class TestEnsureSchema:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_creates_tables(self, mock_connect):
        cur = _FakeCursor()
        mock_connect.return_value = _FakeConn(cur)
        cab.ensure_multilang_ab_schema()
        assert len(cur.executed) >= 3
        sqls = " ".join(sql for sql, _ in cur.executed)
        assert "acc_multilang_job" in sqls
        assert "acc_content_experiment" in sqls
        assert "acc_content_variant" in sqls


# ── S18-06: Create experiment ────────────────────────────────────────


class TestCreateExperiment:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_create_ok(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [(42,)]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.create_experiment(
            name="My Exp", seller_sku="SKU1",
            marketplace_id="MKT1", metric_primary="conversion_rate",
        )
        assert result["id"] == 42
        assert result["name"] == "My Exp"
        assert result["status"] == "draft"

    def test_invalid_metric_raises(self):
        with pytest.raises(ValueError, match="metric"):
            cab.create_experiment(
                name="X", seller_sku="S", marketplace_id="M",
                metric_primary="invalid_metric",
            )


# ── S18-07: Add variant ─────────────────────────────────────────────


class TestAddVariant:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_add_variant_ok(self, mock_connect):
        cur = _FakeCursor()
        # First fetchone: experiment exists (draft)
        # Second fetchone: inserted variant id
        cur.multi_rows = [("draft",), (99,)]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.add_variant(
            experiment_id=1, label="Control",
            is_control=True, content_score=80,
        )
        assert result["id"] == 99
        assert result["label"] == "Control"

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_add_variant_concluded_raises(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [("concluded",)]
        mock_connect.return_value = _FakeConn(cur)
        with pytest.raises(ValueError, match="concluded"):
            cab.add_variant(experiment_id=1, label="X")

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_add_variant_not_found_raises(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_connect.return_value = _FakeConn(cur)
        with pytest.raises(ValueError, match="not found"):
            cab.add_variant(experiment_id=999, label="X")


# ── S18-08: Start experiment ────────────────────────────────────────


class TestStartExperiment:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_start_ok(self, mock_connect):
        cur = _FakeCursor()
        # fetchone: status=draft; fetchone: variant count=2
        cur.multi_rows = [("draft",), (2,)]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.start_experiment(1)
        assert result["status"] == "running"

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_start_not_draft_raises(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [("running",)]
        mock_connect.return_value = _FakeConn(cur)
        with pytest.raises(ValueError, match="draft"):
            cab.start_experiment(1)

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_start_too_few_variants_raises(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [("draft",), (1,)]
        mock_connect.return_value = _FakeConn(cur)
        with pytest.raises(ValueError, match="variant"):
            cab.start_experiment(1)


# ── S18-09: Record metrics ──────────────────────────────────────────


class TestRecordVariantMetrics:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_record_metrics(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_connect.return_value = _FakeConn(cur)
        result = cab.record_variant_metrics(
            variant_id=5, impressions=1000, clicks=100, orders=50, revenue=5000.0,
        )
        assert result["id"] == 5
        assert result["updated"] is True


# ── S18-10: Conclude experiment ─────────────────────────────────────


class TestConcludeExperiment:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_conclude_ok(self, mock_connect):
        cur = _FakeCursor()
        # fetchone: experiment (running, conversion_rate)
        # fetchall: 2 variants (id, label, is_control, imp, clicks, orders, rev, conv_rate, ctr, score)
        variant_a = (10, "A", 1, 500, 50, 20, 1000.0, 4.0, 10.0, 70)
        variant_b = (11, "B", 0, 500, 60, 30, 1500.0, 6.0, 12.0, 75)
        cur.multi_rows = [("running", "conversion_rate"), variant_a, variant_b]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.conclude_experiment(1)
        assert result["status"] == "concluded"
        assert result["winner_variant_id"] == 11  # higher conversion_rate

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_conclude_not_running_raises(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [("draft", "conversion_rate")]
        mock_connect.return_value = _FakeConn(cur)
        with pytest.raises(ValueError, match="running"):
            cab.conclude_experiment(1)


# ── S18-11: Get experiment ──────────────────────────────────────────


class TestGetExperiment:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_get_found(self, mock_connect):
        cur = _FakeCursor()
        exp_row = (
            10, "Test", "SKU", "MKT", "running", "hyp", "ctr",
            datetime(2025, 3, 1), None, None, "admin", datetime(2025, 2, 28), None,
        )
        var_row = (
            5, 10, "V1", None, 1, 100, 10, 5, 500.0, 5.0, 10.0, 70, datetime(2025, 3, 1),
        )
        cur.multi_rows = [exp_row, var_row]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.get_experiment(10)
        assert result is not None
        assert result["id"] == 10
        assert len(result["variants"]) == 1

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_get_not_found(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_connect.return_value = _FakeConn(cur)
        result = cab.get_experiment(999)
        assert result is None


# ── S18-12: List experiments ────────────────────────────────────────


class TestListExperiments:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_list_all(self, mock_connect):
        cur = _FakeCursor()
        exp_row = (
            1, "E1", "SKU", "MKT", "draft", None, "ctr",
            None, None, None, None, datetime(2025, 1, 1), None,
        )
        cur.multi_rows = [(1,), exp_row]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.list_experiments()
        assert result["total"] == 1
        assert len(result["items"]) == 1

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_list_empty(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.list_experiments()
        assert result["total"] == 0
        assert result["items"] == []


# ── S18-13: Experiment summary ──────────────────────────────────────


class TestExperimentSummary:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_summary(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [(10, 3, 4, 2, 1)]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.get_experiment_summary()
        assert result["total"] == 10
        assert result["draft"] == 3
        assert result["running"] == 4
        assert result["concluded"] == 2
        assert result["cancelled"] == 1


# ── S18-14: Multilang generate ──────────────────────────────────────


class TestGenerateMultilangContent:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    @patch("app.services.content_ops.compliance.ai_generate")
    def test_generate_single(self, mock_ai, mock_connect):
        mock_ai.return_value = {
            "output": {
                "title": "Kaffeemaschine Premium",
                "bullets": ["Punkt 1", "Punkt 2", "Punkt 3"],
                "description": "Hochwertige Kaffeemaschine " * 10,
                "keywords": "kaffee espresso",
            },
            "model": "gpt-5.2",
            "policy_flags": [],
        }
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_connect.return_value = _FakeConn(cur)

        result = cab.generate_multilang_content(
            seller_sku="SKU1",
            source_marketplace_id="A1PA6795UKMFR9",
            target_marketplace_id="A13V1IB3VIYZZH",
            target_language="fr_FR",
        )
        assert result["seller_sku"] == "SKU1"
        assert result["status"] in cab.VALID_MULTILANG_STATUSES
        mock_ai.assert_called_once()

    @patch("app.intelligence.content_ab_testing.connect_acc")
    @patch("app.services.content_ops.compliance.ai_generate")
    def test_generate_error_handled(self, mock_ai, mock_connect):
        mock_ai.side_effect = Exception("API down")
        cur = _FakeCursor()
        cur.multi_rows = []
        mock_connect.return_value = _FakeConn(cur)

        result = cab.generate_multilang_content(
            seller_sku="SKU1",
            source_marketplace_id="A1PA6795UKMFR9",
            target_marketplace_id="A13V1IB3VIYZZH",
            target_language="fr_FR",
        )
        assert result["status"] == "failed"
        assert "error" in result or "error_message" in result


class TestGenerateAllLanguages:
    @patch("app.intelligence.content_ab_testing.generate_multilang_content")
    def test_generate_all_skips_source(self, mock_gen):
        mock_gen.return_value = {"status": "completed"}
        result = cab.generate_all_languages(
            seller_sku="SKU1",
            source_marketplace_id="A1PA6795UKMFR9",
        )
        assert "results" in result
        # Should not generate for DE (source market)
        calls = mock_gen.call_args_list
        for call in calls:
            assert call.kwargs.get("target_marketplace_id") != "A1PA6795UKMFR9"


class TestGetMultilangJobs:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_jobs_paginated(self, mock_connect):
        cur = _FakeCursor()
        job_row = (
            1, "SKU1", None, "A1PA6795UKMFR9", "A13V1IB3VIYZZH",
            "fr_FR", "completed", None, None, "gpt-5.2",
            85, "[]", "[]", None,
            datetime(2025, 1, 1), datetime(2025, 1, 2),
        )
        cur.multi_rows = [(1,), job_row]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.get_multilang_jobs("SKU1")
        assert result["total"] == 1
        assert len(result["items"]) == 1

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_jobs_empty(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.get_multilang_jobs()
        assert result["total"] == 0


class TestGetMultilangCoverage:
    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_coverage(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [
            ("A13V1IB3VIYZZH", "fr_FR", "completed", 85, datetime(2025, 1, 2)),
            ("APJ6JRA9NG5V4", "it_IT", "pending", None, None),
        ]
        mock_connect.return_value = _FakeConn(cur)
        result = cab.get_multilang_coverage("SKU1", "A1PA6795UKMFR9")
        assert result["seller_sku"] == "SKU1"
        assert len(result["markets"]) == 2


# ── S18-15: API endpoints ──────────────────────────────────────────


class TestContentABAPI:
    @pytest.fixture(autouse=True)
    def _setup(self):
        from app.api.v1.content_ab_testing import router
        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app, raise_server_exceptions=False)

    @patch("app.intelligence.content_ab_testing.get_experiment_summary")
    def test_get_summary(self, mock_fn):
        mock_fn.return_value = {"total": 5, "draft": 2, "running": 1, "concluded": 1, "cancelled": 1}
        r = self.client.get("/content-optimization/experiments/summary")
        assert r.status_code == 200
        assert r.json()["total"] == 5

    @patch("app.intelligence.content_ab_testing.list_experiments")
    def test_list_experiments(self, mock_fn):
        mock_fn.return_value = {"total": 0, "items": []}
        r = self.client.get("/content-optimization/experiments")
        assert r.status_code == 200
        assert r.json()["total"] == 0

    @patch("app.intelligence.content_ab_testing.create_experiment")
    def test_create_experiment(self, mock_fn):
        mock_fn.return_value = {"id": 1, "name": "E1", "status": "draft"}
        r = self.client.post("/content-optimization/experiments", json={
            "name": "E1", "seller_sku": "S", "marketplace_id": "M",
        })
        assert r.status_code == 200
        assert r.json()["id"] == 1

    @patch("app.intelligence.content_ab_testing.create_experiment")
    def test_create_experiment_bad_metric(self, mock_fn):
        mock_fn.side_effect = ValueError("Invalid metric")
        r = self.client.post("/content-optimization/experiments", json={
            "name": "E1", "seller_sku": "S", "marketplace_id": "M",
            "metric_primary": "bad",
        })
        assert r.status_code == 400

    @patch("app.intelligence.content_ab_testing.get_experiment")
    def test_get_experiment(self, mock_fn):
        mock_fn.return_value = {"id": 1, "name": "E1", "variants": []}
        r = self.client.get("/content-optimization/experiments/1")
        assert r.status_code == 200

    @patch("app.intelligence.content_ab_testing.get_experiment")
    def test_get_experiment_not_found(self, mock_fn):
        mock_fn.return_value = None
        r = self.client.get("/content-optimization/experiments/999")
        assert r.status_code == 404

    @patch("app.intelligence.content_ab_testing.add_variant")
    def test_add_variant(self, mock_fn):
        mock_fn.return_value = {"id": 10, "label": "A"}
        r = self.client.post("/content-optimization/experiments/1/variants", json={
            "label": "A", "is_control": True,
        })
        assert r.status_code == 200
        assert r.json()["id"] == 10

    @patch("app.intelligence.content_ab_testing.start_experiment")
    def test_start_experiment(self, mock_fn):
        mock_fn.return_value = {"id": 1, "status": "running"}
        r = self.client.post("/content-optimization/experiments/1/start")
        assert r.status_code == 200
        assert r.json()["status"] == "running"

    @patch("app.intelligence.content_ab_testing.conclude_experiment")
    def test_conclude_experiment(self, mock_fn):
        mock_fn.return_value = {"id": 1, "status": "concluded", "winner_variant_id": 5}
        r = self.client.post("/content-optimization/experiments/1/conclude")
        assert r.status_code == 200
        assert r.json()["winner_variant_id"] == 5

    @patch("app.intelligence.content_ab_testing.record_variant_metrics")
    def test_record_metrics(self, mock_fn):
        mock_fn.return_value = {"variant_id": 5, "updated": True}
        r = self.client.post("/content-optimization/experiments/variants/5/metrics", json={
            "impressions": 100, "clicks": 10,
        })
        assert r.status_code == 200

    @patch("app.intelligence.content_ab_testing.get_multilang_jobs")
    def test_list_multilang_jobs(self, mock_fn):
        mock_fn.return_value = {"total": 0, "items": []}
        r = self.client.get("/content-optimization/multilang/jobs")
        assert r.status_code == 200

    @patch("app.intelligence.content_ab_testing.generate_all_languages")
    def test_generate_multilang(self, mock_fn):
        mock_fn.return_value = {"results": []}
        r = self.client.post("/content-optimization/multilang/generate", json={
            "seller_sku": "SKU1", "source_marketplace_id": "MKT1",
        })
        assert r.status_code == 200

    @patch("app.intelligence.content_ab_testing.get_multilang_coverage")
    def test_multilang_coverage(self, mock_fn):
        mock_fn.return_value = {"seller_sku": "SKU1", "markets": []}
        r = self.client.get("/content-optimization/multilang/coverage/SKU1",
                            params={"source_marketplace_id": "MKT1"})
        assert r.status_code == 200
        assert r.json()["seller_sku"] == "SKU1"


# ── S18-16: Edge cases ──────────────────────────────────────────────


class TestEdgeCases:
    def test_quality_score_max_100(self):
        fields = {
            "title": "Excellent German Title Product " * 3,
            "bullets": [f"Guter Punkt Nummer {i} mit genug Text" for i in range(5)],
            "description": "Ausgezeichnete Produktbeschreibung " * 30,
            "keywords": "keyword1 keyword2 keyword3 keyword4 keyword5",
        }
        score, _ = cab.validate_language_quality(fields, "de_DE")
        assert 0 <= score <= 100

    def test_quality_score_min_0(self):
        score, _ = cab.validate_language_quality({}, "sv_SE")
        assert 0 <= score <= 100

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_conclude_no_variants_raises(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [("running", "conversion_rate")]
        mock_connect.return_value = _FakeConn(cur)
        with pytest.raises(ValueError, match="variant"):
            cab.conclude_experiment(1)

    @patch("app.intelligence.content_ab_testing.connect_acc")
    def test_start_not_found_raises(self, mock_connect):
        cur = _FakeCursor()
        cur.multi_rows = [None]
        mock_connect.return_value = _FakeConn(cur)
        with pytest.raises(ValueError):
            cab.start_experiment(999)
