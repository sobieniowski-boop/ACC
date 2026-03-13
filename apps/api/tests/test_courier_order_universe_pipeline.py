from __future__ import annotations

from datetime import date

import pytest

from app.services.courier_order_universe_pipeline import (
    _DEFAULT_MONTHS,
    _carrier_predicate,
    _distribution_order_carrier_predicate,
    run_courier_order_universe_pipeline,
)


def test_run_pipeline_uses_defaults_and_builds_report(monkeypatch):
    monkeypatch.setattr(
        "app.services.courier_order_universe_pipeline.refresh_courier_order_relations",
        lambda **kwargs: {"matrix": {kwargs["months"][0]: {kwargs["carriers"][0]: {"strong_relations": 2}}}},
    )
    monkeypatch.setattr(
        "app.services.courier_order_universe_pipeline.backfill_order_links_order_universe",
        lambda **kwargs: {"carrier": kwargs["carrier"], "shipments_with_primary_link": 11},
    )
    monkeypatch.setattr(
        "app.services.courier_order_universe_pipeline._coverage_snapshot",
        lambda **kwargs: {"orders_universe": 100, "orders_linked_primary": 90, "orders_with_fact": 88},
    )
    monkeypatch.setattr(
        "app.services.courier_order_universe_pipeline._run_aggregate_and_shadow",
        lambda **kwargs: {"aggregate": {"orders_aggregated": 88}, "shadow": {"orders_compared": 88}},
    )

    report = run_courier_order_universe_pipeline(run_aggregate_shadow=True)

    first_month = _DEFAULT_MONTHS[0]
    assert first_month in report
    assert "DHL" in report[first_month]
    assert report[first_month]["DHL"]["relations"]["strong_relations"] == 2
    assert report[first_month]["DHL"]["linking"]["carrier"] == "DHL"
    assert report[first_month]["DHL"]["coverage"]["orders_with_fact"] == 88
    assert report[first_month]["DHL"]["post_process"]["aggregate"]["orders_aggregated"] == 88


def test_run_pipeline_rejects_invalid_inputs():
    with pytest.raises(ValueError):
        run_courier_order_universe_pipeline(months=["bad-month"])

    with pytest.raises(ValueError):
        run_courier_order_universe_pipeline(carriers=["UPS"])


def test_run_pipeline_extends_created_window_for_linking(monkeypatch):
    calls: list[dict] = []
    relation_calls: list[dict] = []

    def _fake_backfill(**kwargs):
        calls.append(kwargs)
        return {"carrier": kwargs["carrier"], "shipments_with_primary_link": 1}

    def _fake_relations(**kwargs):
        relation_calls.append(kwargs)
        return {"matrix": {kwargs["months"][0]: {kwargs["carriers"][0]: {"strong_relations": 1}}}}

    monkeypatch.setattr(
        "app.services.courier_order_universe_pipeline.refresh_courier_order_relations",
        _fake_relations,
    )
    monkeypatch.setattr(
        "app.services.courier_order_universe_pipeline.backfill_order_links_order_universe",
        _fake_backfill,
    )
    monkeypatch.setattr(
        "app.services.courier_order_universe_pipeline._coverage_snapshot",
        lambda **kwargs: {"orders_universe": 1, "orders_linked_primary": 1, "orders_with_fact": 1},
    )

    run_courier_order_universe_pipeline(
        months=["2025-12"],
        carriers=["GLS"],
        run_aggregate_shadow=False,
        created_to_buffer_days=31,
    )

    assert len(calls) == 1
    assert len(relation_calls) == 1
    assert relation_calls[0]["lookahead_days"] == 31
    assert calls[0]["created_from"] == date(2025, 12, 1)
    assert calls[0]["created_to"] == date(2026, 1, 31)


def test_pipeline_carrier_predicates_use_charindex_not_like():
    package_sql = _carrier_predicate("p", "GLS").lower()
    distribution_sql = _distribution_order_carrier_predicate("dco", "DHL").lower()

    assert "charindex('gls'" in package_sql
    assert "charindex('dhl'" in distribution_sql
    assert " like " not in package_sql
    assert " like " not in distribution_sql
