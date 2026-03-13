from __future__ import annotations

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient


class TestCourierAPI:
    @pytest.mark.asyncio
    async def test_readiness_endpoint(self, test_app, auth_headers):
        payload = {
            "overall_go_no_go": "NO_GO",
            "summary": {"scopes_total": 6, "scopes_go": 0, "scopes_no_go": 6, "running_jobs": 1},
            "matrix": {"2025-11": {"DHL": {"orders_universe": 1, "orders_with_fact": 0, "go_no_go": "NO_GO"}}},
            "latest_jobs": [],
        }
        with patch("app.services.courier_readiness.get_courier_readiness_snapshot", return_value=payload):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/readiness?months=2025-11&carriers=DHL",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["overall_go_no_go"] == "NO_GO"
        assert response.json()["summary"]["running_jobs"] == 1

    @pytest.mark.asyncio
    async def test_evaluate_alerts_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000551",
                "celery_task_id": None,
                "job_type": "courier_evaluate_alerts",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "completed",
                "progress_pct": 100,
                "progress_message": "Completed",
                "records_processed": 2,
                "error_message": None,
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/evaluate-alerts?window_days=14&cost_coverage_min_pct=97&link_coverage_min_pct=96&shadow_delta_max_pct=5",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "courier_evaluate_alerts"
        assert response.json()["records_processed"] == 2

    @pytest.mark.asyncio
    async def test_monthly_kpis_endpoint(self, test_app, auth_headers):
        payload = {
            "months": ["2026-01"],
            "carriers": ["DHL"],
            "rows": 1,
            "missing_pairs": [],
            "items": [
                {
                    "month": "2026-01",
                    "month_start": "2026-01-01",
                    "carrier": "DHL",
                    "calc_version": "dhl_v1",
                    "as_of": "2026-03-09",
                    "buffer_days": 45,
                    "is_closed_by_buffer": True,
                    "month_closed_cutoff": "2026-03-18",
                    "readiness": "NO_GO",
                    "purchase_month": {
                        "orders_universe": 10,
                        "orders_linked_primary": 9,
                        "orders_with_fact": 9,
                        "orders_with_actual_cost": 8,
                        "orders_without_primary_link": 1,
                        "orders_with_estimated_only": 1,
                        "orders_linked_but_no_cost": 0,
                        "orders_missing_actual_cost": 2,
                        "link_coverage_pct": 90.0,
                        "fact_coverage_pct": 90.0,
                        "actual_cost_coverage_pct": 80.0,
                    },
                    "shipment_month": {
                        "shipments_total": 12,
                        "linked_shipments": 11,
                        "costed_shipments_actual": 10,
                        "link_coverage_pct": 91.67,
                        "cost_coverage_pct": 83.33,
                    },
                    "billing_period": {
                        "billed_shipments_total": 10,
                        "billed_shipments_linked": 9,
                        "link_coverage_pct": 90.0,
                    },
                    "explain": [{"code": "estimated_only", "orders": 1}],
                }
            ],
            "matrix": {"2026-01": {"DHL": {"readiness": "NO_GO"}}},
        }
        with patch("app.services.courier_monthly_kpi.get_courier_monthly_kpi_snapshot", return_value=payload):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/monthly-kpis?months=2026-01&carriers=DHL",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["rows"] == 1
        assert response.json()["items"][0]["carrier"] == "DHL"

    @pytest.mark.asyncio
    async def test_order_relations_endpoint(self, test_app, auth_headers):
        payload = {
            "months": ["2026-02"],
            "carriers": ["GLS"],
            "rows": 1,
            "summary": [
                {
                    "month": "2026-02",
                    "carrier": "GLS",
                    "relations_total": 1,
                    "strong_relations": 1,
                    "replacement_relations": 0,
                    "reshipment_relations": 1,
                    "weak_follow_up_relations": 0,
                }
            ],
            "items": [
                {
                    "month": "2026-02",
                    "carrier": "GLS",
                    "source_amazon_order_id": "405-1234567-1234567",
                    "related_distribution_order_id": 2002,
                    "relation_type": "reshipment",
                    "confidence": 0.98,
                    "is_strong": True,
                }
            ],
            "matrix": {"2026-02": {"GLS": {"relations_total": 1, "strong_relations": 1}}},
        }
        with patch("app.services.courier_order_relations.get_courier_order_relations", return_value=payload):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/order-relations?months=2026-02&carriers=GLS&only_strong=true&limit=50",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["summary"][0]["strong_relations"] == 1
        assert response.json()["items"][0]["relation_type"] == "reshipment"

    @pytest.mark.asyncio
    async def test_shipment_outcomes_endpoint(self, test_app, auth_headers):
        payload = {
            "months": ["2026-02"],
            "carriers": ["DHL"],
            "rows": 1,
            "summary": [
                {
                    "month": "2026-02",
                    "carrier": "DHL",
                    "shipments_total": 1,
                    "outcomes": {"delivered": 1},
                    "cost_reasons": {"replacement_shipment": 1},
                }
            ],
            "items": [
                {
                    "month": "2026-02",
                    "carrier": "DHL",
                    "shipment_id": "00000000-0000-0000-0000-000000000101",
                    "outcome_code": "delivered",
                    "cost_reason": "replacement_shipment",
                }
            ],
            "matrix": {"2026-02": {"DHL": {"shipments_total": 1}}},
        }
        with patch("app.services.courier_shipment_semantics.get_courier_shipment_outcomes", return_value=payload):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/shipment-outcomes?months=2026-02&carriers=DHL&limit=25",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["items"][0]["cost_reason"] == "replacement_shipment"

    @pytest.mark.asyncio
    async def test_link_gap_diagnostics_endpoint(self, test_app, auth_headers):
        payload = {
            "months": ["2026-02"],
            "carriers": ["GLS"],
            "rows": 1,
            "scope_type": "purchase_month_with_shipment_buffer",
            "items": [
                {
                    "month": "2026-02",
                    "carrier": "GLS",
                    "summary": {
                        "shipments_in_scope": 95,
                        "shipments_without_primary_link": 25,
                        "shipments_unlinked_with_actual_cost": 9,
                        "shipments_linked_but_no_actual_cost": 9,
                    },
                    "unlinked_buckets": [{"bucket": "gls_bl_map_tracking_number", "shipments": 10}],
                    "cost_gap_buckets": [{"bucket": "seeded_from_billing_source", "shipments": 5}],
                    "sample_unlinked_shipments": [],
                    "sample_linked_no_actual_cost_shipments": [],
                }
            ],
            "matrix": {
                "2026-02": {
                    "GLS": {
                        "shipments_in_scope": 95,
                        "shipments_without_primary_link": 25,
                        "shipments_unlinked_with_actual_cost": 9,
                        "shipments_linked_but_no_actual_cost": 9,
                    }
                }
            },
        }
        with patch("app.services.courier_link_diagnostics.get_courier_link_gap_diagnostics", return_value=payload) as svc_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/link-gap-diagnostics"
                    "?months=2026-02"
                    "&carriers=GLS"
                    "&created_to_buffer_days=45"
                    "&sample_limit=15",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["items"][0]["unlinked_buckets"][0]["bucket"] == "gls_bl_map_tracking_number"
        kwargs = svc_mock.call_args.kwargs
        assert kwargs["months"] == ["2026-02"]
        assert kwargs["carriers"] == ["GLS"]
        assert kwargs["created_to_buffer_days"] == 45
        assert kwargs["sample_limit"] == 15

    @pytest.mark.asyncio
    async def test_link_gap_summary_endpoint(self, test_app, auth_headers):
        payload = {
            "months": ["2026-02"],
            "carriers": ["GLS"],
            "rows": 1,
            "scope_type": "purchase_month_with_shipment_buffer",
            "items": [
                {
                    "month": "2026-02",
                    "carrier": "GLS",
                    "summary": {
                        "orders_universe": 120,
                        "shipments_in_scope": 95,
                        "shipments_with_primary_link": 70,
                        "shipments_without_primary_link": 25,
                        "shipments_without_primary_link_pct": 26.32,
                        "shipments_unlinked_with_actual_cost": 9,
                        "shipments_unlinked_with_estimated_only": 3,
                        "shipments_linked_with_actual_cost": 61,
                        "shipments_linked_but_no_actual_cost": 9,
                        "shipments_linked_estimated_only": 4,
                    },
                    "unlinked_buckets": [{"bucket": "gls_bl_map_tracking_number", "shipments": 10}],
                    "unlinked_source_systems": [{"source_system": "gls_billing_files", "shipments": 12}],
                    "unlinked_identifier_patterns": [{"identifier_pattern": "numeric_core_token", "shipments": 14}],
                }
            ],
            "matrix": {
                "2026-02": {
                    "GLS": {
                        "shipments_in_scope": 95,
                        "shipments_without_primary_link": 25,
                        "shipments_without_primary_link_pct": 26.32,
                        "shipments_unlinked_with_actual_cost": 9,
                    }
                }
            },
        }
        with patch("app.services.courier_link_diagnostics.get_courier_link_gap_summary", return_value=payload) as svc_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/link-gap-summary"
                    "?months=2026-02"
                    "&carriers=GLS"
                    "&created_to_buffer_days=45",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["items"][0]["unlinked_buckets"][0]["bucket"] == "gls_bl_map_tracking_number"
        assert response.json()["items"][0]["unlinked_identifier_patterns"][0]["identifier_pattern"] == "numeric_core_token"
        kwargs = svc_mock.call_args.kwargs
        assert kwargs["months"] == ["2026-02"]
        assert kwargs["carriers"] == ["GLS"]
        assert kwargs["created_to_buffer_days"] == 45

    @pytest.mark.asyncio
    async def test_identifier_source_gaps_endpoint(self, test_app, auth_headers):
        payload = {
            "months": ["2026-02"],
            "carriers": ["GLS", "DHL"],
            "rows": 2,
            "scope_type": "purchase_month_with_shipment_buffer",
            "items": [
                {
                    "month": "2026-02",
                    "carrier": "GLS",
                    "focus_areas": [
                        {
                            "focus_area": "gls_note1_numeric_unmapped",
                            "shipments": 11,
                            "distinct_values": 9,
                        },
                        {
                            "focus_area": "gls_tracking_numeric_unresolved",
                            "shipments": 14,
                            "distinct_values": 12,
                        },
                    ],
                },
                {
                    "month": "2026-02",
                    "carrier": "DHL",
                    "focus_areas": [
                        {
                            "focus_area": "dhl_jjd_like",
                            "shipments": 7,
                            "distinct_values": 7,
                        }
                    ],
                },
            ],
            "matrix": {
                "2026-02": {
                    "GLS": {
                        "gls_note1_numeric_unmapped": {"shipments": 11, "distinct_values": 9},
                    },
                    "DHL": {
                        "dhl_jjd_like": {"shipments": 7, "distinct_values": 7},
                    },
                }
            },
        }
        with patch(
            "app.services.courier_link_diagnostics.get_courier_identifier_source_gap_summary",
            return_value=payload,
        ) as svc_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/identifier-source-gaps"
                    "?months=2026-02"
                    "&carriers=GLS"
                    "&carriers=DHL"
                    "&created_to_buffer_days=45",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["items"][0]["focus_areas"][0]["focus_area"] == "gls_note1_numeric_unmapped"
        kwargs = svc_mock.call_args.kwargs
        assert kwargs["months"] == ["2026-02"]
        assert kwargs["carriers"] == ["GLS", "DHL"]
        assert kwargs["created_to_buffer_days"] == 45

    @pytest.mark.asyncio
    async def test_order_identity_gaps_endpoint(self, test_app, auth_headers):
        payload = {
            "months": ["2026-02"],
            "carriers": ["GLS", "DHL"],
            "rows": 2,
            "scope_type": "purchase_month_with_shipment_buffer",
            "items": [
                {
                    "month": "2026-02",
                    "carrier": "GLS",
                    "focus_areas": [
                        {
                            "focus_area": "gls_note1_order_identity",
                            "shipments": 11,
                            "distinct_values": 9,
                            "values_missing_acc_order": 4,
                            "broken_identity_samples": [{"candidate_value": "93567583"}],
                        }
                    ],
                },
                {
                    "month": "2026-02",
                    "carrier": "DHL",
                    "focus_areas": [
                        {
                            "focus_area": "dhl_numeric_order_identity",
                            "shipments": 25,
                            "distinct_values": 25,
                            "values_missing_acc_order": 3,
                        },
                        {
                            "focus_area": "dhl_jjd_order_identity",
                            "shipments": 25,
                            "distinct_values": 25,
                            "values_missing_acc_order": 1,
                        },
                    ],
                },
            ],
            "matrix": {
                "2026-02": {
                    "GLS": {
                        "gls_note1_order_identity": {
                            "shipments": 11,
                            "distinct_values": 9,
                            "values_missing_acc_order": 4,
                            "shipments_missing_acc_order": 7,
                        }
                    },
                    "DHL": {
                        "dhl_numeric_order_identity": {
                            "shipments": 25,
                            "distinct_values": 25,
                            "values_missing_acc_order": 3,
                            "shipments_missing_acc_order": 9,
                        }
                    },
                }
            },
        }
        with patch(
            "app.services.courier_link_diagnostics.get_courier_order_identity_gap_summary",
            return_value=payload,
        ) as svc_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/courier/order-identity-gaps"
                    "?months=2026-02"
                    "&carriers=GLS"
                    "&carriers=DHL"
                    "&created_to_buffer_days=45"
                    "&sample_limit=5",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["items"][0]["focus_areas"][0]["focus_area"] == "gls_note1_order_identity"
        kwargs = svc_mock.call_args.kwargs
        assert kwargs["months"] == ["2026-02"]
        assert kwargs["carriers"] == ["GLS", "DHL"]
        assert kwargs["created_to_buffer_days"] == 45
        assert kwargs["sample_limit"] == 5

    @pytest.mark.asyncio
    async def test_verify_billing_completeness_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000552",
                "celery_task_id": None,
                "job_type": "courier_verify_billing_completeness",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "completed",
                "progress_pct": 100,
                "progress_message": "Completed",
                "records_processed": 1,
                "error_message": None,
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/verify-billing-completeness?carrier=DHL&billing_period=2026.02",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "courier_verify_billing_completeness"
        assert response.json()["records_processed"] == 1

    @pytest.mark.asyncio
    async def test_refresh_monthly_kpis_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000555",
                "celery_task_id": None,
                "job_type": "courier_refresh_monthly_kpis",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "pending",
                "progress_pct": 0,
                "progress_message": "Queued",
                "records_processed": 0,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 4,
                "next_retry_at": None,
                "last_error_code": None,
                "last_error_kind": None,
                "retry_policy": "standard",
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ) as enqueue_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/refresh-monthly-kpis"
                    "?months=2026-01"
                    "&months=2026-02"
                    "&carriers=dhl"
                    "&carriers=gls"
                    "&buffer_days=60",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "courier_refresh_monthly_kpis"
        params = enqueue_mock.call_args.kwargs["params"]
        assert params["months"] == ["2026-01", "2026-02"]
        assert params["carriers"] == ["DHL", "GLS"]
        assert params["buffer_days"] == 60

    @pytest.mark.asyncio
    async def test_refresh_order_relations_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000556",
                "celery_task_id": None,
                "job_type": "courier_refresh_order_relations",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "pending",
                "progress_pct": 0,
                "progress_message": "Queued",
                "records_processed": 0,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 4,
                "next_retry_at": None,
                "last_error_code": None,
                "last_error_kind": None,
                "retry_policy": "standard",
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ) as enqueue_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/refresh-order-relations"
                    "?months=2026-02"
                    "&carriers=gls"
                    "&lookahead_days=21",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "courier_refresh_order_relations"
        params = enqueue_mock.call_args.kwargs["params"]
        assert params["months"] == ["2026-02"]
        assert params["carriers"] == ["GLS"]
        assert params["lookahead_days"] == 21

    @pytest.mark.asyncio
    async def test_refresh_shipment_outcomes_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000557",
                "celery_task_id": None,
                "job_type": "courier_refresh_shipment_outcomes",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "pending",
                "progress_pct": 0,
                "progress_message": "Queued",
                "records_processed": 0,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 4,
                "next_retry_at": None,
                "last_error_code": None,
                "last_error_kind": None,
                "retry_policy": "standard",
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ) as enqueue_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/refresh-shipment-outcomes"
                    "?months=2026-02"
                    "&carriers=dhl",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "courier_refresh_shipment_outcomes"
        params = enqueue_mock.call_args.kwargs["params"]
        assert params["months"] == ["2026-02"]
        assert params["carriers"] == ["DHL"]

    @pytest.mark.asyncio
    async def test_sync_bl_distribution_cache_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000553",
                "celery_task_id": None,
                "job_type": "sync_bl_distribution_order_cache",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "pending",
                "progress_pct": 0,
                "progress_message": "Queued",
                "records_processed": 0,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 4,
                "next_retry_at": None,
                "last_error_code": None,
                "last_error_kind": None,
                "retry_policy": "standard",
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ) as enqueue_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/sync-bl-distribution-cache"
                    "?date_confirmed_from=2025-12-01"
                    "&date_confirmed_to=2025-12-31"
                    "&source_ids=645"
                    "&source_ids=2952"
                    "&include_packages=false"
                    "&limit_orders=200",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "sync_bl_distribution_order_cache"
        params = enqueue_mock.call_args.kwargs["params"]
        assert params["date_confirmed_from"] == "2025-12-01"
        assert params["date_confirmed_to"] == "2025-12-31"
        assert params["source_ids"] == [645, 2952]
        assert params["include_packages"] is False
        assert params["limit_orders"] == 200

    @pytest.mark.asyncio
    async def test_backfill_identifier_sources_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000555",
                "celery_task_id": None,
                "job_type": "courier_backfill_identifier_sources",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "pending",
                "progress_pct": 0,
                "progress_message": "Queued",
                "records_processed": 0,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 4,
                "next_retry_at": None,
                "last_error_code": None,
                "last_error_kind": None,
                "retry_policy": "standard",
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ) as enqueue_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/backfill-identifier-sources"
                    "?mode=dhl_jjd"
                    "&months=2026-02"
                    "&created_to_buffer_days=45"
                    "&limit_values=150"
                    "&include_packages=true"
                    "&include_bl_orders=true"
                    "&include_dis_map=false"
                    "&include_dhl_parcel_map=true",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "courier_backfill_identifier_sources"
        params = enqueue_mock.call_args.kwargs["params"]
        assert params["mode"] == "dhl_jjd"
        assert params["months"] == ["2026-02"]
        assert params["created_to_buffer_days"] == 45
        assert params["limit_values"] == 150
        assert params["include_packages"] is True
        assert params["include_bl_orders"] is True
        assert params["include_dis_map"] is False
        assert params["include_dhl_parcel_map"] is True

    @pytest.mark.asyncio
    async def test_order_universe_linking_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000554",
                "celery_task_id": None,
                "job_type": "courier_order_universe_linking",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "pending",
                "progress_pct": 0,
                "progress_message": "Queued",
                "records_processed": 0,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 0,
                "next_retry_at": None,
                "last_error_code": None,
                "last_error_kind": None,
                "retry_policy": "none",
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-07T09:00:00",
            },
        ) as enqueue_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/courier/jobs/order-universe-linking"
                    "?months=2025-11"
                    "&months=2025-12"
                    "&months=2026-01"
                    "&carriers=DHL"
                    "&carriers=GLS"
                    "&reset_existing_in_scope=true"
                    "&run_aggregate_shadow=true"
                    "&limit_orders=400000",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "courier_order_universe_linking"
        params = enqueue_mock.call_args.kwargs["params"]
        assert params["months"] == ["2025-11", "2025-12", "2026-01"]
        assert params["carriers"] == ["DHL", "GLS"]
        assert params["reset_existing_in_scope"] is True
        assert params["run_aggregate_shadow"] is True
        assert params["limit_orders"] == 400000
        assert params["created_to_buffer_days"] == 31
