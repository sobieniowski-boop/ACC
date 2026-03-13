from __future__ import annotations

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import settings


class TestDHLAPI:
    @pytest.mark.asyncio
    async def test_health(self, test_app, auth_headers):
        with patch(
            "app.connectors.dhl24_api.DHL24Client.health_check",
            return_value={
                "ok": True,
                "configured": True,
                "base_url": "https://dhl24.example/ws",
                "write_enabled": False,
                "version": "1.0.0",
                "shipments_probe_count": 7,
                "latency_ms": 12.3,
            },
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get("/api/v1/dhl/health", headers=auth_headers)
        assert response.status_code == 200
        assert response.json()["ok"] is True
        assert response.json()["shipments_probe_count"] == 7

    @pytest.mark.asyncio
    async def test_shipments_count_requires_config(self, test_app, auth_headers):
        old_user = settings.DHL24_API_USERNAME
        old_pwd = settings.DHL24_API_PASSWORD
        try:
            settings.DHL24_API_USERNAME = ""
            settings.DHL24_API_PASSWORD = ""
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get("/api/v1/dhl/shipments/count", headers=auth_headers)
            assert response.status_code == 503
        finally:
            settings.DHL24_API_USERNAME = old_user
            settings.DHL24_API_PASSWORD = old_pwd

    @pytest.mark.asyncio
    async def test_backfill_job_endpoint(self, test_app, auth_headers):
        old_user = settings.DHL24_API_USERNAME
        old_pwd = settings.DHL24_API_PASSWORD
        try:
            settings.DHL24_API_USERNAME = "user"
            settings.DHL24_API_PASSWORD = "pass"
            with patch(
                "app.connectors.mssql.enqueue_job",
                return_value={
                    "id": "00000000-0000-0000-0000-000000000111",
                    "celery_task_id": None,
                    "job_type": "dhl_backfill_shipments",
                    "marketplace_id": None,
                    "trigger_source": "manual",
                    "status": "completed",
                    "progress_pct": 100,
                    "progress_message": "Completed",
                    "records_processed": 12,
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None,
                    "duration_seconds": None,
                    "created_at": "2026-03-06T12:00:00",
                },
            ):
                async with AsyncClient(
                    transport=ASGITransport(app=test_app), base_url="http://test"
                ) as ac:
                    response = await ac.post(
                        "/api/v1/dhl/jobs/backfill?created_from=2026-03-01&created_to=2026-03-05&limit_shipments=50",
                        headers=auth_headers,
                    )
        finally:
            settings.DHL24_API_USERNAME = old_user
            settings.DHL24_API_PASSWORD = old_pwd
        assert response.status_code == 200
        assert response.json()["job_type"] == "dhl_backfill_shipments"
        assert response.json()["records_processed"] == 12

    @pytest.mark.asyncio
    async def test_sync_costs_job_endpoint(self, test_app, auth_headers):
        old_user = settings.DHL24_API_USERNAME
        old_pwd = settings.DHL24_API_PASSWORD
        try:
            settings.DHL24_API_USERNAME = "user"
            settings.DHL24_API_PASSWORD = "pass"
            with patch(
                "app.connectors.mssql.enqueue_job",
                return_value={
                    "id": "00000000-0000-0000-0000-000000000222",
                    "celery_task_id": None,
                    "job_type": "dhl_sync_costs",
                    "marketplace_id": None,
                    "trigger_source": "manual",
                    "status": "completed",
                    "progress_pct": 100,
                    "progress_message": "Completed",
                    "records_processed": 17,
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None,
                    "duration_seconds": None,
                    "created_at": "2026-03-06T12:00:00",
                },
            ):
                async with AsyncClient(
                    transport=ASGITransport(app=test_app), base_url="http://test"
                ) as ac:
                    response = await ac.post(
                        "/api/v1/dhl/jobs/sync-costs?limit_shipments=100&allow_estimated=true",
                        headers=auth_headers,
                    )
        finally:
            settings.DHL24_API_USERNAME = old_user
            settings.DHL24_API_PASSWORD = old_pwd
        assert response.status_code == 200
        assert response.json()["job_type"] == "dhl_sync_costs"
        assert response.json()["records_processed"] == 17

    @pytest.mark.asyncio
    async def test_import_billing_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000444",
                "celery_task_id": None,
                "job_type": "dhl_import_billing_files",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "completed",
                "progress_pct": 100,
                "progress_message": "Completed",
                "records_processed": 123,
                "error_message": None,
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "created_at": "2026-03-06T12:00:00",
            },
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/dhl/jobs/import-billing-files?limit_invoice_files=5&limit_jj_files=2&include_shipment_seed=true",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "dhl_import_billing_files"
        assert response.json()["records_processed"] == 123

    @pytest.mark.asyncio
    async def test_seed_shipments_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000445",
                "celery_task_id": None,
                "job_type": "dhl_seed_shipments_from_staging",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "completed",
                "progress_pct": 100,
                "progress_message": "Completed",
                "records_processed": 77,
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
                    "/api/v1/dhl/jobs/seed-shipments?created_from=2026-02-01&created_to=2026-02-28&limit_parcels=500",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "dhl_seed_shipments_from_staging"
        assert response.json()["records_processed"] == 77

    @pytest.mark.asyncio
    async def test_cost_trace_endpoint(self, test_app, auth_headers):
        with patch(
            "app.services.dhl_observability.get_dhl_cost_trace",
            return_value={
                "count": 1,
                "items": [
                    {
                        "shipment_id": "00000000-0000-0000-0000-000000000001",
                        "shipment_number": "30167116285",
                        "links": [{"amazon_order_id": "ORDER-1", "link_method": "billing_jjd"}],
                        "costs": [{"cost_source": "dhl_billing_files", "gross_amount": 24.87}],
                        "billing_lines": [{"document_number": "1106711106"}],
                    }
                ],
            },
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/dhl/cost-trace?shipment_number=30167116285",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["count"] == 1
        assert response.json()["items"][0]["costs"][0]["cost_source"] == "dhl_billing_files"

    @pytest.mark.asyncio
    async def test_unmatched_shipments_endpoint(self, test_app, auth_headers):
        with patch(
            "app.services.dhl_observability.list_unmatched_dhl_shipments",
            return_value={
                "count": 1,
                "items": [
                    {
                        "shipment_id": "00000000-0000-0000-0000-000000000002",
                        "shipment_number": "30167116286",
                        "reasons": ["missing_order_link", "missing_shipment_cost"],
                    }
                ],
            },
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get("/api/v1/dhl/unmatched-shipments?limit=10", headers=auth_headers)
        assert response.status_code == 200
        assert response.json()["count"] == 1
        assert "missing_order_link" in response.json()["items"][0]["reasons"]

    @pytest.mark.asyncio
    async def test_shadow_diff_endpoint(self, test_app, auth_headers):
        with patch(
            "app.services.dhl_observability.get_dhl_shadow_diff_report",
            return_value={
                "count": 1,
                "items": [
                    {
                        "amazon_order_id": "ORDER-1",
                        "comparison_status": "delta",
                        "delta_abs_pln": 12.5,
                    }
                ],
            },
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.get(
                    "/api/v1/dhl/shadow-diff?purchase_from=2026-02-01&purchase_to=2026-02-28",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["count"] == 1
        assert response.json()["items"][0]["comparison_status"] == "delta"

    @pytest.mark.asyncio
    async def test_shadow_job_endpoint(self, test_app, auth_headers):
        old_user = settings.DHL24_API_USERNAME
        old_pwd = settings.DHL24_API_PASSWORD
        try:
            settings.DHL24_API_USERNAME = "user"
            settings.DHL24_API_PASSWORD = "pass"
            with patch(
                "app.connectors.mssql.enqueue_job",
                return_value={
                    "id": "00000000-0000-0000-0000-000000000333",
                    "celery_task_id": None,
                    "job_type": "dhl_shadow_logistics",
                    "marketplace_id": None,
                    "trigger_source": "manual",
                    "status": "completed",
                    "progress_pct": 100,
                    "progress_message": "Completed",
                    "records_processed": 44,
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None,
                    "duration_seconds": None,
                    "created_at": "2026-03-06T12:00:00",
                },
            ):
                async with AsyncClient(
                    transport=ASGITransport(app=test_app), base_url="http://test"
                ) as ac:
                    response = await ac.post(
                        "/api/v1/dhl/jobs/shadow-logistics?purchase_from=2026-03-01&purchase_to=2026-03-05",
                        headers=auth_headers,
                    )
        finally:
            settings.DHL24_API_USERNAME = old_user
            settings.DHL24_API_PASSWORD = old_pwd
        assert response.status_code == 200
        assert response.json()["job_type"] == "dhl_shadow_logistics"
        assert response.json()["records_processed"] == 44
