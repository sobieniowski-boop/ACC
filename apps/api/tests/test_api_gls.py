from __future__ import annotations

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient


class TestGLSAPI:
    @pytest.mark.asyncio
    async def test_import_billing_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000111",
                "celery_task_id": None,
                "job_type": "gls_import_billing_files",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "completed",
                "progress_pct": 100,
                "progress_message": "Completed",
                "records_processed": 42,
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
                    "/api/v1/gls/jobs/import-billing-files?limit_invoice_files=5&include_shipment_seed=true",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "gls_import_billing_files"
        assert response.json()["records_processed"] == 42

    @pytest.mark.asyncio
    async def test_sync_costs_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000222",
                "celery_task_id": None,
                "job_type": "gls_sync_costs",
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
        ) as enqueue_mock:
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                response = await ac.post(
                    "/api/v1/gls/jobs/sync-costs"
                    "?created_from=2026-02-01"
                    "&created_to=2026-02-28"
                    "&billing_periods=2026.01"
                    "&billing_periods=2026-02"
                    "&limit_shipments=100"
                    "&seeded_only=true"
                    "&only_primary_linked=true",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "gls_sync_costs"
        assert response.json()["records_processed"] == 17
        params = enqueue_mock.call_args.kwargs["params"]
        assert params["created_from"] == "2026-02-01"
        assert params["created_to"] == "2026-02-28"
        assert params["billing_periods"] == ["2026.01", "2026-02"]
        assert params["seeded_only"] is True
        assert params["only_primary_linked"] is True

    @pytest.mark.asyncio
    async def test_seed_shipments_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000223",
                "celery_task_id": None,
                "job_type": "gls_seed_shipments_from_staging",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "completed",
                "progress_pct": 100,
                "progress_message": "Completed",
                "records_processed": 81,
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
                    "/api/v1/gls/jobs/seed-shipments?created_from=2026-02-01&created_to=2026-02-28&limit_parcels=500",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "gls_seed_shipments_from_staging"
        assert response.json()["records_processed"] == 81

    @pytest.mark.asyncio
    async def test_aggregate_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000333",
                "celery_task_id": None,
                "job_type": "gls_aggregate_logistics",
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
                    "/api/v1/gls/jobs/aggregate-logistics?created_from=2026-02-01&created_to=2026-02-28&limit_orders=100",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "gls_aggregate_logistics"
        assert response.json()["records_processed"] == 12

    @pytest.mark.asyncio
    async def test_shadow_job_endpoint(self, test_app, auth_headers):
        with patch(
            "app.connectors.mssql.enqueue_job",
            return_value={
                "id": "00000000-0000-0000-0000-000000000444",
                "celery_task_id": None,
                "job_type": "gls_shadow_logistics",
                "marketplace_id": None,
                "trigger_source": "manual",
                "status": "completed",
                "progress_pct": 100,
                "progress_message": "Completed",
                "records_processed": 9,
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
                    "/api/v1/gls/jobs/shadow-logistics?purchase_from=2026-02-01&purchase_to=2026-02-28&limit_orders=100",
                    headers=auth_headers,
                )
        assert response.status_code == 200
        assert response.json()["job_type"] == "gls_shadow_logistics"
        assert response.json()["records_processed"] == 9
