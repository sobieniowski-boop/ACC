from __future__ import annotations

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient


def _queued_job(job_type: str, job_id: str) -> dict:
    return {
        "id": job_id,
        "celery_task_id": None,
        "job_type": job_type,
        "marketplace_id": None,
        "trigger_source": "manual",
        "status": "pending",
        "progress_pct": 0,
        "progress_message": "Queued",
        "records_processed": 0,
        "error_message": None,
        "started_at": None,
        "finished_at": None,
        "duration_seconds": None,
        "created_at": "2026-03-07T10:00:00",
    }


class TestJobifiedEndpoints:
    @pytest.mark.asyncio
    async def test_alerts_get_evaluate_is_rejected(self, test_app):
        async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
            response = await ac.get("/api/v1/alerts?evaluate=true")
        assert response.status_code == 400
        assert "read-only" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_jobs_run_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("cogs_import", "job-001")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/jobs/run", json={"job_type": "cogs_import", "params": {}})
        assert response.status_code == 202
        assert response.json()["job_type"] == "cogs_import"
        assert response.json()["status"] == "pending"

    @pytest.mark.asyncio
    async def test_jobs_import_cogs_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("cogs_import", "job-002")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/jobs/import-cogs")
        assert response.status_code == 202
        assert response.json()["job_type"] == "cogs_import"

    @pytest.mark.asyncio
    async def test_jobs_sync_listings_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("sync_listings_to_products", "job-003")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/jobs/sync-listings?marketplace_ids=A1PA6795UKMFR9")
        assert response.status_code == 202
        assert response.json()["job_type"] == "sync_listings_to_products"

    @pytest.mark.asyncio
    async def test_ads_sync_enqueues(self, test_app, auth_headers):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("sync_ads", "job-004")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/ads/sync?days_back=5", headers=auth_headers)
        assert response.status_code == 202
        assert response.json()["job_type"] == "sync_ads"

    @pytest.mark.asyncio
    async def test_inventory_taxonomy_refresh_enqueues(self, test_app, auth_headers):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("inventory_taxonomy_refresh", "job-005")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/inventory/taxonomy/refresh?limit=1000&min_auto_confidence=0.95&auto_apply=true",
                    headers=auth_headers,
                )
        assert response.status_code == 202
        assert response.json()["job_type"] == "inventory_taxonomy_refresh"

    @pytest.mark.asyncio
    async def test_planning_refresh_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("planning_refresh_actuals", "job-006")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/planning/refresh?year=2026")
        assert response.status_code == 202
        assert response.json()["job_type"] == "planning_refresh_actuals"

    @pytest.mark.asyncio
    async def test_profit_ai_match_run_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("profit_ai_match_run", "job-007")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/profit/v2/ai-match/run")
        assert response.status_code == 202
        assert response.json()["job_type"] == "profit_ai_match_run"

    @pytest.mark.asyncio
    async def test_returns_sync_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("returns_sync_fba", "job-008")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/returns/sync", json={"days_back": 14, "marketplace_ids": ["A1PA6795UKMFR9"]})
        assert response.status_code == 202
        assert response.json()["job_type"] == "returns_sync_fba"

    @pytest.mark.asyncio
    async def test_returns_backfill_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("returns_backfill_fba", "job-009")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/returns/backfill", json={"days_back": 90, "chunk_days": 30})
        assert response.status_code == 202
        assert response.json()["job_type"] == "returns_backfill_fba"

    @pytest.mark.asyncio
    async def test_families_trigger_sync_enqueues(self, test_app, auth_headers):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("family_sync_marketplace_listings", "job-010")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/families/trigger/sync-mp?marketplace_ids=A13V1IB3VIYZZH&family_ids=1,2",
                    headers=auth_headers,
                )
        assert response.status_code == 202
        assert response.json()["job_type"] == "family_sync_marketplace_listings"

    @pytest.mark.asyncio
    async def test_families_trigger_matching_enqueues(self, test_app, auth_headers):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("family_matching_pipeline", "job-011")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/families/trigger/matching?marketplace_ids=A13V1IB3VIYZZH&family_ids=1,2",
                    headers=auth_headers,
                )
        assert response.status_code == 202
        assert response.json()["job_type"] == "family_matching_pipeline"

    @pytest.mark.asyncio
    async def test_import_products_upload_enqueues(self, test_app):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("import_products_upload", "job-012")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/import-products/upload",
                    files={"file": ("import.xlsx", b"fake-xlsx-content", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                )
        assert response.status_code == 202
        assert response.json()["job_type"] == "import_products_upload"

    @pytest.mark.asyncio
    async def test_inventory_apply_job_enqueues(self, test_app, auth_headers):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("inventory_apply_draft", "job-013")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post("/api/v1/inventory/drafts/draft-1/apply-job", headers=auth_headers)
        assert response.status_code == 202
        assert response.json()["job_type"] == "inventory_apply_draft"

    @pytest.mark.asyncio
    async def test_content_refresh_definition_job_enqueues(self, test_app, auth_headers):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("content_refresh_product_type_definition", "job-014")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/content/publish/product-type-definitions/refresh-job",
                    headers=auth_headers,
                    json={"marketplace": "DE", "product_type": "HOME"},
                )
        assert response.status_code == 202
        assert response.json()["job_type"] == "content_refresh_product_type_definition"

    @pytest.mark.asyncio
    async def test_content_apply_mapping_job_enqueues(self, test_app, auth_headers):
        with patch("app.connectors.mssql.enqueue_job", return_value=_queued_job("content_apply_publish_mapping_suggestions", "job-015")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/content/publish/mapping-suggestions/apply-job",
                    headers=auth_headers,
                    json={"marketplaces": ["DE"], "selection": "approved", "min_confidence": 80, "limit": 50, "dry_run": True},
                )
        assert response.status_code == 202
        assert response.json()["job_type"] == "content_apply_publish_mapping_suggestions"
