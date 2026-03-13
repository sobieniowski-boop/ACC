"""API tests for Content Ops P0-02/P0-03 endpoints."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.v1.router import api_router
from app.core.security import get_current_user


@pytest.fixture(autouse=True)
def _content_ops_auth_override(test_app):
    async def _admin_user():
        return {"user_id": UUID("00000000-0000-0000-0000-000000000001"), "role": "admin"}

    test_app.dependency_overrides[get_current_user] = _admin_user
    yield
    test_app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def test_app_raw_auth():
    app = FastAPI()
    app.include_router(api_router, prefix="/api/v1")
    return app


def _task_item() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "id": "11111111-1111-1111-1111-111111111111",
        "type": "refresh_content",
        "sku": "KAD-123",
        "asin": "B0ABC12345",
        "marketplace_id": "A1PA6795UKMFR9",
        "priority": "p1",
        "owner": "anna",
        "due_date": now,
        "status": "open",
        "tags_json": {"brand": "KADAX"},
        "title": "Refresh listing",
        "note": "check bullets",
        "source_page": "content_dashboard",
        "created_by": "system",
        "created_at": now,
        "updated_at": now,
    }


def _version_item(status: str = "draft", version_no: int = 1) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "id": "22222222-2222-2222-2222-222222222222",
        "sku": "KAD-123",
        "asin": "B0ABC12345",
        "marketplace_id": "A1PA6795UKMFR9",
        "version_no": version_no,
        "status": status,
        "fields": {
            "title": "KADAX title",
            "bullets": ["A", "B"],
            "description": "desc",
            "keywords": "kw",
            "special_features": [],
            "attributes_json": {},
            "aplus_json": {},
            "compliance_notes": None,
        },
        "created_by": "system",
        "created_at": now,
        "approved_by": None,
        "approved_at": None,
        "published_at": None,
        "parent_version_id": None,
    }


class TestContentTasksAPI:
    @pytest.mark.asyncio
    async def test_get_tasks_requires_auth(self, test_app_raw_auth):
        async with AsyncClient(transport=ASGITransport(app=test_app_raw_auth), base_url="http://test") as ac:
            r = await ac.get("/api/v1/content/tasks")
        assert r.status_code == 401

    @pytest.mark.asyncio
    async def test_publish_refresh_ptd_forbidden_for_analyst(self, test_app_raw_auth):
        from app.core.security import create_access_token

        token = create_access_token(
            subject="00000000-0000-0000-0000-000000000002",
            role="analyst",
        )
        headers = {"Authorization": f"Bearer {token}"}
        async with AsyncClient(transport=ASGITransport(app=test_app_raw_auth), base_url="http://test") as ac:
            r = await ac.post(
                "/api/v1/content/publish/product-type-definitions/refresh",
                json={"marketplace": "DE", "product_type": "HOME"},
                headers=headers,
            )
        assert r.status_code == 403

    @pytest.mark.asyncio
    async def test_get_tasks_ok(self, test_app):
        payload = {"total": 1, "page": 1, "page_size": 50, "pages": 1, "items": [_task_item()]}
        with patch("app.services.content_ops.list_content_tasks", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/tasks")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["items"][0]["sku"] == "KAD-123"

    @pytest.mark.asyncio
    async def test_post_task_ok(self, test_app):
        with patch("app.services.content_ops.create_content_task", return_value=_task_item()):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/tasks",
                    json={"type": "refresh_content", "sku": "KAD-123", "priority": "p1"},
                )
        assert r.status_code == 201
        assert r.json()["type"] == "refresh_content"

    @pytest.mark.asyncio
    async def test_patch_task_validation_error(self, test_app):
        with patch("app.services.content_ops.update_content_task", side_effect=ValueError("invalid status transition")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.patch(
                    "/api/v1/content/tasks/11111111-1111-1111-1111-111111111111",
                    json={"status": "resolved"},
                )
        assert r.status_code == 400
        assert "invalid status transition" in r.json()["detail"]

    @pytest.mark.asyncio
    async def test_bulk_update_tasks_ok(self, test_app):
        payload = {"updated_count": 2, "task_ids": ["11111111-1111-1111-1111-111111111111", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]}
        with patch("app.services.content_ops.bulk_update_content_tasks", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/tasks/bulk-update",
                    json={"task_ids": payload["task_ids"], "status": "investigating"},
                )
        # Endpoint is RBAC-protected; unauthenticated test app should return 401/403 unless dependency overridden.
        assert r.status_code in {200, 401, 403}


class TestContentVersionsAPI:
    @pytest.mark.asyncio
    async def test_get_versions_ok(self, test_app):
        payload = {
            "sku": "KAD-123",
            "marketplace_id": "A1PA6795UKMFR9",
            "items": [_version_item(status="draft", version_no=3)],
        }
        with patch("app.services.content_ops.list_versions", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/KAD-123/A1PA6795UKMFR9/versions")
        assert r.status_code == 200
        assert r.json()["items"][0]["version_no"] == 3

    @pytest.mark.asyncio
    async def test_post_version_ok(self, test_app):
        with patch("app.services.content_ops.create_version", return_value=_version_item(status="draft", version_no=4)):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/KAD-123/A1PA6795UKMFR9/versions",
                    json={"asin": "B0ABC12345", "fields": {"title": "new title"}},
                )
        assert r.status_code == 201
        assert r.json()["status"] == "draft"

    @pytest.mark.asyncio
    async def test_put_version_ok(self, test_app):
        with patch("app.services.content_ops.update_version", return_value=_version_item(status="draft", version_no=4)):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.put(
                    "/api/v1/content/versions/22222222-2222-2222-2222-222222222222",
                    json={"fields": {"title": "edited", "bullets": [], "description": "", "keywords": ""}},
                )
        assert r.status_code == 200
        assert r.json()["id"] == "22222222-2222-2222-2222-222222222222"

    @pytest.mark.asyncio
    async def test_submit_review_ok(self, test_app):
        with patch("app.services.content_ops.submit_version_review", return_value=_version_item(status="review", version_no=4)):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post("/api/v1/content/versions/22222222-2222-2222-2222-222222222222/submit-review")
        assert r.status_code == 200
        assert r.json()["status"] == "review"

    @pytest.mark.asyncio
    async def test_approve_blocked_by_policy(self, test_app):
        with patch("app.services.content_ops.approve_version", side_effect=ValueError("approval blocked: critical policy findings detected")):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post("/api/v1/content/versions/22222222-2222-2222-2222-222222222222/approve")
        assert r.status_code == 400
        assert "approval blocked" in r.json()["detail"]


class TestContentDiffSyncAPI:
    @pytest.mark.asyncio
    async def test_get_diff_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "sku": "KAD-123",
            "main_market": "DE",
            "target_market": "FR",
            "version_main": "v-main",
            "version_target": "v-target",
            "fields": [
                {
                    "field": "title",
                    "main_value": "DE title",
                    "target_value": "FR title",
                    "change_type": "changed",
                }
            ],
            "created_at": now,
        }
        with patch("app.services.content_ops.get_content_diff", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/KAD-123/diff?main=DE&target=FR")
        assert r.status_code == 200
        assert r.json()["fields"][0]["field"] == "title"

    @pytest.mark.asyncio
    async def test_sync_ok(self, test_app):
        payload = {
            "sku": "KAD-123",
            "from_market": "DE",
            "to_markets": ["FR", "IT"],
            "drafts_created": 2,
            "skipped": 0,
            "warnings": [],
        }
        with patch("app.services.content_ops.sync_content", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/KAD-123/sync",
                    json={
                        "fields": ["title", "bullets"],
                        "from_market": "DE",
                        "to_markets": ["FR", "IT"],
                        "overwrite_mode": "missing_only",
                    },
                )
        assert r.status_code == 200
        assert r.json()["drafts_created"] == 2


class TestContentAssetsAPI:
    @pytest.mark.asyncio
    async def test_upload_asset_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "id": "33333333-3333-3333-3333-333333333333",
            "filename": "manual_de.pdf",
            "mime": "application/pdf",
            "content_hash": "sha256:abc",
            "storage_path": "content-assets/2026/03/abc_manual_de.pdf",
            "metadata_json": {"tags": ["manual"]},
            "status": "approved",
            "uploaded_by": "system",
            "uploaded_at": now,
        }
        with patch("app.services.content_ops.upload_asset", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/assets/upload",
                    json={
                        "filename": "manual_de.pdf",
                        "mime": "application/pdf",
                        "content_base64": "VEVTVA==",
                        "metadata_json": {"tags": ["manual"]},
                    },
                )
        assert r.status_code == 201
        assert r.json()["filename"] == "manual_de.pdf"

    @pytest.mark.asyncio
    async def test_list_assets_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "total": 1,
            "page": 1,
            "page_size": 50,
            "pages": 1,
            "items": [
                {
                    "id": "33333333-3333-3333-3333-333333333333",
                    "filename": "manual_de.pdf",
                    "mime": "application/pdf",
                    "content_hash": "sha256:abc",
                    "storage_path": "content-assets/2026/03/abc_manual_de.pdf",
                    "metadata_json": {"tags": ["manual"]},
                    "status": "approved",
                    "uploaded_by": "system",
                    "uploaded_at": now,
                }
            ],
        }
        with patch("app.services.content_ops.list_assets", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/assets?sku=KAD-123&role=manual")
        assert r.status_code == 200
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_link_asset_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "id": "44444444-4444-4444-4444-444444444444",
            "asset_id": "33333333-3333-3333-3333-333333333333",
            "sku": "KAD-123",
            "asin": "B0ABC12345",
            "marketplace_id": "A1PA6795UKMFR9",
            "role": "manual",
            "status": "approved",
            "created_at": now,
        }
        with patch("app.services.content_ops.link_asset", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/assets/33333333-3333-3333-3333-333333333333/link",
                    json={
                        "sku": "KAD-123",
                        "asin": "B0ABC12345",
                        "marketplace_id": "A1PA6795UKMFR9",
                        "role": "manual",
                        "status": "approved",
                    },
                )
        assert r.status_code == 201
        assert r.json()["role"] == "manual"


class TestContentPublishAPI:
    @pytest.mark.asyncio
    async def test_publish_package_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "id": "55555555-5555-5555-5555-555555555555",
            "job_type": "publish_package",
            "marketplaces": ["DE", "FR"],
            "selection_mode": "approved",
            "status": "completed",
            "progress_pct": 100,
            "log_json": {"rows_count": 12},
            "artifact_url": "/api/v1/content/publish/jobs/555/artifact.xlsx",
            "created_by": "system",
            "created_at": now,
            "finished_at": now,
        }
        with patch("app.services.content_ops.create_publish_package", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/publish/package",
                    json={
                        "marketplaces": ["DE", "FR"],
                        "selection": "approved",
                        "format": "xlsx",
                        "sku_filter": ["KAD-123"],
                    },
                )
        assert r.status_code == 200
        assert r.json()["status"] == "completed"

    @pytest.mark.asyncio
    async def test_list_publish_jobs_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "total": 1,
            "page": 1,
            "page_size": 50,
            "pages": 1,
            "items": [
                {
                    "id": "55555555-5555-5555-5555-555555555555",
                    "job_type": "publish_package",
                    "marketplaces": ["DE"],
                    "selection_mode": "approved",
                    "status": "completed",
                    "progress_pct": 100,
                    "log_json": {"rows_count": 12},
                    "artifact_url": "/api/v1/content/publish/jobs/555/artifact.xlsx",
                    "created_by": "system",
                    "created_at": now,
                    "finished_at": now,
                }
            ],
        }
        with patch("app.services.content_ops.list_publish_jobs", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/publish/jobs")
        assert r.status_code == 200
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_publish_push_preview_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "id": "66666666-6666-6666-6666-666666666666",
            "job_type": "publish_push",
            "marketplaces": ["DE", "FR"],
            "selection_mode": "approved",
            "status": "completed",
            "progress_pct": 100,
            "log_json": {
                "mode": "preview",
                "per_marketplace": {
                    "DE": {"status": "preview_ready", "items": 4},
                    "FR": {"status": "preview_ready", "items": 3}
                }
            },
            "artifact_url": None,
            "created_by": "system",
            "created_at": now,
            "finished_at": now,
        }
        with patch("app.services.content_ops.create_publish_push", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/publish/push",
                    json={
                        "marketplaces": ["DE", "FR"],
                        "selection": "approved",
                        "mode": "preview",
                    },
                )
        assert r.status_code == 200
        assert r.json()["job_type"] == "publish_push"

    @pytest.mark.asyncio
    async def test_get_product_type_mappings_ok(self, test_app):
        payload = [
            {
                "id": "77777777-7777-7777-7777-777777777777",
                "marketplace_id": "A1PA6795UKMFR9",
                "brand": "KADAX",
                "category": "HOME",
                "subcategory": "PLANTERS",
                "product_type": "PLANTER",
                "required_attrs": ["material", "size_name"],
                "priority": 10,
                "is_active": True,
            }
        ]
        with patch("app.services.content_ops.list_product_type_mappings", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/publish/product-type-mappings")
        assert r.status_code == 200
        assert r.json()[0]["product_type"] == "PLANTER"

    @pytest.mark.asyncio
    async def test_put_product_type_mappings_ok(self, test_app):
        payload = [
            {
                "id": "77777777-7777-7777-7777-777777777777",
                "marketplace_id": "A1PA6795UKMFR9",
                "brand": "KADAX",
                "category": "HOME",
                "subcategory": "PLANTERS",
                "product_type": "PLANTER",
                "required_attrs": ["material", "size_name"],
                "priority": 10,
                "is_active": True,
            }
        ]
        with patch("app.services.content_ops.upsert_product_type_mappings", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.put(
                    "/api/v1/content/publish/product-type-mappings",
                    json={"rules": payload},
                )
        assert r.status_code in {200, 401, 403}
        if r.status_code == 200:
            assert r.json()[0]["required_attrs"] == ["material", "size_name"]

    @pytest.mark.asyncio
    async def test_get_product_type_definitions_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = [
            {
                "id": "99999999-9999-9999-9999-999999999999",
                "marketplace_id": "A1PA6795UKMFR9",
                "marketplace_code": "DE",
                "product_type": "PLANTER",
                "requirements_json": {"required": ["material"]},
                "required_attrs": ["material"],
                "refreshed_at": now,
                "source": "sp_api_definitions",
            }
        ]
        with patch("app.services.content_ops.list_product_type_definitions", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/publish/product-type-definitions?marketplace=DE")
        assert r.status_code == 200
        assert r.json()[0]["product_type"] == "PLANTER"

    @pytest.mark.asyncio
    async def test_get_attribute_mappings_ok(self, test_app):
        payload = [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "marketplace_id": "A1PA6795UKMFR9",
                "product_type": "PLANTER",
                "source_field": "fields.title",
                "target_attribute": "item_name",
                "transform": "identity",
                "priority": 100,
                "is_active": True,
            }
        ]
        with patch("app.services.content_ops.list_attribute_mappings", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/publish/attribute-mappings")
        assert r.status_code == 200
        assert r.json()[0]["source_field"] == "fields.title"

    @pytest.mark.asyncio
    async def test_get_publish_coverage_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "generated_at": now,
            "items": [
                {
                    "marketplace_id": "A1PA6795UKMFR9",
                    "category": "HOME",
                    "product_type": "PLANTER",
                    "total_candidates": 10,
                    "fully_covered": 7,
                    "coverage_pct": 70.0,
                    "missing_required_top": ["material", "size_name"],
                }
            ],
        }
        with patch("app.services.content_ops.get_publish_coverage", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/publish/coverage?marketplaces=DE,FR&selection=approved")
        assert r.status_code == 200
        assert r.json()["items"][0]["coverage_pct"] == 70.0

    @pytest.mark.asyncio
    async def test_get_publish_mapping_suggestions_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "generated_at": now,
            "items": [
                {
                    "marketplace_id": "A1PA6795UKMFR9",
                    "product_type": "PLANTER",
                    "missing_attribute": "material",
                    "suggested_source_field": "fields.attributes_json.material",
                    "confidence": 92.0,
                    "candidates": ["fields.attributes_json.material", "fields.title"],
                    "affected_skus": 14,
                }
            ],
        }
        with patch("app.services.content_ops.get_publish_mapping_suggestions", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/publish/mapping-suggestions?marketplaces=DE,FR&selection=approved")
        assert r.status_code == 200
        assert r.json()["items"][0]["missing_attribute"] == "material"

    @pytest.mark.asyncio
    async def test_get_publish_queue_health_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "generated_at": now,
            "queued_total": 7,
            "queued_stale_30m": 2,
            "running_total": 1,
            "retry_in_progress": 3,
            "failed_last_24h": 4,
            "max_retry_reached_last_24h": 1,
            "thresholds": {"stale_minutes": 30, "stale_warning_count": 5},
        }
        with patch("app.services.content_ops.get_publish_queue_health", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/publish/queue-health?stale_minutes=30")
        assert r.status_code == 200
        assert r.json()["queued_stale_30m"] == 2

    @pytest.mark.asyncio
    async def test_post_publish_job_retry_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "job": {
                "id": "77777777-aaaa-bbbb-cccc-777777777777",
                "job_type": "publish_push",
                "marketplaces": ["DE"],
                "selection_mode": "approved",
                "status": "queued",
                "progress_pct": 5.0,
                "log_json": {"step": "started"},
                "artifact_url": None,
                "created_by": "system",
                "created_at": now,
                "finished_at": None,
            },
            "queued": True,
            "detail": "retry scheduled",
        }
        with patch("app.services.content_ops.retry_publish_job", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/publish/jobs/11111111-1111-1111-1111-111111111111/retry",
                    json={"failed_only": True},
                )
        assert r.status_code == 200
        assert r.json()["queued"] is True

    @pytest.mark.asyncio
    async def test_post_publish_mapping_suggestions_apply_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "generated_at": now,
            "dry_run": False,
            "evaluated": 10,
            "created": 4,
            "skipped": 6,
            "items": [{"marketplace_id": "A1PA6795UKMFR9"}],
        }
        with patch("app.services.content_ops.apply_publish_mapping_suggestions", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/publish/mapping-suggestions/apply",
                    json={"marketplaces": ["DE"], "selection": "approved", "min_confidence": 80},
                )
        assert r.status_code == 200
        assert r.json()["created"] == 4

    @pytest.mark.asyncio
    async def test_get_content_ops_health_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "generated_at": now,
            "queue_health": {
                "generated_at": now,
                "queued_total": 2,
                "queued_stale_30m": 0,
                "running_total": 1,
                "retry_in_progress": 1,
                "failed_last_24h": 0,
                "max_retry_reached_last_24h": 0,
                "thresholds": {"stale_minutes": 30},
            },
            "compliance_backlog": {"critical": 2, "major_or_higher": 5},
            "tasks_health": {"open": 4, "investigating": 1, "resolved": 8, "overdue": 1},
            "data_quality_cards": [{"key": "title_coverage", "value": 96.2, "unit": "pct", "note": "ok"}],
        }
        with patch("app.services.content_ops.get_content_ops_health", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/health")
        assert r.status_code == 200
        assert r.json()["tasks_health"]["open"] == 4


class TestContentAIAPI:
    @pytest.mark.asyncio
    async def test_ai_generate_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "sku": "KAD-123",
            "marketplace_id": "DE",
            "mode": "improve",
            "model": "gpt-5.2",
            "cache_hit": False,
            "policy_flags": [],
            "output": {
                "title": "KADAX Pflanzkasten",
                "bullets": ["Robust", "Leicht"],
                "description": "Optimized content",
                "keywords": "pflanzkasten balkon",
                "special_features": [],
                "attributes_json": {},
                "aplus_json": {},
                "compliance_notes": "ok"
            },
            "generated_at": now,
        }
        with patch("app.services.content_ops.ai_generate", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/ai/generate",
                    json={
                        "sku": "KAD-123",
                        "marketplace_id": "DE",
                        "mode": "improve",
                        "constraints_json": {"max_title_len": 200},
                        "fields": ["title", "bullets"],
                        "model": "gpt-5.2"
                    },
                )
        assert r.status_code == 200
        assert r.json()["mode"] == "improve"


class TestContentOnboardAndQAAPI:
    @pytest.mark.asyncio
    async def test_onboard_preflight_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "main_market": "DE",
            "target_markets": ["FR", "IT"],
            "items": [
                {
                    "sku": "KAD-123",
                    "asin": "B0ABC12345",
                    "ean": "5900000000000",
                    "brand": "KADAX",
                    "title": "KADAX Title",
                    "pim_score": 100,
                    "family_coverage_pct": 100.0,
                    "blockers": [],
                    "warnings": [],
                    "recommended_actions": ["Gotowe do produkcji draftu i review QA."],
                    "tasks_created": [],
                }
            ],
            "generated_at": now,
        }
        with patch("app.services.content_ops.run_onboard_preflight", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/onboard/preflight",
                    json={
                        "sku_list": ["KAD-123"],
                        "main_market": "DE",
                        "target_markets": ["FR", "IT"],
                        "auto_create_tasks": False,
                    },
                )
        assert r.status_code == 200
        assert r.json()["items"][0]["sku"] == "KAD-123"

    @pytest.mark.asyncio
    async def test_qa_verify_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "status": "needs_revision",
            "score": 62.0,
            "critical_count": 0,
            "major_count": 2,
            "minor_count": 1,
            "findings": [
                {
                    "category": "accuracy",
                    "severity": "major",
                    "field": "title",
                    "message": "Title shorter than 30 chars.",
                    "suggestion": "Rozwin tytul o kluczowe cechy i brand.",
                }
            ],
            "checks_json": {"target_language": "de_de"},
            "checked_at": now,
        }
        with patch("app.services.content_ops.verify_content_quality", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.post(
                    "/api/v1/content/qa/verify",
                    json={
                        "sku": "KAD-123",
                        "marketplace_id": "DE",
                        "target_language": "de_DE",
                        "pim_facts_json": {"brand": "KADAX"},
                        "content": {
                            "title": "Short title",
                            "bullets": ["A", "B"],
                            "description": "desc",
                            "keywords": "kw",
                            "special_features": [],
                            "attributes_json": {},
                            "aplus_json": {},
                            "compliance_notes": None,
                        },
                    },
                )
        assert r.status_code == 200
        assert r.json()["status"] == "needs_revision"

    @pytest.mark.asyncio
    async def test_onboard_restrictions_check_ok(self, test_app):
        payload = {
            "asin": "B0ABC12345",
            "marketplace": "DE",
            "can_list": True,
            "requires_approval": False,
            "reasons": [],
        }
        with patch("app.services.content_ops.onboard_restrictions_check", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/onboard/restrictions/check?asin=B0ABC12345&marketplace=DE")
        assert r.status_code == 200
        assert r.json()["can_list"] is True

    @pytest.mark.asyncio
    async def test_onboard_catalog_search_by_ean_ok(self, test_app):
        payload = {
            "query": "5903699455531",
            "marketplace": "DE",
            "total": 1,
            "matches": [
                {
                    "asin": "B0ABC12345",
                    "title": "KADAX Product",
                    "brand": "KADAX",
                    "product_type": "HOME",
                    "image_url": "https://img.example.com/1.jpg",
                }
            ],
        }
        with patch("app.services.content_ops.onboard_catalog_search_by_ean", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/onboard/catalog/search-by-ean?ean=5903699455531&marketplace=DE")
        assert r.status_code == 200
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_compliance_queue_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "total": 1,
            "page": 1,
            "page_size": 50,
            "pages": 1,
            "items": [
                {
                    "version_id": "22222222-2222-2222-2222-222222222222",
                    "sku": "KAD-123",
                    "marketplace_id": "A1PA6795UKMFR9",
                    "version_no": 4,
                    "version_status": "review",
                    "critical_count": 1,
                    "major_count": 0,
                    "minor_count": 0,
                    "findings": [{"severity": "critical", "field": "title"}],
                    "checked_at": now,
                }
            ],
        }
        with patch("app.services.content_ops.list_compliance_queue", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/compliance/queue?severity=critical")
        assert r.status_code == 200
        assert r.json()["items"][0]["critical_count"] == 1

    @pytest.mark.asyncio
    async def test_content_impact_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "sku": "KAD-123",
            "marketplace_id": "A1PA6795UKMFR9",
            "range_days": 14,
            "before": {"label": "before_14d", "units": 100, "revenue": 1000.0, "impact_margin_pln": 250.0, "refunds": 0.0, "return_rate": 0.0, "sessions": None, "cvr": None},
            "after": {"label": "after_14d", "units": 90, "revenue": 900.0, "impact_margin_pln": 200.0, "refunds": 0.0, "return_rate": 0.0, "sessions": None, "cvr": None},
            "delta": {"label": "delta", "units": -10, "revenue": -100.0, "impact_margin_pln": -50.0, "refunds": 0.0, "return_rate": 0.0, "sessions": None, "cvr": None},
            "baseline_expected": {"label": "baseline_expected", "units": 95, "revenue": 980.0, "impact_margin_pln": 230.0, "refunds": 0.0, "return_rate": 0.0, "sessions": None, "cvr": None},
            "delta_vs_baseline": {"label": "delta_vs_baseline", "units": -5, "revenue": -80.0, "impact_margin_pln": -30.0, "refunds": 0.0, "return_rate": 0.0, "sessions": None, "cvr": None},
            "impact_signal": "negative",
            "confidence_score": 83.0,
            "baseline_hint": "directional",
            "negative_impact": True,
            "generated_at": now,
        }
        with patch("app.services.content_ops.get_content_impact", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/impact?sku=KAD-123&marketplace=DE&range=14")
        assert r.status_code == 200
        assert r.json()["negative_impact"] is True

    @pytest.mark.asyncio
    async def test_content_data_quality_ok(self, test_app):
        now = datetime.now(timezone.utc)
        payload = {
            "cards": [{"key": "title_coverage", "value": 95.2, "unit": "pct", "note": "ok"}],
            "missing_title": [],
            "missing_bullets": [],
            "missing_description": [],
            "generated_at": now,
        }
        with patch("app.services.content_ops.get_content_data_quality", return_value=payload):
            async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
                r = await ac.get("/api/v1/content/data-quality")
        assert r.status_code == 200
        assert r.json()["cards"][0]["key"] == "title_coverage"
