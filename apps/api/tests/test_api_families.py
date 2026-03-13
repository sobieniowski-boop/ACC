"""
Smoke tests — Family Mapper API endpoints (httpx + mock DB).

Tests all 15 endpoints return valid HTTP status codes and expected
JSON shapes without touching a real database.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_cursor(rows=None, count=0):
    """Build a FakeCursor that returns *rows* for fetchall / *count* for COUNT."""
    cur = MagicMock()
    _results = list(rows or [])

    def _execute(sql, *a, **kw):
        sql_up = sql.strip().upper()
        if "COUNT" in sql_up:
            cur.fetchone.return_value = (count,)
        else:
            cur.fetchone.return_value = _results[0] if _results else None
            cur.fetchall.return_value = _results
        cur.rowcount = len(_results) or 1

    cur.execute = MagicMock(side_effect=_execute)
    cur.fetchone = MagicMock(return_value=(count,))
    cur.fetchall = MagicMock(return_value=_results)
    cur.close = MagicMock()
    return cur


def _fake_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.close = MagicMock()
    conn.commit = MagicMock()
    return conn


# ---------------------------------------------------------------------------
# 1) GET /families
# ---------------------------------------------------------------------------
class TestListFamilies:
    @pytest.mark.asyncio
    async def test_empty_list(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families", headers=auth_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["items"] == []
        assert body["total"] == 0

    @pytest.mark.asyncio
    async def test_with_search(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families?search=KADAX", headers=auth_headers)
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# 2) GET /families/{id}
# ---------------------------------------------------------------------------
class TestGetFamily:
    @pytest.mark.asyncio
    async def test_not_found(self, test_app, auth_headers):
        cur = _fake_cursor()
        cur.fetchone.return_value = None
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/999", headers=auth_headers)
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_found(self, test_app, auth_headers):
        row = (1, "B0PARENT", "KADAX", "Home", "KITCHENWARE", "Color", "2024-01-01")
        cur = MagicMock()
        call_count = [0]

        def _exec(sql, *a, **kw):
            call_count[0] += 1
            sql_up = sql.strip().upper()
            if call_count[0] == 1:
                # First SELECT — family detail
                cur.fetchone.return_value = row
                cur.fetchall.return_value = []
            else:
                cur.fetchone.return_value = None
                cur.fetchall.return_value = []

        cur.execute = MagicMock(side_effect=_exec)
        cur.fetchone = MagicMock(return_value=row)
        cur.fetchall = MagicMock(return_value=[])
        cur.close = MagicMock()
        conn = _fake_conn(cur)

        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/1", headers=auth_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["de_parent_asin"] == "B0PARENT"


# ---------------------------------------------------------------------------
# 3) GET /families/{id}/children
# ---------------------------------------------------------------------------
class TestGetChildren:
    @pytest.mark.asyncio
    async def test_empty_children(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/1/children", headers=auth_headers)
        assert r.status_code == 200
        assert r.json() == []


# ---------------------------------------------------------------------------
# 4) GET /families/{id}/links
# ---------------------------------------------------------------------------
class TestGetLinks:
    @pytest.mark.asyncio
    async def test_empty_links(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/1/links", headers=auth_headers)
        assert r.status_code == 200
        assert r.json() == []


# ---------------------------------------------------------------------------
# 5) PUT /families/{id}/links/status
# ---------------------------------------------------------------------------
class TestUpdateLinkStatus:
    @pytest.mark.asyncio
    async def test_update_link(self, test_app, auth_headers):
        cur = _fake_cursor()
        cur.rowcount = 1
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.put(
                    "/api/v1/families/1/links/status",
                    headers=auth_headers,
                    json={"status": "approved", "master_key": "KDX-001", "marketplace": "PL"},
                )
        assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_update_link_not_found(self, test_app, auth_headers):
        cur = _fake_cursor()
        # Make rowcount 0 so HTTPException(404) fires
        def _exec(sql, *a, **kw):
            cur.rowcount = 0
        cur.execute = MagicMock(side_effect=_exec)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.put(
                    "/api/v1/families/1/links/status",
                    headers=auth_headers,
                    json={"status": "approved", "master_key": "NOEXIST", "marketplace": "PL"},
                )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# 6) GET /families/{id}/coverage
# ---------------------------------------------------------------------------
class TestGetCoverage:
    @pytest.mark.asyncio
    async def test_empty(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/1/coverage", headers=auth_headers)
        assert r.status_code == 200
        assert r.json() == []


# ---------------------------------------------------------------------------
# 7) GET /families/{id}/issues
# ---------------------------------------------------------------------------
class TestGetIssues:
    @pytest.mark.asyncio
    async def test_empty(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/1/issues", headers=auth_headers)
        assert r.status_code == 200
        assert r.json() == []


# ---------------------------------------------------------------------------
# 8) POST /families/trigger/rebuild-de
# ---------------------------------------------------------------------------
class TestTriggerRebuildDE:
    @pytest.mark.asyncio
    async def test_ok(self, test_app, auth_headers, mock_db):
        with patch(
            "app.api.v1.families.rebuild_de_canonical",
            new_callable=AsyncMock,
            return_value={"families_upserted": 0, "children_upserted": 0, "parents_processed": 0},
        ), patch(
            "app.api.v1.families.get_rebuild_status",
            return_value={"running": False},
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.post(
                    "/api/v1/families/trigger/rebuild-de",
                    headers=auth_headers,
                )
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "started"


# ---------------------------------------------------------------------------
# 9) POST /families/trigger/sync-mp
# ---------------------------------------------------------------------------
class TestTriggerSyncMP:
    @pytest.mark.asyncio
    async def test_ok(self, test_app, auth_headers, mock_db):
        with patch(
            "app.api.v1.families.enqueue_job",
            return_value={"id": "job-1", "job_type": "family_sync_marketplace_listings", "trigger_source": "manual", "status": "queued", "progress_pct": 0, "created_at": "2026-01-01T00:00:00"},
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.post(
                    "/api/v1/families/trigger/sync-mp",
                    headers=auth_headers,
                )
        assert r.status_code == 202


# ---------------------------------------------------------------------------
# 10) POST /families/trigger/matching
# ---------------------------------------------------------------------------
class TestTriggerMatching:
    @pytest.mark.asyncio
    async def test_ok(self, test_app, auth_headers, mock_db):
        with patch(
            "app.api.v1.families.enqueue_job",
            return_value={"id": "job-2", "job_type": "family_matching_pipeline", "trigger_source": "manual", "status": "queued", "progress_pct": 0, "created_at": "2026-01-01T00:00:00"},
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.post(
                    "/api/v1/families/trigger/matching",
                    headers=auth_headers,
                )
        assert r.status_code == 202


# ---------------------------------------------------------------------------
# 11) GET /families/review
# ---------------------------------------------------------------------------
class TestReviewQueue:
    @pytest.mark.asyncio
    async def test_empty(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/review", headers=auth_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["items"] == []
        assert body["total"] == 0


# ---------------------------------------------------------------------------
# 12) GET /families/fix-packages
# ---------------------------------------------------------------------------
class TestFixPackages:
    @pytest.mark.asyncio
    async def test_empty(self, test_app, auth_headers):
        cur = _fake_cursor(rows=[], count=0)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.get("/api/v1/families/fix-packages", headers=auth_headers)
        assert r.status_code == 200
        body = r.json()
        assert body["items"] == []


# ---------------------------------------------------------------------------
# 13) POST /families/fix-packages/generate
# ---------------------------------------------------------------------------
class TestGenerateFixPackages:
    @pytest.mark.asyncio
    async def test_ok(self, test_app, auth_headers, mock_db):
        with patch(
            "app.api.v1.families.generate_fix_package",
            new_callable=AsyncMock,
            return_value={"packages_generated": 0},
        ):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.post(
                    "/api/v1/families/fix-packages/generate",
                    headers=auth_headers,
                )
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# 14) POST /families/fix-packages/{id}/approve
# ---------------------------------------------------------------------------
class TestApproveFixPackage:
    @pytest.mark.asyncio
    async def test_approve(self, test_app, auth_headers):
        cur = _fake_cursor()
        cur.rowcount = 1
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.post(
                    "/api/v1/families/fix-packages/1/approve",
                    headers=auth_headers,
                    json={"approved_by": "test-user"},
                )
        assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_approve_not_found(self, test_app, auth_headers):
        cur = _fake_cursor()
        def _exec(sql, *a, **kw):
            cur.rowcount = 0
        cur.execute = MagicMock(side_effect=_exec)
        conn = _fake_conn(cur)
        with patch("app.api.v1.families._connect", return_value=conn):
            async with AsyncClient(
                transport=ASGITransport(app=test_app), base_url="http://test"
            ) as ac:
                r = await ac.post(
                    "/api/v1/families/fix-packages/999/approve",
                    headers=auth_headers,
                    json={"approved_by": "test-user"},
                )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# 15) GET /families/marketplaces — static data, no DB needed
# ---------------------------------------------------------------------------
class TestMarketplaces:
    @pytest.mark.asyncio
    async def test_lists_all(self, test_app, auth_headers):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as ac:
            r = await ac.get("/api/v1/families/marketplaces", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) >= 1  # At least 1 marketplace in MARKETPLACE_REGISTRY
        # Check shape
        first = data[0]
        assert "marketplace_id" in first
        assert "code" in first


# ---------------------------------------------------------------------------
# Auth tests
# ---------------------------------------------------------------------------
class TestAuthRequired:
    """Endpoints should reject requests without valid JWT."""

    @pytest.mark.asyncio
    async def test_list_families_no_auth(self, test_app):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as ac:
            r = await ac.get("/api/v1/families")
        assert r.status_code in (401, 403)

    @pytest.mark.asyncio
    async def test_trigger_no_auth(self, test_app):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as ac:
            r = await ac.post("/api/v1/families/trigger/rebuild-de")
        assert r.status_code in (401, 403)
