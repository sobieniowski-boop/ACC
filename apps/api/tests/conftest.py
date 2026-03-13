"""
Shared fixtures for Family Mapper smoke tests.

Provides:
  - app_client: httpx.AsyncClient against TestClient (no real DB)
  - mock_db: patches pyodbc.connect to return a fake cursor
  - mock_catalog: patches CatalogClient methods
  - auth_headers: valid JWT Bearer headers for test requests
"""
from __future__ import annotations

import json
import sys
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure app is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Fake JWT token (bypass auth for smoke tests)
# ---------------------------------------------------------------------------
@pytest.fixture
def auth_headers():
    """Create valid JWT headers for test requests."""
    from app.core.config import settings
    from jose import jwt

    payload = {
        "sub": "00000000-0000-0000-0000-000000000001",
        "role": "admin",
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "type": "access",
    }
    token = jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Mock pyodbc connection + cursor
# ---------------------------------------------------------------------------
class FakeCursor:
    """Minimal mock of pyodbc cursor for smoke tests."""

    def __init__(self):
        self._results: list[tuple] = []
        self._idx = 0
        self.rowcount = 0
        self.description = None

    def execute(self, sql: str, *args, **kwargs):
        # Return sensible defaults based on query type
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("SELECT COUNT"):
            self._results = [(0,)]
        elif sql_upper.startswith("SELECT"):
            self._results = []
        elif "MERGE" in sql_upper or "OUTPUT" in sql_upper:
            self._results = [(1,)]  # inserted.id
        else:
            self._results = []
        self._idx = 0
        self.rowcount = 1
        return self

    def fetchone(self):
        if self._idx < len(self._results):
            row = self._results[self._idx]
            self._idx += 1
            return row
        return None

    def fetchall(self):
        rows = self._results[self._idx:]
        self._idx = len(self._results)
        return rows

    def close(self):
        pass


class FakeConnection:
    """Minimal mock of pyodbc connection."""

    def __init__(self):
        self._cursor = FakeCursor()

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def close(self):
        pass


@pytest.fixture
def mock_db():
    """Patch pyodbc.connect globally so no real DB is needed."""
    fake_conn = FakeConnection()
    with patch("pyodbc.connect", return_value=fake_conn) as m:
        yield fake_conn


# ---------------------------------------------------------------------------
# Mock SP-API Catalog client
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_catalog():
    """Patch CatalogClient methods to return canned SP-API responses."""
    sample_item = {
        "asin": "B0TEST01DE",
        "summaries": [
            {
                "marketplaceId": "A1PA6795UKMFR9",
                "brandName": "KADAX",
                "itemName": "Test Product",
                "classifications": [{"displayName": "Home & Garden"}],
                "productType": "KITCHENWARE",
            }
        ],
        "relationships": [
            {
                "marketplaceId": "A1PA6795UKMFR9",
                "relationships": [
                    {
                        "childAsins": ["B0CHILD01", "B0CHILD02"],
                        "variationTheme": {"attributes": ["Color", "Size"]},
                    }
                ],
            }
        ],
        "identifiers": [
            {
                "marketplaceId": "A1PA6795UKMFR9",
                "identifiers": [
                    {"identifierType": "EAN", "identifier": "4066991234567"},
                    {"identifierType": "SKU", "identifier": "KDX-TEST-01"},
                ],
            }
        ],
        "attributes": {
            "color": [{"value": "Schwarz"}],
            "size": [{"value": "XL"}],
            "material": [{"value": "Edelstahl"}],
        },
    }

    child1 = {**sample_item, "asin": "B0CHILD01"}
    child2 = {
        **sample_item,
        "asin": "B0CHILD02",
        "attributes": {
            "color": [{"value": "Weiß"}],
            "size": [{"value": "M"}],
        },
    }

    with patch(
        "app.connectors.amazon_sp_api.catalog.CatalogClient.get_item",
        new_callable=AsyncMock,
        return_value=sample_item,
    ) as mock_get, patch(
        "app.connectors.amazon_sp_api.catalog.CatalogClient.get_items_batch",
        new_callable=AsyncMock,
        return_value=[child1, child2],
    ) as mock_batch:
        yield {"get_item": mock_get, "get_items_batch": mock_batch}


# ---------------------------------------------------------------------------
# FastAPI test client (no lifespan = no real scheduler/DB init)
# ---------------------------------------------------------------------------
@pytest.fixture
def test_app():
    """Create a FastAPI test app without lifespan side effects."""
    from fastapi import FastAPI
    from fastapi.responses import ORJSONResponse

    from app.api.v1.router import api_router

    app = FastAPI(default_response_class=ORJSONResponse)
    app.include_router(api_router, prefix="/api/v1")
    return app
