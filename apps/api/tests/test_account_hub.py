"""Tests for Sprint 25-26 – Account Hub (Multi-seller support).

Covers: constants, encryption helpers, row mappers, seller CRUD,
credential vault (store/get/list/revoke/validate), permission
management (grant/revoke/list/check), scheduler status, dashboard,
schema DDL, API endpoints.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.intelligence import account_hub as ah

# ── Local fakes ──────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, rowcount=1):
        self.executed: list[tuple] = []
        self.multi_rows: list = []
        self._idx = 0
        self._rowcount = rowcount

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

    @property
    def rowcount(self):
        return self._rowcount


class _FakeConn:
    def __init__(self, cursor=None):
        self._cursor = cursor or _FakeCursor()

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


# ── Row helpers ──────────────────────────────────────────────────────

def _make_seller_row(
    id=1, seller_id="A1O0TEST", name="Test Seller",
    company_name="Test Co",
    marketplace_ids='["A1PA6795UKMFR9"]',
    primary_marketplace="A1PA6795UKMFR9",
    region="eu", status="active", notes=None,
    created_at=datetime(2025, 3, 15, 10, 0),
    updated_at=datetime(2025, 3, 15, 10, 0),
):
    return (
        id, seller_id, name, company_name, marketplace_ids,
        primary_marketplace, region, status, notes,
        created_at, updated_at,
    )


def _make_cred_row(
    id=1, credential_type="sp_api", credential_key="refresh_token",
    is_valid=1, expires_at=None, last_validated_at=None,
    created_at=datetime(2025, 3, 15, 10, 0),
    updated_at=datetime(2025, 3, 15, 10, 0),
):
    return (
        id, credential_type, credential_key, is_valid,
        expires_at, last_validated_at, created_at, updated_at,
    )


def _make_perm_row(
    id=1, user_email="user@test.com", permission_level="read_only",
    granted_by="admin", granted_at=datetime(2025, 3, 15, 10, 0),
):
    return (id, user_email, permission_level, granted_by, granted_at)


# ====================================================================
# 1. Constants
# ====================================================================

class TestConstants:
    def test_seller_statuses(self):
        for s in ("active", "inactive", "suspended", "onboarding"):
            assert s in ah.SELLER_STATUSES

    def test_credential_types(self):
        assert ah.CREDENTIAL_TYPES == {"sp_api", "ads_api", "lwa"}

    def test_permission_levels(self):
        for l in ("admin", "full", "read_only", "none"):
            assert l in ah.PERMISSION_LEVELS


# ====================================================================
# 2. Encryption helpers
# ====================================================================

class TestEncryption:
    @patch("app.intelligence.account_hub.settings")
    def test_derive_key_deterministic(self, mock_settings):
        mock_settings.SECRET_KEY = "test-secret-key-for-testing"
        k1 = ah._derive_key()
        k2 = ah._derive_key()
        assert k1 == k2
        assert len(k1) == 44  # base64 of 32 bytes

    @patch("app.intelligence.account_hub.settings")
    def test_encrypt_decrypt_roundtrip(self, mock_settings):
        mock_settings.SECRET_KEY = "test-secret-key-for-testing"
        plaintext = "my-secret-token-value"
        encrypted = ah._encrypt(plaintext)
        assert encrypted != plaintext
        decrypted = ah._decrypt(encrypted)
        assert decrypted == plaintext

    @patch("app.intelligence.account_hub.settings")
    def test_encrypted_value_not_plaintext(self, mock_settings):
        mock_settings.SECRET_KEY = "another-secret-key"
        plaintext = "refresh_token_abc123"
        encrypted = ah._encrypt(plaintext)
        assert plaintext not in encrypted


# ====================================================================
# 3. Row mappers
# ====================================================================

class TestRowMappers:
    def test_row_to_seller(self):
        row = _make_seller_row()
        d = ah._row_to_seller(row)
        assert d["id"] == 1
        assert d["seller_id"] == "A1O0TEST"
        assert d["name"] == "Test Seller"
        assert d["marketplace_ids"] == ["A1PA6795UKMFR9"]
        assert d["status"] == "active"
        assert d["region"] == "eu"

    def test_row_to_seller_null_marketplaces(self):
        row = _make_seller_row(marketplace_ids=None)
        d = ah._row_to_seller(row)
        assert d["marketplace_ids"] == []

    def test_row_to_seller_onboarding(self):
        row = _make_seller_row(status="onboarding")
        d = ah._row_to_seller(row)
        assert d["status"] == "onboarding"


# ====================================================================
# 4. Seller Account CRUD
# ====================================================================

class TestSellerAccounts:
    @patch("app.intelligence.account_hub.connect_acc")
    def test_list_sellers_empty(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(0,)]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.list_seller_accounts()
        assert result["total"] == 0
        assert result["items"] == []

    @patch("app.intelligence.account_hub.connect_acc")
    def test_list_sellers_with_rows(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(2,), _make_seller_row(id=1), _make_seller_row(id=2, name="Seller 2")]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.list_seller_accounts()
        assert result["total"] == 2
        assert len(result["items"]) == 2

    @patch("app.intelligence.account_hub.connect_acc")
    def test_list_sellers_status_filter(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(1,), _make_seller_row(status="active")]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.list_seller_accounts(status="active")
        assert result["total"] == 1
        sql_text = cur.executed[0][0]
        assert "status" in sql_text.lower()

    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_seller_found(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_seller_row(id=5)]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_seller_account(5)
        assert result is not None
        assert result["id"] == 5

    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_seller_not_found(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_seller_account(999)
        assert result is None

    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_seller_by_seller_id(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [_make_seller_row(seller_id="A1XTEST")]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_seller_by_seller_id("A1XTEST")
        assert result is not None
        assert result["seller_id"] == "A1XTEST"

    @patch("app.intelligence.account_hub.connect_acc")
    def test_create_seller(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(42,)]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.create_seller_account(
            seller_id="A1NEWTEST",
            name="New Seller",
            marketplace_ids=["A1PA6795UKMFR9", "A1RKKUPIHCS9HS"],
        )
        assert result["id"] == 42
        assert result["seller_id"] == "A1NEWTEST"
        assert result["status"] == "onboarding"

    @patch("app.intelligence.account_hub.connect_acc")
    def test_update_seller_status(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.update_seller_account(1, status="active")
        assert result is not None
        assert result["updated"] is True

    @patch("app.intelligence.account_hub.connect_acc")
    def test_update_seller_not_found(self, mock_conn):
        cur = _FakeCursor(rowcount=0)
        mock_conn.return_value = _FakeConn(cur)

        result = ah.update_seller_account(999, status="active")
        assert result is None


# ====================================================================
# 5. Credential Vault
# ====================================================================

class TestCredentialVault:
    @patch("app.intelligence.account_hub._encrypt")
    @patch("app.intelligence.account_hub.connect_acc")
    def test_store_credential(self, mock_conn, mock_encrypt):
        mock_encrypt.return_value = "encrypted_value"
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.store_credential(
            seller_account_id=1,
            credential_type="sp_api",
            credential_key="refresh_token",
            plaintext_value="my-secret-token",
        )
        assert result["stored"] is True
        assert result["credential_type"] == "sp_api"
        mock_encrypt.assert_called_once_with("my-secret-token")

    @patch("app.intelligence.account_hub._decrypt")
    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_credential_found(self, mock_conn, mock_decrypt):
        mock_decrypt.return_value = "decrypted_token"
        cur = _FakeCursor()
        cur.multi_rows = [("encrypted_data",)]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_credential(
            seller_account_id=1,
            credential_type="sp_api",
            credential_key="refresh_token",
        )
        assert result == "decrypted_token"

    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_credential_not_found(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_credential(
            seller_account_id=1,
            credential_type="sp_api",
            credential_key="nonexistent",
        )
        assert result is None

    @patch("app.intelligence.account_hub.connect_acc")
    def test_list_credentials(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            _make_cred_row(id=1, credential_key="refresh_token"),
            _make_cred_row(id=2, credential_key="client_id"),
        ]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.list_credentials(1)
        assert len(result) == 2
        assert result[0]["credential_key"] == "refresh_token"
        assert result[1]["credential_key"] == "client_id"
        # No plaintext in response
        for c in result:
            assert "encrypted_value" not in c
            assert "plaintext" not in str(c).lower()

    @patch("app.intelligence.account_hub.connect_acc")
    def test_revoke_credential(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.revoke_credential(5)
        assert result is True

    @patch("app.intelligence.account_hub.connect_acc")
    def test_revoke_credential_not_found(self, mock_conn):
        cur = _FakeCursor(rowcount=0)
        mock_conn.return_value = _FakeConn(cur)

        result = ah.revoke_credential(999)
        assert result is False

    @patch("app.intelligence.account_hub.list_credentials")
    def test_validate_all_present(self, mock_list):
        mock_list.return_value = [
            {"credential_key": "client_id", "credential_type": "sp_api", "is_valid": True},
            {"credential_key": "client_secret", "credential_type": "sp_api", "is_valid": True},
            {"credential_key": "refresh_token", "credential_type": "sp_api", "is_valid": True},
        ]
        result = ah.validate_seller_credentials(1)
        assert result["valid"] is True
        assert result["missing_keys"] == []

    @patch("app.intelligence.account_hub.list_credentials")
    def test_validate_missing_keys(self, mock_list):
        mock_list.return_value = [
            {"credential_key": "client_id", "credential_type": "sp_api", "is_valid": True},
        ]
        result = ah.validate_seller_credentials(1)
        assert result["valid"] is False
        assert "client_secret" in result["missing_keys"]
        assert "refresh_token" in result["missing_keys"]

    @patch("app.intelligence.account_hub.list_credentials")
    def test_validate_invalid_cred_ignored(self, mock_list):
        mock_list.return_value = [
            {"credential_key": "client_id", "credential_type": "sp_api", "is_valid": True},
            {"credential_key": "client_secret", "credential_type": "sp_api", "is_valid": False},  # revoked
            {"credential_key": "refresh_token", "credential_type": "sp_api", "is_valid": True},
        ]
        result = ah.validate_seller_credentials(1)
        assert result["valid"] is False
        assert "client_secret" in result["missing_keys"]


# ====================================================================
# 6. Permissions
# ====================================================================

class TestPermissions:
    @patch("app.intelligence.account_hub.connect_acc")
    def test_grant_permission(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [(10,)]  # OUTPUT INSERTED.id from 2nd execute
        # First execute is the UPDATE (revoke old), second is INSERT
        mock_conn.return_value = _FakeConn(cur)

        result = ah.grant_permission(
            user_email="bob@test.com",
            seller_account_id=1,
            permission_level="full",
            granted_by="admin",
        )
        assert result["id"] == 10
        assert result["granted"] is True
        assert len(cur.executed) == 2  # UPDATE + INSERT

    @patch("app.intelligence.account_hub.connect_acc")
    def test_revoke_permission(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.revoke_permission(user_email="bob@test.com", seller_account_id=1)
        assert result is True

    @patch("app.intelligence.account_hub.connect_acc")
    def test_revoke_permission_not_found(self, mock_conn):
        cur = _FakeCursor(rowcount=0)
        mock_conn.return_value = _FakeConn(cur)

        result = ah.revoke_permission(user_email="nobody@test.com", seller_account_id=999)
        assert result is False

    @patch("app.intelligence.account_hub.connect_acc")
    def test_list_user_permissions(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (1, "bob@test.com", 10, "full", "admin", datetime(2025, 3, 15), "A1XTEST", "Test Seller"),
        ]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.list_user_permissions("bob@test.com")
        assert len(result) == 1
        assert result[0]["user_email"] == "bob@test.com"
        assert result[0]["permission_level"] == "full"
        assert result[0]["seller_name"] == "Test Seller"

    @patch("app.intelligence.account_hub.connect_acc")
    def test_list_seller_permissions(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            _make_perm_row(id=1, user_email="alice@test.com"),
            _make_perm_row(id=2, user_email="bob@test.com", permission_level="full"),
        ]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.list_seller_permissions(1)
        assert len(result) == 2
        assert result[0]["user_email"] == "alice@test.com"

    @patch("app.intelligence.account_hub.connect_acc")
    def test_check_access_found(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [("admin",)]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.check_user_seller_access("admin@test.com", 1)
        assert result == "admin"

    @patch("app.intelligence.account_hub.connect_acc")
    def test_check_access_none(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.check_user_seller_access("nobody@test.com", 999)
        assert result == "none"


# ====================================================================
# 7. Scheduler Status
# ====================================================================

class TestSchedulerStatus:
    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_scheduler_status(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (1, "A1XTEST", "Test Seller", "active", 5, datetime(2025, 3, 15, 9, 30)),
        ]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_seller_scheduler_status()
        assert len(result) == 1
        assert result[0]["seller_id"] == "A1XTEST"
        assert result[0]["jobs_last_24h"] == 5

    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_scheduler_status_empty(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_seller_scheduler_status()
        assert result == []


# ====================================================================
# 8. Dashboard
# ====================================================================

class TestDashboard:
    @patch("app.intelligence.account_hub.connect_acc")
    def test_get_dashboard(self, mock_conn):
        cur = _FakeCursor()
        cur.multi_rows = [
            (5, 3, 1, 1),   # sellers: total, active, onboarding, suspended
            (8,),            # users
            (12,),           # credentials
        ]
        mock_conn.return_value = _FakeConn(cur)

        result = ah.get_account_hub_dashboard()
        assert result["sellers"]["total"] == 5
        assert result["sellers"]["active"] == 3
        assert result["sellers"]["onboarding"] == 1
        assert result["users_with_access"] == 8
        assert result["valid_credentials"] == 12


# ====================================================================
# 9. Schema DDL
# ====================================================================

class TestSchema:
    @patch("app.intelligence.account_hub.connect_acc")
    def test_ensure_schema(self, mock_conn):
        cur = _FakeCursor()
        mock_conn.return_value = _FakeConn(cur)

        ah.ensure_account_hub_schema()
        assert len(cur.executed) == len(ah._SCHEMA_STATEMENTS)


# ====================================================================
# 10. API Endpoints
# ====================================================================

def _make_app():
    from app.api.v1.account_hub import router
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


class TestAPIDashboard:
    def test_get_dashboard(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.get_account_hub_dashboard") as mock:
            mock.return_value = {
                "sellers": {"total": 5, "active": 3, "onboarding": 1, "suspended": 1},
                "users_with_access": 8,
                "valid_credentials": 12,
            }
            resp = client.get("/api/v1/account-hub/dashboard")
            assert resp.status_code == 200
            data = resp.json()
            assert data["sellers"]["total"] == 5

    def test_dashboard_error(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.get_account_hub_dashboard") as mock:
            mock.side_effect = Exception("db error")
            resp = client.get("/api/v1/account-hub/dashboard")
            assert resp.status_code == 500


class TestAPISellerAccounts:
    def test_list_sellers(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.list_seller_accounts") as mock:
            mock.return_value = {"items": [], "total": 0, "page": 1, "page_size": 50}
            resp = client.get("/api/v1/account-hub/sellers")
            assert resp.status_code == 200

    def test_get_seller(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.get_seller_account") as mock:
            mock.return_value = {"id": 1, "seller_id": "A1X", "name": "Test"}
            resp = client.get("/api/v1/account-hub/sellers/1")
            assert resp.status_code == 200
            assert resp.json()["id"] == 1

    def test_get_seller_not_found(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.get_seller_account") as mock:
            mock.return_value = None
            resp = client.get("/api/v1/account-hub/sellers/999")
            assert resp.status_code == 404

    def test_create_seller(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.create_seller_account") as mock:
            mock.return_value = {"id": 10, "seller_id": "A1NEW", "status": "onboarding"}
            resp = client.post("/api/v1/account-hub/sellers", json={
                "seller_id": "A1NEW",
                "name": "New Seller",
            })
            assert resp.status_code == 201
            assert resp.json()["id"] == 10

    def test_update_seller(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.update_seller_account") as mock:
            mock.return_value = {"id": 1, "updated": True}
            resp = client.patch("/api/v1/account-hub/sellers/1", json={
                "status": "active",
            })
            assert resp.status_code == 200

    def test_update_seller_not_found(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.update_seller_account") as mock:
            mock.return_value = None
            resp = client.patch("/api/v1/account-hub/sellers/999", json={"status": "active"})
            assert resp.status_code == 404


class TestAPICredentials:
    def test_list_creds(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.list_credentials") as mock:
            mock.return_value = []
            resp = client.get("/api/v1/account-hub/sellers/1/credentials")
            assert resp.status_code == 200

    def test_store_cred(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.store_credential") as mock:
            mock.return_value = {"stored": True, "credential_type": "sp_api", "credential_key": "refresh_token"}
            resp = client.post("/api/v1/account-hub/sellers/1/credentials", json={
                "credential_type": "sp_api",
                "credential_key": "refresh_token",
                "plaintext_value": "my-token",
            })
            assert resp.status_code == 200
            assert resp.json()["stored"] is True

    def test_revoke_cred(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.revoke_credential") as mock:
            mock.return_value = True
            resp = client.delete("/api/v1/account-hub/credentials/5")
            assert resp.status_code == 204

    def test_revoke_cred_not_found(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.revoke_credential") as mock:
            mock.return_value = False
            resp = client.delete("/api/v1/account-hub/credentials/999")
            assert resp.status_code == 404

    def test_validate_creds(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.validate_seller_credentials") as mock:
            mock.return_value = {"valid": True, "missing_keys": [], "total_credentials": 3}
            resp = client.get("/api/v1/account-hub/sellers/1/credentials/validate")
            assert resp.status_code == 200
            assert resp.json()["valid"] is True


class TestAPIPermissions:
    def test_list_seller_perms(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.list_seller_permissions") as mock:
            mock.return_value = []
            resp = client.get("/api/v1/account-hub/sellers/1/permissions")
            assert resp.status_code == 200

    def test_grant_perm(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.grant_permission") as mock:
            mock.return_value = {"id": 5, "granted": True}
            resp = client.post("/api/v1/account-hub/sellers/1/permissions", json={
                "user_email": "bob@test.com",
                "permission_level": "full",
                "granted_by": "admin",
            })
            assert resp.status_code == 200
            assert resp.json()["granted"] is True

    def test_revoke_perm(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.revoke_permission") as mock:
            mock.return_value = True
            resp = client.request(
                "DELETE",
                "/api/v1/account-hub/sellers/1/permissions",
                json={"user_email": "bob@test.com"},
            )
            assert resp.status_code == 200

    def test_revoke_perm_not_found(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.revoke_permission") as mock:
            mock.return_value = False
            resp = client.request(
                "DELETE",
                "/api/v1/account-hub/sellers/1/permissions",
                json={"user_email": "nobody@test.com"},
            )
            assert resp.status_code == 404

    def test_user_perms(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.list_user_permissions") as mock:
            mock.return_value = []
            resp = client.get("/api/v1/account-hub/users/bob@test.com/permissions")
            assert resp.status_code == 200


class TestAPIScheduler:
    def test_scheduler_status(self):
        client = _make_app()
        with patch("app.intelligence.account_hub.get_seller_scheduler_status") as mock:
            mock.return_value = []
            resp = client.get("/api/v1/account-hub/scheduler-status")
            assert resp.status_code == 200
