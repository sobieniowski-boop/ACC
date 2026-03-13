"""Account Hub — Multi-seller support — Sprint 25-26.

Manages multiple Amazon seller accounts with encrypted credential
storage, per-user seller permissions, and seller-aware scheduling.

Tables managed:
  acc_seller_account      — Seller accounts (name, seller_id, marketplaces, status)
  acc_seller_credential   — Encrypted credentials per seller
  acc_seller_permission   — User-to-seller access mapping

Security:
  Credentials are encrypted at rest using Fernet symmetric encryption
  derived from the application SECRET_KEY.  The encryption key never
  leaves the server process.
"""
from __future__ import annotations

import base64
import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from app.connectors.mssql import connect_acc
from app.core.config import settings

log = structlog.get_logger(__name__)

# ── Encryption helpers ───────────────────────────────────────────────

def _derive_key() -> bytes:
    """Derive a 32-byte Fernet key from the app SECRET_KEY."""
    digest = hashlib.sha256(settings.SECRET_KEY.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _encrypt(plaintext: str) -> str:
    from cryptography.fernet import Fernet
    f = Fernet(_derive_key())
    return f.encrypt(plaintext.encode("utf-8")).decode("utf-8")


def _decrypt(ciphertext: str) -> str:
    from cryptography.fernet import Fernet
    f = Fernet(_derive_key())
    return f.decrypt(ciphertext.encode("utf-8")).decode("utf-8")


# ── Constants ────────────────────────────────────────────────────────

SELLER_STATUSES = {"active", "inactive", "suspended", "onboarding"}
CREDENTIAL_TYPES = {"sp_api", "ads_api", "lwa"}
PERMISSION_LEVELS = {"admin", "full", "read_only", "none"}


# ── Schema DDL ───────────────────────────────────────────────────────

_SCHEMA_STATEMENTS: list[str] = [
    """
    IF OBJECT_ID('dbo.acc_seller_account', 'U') IS NULL
    CREATE TABLE dbo.acc_seller_account (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_id           VARCHAR(30)    NOT NULL UNIQUE,
        name                NVARCHAR(200)  NOT NULL,
        company_name        NVARCHAR(200)  NULL,
        marketplace_ids     NVARCHAR(500)  NULL,
        primary_marketplace VARCHAR(20)    NULL,
        region              VARCHAR(10)    NOT NULL DEFAULT 'eu',
        status              VARCHAR(20)    NOT NULL DEFAULT 'onboarding',
        notes               NVARCHAR(MAX)  NULL,
        created_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,
    """
    IF OBJECT_ID('dbo.acc_seller_credential', 'U') IS NULL
    CREATE TABLE dbo.acc_seller_credential (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_account_id   BIGINT         NOT NULL,
        credential_type     VARCHAR(20)    NOT NULL,
        credential_key      VARCHAR(80)    NOT NULL,
        encrypted_value     NVARCHAR(MAX)  NOT NULL,
        expires_at          DATETIME2      NULL,
        is_valid            BIT            NOT NULL DEFAULT 1,
        last_validated_at   DATETIME2      NULL,
        created_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT fk_seller_cred_account
            FOREIGN KEY (seller_account_id) REFERENCES dbo.acc_seller_account(id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_seller_cred_account_type')
    CREATE UNIQUE INDEX ix_seller_cred_account_type
        ON dbo.acc_seller_credential (seller_account_id, credential_type, credential_key)
    """,
    """
    IF OBJECT_ID('dbo.acc_seller_permission', 'U') IS NULL
    CREATE TABLE dbo.acc_seller_permission (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        user_email          NVARCHAR(200)  NOT NULL,
        seller_account_id   BIGINT         NOT NULL,
        permission_level    VARCHAR(20)    NOT NULL DEFAULT 'read_only',
        granted_by          NVARCHAR(120)  NOT NULL,
        granted_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        revoked_at          DATETIME2      NULL,
        CONSTRAINT fk_seller_perm_account
            FOREIGN KEY (seller_account_id) REFERENCES dbo.acc_seller_account(id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_seller_perm_user')
    CREATE UNIQUE INDEX ix_seller_perm_user
        ON dbo.acc_seller_permission (user_email, seller_account_id)
        WHERE revoked_at IS NULL
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_seller_account_status')
    CREATE INDEX ix_seller_account_status
        ON dbo.acc_seller_account (status)
    """,
]


def ensure_account_hub_schema() -> None:
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
    finally:
        conn.close()


# ── Seller Account CRUD ─────────────────────────────────────────────

def _row_to_seller(row) -> dict[str, Any]:
    return {
        "id": row[0],
        "seller_id": row[1],
        "name": row[2],
        "company_name": row[3],
        "marketplace_ids": json.loads(row[4]) if row[4] else [],
        "primary_marketplace": row[5],
        "region": row[6],
        "status": row[7],
        "notes": row[8],
        "created_at": row[9].isoformat() if isinstance(row[9], (date, datetime)) else row[9],
        "updated_at": row[10].isoformat() if isinstance(row[10], (date, datetime)) else row[10],
    }


_SELLER_COLS = """
    id, seller_id, name, company_name, marketplace_ids,
    primary_marketplace, region, status, notes, created_at, updated_at
"""


def list_seller_accounts(
    *,
    status: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []

        if status:
            where.append("status = %s")
            params.append(status)

        where_clause = " AND ".join(where) if where else "1=1"
        offset = (page - 1) * page_size

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_seller_account WHERE {where_clause}", tuple(params))
        total = (cur.fetchone() or (0,))[0]

        cur.execute(f"""
            SELECT {_SELLER_COLS}
            FROM dbo.acc_seller_account
            WHERE {where_clause}
            ORDER BY name
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, tuple(params) + (offset, page_size))
        rows = cur.fetchall()
        cur.close()

        return {
            "items": [_row_to_seller(r) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        conn.close()


def get_seller_account(seller_account_id: int) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {_SELLER_COLS} FROM dbo.acc_seller_account WHERE id = %s", (seller_account_id,))
        row = cur.fetchone()
        cur.close()
        return _row_to_seller(row) if row else None
    finally:
        conn.close()


def get_seller_by_seller_id(seller_id: str) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {_SELLER_COLS} FROM dbo.acc_seller_account WHERE seller_id = %s", (seller_id,))
        row = cur.fetchone()
        cur.close()
        return _row_to_seller(row) if row else None
    finally:
        conn.close()


def create_seller_account(
    *,
    seller_id: str,
    name: str,
    company_name: str | None = None,
    marketplace_ids: list[str] | None = None,
    primary_marketplace: str | None = None,
    region: str = "eu",
    notes: str | None = None,
) -> dict[str, Any]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        mkt_json = json.dumps(marketplace_ids) if marketplace_ids else None
        cur.execute("""
            INSERT INTO dbo.acc_seller_account
                (seller_id, name, company_name, marketplace_ids,
                 primary_marketplace, region, status, notes)
            OUTPUT INSERTED.id
            VALUES (%s, %s, %s, %s, %s, %s, 'onboarding', %s)
        """, (seller_id, name, company_name, mkt_json, primary_marketplace, region, notes))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        log.info("account_hub.seller_created", id=new_id, seller_id=seller_id)
        return {"id": new_id, "seller_id": seller_id, "status": "onboarding"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_seller_account(
    seller_account_id: int,
    *,
    name: str | None = None,
    company_name: str | None = None,
    marketplace_ids: list[str] | None = None,
    primary_marketplace: str | None = None,
    region: str | None = None,
    status: str | None = None,
    notes: str | None = None,
) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        sets: list[str] = ["updated_at = SYSUTCDATETIME()"]
        params: list[Any] = []

        if name is not None:
            sets.append("name = %s")
            params.append(name)
        if company_name is not None:
            sets.append("company_name = %s")
            params.append(company_name)
        if marketplace_ids is not None:
            sets.append("marketplace_ids = %s")
            params.append(json.dumps(marketplace_ids))
        if primary_marketplace is not None:
            sets.append("primary_marketplace = %s")
            params.append(primary_marketplace)
        if region is not None:
            sets.append("region = %s")
            params.append(region)
        if status is not None:
            sets.append("status = %s")
            params.append(status)
        if notes is not None:
            sets.append("notes = %s")
            params.append(notes)

        params.append(seller_account_id)
        cur.execute(f"""
            UPDATE dbo.acc_seller_account
            SET {', '.join(sets)}
            WHERE id = %s
        """, tuple(params))
        affected = cur.rowcount
        conn.commit()
        cur.close()
        if affected == 0:
            return None
        return {"id": seller_account_id, "updated": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Credential Vault ─────────────────────────────────────────────────

def store_credential(
    *,
    seller_account_id: int,
    credential_type: str,
    credential_key: str,
    plaintext_value: str,
    expires_at: str | None = None,
) -> dict[str, Any]:
    encrypted = _encrypt(plaintext_value)
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            MERGE dbo.acc_seller_credential AS tgt
            USING (SELECT %s AS seller_account_id, %s AS credential_type, %s AS credential_key) AS src
            ON tgt.seller_account_id = src.seller_account_id
               AND tgt.credential_type = src.credential_type
               AND tgt.credential_key = src.credential_key
            WHEN MATCHED THEN
                UPDATE SET encrypted_value = %s, is_valid = 1,
                           expires_at = %s, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (seller_account_id, credential_type, credential_key,
                        encrypted_value, expires_at)
                VALUES (%s, %s, %s, %s, %s);
        """, (
            seller_account_id, credential_type, credential_key,
            encrypted, expires_at,
            seller_account_id, credential_type, credential_key,
            encrypted, expires_at,
        ))
        conn.commit()
        cur.close()
        log.info("account_hub.credential_stored",
                 seller_account_id=seller_account_id, type=credential_type, key=credential_key)
        return {"stored": True, "credential_type": credential_type, "credential_key": credential_key}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_credential(
    *,
    seller_account_id: int,
    credential_type: str,
    credential_key: str,
) -> str | None:
    """Return decrypted credential value, or None if not found."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT encrypted_value FROM dbo.acc_seller_credential
            WHERE seller_account_id = %s AND credential_type = %s
                  AND credential_key = %s AND is_valid = 1
        """, (seller_account_id, credential_type, credential_key))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return _decrypt(row[0])
    finally:
        conn.close()


def list_credentials(seller_account_id: int) -> list[dict[str, Any]]:
    """List credential metadata (no decrypted values)."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, credential_type, credential_key, is_valid,
                   expires_at, last_validated_at, created_at, updated_at
            FROM dbo.acc_seller_credential
            WHERE seller_account_id = %s
            ORDER BY credential_type, credential_key
        """, (seller_account_id,))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "credential_type": r[1],
                "credential_key": r[2],
                "is_valid": bool(r[3]),
                "expires_at": r[4].isoformat() if isinstance(r[4], (date, datetime)) else r[4],
                "last_validated_at": r[5].isoformat() if isinstance(r[5], (date, datetime)) else r[5],
                "created_at": r[6].isoformat() if isinstance(r[6], (date, datetime)) else r[6],
                "updated_at": r[7].isoformat() if isinstance(r[7], (date, datetime)) else r[7],
            }
            for r in rows
        ]
    finally:
        conn.close()


def revoke_credential(credential_id: int) -> bool:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_seller_credential
            SET is_valid = 0, updated_at = SYSUTCDATETIME()
            WHERE id = %s
        """, (credential_id,))
        affected = cur.rowcount
        conn.commit()
        cur.close()
        return affected > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def validate_seller_credentials(seller_account_id: int) -> dict[str, Any]:
    """Check if all required SP-API credentials exist and are valid."""
    creds = list_credentials(seller_account_id)
    required = {"client_id", "client_secret", "refresh_token"}
    found = {c["credential_key"] for c in creds if c["is_valid"] and c["credential_type"] == "sp_api"}
    missing = required - found
    return {
        "seller_account_id": seller_account_id,
        "valid": len(missing) == 0,
        "missing_keys": sorted(missing),
        "total_credentials": len(creds),
    }


# ── Seller Permissions ───────────────────────────────────────────────

def grant_permission(
    *,
    user_email: str,
    seller_account_id: int,
    permission_level: str = "read_only",
    granted_by: str,
) -> dict[str, Any]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        # Upsert: revoke old + insert new
        cur.execute("""
            UPDATE dbo.acc_seller_permission
            SET revoked_at = SYSUTCDATETIME()
            WHERE user_email = %s AND seller_account_id = %s AND revoked_at IS NULL
        """, (user_email, seller_account_id))
        cur.execute("""
            INSERT INTO dbo.acc_seller_permission
                (user_email, seller_account_id, permission_level, granted_by)
            OUTPUT INSERTED.id
            VALUES (%s, %s, %s, %s)
        """, (user_email, seller_account_id, permission_level, granted_by))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        log.info("account_hub.permission_granted",
                 user=user_email, seller=seller_account_id, level=permission_level)
        return {"id": new_id, "granted": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def revoke_permission(*, user_email: str, seller_account_id: int) -> bool:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_seller_permission
            SET revoked_at = SYSUTCDATETIME()
            WHERE user_email = %s AND seller_account_id = %s AND revoked_at IS NULL
        """, (user_email, seller_account_id))
        affected = cur.rowcount
        conn.commit()
        cur.close()
        return affected > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_user_permissions(user_email: str) -> list[dict[str, Any]]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.id, p.user_email, p.seller_account_id, p.permission_level,
                   p.granted_by, p.granted_at,
                   s.seller_id, s.name AS seller_name
            FROM dbo.acc_seller_permission p
            JOIN dbo.acc_seller_account s ON s.id = p.seller_account_id
            WHERE p.user_email = %s AND p.revoked_at IS NULL
            ORDER BY s.name
        """, (user_email,))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "user_email": r[1],
                "seller_account_id": r[2],
                "permission_level": r[3],
                "granted_by": r[4],
                "granted_at": r[5].isoformat() if isinstance(r[5], (date, datetime)) else r[5],
                "seller_id": r[6],
                "seller_name": r[7],
            }
            for r in rows
        ]
    finally:
        conn.close()


def list_seller_permissions(seller_account_id: int) -> list[dict[str, Any]]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_email, permission_level, granted_by, granted_at
            FROM dbo.acc_seller_permission
            WHERE seller_account_id = %s AND revoked_at IS NULL
            ORDER BY user_email
        """, (seller_account_id,))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "user_email": r[1],
                "permission_level": r[2],
                "granted_by": r[3],
                "granted_at": r[4].isoformat() if isinstance(r[4], (date, datetime)) else r[4],
            }
            for r in rows
        ]
    finally:
        conn.close()


def check_user_seller_access(user_email: str, seller_account_id: int) -> str:
    """Return the permission level for a user on a seller, or 'none'."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT permission_level FROM dbo.acc_seller_permission
            WHERE user_email = %s AND seller_account_id = %s AND revoked_at IS NULL
        """, (user_email, seller_account_id))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else "none"
    finally:
        conn.close()


# ── Multi-seller Scheduler Status ────────────────────────────────────

def get_seller_scheduler_status() -> list[dict[str, Any]]:
    """Return scheduling status for all active sellers."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT sa.id, sa.seller_id, sa.name, sa.status,
                   (SELECT COUNT(*) FROM dbo.acc_al_jobs j
                    WHERE j.created_at >= DATEADD(HOUR, -24, SYSUTCDATETIME())) AS jobs_24h,
                   (SELECT MAX(j.created_at) FROM dbo.acc_al_jobs j) AS last_job_at
            FROM dbo.acc_seller_account sa
            WHERE sa.status = 'active'
            ORDER BY sa.name
        """)
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "seller_account_id": r[0],
                "seller_id": r[1],
                "name": r[2],
                "status": r[3],
                "jobs_last_24h": r[4] or 0,
                "last_job_at": r[5].isoformat() if isinstance(r[5], (date, datetime)) else r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_account_hub_dashboard() -> dict[str, Any]:
    """Overview stats for the account hub page."""
    conn = connect_acc()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status = 'onboarding' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END)
            FROM dbo.acc_seller_account
        """)
        sa = cur.fetchone() or (0, 0, 0, 0)

        cur.execute("""
            SELECT COUNT(DISTINCT user_email)
            FROM dbo.acc_seller_permission
            WHERE revoked_at IS NULL
        """)
        users = (cur.fetchone() or (0,))[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.acc_seller_credential
            WHERE is_valid = 1
        """)
        creds = (cur.fetchone() or (0,))[0]

        cur.close()
        return {
            "sellers": {
                "total": sa[0] or 0,
                "active": sa[1] or 0,
                "onboarding": sa[2] or 0,
                "suspended": sa[3] or 0,
            },
            "users_with_access": users or 0,
            "valid_credentials": creds or 0,
        }
    finally:
        conn.close()
