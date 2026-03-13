"""Account Hub — Multi-seller support — Sprint 25-26.

Tables:
  acc_seller_account      — Seller account registry
  acc_seller_credential   — Encrypted credential vault
  acc_seller_permission   — User ↔ seller access mapping

Revision ID: eb038
Revises: eb037
Create Date: 2026-03-15
"""
from alembic import op

revision = "eb038"
down_revision = "eb037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── acc_seller_account ─────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_seller_account', 'U') IS NULL
    CREATE TABLE dbo.acc_seller_account (
        id                INT IDENTITY(1,1) PRIMARY KEY,
        seller_id         NVARCHAR(40)   NOT NULL,
        seller_name       NVARCHAR(200)  NOT NULL,
        marketplace_ids   NVARCHAR(MAX)  NULL,
        status            NVARCHAR(20)   NOT NULL DEFAULT 'onboarding',
        primary_contact   NVARCHAR(200)  NULL,
        notes             NVARCHAR(MAX)  NULL,
        onboarded_at      DATETIME2      NULL,
        created_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_seller_account_seller_id UNIQUE (seller_id)
    );
    """)

    # ── acc_seller_credential ──────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_seller_credential', 'U') IS NULL
    CREATE TABLE dbo.acc_seller_credential (
        id                INT IDENTITY(1,1) PRIMARY KEY,
        seller_account_id INT            NOT NULL,
        credential_type   NVARCHAR(40)   NOT NULL,
        credential_key    NVARCHAR(120)  NOT NULL,
        encrypted_value   NVARCHAR(MAX)  NOT NULL,
        is_valid          BIT            NOT NULL DEFAULT 1,
        expires_at        DATETIME2      NULL,
        created_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_seller_cred_account
            FOREIGN KEY (seller_account_id) REFERENCES dbo.acc_seller_account(id)
    );
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_acc_seller_credential_lookup')
    CREATE UNIQUE INDEX IX_acc_seller_credential_lookup
        ON dbo.acc_seller_credential(seller_account_id, credential_type, credential_key)
        WHERE is_valid = 1;
    """)

    # ── acc_seller_permission ──────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_seller_permission', 'U') IS NULL
    CREATE TABLE dbo.acc_seller_permission (
        id                INT IDENTITY(1,1) PRIMARY KEY,
        seller_account_id INT            NOT NULL,
        user_email        NVARCHAR(200)  NOT NULL,
        permission_level  NVARCHAR(20)   NOT NULL DEFAULT 'read_only',
        granted_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        revoked_at        DATETIME2      NULL,
        CONSTRAINT FK_seller_perm_account
            FOREIGN KEY (seller_account_id) REFERENCES dbo.acc_seller_account(id)
    );
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_acc_seller_permission_active')
    CREATE UNIQUE INDEX IX_acc_seller_permission_active
        ON dbo.acc_seller_permission(user_email, seller_account_id)
        WHERE revoked_at IS NULL;
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_seller_permission;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_seller_credential;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_seller_account;")
