"""Listing state schema: acc_listing_state + acc_listing_state_history.

Converts ensure_listing_state_schema() DDL into a proper Alembic migration.
Idempotent: all CREATE TABLE/INDEX wrapped in IF NOT EXISTS / IF OBJECT_ID checks.

Revision ID: eb008
Revises: eb007
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb008"
down_revision = "eb007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_listing_state', 'U') IS NULL
    CREATE TABLE dbo.acc_listing_state (
        id                   BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku           NVARCHAR(100)  NOT NULL,
        asin                 VARCHAR(20)    NULL,
        marketplace_id       VARCHAR(20)    NOT NULL,
        product_type         VARCHAR(100)   NULL,
        listing_status       VARCHAR(30)    NOT NULL DEFAULT 'UNKNOWN',
        fulfillment_channel  VARCHAR(20)    NULL,
        condition_type       VARCHAR(30)    NULL,
        has_issues           BIT            NOT NULL DEFAULT 0,
        issues_severity      VARCHAR(20)    NULL,
        issues_count_error   INT            NOT NULL DEFAULT 0,
        issues_count_warning INT            NOT NULL DEFAULT 0,
        issues_snapshot      NVARCHAR(MAX)  NULL,
        is_suppressed        BIT            NOT NULL DEFAULT 0,
        suppression_reasons  NVARCHAR(MAX)  NULL,
        title                NVARCHAR(500)  NULL,
        image_url            NVARCHAR(500)  NULL,
        brand                NVARCHAR(100)  NULL,
        current_price        DECIMAL(12,2)  NULL,
        currency_code        VARCHAR(5)     NULL,
        parent_asin          VARCHAR(20)    NULL,
        variation_theme      VARCHAR(120)   NULL,
        sync_source          VARCHAR(50)    NOT NULL DEFAULT 'unknown',
        last_synced_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        last_status_change   DATETIME2      NULL,
        last_issues_change   DATETIME2      NULL,
        product_id           UNIQUEIDENTIFIER NULL,
        created_at           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_listing_state_sku_mkt UNIQUE (seller_sku, marketplace_id)
    )
    """)

    # Indexes
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_mkt_status')
    CREATE INDEX ix_ls_mkt_status
        ON dbo.acc_listing_state (marketplace_id, listing_status)
    """)
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_asin')
    CREATE INDEX ix_ls_asin
        ON dbo.acc_listing_state (asin) WHERE asin IS NOT NULL
    """)
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_suppressed')
    CREATE INDEX ix_ls_suppressed
        ON dbo.acc_listing_state (is_suppressed) WHERE is_suppressed = 1
    """)
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_issues')
    CREATE INDEX ix_ls_issues
        ON dbo.acc_listing_state (has_issues) WHERE has_issues = 1
    """)
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_synced')
    CREATE INDEX ix_ls_synced
        ON dbo.acc_listing_state (last_synced_at)
    """)
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_product')
    CREATE INDEX ix_ls_product
        ON dbo.acc_listing_state (product_id) WHERE product_id IS NOT NULL
    """)

    # History table
    op.execute("""
    IF OBJECT_ID('dbo.acc_listing_state_history', 'U') IS NULL
    CREATE TABLE dbo.acc_listing_state_history (
        id                BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku        NVARCHAR(100)  NOT NULL,
        marketplace_id    VARCHAR(20)    NOT NULL,
        asin              VARCHAR(20)    NULL,
        previous_status   VARCHAR(30)    NULL,
        new_status        VARCHAR(30)    NOT NULL,
        issue_code        NVARCHAR(200)  NULL,
        issue_severity    VARCHAR(20)    NULL,
        changed_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        change_source     VARCHAR(50)    NOT NULL DEFAULT 'unknown',
        INDEX ix_lsh_sku_mkt_changed (seller_sku, marketplace_id, changed_at)
    )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_listing_state_history")
    op.execute("DROP TABLE IF EXISTS dbo.acc_listing_state")
