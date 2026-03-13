"""Amazon listing registry (acc_amazon_listing_registry, sync_state).

Revision ID: eb021
Revises: eb020
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb021"
down_revision = "eb020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_amazon_listing_registry', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_amazon_listing_registry (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        merchant_sku NVARCHAR(255) NULL,
        merchant_sku_alt NVARCHAR(255) NULL,
        internal_sku NVARCHAR(64) NULL,
        ean NVARCHAR(64) NULL,
        asin NVARCHAR(64) NULL,
        parent_asin NVARCHAR(64) NULL,
        brand NVARCHAR(128) NULL,
        product_name NVARCHAR(512) NULL,
        listing_role NVARCHAR(32) NULL,
        priority_label NVARCHAR(64) NULL,
        launch_type NVARCHAR(64) NULL,
        category_1 NVARCHAR(255) NULL,
        category_2 NVARCHAR(255) NULL,
        source_gid NVARCHAR(32) NOT NULL,
        row_hash NVARCHAR(64) NOT NULL,
        raw_json NVARCHAR(MAX) NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_amazon_listing_registry_merchant_sku
        ON dbo.acc_amazon_listing_registry(merchant_sku);
    CREATE INDEX IX_acc_amazon_listing_registry_merchant_sku_alt
        ON dbo.acc_amazon_listing_registry(merchant_sku_alt);
    CREATE INDEX IX_acc_amazon_listing_registry_asin
        ON dbo.acc_amazon_listing_registry(asin);
    CREATE INDEX IX_acc_amazon_listing_registry_ean
        ON dbo.acc_amazon_listing_registry(ean);
    CREATE INDEX IX_acc_amazon_listing_registry_internal_sku
        ON dbo.acc_amazon_listing_registry(internal_sku);
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_amazon_listing_registry_sync_state', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_amazon_listing_registry_sync_state (
        source_gid NVARCHAR(32) NOT NULL PRIMARY KEY,
        source_url NVARCHAR(1000) NULL,
        source_hash NVARCHAR(64) NULL,
        row_count INT NOT NULL DEFAULT 0,
        last_synced_at DATETIME2 NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_amazon_listing_registry_sync_state")
    op.execute("DROP TABLE IF EXISTS dbo.acc_amazon_listing_registry")
