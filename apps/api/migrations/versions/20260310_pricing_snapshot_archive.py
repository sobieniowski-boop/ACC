"""Add acc_pricing_snapshot_archive table + observed_at index on main table.

Revision ID: eb005
Revises: eb004a
Create Date: 2026-03-10
"""
from alembic import op

revision = "eb005"
down_revision = "eb004a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Archive table — same schema as acc_pricing_snapshot (without computed column)
    op.execute("""
    IF OBJECT_ID('dbo.acc_pricing_snapshot_archive', 'U') IS NULL
    CREATE TABLE dbo.acc_pricing_snapshot_archive (
        id                   BIGINT          NOT NULL,
        seller_sku           NVARCHAR(100)   NOT NULL,
        asin                 VARCHAR(20)     NULL,
        marketplace_id       VARCHAR(20)     NOT NULL,
        our_price            DECIMAL(12,2)   NULL,
        our_currency         VARCHAR(5)      NOT NULL DEFAULT 'EUR',
        fulfillment_channel  VARCHAR(10)     NULL,
        buybox_price         DECIMAL(12,2)   NULL,
        buybox_landed_price  DECIMAL(12,2)   NULL,
        has_buybox           BIT             NOT NULL DEFAULT 0,
        is_featured_merchant BIT             NOT NULL DEFAULT 0,
        buybox_seller_id     VARCHAR(20)     NULL,
        lowest_price_new     DECIMAL(12,2)   NULL,
        num_offers_new       INT             NULL,
        num_offers_used      INT             NULL,
        bsr_rank             INT             NULL,
        bsr_category         NVARCHAR(200)   NULL,
        source               VARCHAR(30)     NOT NULL DEFAULT 'competitive_pricing_api',
        observed_at          DATETIME2       NOT NULL,
        created_at           DATETIME2       NOT NULL,
        archived_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_pricing_snap_archive PRIMARY KEY (id)
    )
    """)

    # Index on archive for observed_at range queries
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_pricing_snap_archive_observed')
    CREATE INDEX IX_pricing_snap_archive_observed
        ON dbo.acc_pricing_snapshot_archive (observed_at DESC)
    """)

    # Index on archive for SKU+marketplace lookups
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_pricing_snap_archive_sku_mkt')
    CREATE INDEX IX_pricing_snap_archive_sku_mkt
        ON dbo.acc_pricing_snapshot_archive (seller_sku, marketplace_id, observed_at DESC)
    """)


def downgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_pricing_snapshot_archive', 'U') IS NOT NULL
        DROP TABLE dbo.acc_pricing_snapshot_archive
    """)
