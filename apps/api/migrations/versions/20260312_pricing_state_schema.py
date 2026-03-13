"""Pricing state schema: snapshots, rules, recommendations, sync state, archive.

Converts ensure_pricing_state_schema() DDL into a proper Alembic migration.
Idempotent: all CREATE TABLE/INDEX wrapped in IF OBJECT_ID checks.

Revision ID: eb009
Revises: eb008
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb009"
down_revision = "eb008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_pricing_snapshot', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_pricing_snapshot (
            id                   BIGINT IDENTITY(1,1) PRIMARY KEY,
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
            price_vs_buybox_pct  AS CASE
                WHEN buybox_price > 0 AND our_price IS NOT NULL
                THEN CAST(((our_price - buybox_price) / buybox_price * 100) AS DECIMAL(8,2))
                ELSE NULL END PERSISTED,
            source               VARCHAR(30)     NOT NULL DEFAULT 'competitive_pricing_api',
            observed_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
            created_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
        );

        CREATE INDEX IX_pricing_snap_sku_mkt
            ON dbo.acc_pricing_snapshot (seller_sku, marketplace_id, observed_at DESC);
        CREATE INDEX IX_pricing_snap_asin
            ON dbo.acc_pricing_snapshot (asin, marketplace_id, observed_at DESC)
            WHERE asin IS NOT NULL;
        CREATE INDEX IX_pricing_snap_observed
            ON dbo.acc_pricing_snapshot (observed_at DESC);
        CREATE INDEX IX_pricing_snap_buybox
            ON dbo.acc_pricing_snapshot (marketplace_id, has_buybox)
            INCLUDE (seller_sku, our_price, buybox_price);
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_pricing_rule', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_pricing_rule (
            id                   INT IDENTITY(1,1) PRIMARY KEY,
            seller_sku           NVARCHAR(100)   NULL,
            marketplace_id       VARCHAR(20)     NULL,
            rule_type            VARCHAR(30)     NOT NULL,
            min_margin_pct       DECIMAL(6,2)    NULL,
            max_price_deviation_pct DECIMAL(6,2) NULL,
            floor_price          DECIMAL(12,2)   NULL,
            ceiling_price        DECIMAL(12,2)   NULL,
            target_margin_pct    DECIMAL(6,2)    NULL,
            strategy             VARCHAR(30)     NOT NULL DEFAULT 'monitor',
            is_active            BIT             NOT NULL DEFAULT 1,
            priority             INT             NOT NULL DEFAULT 100,
            created_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_pricing_rule_sku_mkt_type
                UNIQUE (seller_sku, marketplace_id, rule_type)
        );

        CREATE INDEX IX_pricing_rule_active
            ON dbo.acc_pricing_rule (is_active, priority)
            WHERE is_active = 1;
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_pricing_recommendation', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_pricing_recommendation (
            id                   BIGINT IDENTITY(1,1) PRIMARY KEY,
            seller_sku           NVARCHAR(100)   NOT NULL,
            asin                 VARCHAR(20)     NULL,
            marketplace_id       VARCHAR(20)     NOT NULL,
            current_price        DECIMAL(12,2)   NULL,
            recommended_price    DECIMAL(12,2)   NOT NULL,
            buybox_price         DECIMAL(12,2)   NULL,
            price_delta          AS (recommended_price - current_price) PERSISTED,
            price_delta_pct      AS CASE
                WHEN current_price > 0
                THEN CAST(((recommended_price - current_price) / current_price * 100) AS DECIMAL(8,2))
                ELSE NULL END PERSISTED,
            reason_code          VARCHAR(50)     NOT NULL,
            reason_text          NVARCHAR(500)   NULL,
            confidence           DECIMAL(5,2)    NOT NULL DEFAULT 50.0,
            rule_id              INT             NULL,
            snapshot_id          BIGINT          NULL,
            status               VARCHAR(20)     NOT NULL DEFAULT 'pending',
            decided_at           DATETIME2       NULL,
            decided_by           NVARCHAR(100)   NULL,
            created_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
            expires_at           DATETIME2       NULL,
            CONSTRAINT FK_rec_rule FOREIGN KEY (rule_id)
                REFERENCES dbo.acc_pricing_rule(id),
            CONSTRAINT FK_rec_snapshot FOREIGN KEY (snapshot_id)
                REFERENCES dbo.acc_pricing_snapshot(id)
        );

        CREATE INDEX IX_pricing_rec_sku_mkt
            ON dbo.acc_pricing_recommendation (seller_sku, marketplace_id, created_at DESC);
        CREATE INDEX IX_pricing_rec_status
            ON dbo.acc_pricing_recommendation (status, created_at DESC)
            WHERE status = 'pending';
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_pricing_sync_state', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_pricing_sync_state (
            marketplace_id       VARCHAR(20)     NOT NULL PRIMARY KEY,
            last_snapshot_at     DATETIME2       NULL,
            last_rule_eval_at    DATETIME2       NULL,
            snapshots_count      INT             NOT NULL DEFAULT 0,
            recommendations_count INT            NOT NULL DEFAULT 0,
            last_error           NVARCHAR(500)   NULL,
            updated_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_pricing_snapshot_archive', 'U') IS NULL
    BEGIN
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
        );

        CREATE INDEX IX_pricing_snap_archive_observed
            ON dbo.acc_pricing_snapshot_archive (observed_at DESC);
        CREATE INDEX IX_pricing_snap_archive_sku_mkt
            ON dbo.acc_pricing_snapshot_archive (seller_sku, marketplace_id, observed_at DESC);
    END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_pricing_snapshot_archive")
    op.execute("DROP TABLE IF EXISTS dbo.acc_pricing_sync_state")
    op.execute("DROP TABLE IF EXISTS dbo.acc_pricing_recommendation")
    op.execute("DROP TABLE IF EXISTS dbo.acc_pricing_rule")
    op.execute("DROP TABLE IF EXISTS dbo.acc_pricing_snapshot")
