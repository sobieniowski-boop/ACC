"""Buy Box Radar tables: acc_competitor_offer, acc_buybox_trend.

Sprint 11 — S11.1

Revision ID: eb028
Revises: eb027
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb028"
down_revision = "eb027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── acc_competitor_offer ─────────────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_competitor_offer', 'U') IS NULL
    CREATE TABLE dbo.acc_competitor_offer (
        id              BIGINT        IDENTITY(1,1) PRIMARY KEY,
        asin            VARCHAR(20)   NOT NULL,
        marketplace_id  VARCHAR(20)   NOT NULL,
        seller_id       VARCHAR(20)   NULL,
        is_our_offer    BIT           DEFAULT 0,
        listing_price   DECIMAL(12,2) NULL,
        shipping_price  DECIMAL(12,2) NULL,
        landed_price    DECIMAL(12,2) NULL,
        currency        VARCHAR(5)    DEFAULT 'EUR',
        is_buybox_winner BIT          DEFAULT 0,
        is_fba          BIT           DEFAULT 0,
        condition_type  VARCHAR(20)   DEFAULT 'New',
        seller_feedback_rating DECIMAL(4,2) NULL,
        seller_feedback_count  INT    NULL,
        observed_at     DATETIME2     DEFAULT SYSUTCDATETIME(),
        created_at      DATETIME2     DEFAULT SYSUTCDATETIME()
    );
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_comp_offer_asin_mkt'
          AND object_id = OBJECT_ID('dbo.acc_competitor_offer')
    )
    CREATE INDEX IX_comp_offer_asin_mkt
        ON dbo.acc_competitor_offer (asin, marketplace_id, observed_at DESC);
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_comp_offer_seller'
          AND object_id = OBJECT_ID('dbo.acc_competitor_offer')
    )
    CREATE INDEX IX_comp_offer_seller
        ON dbo.acc_competitor_offer (seller_id)
        WHERE seller_id IS NOT NULL;
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_comp_offer_bb_winner'
          AND object_id = OBJECT_ID('dbo.acc_competitor_offer')
    )
    CREATE INDEX IX_comp_offer_bb_winner
        ON dbo.acc_competitor_offer (marketplace_id, is_buybox_winner)
        INCLUDE (asin, landed_price, seller_id);
    """)

    # ── acc_buybox_trend ─────────────────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_buybox_trend', 'U') IS NULL
    CREATE TABLE dbo.acc_buybox_trend (
        id              BIGINT        IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(100) NOT NULL,
        asin            VARCHAR(20)   NULL,
        marketplace_id  VARCHAR(20)   NOT NULL,
        trend_date      DATE          NOT NULL,
        snapshots_total INT           DEFAULT 0,
        snapshots_won   INT           DEFAULT 0,
        win_rate        AS CASE WHEN snapshots_total > 0
                           THEN CAST(snapshots_won AS DECIMAL(5,2))
                                / snapshots_total * 100
                           ELSE 0 END PERSISTED,
        avg_our_price       DECIMAL(12,2) NULL,
        avg_buybox_price    DECIMAL(12,2) NULL,
        avg_price_gap_pct   DECIMAL(8,2)  NULL,
        num_competitors     INT           NULL,
        lowest_competitor_price DECIMAL(12,2) NULL,
        computed_at     DATETIME2     DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_buybox_trend_sku_mkt_date
            UNIQUE (seller_sku, marketplace_id, trend_date)
    );
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_bb_trend_date'
          AND object_id = OBJECT_ID('dbo.acc_buybox_trend')
    )
    CREATE INDEX IX_bb_trend_date
        ON dbo.acc_buybox_trend (trend_date);
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_bb_trend_mkt_winrate'
          AND object_id = OBJECT_ID('dbo.acc_buybox_trend')
    )
    CREATE INDEX IX_bb_trend_mkt_winrate
        ON dbo.acc_buybox_trend (marketplace_id, win_rate);
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_bb_trend_sku_mkt_date'
          AND object_id = OBJECT_ID('dbo.acc_buybox_trend')
    )
    CREATE INDEX IX_bb_trend_sku_mkt_date
        ON dbo.acc_buybox_trend (seller_sku, marketplace_id, trend_date DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_buybox_trend;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_competitor_offer;")
