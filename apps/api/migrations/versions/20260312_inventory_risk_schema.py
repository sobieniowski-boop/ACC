"""Sprint 13 – Inventory Risk Engine schema.

Creates ``acc_inventory_risk_score`` for daily per-SKU risk scoring
(stockout probability, overstock holding cost, aging write-off risk).

Revision ID: eb029
Revises: eb028
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb029"
down_revision = "eb028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        IF OBJECT_ID('dbo.acc_inventory_risk_score', 'U') IS NULL
        CREATE TABLE dbo.acc_inventory_risk_score (
            id                     BIGINT IDENTITY(1,1) PRIMARY KEY,
            seller_sku             NVARCHAR(100) NOT NULL,
            asin                   NVARCHAR(20)  NULL,
            marketplace_id         VARCHAR(20)   NOT NULL,
            score_date             DATE          NOT NULL,

            -- Stockout probability model
            stockout_prob_7d       DECIMAL(5,4)  NULL,
            stockout_prob_14d      DECIMAL(5,4)  NULL,
            stockout_prob_30d      DECIMAL(5,4)  NULL,
            days_cover             DECIMAL(10,1) NULL,
            velocity_7d            DECIMAL(10,2) NULL DEFAULT 0,
            velocity_30d           DECIMAL(10,2) NULL DEFAULT 0,
            velocity_cv            DECIMAL(6,3)  NULL,
            units_available        INT           NULL DEFAULT 0,

            -- Overstock cost model
            overstock_holding_cost_pln DECIMAL(14,2) NULL DEFAULT 0,
            storage_fee_30d_pln    DECIMAL(14,2) NULL DEFAULT 0,
            capital_tie_up_pln     DECIMAL(14,2) NULL DEFAULT 0,
            excess_units           INT           NULL DEFAULT 0,
            excess_value_pln       DECIMAL(14,2) NULL DEFAULT 0,

            -- Aging write-off risk
            aging_risk_pln         DECIMAL(14,2) NULL DEFAULT 0,
            aged_90_plus_units     INT           NULL DEFAULT 0,
            aged_90_plus_value_pln DECIMAL(14,2) NULL DEFAULT 0,
            projected_aged_90_30d  INT           NULL DEFAULT 0,

            -- Composite
            risk_tier              VARCHAR(20)   NOT NULL DEFAULT 'low',
            risk_score             SMALLINT      NOT NULL DEFAULT 0,

            computed_at            DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),

            CONSTRAINT uq_irs_sku_mkt_date UNIQUE (seller_sku, marketplace_id, score_date)
        )
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_irs_date')
        CREATE INDEX ix_irs_date
            ON dbo.acc_inventory_risk_score (score_date DESC)
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_irs_tier')
        CREATE INDEX ix_irs_tier
            ON dbo.acc_inventory_risk_score (risk_tier, score_date DESC)
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_irs_mkt_date')
        CREATE INDEX ix_irs_mkt_date
            ON dbo.acc_inventory_risk_score (marketplace_id, score_date DESC)
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_irs_stockout')
        CREATE INDEX ix_irs_stockout
            ON dbo.acc_inventory_risk_score (stockout_prob_7d DESC)
            WHERE stockout_prob_7d > 0.3
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_inventory_risk_score")
