"""Sprint 14 – Replenishment Plan & Risk Alerts schema.

Creates ``acc_replenishment_plan`` for risk-informed reorder suggestions
and ``acc_inventory_risk_alert`` for risk-tier escalation alerts.

Revision ID: eb030
Revises: eb029
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb030"
down_revision = "eb029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_replenishment_plan', 'U') IS NULL
    CREATE TABLE dbo.acc_replenishment_plan (
        id                     BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku             NVARCHAR(100) NOT NULL,
        asin                   NVARCHAR(20)  NULL,
        marketplace_id         VARCHAR(20)   NOT NULL,
        plan_date              DATE          NOT NULL,
        risk_score             SMALLINT      NOT NULL DEFAULT 0,
        risk_tier              VARCHAR(20)   NOT NULL DEFAULT 'low',
        stockout_prob_7d       DECIMAL(5,4)  NULL,
        days_cover             DECIMAL(10,1) NULL,
        velocity_7d            DECIMAL(10,2) NULL DEFAULT 0,
        velocity_30d           DECIMAL(10,2) NULL DEFAULT 0,
        velocity_trend         VARCHAR(20)   NULL DEFAULT 'stable',
        velocity_change_pct    DECIMAL(7,2)  NULL,
        suggested_reorder_qty  INT           NOT NULL DEFAULT 0,
        reorder_urgency        VARCHAR(20)   NOT NULL DEFAULT 'low',
        target_days_cover      INT           NOT NULL DEFAULT 45,
        lead_time_days         INT           NOT NULL DEFAULT 21,
        safety_stock_days      INT           NOT NULL DEFAULT 14,
        estimated_stockout_date DATE         NULL,
        overstock_holding_cost_pln DECIMAL(14,2) NULL DEFAULT 0,
        aging_risk_pln         DECIMAL(14,2) NULL DEFAULT 0,
        units_available        INT           NULL DEFAULT 0,
        is_acknowledged        BIT           NOT NULL DEFAULT 0,
        acknowledged_at        DATETIME2     NULL,
        acknowledged_by        NVARCHAR(100) NULL,
        computed_at            DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_rp_sku_mkt_date UNIQUE (seller_sku, marketplace_id, plan_date)
    )
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_rp_date')
    CREATE INDEX ix_rp_date ON dbo.acc_replenishment_plan (plan_date DESC)
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_rp_urgency')
    CREATE INDEX ix_rp_urgency ON dbo.acc_replenishment_plan (reorder_urgency, plan_date DESC)
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_rp_mkt_date')
    CREATE INDEX ix_rp_mkt_date ON dbo.acc_replenishment_plan (marketplace_id, plan_date DESC)
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inventory_risk_alert', 'U') IS NULL
    CREATE TABLE dbo.acc_inventory_risk_alert (
        id                     BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku             NVARCHAR(100) NOT NULL,
        marketplace_id         VARCHAR(20)   NOT NULL,
        alert_type             VARCHAR(50)   NOT NULL,
        severity               VARCHAR(20)   NOT NULL DEFAULT 'warning',
        title                  NVARCHAR(200) NOT NULL,
        detail                 NVARCHAR(MAX) NULL,
        current_value          FLOAT         NULL,
        previous_value         FLOAT         NULL,
        threshold              FLOAT         NULL,
        risk_score             SMALLINT      NULL,
        risk_tier              VARCHAR(20)   NULL,
        is_resolved            BIT           NOT NULL DEFAULT 0,
        resolved_at            DATETIME2     NULL,
        triggered_at           DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ira_type_date')
    CREATE INDEX ix_ira_type_date ON dbo.acc_inventory_risk_alert (alert_type, triggered_at DESC)
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ira_sku_mkt')
    CREATE INDEX ix_ira_sku_mkt ON dbo.acc_inventory_risk_alert (seller_sku, marketplace_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_inventory_risk_alert")
    op.execute("DROP TABLE IF EXISTS dbo.acc_replenishment_plan")
