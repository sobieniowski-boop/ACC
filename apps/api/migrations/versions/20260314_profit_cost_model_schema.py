"""Profit cost model tables (acc_profit_cost_config, acc_profit_overhead_pool).

Revision ID: eb024
Revises: eb023
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb024"
down_revision = "eb023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_profit_cost_config', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_profit_cost_config (
        config_key NVARCHAR(120) NOT NULL PRIMARY KEY,
        value_decimal DECIMAL(18,6) NULL,
        value_text NVARCHAR(200) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
""")
    # Seed default config row
    op.execute("""
IF NOT EXISTS (SELECT 1 FROM dbo.acc_profit_cost_config WHERE config_key = 'return_handling_per_unit_pln')
BEGIN
    INSERT INTO dbo.acc_profit_cost_config(config_key, value_decimal, value_text)
    VALUES ('return_handling_per_unit_pln', 0, 'default');
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_profit_overhead_pool', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_profit_overhead_pool (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        period_from DATE NOT NULL,
        period_to DATE NOT NULL,
        marketplace_id NVARCHAR(32) NULL,
        pool_name NVARCHAR(120) NOT NULL,
        amount_pln DECIMAL(18,4) NOT NULL,
        allocation_method NVARCHAR(32) NOT NULL DEFAULT 'revenue_share',
        confidence_pct DECIMAL(9,4) NOT NULL DEFAULT 50,
        notes NVARCHAR(500) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_profit_overhead_pool_scope
        ON dbo.acc_profit_overhead_pool(is_active, period_from, period_to, marketplace_id);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_profit_overhead_pool")
    op.execute("DROP TABLE IF EXISTS dbo.acc_profit_cost_config")
