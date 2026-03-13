"""Sprint 15 — Repricing Decision Engine schema.

New tables:
  acc_repricing_strategy   — per-SKU pricing strategy definitions
  acc_repricing_execution  — execution proposals & audit trail

Revision ID: eb031
Revises: eb030
"""
from alembic import op

revision = "eb031"
down_revision = "eb030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        -- 1. Repricing Strategy definitions
        IF OBJECT_ID('dbo.acc_repricing_strategy', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.acc_repricing_strategy (
                id                    INT            IDENTITY(1,1) PRIMARY KEY,
                seller_sku            NVARCHAR(100)  NULL,
                marketplace_id        VARCHAR(20)    NULL,
                strategy_type         VARCHAR(30)    NOT NULL,
                is_active             BIT            NOT NULL DEFAULT 1,
                parameters            NVARCHAR(MAX)  NULL,
                min_price             DECIMAL(12,2)  NULL,
                max_price             DECIMAL(12,2)  NULL,
                min_margin_pct        DECIMAL(6,2)   NULL,
                max_daily_change_pct  DECIMAL(6,2)   NULL DEFAULT 10.0,
                requires_approval     BIT            NOT NULL DEFAULT 1,
                priority              INT            NOT NULL DEFAULT 100,
                created_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
                updated_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

                CONSTRAINT UQ_reprice_strat_sku_mkt_type
                    UNIQUE (seller_sku, marketplace_id, strategy_type)
            );

            CREATE INDEX IX_reprice_strat_active
                ON dbo.acc_repricing_strategy (is_active, priority)
                WHERE is_active = 1;

            CREATE INDEX IX_reprice_strat_type
                ON dbo.acc_repricing_strategy (strategy_type, is_active);
        END

        -- 2. Repricing Execution proposals & audit trail
        IF OBJECT_ID('dbo.acc_repricing_execution', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.acc_repricing_execution (
                id                    BIGINT         IDENTITY(1,1) PRIMARY KEY,
                seller_sku            NVARCHAR(100)  NOT NULL,
                asin                  VARCHAR(20)    NULL,
                marketplace_id        VARCHAR(20)    NOT NULL,
                strategy_id           INT            NULL,
                strategy_type         VARCHAR(30)    NOT NULL,
                current_price         DECIMAL(12,2)  NULL,
                target_price          DECIMAL(12,2)  NOT NULL,
                final_price           DECIMAL(12,2)  NULL,
                price_change          AS (target_price - current_price) PERSISTED,
                price_change_pct      AS CASE
                    WHEN current_price > 0
                    THEN CAST(((target_price - current_price) / current_price * 100) AS DECIMAL(8,2))
                    ELSE NULL END PERSISTED,
                estimated_margin_pct  DECIMAL(6,2)   NULL,
                buybox_price          DECIMAL(12,2)  NULL,
                competitor_lowest     DECIMAL(12,2)  NULL,
                reason_code           VARCHAR(50)    NOT NULL,
                reason_text           NVARCHAR(500)  NULL,
                guardrail_applied     NVARCHAR(200)  NULL,
                status                VARCHAR(20)    NOT NULL DEFAULT 'proposed',
                approved_by           NVARCHAR(100)  NULL,
                approved_at           DATETIME2      NULL,
                executed_at           DATETIME2      NULL,
                error_message         NVARCHAR(500)  NULL,
                created_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
                expires_at            DATETIME2      NULL,

                CONSTRAINT FK_exec_strategy
                    FOREIGN KEY (strategy_id)
                    REFERENCES dbo.acc_repricing_strategy(id)
            );

            CREATE INDEX IX_reprice_exec_status
                ON dbo.acc_repricing_execution (status, created_at DESC)
                WHERE status IN ('proposed', 'approved');

            CREATE INDEX IX_reprice_exec_sku_mkt
                ON dbo.acc_repricing_execution (seller_sku, marketplace_id, created_at DESC);

            CREATE INDEX IX_reprice_exec_strategy
                ON dbo.acc_repricing_execution (strategy_id)
                WHERE strategy_id IS NOT NULL;
        END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_repricing_execution")
    op.execute("DROP TABLE IF EXISTS dbo.acc_repricing_strategy")
