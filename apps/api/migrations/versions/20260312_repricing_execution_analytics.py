"""Sprint 16 – Repricing Execution & Analytics schema additions.

Revision ID: eb032
Revises: eb031
Create Date: 2026-03-12

Adds:
  - feed_id, auto_approved columns to acc_repricing_execution
  - acc_repricing_analytics table for daily aggregated metrics
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "eb032"
down_revision: Union[str, None] = "eb031"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── acc_repricing_execution: add columns ──
    op.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('dbo.acc_repricing_execution')
              AND name = 'feed_id'
        )
        ALTER TABLE dbo.acc_repricing_execution
        ADD feed_id NVARCHAR(100) NULL
    """)
    op.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('dbo.acc_repricing_execution')
              AND name = 'auto_approved'
        )
        ALTER TABLE dbo.acc_repricing_execution
        ADD auto_approved BIT NOT NULL DEFAULT 0
    """)

    # ── acc_repricing_analytics ──
    op.execute("""
        IF OBJECT_ID('dbo.acc_repricing_analytics', 'U') IS NULL
        CREATE TABLE dbo.acc_repricing_analytics (
            id                    INT            IDENTITY(1,1) PRIMARY KEY,
            analytics_date        DATE           NOT NULL,
            marketplace_id        VARCHAR(20)    NULL,
            proposals_created     INT            NOT NULL DEFAULT 0,
            proposals_approved    INT            NOT NULL DEFAULT 0,
            proposals_rejected    INT            NOT NULL DEFAULT 0,
            proposals_expired     INT            NOT NULL DEFAULT 0,
            executions_submitted  INT            NOT NULL DEFAULT 0,
            executions_succeeded  INT            NOT NULL DEFAULT 0,
            executions_failed     INT            NOT NULL DEFAULT 0,
            auto_approved_count   INT            NOT NULL DEFAULT 0,
            avg_price_change_pct  DECIMAL(8,2)   NULL,
            avg_margin_after      DECIMAL(6,2)   NULL,
            total_revenue_impact  DECIMAL(14,2)  NULL,
            created_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT UQ_reprice_analytics_date_mkt
                UNIQUE (analytics_date, marketplace_id)
        )
    """)

    op.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'ix_reprice_analytics_date'
              AND object_id = OBJECT_ID('dbo.acc_repricing_analytics')
        )
        CREATE INDEX ix_reprice_analytics_date
        ON dbo.acc_repricing_analytics (analytics_date DESC)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_repricing_analytics")
    op.execute("""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('dbo.acc_repricing_execution')
              AND name = 'feed_id'
        )
        ALTER TABLE dbo.acc_repricing_execution DROP COLUMN feed_id
    """)
    op.execute("""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('dbo.acc_repricing_execution')
              AND name = 'auto_approved'
        )
        ALTER TABLE dbo.acc_repricing_execution DROP COLUMN auto_approved
    """)
