"""Add acc_event_handler_health table + handler_timeout/circuit_open columns.

Revision ID: eb002
Revises: fm001
Create Date: 2026-03-10
"""
from alembic import op

revision = "eb002"
down_revision = "fm001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1) acc_event_handler_health — per-handler circuit breaker state
    op.execute("""
    IF OBJECT_ID('dbo.acc_event_handler_health', 'U') IS NULL
    CREATE TABLE dbo.acc_event_handler_health (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        handler_name        VARCHAR(100)  NOT NULL UNIQUE,
        failure_count       INT           NOT NULL DEFAULT 0,
        last_failure_at     DATETIME2     NULL,
        circuit_open_until  DATETIME2     NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """)

    # 2) handler_timeout column on acc_event_processing_log
    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_event_processing_log')
          AND name = 'handler_timeout'
    )
    ALTER TABLE dbo.acc_event_processing_log
        ADD handler_timeout BIT NOT NULL DEFAULT 0
    """)

    # 3) circuit_open column on acc_event_processing_log
    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_event_processing_log')
          AND name = 'circuit_open'
    )
    ALTER TABLE dbo.acc_event_processing_log
        ADD circuit_open BIT NOT NULL DEFAULT 0
    """)


def downgrade() -> None:
    op.execute("""
    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_event_processing_log')
          AND name = 'circuit_open'
    )
    ALTER TABLE dbo.acc_event_processing_log DROP COLUMN circuit_open
    """)

    op.execute("""
    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('dbo.acc_event_processing_log')
          AND name = 'handler_timeout'
    )
    ALTER TABLE dbo.acc_event_processing_log DROP COLUMN handler_timeout
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_event_handler_health', 'U') IS NOT NULL
    DROP TABLE dbo.acc_event_handler_health
    """)
