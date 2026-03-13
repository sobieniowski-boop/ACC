"""Add acc_system_alert table for backbone dead-letter monitoring.

Revision ID: eb003
Revises: eb002
Create Date: 2026-03-10
"""
from alembic import op

revision = "eb003"
down_revision = "eb002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_system_alert', 'U') IS NULL
    CREATE TABLE dbo.acc_system_alert (
        id          BIGINT IDENTITY(1,1) PRIMARY KEY,
        alert_type  VARCHAR(100)   NOT NULL,
        severity    VARCHAR(20)    NOT NULL,
        message     NVARCHAR(2000) NOT NULL,
        details     NVARCHAR(MAX)  NULL,
        created_at  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        INDEX ix_system_alert_type_created (alert_type, created_at)
    )
    """)


def downgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_system_alert', 'U') IS NOT NULL
        DROP TABLE dbo.acc_system_alert
    """)
