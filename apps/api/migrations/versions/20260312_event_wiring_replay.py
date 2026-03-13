"""Event wiring & replay operations schema.

Sprint 20 — Full SQS Topology (phase 2: event wiring & replay).

New tables:
  acc_event_wire_config  — Module-to-domain-event wiring registry
  acc_replay_job         — Replay operation audit trail

Revision ID: eb035
Revises: eb034
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb035"
down_revision = "eb034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_event_wire_config')
        CREATE TABLE dbo.acc_event_wire_config (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            module_name     NVARCHAR(80)  NOT NULL,
            event_domain    NVARCHAR(40)  NOT NULL,
            event_action    NVARCHAR(80)  NOT NULL DEFAULT '*',
            handler_name    NVARCHAR(120) NOT NULL,
            description     NVARCHAR(500) NULL,
            enabled         BIT           NOT NULL DEFAULT 1,
            priority        INT           NOT NULL DEFAULT 100,
            timeout_seconds INT           NOT NULL DEFAULT 30,
            created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_event_wire_handler UNIQUE (handler_name)
        );
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_replay_job')
        CREATE TABLE dbo.acc_replay_job (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            replay_type     NVARCHAR(40)  NOT NULL,
            scope_domain    NVARCHAR(40)  NULL,
            scope_action    NVARCHAR(80)  NULL,
            scope_event_ids NVARCHAR(MAX) NULL,
            scope_since     DATETIME2     NULL,
            scope_until     DATETIME2     NULL,
            events_matched  INT           NOT NULL DEFAULT 0,
            events_replayed INT           NOT NULL DEFAULT 0,
            events_processed INT          NOT NULL DEFAULT 0,
            events_failed   INT           NOT NULL DEFAULT 0,
            status          NVARCHAR(20)  NOT NULL DEFAULT 'pending',
            triggered_by    NVARCHAR(60)  NULL,
            error_message   NVARCHAR(1000) NULL,
            started_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
            completed_at    DATETIME2     NULL
        );
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_wire_config_domain')
        CREATE INDEX ix_wire_config_domain
        ON dbo.acc_event_wire_config (event_domain, event_action)
        INCLUDE (module_name, enabled);
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_replay_job_status')
        CREATE INDEX ix_replay_job_status
        ON dbo.acc_replay_job (status, started_at DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_replay_job")
    op.execute("DROP TABLE IF EXISTS dbo.acc_event_wire_config")
