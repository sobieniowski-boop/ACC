"""SQS queue topology & dead-letter queue tracking.

Sprint 19 — Full SQS Topology (first half).

New tables:
  acc_sqs_queue_topology  — Per-domain queue registry with DLQ config
  acc_dlq_entry           — Dead-letter queue entry tracking

Revision ID: eb019
Revises: eb018
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb019"
down_revision = "eb018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_sqs_queue_topology')
        CREATE TABLE dbo.acc_sqs_queue_topology (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            domain          NVARCHAR(40)  NOT NULL,
            queue_url       NVARCHAR(500) NOT NULL,
            queue_arn       NVARCHAR(500) NULL,
            dlq_url         NVARCHAR(500) NULL,
            dlq_arn         NVARCHAR(500) NULL,
            region          NVARCHAR(20)  NOT NULL DEFAULT 'eu-west-1',
            max_receive_count INT         NOT NULL DEFAULT 3,
            visibility_timeout_seconds INT NOT NULL DEFAULT 30,
            message_retention_days INT    NOT NULL DEFAULT 14,
            polling_interval_seconds INT  NOT NULL DEFAULT 120,
            batch_size      INT           NOT NULL DEFAULT 10,
            enabled         BIT           NOT NULL DEFAULT 1,
            status          NVARCHAR(20)  NOT NULL DEFAULT 'active',
            messages_received BIGINT      NOT NULL DEFAULT 0,
            messages_processed BIGINT     NOT NULL DEFAULT 0,
            messages_failed BIGINT        NOT NULL DEFAULT 0,
            messages_dlq    BIGINT        NOT NULL DEFAULT 0,
            last_poll_at    DATETIME2     NULL,
            last_error      NVARCHAR(500) NULL,
            created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_sqs_topology_domain UNIQUE (domain)
        );
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_dlq_entry')
        CREATE TABLE dbo.acc_dlq_entry (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            domain          NVARCHAR(40)  NOT NULL,
            queue_url       NVARCHAR(500) NOT NULL,
            message_id      NVARCHAR(200) NOT NULL,
            receipt_handle  NVARCHAR(2000) NULL,
            body            NVARCHAR(MAX) NULL,
            approximate_receive_count INT NOT NULL DEFAULT 0,
            original_event_id NVARCHAR(64) NULL,
            error_message   NVARCHAR(1000) NULL,
            status          NVARCHAR(20)  NOT NULL DEFAULT 'unresolved',
            resolution      NVARCHAR(20)  NULL,
            resolved_by     NVARCHAR(60)  NULL,
            resolved_at     DATETIME2     NULL,
            created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
        );
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_dlq_domain_status')
        CREATE INDEX ix_dlq_domain_status
        ON dbo.acc_dlq_entry (domain, status)
        INCLUDE (created_at);
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_dlq_message_id')
        CREATE UNIQUE INDEX ix_dlq_message_id
        ON dbo.acc_dlq_entry (message_id);
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_topology_enabled')
        CREATE INDEX ix_topology_enabled
        ON dbo.acc_sqs_queue_topology (enabled, domain);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_dlq_entry")
    op.execute("DROP TABLE IF EXISTS dbo.acc_sqs_queue_topology")
