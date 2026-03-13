"""Operator Console v2 — Sprint 23-24.

Tables:
  acc_operator_case    — Generic case/ticket management
  acc_action_queue     — Action queue with approval workflow

Revision ID: eb037
Revises: eb036
Create Date: 2026-03-15
"""
from alembic import op

revision = "eb037"
down_revision = "eb036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── acc_operator_case ──────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_operator_case', 'U') IS NULL
    CREATE TABLE dbo.acc_operator_case (
        id                INT IDENTITY(1,1) PRIMARY KEY,
        title             NVARCHAR(400)  NOT NULL,
        description       NVARCHAR(MAX)  NULL,
        category          NVARCHAR(40)   NOT NULL DEFAULT 'other',
        priority          NVARCHAR(20)   NOT NULL DEFAULT 'medium',
        status            NVARCHAR(20)   NOT NULL DEFAULT 'open',
        marketplace_id    NVARCHAR(20)   NULL,
        sku               NVARCHAR(60)   NULL,
        asin              NVARCHAR(20)   NULL,
        source_type       NVARCHAR(40)   NULL,
        source_id         INT            NULL,
        assigned_to       NVARCHAR(200)  NULL,
        resolution_note   NVARCHAR(MAX)  NULL,
        resolved_by       NVARCHAR(200)  NULL,
        resolved_at       DATETIME2      NULL,
        due_date          DATE           NULL,
        tags              NVARCHAR(500)  NULL,
        created_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_acc_operator_case_status_priority')
    CREATE INDEX IX_acc_operator_case_status_priority
        ON dbo.acc_operator_case(status, priority);
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_acc_operator_case_category')
    CREATE INDEX IX_acc_operator_case_category
        ON dbo.acc_operator_case(category, status);
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_acc_operator_case_assigned')
    CREATE INDEX IX_acc_operator_case_assigned
        ON dbo.acc_operator_case(assigned_to, status);
    """)

    # ── acc_action_queue ───────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_action_queue', 'U') IS NULL
    CREATE TABLE dbo.acc_action_queue (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        action_type         NVARCHAR(60)   NOT NULL,
        title               NVARCHAR(400)  NOT NULL,
        description         NVARCHAR(MAX)  NULL,
        marketplace_id      NVARCHAR(20)   NULL,
        sku                 NVARCHAR(60)   NULL,
        asin                NVARCHAR(20)   NULL,
        payload             NVARCHAR(MAX)  NULL,
        risk_level          NVARCHAR(20)   NOT NULL DEFAULT 'low',
        requires_approval   BIT            NOT NULL DEFAULT 1,
        status              NVARCHAR(30)   NOT NULL DEFAULT 'pending_approval',
        requested_by        NVARCHAR(200)  NOT NULL,
        approved_by         NVARCHAR(200)  NULL,
        approved_at         DATETIME2      NULL,
        rejected_by         NVARCHAR(200)  NULL,
        rejected_at         DATETIME2      NULL,
        rejection_reason    NVARCHAR(MAX)  NULL,
        executed_at         DATETIME2      NULL,
        execution_result    NVARCHAR(MAX)  NULL,
        error_message       NVARCHAR(MAX)  NULL,
        expires_at          DATETIME2      NULL,
        created_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_acc_action_queue_status')
    CREATE INDEX IX_acc_action_queue_status
        ON dbo.acc_action_queue(status, created_at DESC);
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_acc_action_queue_type')
    CREATE INDEX IX_acc_action_queue_type
        ON dbo.acc_action_queue(action_type, status);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_action_queue;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_operator_case;")
