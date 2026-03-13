"""Profit data quality tables (acc_fee_gap_watch, acc_fee_gap_recheck_run).

Revision ID: eb023
Revises: eb022
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb023"
down_revision = "eb022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_fee_gap_watch', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fee_gap_watch (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        gap_type NVARCHAR(20) NOT NULL,
        gap_reason NVARCHAR(80) NOT NULL,
        marketplace_id NVARCHAR(160) NOT NULL,
        amazon_order_id NVARCHAR(40) NOT NULL,
        sample_sku NVARCHAR(120) NULL,
        sample_asin NVARCHAR(40) NULL,
        fulfillment_channel NVARCHAR(20) NULL,
        status NVARCHAR(40) NOT NULL DEFAULT 'open',
        first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_checked_at DATETIME2 NULL,
        resolved_at DATETIME2 NULL,
        last_amazon_event_count INT NOT NULL DEFAULT 0,
        last_note NVARCHAR(500) NULL
    );
    CREATE UNIQUE INDEX IX_acc_fee_gap_watch_unique
        ON dbo.acc_fee_gap_watch(gap_type, marketplace_id, amazon_order_id);
    CREATE INDEX IX_acc_fee_gap_watch_state
        ON dbo.acc_fee_gap_watch(status, gap_type, marketplace_id, last_seen_at DESC);
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_fee_gap_recheck_run', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fee_gap_recheck_run (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        started_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_at DATETIME2 NULL,
        scope_json NVARCHAR(MAX) NULL,
        checked_count INT NOT NULL DEFAULT 0,
        resolved_count INT NOT NULL DEFAULT 0,
        amazon_events_available_count INT NOT NULL DEFAULT 0,
        still_missing_count INT NOT NULL DEFAULT 0,
        note NVARCHAR(500) NULL
    );
    CREATE INDEX IX_acc_fee_gap_recheck_run_started
        ON dbo.acc_fee_gap_recheck_run(started_at DESC);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_fee_gap_recheck_run")
    op.execute("DROP TABLE IF EXISTS dbo.acc_fee_gap_watch")
