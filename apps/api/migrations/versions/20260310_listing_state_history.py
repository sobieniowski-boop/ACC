"""Add acc_listing_state_history table for tracking listing status transitions.

Revision ID: eb004a
Revises: eb004
Create Date: 2026-03-10
"""
from alembic import op

revision = "eb004a"
down_revision = "eb004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_listing_state_history', 'U') IS NULL
    CREATE TABLE dbo.acc_listing_state_history (
        id                BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku        NVARCHAR(100)  NOT NULL,
        marketplace_id    VARCHAR(20)    NOT NULL,
        asin              VARCHAR(20)    NULL,
        previous_status   VARCHAR(30)    NULL,
        new_status        VARCHAR(30)    NOT NULL,
        issue_code        NVARCHAR(200)  NULL,
        issue_severity    VARCHAR(20)    NULL,
        changed_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        change_source     VARCHAR(50)    NOT NULL DEFAULT 'unknown',
        INDEX ix_lsh_sku_mkt_changed (seller_sku, marketplace_id, changed_at)
    )
    """)


def downgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_listing_state_history', 'U') IS NOT NULL
        DROP TABLE dbo.acc_listing_state_history
    """)
