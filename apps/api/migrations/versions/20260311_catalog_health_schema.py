"""Catalog health tables: acc_listing_field_diff, acc_listing_health_snapshot.

Sprint 10 — S10.1

Revision ID: eb027
Revises: eb026
Create Date: 2026-03-11
"""
from alembic import op

revision = "eb027"
down_revision = "eb026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Field-level diff tracking ──
    op.execute("""
IF OBJECT_ID('dbo.acc_listing_field_diff', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_listing_field_diff (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(100) NOT NULL,
        marketplace_id  VARCHAR(20)   NOT NULL,
        field_name      VARCHAR(50)   NOT NULL,
        old_value       NVARCHAR(500) NULL,
        new_value       NVARCHAR(500) NULL,
        change_source   VARCHAR(50)   NOT NULL DEFAULT 'unknown',
        detected_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX ix_lfd_sku_mkt
        ON dbo.acc_listing_field_diff (seller_sku, marketplace_id, detected_at DESC);
    CREATE INDEX ix_lfd_detected
        ON dbo.acc_listing_field_diff (detected_at DESC);
END
""")

    # ── Persisted health score snapshots ──
    op.execute("""
IF OBJECT_ID('dbo.acc_listing_health_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_listing_health_snapshot (
        id                      BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku              NVARCHAR(100)  NOT NULL,
        marketplace_id          VARCHAR(20)    NOT NULL,
        snapshot_date           DATE           NOT NULL,
        health_score            SMALLINT       NOT NULL,
        status_pts              SMALLINT       NOT NULL DEFAULT 0,
        issues_pts              SMALLINT       NOT NULL DEFAULT 0,
        suppression_pts         SMALLINT       NOT NULL DEFAULT 0,
        basic_content_pts       SMALLINT       NOT NULL DEFAULT 0,
        content_completeness_pts SMALLINT      NOT NULL DEFAULT 0,
        listing_status          VARCHAR(30)    NULL,
        is_suppressed           BIT            NOT NULL DEFAULT 0,
        has_issues              BIT            NOT NULL DEFAULT 0,
        computed_at             DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_lhs_sku_mkt_date UNIQUE (seller_sku, marketplace_id, snapshot_date)
    );
    CREATE INDEX ix_lhs_date ON dbo.acc_listing_health_snapshot (snapshot_date DESC);
    CREATE INDEX ix_lhs_score ON dbo.acc_listing_health_snapshot (snapshot_date, health_score);
    CREATE INDEX ix_lhs_mkt ON dbo.acc_listing_health_snapshot (marketplace_id, snapshot_date DESC);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_listing_health_snapshot")
    op.execute("DROP TABLE IF EXISTS dbo.acc_listing_field_diff")
