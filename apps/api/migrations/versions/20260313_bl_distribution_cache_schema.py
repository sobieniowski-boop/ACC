"""BaseLinker distribution cache schema: order and package cache tables.

Converts ensure_bl_distribution_cache_schema() DDL into a proper Alembic migration.
Idempotent: CREATE TABLE / INDEX wrapped in IF checks.

Revision ID: eb015
Revises: eb014
Create Date: 2026-03-13
"""
from alembic import op

revision = "eb015"
down_revision = "eb014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_bl_distribution_order_cache', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_bl_distribution_order_cache (
            order_id BIGINT NOT NULL PRIMARY KEY,
            shop_order_id BIGINT NULL,
            external_order_id NVARCHAR(128) NULL,
            order_source NVARCHAR(64) NOT NULL,
            order_source_id INT NULL,
            order_status_id INT NULL,
            date_add DATETIME2 NULL,
            date_confirmed DATETIME2 NULL,
            date_in_status DATETIME2 NULL,
            confirmed BIT NULL,
            delivery_method NVARCHAR(255) NULL,
            delivery_package_module NVARCHAR(128) NULL,
            delivery_package_nr NVARCHAR(255) NULL,
            delivery_country_code NVARCHAR(16) NULL,
            delivery_fullname NVARCHAR(255) NULL,
            email NVARCHAR(255) NULL,
            phone NVARCHAR(64) NULL,
            admin_comments NVARCHAR(MAX) NULL,
            extra_field_1 NVARCHAR(255) NULL,
            extra_field_2 NVARCHAR(255) NULL,
            order_page NVARCHAR(500) NULL,
            pick_state INT NULL,
            pack_state INT NULL,
            raw_payload_json NVARCHAR(MAX) NULL,
            source_hash NVARCHAR(64) NULL,
            last_synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_bl_distribution_package_cache', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_bl_distribution_package_cache (
            package_id BIGINT NOT NULL PRIMARY KEY,
            order_id BIGINT NOT NULL,
            courier_package_nr NVARCHAR(255) NULL,
            courier_inner_number NVARCHAR(255) NULL,
            courier_code NVARCHAR(64) NULL,
            courier_other_name NVARCHAR(128) NULL,
            account_id NVARCHAR(64) NULL,
            tracking_status_date DATETIME2 NULL,
            tracking_delivery_days INT NULL,
            tracking_status NVARCHAR(32) NULL,
            tracking_url NVARCHAR(500) NULL,
            is_return BIT NULL,
            package_type NVARCHAR(32) NULL,
            raw_payload_json NVARCHAR(MAX) NULL,
            source_hash NVARCHAR(64) NULL,
            last_synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    # ── Indexes ───────────────────────────────────────────────────────
    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_bl_distribution_order_confirmed'
          AND object_id = OBJECT_ID('dbo.acc_bl_distribution_order_cache')
    )
    BEGIN
        CREATE INDEX IX_acc_bl_distribution_order_confirmed
            ON dbo.acc_bl_distribution_order_cache(order_source, order_source_id, date_confirmed, order_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_bl_distribution_order_package_nr'
          AND object_id = OBJECT_ID('dbo.acc_bl_distribution_order_cache')
    )
    BEGIN
        CREATE INDEX IX_acc_bl_distribution_order_package_nr
            ON dbo.acc_bl_distribution_order_cache(delivery_package_nr, order_source_id, date_confirmed);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_bl_distribution_order_external'
          AND object_id = OBJECT_ID('dbo.acc_bl_distribution_order_cache')
    )
    BEGIN
        CREATE INDEX IX_acc_bl_distribution_order_external
            ON dbo.acc_bl_distribution_order_cache(external_order_id, order_source_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_bl_distribution_package_order'
          AND object_id = OBJECT_ID('dbo.acc_bl_distribution_package_cache')
    )
    BEGIN
        CREATE INDEX IX_acc_bl_distribution_package_order
            ON dbo.acc_bl_distribution_package_cache(order_id, package_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_bl_distribution_package_tracking'
          AND object_id = OBJECT_ID('dbo.acc_bl_distribution_package_cache')
    )
    BEGIN
        CREATE INDEX IX_acc_bl_distribution_package_tracking
            ON dbo.acc_bl_distribution_package_cache(courier_package_nr, order_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_bl_distribution_package_inner'
          AND object_id = OBJECT_ID('dbo.acc_bl_distribution_package_cache')
    )
    BEGIN
        CREATE INDEX IX_acc_bl_distribution_package_inner
            ON dbo.acc_bl_distribution_package_cache(courier_inner_number, order_id);
    END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_bl_distribution_package_cache")
    op.execute("DROP TABLE IF EXISTS dbo.acc_bl_distribution_order_cache")
