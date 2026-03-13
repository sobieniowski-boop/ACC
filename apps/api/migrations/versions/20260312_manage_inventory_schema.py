"""Manage inventory schema: traffic tables, item cache, change drafts, settings.

Converts ensure_manage_inventory_schema() DDL into a proper Alembic migration.
Idempotent: all CREATE TABLE/INDEX wrapped in IF OBJECT_ID checks.

Revision ID: eb010
Revises: eb009
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb010"
down_revision = "eb009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_traffic_sku_daily', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_traffic_sku_daily (
            marketplace_id NVARCHAR(32) NOT NULL,
            sku NVARCHAR(120) NOT NULL,
            report_date DATE NOT NULL,
            asin NVARCHAR(40) NULL,
            sessions INT NULL,
            page_views INT NULL,
            units_ordered INT NULL,
            orders_count INT NULL,
            revenue DECIMAL(18,4) NULL,
            unit_session_pct DECIMAL(18,6) NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_acc_inv_traffic_sku_daily PRIMARY KEY (marketplace_id, sku, report_date)
        );
        CREATE INDEX IX_acc_inv_traffic_sku_daily_date
            ON dbo.acc_inv_traffic_sku_daily(report_date, marketplace_id);
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_traffic_asin_daily', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_traffic_asin_daily (
            marketplace_id NVARCHAR(32) NOT NULL,
            asin NVARCHAR(40) NOT NULL,
            report_date DATE NOT NULL,
            sessions INT NULL,
            page_views INT NULL,
            units_ordered INT NULL,
            orders_count INT NULL,
            revenue DECIMAL(18,4) NULL,
            unit_session_pct DECIMAL(18,6) NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_acc_inv_traffic_asin_daily PRIMARY KEY (marketplace_id, asin, report_date)
        );
        CREATE INDEX IX_acc_inv_traffic_asin_daily_date
            ON dbo.acc_inv_traffic_asin_daily(report_date, marketplace_id);
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_traffic_rollup', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_traffic_rollup (
            marketplace_id NVARCHAR(32) NOT NULL,
            sku NVARCHAR(120) NULL,
            asin NVARCHAR(40) NULL,
            range_key NVARCHAR(10) NOT NULL,
            sessions INT NULL,
            page_views INT NULL,
            units INT NULL,
            orders_count INT NULL,
            revenue DECIMAL(18,4) NULL,
            unit_session_pct DECIMAL(18,6) NULL,
            sessions_delta_pct DECIMAL(18,6) NULL,
            cvr_delta_pct DECIMAL(18,6) NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE UNIQUE INDEX UX_acc_inv_traffic_rollup
            ON dbo.acc_inv_traffic_rollup(marketplace_id, range_key, sku, asin);
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_item_cache', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_item_cache (
            marketplace_id NVARCHAR(32) NOT NULL,
            sku NVARCHAR(120) NOT NULL,
            asin NVARCHAR(40) NULL,
            snapshot_date DATE NULL,
            title_preferred NVARCHAR(400) NULL,
            listing_status NVARCHAR(32) NULL,
            stockout_risk_badge NVARCHAR(32) NULL,
            overstock_risk_badge NVARCHAR(32) NULL,
            family_health NVARCHAR(40) NULL,
            global_family_status NVARCHAR(40) NULL,
            days_cover DECIMAL(18,4) NULL,
            sessions_7d INT NULL,
            orders_7d INT NULL,
            units_ordered_7d INT NULL,
            unit_session_pct_7d DECIMAL(18,6) NULL,
            cvr_delta_pct DECIMAL(18,6) NULL,
            sessions_delta_pct DECIMAL(18,6) NULL,
            traffic_coverage_flag BIT NOT NULL DEFAULT 1,
            internal_sku NVARCHAR(80) NULL,
            ean NVARCHAR(80) NULL,
            payload_json NVARCHAR(MAX) NOT NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_acc_inv_item_cache PRIMARY KEY (marketplace_id, sku)
        );
        CREATE INDEX IX_acc_inv_item_cache_filters
            ON dbo.acc_inv_item_cache(marketplace_id, listing_status, stockout_risk_badge, family_health, traffic_coverage_flag);
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_change_draft', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_change_draft (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            draft_type NVARCHAR(40) NOT NULL,
            marketplace_id NVARCHAR(32) NULL,
            affected_parent_asin NVARCHAR(40) NULL,
            affected_sku NVARCHAR(120) NULL,
            payload_json NVARCHAR(MAX) NOT NULL,
            snapshot_before_json NVARCHAR(MAX) NULL,
            snapshot_after_json NVARCHAR(MAX) NULL,
            validation_status NVARCHAR(20) NOT NULL DEFAULT 'pending',
            validation_errors_json NVARCHAR(MAX) NULL,
            approval_status NVARCHAR(20) NOT NULL DEFAULT 'draft',
            apply_status NVARCHAR(20) NOT NULL DEFAULT 'pending',
            created_by NVARCHAR(120) NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            approved_by NVARCHAR(120) NULL,
            approved_at DATETIME2 NULL,
            apply_started_at DATETIME2 NULL,
            applied_at DATETIME2 NULL,
            rolled_back_at DATETIME2 NULL
        );
        CREATE INDEX IX_acc_inv_change_draft_status
            ON dbo.acc_inv_change_draft(validation_status, approval_status, apply_status, created_at);
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_change_event', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_change_event (
            id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            draft_id UNIQUEIDENTIFIER NOT NULL,
            event_type NVARCHAR(40) NOT NULL,
            actor NVARCHAR(120) NULL,
            payload_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE INDEX IX_acc_inv_change_event_main
            ON dbo.acc_inv_change_event(draft_id, created_at DESC);
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_settings', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_settings (
            [key] NVARCHAR(80) NOT NULL PRIMARY KEY,
            value_json NVARCHAR(MAX) NOT NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_inv_category_cvr_baseline', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_inv_category_cvr_baseline (
            marketplace_id NVARCHAR(32) NOT NULL,
            category NVARCHAR(200) NOT NULL,
            p25 DECIMAL(18,6) NULL,
            p50 DECIMAL(18,6) NULL,
            p75 DECIMAL(18,6) NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_acc_inv_category_cvr_baseline PRIMARY KEY (marketplace_id, category)
        );
    END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_category_cvr_baseline")
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_settings")
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_change_event")
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_change_draft")
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_item_cache")
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_traffic_rollup")
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_traffic_asin_daily")
    op.execute("DROP TABLE IF EXISTS dbo.acc_inv_traffic_sku_daily")
