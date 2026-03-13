"""Sellerboard history tables (acc_sb_order_line_staging, sync_state, rebuild_state).

Revision ID: eb019
Revises: eb018
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb019"
down_revision = "eb018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_sb_order_line_staging', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_sb_order_line_staging (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        source_name NVARCHAR(255) NOT NULL,
        source_hash NVARCHAR(64) NOT NULL,
        source_row_number INT NOT NULL,
        source_row_hash NVARCHAR(64) NOT NULL,
        amazon_order_id NVARCHAR(40) NOT NULL,
        purchase_date DATETIME2 NOT NULL,
        marketplace_id NVARCHAR(32) NULL,
        marketplace_code NVARCHAR(10) NULL,
        sales_channel NVARCHAR(120) NULL,
        fulfillment_channel NVARCHAR(20) NULL,
        order_status NVARCHAR(40) NULL,
        currency NVARCHAR(8) NULL,
        asin NVARCHAR(40) NULL,
        product_token NVARCHAR(255) NULL,
        quantity DECIMAL(18,4) NULL,
        order_total_amount DECIMAL(18,4) NULL,
        shipping_amount DECIMAL(18,4) NULL,
        gift_wrap_amount DECIMAL(18,4) NULL,
        tax_amount DECIMAL(18,4) NULL,
        item_promotion_amount DECIMAL(18,4) NULL,
        ship_promotion_amount DECIMAL(18,4) NULL,
        commission_amount DECIMAL(18,4) NULL,
        fba_fee_amount DECIMAL(18,4) NULL,
        coupon_amount DECIMAL(18,4) NULL,
        raw_shipping_cost_amount DECIMAL(18,4) NULL,
        is_premium_order BIT NULL,
        shipped_by_amazon_tfm BIT NULL,
        is_replacement_order BIT NULL,
        is_business_order BIT NULL,
        is_prime BIT NULL,
        shipment_service_level NVARCHAR(80) NULL,
        raw_json NVARCHAR(MAX) NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_sb_order_line_staging_source_row
        ON dbo.acc_sb_order_line_staging(source_name, source_row_number);
    CREATE INDEX IX_acc_sb_order_line_staging_order
        ON dbo.acc_sb_order_line_staging(amazon_order_id, purchase_date);
    CREATE INDEX IX_acc_sb_order_line_staging_marketplace
        ON dbo.acc_sb_order_line_staging(marketplace_id, purchase_date);
    CREATE INDEX IX_acc_sb_order_line_staging_asin
        ON dbo.acc_sb_order_line_staging(asin, purchase_date);
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_sb_order_line_sync_state', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_sb_order_line_sync_state (
        source_name NVARCHAR(255) NOT NULL PRIMARY KEY,
        source_hash NVARCHAR(64) NULL,
        row_count_total INT NOT NULL DEFAULT 0,
        row_count_2025 INT NOT NULL DEFAULT 0,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        note NVARCHAR(400) NULL,
        last_imported_at DATETIME2 NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_sb_order_line_rebuild_state', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_sb_order_line_rebuild_state (
        period_key NVARCHAR(32) NOT NULL PRIMARY KEY,
        date_from DATE NOT NULL,
        date_to DATE NOT NULL,
        source_name NVARCHAR(255) NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        target_orders INT NOT NULL DEFAULT 0,
        candidate_lines INT NOT NULL DEFAULT 0,
        candidate_with_product INT NOT NULL DEFAULT 0,
        candidate_with_sku INT NOT NULL DEFAULT 0,
        inserted_lines INT NOT NULL DEFAULT 0,
        note NVARCHAR(400) NULL,
        started_at DATETIME2 NULL,
        finished_at DATETIME2 NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
""")
    # Widen period_key if it was created with a shorter length
    op.execute("""
IF EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME = 'acc_sb_order_line_rebuild_state'
      AND COLUMN_NAME = 'period_key'
      AND CHARACTER_MAXIMUM_LENGTH < 32
)
BEGIN
    ALTER TABLE dbo.acc_sb_order_line_rebuild_state
    ALTER COLUMN period_key NVARCHAR(32) NOT NULL;
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_sb_order_line_rebuild_state")
    op.execute("DROP TABLE IF EXISTS dbo.acc_sb_order_line_sync_state")
    op.execute("DROP TABLE IF EXISTS dbo.acc_sb_order_line_staging")
