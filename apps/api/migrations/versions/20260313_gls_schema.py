"""GLS logistics schema: billing import, billing lines, corrections, BL map.

Converts ensure_gls_schema() DDL into a proper Alembic migration.
Idempotent: all CREATE TABLE / INDEX wrapped in IF checks.

Revision ID: eb012
Revises: eb011
Create Date: 2026-03-13
"""
from alembic import op

revision = "eb012"
down_revision = "eb011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_gls_import_file', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_gls_import_file (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            source_kind NVARCHAR(32) NOT NULL,
            file_path NVARCHAR(500) NOT NULL,
            file_name NVARCHAR(260) NOT NULL,
            document_number NVARCHAR(64) NULL,
            file_size_bytes BIGINT NULL,
            file_mtime_utc DATETIME2 NULL,
            status NVARCHAR(32) NOT NULL DEFAULT 'pending',
            rows_imported INT NOT NULL DEFAULT 0,
            error_message NVARCHAR(MAX) NULL,
            last_imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_gls_billing_document', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_gls_billing_document (
            document_number NVARCHAR(64) NOT NULL PRIMARY KEY,
            billing_period NVARCHAR(16) NULL,
            source_file NVARCHAR(500) NULL,
            detail_rows_count INT NOT NULL DEFAULT 0,
            last_imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_gls_billing_line', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_gls_billing_line (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            document_number NVARCHAR(64) NOT NULL,
            billing_period NVARCHAR(16) NULL,
            row_date DATE NULL,
            delivery_date DATE NULL,
            parcel_number NVARCHAR(64) NOT NULL,
            recipient_name NVARCHAR(255) NULL,
            recipient_postal_code NVARCHAR(32) NULL,
            recipient_city NVARCHAR(255) NULL,
            recipient_country NVARCHAR(16) NULL,
            weight DECIMAL(18,4) NULL,
            declared_weight DECIMAL(18,4) NULL,
            billing_weight DECIMAL(18,4) NULL,
            net_amount DECIMAL(18,4) NULL,
            toll_amount DECIMAL(18,4) NULL,
            fuel_amount DECIMAL(18,4) NULL,
            storewarehouse_amount DECIMAL(18,4) NULL,
            surcharge_amount DECIMAL(18,4) NULL,
            billing_type NVARCHAR(16) NULL,
            note1 NVARCHAR(64) NULL,
            dimension_combined NVARCHAR(128) NULL,
            volumetric_weight DECIMAL(18,4) NULL,
            parcel_status NVARCHAR(255) NULL,
            service_code NVARCHAR(32) NULL,
            source_file NVARCHAR(500) NOT NULL,
            source_row_no INT NOT NULL,
            source_hash NVARCHAR(64) NULL,
            imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_gls_billing_correction_line', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_gls_billing_correction_line (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            document_number NVARCHAR(64) NOT NULL,
            issue_date DATE NULL,
            sales_date DATE NULL,
            parcel_number NVARCHAR(64) NOT NULL,
            recipient_name NVARCHAR(255) NULL,
            recipient_postal_code NVARCHAR(32) NULL,
            recipient_city NVARCHAR(255) NULL,
            recipient_country NVARCHAR(16) NULL,
            original_net_amount DECIMAL(18,4) NULL,
            corrected_net_amount DECIMAL(18,4) NULL,
            net_delta_amount DECIMAL(18,4) NULL,
            fuel_rate_pct DECIMAL(9,6) NULL,
            fuel_correction_amount DECIMAL(18,4) NULL,
            toll_amount DECIMAL(18,4) NULL,
            source_file NVARCHAR(500) NOT NULL,
            source_row_no INT NOT NULL,
            source_hash NVARCHAR(64) NULL,
            imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_gls_bl_map', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_gls_bl_map (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            tracking_number NVARCHAR(64) NOT NULL,
            bl_order_id INT NOT NULL,
            map_source NVARCHAR(64) NULL,
            source_file NVARCHAR(500) NOT NULL,
            source_row_no INT NOT NULL,
            source_hash NVARCHAR(64) NULL,
            imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    # ── Indexes ───────────────────────────────────────────────────────
    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_gls_import_file_kind_path'
          AND object_id = OBJECT_ID('dbo.acc_gls_import_file')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_gls_import_file_kind_path
            ON dbo.acc_gls_import_file(source_kind, file_path);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_gls_import_file_status'
          AND object_id = OBJECT_ID('dbo.acc_gls_import_file')
    )
    BEGIN
        CREATE INDEX IX_acc_gls_import_file_status
            ON dbo.acc_gls_import_file(source_kind, status, updated_at);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_gls_billing_line_parcel_number'
          AND object_id = OBJECT_ID('dbo.acc_gls_billing_line')
    )
    BEGIN
        CREATE INDEX IX_acc_gls_billing_line_parcel_number
            ON dbo.acc_gls_billing_line(parcel_number, row_date, document_number);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_gls_billing_line_note1'
          AND object_id = OBJECT_ID('dbo.acc_gls_billing_line')
    )
    BEGIN
        CREATE INDEX IX_acc_gls_billing_line_note1
            ON dbo.acc_gls_billing_line(note1, row_date);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_gls_billing_line_source_row'
          AND object_id = OBJECT_ID('dbo.acc_gls_billing_line')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_gls_billing_line_source_row
            ON dbo.acc_gls_billing_line(source_file, source_row_no);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_gls_billing_correction_parcel'
          AND object_id = OBJECT_ID('dbo.acc_gls_billing_correction_line')
    )
    BEGIN
        CREATE INDEX IX_acc_gls_billing_correction_parcel
            ON dbo.acc_gls_billing_correction_line(parcel_number, issue_date, imported_at);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_gls_billing_correction_source_row'
          AND object_id = OBJECT_ID('dbo.acc_gls_billing_correction_line')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_gls_billing_correction_source_row
            ON dbo.acc_gls_billing_correction_line(source_file, source_row_no);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_gls_bl_map_tracking'
          AND object_id = OBJECT_ID('dbo.acc_gls_bl_map')
    )
    BEGIN
        CREATE INDEX IX_acc_gls_bl_map_tracking
            ON dbo.acc_gls_bl_map(tracking_number, bl_order_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_gls_bl_map_bl_order_id'
          AND object_id = OBJECT_ID('dbo.acc_gls_bl_map')
    )
    BEGIN
        CREATE INDEX IX_acc_gls_bl_map_bl_order_id
            ON dbo.acc_gls_bl_map(bl_order_id, tracking_number);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_gls_bl_map_source_row'
          AND object_id = OBJECT_ID('dbo.acc_gls_bl_map')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_gls_bl_map_source_row
            ON dbo.acc_gls_bl_map(source_file, source_row_no);
    END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_gls_bl_map")
    op.execute("DROP TABLE IF EXISTS dbo.acc_gls_billing_correction_line")
    op.execute("DROP TABLE IF EXISTS dbo.acc_gls_billing_line")
    op.execute("DROP TABLE IF EXISTS dbo.acc_gls_billing_document")
    op.execute("DROP TABLE IF EXISTS dbo.acc_gls_import_file")
