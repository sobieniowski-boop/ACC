"""Profit TKL cache tables (acc_tkl_cache_meta, acc_tkl_cache_rows).

Revision ID: eb025
Revises: eb024
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb025"
down_revision = "eb024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_tkl_cache_meta', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_tkl_cache_meta (
        cache_key NVARCHAR(40) NOT NULL PRIMARY KEY,
        signature NVARCHAR(1200) NULL,
        source_courier_path NVARCHAR(500) NULL,
        source_courier_mtime DATETIME2 NULL,
        source_tkl_path NVARCHAR(500) NULL,
        source_tkl_mtime DATETIME2 NULL,
        loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_tkl_cache_rows', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_tkl_cache_rows (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        cache_key NVARCHAR(40) NOT NULL,
        row_type NVARCHAR(20) NOT NULL,
        internal_sku NVARCHAR(80) NOT NULL,
        country_code NVARCHAR(10) NULL,
        cost DECIMAL(18,4) NOT NULL,
        courier NVARCHAR(120) NULL,
        source NVARCHAR(160) NULL,
        pack_qty INT NULL,
        [rank] INT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_tkl_cache_rows_lookup
        ON dbo.acc_tkl_cache_rows(cache_key, row_type, internal_sku, country_code);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_tkl_cache_rows")
    op.execute("DROP TABLE IF EXISTS dbo.acc_tkl_cache_meta")
