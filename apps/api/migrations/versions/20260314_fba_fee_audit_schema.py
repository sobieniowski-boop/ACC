"""FBA fee reference table (acc_fba_fee_reference).

Revision ID: eb022
Revises: eb021
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb022"
down_revision = "eb021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_fba_fee_reference', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_fee_reference (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        sku NVARCHAR(100) NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        size_tier NVARCHAR(100) NULL,
        weight_kg DECIMAL(8,3) NULL,
        longest_side_cm DECIMAL(8,2) NULL,
        median_side_cm DECIMAL(8,2) NULL,
        shortest_side_cm DECIMAL(8,2) NULL,
        expected_fee_eur DECIMAL(10,4) NOT NULL,
        valid_from DATE NULL,
        valid_to DATE NULL,
        source NVARCHAR(100) DEFAULT 'manual',
        notes NVARCHAR(500) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_fba_fee_ref_sku ON dbo.acc_fba_fee_reference(sku);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_fba_fee_reference")
