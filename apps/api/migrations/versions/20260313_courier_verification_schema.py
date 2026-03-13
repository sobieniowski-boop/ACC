"""Courier verification schema: audit log table.

Converts ensure_courier_verification_schema() DDL into a proper Alembic migration.
Idempotent: CREATE TABLE / INDEX wrapped in IF OBJECT_ID check.

Revision ID: eb013
Revises: eb012
Create Date: 2026-03-13
"""
from alembic import op

revision = "eb013"
down_revision = "eb012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_courier_audit_log', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_courier_audit_log (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            audit_type NVARCHAR(64) NOT NULL,
            carrier NVARCHAR(16) NOT NULL,
            scope_key NVARCHAR(64) NOT NULL,
            status NVARCHAR(20) NOT NULL,
            expected_count INT NOT NULL DEFAULT 0,
            imported_count INT NOT NULL DEFAULT 0,
            failed_count INT NOT NULL DEFAULT 0,
            missing_count INT NOT NULL DEFAULT 0,
            extra_count INT NOT NULL DEFAULT 0,
            matched_count INT NOT NULL DEFAULT 0,
            detail_json NVARCHAR(MAX) NULL,
            trigger_source NVARCHAR(32) NOT NULL DEFAULT 'manual',
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE UNIQUE INDEX UX_acc_courier_audit_scope
            ON dbo.acc_courier_audit_log(audit_type, carrier, scope_key, trigger_source);
    END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_courier_audit_log")
