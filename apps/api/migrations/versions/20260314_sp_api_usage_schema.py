"""SP-API usage tracking (acc_sp_api_usage_daily).

Revision ID: eb018
Revises: eb017
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb018"
down_revision = "eb017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_sp_api_usage_daily', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_sp_api_usage_daily (
        usage_date DATE NOT NULL,
        endpoint_name NVARCHAR(160) NOT NULL,
        http_method NVARCHAR(10) NOT NULL,
        marketplace_id NVARCHAR(32) NOT NULL DEFAULT '',
        sync_profile NVARCHAR(40) NOT NULL DEFAULT '',
        status_code INT NOT NULL,
        calls_count BIGINT NOT NULL DEFAULT 0,
        success_count BIGINT NOT NULL DEFAULT 0,
        error_count BIGINT NOT NULL DEFAULT 0,
        throttled_count BIGINT NOT NULL DEFAULT 0,
        total_duration_ms BIGINT NOT NULL DEFAULT 0,
        rows_returned BIGINT NOT NULL DEFAULT 0,
        last_error NVARCHAR(500) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_acc_sp_api_usage_daily PRIMARY KEY
            (usage_date, endpoint_name, http_method, marketplace_id, sync_profile, status_code)
    );
    CREATE INDEX IX_acc_sp_api_usage_daily_lookup
        ON dbo.acc_sp_api_usage_daily(endpoint_name, marketplace_id, sync_profile, usage_date);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_sp_api_usage_daily")
