"""PTD cache tables (acc_ptd_cache, acc_ptd_sync_state).

Revision ID: eb017
Revises: eb016
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb017"
down_revision = "eb016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_ptd_cache', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_ptd_cache (
        id                   INT IDENTITY(1,1) PRIMARY KEY,
        product_type         NVARCHAR(200)   NOT NULL,
        marketplace_id       VARCHAR(20)     NOT NULL,
        requirements         VARCHAR(30)     NOT NULL DEFAULT 'LISTING',
        locale               VARCHAR(20)     NOT NULL DEFAULT 'DEFAULT',
        schema_json_gz       VARBINARY(MAX)  NULL,
        schema_size_bytes    INT             NOT NULL DEFAULT 0,
        schema_version_hash  VARCHAR(64)     NOT NULL,
        sp_api_version       VARCHAR(50)     NULL,
        property_groups      INT             NOT NULL DEFAULT 0,
        required_attributes  INT             NOT NULL DEFAULT 0,
        total_attributes     INT             NOT NULL DEFAULT 0,
        has_variations       BIT             NOT NULL DEFAULT 0,
        variation_theme      NVARCHAR(500)   NULL,
        fetched_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        created_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_ptd_cache_type_mkt_req_locale
            UNIQUE (product_type, marketplace_id, requirements, locale)
    );
    CREATE INDEX ix_ptd_cache_mkt     ON dbo.acc_ptd_cache(marketplace_id);
    CREATE INDEX ix_ptd_cache_fetched ON dbo.acc_ptd_cache(fetched_at);
    CREATE INDEX ix_ptd_cache_type    ON dbo.acc_ptd_cache(product_type);
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_ptd_sync_state', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_ptd_sync_state (
        marketplace_id       VARCHAR(20)     NOT NULL PRIMARY KEY,
        last_synced_at       DATETIME2       NULL,
        product_types_count  INT             NOT NULL DEFAULT 0,
        last_error           NVARCHAR(500)   NULL,
        updated_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_ptd_sync_state")
    op.execute("DROP TABLE IF EXISTS dbo.acc_ptd_cache")
