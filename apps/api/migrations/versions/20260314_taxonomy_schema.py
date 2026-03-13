"""Taxonomy tables (acc_taxonomy_node, acc_taxonomy_alias, acc_taxonomy_prediction).

Revision ID: eb016
Revises: eb015
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb016"
down_revision = "eb015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_taxonomy_node', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_taxonomy_node (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        node_key NVARCHAR(160) NOT NULL UNIQUE,
        canonical_label_pl NVARCHAR(255) NOT NULL,
        parent_node_key NVARCHAR(160) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_taxonomy_node_parent
        ON dbo.acc_taxonomy_node(parent_node_key, is_active);
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_taxonomy_alias', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_taxonomy_alias (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        alias_key NVARCHAR(200) NOT NULL,
        node_key NVARCHAR(160) NOT NULL,
        source NVARCHAR(40) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_taxonomy_alias_key_source
        ON dbo.acc_taxonomy_alias(alias_key, source);
    CREATE INDEX IX_acc_taxonomy_alias_node
        ON dbo.acc_taxonomy_alias(node_key);
END
""")
    op.execute("""
IF OBJECT_ID('dbo.acc_taxonomy_prediction', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_taxonomy_prediction (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(32) NULL,
        sku NVARCHAR(120) NULL,
        asin NVARCHAR(40) NULL,
        ean NVARCHAR(80) NULL,
        suggested_brand NVARCHAR(128) NULL,
        suggested_category NVARCHAR(255) NULL,
        suggested_product_type NVARCHAR(255) NULL,
        confidence DECIMAL(8,4) NOT NULL,
        source NVARCHAR(40) NOT NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        reason NVARCHAR(500) NULL,
        evidence_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        reviewed_by NVARCHAR(120) NULL,
        reviewed_at DATETIME2 NULL
    );
    CREATE INDEX IX_acc_taxonomy_prediction_state
        ON dbo.acc_taxonomy_prediction(status, confidence, source, updated_at DESC);
    CREATE INDEX IX_acc_taxonomy_prediction_sku
        ON dbo.acc_taxonomy_prediction(sku, status, confidence DESC);
    CREATE INDEX IX_acc_taxonomy_prediction_asin
        ON dbo.acc_taxonomy_prediction(asin, status, confidence DESC);
    CREATE INDEX IX_acc_taxonomy_prediction_ean
        ON dbo.acc_taxonomy_prediction(ean, status, confidence DESC);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_taxonomy_prediction")
    op.execute("DROP TABLE IF EXISTS dbo.acc_taxonomy_alias")
    op.execute("DROP TABLE IF EXISTS dbo.acc_taxonomy_node")
