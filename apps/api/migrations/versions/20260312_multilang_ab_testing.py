"""Sprint 18 – multi-language content generation & A/B content testing schema.

Revision ID: eb034
Revises: eb033
"""
from alembic import op
import sqlalchemy as sa

revision = "eb034"
down_revision = "eb033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Multi-language generation log ────────────────────────────────
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_multilang_job')
    CREATE TABLE dbo.acc_multilang_job (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(60)  NOT NULL,
        asin            NVARCHAR(20)  NULL,
        source_marketplace_id NVARCHAR(20) NOT NULL,
        target_marketplace_id NVARCHAR(20) NOT NULL,
        target_language  NVARCHAR(10)  NOT NULL,
        status           NVARCHAR(20)  NOT NULL DEFAULT 'pending',
        source_version_id NVARCHAR(60) NULL,
        target_version_id NVARCHAR(60) NULL,
        model            NVARCHAR(40)  NULL,
        quality_score    INT           NULL,
        quality_issues_json NVARCHAR(MAX) NULL,
        policy_flags_json   NVARCHAR(MAX) NULL,
        error_message    NVARCHAR(500) NULL,
        created_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        completed_at     DATETIME2     NULL,
        CONSTRAINT uq_multilang_job UNIQUE (seller_sku, source_marketplace_id, target_marketplace_id)
    );
    """)

    # ── A/B Content Experiment ───────────────────────────────────────
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_experiment')
    CREATE TABLE dbo.acc_content_experiment (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        name            NVARCHAR(200) NOT NULL,
        seller_sku      NVARCHAR(60)  NOT NULL,
        marketplace_id  NVARCHAR(20)  NOT NULL,
        status          NVARCHAR(20)  NOT NULL DEFAULT 'draft',
        hypothesis      NVARCHAR(500) NULL,
        metric_primary  NVARCHAR(40)  NOT NULL DEFAULT 'conversion_rate',
        start_date      DATE          NULL,
        end_date        DATE          NULL,
        winner_variant_id INT         NULL,
        created_by      NVARCHAR(60)  NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        concluded_at    DATETIME2     NULL
    );
    """)

    # ── A/B Content Variant ──────────────────────────────────────────
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_variant')
    CREATE TABLE dbo.acc_content_variant (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        experiment_id   INT           NOT NULL,
        label           NVARCHAR(10)  NOT NULL DEFAULT 'A',
        version_id      NVARCHAR(60)  NULL,
        is_control      BIT           NOT NULL DEFAULT 0,
        impressions     INT           NOT NULL DEFAULT 0,
        clicks          INT           NOT NULL DEFAULT 0,
        orders          INT           NOT NULL DEFAULT 0,
        revenue         DECIMAL(12,2) NOT NULL DEFAULT 0,
        conversion_rate DECIMAL(6,3)  NULL,
        ctr             DECIMAL(6,3)  NULL,
        content_score   INT           NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT fk_variant_experiment FOREIGN KEY (experiment_id) REFERENCES dbo.acc_content_experiment(id),
        CONSTRAINT uq_variant_exp_label UNIQUE (experiment_id, label)
    );
    """)

    # Indexes
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_multilang_job_sku')
    CREATE INDEX ix_multilang_job_sku ON dbo.acc_multilang_job (seller_sku, source_marketplace_id);
    """)
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_experiment_sku_mkt')
    CREATE INDEX ix_experiment_sku_mkt ON dbo.acc_content_experiment (seller_sku, marketplace_id);
    """)
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_variant_experiment')
    CREATE INDEX ix_variant_experiment ON dbo.acc_content_variant (experiment_id);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_content_variant")
    op.execute("DROP TABLE IF EXISTS dbo.acc_content_experiment")
    op.execute("DROP TABLE IF EXISTS dbo.acc_multilang_job")
