"""Sprint 17 – Content Optimization Engine schema.

Revision ID: eb033
Revises: eb032
Create Date: 2026-03-12
"""
from __future__ import annotations

from alembic import op

revision = "eb033"
down_revision = "eb032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_score')
        CREATE TABLE dbo.acc_content_score (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            seller_sku      NVARCHAR(60)  NOT NULL,
            asin            NVARCHAR(20)  NULL,
            marketplace_id  NVARCHAR(20)  NOT NULL,

            /* Overall score 0-100 */
            total_score     INT           NOT NULL DEFAULT 0,

            /* Sub-scores (each 0-100, weighted into total) */
            title_score     INT           NOT NULL DEFAULT 0,
            bullet_score    INT           NOT NULL DEFAULT 0,
            description_score INT         NOT NULL DEFAULT 0,
            keyword_score   INT           NOT NULL DEFAULT 0,
            image_score     INT           NOT NULL DEFAULT 0,
            aplus_score     INT           NOT NULL DEFAULT 0,

            /* Raw metrics */
            title_length    INT           NULL,
            bullet_count    INT           NULL,
            avg_bullet_len  INT           NULL,
            description_length INT        NULL,
            keyword_length  INT           NULL,
            image_count     INT           NULL,
            has_aplus       BIT           NOT NULL DEFAULT 0,

            /* Diagnostics */
            issues_json     NVARCHAR(MAX) NULL,
            recommendations_json NVARCHAR(MAX) NULL,

            score_version   INT           NOT NULL DEFAULT 1,
            scored_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),

            CONSTRAINT uq_content_score_sku_mkt
                UNIQUE (seller_sku, marketplace_id)
        );
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_cs_mkt_score')
        CREATE INDEX ix_cs_mkt_score
            ON dbo.acc_content_score (marketplace_id, total_score DESC);
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_seo_analysis')
        CREATE TABLE dbo.acc_seo_analysis (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            seller_sku      NVARCHAR(60)  NOT NULL,
            asin            NVARCHAR(20)  NULL,
            marketplace_id  NVARCHAR(20)  NOT NULL,

            /* SEO metrics */
            seo_score       INT           NOT NULL DEFAULT 0,
            keyword_coverage_pct DECIMAL(5,1) NULL,
            missing_keywords_json NVARCHAR(MAX) NULL,
            top_search_terms_json NVARCHAR(MAX) NULL,
            keyword_density_json  NVARCHAR(MAX) NULL,

            /* Title analysis */
            title_keyword_count   INT     NULL,
            title_has_brand       BIT     NOT NULL DEFAULT 0,
            title_has_primary_kw  BIT     NOT NULL DEFAULT 0,

            analyzed_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),

            CONSTRAINT uq_seo_sku_mkt
                UNIQUE (seller_sku, marketplace_id)
        );
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_seo_mkt_score')
        CREATE INDEX ix_seo_mkt_score
            ON dbo.acc_seo_analysis (marketplace_id, seo_score DESC);
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_score_history')
        CREATE TABLE dbo.acc_content_score_history (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            seller_sku      NVARCHAR(60)  NOT NULL,
            marketplace_id  NVARCHAR(20)  NOT NULL,
            total_score     INT           NOT NULL,
            title_score     INT           NOT NULL DEFAULT 0,
            bullet_score    INT           NOT NULL DEFAULT 0,
            description_score INT         NOT NULL DEFAULT 0,
            keyword_score   INT           NOT NULL DEFAULT 0,
            image_score     INT           NOT NULL DEFAULT 0,
            aplus_score     INT           NOT NULL DEFAULT 0,
            snapshot_date   DATE          NOT NULL DEFAULT CAST(SYSUTCDATETIME() AS DATE),

            CONSTRAINT uq_cs_hist_sku_mkt_date
                UNIQUE (seller_sku, marketplace_id, snapshot_date)
        );
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_csh_date')
        CREATE INDEX ix_csh_date
            ON dbo.acc_content_score_history (snapshot_date DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_content_score_history;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_seo_analysis;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_content_score;")
