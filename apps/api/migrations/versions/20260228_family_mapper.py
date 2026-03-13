"""Family Mapper (DE Canonical → EU) schema.

Revision ID: fm001
Revises: (none — first migration)
Create Date: 2026-02-28
"""
from alembic import op
import sqlalchemy as sa

revision = "fm001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1) global_family
    op.execute("""
    IF OBJECT_ID('dbo.global_family', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.global_family (
            id              INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            de_parent_asin  NVARCHAR(20)  NOT NULL,
            brand           NVARCHAR(120) NULL,
            category        NVARCHAR(200) NULL,
            product_type    NVARCHAR(120) NULL,
            variation_theme_de NVARCHAR(120) NULL,
            created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE UNIQUE INDEX UX_global_family_de_parent_asin
            ON dbo.global_family(de_parent_asin);
    END
    """)

    # 2) global_family_child
    op.execute("""
    IF OBJECT_ID('dbo.global_family_child', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.global_family_child (
            id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            global_family_id  INT            NOT NULL,
            master_key        NVARCHAR(120)  NOT NULL,
            key_type          NVARCHAR(20)   NOT NULL,
            de_child_asin     NVARCHAR(20)   NOT NULL,
            sku_de            NVARCHAR(80)   NULL,
            ean_de            NVARCHAR(20)   NULL,
            attributes_json   NVARCHAR(MAX)  NULL,
            created_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT FK_gfc_family FOREIGN KEY (global_family_id)
                REFERENCES dbo.global_family(id)
        );
        CREATE UNIQUE INDEX UX_gfc_family_master
            ON dbo.global_family_child(global_family_id, master_key);
        CREATE INDEX IX_gfc_master_key
            ON dbo.global_family_child(master_key);
        CREATE INDEX IX_gfc_de_child_asin
            ON dbo.global_family_child(de_child_asin);
    END
    """)

    # 3) marketplace_listing_child
    op.execute("""
    IF OBJECT_ID('dbo.marketplace_listing_child', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.marketplace_listing_child (
            marketplace       NVARCHAR(10)  NOT NULL,
            asin              NVARCHAR(20)  NOT NULL,
            sku               NVARCHAR(80)  NULL,
            ean               NVARCHAR(20)  NULL,
            current_parent_asin NVARCHAR(20) NULL,
            variation_theme   NVARCHAR(120) NULL,
            attributes_json   NVARCHAR(MAX) NULL,
            updated_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_mlc PRIMARY KEY (marketplace, asin)
        );
        CREATE INDEX IX_mlc_mp_sku
            ON dbo.marketplace_listing_child(marketplace, sku);
        CREATE INDEX IX_mlc_mp_ean
            ON dbo.marketplace_listing_child(marketplace, ean);
        CREATE INDEX IX_mlc_mp_parent
            ON dbo.marketplace_listing_child(marketplace, current_parent_asin);
    END
    """)

    # 4) global_family_child_market_link
    op.execute("""
    IF OBJECT_ID('dbo.global_family_child_market_link', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.global_family_child_market_link (
            global_family_id   INT           NOT NULL,
            master_key         NVARCHAR(120) NOT NULL,
            marketplace        NVARCHAR(10)  NOT NULL,
            target_child_asin  NVARCHAR(20)  NULL,
            current_parent_asin NVARCHAR(20) NULL,
            match_type         NVARCHAR(20)  NOT NULL,
            confidence         INT           NOT NULL,
            status             NVARCHAR(20)  NOT NULL,
            reason_json        NVARCHAR(MAX) NULL,
            updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_gfcml PRIMARY KEY (global_family_id, master_key, marketplace)
        );
        CREATE INDEX IX_gfcl_mp_target_child
            ON dbo.global_family_child_market_link(marketplace, target_child_asin);
        CREATE INDEX IX_gfcl_mp_current_parent
            ON dbo.global_family_child_market_link(marketplace, current_parent_asin);
    END
    """)

    # 5) global_family_market_link
    op.execute("""
    IF OBJECT_ID('dbo.global_family_market_link', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.global_family_market_link (
            global_family_id  INT           NOT NULL,
            marketplace       NVARCHAR(10)  NOT NULL,
            target_parent_asin NVARCHAR(20) NULL,
            status            NVARCHAR(20)  NOT NULL,
            confidence_avg    INT           NOT NULL DEFAULT 0,
            notes             NVARCHAR(MAX) NULL,
            updated_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_gfml PRIMARY KEY (global_family_id, marketplace)
        );
    END
    """)

    # 6) family_coverage_cache
    op.execute("""
    IF OBJECT_ID('dbo.family_coverage_cache', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.family_coverage_cache (
            global_family_id     INT          NOT NULL,
            marketplace          NVARCHAR(10) NOT NULL,
            de_children_count    INT          NOT NULL,
            matched_children_count INT        NOT NULL,
            coverage_pct         INT          NOT NULL,
            missing_children_count INT        NOT NULL,
            extra_children_count  INT         NOT NULL,
            theme_mismatch       BIT          NOT NULL DEFAULT 0,
            confidence_avg       INT          NOT NULL DEFAULT 0,
            updated_at           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_fcc PRIMARY KEY (global_family_id, marketplace)
        );
    END
    """)

    # 7) family_issues_cache
    op.execute("""
    IF OBJECT_ID('dbo.family_issues_cache', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.family_issues_cache (
            id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            global_family_id  INT            NOT NULL,
            marketplace       NVARCHAR(10)   NULL,
            issue_type        NVARCHAR(40)   NOT NULL,
            severity          NVARCHAR(10)   NOT NULL,
            payload_json      NVARCHAR(MAX)  NULL,
            created_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        CREATE INDEX IX_fic_family
            ON dbo.family_issues_cache(global_family_id, marketplace);
        CREATE INDEX IX_fic_severity
            ON dbo.family_issues_cache(severity, issue_type);
    END
    """)

    # 8) family_fix_package
    op.execute("""
    IF OBJECT_ID('dbo.family_fix_package', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.family_fix_package (
            id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            marketplace      NVARCHAR(10)  NOT NULL,
            global_family_id INT           NOT NULL,
            action_plan_json NVARCHAR(MAX) NOT NULL,
            status           NVARCHAR(20)  NOT NULL DEFAULT 'draft',
            generated_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            approved_by      NVARCHAR(120) NULL,
            approved_at      DATETIME2 NULL,
            applied_at       DATETIME2 NULL
        );
        CREATE INDEX IX_ffp_mp_status
            ON dbo.family_fix_package(marketplace, status);
        CREATE INDEX IX_ffp_family
            ON dbo.family_fix_package(global_family_id);
    END
    """)

    # 9) family_fix_job
    op.execute("""
    IF OBJECT_ID('dbo.family_fix_job', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.family_fix_job (
            id           INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            job_type     NVARCHAR(40)  NOT NULL DEFAULT 'unknown',
            marketplace  NVARCHAR(10)  NOT NULL,
            status       NVARCHAR(20)  NOT NULL DEFAULT 'pending',
            progress     INT           NOT NULL DEFAULT 0,
            started_at   DATETIME2     NULL,
            finished_at  DATETIME2     NULL,
            log          NVARCHAR(MAX) NULL
        );
        CREATE INDEX IX_ffj_status
            ON dbo.family_fix_job(status, marketplace);
    END
    """)


def downgrade() -> None:
    for table in [
        "family_fix_job",
        "family_fix_package",
        "family_issues_cache",
        "family_coverage_cache",
        "global_family_market_link",
        "global_family_child_market_link",
        "marketplace_listing_child",
        "global_family_child",
        "global_family",
    ]:
        op.execute(f"IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.{table}")
