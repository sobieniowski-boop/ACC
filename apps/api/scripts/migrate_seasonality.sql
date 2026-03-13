/*
 * Seasonality & Demand Intelligence Module — MSSQL Schema
 * Tables: monthly metrics, profiles, index cache, opportunities, clusters
 *
 * Depends on existing: acc_sku_profitability_rollup, acc_inv_item_cache
 *
 * Run once on Azure SQL (ACC database).
 */

-- =====================================================================
-- 1. seasonality_monthly_metrics — raw monthly aggregated data per entity
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'seasonality_monthly_metrics')
BEGIN
    CREATE TABLE dbo.seasonality_monthly_metrics (
        id                  INT         IDENTITY(1,1) PRIMARY KEY,
        marketplace         NVARCHAR(10)  NOT NULL,
        entity_type         NVARCHAR(30)  NOT NULL,
        entity_id           NVARCHAR(150) NOT NULL,
        year                INT           NOT NULL,
        month               INT           NOT NULL,
        sessions            DECIMAL(18,4) NULL,
        page_views          DECIMAL(18,4) NULL,
        clicks              DECIMAL(18,4) NULL,
        impressions         DECIMAL(18,4) NULL,
        purchases           DECIMAL(18,4) NULL,
        units               DECIMAL(18,4) NULL,
        orders              DECIMAL(18,4) NULL,
        revenue             DECIMAL(18,4) NULL,
        profit_cm1          DECIMAL(18,4) NULL,
        profit_cm2          DECIMAL(18,4) NULL,
        profit_np           DECIMAL(18,4) NULL,
        unit_session_pct    DECIMAL(18,6) NULL,
        ad_spend            DECIMAL(18,4) NULL,
        refunds             DECIMAL(18,4) NULL,
        stockout_days       INT           NULL,
        suppression_days    INT           NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE UNIQUE INDEX UX_season_monthly
        ON dbo.seasonality_monthly_metrics (marketplace, entity_type, entity_id, year, month);

    CREATE INDEX IX_season_monthly_entity
        ON dbo.seasonality_monthly_metrics (entity_type, entity_id, marketplace);

    PRINT 'Created seasonality_monthly_metrics';
END
GO

-- =====================================================================
-- 2. seasonality_profile — classification + scores per entity
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'seasonality_profile')
BEGIN
    CREATE TABLE dbo.seasonality_profile (
        id                          INT           IDENTITY(1,1) PRIMARY KEY,
        marketplace                 NVARCHAR(10)  NOT NULL,
        entity_type                 NVARCHAR(30)  NOT NULL,
        entity_id                   NVARCHAR(150) NOT NULL,
        seasonality_class           NVARCHAR(30)  NOT NULL,
        demand_strength_score       DECIMAL(9,4)  NOT NULL DEFAULT 0,
        sales_strength_score        DECIMAL(9,4)  NOT NULL DEFAULT 0,
        profit_strength_score       DECIMAL(9,4)  NOT NULL DEFAULT 0,
        evergreen_score             DECIMAL(9,4)  NOT NULL DEFAULT 0,
        volatility_score            DECIMAL(9,4)  NOT NULL DEFAULT 0,
        seasonality_confidence_score DECIMAL(9,4) NOT NULL DEFAULT 0,
        peak_months_json            NVARCHAR(MAX) NULL,
        ramp_months_json            NVARCHAR(MAX) NULL,
        decay_months_json           NVARCHAR(MAX) NULL,
        season_length_months        INT           NULL,
        demand_vs_sales_gap         DECIMAL(9,4)  NULL,
        sales_vs_profit_gap         DECIMAL(9,4)  NULL,
        updated_at                  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE UNIQUE INDEX UX_season_profile
        ON dbo.seasonality_profile (marketplace, entity_type, entity_id);

    CREATE INDEX IX_season_profile_class
        ON dbo.seasonality_profile (seasonality_class, marketplace);

    PRINT 'Created seasonality_profile';
END
GO

-- =====================================================================
-- 3. seasonality_index_cache — normalized 1-12 month indices
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'seasonality_index_cache')
BEGIN
    CREATE TABLE dbo.seasonality_index_cache (
        marketplace         NVARCHAR(10)  NOT NULL,
        entity_type         NVARCHAR(30)  NOT NULL,
        entity_id           NVARCHAR(150) NOT NULL,
        month               INT           NOT NULL,
        demand_index        DECIMAL(18,6) NULL,
        sales_index         DECIMAL(18,6) NULL,
        profit_index        DECIMAL(18,6) NULL,
        updated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_season_index_cache
            PRIMARY KEY (marketplace, entity_type, entity_id, month)
    );

    PRINT 'Created seasonality_index_cache';
END
GO

-- =====================================================================
-- 4. seasonality_opportunity — seasonal action items
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'seasonality_opportunity')
BEGIN
    CREATE TABLE dbo.seasonality_opportunity (
        id                        INT           IDENTITY(1,1) PRIMARY KEY,
        marketplace               NVARCHAR(10)  NOT NULL,
        entity_type               NVARCHAR(30)  NOT NULL,
        entity_id                 NVARCHAR(150) NOT NULL,
        opportunity_type          NVARCHAR(40)  NOT NULL,
        title                     NVARCHAR(300) NOT NULL,
        description               NVARCHAR(MAX) NOT NULL,
        priority_score            DECIMAL(9,4)  NOT NULL,
        confidence_score          DECIMAL(9,4)  NOT NULL,
        estimated_revenue_uplift  DECIMAL(18,4) NULL,
        estimated_profit_uplift   DECIMAL(18,4) NULL,
        recommended_start_date    DATE          NULL,
        status                    NVARCHAR(20)  NOT NULL DEFAULT 'new',
        source_signals_json       NVARCHAR(MAX) NULL,
        created_at                DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_season_opp_status
        ON dbo.seasonality_opportunity (status, marketplace);

    CREATE INDEX IX_season_opp_entity
        ON dbo.seasonality_opportunity (entity_type, entity_id, marketplace);

    CREATE INDEX IX_season_opp_type
        ON dbo.seasonality_opportunity (opportunity_type, marketplace);

    PRINT 'Created seasonality_opportunity';
END
GO

-- =====================================================================
-- 5. seasonality_cluster — custom product groupings
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'seasonality_cluster')
BEGIN
    CREATE TABLE dbo.seasonality_cluster (
        id              INT           IDENTITY(1,1) PRIMARY KEY,
        cluster_name    NVARCHAR(150) NOT NULL,
        description     NVARCHAR(MAX) NULL,
        rules_json      NVARCHAR(MAX) NULL,
        created_by      NVARCHAR(120) NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE UNIQUE INDEX UX_cluster_name
        ON dbo.seasonality_cluster (cluster_name);

    PRINT 'Created seasonality_cluster';
END
GO

-- =====================================================================
-- 6. seasonality_cluster_member — cluster membership
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'seasonality_cluster_member')
BEGIN
    CREATE TABLE dbo.seasonality_cluster_member (
        id              INT           IDENTITY(1,1) PRIMARY KEY,
        cluster_id      INT           NOT NULL,
        sku             NVARCHAR(80)  NULL,
        asin            NVARCHAR(20)  NULL,
        product_type    NVARCHAR(120) NULL,
        category        NVARCHAR(200) NULL,

        CONSTRAINT FK_cluster_member_cluster
            FOREIGN KEY (cluster_id) REFERENCES dbo.seasonality_cluster(id)
    );

    CREATE INDEX IX_cluster_member_cid
        ON dbo.seasonality_cluster_member (cluster_id);

    CREATE INDEX IX_cluster_member_sku
        ON dbo.seasonality_cluster_member (sku);

    PRINT 'Created seasonality_cluster_member';
END
GO

-- =====================================================================
-- 7. seasonality_settings — per-tenant config
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'seasonality_settings')
BEGIN
    CREATE TABLE dbo.seasonality_settings (
        id                  INT           IDENTITY(1,1) PRIMARY KEY,
        setting_key         NVARCHAR(80)  NOT NULL,
        setting_value       NVARCHAR(MAX) NOT NULL,
        updated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE UNIQUE INDEX UX_season_setting_key
        ON dbo.seasonality_settings (setting_key);

    -- Seed defaults
    INSERT INTO dbo.seasonality_settings (setting_key, setting_value) VALUES
        ('reference_window_months', '24'),
        ('min_confidence_threshold', '40'),
        ('upcoming_peak_horizon_days', '60'),
        ('evergreen_threshold', '70'),
        ('strong_seasonal_threshold', '60'),
        ('peak_seasonal_threshold', '80'),
        ('mild_seasonal_threshold', '30');

    PRINT 'Created seasonality_settings';
END
GO
