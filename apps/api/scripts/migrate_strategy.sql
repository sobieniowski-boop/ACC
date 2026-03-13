-- Strategy / Growth Engine — 3 tables
-- Run against ACC Azure SQL

-- 1) Main opportunity entity
IF OBJECT_ID('dbo.growth_opportunity', 'U') IS NULL
CREATE TABLE dbo.growth_opportunity (
    id                       INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_type         VARCHAR(40)   NOT NULL,      -- PRICE_INCREASE, ADS_SCALE_UP, CONTENT_FIX, etc.
    marketplace_id           VARCHAR(30)   NULL,
    sku                      VARCHAR(100)  NULL,
    asin                     VARCHAR(20)   NULL,
    parent_asin              VARCHAR(20)   NULL,
    family_id                INT           NULL,
    title                    NVARCHAR(300) NOT NULL,
    description              NVARCHAR(MAX) NULL,
    root_cause               VARCHAR(40)   NULL,           -- traffic_problem, pricing_problem, etc.
    recommendation           NVARCHAR(MAX) NULL,
    priority_score           DECIMAL(5,1)  NOT NULL DEFAULT 0,
    confidence_score         DECIMAL(5,1)  NOT NULL DEFAULT 0,
    estimated_revenue_uplift DECIMAL(18,2) NULL,
    estimated_profit_uplift  DECIMAL(18,2) NULL,
    estimated_margin_uplift  DECIMAL(8,2)  NULL,
    estimated_units_uplift   INT           NULL,
    effort_score             DECIMAL(5,1)  NULL,            -- 0-100, higher = harder
    owner_role               VARCHAR(40)   NULL,            -- pricing_team, content_team, ads_team, etc.
    blocker_json             NVARCHAR(MAX) NULL,
    source_signals_json      NVARCHAR(MAX) NULL,
    status                   VARCHAR(20)   NOT NULL DEFAULT 'new',  -- new/in_review/accepted/rejected/completed
    created_at               DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at               DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE NONCLUSTERED INDEX IX_growth_opp_status_priority
    ON dbo.growth_opportunity (status, priority_score DESC)
    INCLUDE (opportunity_type, marketplace_id, sku);

CREATE NONCLUSTERED INDEX IX_growth_opp_type
    ON dbo.growth_opportunity (opportunity_type)
    INCLUDE (status, priority_score, marketplace_id, sku);

CREATE NONCLUSTERED INDEX IX_growth_opp_sku
    ON dbo.growth_opportunity (sku)
    INCLUDE (marketplace_id, opportunity_type, status);

-- 2) Experiments table
IF OBJECT_ID('dbo.strategy_experiment', 'U') IS NULL
CREATE TABLE dbo.strategy_experiment (
    id                INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_id    INT           NULL,
    experiment_type   VARCHAR(40)   NOT NULL,  -- price_test, content_refresh, ads_budget, bundle_launch, etc.
    marketplace_id    VARCHAR(30)   NULL,
    sku               VARCHAR(100)  NULL,
    asin              VARCHAR(20)   NULL,
    hypothesis        NVARCHAR(500) NOT NULL,
    owner             VARCHAR(80)   NULL,
    status            VARCHAR(20)   NOT NULL DEFAULT 'planned', -- planned/running/completed/cancelled
    start_date        DATE          NULL,
    end_date          DATE          NULL,
    success_metric    VARCHAR(100)  NULL,
    baseline_value    DECIMAL(18,2) NULL,
    result_value      DECIMAL(18,2) NULL,
    result_summary    NVARCHAR(MAX) NULL,
    created_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE NONCLUSTERED INDEX IX_strategy_exp_status
    ON dbo.strategy_experiment (status)
    INCLUDE (experiment_type, opportunity_id);

-- 3) Opportunity status history / timeline
IF OBJECT_ID('dbo.growth_opportunity_log', 'U') IS NULL
CREATE TABLE dbo.growth_opportunity_log (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_id  INT           NOT NULL,
    action          VARCHAR(30)   NOT NULL,     -- created, accepted, rejected, completed, comment
    actor           VARCHAR(80)   NULL,
    note            NVARCHAR(MAX) NULL,
    created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE NONCLUSTERED INDEX IX_growth_log_opp
    ON dbo.growth_opportunity_log (opportunity_id, created_at);

PRINT 'Strategy / Growth Engine tables created OK';
