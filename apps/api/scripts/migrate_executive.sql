-- Executive Command Center — 3 tables
-- Run against ACC Azure SQL

-- 1) Daily aggregated metrics per marketplace
IF OBJECT_ID('dbo.executive_daily_metrics', 'U') IS NULL
CREATE TABLE dbo.executive_daily_metrics (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    period_date     DATE NOT NULL,
    marketplace_id  VARCHAR(30) NOT NULL,
    revenue_pln     DECIMAL(18,2) NOT NULL DEFAULT 0,
    profit_pln      DECIMAL(18,2) NOT NULL DEFAULT 0,
    margin_pct      DECIMAL(8,2)  NOT NULL DEFAULT 0,
    units           INT NOT NULL DEFAULT 0,
    orders          INT NOT NULL DEFAULT 0,
    ad_spend_pln    DECIMAL(18,2) NOT NULL DEFAULT 0,
    acos_pct        DECIMAL(8,2)  NULL,
    return_rate_pct DECIMAL(8,2)  NULL,
    refund_pln      DECIMAL(18,2) NOT NULL DEFAULT 0,
    cogs_pln        DECIMAL(18,2) NOT NULL DEFAULT 0,
    sessions        INT NULL,
    page_views      INT NULL,
    cvr_pct         DECIMAL(8,2) NULL,
    stockout_skus   INT NOT NULL DEFAULT 0,
    suppressed_skus INT NOT NULL DEFAULT 0,
    computed_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_exec_daily_mkt UNIQUE (period_date, marketplace_id)
);

CREATE NONCLUSTERED INDEX IX_exec_daily_date
    ON dbo.executive_daily_metrics (period_date)
    INCLUDE (marketplace_id, revenue_pln, profit_pln);

-- 2) Health score per day
IF OBJECT_ID('dbo.executive_health_score', 'U') IS NULL
CREATE TABLE dbo.executive_health_score (
    id                INT IDENTITY(1,1) PRIMARY KEY,
    period_date       DATE NOT NULL UNIQUE,
    revenue_score     DECIMAL(5,1) NOT NULL DEFAULT 0,
    profit_score      DECIMAL(5,1) NOT NULL DEFAULT 0,
    demand_score      DECIMAL(5,1) NOT NULL DEFAULT 0,
    inventory_score   DECIMAL(5,1) NOT NULL DEFAULT 0,
    operations_score  DECIMAL(5,1) NOT NULL DEFAULT 0,
    overall_score     DECIMAL(5,1) NOT NULL DEFAULT 0,
    details_json      NVARCHAR(MAX) NULL,
    computed_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 3) Growth opportunities + risks
IF OBJECT_ID('dbo.executive_opportunities', 'U') IS NULL
CREATE TABLE dbo.executive_opportunities (
    id                INT IDENTITY(1,1) PRIMARY KEY,
    opp_type          VARCHAR(20) NOT NULL,  -- 'growth' or 'risk'
    category          VARCHAR(40) NOT NULL,  -- e.g. 'high_sessions_low_cvr', 'profit_decline', 'stockout'
    priority          VARCHAR(5)  NOT NULL DEFAULT 'P2',  -- P1, P2, P3
    marketplace_id    VARCHAR(30) NULL,
    sku               VARCHAR(100) NULL,
    title             NVARCHAR(300) NOT NULL,
    description       NVARCHAR(MAX) NULL,
    impact_estimate   DECIMAL(18,2) NULL,
    confidence        DECIMAL(5,2)  NULL,
    is_active         BIT NOT NULL DEFAULT 1,
    created_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    resolved_at       DATETIME2 NULL,
);

CREATE NONCLUSTERED INDEX IX_exec_opp_active
    ON dbo.executive_opportunities (is_active, opp_type, priority)
    INCLUDE (category, marketplace_id, sku);

PRINT 'Executive Command Center tables created OK';
