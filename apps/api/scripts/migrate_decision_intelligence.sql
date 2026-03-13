-- Decision Intelligence — Feedback Loop tables
-- Run against ACC Azure SQL

-- 1) Execution tracking (when an opportunity action is executed)
IF OBJECT_ID('dbo.opportunity_execution', 'U') IS NULL
CREATE TABLE dbo.opportunity_execution (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_id        INT           NOT NULL,
    entity_type           VARCHAR(30)   NULL,     -- sku, asin, family, marketplace
    entity_id             VARCHAR(100)  NULL,     -- the actual SKU / ASIN / family_id
    action_type           VARCHAR(40)   NOT NULL, -- price_change, content_update, ads_adjustment, etc.
    executed_by           VARCHAR(80)   NULL,
    executed_at           DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    baseline_metrics_json NVARCHAR(MAX) NULL,
    expected_metrics_json NVARCHAR(MAX) NULL,
    monitoring_start      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    monitoring_end        DATETIME2     NULL,
    status                VARCHAR(20)   NOT NULL DEFAULT 'monitoring' -- monitoring / evaluated / expired
);

-- 2) Outcome evaluation
IF OBJECT_ID('dbo.opportunity_outcome', 'U') IS NULL
CREATE TABLE dbo.opportunity_outcome (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    execution_id          INT           NOT NULL,
    monitoring_days       INT           NOT NULL,  -- 7, 14, 30, 60
    actual_metrics_json   NVARCHAR(MAX) NULL,
    expected_metrics_json NVARCHAR(MAX) NULL,
    delta_json            NVARCHAR(MAX) NULL,
    success_score         DECIMAL(8,4)  NULL,      -- actual / expected ratio
    impact_score          DECIMAL(8,4)  NULL,      -- normalized 0-100
    confidence_adjustment DECIMAL(6,3)  NULL,      -- e.g. -0.12 or +0.05
    evaluated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 3) Aggregated learning per opportunity type
IF OBJECT_ID('dbo.decision_learning', 'U') IS NULL
CREATE TABLE dbo.decision_learning (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_type      VARCHAR(40)   NOT NULL,
    sample_size           INT           NOT NULL DEFAULT 0,
    avg_expected_profit   DECIMAL(18,2) NULL,
    avg_actual_profit     DECIMAL(18,2) NULL,
    prediction_accuracy   DECIMAL(8,4)  NULL,      -- 0-1 scale
    avg_success_score     DECIMAL(8,4)  NULL,
    confidence_adjustment DECIMAL(6,3)  NULL,
    win_rate              DECIMAL(8,4)  NULL,       -- % success_score >= 0.8
    avg_roi               DECIMAL(8,4)  NULL,
    last_updated          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 4) Model weight adjustments derived from learning
IF OBJECT_ID('dbo.opportunity_model_adjustments', 'U') IS NULL
CREATE TABLE dbo.opportunity_model_adjustments (
    id                          INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_type            VARCHAR(40)   NOT NULL,
    impact_weight_adjustment    DECIMAL(6,3)  NOT NULL DEFAULT 0,
    confidence_weight_adjustment DECIMAL(6,3) NOT NULL DEFAULT 0,
    priority_weight_adjustment  DECIMAL(6,3)  NOT NULL DEFAULT 0,
    reason                      NVARCHAR(500) NULL,
    updated_at                  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Indexes
CREATE NONCLUSTERED INDEX IX_opp_exec_opp_id ON dbo.opportunity_execution (opportunity_id);
CREATE NONCLUSTERED INDEX IX_opp_exec_status ON dbo.opportunity_execution (status, monitoring_end);
CREATE NONCLUSTERED INDEX IX_opp_outcome_exec ON dbo.opportunity_outcome (execution_id, monitoring_days);
CREATE NONCLUSTERED INDEX IX_decision_learning_type ON dbo.decision_learning (opportunity_type);
CREATE NONCLUSTERED INDEX IX_model_adj_type ON dbo.opportunity_model_adjustments (opportunity_type);
