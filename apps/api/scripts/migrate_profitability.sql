/*
 * Finance Profitability Module — MSSQL Schema
 * Tables: rollup caches + profitability alert config
 * 
 * Depends on existing: acc_order, acc_order_line, acc_finance_transaction,
 *   acc_ads_campaign_day, acc_shipment_cost, acc_exchange_rate, acc_product
 *
 * Run once on Azure SQL (ACC database).
 */

-- =====================================================================
-- 1. SKU Profitability Rollup (daily pre-aggregated)
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_sku_profitability_rollup')
BEGIN
    CREATE TABLE dbo.acc_sku_profitability_rollup (
        id                  BIGINT      IDENTITY(1,1) PRIMARY KEY,
        period_date         DATE        NOT NULL,               -- day granularity
        marketplace_id      VARCHAR(30) NOT NULL,
        sku                 VARCHAR(100) NOT NULL,
        asin                VARCHAR(20) NULL,

        -- Volume
        units_sold          INT         NOT NULL DEFAULT 0,
        orders_count        INT         NOT NULL DEFAULT 0,

        -- Revenue (PLN)
        revenue_pln         DECIMAL(14,2) NOT NULL DEFAULT 0,

        -- Cost buckets (PLN, all positive = cost)
        cogs_pln            DECIMAL(14,2) NOT NULL DEFAULT 0,
        amazon_fees_pln     DECIMAL(14,2) NOT NULL DEFAULT 0,
        fba_fees_pln        DECIMAL(14,2) NOT NULL DEFAULT 0,
        logistics_pln       DECIMAL(14,2) NOT NULL DEFAULT 0,
        ad_spend_pln        DECIMAL(14,2) NOT NULL DEFAULT 0,
        refund_pln          DECIMAL(14,2) NOT NULL DEFAULT 0,
        storage_fee_pln     DECIMAL(14,2) NOT NULL DEFAULT 0,
        other_fees_pln      DECIMAL(14,2) NOT NULL DEFAULT 0,

        -- Calculated
        profit_pln          DECIMAL(14,2) NOT NULL DEFAULT 0,   -- revenue - all costs
        margin_pct          DECIMAL(8,4)  NULL,                 -- profit / revenue * 100
        acos_pct            DECIMAL(8,4)  NULL,                 -- ad_spend / revenue * 100

        -- Refund metrics
        refund_units        INT         NOT NULL DEFAULT 0,
        return_rate_pct     DECIMAL(8,4) NULL,                  -- refund_units / units_sold * 100

        -- Meta
        computed_at         DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_sku_rollup_day UNIQUE (period_date, marketplace_id, sku)
    );

    CREATE NONCLUSTERED INDEX IX_sku_rollup_mkt_sku
        ON dbo.acc_sku_profitability_rollup (marketplace_id, sku, period_date)
        INCLUDE (revenue_pln, profit_pln, margin_pct);

    CREATE NONCLUSTERED INDEX IX_sku_rollup_date
        ON dbo.acc_sku_profitability_rollup (period_date)
        INCLUDE (marketplace_id, sku, revenue_pln, profit_pln);

    CREATE NONCLUSTERED INDEX IX_sku_rollup_margin
        ON dbo.acc_sku_profitability_rollup (margin_pct)
        INCLUDE (period_date, marketplace_id, sku, profit_pln);

    PRINT 'Created acc_sku_profitability_rollup';
END
GO

-- =====================================================================
-- 2. Marketplace Profitability Rollup (daily pre-aggregated)
-- =====================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_marketplace_profitability_rollup')
BEGIN
    CREATE TABLE dbo.acc_marketplace_profitability_rollup (
        id                  BIGINT      IDENTITY(1,1) PRIMARY KEY,
        period_date         DATE        NOT NULL,
        marketplace_id      VARCHAR(30) NOT NULL,

        -- Volume
        total_orders        INT         NOT NULL DEFAULT 0,
        total_units         INT         NOT NULL DEFAULT 0,
        unique_skus         INT         NOT NULL DEFAULT 0,

        -- Revenue (PLN)
        revenue_pln         DECIMAL(14,2) NOT NULL DEFAULT 0,

        -- Cost buckets (PLN)
        cogs_pln            DECIMAL(14,2) NOT NULL DEFAULT 0,
        amazon_fees_pln     DECIMAL(14,2) NOT NULL DEFAULT 0,
        fba_fees_pln        DECIMAL(14,2) NOT NULL DEFAULT 0,
        logistics_pln       DECIMAL(14,2) NOT NULL DEFAULT 0,
        ad_spend_pln        DECIMAL(14,2) NOT NULL DEFAULT 0,
        refund_pln          DECIMAL(14,2) NOT NULL DEFAULT 0,
        storage_fee_pln     DECIMAL(14,2) NOT NULL DEFAULT 0,
        other_fees_pln      DECIMAL(14,2) NOT NULL DEFAULT 0,

        -- Calculated
        profit_pln          DECIMAL(14,2) NOT NULL DEFAULT 0,
        margin_pct          DECIMAL(8,4)  NULL,
        acos_pct            DECIMAL(8,4)  NULL,

        -- Refunds
        refund_units        INT         NOT NULL DEFAULT 0,
        return_rate_pct     DECIMAL(8,4) NULL,

        computed_at         DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_mkt_rollup_day UNIQUE (period_date, marketplace_id)
    );

    CREATE NONCLUSTERED INDEX IX_mkt_rollup_date
        ON dbo.acc_marketplace_profitability_rollup (period_date)
        INCLUDE (marketplace_id, revenue_pln, profit_pln, margin_pct);

    PRINT 'Created acc_marketplace_profitability_rollup';
END
GO

-- =====================================================================
-- 3. Profitability alert thresholds (extends existing acc_alert_rule)
--    No new table needed — use existing alert_rule with rule_type:
--      'loss_order', 'high_acos', 'high_return_rate', 'low_margin'
-- =====================================================================

PRINT 'Schema migration complete.';
