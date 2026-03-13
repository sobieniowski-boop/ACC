/*
 * Phase 3 — P&L Layer Columns for Rollup Tables
 *
 * Adds cm1_pln and cm2_pln columns to both rollup tables.
 * Existing profit_pln is preserved and becomes the NP (net profit) figure.
 *
 * Run once on Azure SQL (ACC database).
 * Safe to re-run (IF NOT EXISTS guards).
 */

-- =====================================================================
-- 1. acc_sku_profitability_rollup — add cm1_pln, cm2_pln
-- =====================================================================
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.acc_sku_profitability_rollup')
      AND name = 'cm1_pln'
)
BEGIN
    ALTER TABLE dbo.acc_sku_profitability_rollup
        ADD cm1_pln DECIMAL(14,2) NOT NULL DEFAULT 0;
    PRINT 'Added cm1_pln to acc_sku_profitability_rollup';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.acc_sku_profitability_rollup')
      AND name = 'cm2_pln'
)
BEGIN
    ALTER TABLE dbo.acc_sku_profitability_rollup
        ADD cm2_pln DECIMAL(14,2) NOT NULL DEFAULT 0;
    PRINT 'Added cm2_pln to acc_sku_profitability_rollup';
END
GO

-- =====================================================================
-- 2. acc_marketplace_profitability_rollup — add cm1_pln, cm2_pln
-- =====================================================================
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.acc_marketplace_profitability_rollup')
      AND name = 'cm1_pln'
)
BEGIN
    ALTER TABLE dbo.acc_marketplace_profitability_rollup
        ADD cm1_pln DECIMAL(14,2) NOT NULL DEFAULT 0;
    PRINT 'Added cm1_pln to acc_marketplace_profitability_rollup';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.acc_marketplace_profitability_rollup')
      AND name = 'cm2_pln'
)
BEGIN
    ALTER TABLE dbo.acc_marketplace_profitability_rollup
        ADD cm2_pln DECIMAL(14,2) NOT NULL DEFAULT 0;
    PRINT 'Added cm2_pln to acc_marketplace_profitability_rollup';
END
GO

PRINT 'Phase 3 schema migration complete.';
