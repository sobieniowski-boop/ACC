/*
ACC performance recommendations (prepared, NOT executed automatically).
Apply in a controlled maintenance window and monitor DTU/IO after each index.
*/

/* 1) Executive / Growth: active opportunities sorted by impact + recency */
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.executive_opportunities')
      AND name = 'IX_exec_opp_active_impact'
)
BEGIN
    CREATE INDEX IX_exec_opp_active_impact
    ON dbo.executive_opportunities (is_active, opp_type, impact_estimate DESC, created_at DESC)
    INCLUDE (priority, category, marketplace_id, sku, title, description, confidence);
END;
GO

/* 2) Profitability overview: wide rollup reads by date + marketplace */
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.acc_sku_profitability_rollup')
      AND name = 'IX_sku_rollup_overview_cover'
)
BEGIN
    CREATE INDEX IX_sku_rollup_overview_cover
    ON dbo.acc_sku_profitability_rollup (period_date, marketplace_id)
    INCLUDE (sku, asin, revenue_pln, profit_pln, orders_count, units_sold, ad_spend_pln, refund_pln, refund_units, margin_pct);
END;
GO

/* 3) Inventory module: cache table filtered by snapshot + marketplace */
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.acc_inv_item_cache')
      AND name = 'IX_acc_inv_item_cache_snapshot'
)
BEGIN
    CREATE INDEX IX_acc_inv_item_cache_snapshot
    ON dbo.acc_inv_item_cache (snapshot_date, marketplace_id, listing_status, stockout_risk_badge, family_health)
    INCLUDE (sku, asin, parent_asin, local_parent_asin, fba_available, sessions_7d, orders_7d, units_ordered_7d, days_cover, cvr_delta_pct, sessions_delta_pct, traffic_coverage_flag);
END;
GO

/* 4) Missing-index DMV repeatedly suggests fulfillment_channel + purchase_date */
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.acc_order')
      AND name = 'IX_acc_order_fc_purchase'
)
BEGIN
    CREATE INDEX IX_acc_order_fc_purchase
    ON dbo.acc_order (fulfillment_channel, purchase_date)
    INCLUDE (amazon_order_id, status, marketplace_id, revenue_pln, cogs_pln, amazon_fees_pln, ads_cost_pln, logistics_pln, contribution_margin_pln);
END;
GO
