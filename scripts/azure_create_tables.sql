-- ====================================================================
-- Amazon Command Center — Azure SQL Database Schema
-- All acc_* tables for a fresh Azure SQL Free Tier database
-- Generated: 2026-02-28
-- ====================================================================
-- Run this script ONCE against the new Azure SQL database.
-- Order matters — parent tables first, then children with FK references.
-- ====================================================================

-- ==========================
-- 1. acc_user
-- ==========================
IF OBJECT_ID('dbo.acc_user', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_user (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        email NVARCHAR(255) NOT NULL,
        full_name NVARCHAR(255) NOT NULL,
        hashed_password NVARCHAR(255) NOT NULL,
        role NVARCHAR(50) NOT NULL DEFAULT 'analyst',
        is_active BIT NOT NULL DEFAULT 1,
        is_superuser BIT NOT NULL DEFAULT 0,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_login_at DATETIME2 NULL,
        CONSTRAINT UQ_acc_user_email UNIQUE (email)
    );
    CREATE INDEX IX_acc_user_email ON dbo.acc_user(email);
END
GO

-- ==========================
-- 2. acc_marketplace
-- ==========================
IF OBJECT_ID('dbo.acc_marketplace', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_marketplace (
        id NVARCHAR(20) NOT NULL PRIMARY KEY,
        code NVARCHAR(5) NOT NULL,
        name NVARCHAR(100) NOT NULL,
        currency NVARCHAR(5) NOT NULL,
        timezone NVARCHAR(50) NOT NULL,
        is_active BIT NOT NULL DEFAULT 1,
        CONSTRAINT UQ_acc_marketplace_code UNIQUE (code)
    );
    CREATE INDEX IX_acc_marketplace_code ON dbo.acc_marketplace(code);
END
GO

-- ==========================
-- 3. acc_product
-- ==========================
IF OBJECT_ID('dbo.acc_product', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_product (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        asin NVARCHAR(20) NULL,
        ean NVARCHAR(20) NULL,
        sku NVARCHAR(100) NULL,
        brand NVARCHAR(100) NULL,
        category NVARCHAR(200) NULL,
        subcategory NVARCHAR(200) NULL,
        title NVARCHAR(500) NULL,
        image_url NVARCHAR(500) NULL,
        is_parent BIT NOT NULL DEFAULT 0,
        parent_asin NVARCHAR(20) NULL,
        netto_purchase_price_pln DECIMAL(10,4) NULL,
        vat_rate DECIMAL(5,2) NULL DEFAULT 23.0,
        internal_sku NVARCHAR(20) NULL,
        k_number NVARCHAR(20) NULL,
        ergonode_id NVARCHAR(36) NULL,
        mapping_source NVARCHAR(20) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_product_asin UNIQUE (asin)
    );
    CREATE INDEX IX_acc_product_asin ON dbo.acc_product(asin);
    CREATE INDEX IX_acc_product_ean ON dbo.acc_product(ean);
    CREATE INDEX IX_acc_product_sku ON dbo.acc_product(sku);
    CREATE INDEX IX_acc_product_internal_sku ON dbo.acc_product(internal_sku);
    CREATE INDEX IX_acc_product_k_number ON dbo.acc_product(k_number);
    CREATE INDEX IX_acc_product_parent_asin ON dbo.acc_product(parent_asin);
END
GO

-- ==========================
-- 4. acc_order
-- ==========================
IF OBJECT_ID('dbo.acc_order', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_order (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        amazon_order_id NVARCHAR(50) NOT NULL,
        marketplace_id NVARCHAR(20) NOT NULL,
        status NVARCHAR(50) NOT NULL,
        fulfillment_channel NVARCHAR(20) NOT NULL DEFAULT 'FBA',
        sales_channel NVARCHAR(100) NULL,
        purchase_date DATETIMEOFFSET NOT NULL,
        last_update_date DATETIMEOFFSET NULL,
        ship_date DATETIMEOFFSET NULL,
        order_total DECIMAL(12,2) NULL,
        currency NVARCHAR(5) NOT NULL DEFAULT 'EUR',
        revenue_pln DECIMAL(12,2) NULL,
        vat_pln DECIMAL(12,2) NULL,
        cogs_pln DECIMAL(12,2) NULL,
        amazon_fees_pln DECIMAL(12,2) NULL,
        ads_cost_pln DECIMAL(12,2) NULL,
        logistics_pln DECIMAL(12,2) NULL,
        contribution_margin_pln DECIMAL(12,2) NULL,
        cm_percent DECIMAL(8,4) NULL,
        buyer_country NVARCHAR(5) NULL,
        ship_country NVARCHAR(5) NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_order_amazon_id UNIQUE (amazon_order_id),
        CONSTRAINT FK_acc_order_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id)
    );
    CREATE INDEX IX_acc_order_amazon_id ON dbo.acc_order(amazon_order_id);
    CREATE INDEX IX_acc_order_marketplace ON dbo.acc_order(marketplace_id);
    CREATE INDEX IX_acc_order_status ON dbo.acc_order(status);
    CREATE INDEX IX_acc_order_purchase_date ON dbo.acc_order(purchase_date);
END
GO

-- ==========================
-- 5. acc_order_line
-- ==========================
IF OBJECT_ID('dbo.acc_order_line', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_order_line (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        order_id UNIQUEIDENTIFIER NOT NULL,
        product_id UNIQUEIDENTIFIER NULL,
        amazon_order_item_id NVARCHAR(50) NOT NULL,
        sku NVARCHAR(100) NULL,
        asin NVARCHAR(20) NULL,
        title NVARCHAR(500) NULL,
        quantity_ordered INT NOT NULL DEFAULT 1,
        quantity_shipped INT NOT NULL DEFAULT 0,
        item_price DECIMAL(10,2) NULL,
        item_tax DECIMAL(10,2) NULL,
        promotion_discount DECIMAL(10,2) NULL,
        currency NVARCHAR(5) NOT NULL DEFAULT 'EUR',
        cogs_pln DECIMAL(10,4) NULL,
        fba_fee_pln DECIMAL(10,4) NULL,
        referral_fee_pln DECIMAL(10,4) NULL,
        purchase_price_pln DECIMAL(12,4) NULL,
        price_source NVARCHAR(20) NULL,
        CONSTRAINT FK_acc_order_line_order FOREIGN KEY (order_id) REFERENCES dbo.acc_order(id),
        CONSTRAINT FK_acc_order_line_product FOREIGN KEY (product_id) REFERENCES dbo.acc_product(id)
    );
    CREATE INDEX IX_acc_order_line_order ON dbo.acc_order_line(order_id);
    CREATE INDEX IX_acc_order_line_product ON dbo.acc_order_line(product_id);
    CREATE INDEX IX_acc_order_line_sku ON dbo.acc_order_line(sku);
    CREATE INDEX IX_acc_order_line_asin ON dbo.acc_order_line(asin);
END
GO

-- ==========================
-- 6. acc_exchange_rate
-- ==========================
IF OBJECT_ID('dbo.acc_exchange_rate', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_exchange_rate (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        rate_date DATE NOT NULL,
        currency NVARCHAR(5) NOT NULL,
        rate_to_pln DECIMAL(10,6) NOT NULL,
        source NVARCHAR(50) NOT NULL DEFAULT 'NBP',
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_rate_date_currency UNIQUE (rate_date, currency)
    );
    CREATE INDEX IX_acc_exchange_rate_date ON dbo.acc_exchange_rate(rate_date);
    CREATE INDEX IX_acc_exchange_rate_currency ON dbo.acc_exchange_rate(currency);
END
GO

-- ==========================
-- 7. acc_purchase_price
-- ==========================
IF OBJECT_ID('dbo.acc_purchase_price', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_purchase_price (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        internal_sku NVARCHAR(20) NOT NULL,
        netto_price_pln DECIMAL(12,4) NOT NULL,
        valid_from DATE NOT NULL,
        valid_to DATE NULL,
        source NVARCHAR(20) NOT NULL,
        source_document NVARCHAR(200) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_purchase_price_sku ON dbo.acc_purchase_price(internal_sku);
END
GO

-- ==========================
-- 8. acc_finance_transaction
-- ==========================
IF OBJECT_ID('dbo.acc_finance_transaction', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_finance_transaction (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        marketplace_id NVARCHAR(20) NOT NULL,
        transaction_type NVARCHAR(100) NOT NULL,
        amazon_order_id NVARCHAR(50) NULL,
        shipment_id NVARCHAR(50) NULL,
        sku NVARCHAR(100) NULL,
        posted_date DATETIMEOFFSET NOT NULL,
        settlement_id NVARCHAR(50) NULL,
        amount DECIMAL(12,4) NOT NULL,
        currency NVARCHAR(5) NOT NULL,
        charge_type NVARCHAR(100) NULL,
        amount_pln DECIMAL(12,4) NULL,
        exchange_rate DECIMAL(10,6) NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_finance_tx_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id)
    );
    CREATE INDEX IX_acc_finance_tx_type ON dbo.acc_finance_transaction(transaction_type);
    CREATE INDEX IX_acc_finance_tx_order ON dbo.acc_finance_transaction(amazon_order_id);
    CREATE INDEX IX_acc_finance_tx_sku ON dbo.acc_finance_transaction(sku);
    CREATE INDEX IX_acc_finance_tx_posted ON dbo.acc_finance_transaction(posted_date);
    CREATE INDEX IX_acc_finance_tx_settlement ON dbo.acc_finance_transaction(settlement_id);
END
GO

-- ==========================
-- 9. acc_inventory_snapshot
-- ==========================
IF OBJECT_ID('dbo.acc_inventory_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inventory_snapshot (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        product_id UNIQUEIDENTIFIER NOT NULL,
        marketplace_id NVARCHAR(20) NOT NULL,
        snapshot_date DATE NOT NULL,
        sku NVARCHAR(100) NOT NULL,
        fnsku NVARCHAR(20) NULL,
        asin NVARCHAR(20) NULL,
        qty_fulfillable INT NOT NULL DEFAULT 0,
        qty_reserved INT NOT NULL DEFAULT 0,
        qty_inbound INT NOT NULL DEFAULT 0,
        qty_unfulfillable INT NOT NULL DEFAULT 0,
        avg_daily_sales_7d DECIMAL(10,4) NULL,
        doi DECIMAL(8,2) NULL,
        inventory_value_pln DECIMAL(12,2) NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_inventory_product FOREIGN KEY (product_id) REFERENCES dbo.acc_product(id),
        CONSTRAINT FK_acc_inventory_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id)
    );
    CREATE INDEX IX_acc_inventory_product ON dbo.acc_inventory_snapshot(product_id);
    CREATE INDEX IX_acc_inventory_marketplace ON dbo.acc_inventory_snapshot(marketplace_id);
    CREATE INDEX IX_acc_inventory_date ON dbo.acc_inventory_snapshot(snapshot_date);
    CREATE INDEX IX_acc_inventory_sku ON dbo.acc_inventory_snapshot(sku);
END
GO

-- ==========================
-- 10. acc_offer
-- ==========================
IF OBJECT_ID('dbo.acc_offer', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_offer (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        product_id UNIQUEIDENTIFIER NOT NULL,
        marketplace_id NVARCHAR(20) NOT NULL,
        sku NVARCHAR(100) NOT NULL,
        asin NVARCHAR(20) NULL,
        fnsku NVARCHAR(20) NULL,
        price DECIMAL(10,2) NULL,
        currency NVARCHAR(5) NOT NULL DEFAULT 'EUR',
        buybox_price DECIMAL(10,2) NULL,
        has_buybox BIT NOT NULL DEFAULT 0,
        is_featured_merchant BIT NOT NULL DEFAULT 0,
        fulfillment_channel NVARCHAR(20) NOT NULL DEFAULT 'FBA',
        status NVARCHAR(50) NOT NULL DEFAULT 'Active',
        bsr_rank INT NULL,
        bsr_category NVARCHAR(200) NULL,
        fba_fee DECIMAL(10,4) NULL,
        referral_fee_rate DECIMAL(5,4) NULL,
        last_synced_at DATETIME2 NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_offer_product FOREIGN KEY (product_id) REFERENCES dbo.acc_product(id),
        CONSTRAINT FK_acc_offer_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id)
    );
    CREATE INDEX IX_acc_offer_product ON dbo.acc_offer(product_id);
    CREATE INDEX IX_acc_offer_marketplace ON dbo.acc_offer(marketplace_id);
    CREATE INDEX IX_acc_offer_sku ON dbo.acc_offer(sku);
    CREATE INDEX IX_acc_offer_asin ON dbo.acc_offer(asin);
END
GO

-- ==========================
-- 11. acc_ads_campaign
-- ==========================
IF OBJECT_ID('dbo.acc_ads_campaign', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_ads_campaign (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        marketplace_id NVARCHAR(20) NOT NULL,
        campaign_id NVARCHAR(50) NOT NULL,
        campaign_name NVARCHAR(500) NOT NULL,
        campaign_type NVARCHAR(50) NOT NULL,
        targeting_type NVARCHAR(50) NULL,
        state NVARCHAR(20) NOT NULL,
        daily_budget DECIMAL(10,2) NULL,
        currency NVARCHAR(5) NOT NULL DEFAULT 'EUR',
        start_date DATE NULL,
        end_date DATE NULL,
        last_synced_at DATETIME2 NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_ads_campaign_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id)
    );
    CREATE INDEX IX_acc_ads_campaign_marketplace ON dbo.acc_ads_campaign(marketplace_id);
    CREATE INDEX IX_acc_ads_campaign_id ON dbo.acc_ads_campaign(campaign_id);
END
GO

-- ==========================
-- 12. acc_ads_campaign_day
-- ==========================
IF OBJECT_ID('dbo.acc_ads_campaign_day', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_ads_campaign_day (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        campaign_id UNIQUEIDENTIFIER NOT NULL,
        report_date DATE NOT NULL,
        impressions INT NOT NULL DEFAULT 0,
        clicks INT NOT NULL DEFAULT 0,
        spend DECIMAL(10,4) NOT NULL DEFAULT 0,
        sales_7d DECIMAL(12,4) NOT NULL DEFAULT 0,
        orders_7d INT NOT NULL DEFAULT 0,
        units_7d INT NOT NULL DEFAULT 0,
        acos DECIMAL(8,4) NULL,
        roas DECIMAL(8,4) NULL,
        spend_pln DECIMAL(10,4) NULL,
        sales_pln DECIMAL(12,4) NULL,
        CONSTRAINT FK_acc_ads_day_campaign FOREIGN KEY (campaign_id) REFERENCES dbo.acc_ads_campaign(id)
    );
    CREATE INDEX IX_acc_ads_day_campaign ON dbo.acc_ads_campaign_day(campaign_id);
    CREATE INDEX IX_acc_ads_day_date ON dbo.acc_ads_campaign_day(report_date);
END
GO

-- ==========================
-- 13. acc_alert_rule
-- ==========================
IF OBJECT_ID('dbo.acc_alert_rule', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_alert_rule (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        description NVARCHAR(MAX) NULL,
        rule_type NVARCHAR(100) NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        sku NVARCHAR(100) NULL,
        category NVARCHAR(200) NULL,
        threshold_value DECIMAL(12,4) NULL,
        threshold_operator NVARCHAR(10) NULL,
        severity NVARCHAR(20) NOT NULL DEFAULT 'warning',
        is_active BIT NOT NULL DEFAULT 1,
        created_by UNIQUEIDENTIFIER NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_alert_rule_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id),
        CONSTRAINT FK_acc_alert_rule_user FOREIGN KEY (created_by) REFERENCES dbo.acc_user(id)
    );
END
GO

-- ==========================
-- 14. acc_alert
-- ==========================
IF OBJECT_ID('dbo.acc_alert', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_alert (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        rule_id UNIQUEIDENTIFIER NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        sku NVARCHAR(100) NULL,
        title NVARCHAR(500) NOT NULL,
        detail NVARCHAR(MAX) NULL,
        severity NVARCHAR(20) NOT NULL DEFAULT 'warning',
        current_value DECIMAL(12,4) NULL,
        is_read BIT NOT NULL DEFAULT 0,
        is_resolved BIT NOT NULL DEFAULT 0,
        resolved_at DATETIME2 NULL,
        resolved_by UNIQUEIDENTIFIER NULL,
        triggered_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_alert_rule FOREIGN KEY (rule_id) REFERENCES dbo.acc_alert_rule(id),
        CONSTRAINT FK_acc_alert_resolved_by FOREIGN KEY (resolved_by) REFERENCES dbo.acc_user(id)
    );
    CREATE INDEX IX_acc_alert_rule ON dbo.acc_alert(rule_id);
    CREATE INDEX IX_acc_alert_triggered ON dbo.acc_alert(triggered_at);
    CREATE INDEX IX_acc_alert_state ON dbo.acc_alert(is_resolved, severity);
END
GO

-- ==========================
-- 15. acc_plan_month
-- ==========================
IF OBJECT_ID('dbo.acc_plan_month', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_plan_month (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        marketplace_id NVARCHAR(20) NOT NULL,
        [year] INT NOT NULL,
        [month] INT NOT NULL,
        label NVARCHAR(200) NULL,
        target_revenue_pln DECIMAL(14,2) NULL,
        target_orders INT NULL,
        target_acos DECIMAL(8,4) NULL,
        target_cm_percent DECIMAL(8,4) NULL,
        budget_ads_pln DECIMAL(12,2) NULL,
        notes NVARCHAR(MAX) NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'draft',
        created_by UNIQUEIDENTIFIER NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_plan_month_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id),
        CONSTRAINT FK_acc_plan_month_user FOREIGN KEY (created_by) REFERENCES dbo.acc_user(id),
        CONSTRAINT UQ_acc_plan_month_mkt UNIQUE ([year], [month], marketplace_id)
    );
END
GO

-- ==========================
-- 16. acc_plan_line
-- ==========================
IF OBJECT_ID('dbo.acc_plan_line', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_plan_line (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        plan_month_id UNIQUEIDENTIFIER NOT NULL,
        product_id UNIQUEIDENTIFIER NULL,
        sku NVARCHAR(100) NULL,
        category NVARCHAR(200) NULL,
        target_units INT NULL,
        target_revenue_pln DECIMAL(12,2) NULL,
        target_price DECIMAL(10,2) NULL,
        target_ads_spend_pln DECIMAL(10,2) NULL,
        notes NVARCHAR(MAX) NULL,
        CONSTRAINT FK_acc_plan_line_month FOREIGN KEY (plan_month_id) REFERENCES dbo.acc_plan_month(id),
        CONSTRAINT FK_acc_plan_line_product FOREIGN KEY (product_id) REFERENCES dbo.acc_product(id)
    );
    CREATE INDEX IX_acc_plan_line_month ON dbo.acc_plan_line(plan_month_id);
    CREATE INDEX IX_acc_plan_line_product ON dbo.acc_plan_line(product_id);
    CREATE INDEX IX_acc_plan_line_sku ON dbo.acc_plan_line(sku);
END
GO

-- ==========================
-- 17. acc_job_run
-- ==========================
IF OBJECT_ID('dbo.acc_job_run', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_job_run (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        celery_task_id NVARCHAR(100) NULL,
        job_type NVARCHAR(100) NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        triggered_by UNIQUEIDENTIFIER NULL,
        trigger_source NVARCHAR(50) NOT NULL DEFAULT 'manual',
        status NVARCHAR(30) NOT NULL DEFAULT 'pending',
        progress_pct INT NOT NULL DEFAULT 0,
        progress_message NVARCHAR(500) NULL,
        records_processed INT NULL,
        error_message NVARCHAR(MAX) NULL,
        result_summary NVARCHAR(MAX) NULL,
        started_at DATETIME2 NULL,
        finished_at DATETIME2 NULL,
        duration_seconds DECIMAL(10,2) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_job_run_user FOREIGN KEY (triggered_by) REFERENCES dbo.acc_user(id)
    );
    CREATE UNIQUE INDEX IX_acc_job_celery ON dbo.acc_job_run(celery_task_id) WHERE celery_task_id IS NOT NULL;
    CREATE INDEX IX_acc_job_type ON dbo.acc_job_run(job_type);
    CREATE INDEX IX_acc_job_status ON dbo.acc_job_run(status);
    CREATE INDEX IX_acc_job_created ON dbo.acc_job_run(created_at);
END
GO

-- ==========================
-- 18. acc_ai_recommendation
-- ==========================
IF OBJECT_ID('dbo.acc_ai_recommendation', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_ai_recommendation (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        marketplace_id NVARCHAR(20) NULL,
        product_id UNIQUEIDENTIFIER NULL,
        sku NVARCHAR(100) NULL,
        recommendation_type NVARCHAR(100) NOT NULL,
        title NVARCHAR(500) NOT NULL,
        summary NVARCHAR(MAX) NOT NULL,
        action_items NVARCHAR(MAX) NULL,
        confidence_score DECIMAL(5,4) NULL,
        model_used NVARCHAR(100) NOT NULL DEFAULT 'gpt-4o',
        prompt_tokens INT NULL,
        completion_tokens INT NULL,
        status NVARCHAR(30) NOT NULL DEFAULT 'new',
        user_feedback NVARCHAR(MAX) NULL,
        acted_by UNIQUEIDENTIFIER NULL,
        acted_at DATETIME2 NULL,
        generated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_acc_ai_marketplace FOREIGN KEY (marketplace_id) REFERENCES dbo.acc_marketplace(id),
        CONSTRAINT FK_acc_ai_product FOREIGN KEY (product_id) REFERENCES dbo.acc_product(id),
        CONSTRAINT FK_acc_ai_user FOREIGN KEY (acted_by) REFERENCES dbo.acc_user(id)
    );
    CREATE INDEX IX_acc_ai_marketplace ON dbo.acc_ai_recommendation(marketplace_id);
    CREATE INDEX IX_acc_ai_product ON dbo.acc_ai_recommendation(product_id);
    CREATE INDEX IX_acc_ai_type ON dbo.acc_ai_recommendation(recommendation_type);
    CREATE INDEX IX_acc_ai_generated ON dbo.acc_ai_recommendation(generated_at);
END
GO

-- ====================================================================
-- Helper tables (acc_al_* used by mssql_store.py ensure_v2_schema)
-- ====================================================================

-- ==========================
-- 19. acc_al_alert_rules
-- ==========================
IF OBJECT_ID('dbo.acc_al_alert_rules', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_alert_rules (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        description NVARCHAR(500) NULL,
        rule_type NVARCHAR(80) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        sku NVARCHAR(120) NULL,
        category NVARCHAR(120) NULL,
        threshold_value DECIMAL(18,4) NULL,
        threshold_operator NVARCHAR(8) NULL,
        severity NVARCHAR(20) NOT NULL DEFAULT 'warning',
        is_active BIT NOT NULL DEFAULT 1,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_alert_rules_type ON dbo.acc_al_alert_rules(rule_type, is_active);
END
GO

-- ==========================
-- 20. acc_al_alerts
-- ==========================
IF OBJECT_ID('dbo.acc_al_alerts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_alerts (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        rule_id UNIQUEIDENTIFIER NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        sku NVARCHAR(120) NULL,
        title NVARCHAR(300) NOT NULL,
        detail NVARCHAR(MAX) NULL,
        severity NVARCHAR(20) NOT NULL,
        current_value DECIMAL(18,4) NULL,
        is_read BIT NOT NULL DEFAULT 0,
        is_resolved BIT NOT NULL DEFAULT 0,
        triggered_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        resolved_at DATETIME2 NULL,
        resolved_by NVARCHAR(120) NULL
    );
    CREATE INDEX IX_acc_al_alerts_state ON dbo.acc_al_alerts(is_resolved, severity, triggered_at);
    CREATE INDEX IX_acc_al_alerts_rule ON dbo.acc_al_alerts(rule_id, is_resolved);
END
GO

-- ==========================
-- 21. acc_al_jobs
-- ==========================
IF OBJECT_ID('dbo.acc_al_jobs', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_jobs (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        celery_task_id NVARCHAR(80) NULL,
        job_type NVARCHAR(80) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        trigger_source NVARCHAR(20) NOT NULL DEFAULT 'manual',
        triggered_by NVARCHAR(120) NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        progress_pct INT NOT NULL DEFAULT 0,
        progress_message NVARCHAR(MAX) NULL,
        records_processed INT NULL,
        error_message NVARCHAR(MAX) NULL,
        started_at DATETIME2 NULL,
        finished_at DATETIME2 NULL,
        duration_seconds FLOAT NULL,
        params_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_jobs_main ON dbo.acc_al_jobs(job_type, status, created_at);
END
GO

-- ==========================
-- 22. acc_al_plans
-- ==========================
IF OBJECT_ID('dbo.acc_al_plans', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_plans (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [year] INT NOT NULL,
        [month] INT NOT NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'draft',
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_al_plan_month UNIQUE([year], [month])
    );
END
GO

-- ==========================
-- 23. acc_al_plan_lines
-- ==========================
IF OBJECT_ID('dbo.acc_al_plan_lines', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_plan_lines (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        plan_id INT NOT NULL,
        marketplace_id NVARCHAR(160) NOT NULL,
        target_revenue_pln DECIMAL(18,2) NOT NULL DEFAULT 0,
        target_orders INT NOT NULL DEFAULT 0,
        target_acos_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
        target_cm_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
        budget_ads_pln DECIMAL(18,2) NOT NULL DEFAULT 0,
        actual_revenue_pln DECIMAL(18,2) NULL,
        actual_orders INT NULL,
        actual_acos_pct DECIMAL(9,2) NULL,
        actual_cm_pct DECIMAL(9,2) NULL,
        CONSTRAINT FK_acc_al_plan_lines_plan FOREIGN KEY (plan_id) REFERENCES dbo.acc_al_plans(id)
    );
    CREATE INDEX IX_acc_al_plan_lines_plan ON dbo.acc_al_plan_lines(plan_id, marketplace_id);
END
GO

-- ==========================
-- 24. acc_al_profit_snapshot
-- ==========================
IF OBJECT_ID('dbo.acc_al_profit_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_profit_snapshot (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        sales_date DATE NOT NULL,
        order_number NVARCHAR(180) NULL,
        sku NVARCHAR(120) NULL,
        title NVARCHAR(300) NULL,
        quantity FLOAT NOT NULL DEFAULT 0,
        revenue_net DECIMAL(18,2) NOT NULL DEFAULT 0,
        revenue_gross DECIMAL(18,2) NOT NULL DEFAULT 0,
        cogs DECIMAL(18,2) NOT NULL DEFAULT 0,
        transport DECIMAL(18,2) NOT NULL DEFAULT 0,
        channel NVARCHAR(180) NULL,
        source_table NVARCHAR(180) NOT NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_profit_snapshot_date ON dbo.acc_al_profit_snapshot(sales_date, channel);
END
GO

-- ==========================
-- 25. acc_audit_log
-- ==========================
IF OBJECT_ID('dbo.acc_audit_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_audit_log (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        audit_date DATE NOT NULL,
        overall_status NVARCHAR(20) NOT NULL,
        cogs_coverage_pct DECIMAL(9,2) NULL,
        mapping_coverage_pct DECIMAL(9,2) NULL,
        total_issues INT NOT NULL DEFAULT 0,
        loss_lines INT NULL,
        avg_cogs_pct DECIMAL(9,2) NULL,
        issues_json NVARCHAR(MAX) NULL,
        checks_json NVARCHAR(MAX) NULL,
        trigger_source NVARCHAR(20) NOT NULL DEFAULT 'scheduler',
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX IX_acc_audit_log_date ON dbo.acc_audit_log(audit_date, trigger_source);
END
GO

-- ====================================================================
-- Family Mapper tables (9 tables)
-- ====================================================================

-- ==========================
-- 26. global_family
-- ==========================
IF OBJECT_ID('dbo.global_family', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.global_family (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        de_parent_asin NVARCHAR(20) NOT NULL,
        brand NVARCHAR(120) NULL,
        category NVARCHAR(200) NULL,
        product_type NVARCHAR(120) NULL,
        variation_theme_de NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_global_family_de_parent UNIQUE (de_parent_asin)
    );
    CREATE INDEX IX_global_family_de_parent ON dbo.global_family(de_parent_asin);
END
GO

-- ==========================
-- 27. global_family_child
-- ==========================
IF OBJECT_ID('dbo.global_family_child', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.global_family_child (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        global_family_id INT NOT NULL,
        master_key NVARCHAR(120) NOT NULL,
        key_type NVARCHAR(20) NOT NULL,
        de_child_asin NVARCHAR(20) NOT NULL,
        sku_de NVARCHAR(80) NULL,
        ean_de NVARCHAR(20) NULL,
        attributes_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_gfc_family FOREIGN KEY (global_family_id) REFERENCES dbo.global_family(id),
        CONSTRAINT UX_gfc_family_master UNIQUE (global_family_id, master_key)
    );
    CREATE INDEX IX_gfc_master_key ON dbo.global_family_child(master_key);
    CREATE INDEX IX_gfc_de_child_asin ON dbo.global_family_child(de_child_asin);
END
GO

-- ==========================
-- 28. marketplace_listing_child
-- ==========================
IF OBJECT_ID('dbo.marketplace_listing_child', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.marketplace_listing_child (
        marketplace NVARCHAR(10) NOT NULL,
        asin NVARCHAR(20) NOT NULL,
        sku NVARCHAR(80) NULL,
        ean NVARCHAR(20) NULL,
        current_parent_asin NVARCHAR(20) NULL,
        variation_theme NVARCHAR(120) NULL,
        attributes_json NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_marketplace_listing_child PRIMARY KEY (marketplace, asin)
    );
    CREATE INDEX IX_mlc_mp_sku ON dbo.marketplace_listing_child(marketplace, sku);
    CREATE INDEX IX_mlc_mp_ean ON dbo.marketplace_listing_child(marketplace, ean);
    CREATE INDEX IX_mlc_mp_parent ON dbo.marketplace_listing_child(marketplace, current_parent_asin);
END
GO

-- ==========================
-- 29. global_family_child_market_link
-- ==========================
IF OBJECT_ID('dbo.global_family_child_market_link', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.global_family_child_market_link (
        global_family_id INT NOT NULL,
        master_key NVARCHAR(120) NOT NULL,
        marketplace NVARCHAR(10) NOT NULL,
        target_child_asin NVARCHAR(20) NULL,
        current_parent_asin NVARCHAR(20) NULL,
        match_type NVARCHAR(20) NOT NULL,
        confidence INT NOT NULL DEFAULT 0,
        status NVARCHAR(20) NOT NULL DEFAULT 'proposed',
        reason_json NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_gfcml PRIMARY KEY (global_family_id, master_key, marketplace)
    );
    CREATE INDEX IX_gfcl_mp_target_child ON dbo.global_family_child_market_link(marketplace, target_child_asin);
    CREATE INDEX IX_gfcl_mp_current_parent ON dbo.global_family_child_market_link(marketplace, current_parent_asin);
END
GO

-- ==========================
-- 30. global_family_market_link
-- ==========================
IF OBJECT_ID('dbo.global_family_market_link', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.global_family_market_link (
        global_family_id INT NOT NULL,
        marketplace NVARCHAR(10) NOT NULL,
        target_parent_asin NVARCHAR(20) NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'unmapped',
        confidence_avg INT NOT NULL DEFAULT 0,
        notes NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_gfml PRIMARY KEY (global_family_id, marketplace),
        CONSTRAINT FK_gfml_family FOREIGN KEY (global_family_id) REFERENCES dbo.global_family(id)
    );
END
GO

-- ==========================
-- 31. family_coverage_cache
-- ==========================
IF OBJECT_ID('dbo.family_coverage_cache', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.family_coverage_cache (
        global_family_id INT NOT NULL,
        marketplace NVARCHAR(10) NOT NULL,
        de_children_count INT NOT NULL,
        matched_children_count INT NOT NULL,
        coverage_pct INT NOT NULL,
        missing_children_count INT NOT NULL,
        extra_children_count INT NOT NULL,
        theme_mismatch BIT NOT NULL DEFAULT 0,
        confidence_avg INT NOT NULL DEFAULT 0,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_family_coverage PRIMARY KEY (global_family_id, marketplace)
    );
END
GO

-- ==========================
-- 32. family_issues_cache
-- ==========================
IF OBJECT_ID('dbo.family_issues_cache', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.family_issues_cache (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        global_family_id INT NOT NULL,
        marketplace NVARCHAR(10) NULL,
        issue_type NVARCHAR(40) NOT NULL,
        severity NVARCHAR(10) NOT NULL,
        payload_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_fic_family ON dbo.family_issues_cache(global_family_id, marketplace);
    CREATE INDEX IX_fic_severity ON dbo.family_issues_cache(severity, issue_type);
END
GO

-- ==========================
-- 33. family_fix_package
-- ==========================
IF OBJECT_ID('dbo.family_fix_package', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.family_fix_package (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        marketplace NVARCHAR(10) NOT NULL,
        global_family_id INT NOT NULL,
        action_plan_json NVARCHAR(MAX) NOT NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'draft',
        generated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        approved_by NVARCHAR(120) NULL,
        approved_at DATETIME2 NULL,
        applied_at DATETIME2 NULL
    );
    CREATE INDEX IX_ffp_mp_status ON dbo.family_fix_package(marketplace, status);
    CREATE INDEX IX_ffp_family ON dbo.family_fix_package(global_family_id);
END
GO

-- ==========================
-- 34. family_fix_job
-- ==========================
IF OBJECT_ID('dbo.family_fix_job', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.family_fix_job (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        job_type NVARCHAR(40) NOT NULL DEFAULT 'unknown',
        marketplace NVARCHAR(10) NOT NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        progress INT NOT NULL DEFAULT 0,
        started_at DATETIME2 NULL,
        finished_at DATETIME2 NULL,
        log NVARCHAR(MAX) NULL
    );
    CREATE INDEX IX_ffj_status ON dbo.family_fix_job(status, marketplace);
END
GO

-- ====================================================================
-- Seed marketplace data (13 EU marketplaces)
-- ====================================================================
MERGE dbo.acc_marketplace AS tgt
USING (VALUES
    ('A1PA6795UKMFR9', 'DE', 'Amazon.de',     'EUR', 'Europe/Berlin'),
    ('A1C3SOZRARQ6R3', 'PL', 'Amazon.pl',     'PLN', 'Europe/Warsaw'),
    ('A1F83G8C2ARO7P', 'GB', 'Amazon.co.uk',  'GBP', 'Europe/London'),
    ('A1RKKUPIHCS9HS', 'ES', 'Amazon.es',     'EUR', 'Europe/Madrid'),
    ('A13V1IB3VIYZZH', 'FR', 'Amazon.fr',     'EUR', 'Europe/Paris'),
    ('A1805IZSGTT6HS', 'NL', 'Amazon.nl',     'EUR', 'Europe/Amsterdam'),
    ('APJ6JRA9NG5V4',  'IT', 'Amazon.it',     'EUR', 'Europe/Rome'),
    ('A2NODRKZP88ZB9', 'SE', 'Amazon.se',     'SEK', 'Europe/Stockholm'),
    ('AMEN7PMS3EDWL',  'BE', 'Amazon.be',     'EUR', 'Europe/Brussels'),
    ('A28R8C7NBKEWEA', 'IE', 'Amazon.ie',     'EUR', 'Europe/Dublin'),
    ('A2VIGQ35RCS4UG', 'AE', 'Amazon.ae',     'AED', 'Asia/Dubai'),
    ('A17E79C6D8DWNP', 'SA', 'Amazon.sa',     'SAR', 'Asia/Riyadh'),
    ('A33AVAJ2PDY3EV', 'TR', 'Amazon.tr',     'TRY', 'Europe/Istanbul')
) AS src (id, code, name, currency, timezone)
ON tgt.id = src.id
WHEN NOT MATCHED THEN
    INSERT (id, code, name, currency, timezone, is_active)
    VALUES (src.id, src.code, src.name, src.currency, src.timezone, 1);
GO

PRINT '=== All 34 tables + marketplace seed created successfully ==='
GO
