-- ============================================================
-- Return Tracker Module — Database Schema
-- Amazon Command Center (ACC)
-- Created: 2026-03-05
-- ============================================================

-- 1. Main return tracking table — one row per returned order line
--    Links refund event (already in acc_order) with physical return status
CREATE TABLE dbo.acc_return_item (
    id                      BIGINT IDENTITY(1,1) PRIMARY KEY,
    amazon_order_id         NVARCHAR(50)    NOT NULL,
    order_line_id           BIGINT          NULL,       -- FK to acc_order_line.id
    order_id                BIGINT          NULL,       -- FK to acc_order.id
    marketplace_id          NVARCHAR(160)   NOT NULL,
    sku                     NVARCHAR(200)   NOT NULL,
    asin                    NVARCHAR(20)    NULL,
    fnsku                   NVARCHAR(20)    NULL,

    -- Refund info (from acc_order)
    refund_date             DATETIME2       NULL,
    refund_type             VARCHAR(20)     NULL,       -- full / partial
    refund_amount_pln       DECIMAL(18,4)   NULL,       -- negative = money back to customer

    -- Physical return tracking (from FBA Customer Returns report)
    return_date             DATETIME2       NULL,       -- when Amazon received item back
    return_reason           NVARCHAR(500)   NULL,       -- Amazon return reason code
    return_reason_detail    NVARCHAR(1000)  NULL,       -- customer's reason text
    disposition             VARCHAR(30)     NULL,       -- SELLABLE / DAMAGED / DEFECTIVE / CUSTOMER_DAMAGED / CARRIER_DAMAGED / EXPIRED
    quantity                INT             NOT NULL DEFAULT 1,

    -- Financial classification
    --   sellable_return  = item returned to inventory, COGS recovered (WZ reversal)
    --   damaged_return   = item returned but unsellable, COGS = write-off loss
    --   pending          = refund issued but no physical return yet
    --   lost_in_transit  = never arrived back
    --   reimbursed       = Amazon reimbursed us (no physical return)
    financial_status        VARCHAR(30)     NOT NULL DEFAULT 'pending',

    -- COGS impact
    cogs_pln                DECIMAL(18,4)   NULL,       -- COGS of this product (from order line)
    cogs_recovered_pln      DECIMAL(18,4)   NULL,       -- if sellable: = cogs_pln, else 0
    write_off_pln           DECIMAL(18,4)   NULL,       -- if damaged: = cogs_pln, else 0

    -- Manual override (for warehouse confirmation)
    manual_status           VARCHAR(30)     NULL,       -- manual override of financial_status
    manual_note             NVARCHAR(1000)  NULL,
    manual_updated_by       NVARCHAR(100)   NULL,
    manual_updated_at       DATETIME2       NULL,

    -- Metadata
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    source                  VARCHAR(30)     NOT NULL DEFAULT 'auto',  -- auto / manual / fba_report

    -- Composite uniqueness: one return item per order line
    CONSTRAINT UQ_acc_return_item_order_line UNIQUE (amazon_order_id, sku, quantity, refund_date)
);

-- Indexes for common queries
CREATE INDEX IX_acc_return_item_marketplace
    ON dbo.acc_return_item (marketplace_id, return_date)
    INCLUDE (financial_status, disposition);

CREATE INDEX IX_acc_return_item_sku
    ON dbo.acc_return_item (sku)
    INCLUDE (marketplace_id, financial_status, cogs_pln, cogs_recovered_pln);

CREATE INDEX IX_acc_return_item_status
    ON dbo.acc_return_item (financial_status)
    INCLUDE (marketplace_id, cogs_pln, cogs_recovered_pln, write_off_pln);

CREATE INDEX IX_acc_return_item_order
    ON dbo.acc_return_item (amazon_order_id);


-- 2. FBA Customer Returns raw data (from GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA report)
--    Keeps raw report rows for audit trail
CREATE TABLE dbo.acc_fba_customer_return (
    id                      BIGINT IDENTITY(1,1) PRIMARY KEY,
    return_date             DATETIME2       NOT NULL,
    order_id                NVARCHAR(50)    NOT NULL,   -- Amazon order ID
    sku                     NVARCHAR(200)   NOT NULL,
    asin                    NVARCHAR(20)    NULL,
    fnsku                   NVARCHAR(20)    NULL,
    product_name            NVARCHAR(1000)  NULL,
    quantity                INT             NOT NULL DEFAULT 1,
    fulfillment_center_id   NVARCHAR(20)    NULL,       -- e.g. FRA3, WRO1
    detailed_disposition    VARCHAR(50)     NULL,        -- SELLABLE / DAMAGED / DEFECTIVE etc.
    reason                  NVARCHAR(500)   NULL,
    status                  NVARCHAR(100)   NULL,        -- Unit returned to inventory / etc.
    license_plate_number    NVARCHAR(100)   NULL,
    customer_comments       NVARCHAR(2000)  NULL,
    marketplace_id          NVARCHAR(160)   NULL,        -- added by sync
    currency                VARCHAR(5)      NULL,

    -- Sync metadata
    report_id               NVARCHAR(100)   NULL,
    synced_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT UQ_acc_fba_return_row UNIQUE (order_id, sku, return_date, quantity, fulfillment_center_id)
);

CREATE INDEX IX_acc_fba_customer_return_order
    ON dbo.acc_fba_customer_return (order_id);

CREATE INDEX IX_acc_fba_customer_return_date
    ON dbo.acc_fba_customer_return (return_date, marketplace_id);


-- 3. Return sync state (watermark per marketplace)
CREATE TABLE dbo.acc_return_sync_state (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    marketplace_id          NVARCHAR(160)   NOT NULL UNIQUE,
    last_synced_to          DATETIME2       NOT NULL,
    last_sync_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    rows_synced             INT             NOT NULL DEFAULT 0,
    status                  VARCHAR(20)     NOT NULL DEFAULT 'ok', -- ok / error
    error_message           NVARCHAR(1000)  NULL
);


-- 4. Daily aggregated return metrics (for dashboard KPIs)
CREATE TABLE dbo.acc_return_daily_summary (
    id                      BIGINT IDENTITY(1,1) PRIMARY KEY,
    report_date             DATE            NOT NULL,
    marketplace_id          NVARCHAR(160)   NOT NULL,

    -- Counts
    refund_orders           INT             NOT NULL DEFAULT 0,
    refund_units            INT             NOT NULL DEFAULT 0,
    return_received_units   INT             NOT NULL DEFAULT 0,
    sellable_units          INT             NOT NULL DEFAULT 0,
    damaged_units           INT             NOT NULL DEFAULT 0,
    pending_units           INT             NOT NULL DEFAULT 0,
    reimbursed_units        INT             NOT NULL DEFAULT 0,

    -- Financial
    refund_amount_pln       DECIMAL(18,4)   NOT NULL DEFAULT 0,
    cogs_total_pln          DECIMAL(18,4)   NOT NULL DEFAULT 0,
    cogs_recovered_pln      DECIMAL(18,4)   NOT NULL DEFAULT 0,     -- sellable returns
    cogs_write_off_pln      DECIMAL(18,4)   NOT NULL DEFAULT 0,     -- damaged/defective
    cogs_pending_pln        DECIMAL(18,4)   NOT NULL DEFAULT 0,     -- awaiting return

    -- Rates
    return_rate_pct         DECIMAL(8,2)    NULL,       -- returns / total orders (from that day's orders)
    sellable_rate_pct       DECIMAL(8,2)    NULL,       -- sellable / returned
    
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT UQ_acc_return_daily UNIQUE (report_date, marketplace_id)
);

CREATE INDEX IX_acc_return_daily_date
    ON dbo.acc_return_daily_summary (report_date, marketplace_id);
