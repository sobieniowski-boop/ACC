"""DHL logistics schema: shipments, billing, cost estimation, relations.

Converts ensure_dhl_schema() DDL into a proper Alembic migration.
Idempotent: all CREATE TABLE / INDEX / ALTER TABLE wrapped in IF checks.

Revision ID: eb011
Revises: eb010
Create Date: 2026-03-13
"""
from alembic import op

revision = "eb011"
down_revision = "eb010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Core shipment tables ──────────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_shipment', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_shipment (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            carrier NVARCHAR(16) NOT NULL DEFAULT 'DHL',
            carrier_account NVARCHAR(64) NULL,
            shipment_number NVARCHAR(64) NULL,
            piece_id NVARCHAR(64) NULL,
            tracking_number NVARCHAR(120) NULL,
            cedex_number NVARCHAR(64) NULL,
            service_code NVARCHAR(64) NULL,
            ship_date DATE NULL,
            created_at_carrier DATETIME2 NULL,
            status_code NVARCHAR(64) NULL,
            status_label NVARCHAR(255) NULL,
            received_by NVARCHAR(255) NULL,
            is_delivered BIT NOT NULL DEFAULT 0,
            delivered_at DATETIME2 NULL,
            recipient_name NVARCHAR(255) NULL,
            recipient_country NVARCHAR(8) NULL,
            shipper_name NVARCHAR(255) NULL,
            shipper_country NVARCHAR(8) NULL,
            source_system NVARCHAR(32) NOT NULL DEFAULT 'dhl_webapi2',
            source_payload_json NVARCHAR(MAX) NULL,
            source_payload_hash NVARCHAR(64) NULL,
            first_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            last_seen_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            last_sync_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_shipment_order_link', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_shipment_order_link (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            shipment_id UNIQUEIDENTIFIER NOT NULL,
            amazon_order_id NVARCHAR(80) NULL,
            acc_order_id UNIQUEIDENTIFIER NULL,
            bl_order_id INT NULL,
            link_method NVARCHAR(64) NOT NULL DEFAULT 'unknown',
            link_confidence DECIMAL(9,4) NOT NULL DEFAULT 0,
            is_primary BIT NOT NULL DEFAULT 0,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_shipment_event', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_shipment_event (
            id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            shipment_id UNIQUEIDENTIFIER NOT NULL,
            event_code NVARCHAR(64) NULL,
            event_label NVARCHAR(500) NULL,
            event_terminal NVARCHAR(255) NULL,
            event_at DATETIME2 NULL,
            location_city NVARCHAR(255) NULL,
            location_country NVARCHAR(8) NULL,
            raw_payload_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_shipment_pod', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_shipment_pod (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            shipment_id UNIQUEIDENTIFIER NOT NULL,
            pod_type NVARCHAR(32) NOT NULL DEFAULT 'epod',
            available BIT NOT NULL DEFAULT 0,
            mime_type NVARCHAR(128) NULL,
            document_base64 NVARCHAR(MAX) NULL,
            document_ref NVARCHAR(255) NULL,
            raw_payload_json NVARCHAR(MAX) NULL,
            downloaded_at DATETIME2 NULL,
            last_sync_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_shipment_cost', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_shipment_cost (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            shipment_id UNIQUEIDENTIFIER NOT NULL,
            cost_source NVARCHAR(64) NOT NULL,
            currency NVARCHAR(8) NOT NULL DEFAULT 'PLN',
            net_amount DECIMAL(18,4) NULL,
            fuel_amount DECIMAL(18,4) NULL,
            toll_amount DECIMAL(18,4) NULL,
            gross_amount DECIMAL(18,4) NULL,
            invoice_number NVARCHAR(64) NULL,
            invoice_date DATE NULL,
            billing_period NVARCHAR(32) NULL,
            is_estimated BIT NOT NULL DEFAULT 0,
            raw_payload_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    # ── DHL billing tables ────────────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_dhl_import_file', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_dhl_import_file (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            source_kind NVARCHAR(32) NOT NULL,
            file_path NVARCHAR(500) NOT NULL,
            file_name NVARCHAR(260) NOT NULL,
            document_number NVARCHAR(64) NULL,
            file_size_bytes BIGINT NULL,
            file_mtime_utc DATETIME2 NULL,
            status NVARCHAR(32) NOT NULL DEFAULT 'pending',
            rows_imported INT NOT NULL DEFAULT 0,
            error_message NVARCHAR(MAX) NULL,
            last_imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_dhl_billing_document', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_dhl_billing_document (
            document_number NVARCHAR(64) NOT NULL PRIMARY KEY,
            document_type NVARCHAR(64) NULL,
            issue_date DATE NULL,
            ship_date DATE NULL,
            due_date DATE NULL,
            net_amount DECIMAL(18,4) NULL,
            vat_amount DECIMAL(18,4) NULL,
            gross_amount DECIMAL(18,4) NULL,
            currency NVARCHAR(8) NOT NULL DEFAULT 'PLN',
            source_file NVARCHAR(500) NULL,
            source_manifest_file NVARCHAR(500) NULL,
            detail_rows_count INT NOT NULL DEFAULT 0,
            last_imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_dhl_billing_line', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_dhl_billing_line (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            document_number NVARCHAR(64) NOT NULL,
            issue_date DATE NULL,
            sales_date DATE NULL,
            due_date DATE NULL,
            parcel_number NVARCHAR(64) NOT NULL,
            parcel_number_base NVARCHAR(64) NOT NULL,
            parcel_number_suffix NVARCHAR(16) NULL,
            delivery_date DATE NULL,
            quantity DECIMAL(18,4) NULL,
            product_code NVARCHAR(64) NULL,
            description NVARCHAR(255) NULL,
            weight DECIMAL(18,4) NULL,
            weight_kind NVARCHAR(16) NULL,
            shipper_receiver NVARCHAR(255) NULL,
            payer_type NVARCHAR(16) NULL,
            sap_order NVARCHAR(64) NULL,
            mpk NVARCHAR(64) NULL,
            pkwiu NVARCHAR(64) NULL,
            notes NVARCHAR(64) NULL,
            net_amount DECIMAL(18,4) NULL,
            base_fee DECIMAL(18,4) NULL,
            base_discount DECIMAL(18,4) NULL,
            non_standard_fee DECIMAL(18,4) NULL,
            seasonal_fee DECIMAL(18,4) NULL,
            fuel_road_fee DECIMAL(18,4) NULL,
            insurance_fee DECIMAL(18,4) NULL,
            cod_fee DECIMAL(18,4) NULL,
            label_fee DECIMAL(18,4) NULL,
            volumetric_fee DECIMAL(18,4) NULL,
            source_file NVARCHAR(500) NOT NULL,
            source_row_no INT NOT NULL,
            source_hash NVARCHAR(64) NULL,
            imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_dhl_parcel_map', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_dhl_parcel_map (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            parcel_number NVARCHAR(64) NOT NULL,
            parcel_number_base NVARCHAR(64) NOT NULL,
            parcel_number_suffix NVARCHAR(16) NULL,
            jjd_number NVARCHAR(64) NOT NULL,
            shipment_type NVARCHAR(16) NULL,
            ship_date DATETIME2 NULL,
            delivery_date DATETIME2 NULL,
            last_event_code NVARCHAR(64) NULL,
            last_event_at DATETIME2 NULL,
            source_file NVARCHAR(500) NOT NULL,
            source_row_no INT NOT NULL,
            source_hash NVARCHAR(64) NULL,
            imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    # ── Logistics fact / shadow tables ────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_order_logistics_fact', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_order_logistics_fact (
            amazon_order_id NVARCHAR(80) NOT NULL PRIMARY KEY,
            acc_order_id UNIQUEIDENTIFIER NULL,
            shipments_count INT NOT NULL DEFAULT 0,
            delivered_shipments_count INT NOT NULL DEFAULT 0,
            actual_shipments_count INT NOT NULL DEFAULT 0,
            estimated_shipments_count INT NOT NULL DEFAULT 0,
            total_logistics_pln DECIMAL(18,4) NOT NULL DEFAULT 0,
            last_delivery_at DATETIME2 NULL,
            calc_version NVARCHAR(32) NOT NULL DEFAULT 'dhl_v1',
            source_system NVARCHAR(32) NOT NULL DEFAULT 'shipment_aggregate',
            calculated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_order_logistics_shadow', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_order_logistics_shadow (
            amazon_order_id NVARCHAR(80) NOT NULL PRIMARY KEY,
            acc_order_id UNIQUEIDENTIFIER NULL,
            legacy_logistics_pln DECIMAL(18,4) NOT NULL DEFAULT 0,
            shadow_logistics_pln DECIMAL(18,4) NOT NULL DEFAULT 0,
            delta_pln DECIMAL(18,4) NOT NULL DEFAULT 0,
            delta_abs_pln DECIMAL(18,4) NOT NULL DEFAULT 0,
            shipments_count INT NOT NULL DEFAULT 0,
            actual_shipments_count INT NOT NULL DEFAULT 0,
            estimated_shipments_count INT NOT NULL DEFAULT 0,
            comparison_status NVARCHAR(32) NOT NULL DEFAULT 'unknown',
            calc_version NVARCHAR(32) NOT NULL DEFAULT 'dhl_v1',
            calculated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    # ── Cost estimation / relation tables ─────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_courier_cost_estimate', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_courier_cost_estimate (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            shipment_id UNIQUEIDENTIFIER NOT NULL,
            carrier NVARCHAR(16) NOT NULL,
            amazon_order_id NVARCHAR(80) NULL,
            estimator_name NVARCHAR(64) NOT NULL DEFAULT 'courier_hist_v1',
            model_version NVARCHAR(32) NOT NULL DEFAULT 'courier_hist_v1',
            horizon_days INT NOT NULL DEFAULT 180,
            min_samples INT NOT NULL DEFAULT 10,
            bucket_key NVARCHAR(300) NOT NULL,
            sample_count INT NOT NULL DEFAULT 0,
            estimated_amount_pln DECIMAL(18,4) NOT NULL DEFAULT 0,
            estimated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            status NVARCHAR(24) NOT NULL DEFAULT 'estimated',
            reconciled_at DATETIME2 NULL,
            actual_amount_pln DECIMAL(18,4) NULL,
            abs_error_pln DECIMAL(18,4) NULL,
            ape_pct DECIMAL(18,4) NULL,
            replaced_by_cost_source NVARCHAR(64) NULL,
            raw_payload_json NVARCHAR(MAX) NULL
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_courier_estimation_kpi_daily', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_courier_estimation_kpi_daily (
            kpi_date DATE NOT NULL,
            carrier NVARCHAR(16) NOT NULL,
            model_version NVARCHAR(32) NOT NULL,
            samples_count INT NOT NULL DEFAULT 0,
            mape_pct DECIMAL(18,4) NULL,
            mae_pln DECIMAL(18,4) NULL,
            p95_ape_pct DECIMAL(18,4) NULL,
            calculated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_acc_courier_estimation_kpi_daily PRIMARY KEY (kpi_date, carrier, model_version)
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_order_courier_relation', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_order_courier_relation (
            id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
            carrier NVARCHAR(16) NOT NULL,
            source_amazon_order_id NVARCHAR(80) NOT NULL,
            source_acc_order_id UNIQUEIDENTIFIER NULL,
            source_distribution_order_id BIGINT NULL,
            source_bl_order_id BIGINT NULL,
            source_purchase_date DATE NULL,
            related_distribution_order_id BIGINT NOT NULL,
            related_bl_order_id BIGINT NULL,
            related_external_order_id NVARCHAR(128) NULL,
            related_order_source NVARCHAR(64) NULL,
            related_order_source_id INT NULL,
            related_order_date DATE NULL,
            relation_type NVARCHAR(32) NOT NULL,
            detection_method NVARCHAR(64) NOT NULL,
            confidence DECIMAL(9,4) NOT NULL DEFAULT 0,
            is_strong BIT NOT NULL DEFAULT 0,
            evidence_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    op.execute("""
    IF OBJECT_ID('dbo.acc_shipment_outcome_fact', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_shipment_outcome_fact (
            shipment_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            carrier NVARCHAR(16) NOT NULL,
            ship_month DATE NULL,
            amazon_order_id NVARCHAR(80) NULL,
            acc_order_id UNIQUEIDENTIFIER NULL,
            bl_order_id BIGINT NULL,
            primary_link_method NVARCHAR(64) NULL,
            relation_type NVARCHAR(32) NULL,
            relation_confidence DECIMAL(9,4) NULL,
            outcome_code NVARCHAR(32) NOT NULL,
            outcome_confidence DECIMAL(9,4) NOT NULL DEFAULT 0,
            cost_reason NVARCHAR(32) NOT NULL,
            cost_reason_confidence DECIMAL(9,4) NOT NULL DEFAULT 0,
            classifier_version NVARCHAR(32) NOT NULL DEFAULT 'courier_semantics_v1',
            evidence_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """)

    # ── ALTER TABLE: add columns to existing tables ───────────────────
    op.execute("""
    IF COL_LENGTH('dbo.acc_order_logistics_fact', 'actual_shipments_count') IS NULL
    BEGIN
        ALTER TABLE dbo.acc_order_logistics_fact
            ADD actual_shipments_count INT NOT NULL CONSTRAINT DF_acc_order_logistics_fact_actual DEFAULT 0;
    END
    """)

    op.execute("""
    IF COL_LENGTH('dbo.acc_order_logistics_fact', 'estimated_shipments_count') IS NULL
    BEGIN
        ALTER TABLE dbo.acc_order_logistics_fact
            ADD estimated_shipments_count INT NOT NULL CONSTRAINT DF_acc_order_logistics_fact_estimated DEFAULT 0;
    END
    """)

    # ── PK migration: acc_order_logistics_fact ────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_order_logistics_fact', 'U') IS NOT NULL
    BEGIN
        DECLARE @pk_fact_to_drop SYSNAME = NULL;
        SELECT TOP 1 @pk_fact_to_drop = kc.name
        FROM sys.key_constraints kc
        WHERE kc.parent_object_id = OBJECT_ID('dbo.acc_order_logistics_fact')
          AND kc.type = 'PK'
          AND NOT EXISTS (
              SELECT 1
              FROM sys.index_columns ic
              JOIN sys.columns c
                ON c.object_id = ic.object_id
               AND c.column_id = ic.column_id
              WHERE ic.object_id = kc.parent_object_id
                AND ic.index_id = kc.unique_index_id
              GROUP BY ic.object_id, ic.index_id
              HAVING COUNT(*) = 2
                 AND SUM(CASE WHEN c.name = 'amazon_order_id' AND ic.key_ordinal = 1 THEN 1 ELSE 0 END) = 1
                 AND SUM(CASE WHEN c.name = 'calc_version' AND ic.key_ordinal = 2 THEN 1 ELSE 0 END) = 1
          );
        IF @pk_fact_to_drop IS NOT NULL
        BEGIN
            EXEC ('ALTER TABLE dbo.acc_order_logistics_fact DROP CONSTRAINT [' + @pk_fact_to_drop + ']');
        END;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.key_constraints
            WHERE parent_object_id = OBJECT_ID('dbo.acc_order_logistics_fact')
              AND type = 'PK'
        )
        BEGIN
            ALTER TABLE dbo.acc_order_logistics_fact
                ADD CONSTRAINT PK_acc_order_logistics_fact PRIMARY KEY (amazon_order_id, calc_version);
        END;
    END
    """)

    # ── PK migration: acc_order_logistics_shadow ──────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_order_logistics_shadow', 'U') IS NOT NULL
    BEGIN
        DECLARE @pk_shadow_to_drop SYSNAME = NULL;
        SELECT TOP 1 @pk_shadow_to_drop = kc.name
        FROM sys.key_constraints kc
        WHERE kc.parent_object_id = OBJECT_ID('dbo.acc_order_logistics_shadow')
          AND kc.type = 'PK'
          AND NOT EXISTS (
              SELECT 1
              FROM sys.index_columns ic
              JOIN sys.columns c
                ON c.object_id = ic.object_id
               AND c.column_id = ic.column_id
              WHERE ic.object_id = kc.parent_object_id
                AND ic.index_id = kc.unique_index_id
              GROUP BY ic.object_id, ic.index_id
              HAVING COUNT(*) = 2
                 AND SUM(CASE WHEN c.name = 'amazon_order_id' AND ic.key_ordinal = 1 THEN 1 ELSE 0 END) = 1
                 AND SUM(CASE WHEN c.name = 'calc_version' AND ic.key_ordinal = 2 THEN 1 ELSE 0 END) = 1
          );
        IF @pk_shadow_to_drop IS NOT NULL
        BEGIN
            EXEC ('ALTER TABLE dbo.acc_order_logistics_shadow DROP CONSTRAINT [' + @pk_shadow_to_drop + ']');
        END;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.key_constraints
            WHERE parent_object_id = OBJECT_ID('dbo.acc_order_logistics_shadow')
              AND type = 'PK'
        )
        BEGIN
            ALTER TABLE dbo.acc_order_logistics_shadow
                ADD CONSTRAINT PK_acc_order_logistics_shadow PRIMARY KEY (amazon_order_id, calc_version);
        END;
    END
    """)

    # ── Indexes ───────────────────────────────────────────────────────
    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_shipment_carrier_shipment_number'
          AND object_id = OBJECT_ID('dbo.acc_shipment')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_shipment_carrier_shipment_number
            ON dbo.acc_shipment(carrier, shipment_number)
            WHERE shipment_number IS NOT NULL;
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_shipment_tracking_number'
          AND object_id = OBJECT_ID('dbo.acc_shipment')
    )
    BEGIN
        CREATE INDEX IX_acc_shipment_tracking_number
            ON dbo.acc_shipment(tracking_number, last_sync_at);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_shipment_piece_id'
          AND object_id = OBJECT_ID('dbo.acc_shipment')
    )
    BEGIN
        CREATE INDEX IX_acc_shipment_piece_id
            ON dbo.acc_shipment(piece_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_shipment_delivery'
          AND object_id = OBJECT_ID('dbo.acc_shipment')
    )
    BEGIN
        CREATE INDEX IX_acc_shipment_delivery
            ON dbo.acc_shipment(is_delivered, delivered_at);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_shipment_order_link_main'
          AND object_id = OBJECT_ID('dbo.acc_shipment_order_link')
    )
    BEGIN
        CREATE INDEX IX_acc_shipment_order_link_main
            ON dbo.acc_shipment_order_link(shipment_id, amazon_order_id, bl_order_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_shipment_order_link_primary'
          AND object_id = OBJECT_ID('dbo.acc_shipment_order_link')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_shipment_order_link_primary
            ON dbo.acc_shipment_order_link(shipment_id, amazon_order_id, link_method)
            WHERE amazon_order_id IS NOT NULL;
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_shipment_event_dedup'
          AND object_id = OBJECT_ID('dbo.acc_shipment_event')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_shipment_event_dedup
            ON dbo.acc_shipment_event(shipment_id, event_code, event_at, event_label);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_shipment_pod_one'
          AND object_id = OBJECT_ID('dbo.acc_shipment_pod')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_shipment_pod_one
            ON dbo.acc_shipment_pod(shipment_id, pod_type);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_shipment_cost_main'
          AND object_id = OBJECT_ID('dbo.acc_shipment_cost')
    )
    BEGIN
        CREATE INDEX IX_acc_shipment_cost_main
            ON dbo.acc_shipment_cost(shipment_id, cost_source, invoice_date);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_dhl_import_file_kind_path'
          AND object_id = OBJECT_ID('dbo.acc_dhl_import_file')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_dhl_import_file_kind_path
            ON dbo.acc_dhl_import_file(source_kind, file_path);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_dhl_billing_document_issue'
          AND object_id = OBJECT_ID('dbo.acc_dhl_billing_document')
    )
    BEGIN
        CREATE INDEX IX_acc_dhl_billing_document_issue
            ON dbo.acc_dhl_billing_document(issue_date, document_type);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_dhl_billing_line_doc_row'
          AND object_id = OBJECT_ID('dbo.acc_dhl_billing_line')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_dhl_billing_line_doc_row
            ON dbo.acc_dhl_billing_line(document_number, source_row_no);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_dhl_billing_line_parcel'
          AND object_id = OBJECT_ID('dbo.acc_dhl_billing_line')
    )
    BEGIN
        CREATE INDEX IX_acc_dhl_billing_line_parcel
            ON dbo.acc_dhl_billing_line(parcel_number_base, issue_date);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_dhl_parcel_map_source_row'
          AND object_id = OBJECT_ID('dbo.acc_dhl_parcel_map')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_dhl_parcel_map_source_row
            ON dbo.acc_dhl_parcel_map(source_file, source_row_no);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_dhl_parcel_map_lookup'
          AND object_id = OBJECT_ID('dbo.acc_dhl_parcel_map')
    )
    BEGIN
        CREATE INDEX IX_acc_dhl_parcel_map_lookup
            ON dbo.acc_dhl_parcel_map(jjd_number, parcel_number_base, delivery_date);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_order_logistics_fact_order'
          AND object_id = OBJECT_ID('dbo.acc_order_logistics_fact')
    )
    BEGIN
        CREATE INDEX IX_acc_order_logistics_fact_order
            ON dbo.acc_order_logistics_fact(acc_order_id, calculated_at);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_order_logistics_shadow_status'
          AND object_id = OBJECT_ID('dbo.acc_order_logistics_shadow')
    )
    BEGIN
        CREATE INDEX IX_acc_order_logistics_shadow_status
            ON dbo.acc_order_logistics_shadow(comparison_status, calculated_at);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_order_logistics_shadow_order'
          AND object_id = OBJECT_ID('dbo.acc_order_logistics_shadow')
    )
    BEGIN
        CREATE INDEX IX_acc_order_logistics_shadow_order
            ON dbo.acc_order_logistics_shadow(acc_order_id, calculated_at);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_courier_cost_estimate_shipment_estimator'
          AND object_id = OBJECT_ID('dbo.acc_courier_cost_estimate')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_courier_cost_estimate_shipment_estimator
            ON dbo.acc_courier_cost_estimate(shipment_id, estimator_name);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_courier_cost_estimate_status'
          AND object_id = OBJECT_ID('dbo.acc_courier_cost_estimate')
    )
    BEGIN
        CREATE INDEX IX_acc_courier_cost_estimate_status
            ON dbo.acc_courier_cost_estimate(status, carrier, estimated_at DESC);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UX_acc_order_courier_relation_scope'
          AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_acc_order_courier_relation_scope
            ON dbo.acc_order_courier_relation(carrier, source_amazon_order_id, related_distribution_order_id, relation_type);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_order_courier_relation_source_scope'
          AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
    )
    BEGIN
        CREATE INDEX IX_acc_order_courier_relation_source_scope
            ON dbo.acc_order_courier_relation(source_purchase_date, carrier, is_strong, source_amazon_order_id);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_order_courier_relation_related_scope'
          AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
    )
    BEGIN
        CREATE INDEX IX_acc_order_courier_relation_related_scope
            ON dbo.acc_order_courier_relation(carrier, related_bl_order_id, source_amazon_order_id, is_strong);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_shipment_outcome_fact_ship_month'
          AND object_id = OBJECT_ID('dbo.acc_shipment_outcome_fact')
    )
    BEGIN
        CREATE INDEX IX_acc_shipment_outcome_fact_ship_month
            ON dbo.acc_shipment_outcome_fact(ship_month, carrier, outcome_code, cost_reason);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_shipment_outcome_fact_amazon_order'
          AND object_id = OBJECT_ID('dbo.acc_shipment_outcome_fact')
    )
    BEGIN
        CREATE INDEX IX_acc_shipment_outcome_fact_amazon_order
            ON dbo.acc_shipment_outcome_fact(amazon_order_id, carrier, ship_month);
    END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_shipment_outcome_fact")
    op.execute("DROP TABLE IF EXISTS dbo.acc_order_courier_relation")
    op.execute("DROP TABLE IF EXISTS dbo.acc_courier_estimation_kpi_daily")
    op.execute("DROP TABLE IF EXISTS dbo.acc_courier_cost_estimate")
    op.execute("DROP TABLE IF EXISTS dbo.acc_order_logistics_shadow")
    op.execute("DROP TABLE IF EXISTS dbo.acc_order_logistics_fact")
    op.execute("DROP TABLE IF EXISTS dbo.acc_dhl_parcel_map")
    op.execute("DROP TABLE IF EXISTS dbo.acc_dhl_billing_line")
    op.execute("DROP TABLE IF EXISTS dbo.acc_dhl_billing_document")
    op.execute("DROP TABLE IF EXISTS dbo.acc_dhl_import_file")
    op.execute("DROP TABLE IF EXISTS dbo.acc_shipment_cost")
    op.execute("DROP TABLE IF EXISTS dbo.acc_shipment_pod")
    op.execute("DROP TABLE IF EXISTS dbo.acc_shipment_event")
    op.execute("DROP TABLE IF EXISTS dbo.acc_shipment_order_link")
    op.execute("DROP TABLE IF EXISTS dbo.acc_shipment")
