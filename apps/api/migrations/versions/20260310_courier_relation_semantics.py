"""Add explicit courier relation and shipment outcome fact tables.

Revision ID: eb004
Revises: eb003
Create Date: 2026-03-10
"""
from alembic import op

revision = "eb004"
down_revision = "eb003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
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
END;

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
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UX_acc_order_courier_relation_scope'
      AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
)
BEGIN
    CREATE UNIQUE INDEX UX_acc_order_courier_relation_scope
        ON dbo.acc_order_courier_relation(carrier, source_amazon_order_id, related_distribution_order_id, relation_type);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_order_courier_relation_source_scope'
      AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
)
BEGIN
    CREATE INDEX IX_acc_order_courier_relation_source_scope
        ON dbo.acc_order_courier_relation(source_purchase_date, carrier, is_strong, source_amazon_order_id);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_order_courier_relation_related_scope'
      AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
)
BEGIN
    CREATE INDEX IX_acc_order_courier_relation_related_scope
        ON dbo.acc_order_courier_relation(carrier, related_bl_order_id, source_amazon_order_id, is_strong);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_shipment_outcome_fact_ship_month'
      AND object_id = OBJECT_ID('dbo.acc_shipment_outcome_fact')
)
BEGIN
    CREATE INDEX IX_acc_shipment_outcome_fact_ship_month
        ON dbo.acc_shipment_outcome_fact(ship_month, carrier, outcome_code, cost_reason);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_shipment_outcome_fact_amazon_order'
      AND object_id = OBJECT_ID('dbo.acc_shipment_outcome_fact')
)
BEGIN
    CREATE INDEX IX_acc_shipment_outcome_fact_amazon_order
        ON dbo.acc_shipment_outcome_fact(amazon_order_id, carrier, ship_month);
END;
        """
    )


def downgrade() -> None:
    op.execute(
        """
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_shipment_outcome_fact_amazon_order'
      AND object_id = OBJECT_ID('dbo.acc_shipment_outcome_fact')
)
    DROP INDEX IX_acc_shipment_outcome_fact_amazon_order ON dbo.acc_shipment_outcome_fact;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_shipment_outcome_fact_ship_month'
      AND object_id = OBJECT_ID('dbo.acc_shipment_outcome_fact')
)
    DROP INDEX IX_acc_shipment_outcome_fact_ship_month ON dbo.acc_shipment_outcome_fact;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_order_courier_relation_related_scope'
      AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
)
    DROP INDEX IX_acc_order_courier_relation_related_scope ON dbo.acc_order_courier_relation;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_order_courier_relation_source_scope'
      AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
)
    DROP INDEX IX_acc_order_courier_relation_source_scope ON dbo.acc_order_courier_relation;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UX_acc_order_courier_relation_scope'
      AND object_id = OBJECT_ID('dbo.acc_order_courier_relation')
)
    DROP INDEX UX_acc_order_courier_relation_scope ON dbo.acc_order_courier_relation;

IF OBJECT_ID('dbo.acc_shipment_outcome_fact', 'U') IS NOT NULL
    DROP TABLE dbo.acc_shipment_outcome_fact;

IF OBJECT_ID('dbo.acc_order_courier_relation', 'U') IS NOT NULL
    DROP TABLE dbo.acc_order_courier_relation;
        """
    )
