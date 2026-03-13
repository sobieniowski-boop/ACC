"""Courier monthly KPI schema: monthly snapshot table.

Converts ensure_courier_monthly_kpi_schema() DDL into a proper Alembic migration.
Idempotent: CREATE TABLE / INDEX wrapped in IF checks.

Revision ID: eb014
Revises: eb013
Create Date: 2026-03-13
"""
from alembic import op

revision = "eb014"
down_revision = "eb013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    IF OBJECT_ID('dbo.acc_courier_monthly_kpi_snapshot', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.acc_courier_monthly_kpi_snapshot (
            month_token NVARCHAR(7) NOT NULL,
            month_start DATE NOT NULL,
            carrier NVARCHAR(16) NOT NULL,
            calc_version NVARCHAR(32) NOT NULL,
            as_of_date DATE NOT NULL,
            buffer_days INT NOT NULL,
            is_closed_by_buffer BIT NOT NULL DEFAULT 0,
            month_closed_cutoff DATE NOT NULL,
            purchase_orders_universe INT NOT NULL DEFAULT 0,
            purchase_orders_linked_primary INT NOT NULL DEFAULT 0,
            purchase_orders_with_fact INT NOT NULL DEFAULT 0,
            purchase_orders_with_actual_cost INT NOT NULL DEFAULT 0,
            purchase_orders_without_primary_link INT NOT NULL DEFAULT 0,
            purchase_orders_with_estimated_only INT NOT NULL DEFAULT 0,
            purchase_orders_linked_but_no_cost INT NOT NULL DEFAULT 0,
            purchase_orders_missing_actual_cost INT NOT NULL DEFAULT 0,
            purchase_link_coverage_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
            purchase_fact_coverage_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
            purchase_actual_cost_coverage_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
            shipment_total INT NOT NULL DEFAULT 0,
            shipment_linked INT NOT NULL DEFAULT 0,
            shipment_actual_cost INT NOT NULL DEFAULT 0,
            shipment_link_coverage_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
            shipment_actual_cost_coverage_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
            billing_shipments_total INT NOT NULL DEFAULT 0,
            billing_shipments_linked INT NOT NULL DEFAULT 0,
            billing_link_coverage_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
            readiness NVARCHAR(16) NOT NULL DEFAULT 'PENDING',
            explain_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_acc_courier_monthly_kpi_snapshot PRIMARY KEY (month_token, carrier)
        );
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_courier_monthly_kpi_snapshot_readiness'
          AND object_id = OBJECT_ID('dbo.acc_courier_monthly_kpi_snapshot')
    )
    BEGIN
        CREATE INDEX IX_acc_courier_monthly_kpi_snapshot_readiness
            ON dbo.acc_courier_monthly_kpi_snapshot(readiness, month_start, carrier, as_of_date);
    END
    """)

    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_acc_courier_monthly_kpi_snapshot_updated'
          AND object_id = OBJECT_ID('dbo.acc_courier_monthly_kpi_snapshot')
    )
    BEGIN
        CREATE INDEX IX_acc_courier_monthly_kpi_snapshot_updated
            ON dbo.acc_courier_monthly_kpi_snapshot(updated_at DESC, month_start, carrier);
    END
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_courier_monthly_kpi_snapshot")
