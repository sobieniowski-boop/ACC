"""Refund / Fee Anomaly Engine schema — Sprint 21.

Tables:
  acc_refund_anomaly        — Detected refund spike anomalies per SKU/period
  acc_serial_returner       — Identified serial returner patterns
  acc_reimbursement_case    — FBA reimbursement claim tracking

Revision ID: eb036
Revises: eb035
Create Date: 2026-03-12
"""
from alembic import op

revision = "eb036"
down_revision = "eb035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── acc_refund_anomaly ──────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_refund_anomaly', 'U') IS NULL
    CREATE TABLE dbo.acc_refund_anomaly (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        sku                     NVARCHAR(60)   NOT NULL,
        asin                    NVARCHAR(20)   NULL,
        marketplace_id          NVARCHAR(20)   NOT NULL,
        anomaly_type            NVARCHAR(40)   NOT NULL,  -- refund_spike, fee_spike, return_rate_spike
        detection_date          DATE           NOT NULL,
        period_start            DATE           NOT NULL,
        period_end              DATE           NOT NULL,
        baseline_rate           FLOAT          NOT NULL DEFAULT 0,
        current_rate            FLOAT          NOT NULL DEFAULT 0,
        spike_ratio             FLOAT          NOT NULL DEFAULT 0,
        refund_count            INT            NOT NULL DEFAULT 0,
        order_count             INT            NOT NULL DEFAULT 0,
        refund_amount_pln       FLOAT          NOT NULL DEFAULT 0,
        estimated_loss_pln      FLOAT          NOT NULL DEFAULT 0,
        severity                NVARCHAR(20)   NOT NULL DEFAULT 'medium',  -- critical, high, medium, low
        status                  NVARCHAR(20)   NOT NULL DEFAULT 'open',    -- open, investigating, resolved, dismissed
        resolution_note         NVARCHAR(500)  NULL,
        resolved_by             NVARCHAR(60)   NULL,
        resolved_at             DATETIME2      NULL,
        created_at              DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at              DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """)

    # ── acc_serial_returner ─────────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_serial_returner', 'U') IS NULL
    CREATE TABLE dbo.acc_serial_returner (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        buyer_identifier        NVARCHAR(120)  NOT NULL,       -- hashed/anonymised buyer id or order pattern
        marketplace_id          NVARCHAR(20)   NOT NULL,
        detection_date          DATE           NOT NULL,
        return_count            INT            NOT NULL DEFAULT 0,
        order_count             INT            NOT NULL DEFAULT 0,
        return_rate             FLOAT          NOT NULL DEFAULT 0,
        total_refund_pln        FLOAT          NOT NULL DEFAULT 0,
        avg_refund_pln          FLOAT          NOT NULL DEFAULT 0,
        first_return_date       DATE           NULL,
        last_return_date        DATE           NULL,
        top_skus                NVARCHAR(500)  NULL,            -- JSON array of most-returned SKUs
        risk_score              INT            NOT NULL DEFAULT 0,  -- 0-100
        risk_tier               NVARCHAR(20)   NOT NULL DEFAULT 'low',  -- critical, high, medium, low
        status                  NVARCHAR(20)   NOT NULL DEFAULT 'flagged',  -- flagged, monitoring, cleared, blocked
        notes                   NVARCHAR(500)  NULL,
        created_at              DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at              DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """)

    # ── acc_reimbursement_case ──────────────────────────────────
    op.execute("""
    IF OBJECT_ID('dbo.acc_reimbursement_case', 'U') IS NULL
    CREATE TABLE dbo.acc_reimbursement_case (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        case_type               NVARCHAR(40)   NOT NULL,  -- lost_inventory, damaged_inbound, fee_overcharge, customer_return_not_received
        sku                     NVARCHAR(60)   NOT NULL,
        asin                    NVARCHAR(20)   NULL,
        marketplace_id          NVARCHAR(20)   NOT NULL,
        amazon_order_id         NVARCHAR(30)   NULL,
        fnsku                   NVARCHAR(20)   NULL,
        quantity                INT            NOT NULL DEFAULT 1,
        estimated_value_pln     FLOAT          NOT NULL DEFAULT 0,
        evidence_summary        NVARCHAR(1000) NULL,
        amazon_case_id          NVARCHAR(40)   NULL,
        status                  NVARCHAR(20)   NOT NULL DEFAULT 'identified',  -- identified, filed, accepted, rejected, paid
        filed_at                DATETIME2      NULL,
        resolved_at             DATETIME2      NULL,
        reimbursed_amount_pln   FLOAT          NULL,
        resolution_note         NVARCHAR(500)  NULL,
        created_at              DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at              DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """)

    # ── Indexes ─────────────────────────────────────────────────
    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_refund_anomaly_sku_date')
    CREATE INDEX ix_refund_anomaly_sku_date
        ON dbo.acc_refund_anomaly (sku, detection_date DESC)
        INCLUDE (marketplace_id, severity, status);
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_refund_anomaly_severity')
    CREATE INDEX ix_refund_anomaly_severity
        ON dbo.acc_refund_anomaly (severity, status, detection_date DESC);
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_serial_returner_risk')
    CREATE INDEX ix_serial_returner_risk
        ON dbo.acc_serial_returner (risk_tier, risk_score DESC)
        INCLUDE (marketplace_id, return_count, total_refund_pln);
    """)

    op.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_reimbursement_case_status')
    CREATE INDEX ix_reimbursement_case_status
        ON dbo.acc_reimbursement_case (status, case_type)
        INCLUDE (sku, estimated_value_pln, marketplace_id);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_reimbursement_case;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_serial_returner;")
    op.execute("DROP TABLE IF EXISTS dbo.acc_refund_anomaly;")
