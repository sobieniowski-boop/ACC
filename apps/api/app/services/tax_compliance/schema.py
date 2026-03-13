"""
VAT & OSS Compliance Center – Database Schema Init.

Creates all compliance tables on startup (idempotent).
Called from main.py lifespan via ensure_tax_compliance_schema().
"""
from __future__ import annotations

from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── EU VAT rates (standard) ──────────────────────────────────────
EU_VAT_RATES: list[tuple[str, float, str]] = [
    ("AT", 20.0, "2024-01-01"),
    ("BE", 21.0, "2024-01-01"),
    ("BG", 20.0, "2024-01-01"),
    ("HR", 25.0, "2024-01-01"),
    ("CY", 19.0, "2024-01-01"),
    ("CZ", 21.0, "2024-01-01"),
    ("DK", 25.0, "2024-01-01"),
    ("EE", 22.0, "2024-01-01"),
    ("FI", 25.5, "2024-01-01"),
    ("FR", 20.0, "2024-01-01"),
    ("DE", 19.0, "2024-01-01"),
    ("GR", 24.0, "2024-01-01"),
    ("HU", 27.0, "2024-01-01"),
    ("IE", 23.0, "2024-01-01"),
    ("IT", 22.0, "2024-01-01"),
    ("LV", 21.0, "2024-01-01"),
    ("LT", 21.0, "2024-01-01"),
    ("LU", 17.0, "2024-01-01"),
    ("MT", 18.0, "2024-01-01"),
    ("NL", 21.0, "2024-01-01"),
    ("PL", 23.0, "2024-01-01"),
    ("PT", 23.0, "2024-01-01"),
    ("RO", 19.0, "2024-01-01"),
    ("SK", 23.0, "2024-01-01"),
    ("SI", 22.0, "2024-01-01"),
    ("ES", 21.0, "2024-01-01"),
    ("SE", 25.0, "2024-01-01"),
]


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def ensure_tax_compliance_schema() -> None:
    """Idempotent DDL – creates all VAT/OSS compliance tables + seed data."""
    conn = _connect()
    try:
        cur = conn.cursor()

        # ── A) vat_event_ledger ──────────────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.vat_event_ledger', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.vat_event_ledger (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        event_type      NVARCHAR(40)   NOT NULL,
        source_system   NVARCHAR(30)   NOT NULL,
        source_ref      NVARCHAR(120)  NULL,
        order_id        NVARCHAR(120)  NULL,
        transaction_id  NVARCHAR(120)  NULL,
        marketplace     NVARCHAR(10)   NULL,
        sku             NVARCHAR(80)   NULL,
        asin            NVARCHAR(20)   NULL,
        quantity        DECIMAL(18,4)  NULL,
        ship_from_country NVARCHAR(2)  NULL,
        ship_to_country NVARCHAR(2)    NULL,
        warehouse_country NVARCHAR(2)  NULL,
        consumption_country NVARCHAR(2) NULL,
        vat_classification NVARCHAR(40) NOT NULL,
        tax_jurisdiction NVARCHAR(20)  NULL,
        tax_rate        DECIMAL(9,4)   NULL,
        tax_base_amount DECIMAL(18,4)  NULL,
        tax_amount      DECIMAL(18,4)  NULL,
        gross_amount    DECIMAL(18,4)  NULL,
        currency        NVARCHAR(3)    NULL,
        amount_eur      DECIMAL(18,4)  NULL,
        ecb_rate        DECIMAL(18,8)  NULL,
        event_date      DATE           NOT NULL,
        evidence_status NVARCHAR(20)   NULL,
        confidence_score DECIMAL(9,4)  NULL,
        notes           NVARCHAR(MAX)  NULL,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_vat_event_order_id
        ON dbo.vat_event_ledger(order_id);
    CREATE INDEX IX_vat_event_marketplace_date
        ON dbo.vat_event_ledger(marketplace, event_date);
    CREATE INDEX IX_vat_event_classification
        ON dbo.vat_event_ledger(vat_classification);
    CREATE INDEX IX_vat_event_jurisdiction
        ON dbo.vat_event_ledger(tax_jurisdiction);
    CREATE INDEX IX_vat_event_ship_from_to
        ON dbo.vat_event_ledger(ship_from_country, ship_to_country);
    CREATE INDEX IX_vat_event_sku
        ON dbo.vat_event_ledger(sku);
END;
        """)

        # ── B) vat_transaction_classification ────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.vat_transaction_classification', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.vat_transaction_classification (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        source_ref      NVARCHAR(120)  NOT NULL,
        source_type     NVARCHAR(30)   NOT NULL,
        classification  NVARCHAR(40)   NOT NULL,
        reason_json     NVARCHAR(MAX)  NULL,
        confidence_score DECIMAL(9,4)  NOT NULL,
        reviewed_by     NVARCHAR(120)  NULL,
        reviewed_at     DATETIME2      NULL,
        status          NVARCHAR(20)   NOT NULL DEFAULT 'auto',
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_vat_class_source ON dbo.vat_transaction_classification(source_ref, source_type);
    CREATE INDEX IX_vat_class_status ON dbo.vat_transaction_classification(status, classification);
END;
        """)

        # ── C) transport_evidence_record ─────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.transport_evidence_record', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.transport_evidence_record (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        source_ref      NVARCHAR(120)  NOT NULL,
        order_id        NVARCHAR(120)  NULL,
        marketplace     NVARCHAR(10)   NULL,
        carrier         NVARCHAR(80)   NULL,
        tracking_id     NVARCHAR(120)  NULL,
        dispatch_date   DATE           NULL,
        delivery_date   DATE           NULL,
        proof_transport BIT            NOT NULL DEFAULT 0,
        proof_delivery  BIT            NOT NULL DEFAULT 0,
        proof_order     BIT            NOT NULL DEFAULT 0,
        proof_payment   BIT            NOT NULL DEFAULT 0,
        evidence_status NVARCHAR(20)   NOT NULL DEFAULT 'missing',
        evidence_json   NVARCHAR(MAX)  NULL,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_transport_evidence_order ON dbo.transport_evidence_record(order_id);
    CREATE INDEX IX_transport_evidence_status ON dbo.transport_evidence_record(evidence_status);
END;
        """)

        # ── D) fba_stock_movement_ledger ─────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.fba_stock_movement_ledger', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fba_stock_movement_ledger (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        movement_ref    NVARCHAR(120)  NOT NULL,
        sku             NVARCHAR(80)   NOT NULL,
        asin            NVARCHAR(20)   NULL,
        quantity        DECIMAL(18,4)  NOT NULL,
        movement_date   DATE           NOT NULL,
        from_country    NVARCHAR(2)    NOT NULL,
        to_country      NVARCHAR(2)    NOT NULL,
        movement_type   NVARCHAR(30)   NOT NULL,
        vat_treatment   NVARCHAR(30)   NOT NULL DEFAULT 'PENDING',
        matching_pair_status NVARCHAR(20) NOT NULL DEFAULT 'unmatched',
        transport_evidence_status NVARCHAR(20) NOT NULL DEFAULT 'missing',
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_fba_movement_sku ON dbo.fba_stock_movement_ledger(sku);
    CREATE INDEX IX_fba_movement_date ON dbo.fba_stock_movement_ledger(movement_date);
    CREATE INDEX IX_fba_movement_countries ON dbo.fba_stock_movement_ledger(from_country, to_country);
    CREATE INDEX IX_fba_movement_treatment ON dbo.fba_stock_movement_ledger(vat_treatment);
END;
        """)

        # ── E) oss_return_period ─────────────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.oss_return_period', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.oss_return_period (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        year            INT            NOT NULL,
        quarter         INT            NOT NULL,
        status          NVARCHAR(20)   NOT NULL DEFAULT 'open',
        total_base_eur  DECIMAL(18,4)  NULL,
        total_tax_eur   DECIMAL(18,4)  NULL,
        corrections_count INT          NOT NULL DEFAULT 0,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        filed_at        DATETIME2      NULL
    );

    CREATE UNIQUE INDEX UX_oss_return_period ON dbo.oss_return_period(year, quarter);
END;
        """)

        # ── F) oss_return_line ───────────────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.oss_return_line', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.oss_return_line (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        oss_period_id   INT            NOT NULL,
        consumption_country NVARCHAR(2) NOT NULL,
        vat_rate        DECIMAL(9,4)   NOT NULL,
        tax_base_eur    DECIMAL(18,4)  NOT NULL,
        tax_amount_eur  DECIMAL(18,4)  NOT NULL,
        correction_flag BIT            NOT NULL DEFAULT 0,
        source_count    INT            NOT NULL DEFAULT 0,

        CONSTRAINT FK_oss_line_period FOREIGN KEY (oss_period_id)
            REFERENCES dbo.oss_return_period(id)
    );

    CREATE INDEX IX_oss_line_period ON dbo.oss_return_line(oss_period_id);
    CREATE INDEX IX_oss_line_country ON dbo.oss_return_line(consumption_country);
END;
        """)

        # ── G) local_vat_ledger ──────────────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.local_vat_ledger', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.local_vat_ledger (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        country         NVARCHAR(2)    NOT NULL,
        source_ref      NVARCHAR(120)  NOT NULL,
        event_type      NVARCHAR(30)   NOT NULL,
        tax_base        DECIMAL(18,4)  NOT NULL,
        tax_amount      DECIMAL(18,4)  NOT NULL,
        currency        NVARCHAR(3)    NOT NULL,
        event_date      DATE           NOT NULL,
        status          NVARCHAR(20)   NOT NULL DEFAULT 'open'
    );

    CREATE INDEX IX_local_vat_country ON dbo.local_vat_ledger(country, event_date);
    CREATE INDEX IX_local_vat_status ON dbo.local_vat_ledger(status);
END;
        """)

        # ── H) amazon_clearing_reconciliation ────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.amazon_clearing_reconciliation', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.amazon_clearing_reconciliation (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        settlement_id   NVARCHAR(120)  NOT NULL,
        settlement_date DATE           NOT NULL,
        gross_sales     DECIMAL(18,4)  NULL,
        vat_oss         DECIMAL(18,4)  NULL,
        vat_local       DECIMAL(18,4)  NULL,
        amazon_fees     DECIMAL(18,4)  NULL,
        refunds         DECIMAL(18,4)  NULL,
        ads             DECIMAL(18,4)  NULL,
        payout_net      DECIMAL(18,4)  NULL,
        expected_net    DECIMAL(18,4)  NULL,
        difference_amount DECIMAL(18,4) NULL,
        status          NVARCHAR(20)   NOT NULL DEFAULT 'partial',
        details_json    NVARCHAR(MAX)  NULL
    );

    CREATE UNIQUE INDEX UX_amazon_clearing_settlement ON dbo.amazon_clearing_reconciliation(settlement_id);
END;
        """)

        # ── I) vat_rate_mapping ──────────────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.vat_rate_mapping', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.vat_rate_mapping (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        country         NVARCHAR(2)    NOT NULL,
        product_type    NVARCHAR(120)  NULL,
        category        NVARCHAR(200)  NULL,
        rate            DECIMAL(9,4)   NOT NULL,
        valid_from      DATE           NOT NULL,
        valid_to        DATE           NULL,
        source          NVARCHAR(120)  NULL,
        is_default      BIT            NOT NULL DEFAULT 0
    );

    CREATE INDEX IX_vat_rate_country ON dbo.vat_rate_mapping(country, valid_from);
END;
        """)

        # ── J) filing_readiness_snapshot ─────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.filing_readiness_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.filing_readiness_snapshot (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        period_type     NVARCHAR(20)   NOT NULL,
        period_ref      NVARCHAR(20)   NOT NULL,
        viu_do_ready_pct   DECIMAL(9,4) NULL,
        jpk_ready_pct      DECIMAL(9,4) NULL,
        local_vat_ready_pct DECIMAL(9,4) NULL,
        evidence_complete_pct DECIMAL(9,4) NULL,
        movement_match_pct DECIMAL(9,4) NULL,
        critical_issues_count INT NOT NULL DEFAULT 0,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_filing_readiness_period ON dbo.filing_readiness_snapshot(period_type, period_ref);
END;
        """)

        # ── K) compliance_issue ──────────────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.compliance_issue', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.compliance_issue (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        issue_type      NVARCHAR(40)   NOT NULL,
        severity        NVARCHAR(10)   NOT NULL,
        source_ref      NVARCHAR(120)  NULL,
        country         NVARCHAR(2)    NULL,
        marketplace     NVARCHAR(10)   NULL,
        description     NVARCHAR(MAX)  NOT NULL,
        status          NVARCHAR(20)   NOT NULL DEFAULT 'open',
        owner           NVARCHAR(120)  NULL,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_compliance_issue_type ON dbo.compliance_issue(issue_type, severity);
    CREATE INDEX IX_compliance_issue_status ON dbo.compliance_issue(status);
END;
        """)

        # ── L) ecb_exchange_rate cache ───────────────────────────
        cur.execute("""
IF OBJECT_ID('dbo.ecb_exchange_rate', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ecb_exchange_rate (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        rate_date       DATE           NOT NULL,
        source_currency NVARCHAR(3)    NOT NULL,
        target_currency NVARCHAR(3)    NOT NULL DEFAULT 'EUR',
        rate            DECIMAL(18,8)  NOT NULL,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE UNIQUE INDEX UX_ecb_rate ON dbo.ecb_exchange_rate(rate_date, source_currency, target_currency);
END;
        """)

        conn.commit()

        # ── Seed VAT rates ───────────────────────────────────────
        for country, rate, valid_from in EU_VAT_RATES:
            cur.execute("""
IF NOT EXISTS (
    SELECT 1 FROM dbo.vat_rate_mapping
    WHERE country = ? AND is_default = 1 AND valid_from = ?
)
BEGIN
    INSERT INTO dbo.vat_rate_mapping(country, rate, valid_from, is_default, source)
    VALUES (?, ?, ?, 1, 'EU_standard_rate_seed')
END
            """, (country, valid_from, country, rate, valid_from))
        conn.commit()

        log.info("tax_compliance.schema_ready", tables=12)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
