"""Canonical product model (acc_canonical_product, acc_marketplace_presence).

Revision ID: eb026
Revises: eb025
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb026"
down_revision = "eb025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── canonical product: single source of truth per internal SKU ──
    op.execute("""
IF OBJECT_ID('dbo.acc_canonical_product', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_canonical_product (
        internal_sku        NVARCHAR(64)    NOT NULL PRIMARY KEY,
        ean                 NVARCHAR(64)    NULL,
        brand               NVARCHAR(128)   NULL,
        category            NVARCHAR(255)   NULL,
        subcategory         NVARCHAR(255)   NULL,
        product_name        NVARCHAR(500)   NULL,
        image_url           NVARCHAR(500)   NULL,
        lifecycle_status    NVARCHAR(30)    NOT NULL DEFAULT 'active',
        k_number            NVARCHAR(20)    NULL,
        ergonode_id         NVARCHAR(36)    NULL,
        netto_purchase_price_pln DECIMAL(10,4) NULL,
        vat_rate            DECIMAL(5,2)    NULL DEFAULT 23.00,
        mapping_confidence  DECIMAL(5,2)    NULL,
        mapping_source      NVARCHAR(40)    NULL,
        needs_review        BIT             NOT NULL DEFAULT 0,
        created_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_canonical_product_ean
        ON dbo.acc_canonical_product(ean);
    CREATE INDEX IX_acc_canonical_product_brand
        ON dbo.acc_canonical_product(brand, lifecycle_status);
    CREATE INDEX IX_acc_canonical_product_lifecycle
        ON dbo.acc_canonical_product(lifecycle_status, needs_review);
    CREATE INDEX IX_acc_canonical_product_k_number
        ON dbo.acc_canonical_product(k_number);
END
""")

    # ── marketplace presence: per-marketplace listing for a canonical product ──
    op.execute("""
IF OBJECT_ID('dbo.acc_marketplace_presence', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_marketplace_presence (
        id                  UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        internal_sku        NVARCHAR(64)    NOT NULL,
        marketplace_id      VARCHAR(20)     NOT NULL,
        seller_sku          NVARCHAR(255)   NULL,
        asin                NVARCHAR(40)    NULL,
        parent_asin         NVARCHAR(40)    NULL,
        fnsku               NVARCHAR(20)    NULL,
        listing_status      NVARCHAR(30)    NOT NULL DEFAULT 'UNKNOWN',
        fulfillment_channel NVARCHAR(20)    NULL,
        last_seen_at        DATETIME2       NULL,
        created_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_marketplace_presence_sku_mkt
        ON dbo.acc_marketplace_presence(internal_sku, marketplace_id, seller_sku);
    CREATE INDEX IX_acc_marketplace_presence_asin
        ON dbo.acc_marketplace_presence(asin, marketplace_id);
    CREATE INDEX IX_acc_marketplace_presence_seller_sku
        ON dbo.acc_marketplace_presence(seller_sku, marketplace_id);
    CREATE INDEX IX_acc_marketplace_presence_canonical
        ON dbo.acc_marketplace_presence(internal_sku);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_marketplace_presence")
    op.execute("DROP TABLE IF EXISTS dbo.acc_canonical_product")
