"""Add UNIQUE constraints for finance transaction dedup and inventory upsert.

E0.2: acc_finance_transaction — prevent duplicate financial events.
E0.3: acc_inventory_snapshot — exactly one snapshot per (product, marketplace, date).

Revision ID: eb007
Revises: eb006
Create Date: 2026-03-11
"""
from alembic import op

revision = "eb007"
down_revision = "eb006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── E0.2  Finance transaction dedup ──────────────────────────────────
    # Step 1: Remove duplicate rows, keeping the earliest synced_at per group
    op.execute("""
    WITH dupes AS (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY posted_date, amazon_order_id, sku, charge_type, amount, currency, marketplace_id
                   ORDER BY synced_at ASC
               ) AS rn
        FROM dbo.acc_finance_transaction
    )
    DELETE FROM dupes WHERE rn > 1
    """)

    # Step 2: Add unique constraint
    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UQ_finance_tx_dedup'
          AND object_id = OBJECT_ID('dbo.acc_finance_transaction')
    )
    CREATE UNIQUE INDEX UQ_finance_tx_dedup
        ON dbo.acc_finance_transaction (
            posted_date, marketplace_id, amazon_order_id, sku, charge_type, amount, currency
        )
        WHERE amazon_order_id IS NOT NULL AND sku IS NOT NULL
    """)

    # ── E0.3  Inventory snapshot upsert constraint ───────────────────────
    # Step 1: Remove duplicate snapshots, keeping the latest synced_at per group
    op.execute("""
    WITH dupes AS (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY product_id, marketplace_id, snapshot_date
                   ORDER BY synced_at DESC
               ) AS rn
        FROM dbo.acc_inventory_snapshot
    )
    DELETE FROM dupes WHERE rn > 1
    """)

    # Step 2: Add unique constraint
    op.execute("""
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UQ_inventory_snap_dedup'
          AND object_id = OBJECT_ID('dbo.acc_inventory_snapshot')
    )
    CREATE UNIQUE INDEX UQ_inventory_snap_dedup
        ON dbo.acc_inventory_snapshot (product_id, marketplace_id, snapshot_date)
    """)


def downgrade() -> None:
    op.execute("""
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UQ_inventory_snap_dedup'
          AND object_id = OBJECT_ID('dbo.acc_inventory_snapshot')
    )
    DROP INDEX UQ_inventory_snap_dedup ON dbo.acc_inventory_snapshot
    """)

    op.execute("""
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'UQ_finance_tx_dedup'
          AND object_id = OBJECT_ID('dbo.acc_finance_transaction')
    )
    DROP INDEX UQ_finance_tx_dedup ON dbo.acc_finance_transaction
    """)
