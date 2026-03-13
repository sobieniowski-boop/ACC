"""Add persisted order-level CM1 direct cost columns on acc_order.

Revision ID: eb006
Revises: eb005
Create Date: 2026-03-10
"""
from alembic import op

revision = "eb006"
down_revision = "eb005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
IF COL_LENGTH('dbo.acc_order', 'shipping_surcharge_pln') IS NULL
BEGIN
    ALTER TABLE dbo.acc_order
        ADD shipping_surcharge_pln DECIMAL(12,2) NULL;
END;

IF COL_LENGTH('dbo.acc_order', 'promo_order_fee_pln') IS NULL
BEGIN
    ALTER TABLE dbo.acc_order
        ADD promo_order_fee_pln DECIMAL(12,2) NULL;
END;

IF COL_LENGTH('dbo.acc_order', 'refund_commission_pln') IS NULL
BEGIN
    ALTER TABLE dbo.acc_order
        ADD refund_commission_pln DECIMAL(12,2) NULL;
END;
        """
    )


def downgrade() -> None:
    op.execute(
        """
IF COL_LENGTH('dbo.acc_order', 'refund_commission_pln') IS NOT NULL
BEGIN
    ALTER TABLE dbo.acc_order DROP COLUMN refund_commission_pln;
END;

IF COL_LENGTH('dbo.acc_order', 'promo_order_fee_pln') IS NOT NULL
BEGIN
    ALTER TABLE dbo.acc_order DROP COLUMN promo_order_fee_pln;
END;

IF COL_LENGTH('dbo.acc_order', 'shipping_surcharge_pln') IS NOT NULL
BEGIN
    ALTER TABLE dbo.acc_order DROP COLUMN shipping_surcharge_pln;
END;
        """
    )