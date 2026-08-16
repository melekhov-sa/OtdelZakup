"""add comparison fields: external_ref, position unit/weight, quote pack size

Revision ID: 037
Revises: 036
Create Date: 2026-08-16
"""
from alembic import op
import sqlalchemy as sa

revision = "037"
down_revision = "036"
branch_labels = None
depends_on = None


def upgrade():
    # Link a comparison back to the 1C заявка that created it
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(sa.Column("external_ref", sa.String(100), nullable=True))
        batch_op.create_index("ix_orders_external_ref", ["external_ref"])

    # Reference unit and weight come from 1C and drive price conversion
    with op.batch_alter_table("order_items") as batch_op:
        batch_op.add_column(sa.Column("qty", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("unit", sa.String(50), nullable=True))
        batch_op.add_column(sa.Column("weight_kg", sa.Float(), nullable=True))

    # Pieces per package, read from the supplier's own line text
    with op.batch_alter_table("quote_lines") as batch_op:
        batch_op.add_column(sa.Column("pack_size", sa.Float(), nullable=True))


def downgrade():
    with op.batch_alter_table("quote_lines") as batch_op:
        batch_op.drop_column("pack_size")

    with op.batch_alter_table("order_items") as batch_op:
        batch_op.drop_column("weight_kg")
        batch_op.drop_column("unit")
        batch_op.drop_column("qty")

    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_index("ix_orders_external_ref")
        batch_op.drop_column("external_ref")
