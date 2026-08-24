"""add supplementary quantity to quote lines

Suppliers price in one unit and restate the amount in another —
"15 кг" alongside "1 152,00 шт (справочно)". That restatement is an exact
per-line conversion factor, better than the averaged weight in our catalog.

Revision ID: 038
Revises: 037
Create Date: 2026-08-20
"""
from alembic import op
import sqlalchemy as sa

revision = "038"
down_revision = "037"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("quote_lines") as batch_op:
        batch_op.add_column(sa.Column("ref_qty", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("ref_unit", sa.String(50), nullable=True))


def downgrade():
    with op.batch_alter_table("quote_lines") as batch_op:
        batch_op.drop_column("ref_unit")
        batch_op.drop_column("ref_qty")
