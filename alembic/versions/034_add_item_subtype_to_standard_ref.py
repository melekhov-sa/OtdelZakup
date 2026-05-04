"""add item_subtype to standard_ref

Revision ID: 034
Revises: 033
Create Date: 2026-05-04
"""
from alembic import op
import sqlalchemy as sa

revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("standard_ref") as batch_op:
        batch_op.add_column(sa.Column("item_subtype", sa.String(100), nullable=True))


def downgrade():
    with op.batch_alter_table("standard_ref") as batch_op:
        batch_op.drop_column("item_subtype")
