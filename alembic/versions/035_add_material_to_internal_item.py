"""add material to internal_item

Revision ID: 035
Revises: 034
Create Date: 2026-05-15
"""
from alembic import op
import sqlalchemy as sa

revision = "035"
down_revision = "034"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.add_column(sa.Column("material", sa.String(100), nullable=True))


def downgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.drop_column("material")
