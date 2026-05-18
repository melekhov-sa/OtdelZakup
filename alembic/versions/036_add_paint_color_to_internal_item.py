"""add paint_color to internal_item

Revision ID: 036
Revises: 035
Create Date: 2026-05-16
"""
from alembic import op
import sqlalchemy as sa

revision = "036"
down_revision = "035"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.add_column(sa.Column("paint_color", sa.String(100), nullable=True))


def downgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.drop_column("paint_color")
