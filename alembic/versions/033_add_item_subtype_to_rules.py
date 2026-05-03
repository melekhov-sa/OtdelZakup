"""Add item_subtype to readiness_rule and validation_rule.

Revision ID: 033
Revises: 032
"""
from alembic import op
import sqlalchemy as sa

revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("readiness_rule") as batch_op:
        batch_op.add_column(sa.Column("item_subtype", sa.String(100), nullable=True))

    with op.batch_alter_table("validation_rule") as batch_op:
        batch_op.add_column(sa.Column("item_subtype", sa.String(100), nullable=True))


def downgrade():
    with op.batch_alter_table("readiness_rule") as batch_op:
        batch_op.drop_column("item_subtype")

    with op.batch_alter_table("validation_rule") as batch_op:
        batch_op.drop_column("item_subtype")
