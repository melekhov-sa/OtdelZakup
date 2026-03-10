"""Add classification and qty fields to quote_lines.

- line_class: item/header/total/requisites/garbage
- filter_reason: human-readable reason for classification
- qty: quantity extracted from the quote line

Revision ID: 023
Revises: 022
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("quote_lines") as batch_op:
        batch_op.add_column(sa.Column("line_class", sa.String(20), nullable=True))
        batch_op.add_column(sa.Column("filter_reason", sa.String(200), nullable=True))
        batch_op.add_column(sa.Column("qty", sa.Float, nullable=True))


def downgrade():
    with op.batch_alter_table("quote_lines") as batch_op:
        batch_op.drop_column("qty")
        batch_op.drop_column("filter_reason")
        batch_op.drop_column("line_class")
