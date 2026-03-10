"""Add unified normalization_rules table.

Revision ID: 030
Revises: 029
"""
from alembic import op
import sqlalchemy as sa

revision = "030"
down_revision = "029"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "normalization_rules",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("rule_type", sa.String(30), nullable=False, index=True),
        sa.Column("pattern_raw", sa.String(500), nullable=False),
        sa.Column("match_type", sa.String(20), nullable=False, server_default="contains"),
        sa.Column("normalized_code", sa.String(200), nullable=False),
        sa.Column("normalized_name", sa.String(200), nullable=False),
        sa.Column("extra_json", sa.Text(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_normalization_rules_type_priority", "normalization_rules",
                     ["rule_type", "priority"])


def downgrade():
    op.drop_index("ix_normalization_rules_type_priority", "normalization_rules")
    op.drop_table("normalization_rules")
