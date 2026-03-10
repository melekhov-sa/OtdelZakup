"""Add size_rule table for DB-backed size/diameter/length detection.

Revision ID: 029
Revises: 028
Create Date: 2026-03-06
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "029"
down_revision: Union[str, None] = "028"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "size_rule",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pattern_raw", sa.String(500), nullable=False),
        sa.Column("match_type", sa.String(20), nullable=False, server_default="regex"),
        sa.Column("size_kind", sa.String(50), nullable=False),
        sa.Column("normalize_template", sa.String(200), nullable=True),
        sa.Column("priority", sa.Integer, nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("1")),
        sa.Column("note", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_size_rule_active_priority", "size_rule", ["is_active", "priority"])


def downgrade() -> None:
    op.drop_index("ix_size_rule_active_priority", table_name="size_rule")
    op.drop_table("size_rule")
