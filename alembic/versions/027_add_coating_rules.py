"""Add coating_rule table for DB-backed coating detection.

Revision ID: 027
Revises: 026
Create Date: 2026-03-06
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "027"
down_revision: Union[str, None] = "026"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "coating_rule",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pattern_raw", sa.String(200), nullable=False),
        sa.Column("match_type", sa.String(20), nullable=False, server_default="contains"),
        sa.Column("coating_code", sa.String(50), nullable=False),
        sa.Column("coating_name", sa.String(200), nullable=False),
        sa.Column("priority", sa.Integer, nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("1")),
        sa.Column("note", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_coating_rule_active_priority", "coating_rule", ["is_active", "priority"])


def downgrade() -> None:
    op.drop_index("ix_coating_rule_active_priority", table_name="coating_rule")
    op.drop_table("coating_rule")
