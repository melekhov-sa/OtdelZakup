"""Add sandbox_session and rule_version tables.

Revision ID: 007
Revises: 006
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sandbox_session",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        sa.Column("base_version", sa.String(200), nullable=True),
        sa.Column("rule_snapshot_json", sa.Text, nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="1"),
        sa.Column("is_applied", sa.Boolean, nullable=False, server_default="0"),
        sa.Column("last_file_id", sa.String(64), nullable=True),
    )
    op.create_table(
        "rule_version",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("description", sa.String(500), nullable=True),
        sa.Column("snapshot_json", sa.Text, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("sandbox_session")
    op.drop_table("rule_version")
