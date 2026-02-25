"""Create standard_ref table.

Revision ID: 002
Revises: 001
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "standard_ref",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("standard_kind", sa.String(10), nullable=False),
        sa.Column("standard_code", sa.String(100), nullable=False),
        sa.Column("title", sa.String(300), nullable=True),
        sa.Column("item_type", sa.String(50), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("standard_kind", "standard_code", name="uq_standard_ref_kind_code"),
    )


def downgrade() -> None:
    op.drop_table("standard_ref")
