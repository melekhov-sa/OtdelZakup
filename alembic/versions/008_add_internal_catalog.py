"""Add internal_item and supplier_internal_match tables.

Revision ID: 008
Revises: 007
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "008"
down_revision: Union[str, None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "internal_item",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("item_type", sa.String(50), nullable=True),
        sa.Column("size", sa.String(50), nullable=True),
        sa.Column("diameter", sa.String(30), nullable=True),
        sa.Column("length", sa.String(30), nullable=True),
        sa.Column("standard_text", sa.String(100), nullable=True),
        sa.Column("strength_class", sa.String(30), nullable=True),
        sa.Column("material_coating", sa.String(100), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )
    op.create_table(
        "supplier_internal_match",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("internal_item_id", sa.Integer, nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        sa.UniqueConstraint("fingerprint", name="uq_sim_fingerprint"),
    )


def downgrade() -> None:
    op.drop_table("supplier_internal_match")
    op.drop_table("internal_item")
