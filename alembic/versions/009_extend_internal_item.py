"""Add name_full, parse_status, parse_reason to internal_item.

Revision ID: 009
Revises: 008
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "009"
down_revision: Union[str, None] = "008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.add_column(sa.Column("name_full", sa.String(500), nullable=True))
        batch_op.add_column(sa.Column("parse_status", sa.String(10), nullable=True))
        batch_op.add_column(sa.Column("parse_reason", sa.String(300), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.drop_column("parse_reason")
        batch_op.drop_column("parse_status")
        batch_op.drop_column("name_full")
