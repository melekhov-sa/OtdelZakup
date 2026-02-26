"""Add standard_key + aliases_json to standard_ref; standard_key to internal_item.

Revision ID: 011
Revises: 010
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("standard_ref") as batch_op:
        batch_op.add_column(sa.Column("standard_key", sa.String(120), nullable=True))
        batch_op.add_column(sa.Column("aliases_json", sa.Text, nullable=True))

    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.add_column(sa.Column("standard_key", sa.String(120), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.drop_column("standard_key")

    with op.batch_alter_table("standard_ref") as batch_op:
        batch_op.drop_column("aliases_json")
        batch_op.drop_column("standard_key")
