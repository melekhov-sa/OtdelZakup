"""Add condition_type and STANDARD_MATCH fields to validation_rule.

Revision ID: 006
Revises: 005
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("validation_rule") as batch_op:
        batch_op.add_column(sa.Column(
            "condition_type", sa.String(20), nullable=False, server_default="FIELDS_REQUIRED"
        ))
        batch_op.add_column(sa.Column(
            "standard_source", sa.String(10), nullable=False, server_default="ANY"
        ))
        batch_op.add_column(sa.Column(
            "expected_item_type_mode", sa.String(20), nullable=False, server_default="FROM_DIRECTORY"
        ))
        batch_op.add_column(sa.Column(
            "expected_item_type", sa.String(50), nullable=True
        ))


def downgrade() -> None:
    with op.batch_alter_table("validation_rule") as batch_op:
        batch_op.drop_column("condition_type")
        batch_op.drop_column("standard_source")
        batch_op.drop_column("expected_item_type_mode")
        batch_op.drop_column("expected_item_type")
