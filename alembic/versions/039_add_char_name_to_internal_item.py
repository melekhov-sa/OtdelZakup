"""store the characteristic name on catalog items

A characteristic in the customer's 1C is a distinct nomenclature variant —
often a coating, but not always, so it cannot be derived from the parsed
``coating`` field. The name arrives in the sync payload; until now it was
only glued onto the match text and kept nowhere, so it could not be handed
back to 1C alongside the matched item.

Existing rows stay empty until the catalog is re-imported.

Revision ID: 039
Revises: 038
Create Date: 2026-08-20
"""
from alembic import op
import sqlalchemy as sa

revision = "039"
down_revision = "038"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.add_column(sa.Column("char_name", sa.String(300), nullable=True))


def downgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.drop_column("char_name")
