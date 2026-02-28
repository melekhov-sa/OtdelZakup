"""Add 1C sync support: NomenclatureFolder table + 1C sync fields on InternalItem.

Revision ID: 016
Revises: 015
Create Date: 2026-02-28
"""

import sqlalchemy as sa
from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_tables = inspector.get_table_names()
    existing_item_cols = {c["name"] for c in inspector.get_columns("internal_item")}

    # ── New table: nomenclature_folder ─────────────────────────────────────
    if "nomenclature_folder" not in existing_tables:
        op.create_table(
            "nomenclature_folder",
            sa.Column("folder_uid",  sa.String(100), primary_key=True),
            sa.Column("folder_name", sa.String(300), nullable=False, server_default=""),
            sa.Column("parent_uid",  sa.String(100), nullable=True),
            sa.Column("folder_path", sa.String(500), nullable=False, server_default=""),
            sa.Column("priority",    sa.Integer,     nullable=True),
            sa.Column("updated_at",  sa.DateTime,    nullable=True),
        )

    # ── New columns on internal_item ────────────────────────────────────────
    if "uid_1c" not in existing_item_cols:
        op.add_column("internal_item", sa.Column("uid_1c",      sa.String(100), nullable=True))
    if "uid_1c_char" not in existing_item_cols:
        op.add_column("internal_item", sa.Column("uid_1c_char", sa.String(100), nullable=True))
    if "folder_uid" not in existing_item_cols:
        op.add_column("internal_item", sa.Column("folder_uid",  sa.String(100), nullable=True))
    if "folder_name" not in existing_item_cols:
        op.add_column("internal_item", sa.Column("folder_name", sa.String(300), nullable=True))
    if "folder_path" not in existing_item_cols:
        op.add_column("internal_item", sa.Column("folder_path", sa.String(500), nullable=True))
    if "folder_priority" not in existing_item_cols:
        op.add_column("internal_item", sa.Column("folder_priority", sa.Integer, nullable=True))

    # Index (ignore if already exists)
    existing_indexes = {i["name"] for i in inspector.get_indexes("internal_item")}
    if "ix_internal_item_uid_1c" not in existing_indexes:
        op.create_index("ix_internal_item_uid_1c", "internal_item", ["uid_1c"])


def downgrade():
    op.drop_index("ix_internal_item_uid_1c", table_name="internal_item")
    op.drop_column("internal_item", "folder_priority")
    op.drop_column("internal_item", "folder_path")
    op.drop_column("internal_item", "folder_name")
    op.drop_column("internal_item", "folder_uid")
    op.drop_column("internal_item", "uid_1c_char")
    op.drop_column("internal_item", "uid_1c")
    op.drop_table("nomenclature_folder")
