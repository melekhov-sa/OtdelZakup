"""Add master_items and master_item_members tables.

Revision ID: 018
Revises: 017
Create Date: 2026-03-01
"""

import sqlalchemy as sa
from alembic import op

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS master_items (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            is_active BOOLEAN NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        )
    """))

    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS master_item_members (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            master_item_id INTEGER NOT NULL
                REFERENCES master_items(id) ON DELETE CASCADE,
            onec_guid TEXT NOT NULL,
            name_original TEXT,
            is_primary BOOLEAN NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL,
            CONSTRAINT uq_master_member_pair UNIQUE (master_item_id, onec_guid)
        )
    """))

    conn.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_master_item_members_guid "
        "ON master_item_members (onec_guid)"
    ))


def downgrade():
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_master_item_members_guid"))
    conn.execute(sa.text("DROP TABLE IF EXISTS master_item_members"))
    conn.execute(sa.text("DROP TABLE IF EXISTS master_items"))
