"""Add import_attachment and import_parse_attempt tables.

Revision ID: 019
Revises: 018
Create Date: 2026-03-01
"""

import sqlalchemy as sa
from alembic import op

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS import_attachment (
            id           INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            file_id      TEXT    NOT NULL,
            filename     TEXT    NOT NULL DEFAULT '',
            mime_type    TEXT    NOT NULL DEFAULT '',
            storage_path TEXT    NOT NULL DEFAULT '',
            kind         TEXT    NOT NULL DEFAULT '',
            created_at   DATETIME NOT NULL
        )
    """))
    conn.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_import_attachment_file_id "
        "ON import_attachment (file_id)"
    ))

    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS import_parse_attempt (
            id            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            file_id       TEXT    NOT NULL,
            attachment_id INTEGER,
            method        TEXT    NOT NULL DEFAULT '',
            status        TEXT    NOT NULL DEFAULT '',
            rows_found    INTEGER NOT NULL DEFAULT 0,
            metrics_json  TEXT    NOT NULL DEFAULT '{}',
            error_text    TEXT,
            created_at    DATETIME NOT NULL
        )
    """))
    conn.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_import_parse_attempt_file_id "
        "ON import_parse_attempt (file_id)"
    ))


def downgrade():
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_import_parse_attempt_file_id"))
    conn.execute(sa.text("DROP TABLE IF EXISTS import_parse_attempt"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_import_attachment_file_id"))
    conn.execute(sa.text("DROP TABLE IF EXISTS import_attachment"))
