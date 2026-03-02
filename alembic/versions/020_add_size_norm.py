"""add size_norm column to internal_item

Adds a pre-computed normalized size field (e.g. "M24X50") for efficient
MinHash token generation and strict size filtering.

Revision ID: 020
Revises: 019
Create Date: 2026-03-02
"""
import re

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


# ── Inline normalization (mirrors app.matching.normalizer.normalize_size) ─────
# Implemented here to avoid importing app code during Alembic migrations.

_CYR_TO_LAT = str.maketrans("МмХх", "MmXx")


def _normalize_size(size: str) -> str:
    """Canonical uppercase size for backfill — must stay in sync with normalize_size()."""
    if not size:
        return ""
    s = size.strip()
    s = s.translate(_CYR_TO_LAT)
    s = s.replace("\u00d7", "X")
    s = s.upper()
    s = re.sub(r"M[\s\-]+(\d)", r"M\1", s)
    s = s.replace(",", ".")
    s = re.sub(r"\s*MM\s*$", "", s)
    s = re.sub(r"\s*X\s*", "X", s)
    s = s.replace(" ", "")
    return s


def upgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.add_column(sa.Column("size_norm", sa.String(100), nullable=True))

    # Create index manually after adding column
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.create_index("ix_internal_item_size_norm", ["size_norm"])

    # Backfill existing rows
    conn = op.get_bind()
    rows = conn.execute(
        text("SELECT id, size FROM internal_item WHERE size IS NOT NULL AND size != ''")
    ).fetchall()
    for row in rows:
        norm = _normalize_size(row[1])
        if norm:
            conn.execute(
                text("UPDATE internal_item SET size_norm = :norm WHERE id = :id"),
                {"norm": norm, "id": row[0]},
            )


def downgrade():
    with op.batch_alter_table("internal_item") as batch_op:
        batch_op.drop_index("ix_internal_item_size_norm")
        batch_op.drop_column("size_norm")
