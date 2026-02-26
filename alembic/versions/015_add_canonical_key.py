"""Add canonical_key to internal_item and backfill existing rows.

Revision ID: 015
Revises: 014
Create Date: 2026-02-26
"""

import re

import sqlalchemy as sa
from alembic import op

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


# ── Inline canonicalization (avoids importing app code in migration) ───────────
# Must stay in sync with app/matching/canonicalize.py and normalizer.py.

def _normalize_size_inline(size: str) -> str:
    if not size:
        return ""
    s = size.strip().translate(str.maketrans("МмХх", "MmXx"))
    s = s.replace("\u00d7", "x")
    s = re.sub(r"([Mm])\s+(\d)", r"\1\2", s)
    s = s.lower().replace(",", ".")
    s = re.sub(r"\s*мм\s*$", "", s)
    s = re.sub(r"\s*mm\s*$", "", s)
    s = re.sub(r"\s*x\s*", "x", s).replace(" ", "")
    return s


def _parse_tokens_inline(norm: str) -> list:
    s = re.sub(r"^m", "", norm.lower())
    try:
        return sorted(float(n) for n in re.findall(r"\d+(?:\.\d+)?", s))
    except ValueError:
        return []


def _compute_ck(item_type, size, standard_key) -> str:
    parts = []
    t = (item_type or "").strip().lower()
    if t:
        parts.append(f"type={t}")
    sk = (standard_key or "").strip()
    if sk:
        parts.append(f"std={sk}")
    toks = _parse_tokens_inline(_normalize_size_inline(size or ""))
    if toks:
        parts.append("size=" + "x".join(f"{v:g}" for v in toks))
    return "|".join(parts)


# ── Migration ──────────────────────────────────────────────────────────────────

def upgrade():
    op.add_column(
        "internal_item",
        sa.Column("canonical_key", sa.String(500), nullable=True),
    )
    op.create_index("ix_internal_item_canonical_key", "internal_item", ["canonical_key"])

    # Backfill canonical_key for all existing rows
    conn = op.get_bind()
    rows = conn.execute(
        sa.text("SELECT id, item_type, size, standard_key FROM internal_item")
    ).fetchall()
    for row in rows:
        ck = _compute_ck(row[1], row[2], row[3])
        conn.execute(
            sa.text("UPDATE internal_item SET canonical_key = :ck WHERE id = :id"),
            {"ck": ck, "id": row[0]},
        )


def downgrade():
    op.drop_index("ix_internal_item_canonical_key", table_name="internal_item")
    op.drop_column("internal_item", "canonical_key")
