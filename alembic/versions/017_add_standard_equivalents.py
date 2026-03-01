"""Add standard_equivalents table with seed data.

Revision ID: 017
Revises: 016
Create Date: 2026-03-01
"""

import sqlalchemy as sa
from alembic import op

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None

# Well-known equivalent standard pairs (src_canonical, dst_canonical, confidence)
_SEED = [
    ("GOST-7798-70",  "DIN-933",   95),   # Болт с полной резьбой
    ("DIN-933",       "ISO-4017",  95),
    ("GOST-5927-70",  "DIN-934",   95),   # Гайка шестигранная
    ("DIN-934",       "ISO-4032",  95),
    ("GOST-11371-78", "DIN-125",   90),   # Шайба плоская
    ("DIN-125",       "ISO-7089",  90),
    ("GOST-6402-70",  "DIN-127",   90),   # Шайба пружинная (Гровера)
    ("GOST-7796-70",  "DIN-931",   90),   # Болт с неполной резьбой
    ("DIN-931",       "ISO-4014",  90),
    ("GOST-15589-70", "DIN-7998",  85),   # Саморез по дереву
    ("GOST-7808-70",  "DIN-965",   85),   # Саморез потайной
]


def upgrade():
    conn = op.get_bind()

    # Table may already exist if init_db() ran before this migration
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS standard_equivalents (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            src_canonical VARCHAR(120) NOT NULL,
            dst_canonical VARCHAR(120) NOT NULL,
            confidence INTEGER NOT NULL DEFAULT 100,
            is_active BOOLEAN NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            CONSTRAINT uq_std_equiv_pair UNIQUE (src_canonical, dst_canonical)
        )
    """))
    conn.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_std_equiv_src ON standard_equivalents (src_canonical)"
    ))
    conn.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_std_equiv_dst ON standard_equivalents (dst_canonical)"
    ))

    now = "2026-03-01 00:00:00"
    for src, dst, conf in _SEED:
        conn.execute(
            sa.text(
                "INSERT OR IGNORE INTO standard_equivalents "
                "(src_canonical, dst_canonical, confidence, is_active, created_at, updated_at) "
                "VALUES (:src, :dst, :conf, 1, :now, :now)"
            ),
            {"src": src, "dst": dst, "conf": conf, "now": now},
        )


def downgrade():
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_std_equiv_src"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_std_equiv_dst"))
    conn.execute(sa.text("DROP TABLE IF EXISTS standard_equivalents"))
