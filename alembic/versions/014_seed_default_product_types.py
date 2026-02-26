"""Seed default product types — inserts missing entries only (idempotent).

Revision ID: 014
Revises: 013
Create Date: 2026-02-26
"""
import json
from datetime import datetime, timezone
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = '014'
down_revision: Union[str, None] = '013'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DEFAULTS = [
    ("болт",                  ["болта", "болты", "болтов"]),
    ("винт",                  ["винта", "винты", "винтов"]),
    ("гайка",                 ["гайки", "гаек", "гайке"]),
    ("шайба",                 ["шайбы", "шайб"]),
    ("шпилька",               ["шпильки", "шпилек"]),
    ("анкер",                 ["анкера", "анкеры", "анкеров"]),
    ("заклёпка",              ["заклепка", "заклёпки", "заклепки", "заклёпок"]),
    ("гвоздь",                ["гвоздя", "гвозди", "гвоздей"]),
    ("саморез",               ["самореза", "саморезы", "саморезов"]),
    ("шуруп",                 ["шурупа", "шурупы", "шурупов"]),
    ("перфорированная лента", ["перфолента", "лента перфорированная"]),
    ("диск",                  ["диска", "диски", "дисков"]),
    ("герметик",              ["герметика", "герметики"]),
    ("пена",                  ["пены", "монтажная пена"]),
    ("пистолет",              ["пистолета", "пистолеты"]),
    ("очиститель",            ["очистителя", "очистители"]),
]


def upgrade() -> None:
    bind = op.get_bind()
    now = datetime.now(timezone.utc)

    # Fetch names that already exist to avoid unique-constraint error
    existing = {
        row[0]
        for row in bind.execute(sa.text("SELECT name FROM product_type"))
    }

    rows_to_insert = [
        {
            "name": name,
            "aliases_json": json.dumps(aliases, ensure_ascii=False),
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }
        for name, aliases in _DEFAULTS
        if name not in existing
    ]

    if rows_to_insert:
        bind.execute(
            sa.text(
                "INSERT INTO product_type (name, aliases_json, is_active, created_at, updated_at) "
                "VALUES (:name, :aliases_json, :is_active, :created_at, :updated_at)"
            ),
            rows_to_insert,
        )


def downgrade() -> None:
    # Remove only the defaults we added; leave user-created types untouched
    names = [name for name, _ in _DEFAULTS]
    bind = op.get_bind()
    bind.execute(
        sa.text(f"DELETE FROM product_type WHERE name IN ({','.join(':n'+str(i) for i in range(len(names)))})" ),
        {f"n{i}": n for i, n in enumerate(names)},
    )
