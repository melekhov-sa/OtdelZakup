"""Seed default readiness rules into the database."""

from app.database import get_db_session
from app.models import ReadinessRule

_DEFAULTS = [
    ("По умолчанию", None, ["name", "qty"], 0,
     "Правило по умолчанию: наименование и количество"),
    ("Шайба", "шайба", ["size", "qty"], 10,
     "Шайба: размер и количество"),
    ("Гайка", "гайка", ["size", "qty"], 10,
     "Гайка: размер и количество"),
    ("Болт", "болт", ["size", "qty"], 10,
     "Болт: размер и количество"),
    ("Винт", "винт", ["size", "qty"], 10,
     "Винт: размер и количество"),
    ("Саморез", "саморез", ["size", "qty"], 10,
     "Саморез: размер и количество"),
]


def seed_default_rules():
    """Insert default rules if the readiness_rule table is empty."""
    session = get_db_session()
    try:
        if session.query(ReadinessRule).count() > 0:
            return
        for name, item_type, fields, priority, desc in _DEFAULTS:
            rule = ReadinessRule(
                name=name,
                description=desc,
                item_type=item_type,
                priority=priority,
                is_active=True,
            )
            rule.require_fields_list = fields
            session.add(rule)
        session.commit()
    finally:
        session.close()
