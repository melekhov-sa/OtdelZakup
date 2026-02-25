"""Seed default readiness rules and standards into the database."""

from app.database import get_db_session
from app.models import ReadinessRule, StandardRef

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


# (kind, code, title, item_type)
_DEFAULT_STANDARDS = [
    ("DIN", "931",  "Болт с неполной резьбой",    "болт"),
    ("DIN", "933",  "Болт с полной резьбой",       "болт"),
    ("DIN", "934",  "Гайка шестигранная",          "гайка"),
    ("DIN", "125",  "Шайба плоская",               "шайба"),
    ("DIN", "127",  "Шайба пружинная (гровер)",    "шайба"),
    ("ISO", "4017", "Болт с полной резьбой",       "болт"),
    ("ISO", "4014", "Болт с неполной резьбой",     "болт"),
    ("ISO", "4032", "Гайка шестигранная",          "гайка"),
    ("ISO", "7089", "Шайба плоская",               "шайба"),
    ("ISO", "7093", "Шайба плоская увеличенная",   "шайба"),
]


def seed_default_standards():
    """Insert default standards if the standard_ref table is empty."""
    session = get_db_session()
    try:
        if session.query(StandardRef).count() > 0:
            return
        for kind, code, title, item_type in _DEFAULT_STANDARDS:
            ref = StandardRef(
                standard_kind=kind,
                standard_code=code,
                title=title,
                item_type=item_type,
                is_active=True,
            )
            session.add(ref)
        session.commit()
    finally:
        session.close()
