"""Seed anchors and dowels: product type, inference rules, readiness rules."""

import json

from app.database import get_db_session
from app.models import InferenceRule, ProductType, ReadinessRule

_PRODUCT_TYPES = [
    ("дюбель", ["дюбеля", "дюбелей", "дюбелем"]),
]

# (rule_name, keyword, item_types_filter, subtype, priority)
_INFERENCE_RULES = [
    # Анкеры
    ("анкер клиновой",        "клиновой",    ["анкер"],   "анкер клиновой",        10),
    ("анкер химический",      "химический",  ["анкер"],   "анкер химический",      11),
    ("анкер забивной",        "забивной",    ["анкер"],   "анкер забивной",        12),
    ("анкер рамный",          "рамный",      ["анкер"],   "анкер рамный",          13),
    ("анкер потолочный",      "потолочный",  ["анкер"],   "анкер потолочный",      14),
    ("анкер для газобетона",  "газобетон",   ["анкер"],   "анкер для газобетона",  15),
    ("анкер с гайкой",        "с гайкой",    ["анкер"],   "анкер с гайкой",        16),
    # Дюбели
    ("дюбель-гвоздь",             "дюбель-гвоздь",  ["дюбель"],  "дюбель-гвоздь",             20),
    ("дюбель распорный",          "распорный",      ["дюбель"],  "дюбель распорный",          21),
    ("дюбель рамный",             "рамный",         ["дюбель"],  "дюбель рамный",             22),
    ("дюбель металлический",      "металлический",  ["дюбель"],  "дюбель металлический",      23),
    ("дюбель для газобетона",     "газобетон",      ["дюбель"],  "дюбель для газобетона",     24),
    ("дюбель для пустотелых",     "пустотел",       ["дюбель"],  "дюбель для пустотелых",     25),
    ("дюбель универсальный",      "универсальный",  ["дюбель"],  "дюбель универсальный",      26),
    ("дюбель KRHS",               "KRHS",           ["дюбель"],  "дюбель KRHS",               27),
    ("дюбель KRHP",               "KRHP",           ["дюбель"],  "дюбель KRHP",               28),
]

# (name, item_type, item_subtype, require_fields, priority)
_READINESS_RULES = [
    ("Анкер — общее",          "анкер",   None,                    ["size", "qty", "uom"], 10),
    ("Анкер клиновой",         "анкер",   "анкер клиновой",        ["size", "qty", "uom"],  5),
    ("Анкер химический",       "анкер",   "анкер химический",      ["size", "qty", "uom"],  5),
    ("Анкер забивной",         "анкер",   "анкер забивной",        ["size", "qty", "uom"],  5),
    ("Анкер рамный",           "анкер",   "анкер рамный",          ["size", "qty", "uom"],  5),
    ("Анкер для газобетона",   "анкер",   "анкер для газобетона",  ["size", "qty", "uom"],  5),
    ("Дюбель — общее",         "дюбель",  None,                    ["size", "qty", "uom"], 10),
    ("Дюбель-гвоздь",          "дюбель",  "дюбель-гвоздь",        ["size", "qty", "uom"],  5),
    ("Дюбель распорный",       "дюбель",  "дюбель распорный",      ["size", "qty", "uom"],  5),
    ("Дюбель рамный",          "дюбель",  "дюбель рамный",         ["size", "qty", "uom"],  5),
    ("Дюбель металлический",   "дюбель",  "дюбель металлический",  ["size", "qty", "uom"],  5),
]


def seed_anchors_dowels() -> None:
    session = get_db_session()
    try:
        # Product types
        for name, aliases in _PRODUCT_TYPES:
            if not session.query(ProductType).filter_by(name=name).first():
                pt = ProductType(name=name, is_active=True)
                pt.aliases = aliases
                session.add(pt)

        # Inference rules (by name)
        existing_inf = {r.name for r in session.query(InferenceRule).all()}
        for rule_name, keyword, item_types, subtype, priority in _INFERENCE_RULES:
            if rule_name not in existing_inf:
                rule = InferenceRule(
                    name=rule_name,
                    mode="KEYWORD_TO_ITEM_TYPE",
                    target_field="item_type",
                    priority=priority,
                    is_active=True,
                    conditions_json=json.dumps(
                        {"keyword": keyword, "target_item_type": subtype},
                        ensure_ascii=False,
                    ),
                )
                rule.item_types_list = item_types
                session.add(rule)

        # Readiness rules (by name)
        existing_rd = {r.name for r in session.query(ReadinessRule).all()}
        for name, item_type, item_subtype, fields, priority in _READINESS_RULES:
            if name not in existing_rd:
                r = ReadinessRule(
                    name=name,
                    item_type=item_type,
                    item_subtype=item_subtype,
                    priority=priority,
                    is_active=True,
                )
                r.require_fields_list = fields
                session.add(r)

        session.commit()
    finally:
        session.close()
