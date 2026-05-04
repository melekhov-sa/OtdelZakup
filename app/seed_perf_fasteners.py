"""Seed perforated / mounting hardware: product types, inference rules, readiness rules."""

import json

from app.database import get_db_session
from app.models import InferenceRule, ProductType, ReadinessRule

_PRODUCT_TYPES = [
    ("уголок монтажный",       ["уголок", "уголки", "монтажный уголок", "угол монтажный"]),
    ("пластина соединительная",["пластина", "пластины", "пластину", "накладка соединительная"]),
    ("опора балки",            ["опора", "опоры", "балочная опора", "опора стропил", "держатель балки"]),
    ("профиль монтажный",      ["профиль перфорированный", "монтажный профиль", "перфопрофиль", "профиль"]),
    ("держатель",              ["держатели", "держателем", "балкодержатель"]),
]

# (rule_name, keyword, item_types_filter, subtype, priority)
_INFERENCE_RULES = [
    # Перфорированная лента
    ("лента усиленная",              "усиленн",        ["перфорированная лента"], "лента усиленная",              10),
    ("лента волнообразная",          "волнообразн",    ["перфорированная лента"], "лента волнообразная",          11),

    # Уголок монтажный
    ("уголок усиленный",             "усиленн",        ["уголок монтажный"],      "уголок усиленный",             20),
    ("уголок для стропил",           "стропил",        ["уголок монтажный"],      "уголок для стропил",           21),
    ("уголок перфорированный",       "перфорирован",   ["уголок монтажный"],      "уголок перфорированный",       22),
    ("уголок равнополочный",         "равнополочн",    ["уголок монтажный"],      "уголок равнополочный",         23),

    # Пластина соединительная
    ("пластина накладная",           "накладн",        ["пластина соединительная"], "пластина накладная",         30),
    ("пластина стыковая",            "стыков",         ["пластина соединительная"], "пластина стыковая",          31),
    ("пластина зубчатая",            "зубчат",         ["пластина соединительная"], "пластина зубчатая",          32),

    # Опора балки
    ("опора балки открытая",         "открыт",         ["опора балки"],            "опора балки открытая",        40),
    ("опора балки закрытая",         "закрыт",         ["опора балки"],            "опора балки закрытая",        41),
    ("опора балки регулируемая",     "регулируем",     ["опора балки"],            "опора балки регулируемая",    42),
    ("опора для стропил",            "стропил",        ["опора балки"],            "опора для стропил",           43),

    # Профиль монтажный
    ("профиль С-образный",           "С-образн",       ["профиль монтажный"],      "профиль С-образный",          50),
    ("профиль П-образный",           "П-образн",       ["профиль монтажный"],      "профиль П-образный",          51),
    ("профиль перфорированный",      "перфорирован",   ["профиль монтажный"],      "профиль перфорированный",     52),
]

# (name, item_type, item_subtype, require_fields, priority)
_READINESS_RULES = [
    # Перфорированная лента
    ("Лента монтажная — общее",          "перфорированная лента", None,                      ["size", "qty", "uom"], 10),
    ("Лента усиленная",                  "перфорированная лента", "лента усиленная",          ["size", "qty", "uom"],  5),

    # Уголок монтажный
    ("Уголок монтажный — общее",         "уголок монтажный",      None,                      ["size", "qty", "uom"], 10),
    ("Уголок усиленный",                 "уголок монтажный",      "уголок усиленный",         ["size", "qty", "uom"],  5),
    ("Уголок для стропил",               "уголок монтажный",      "уголок для стропил",       ["size", "qty", "uom"],  5),

    # Пластина соединительная
    ("Пластина соединительная — общее",  "пластина соединительная", None,                    ["size", "qty", "uom"], 10),
    ("Пластина накладная",               "пластина соединительная", "пластина накладная",     ["size", "qty", "uom"],  5),
    ("Пластина стыковая",                "пластина соединительная", "пластина стыковая",      ["size", "qty", "uom"],  5),

    # Опора балки
    ("Опора балки — общее",              "опора балки",           None,                      ["size", "qty", "uom"], 10),
    ("Опора балки открытая",             "опора балки",           "опора балки открытая",     ["size", "qty", "uom"],  5),
    ("Опора балки закрытая",             "опора балки",           "опора балки закрытая",     ["size", "qty", "uom"],  5),
    ("Опора балки регулируемая",         "опора балки",           "опора балки регулируемая", ["size", "qty", "uom"],  5),

    # Профиль монтажный
    ("Профиль монтажный — общее",        "профиль монтажный",     None,                      ["size", "qty", "uom"], 10),

    # Держатель
    ("Держатель — общее",                "держатель",             None,                      ["size", "qty", "uom"], 10),
]


def seed_perf_fasteners() -> None:
    session = get_db_session()
    try:
        # Product types
        for name, aliases in _PRODUCT_TYPES:
            if not session.query(ProductType).filter_by(name=name).first():
                pt = ProductType(name=name, is_active=True)
                pt.aliases = aliases
                session.add(pt)

        # Inference rules
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

        # Readiness rules
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
