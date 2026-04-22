"""Seed default readiness rules and standards into the database."""

import json
import logging
from datetime import datetime, timezone

from app.database import get_db_session
from app.models import (
    BaseValidationRule,
    CoatingRule,
    InferenceRule,
    NameTemplate,
    NormalizationRule,
    ProductType,
    ReadinessRule,
    SizeRule,
    StandardRef,
    StrengthRule,
    SystemSetting,
    ValidationRuleException,
)

log = logging.getLogger(__name__)

_DEFAULTS = [
    ("По умолчанию", None, ["name", "qty", "uom"], 0,
     "Правило по умолчанию: наименование, количество и единица измерения"),
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


_DEFAULT_INFERENCE_RULES = [
    ("Гайка: размер = диаметр",   ["гайка"],  "DIAMETER_AS_SIZE",          10),
    ("Шайба: размер = диаметр",   ["шайба"],  "DIAMETER_AS_SIZE",          11),
    ("Болт: размер = MxL",        ["болт"],   "DIAMETER_X_LENGTH_AS_SIZE", 20),
    ("Винт: размер = MxL",        ["винт"],   "DIAMETER_X_LENGTH_AS_SIZE", 21),
    ("Анкер: размер = MxL",       ["анкер"],  "DIAMETER_X_LENGTH_AS_SIZE", 22),
]


def seed_default_inference_rules():
    """Insert default inference rules if the inference_rule table is empty."""
    session = get_db_session()
    try:
        if session.query(InferenceRule).count() > 0:
            return
        for name, item_types, mode, priority in _DEFAULT_INFERENCE_RULES:
            rule = InferenceRule(
                name=name,
                mode=mode,
                priority=priority,
                is_active=True,
                target_field="size",
            )
            rule.item_types_list = item_types
            session.add(rule)
        session.commit()
    finally:
        session.close()


def seed_default_template():
    """Insert default name template if the name_template table is empty."""
    session = get_db_session()
    try:
        if session.query(NameTemplate).count() > 0:
            return
        tpl = NameTemplate(
            name="Основной",
            template_string="{item_type} {size} {strength} {standard}",
            is_active=True,
            priority=1,
        )
        session.add(tpl)
        session.commit()
    finally:
        session.close()


# (primary_name, [aliases])
_DEFAULT_PRODUCT_TYPES: list[tuple[str, list[str]]] = [
    ("болт",                ["болта", "болты", "болтов"]),
    ("винт",                ["винта", "винты", "винтов"]),
    ("гайка",               ["гайки", "гаек", "гайке"]),
    ("шайба",               ["шайбы", "шайб"]),
    ("шпилька",             ["шпильки", "шпилек"]),
    ("анкер",               ["анкера", "анкеры", "анкеров"]),
    ("заклёпка",            ["заклепка", "заклёпки", "заклепки", "заклёпок"]),
    ("гвоздь",              ["гвоздя", "гвозди", "гвоздей"]),
    ("саморез",             ["самореза", "саморезы", "саморезов"]),
    ("шуруп",               ["шурупа", "шурупы", "шурупов"]),
    ("перфорированная лента", ["перфолента", "лента перфорированная"]),
    ("диск",                ["диска", "диски", "дисков"]),
    ("герметик",            ["герметика", "герметики"]),
    ("пена",                ["пены", "монтажная пена"]),
    ("пистолет",            ["пистолета", "пистолеты"]),
    ("очиститель",          ["очистителя", "очистители"]),
]


def seed_default_product_types():
    """Insert default product types if the product_type table is empty."""
    session = get_db_session()
    try:
        if session.query(ProductType).count() > 0:
            return
        now = datetime.now(timezone.utc)
        for name, aliases in _DEFAULT_PRODUCT_TYPES:
            session.add(ProductType(
                name=name,
                aliases_json=json.dumps(aliases, ensure_ascii=False),
                is_active=True,
                created_at=now,
                updated_at=now,
            ))
        session.commit()
    finally:
        session.close()


# ── Category-based validation rules seed ─────────────────────────────────────

def _jf(fields: list[str]) -> str:
    return json.dumps(fields, ensure_ascii=False)


# (category_code, category_name, subcategory_code, subcategory_name,
#  item_type_code, item_type_name, required_fields_json, priority)
_VALIDATION_RULES: list[tuple] = [
    # 1: Анкеры
    ("anchors", "Анкеры", None, None, None, None,
     _jf(["type", "diameter", "length"]), 100),
    # 2: Болты фундаментные
    ("foundation_bolts", "Болты фундаментные", None, None, None, None,
     _jf(["execution_type", "standard", "diameter", "length"]), 100),
    # 3: Дюбели
    ("dowels", "Дюбели", None, None, None, None,
     _jf(["name", "diameter", "length"]), 100),
    # 4: Гвозди
    ("nails", "Гвозди", None, None, None, None,
     _jf(["name", "diameter", "length"]), 100),
    # 5: Заклепки вытяжные
    ("rivets_blind", "Заклепки вытяжные", None, None, None, None,
     _jf(["material", "diameter", "length"]), 100),
    # 6: Заклепки резьбовые
    ("rivets_threaded", "Заклепки резьбовые", None, None, None, None,
     _jf(["shape", "flange_type", "diameter"]), 100),
    # 7: Мебельный крепеж — болты/винты/шурупы
    ("furniture_fasteners", "Мебельный крепеж", None, None,
     "bolt_screw", "Болты, винты, шурупы",
     _jf(["diameter", "length"]), 100),
    # 8: Мебельный крепеж — гайки
    ("furniture_fasteners", "Мебельный крепеж", None, None,
     "nut", "Гайки",
     _jf(["diameter"]), 100),
    # 9: Метрический крепеж — болты/винты/шпильки
    ("metric_fasteners", "Метрический крепеж", None, None,
     "bolt_screw_stud", "Болты, винты, шпильки",
     _jf(["standard", "strength_class", "coating", "diameter", "length"]), 100),
    # 10: Метрический крепеж — гайки
    ("metric_fasteners", "Метрический крепеж", None, None,
     "nut", "Гайки",
     _jf(["standard", "strength_class", "coating", "diameter"]), 100),
    # 11: Метрический крепеж — шайбы
    ("metric_fasteners", "Метрический крепеж", None, None,
     "washer", "Шайбы",
     _jf(["standard", "coating", "diameter"]), 100),
    # 12: Нержавеющая сталь — заклепки
    ("stainless_fasteners", "Нержавеющая сталь", None, None,
     "rivets", "Заклепки",
     _jf(["steel_grade", "diameter", "length"]), 100),
    # 13: Нержавеющая сталь — болты/винты/шпильки
    ("stainless_fasteners", "Нержавеющая сталь", None, None,
     "bolt_screw_stud", "Болты, винты, шпильки",
     _jf(["standard", "steel_grade", "diameter", "length"]), 100),
    # 14: Нержавеющая сталь — гайки
    ("stainless_fasteners", "Нержавеющая сталь", None, None,
     "nut", "Гайки",
     _jf(["standard", "steel_grade", "diameter"]), 100),
    # 15: Нержавеющая сталь — шайбы
    ("stainless_fasteners", "Нержавеющая сталь", None, None,
     "washer", "Шайбы",
     _jf(["standard", "steel_grade", "diameter"]), 100),
    # 16: Штифты и шплинты
    ("pins_cotter", "Штифты и шплинты", None, None, None, None,
     _jf(["standard", "steel_grade", "diameter", "length"]), 100),
    # 17: Саморезы DIN
    ("din_screws", "Саморезы DIN", None, None, None, None,
     _jf(["standard", "diameter", "length"]), 100),
    # 18: Саморезы и шурупы
    ("screws", "Саморезы и шурупы", None, None, None, None,
     _jf(["name", "diameter", "length"]), 100),
    # 19: Стяжки
    ("clamps_ties", "Стяжки и хомуты", "ties", "Стяжки", None, None,
     _jf(["material", "diameter", "length"]), 100),
    # 20: Хомуты
    ("clamps_ties", "Стяжки и хомуты", "clamps", "Хомуты", None, None,
     _jf(["name", "diameter"]), 100),
    # 21: Такелаж — грузоподъемные приспособления
    ("rigging", "Такелаж", "lifting", "Грузоподъемные приспособления", None, None,
     _jf(["name", "diameter", "load_capacity"]), 100),
    # 22: Такелаж — такелаж
    ("rigging", "Такелаж", "rigging", "Такелаж", None, None,
     _jf(["standard", "size"]), 100),
    # 23: Такелаж — цепи/тросы
    ("rigging", "Такелаж", "chains_ropes", "Цепи, тросы, шнуры", None, None,
     _jf(["standard", "diameter"]), 100),
    # 24: Фиксаторы арматуры
    ("rebar_fixators", "Фиксаторы арматуры", None, None, None, None,
     _jf(["type", "size"]), 100),
    # 25: Перфорированный крепеж — ленты
    ("perforated_fasteners", "Перфорированный крепеж", "tapes", "Ленты", None, None,
     _jf(["type", "thickness", "length", "width"]), 100),
    # 26: Перфорированный крепеж — опоры/держатели
    ("perforated_fasteners", "Перфорированный крепеж", "supports", "Опоры и держатели",
     None, None,
     _jf(["type", "size"]), 100),
    # 27: Перфорированный крепеж — пластины
    ("perforated_fasteners", "Перфорированный крепеж", "plates", "Пластины", None, None,
     _jf(["type", "size"]), 100),
    # 28: Перфорированный крепеж — монтажный профиль
    ("perforated_fasteners", "Перфорированный крепеж", "profile", "Монтажный профиль",
     None, None,
     _jf(["type", "diameter", "length"]), 100),
    # 29: Перфорированный крепеж — уголки
    ("perforated_fasteners", "Перфорированный крепеж", "angles", "Уголки", None, None,
     _jf(["type", "width", "length", "thickness"]), 100),
]

# (parent_rule_key, match_type_name, match_standard, override_fields_json, note)
# parent_rule_key = (category_code, subcategory_code, item_type_code)
_VALIDATION_EXCEPTIONS: list[tuple] = [
    # Анкер забиваемый стальной → только diameter
    (("anchors", None, None),
     "анкер забиваемый стальной", None,
     _jf(["diameter"]),
     "Анкер забиваемый стальной — только диаметр"),
    # Забивной анкер латунный → только diameter
    (("anchors", None, None),
     "забивной анкер латунный", None,
     _jf(["diameter"]),
     "Забивной анкер латунный — только диаметр"),
    # Дюбели с полукольцом KRHS → только diameter
    (("dowels", None, None),
     "дюбели с полукольцом", None,
     _jf(["diameter"]),
     "Дюбели с полукольцом KRHS — только диаметр"),
    # Дюбели с прямым крюком KRHP → только diameter
    (("dowels", None, None),
     "дюбели с прямым крюком", None,
     _jf(["diameter"]),
     "Дюбели с прямым крюком KRHP — только диаметр"),
    # Штифты DIN 11024 → только diameter
    (("pins_cotter", None, None),
     None, "DIN 11024",
     _jf(["diameter"]),
     "DIN 11024 — длина не требуется"),
    # Штифты DIN 94 → standard + diameter + length + coating
    (("pins_cotter", None, None),
     None, "DIN 94",
     _jf(["standard", "diameter", "length", "coating"]),
     "DIN 94 — дополнительно обязательно покрытие"),
]


def seed_initial_validation_rules():
    """Seed category-based validation rules and exceptions.

    Idempotent: checks by (category_code, subcategory_code, item_type_code)
    for rules and by (base_rule_id, match_type_name, match_standard) for exceptions.
    Does not duplicate existing records. Updates required_fields if rule exists
    but fields differ.
    """
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        created_rules = 0
        updated_rules = 0
        created_exceptions = 0

        # Build a lookup for rules by their unique key
        rule_id_map: dict[tuple, int] = {}

        for (cat_code, cat_name, sub_code, sub_name,
             type_code, type_name, req_fields_json, priority) in _VALIDATION_RULES:

            key = (cat_code, sub_code, type_code)

            # Check if rule already exists
            q = session.query(BaseValidationRule).filter(
                BaseValidationRule.category_code == cat_code,
            )
            if sub_code:
                q = q.filter(BaseValidationRule.subcategory_code == sub_code)
            else:
                q = q.filter(
                    (BaseValidationRule.subcategory_code.is_(None))
                    | (BaseValidationRule.subcategory_code == "")
                )
            if type_code:
                q = q.filter(BaseValidationRule.item_type_code == type_code)
            else:
                q = q.filter(
                    (BaseValidationRule.item_type_code.is_(None))
                    | (BaseValidationRule.item_type_code == "")
                )

            existing = q.first()

            if existing:
                # Check if required_fields need updating
                if existing.required_fields != req_fields_json:
                    existing.required_fields = req_fields_json
                    existing.priority = priority
                    existing.category_name = cat_name
                    if sub_name:
                        existing.subcategory_name = sub_name
                    if type_name:
                        existing.item_type_name = type_name
                    existing.updated_at = now
                    updated_rules += 1
                    display = existing.display_name
                    log.info("Updated rule: %s", display)
                rule_id_map[key] = existing.id
            else:
                rule = BaseValidationRule(
                    category_code=cat_code,
                    category_name=cat_name,
                    subcategory_code=sub_code,
                    subcategory_name=sub_name,
                    item_type_code=type_code,
                    item_type_name=type_name,
                    required_fields=req_fields_json,
                    priority=priority,
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                )
                session.add(rule)
                session.flush()
                rule_id_map[key] = rule.id
                created_rules += 1
                display = rule.display_name
                log.info("Created rule: %s", display)

        # Seed exceptions
        for (parent_key, match_type, match_std,
             override_json, note) in _VALIDATION_EXCEPTIONS:

            parent_id = rule_id_map.get(parent_key)
            if parent_id is None:
                log.warning("Parent rule not found for exception: %s", parent_key)
                continue

            # Check if exception already exists
            q = session.query(ValidationRuleException).filter(
                ValidationRuleException.base_rule_id == parent_id,
            )
            if match_type:
                q = q.filter(ValidationRuleException.match_type_name == match_type)
            else:
                q = q.filter(
                    (ValidationRuleException.match_type_name.is_(None))
                    | (ValidationRuleException.match_type_name == "")
                )
            if match_std:
                q = q.filter(ValidationRuleException.match_standard == match_std)
            else:
                q = q.filter(
                    (ValidationRuleException.match_standard.is_(None))
                    | (ValidationRuleException.match_standard == "")
                )

            if q.first():
                continue  # already exists

            exc = ValidationRuleException(
                base_rule_id=parent_id,
                match_type_name=match_type,
                match_standard=match_std,
                override_required_fields=override_json,
                note=note,
                priority=10,
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            session.add(exc)
            created_exceptions += 1
            label = match_type or match_std or "?"
            log.info("Created exception: %s", label)

        session.commit()

        log.info(
            "Seed validation rules: created=%d, updated=%d, exceptions=%d",
            created_rules, updated_rules, created_exceptions,
        )
        return {"created": created_rules, "updated": updated_rules,
                "exceptions": created_exceptions}
    finally:
        session.close()


# ── Coating rules seed ────────────────────────────────────────────────────────

# (pattern_raw, match_type, coating_code, coating_name, priority)
_COATING_RULES = [
    # Zinc / цинк
    ("zn",             "contains", "zinc", "цинк", 100),
    ("zinc",           "contains", "zinc", "цинк", 100),
    ("цинк",           "contains", "zinc", "цинк", 100),
    ("оцинк",          "contains", "zinc", "цинк", 100),
    ("оц",             "contains", "zinc", "цинк", 90),
    ("гальв",          "contains", "zinc", "цинк", 80),
    ("galvanic",       "contains", "zinc", "цинк", 80),
    ("galvanized",     "contains", "zinc", "цинк", 80),
    (".016",           "contains", "zinc", "цинк", 70),
    # No coating / без покрытия
    ("б/п",            "contains", "none", "без покрытия", 100),
    ("без покрытия",   "contains", "none", "без покрытия", 100),
    ("черн",           "contains", "none", "без покрытия", 90),
    # Stainless / нержавейка
    ("нерж",           "contains", "stainless", "нержавейка", 100),
    # Brass / латунь
    ("латун",          "contains", "brass", "латунь", 100),
    # Phosphate / фосфат
    ("фосфат",         "contains", "phosphate", "фосфат", 100),
    # Oxide / оксид
    ("оксид",          "contains", "oxide", "оксид", 100),
    ("ворон",          "contains", "oxide", "оксид", 100),
    # Nickel / никель
    ("никел",          "contains", "nickel", "никель", 100),
    # Chrome / хром
    ("хром",           "contains", "chrome", "хром", 100),
]


def seed_default_coating_rules():
    """Insert default coating rules if the coating_rule table is empty."""
    session = get_db_session()
    try:
        if session.query(CoatingRule).count() > 0:
            return
        now = datetime.now(timezone.utc)
        for pattern_raw, match_type, code, name, priority in _COATING_RULES:
            session.add(CoatingRule(
                pattern_raw=pattern_raw,
                match_type=match_type,
                coating_code=code,
                coating_name=name,
                priority=priority,
                is_active=True,
                created_at=now,
                updated_at=now,
            ))
        session.commit()
        log.info("Seed coating rules: created=%d", len(_COATING_RULES))
    finally:
        session.close()


# ── Strength rules seed ──────────────────────────────────────────────────────

# (pattern_raw, match_type, strength_code, strength_name, strength_family, priority)
_STRENGTH_RULES = [
    # Stainless — higher priority so they win over numeric patterns
    (r"(?<![a-zа-яё0-9])a2[\s-]70(?![a-zа-яё0-9])",  "regex", "A2-70",  "A2-70",  "stainless", 200),
    (r"(?<![a-zа-яё0-9])a2[\s-]80(?![a-zа-яё0-9])",  "regex", "A2-80",  "A2-80",  "stainless", 200),
    (r"(?<![a-zа-яё0-9])a4[\s-]70(?![a-zа-яё0-9])",  "regex", "A4-70",  "A4-70",  "stainless", 200),
    (r"(?<![a-zа-яё0-9])a4[\s-]80(?![a-zа-яё0-9])",  "regex", "A4-80",  "A4-80",  "stainless", 200),
    # Metric — "кл.пр." prefix forms (higher priority than bare numbers)
    (r"кл\.?\s*пр\.?\s*12[.,]9",   "regex", "12.9", "12.9", "metric", 150),
    (r"кл\.?\s*пр\.?\s*10[.,]9",   "regex", "10.9", "10.9", "metric", 150),
    (r"кл\.?\s*пр\.?\s*8[.,]8",    "regex", "8.8",  "8.8",  "metric", 150),
    (r"кл\.?\s*пр\.?\s*5[.,]8",    "regex", "5.8",  "5.8",  "metric", 150),
    (r"кл\.?\s*пр\.?\s*4[.,]6",    "regex", "4.6",  "4.6",  "metric", 150),
    (r"класс\s+(?:прочности\s+)?12[.,]9", "regex", "12.9", "12.9", "metric", 150),
    (r"класс\s+(?:прочности\s+)?10[.,]9", "regex", "10.9", "10.9", "metric", 150),
    (r"класс\s+(?:прочности\s+)?8[.,]8",  "regex", "8.8",  "8.8",  "metric", 150),
    (r"класс\s+(?:прочности\s+)?5[.,]8",  "regex", "5.8",  "5.8",  "metric", 150),
    (r"класс\s+(?:прочности\s+)?4[.,]6",  "regex", "4.6",  "4.6",  "metric", 150),
    # Metric — bare number forms (dot or comma)
    (r"(?<!\d)12[.,]9(?!\d)",  "regex", "12.9", "12.9", "metric", 100),
    (r"(?<!\d)10[.,]9(?!\d)",  "regex", "10.9", "10.9", "metric", 100),
    (r"(?<!\d)8[.,]8(?!\d)",   "regex", "8.8",  "8.8",  "metric", 100),
    (r"(?<!\d)5[.,]8(?!\d)",   "regex", "5.8",  "5.8",  "metric", 100),
    (r"(?<!\d)4[.,]6(?!\d)",   "regex", "4.6",  "4.6",  "metric", 100),
    # Encoded tail forms (.88, .109, .129)
    (r"\.129(?!\d)", "regex", "12.9", "12.9", "metric", 80),
    (r"\.109(?!\d)", "regex", "10.9", "10.9", "metric", 80),
    (r"\.88(?!\d)",  "regex", "8.8",  "8.8",  "metric", 80),
]


def seed_default_strength_rules():
    """Insert default strength rules if the strength_rule table is empty."""
    session = get_db_session()
    try:
        if session.query(StrengthRule).count() > 0:
            return
        now = datetime.now(timezone.utc)
        for pattern_raw, match_type, code, name, family, priority in _STRENGTH_RULES:
            session.add(StrengthRule(
                pattern_raw=pattern_raw,
                match_type=match_type,
                strength_code=code,
                strength_name=name,
                strength_family=family,
                priority=priority,
                is_active=True,
                created_at=now,
                updated_at=now,
            ))
        session.commit()
        log.info("Seed strength rules: created=%d", len(_STRENGTH_RULES))
    finally:
        session.close()


# ── Size rules seed ──────────────────────────────────────────────────────────

# (pattern_raw, match_type, size_kind, normalize_template, priority, note)
# Regex patterns use named groups: d=diameter, l=length, w=width, t=thickness, tol=tolerance, pitch=pitch
# Text is preprocessed: Cyrillic М→M, х→x, *→x, comma→dot before matching
_SIZE_RULES = [
    # Thread with tolerance: M20-7H, M16-6H
    (
        r"(?<![a-z0-9])m\s*(?P<d>\d{1,3})\s*-\s*(?P<tol>\d[hHgG])",
        "regex", "thread", "M{d}-{tol}", 250,
        "Метрическая резьба с допуском",
    ),
    # Triple size: 125x1.6x22 (discs, circles)
    (
        r"(?<![a-z0-9])(?P<d>\d{2,4}(?:\.\d+)?)\s*x\s*(?P<t>\d+(?:\.\d+)?)\s*x\s*(?P<w>\d+(?:\.\d+)?)",
        "regex", "triple_size", "{d}x{t}x{w}", 200,
        "Тройной размер (диск/круг)",
    ),
    # Metric diameter x length: M12x50, m 12 x 50
    (
        r"(?<![a-z0-9])m\s*(?P<d>\d{1,3}(?:\.\d+)?)\s*x\s*(?P<l>\d{1,4}(?:\.\d+)?)",
        "regex", "diameter_length", "M{d}x{l}", 180,
        "Метрический MxL",
    ),
    # Screw/self-tap size: 4.8x35, 6.3x64 (decimal diameter x length)
    (
        r"(?<![a-z0-9])(?P<d>\d{1,2}\.\d+)\s*x\s*(?P<l>\d{1,4}(?:\.\d+)?)",
        "regex", "diameter_length", "{d}x{l}", 170,
        "Саморез/шуруп dxL",
    ),
    # Plain NxL (no M prefix): 12x50, 5x70
    (
        r"(?<![a-z0-9.])(?P<d>\d{1,3})\s*x\s*(?P<l>\d{1,4})(?![0-9]*\s*x)",
        "regex", "diameter_length", "{d}x{l}", 150,
        "Размер NxL (без M)",
    ),
    # Metric diameter only: M12, m 12
    (
        r"(?<![a-z0-9])m\s*(?P<d>\d{1,3})(?![0-9x.])",
        "regex", "diameter", "M{d}", 120,
        "Метрический диаметр",
    ),
    # d/D prefix diameter: d12, D12
    (
        r"(?<![a-z0-9])d\s*(?P<d>\d{1,3})(?![0-9x.])",
        "regex", "diameter", "d{d}", 110,
        "Диаметр d/D",
    ),
    # Ø prefix diameter
    (
        r"\u00d8\s*(?P<d>\d{1,3}(?:\.\d+)?)",
        "regex", "diameter", "Ø{d}", 110,
        "Диаметр Ø",
    ),
]


def seed_default_size_rules():
    """Insert default size rules if the size_rule table is empty."""
    session = get_db_session()
    try:
        if session.query(SizeRule).count() > 0:
            return
        now = datetime.now(timezone.utc)
        for pattern_raw, match_type, kind, tpl, priority, note in _SIZE_RULES:
            session.add(SizeRule(
                pattern_raw=pattern_raw,
                match_type=match_type,
                size_kind=kind,
                normalize_template=tpl,
                priority=priority,
                is_active=True,
                note=note,
                created_at=now,
                updated_at=now,
            ))
        session.commit()
        log.info("Seed size rules: created=%d", len(_SIZE_RULES))
    finally:
        session.close()


# ── Unified normalization rules seed ─────────────────────────────────────────

# (rule_type, pattern_raw, match_type, normalized_code, normalized_name, extra_json, priority, note)
_NORMALIZATION_RULES: list[tuple] = [
    # ── Coating rules ────────────────────────────────────────────────────────
    ("coating", "zn",             "contains", "zinc", "цинк",           None, 100, None),
    ("coating", "zinc",           "contains", "zinc", "цинк",           None, 100, None),
    ("coating", "цинк",           "contains", "zinc", "цинк",           None, 100, None),
    ("coating", "оцинк",          "contains", "zinc", "цинк",           None, 100, None),
    ("coating", "оц",             "contains", "zinc", "цинк",           None, 90, None),
    ("coating", "гальв",          "contains", "zinc", "цинк",           None, 80, None),
    ("coating", "galvanic",       "contains", "zinc", "цинк",           None, 80, None),
    ("coating", "galvanized",     "contains", "zinc", "цинк",           None, 80, None),
    ("coating", ".016",           "contains", "zinc", "цинк",           None, 70, None),
    ("coating", "б/п",            "contains", "none", "без покрытия",   None, 100, None),
    ("coating", "без покрытия",   "contains", "none", "без покрытия",   None, 100, None),
    ("coating", "черн",           "contains", "none", "без покрытия",   None, 90, None),
    ("coating", "нерж",           "contains", "stainless", "нержавейка", None, 100, None),
    ("coating", "латун",          "contains", "brass", "латунь",        None, 100, None),
    ("coating", "фосфат",         "contains", "phosphate", "фосфат",    None, 100, None),
    ("coating", "оксид",          "contains", "oxide", "оксид",         None, 100, None),
    ("coating", "ворон",          "contains", "oxide", "оксид",         None, 100, None),
    ("coating", "никел",          "contains", "nickel", "никель",       None, 100, None),
    ("coating", "хром",           "contains", "chrome", "хром",         None, 100, None),

    # ── Strength rules ───────────────────────────────────────────────────────
    # Stainless (higher priority)
    ("strength", r"(?<![a-zа-яё0-9])a2[\s-]70(?![a-zа-яё0-9])", "regex", "A2-70", "A2-70",
     '{"family":"stainless"}', 200, None),
    ("strength", r"(?<![a-zа-яё0-9])a2[\s-]80(?![a-zа-яё0-9])", "regex", "A2-80", "A2-80",
     '{"family":"stainless"}', 200, None),
    ("strength", r"(?<![a-zа-яё0-9])a4[\s-]70(?![a-zа-яё0-9])", "regex", "A4-70", "A4-70",
     '{"family":"stainless"}', 200, None),
    ("strength", r"(?<![a-zа-яё0-9])a4[\s-]80(?![a-zа-яё0-9])", "regex", "A4-80", "A4-80",
     '{"family":"stainless"}', 200, None),
    # кл.пр. prefix forms
    ("strength", r"кл\.?\s*пр\.?\s*12[.,]9",  "regex", "12.9", "12.9",
     '{"family":"metric"}', 150, None),
    ("strength", r"кл\.?\s*пр\.?\s*10[.,]9",  "regex", "10.9", "10.9",
     '{"family":"metric"}', 150, None),
    ("strength", r"кл\.?\s*пр\.?\s*8[.,]8",   "regex", "8.8",  "8.8",
     '{"family":"metric"}', 150, None),
    ("strength", r"кл\.?\s*пр\.?\s*5[.,]8",   "regex", "5.8",  "5.8",
     '{"family":"metric"}', 150, None),
    ("strength", r"кл\.?\s*пр\.?\s*4[.,]6",   "regex", "4.6",  "4.6",
     '{"family":"metric"}', 150, None),
    # "класс прочности" prefix
    ("strength", r"класс\s+(?:прочности\s+)?12[.,]9", "regex", "12.9", "12.9",
     '{"family":"metric"}', 150, None),
    ("strength", r"класс\s+(?:прочности\s+)?10[.,]9", "regex", "10.9", "10.9",
     '{"family":"metric"}', 150, None),
    ("strength", r"класс\s+(?:прочности\s+)?8[.,]8",  "regex", "8.8",  "8.8",
     '{"family":"metric"}', 150, None),
    ("strength", r"класс\s+(?:прочности\s+)?5[.,]8",  "regex", "5.8",  "5.8",
     '{"family":"metric"}', 150, None),
    ("strength", r"класс\s+(?:прочности\s+)?4[.,]6",  "regex", "4.6",  "4.6",
     '{"family":"metric"}', 150, None),
    # Bare number forms
    ("strength", r"(?<!\d)12[.,]9(?!\d)", "regex", "12.9", "12.9",
     '{"family":"metric"}', 100, None),
    ("strength", r"(?<!\d)10[.,]9(?!\d)", "regex", "10.9", "10.9",
     '{"family":"metric"}', 100, None),
    ("strength", r"(?<!\d)8[.,]8(?!\d)",  "regex", "8.8",  "8.8",
     '{"family":"metric"}', 100, None),
    ("strength", r"(?<!\d)5[.,]8(?!\d)",  "regex", "5.8",  "5.8",
     '{"family":"metric"}', 100, None),
    ("strength", r"(?<!\d)4[.,]6(?!\d)",  "regex", "4.6",  "4.6",
     '{"family":"metric"}', 100, None),
    # Encoded tail forms
    ("strength", r"\.129(?!\d)", "regex", "12.9", "12.9",
     '{"family":"metric"}', 80, None),
    ("strength", r"\.109(?!\d)", "regex", "10.9", "10.9",
     '{"family":"metric"}', 80, None),
    ("strength", r"\.88(?!\d)",  "regex", "8.8",  "8.8",
     '{"family":"metric"}', 80, None),

    # ── Size rules ───────────────────────────────────────────────────────────
    # Thread with tolerance: M20-7H
    ("size", r"(?<![a-z0-9])m\s*(?P<d>\d{1,3})\s*-\s*(?P<tol>\d[hHgG])", "regex",
     "M{d}-{tol}", "M{d}-{tol}",
     '{"size_kind":"thread","normalize_template":"M{d}-{tol}"}', 250,
     "Метрическая резьба с допуском"),
    # Triple size: 125x1.6x22
    ("size", r"(?<![a-z0-9])(?P<d>\d{2,4}(?:\.\d+)?)\s*x\s*(?P<t>\d+(?:\.\d+)?)\s*x\s*(?P<w>\d+(?:\.\d+)?)",
     "regex", "{d}x{t}x{w}", "{d}x{t}x{w}",
     '{"size_kind":"triple_size","normalize_template":"{d}x{t}x{w}"}', 200,
     "Тройной размер (диск/круг)"),
    # Metric MxL: M12x50
    ("size", r"(?<![a-z0-9])m\s*(?P<d>\d{1,3}(?:\.\d+)?)\s*x\s*(?P<l>\d{1,4}(?:\.\d+)?)",
     "regex", "M{d}x{l}", "M{d}x{l}",
     '{"size_kind":"diameter_length","normalize_template":"M{d}x{l}"}', 180,
     "Метрический MxL"),
    # Screw dxL: 4.8x35
    ("size", r"(?<![a-z0-9])(?P<d>\d{1,2}\.\d+)\s*x\s*(?P<l>\d{1,4}(?:\.\d+)?)",
     "regex", "{d}x{l}", "{d}x{l}",
     '{"size_kind":"diameter_length","normalize_template":"{d}x{l}"}', 170,
     "Саморез/шуруп dxL"),
    # Plain NxL: 12x50
    ("size", r"(?<![a-z0-9.])(?P<d>\d{1,3})\s*x\s*(?P<l>\d{1,4})(?![0-9]*\s*x)",
     "regex", "{d}x{l}", "{d}x{l}",
     '{"size_kind":"diameter_length","normalize_template":"{d}x{l}"}', 150,
     "Размер NxL (без M)"),
    # Metric diameter only: M12
    ("size", r"(?<![a-z0-9])m\s*(?P<d>\d{1,3})(?![0-9x.])",
     "regex", "M{d}", "M{d}",
     '{"size_kind":"diameter","normalize_template":"M{d}"}', 120,
     "Метрический диаметр"),
    # d/D prefix diameter: d12
    ("size", r"(?<![a-z0-9])d\s*(?P<d>\d{1,3})(?![0-9x.])",
     "regex", "d{d}", "d{d}",
     '{"size_kind":"diameter","normalize_template":"d{d}"}', 110,
     "Диаметр d/D"),
    # Ø prefix diameter
    ("size", r"\u00d8\s*(?P<d>\d{1,3}(?:\.\d+)?)",
     "regex", "\u00d8{d}", "\u00d8{d}",
     '{"size_kind":"diameter","normalize_template":"\u00d8{d}"}', 110,
     "Диаметр \u00d8"),
]


def seed_default_normalization_rules():
    """Insert default normalization rules if the normalization_rules table is empty."""
    session = get_db_session()
    try:
        if session.query(NormalizationRule).count() > 0:
            return
        now = datetime.now(timezone.utc)
        for (rule_type, pattern_raw, match_type, code, name,
             extra_json, priority, note) in _NORMALIZATION_RULES:
            session.add(NormalizationRule(
                rule_type=rule_type,
                pattern_raw=pattern_raw,
                match_type=match_type,
                normalized_code=code,
                normalized_name=name,
                extra_json=extra_json,
                priority=priority,
                is_active=True,
                note=note,
                created_at=now,
                updated_at=now,
            ))
        session.commit()
        log.info("Seed normalization rules: created=%d", len(_NORMALIZATION_RULES))
    finally:
        session.close()


def seed_catalog_version() -> None:
    """Ensure system_setting['catalog_version'] exists so the MinHash fingerprint is stable.

    Without this row, get_catalog_version() always returns 0 and invalidation
    after catalog CRUD cannot change the fingerprint — the cache would never
    become stale, and (worse) the on-disk cache file would effectively
    co-exist with a different catalog state after edits.
    """
    session = get_db_session()
    try:
        if session.get(SystemSetting, "catalog_version") is not None:
            return
        session.add(SystemSetting(key="catalog_version", value="1"))
        session.commit()
    finally:
        session.close()
