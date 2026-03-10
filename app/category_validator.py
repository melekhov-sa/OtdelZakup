"""Category-based validation engine.

Classifies a parsed row into a product category, finds the matching
BaseValidationRule, checks for exceptions (by type name or standard),
and returns a structured validation result.

Falls back gracefully: if no rule matches, returns None so the caller
can use the old validation logic.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from app.database import get_db_session
from app.models import BaseValidationRule, ValidationRuleException, VALIDATION_FIELD_LABELS

# Status display labels
_STATUS_LABELS = {
    "ok": "ОК",
    "needs_review": "Уточнить",
    "manual_required": "Заполнить вручную",
}


def format_missing_fields(fields: list[str]) -> str:
    """Format a list of missing field keys as a comma-separated Russian string."""
    if not fields:
        return ""
    return ", ".join(VALIDATION_FIELD_LABELS.get(f, f) for f in fields)


def status_label(status: str) -> str:
    """Return Russian label for a validation status code."""
    return _STATUS_LABELS.get(status, status)


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class CategoryValidationResult:
    category_name: str
    subcategory_name: str | None
    item_type_name: str | None
    rule_display_name: str
    exception_note: str | None
    required_fields: list[str]
    missing_fields: list[str]
    status: str  # "ok" | "needs_review" | "manual_required"

    @property
    def required_labels(self) -> list[str]:
        return [VALIDATION_FIELD_LABELS.get(f, f) for f in self.required_fields]

    @property
    def missing_labels(self) -> list[str]:
        return [VALIDATION_FIELD_LABELS.get(f, f) for f in self.missing_fields]


# ── Loading rules from DB ────────────────────────────────────────────────────

def load_base_rules() -> list[BaseValidationRule]:
    session = get_db_session()
    try:
        rules = (
            session.query(BaseValidationRule)
            .filter(BaseValidationRule.is_active.is_(True))
            .order_by(BaseValidationRule.priority.desc(), BaseValidationRule.id)
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def load_exceptions() -> list[ValidationRuleException]:
    session = get_db_session()
    try:
        excs = (
            session.query(ValidationRuleException)
            .filter(ValidationRuleException.is_active.is_(True))
            .order_by(ValidationRuleException.priority.desc(), ValidationRuleException.id)
            .all()
        )
        session.expunge_all()
        return excs
    finally:
        session.close()


# ── Row classification ────────────────────────────────────────────────────────

def classify_row(row_dict: dict) -> tuple[str | None, str | None, str | None]:
    """Classify a row into (category_code, subcategory_code, item_type_code).

    Uses item_type and raw name text to determine the product category.
    Returns (None, None, None) if no category can be determined.
    """
    item_type = (row_dict.get("item_type") or "").strip().lower()
    raw_name = (row_dict.get("name_raw") or row_dict.get("name") or "").strip().lower()
    standard_text = ""
    for k in ("gost", "din", "iso"):
        v = (row_dict.get(k) or "").strip()
        if v:
            standard_text = v
            break

    combined = f"{item_type} {raw_name}"

    # Foundation bolts — check before generic bolt
    if "фундаментн" in combined:
        return ("foundation_bolts", None, None)

    # Anchors
    if "анкер" in combined:
        return ("anchors", None, None)

    # Dowels
    if "дюбель" in combined:
        return ("dowels", None, None)

    # Threaded rivets
    if "заклеп" in combined and "резьбов" in combined:
        return ("rivets_threaded", None, None)

    # Blind rivets
    if "заклеп" in combined and ("вытяжн" in combined or "тяговая" in combined):
        return ("rivets_blind", None, None)

    # Generic rivets → blind by default
    if "заклеп" in combined:
        return ("rivets_blind", None, None)

    # Nails
    if "гвозд" in combined:
        return ("nails", None, None)

    # Pins & cotters
    if "штифт" in combined or "шплинт" in combined:
        return ("pins_cotter", None, None)

    # Ties
    if "стяжк" in combined:
        return ("clamps_ties", "ties", None)

    # Clamps
    if "хомут" in combined:
        return ("clamps_ties", "clamps", None)

    # Screws DIN (саморез + DIN standard)
    if ("саморез" in combined or "шуруп" in combined) and standard_text.upper().startswith("DIN"):
        return ("din_screws", None, None)

    # Generic screws
    if "саморез" in combined or "шуруп" in combined:
        return ("screws", None, None)

    # Stainless steel check
    is_stainless = any(w in combined for w in ["нержав", "a2", "a4", "aisi"])
    if not is_stainless:
        steel_grade = (row_dict.get("steel_grade") or "").lower()
        if any(w in steel_grade for w in ["a2", "a4", "aisi", "нерж"]):
            is_stainless = True

    # Perforated
    if "лент" in combined and "перфор" in combined:
        return ("perforated_fasteners", "tapes", None)
    if ("опора" in combined or "держатель" in combined) and "перфор" in combined:
        return ("perforated_fasteners", "supports", None)
    if "пластин" in combined and "перфор" in combined:
        return ("perforated_fasteners", "plates", None)
    if "профиль" in combined and "монтаж" in combined:
        return ("perforated_fasteners", "profile", None)
    if "уголок" in combined and "перфор" in combined:
        return ("perforated_fasteners", "angles", None)

    # Rigging
    if "грузоподъемн" in combined or "строп" in combined:
        return ("rigging", "lifting", None)
    if "цеп" in combined or "трос" in combined or "шнур" in combined:
        return ("rigging", "chains_ropes", None)
    if "такелаж" in combined or "карабин" in combined or "коуш" in combined:
        return ("rigging", "rigging", None)

    # Rebar spacers
    if "фиксатор" in combined and "арматур" in combined:
        return ("rebar_fixators", None, None)

    # Stainless variants of metric fasteners
    if is_stainless:
        if "болт" in combined or "винт" in combined or "шпильк" in combined:
            return ("stainless_fasteners", None, "bolt_screw_stud")
        if "гайк" in combined:
            return ("stainless_fasteners", None, "nut")
        if "шайб" in combined:
            return ("stainless_fasteners", None, "washer")
        if "заклеп" in combined:
            return ("stainless_fasteners", None, "rivets")
        # Generic stainless — treat as stainless fasteners
        return ("stainless_fasteners", None, None)

    # Metric fasteners (standard carbon steel)
    if "болт" in combined or "винт" in combined or "шпильк" in combined:
        return ("metric_fasteners", None, "bolt_screw_stud")
    if "гайк" in combined:
        return ("metric_fasteners", None, "nut")
    if "шайб" in combined:
        return ("metric_fasteners", None, "washer")

    # Furniture fasteners
    if "мебельн" in combined:
        if "гайк" in combined:
            return ("furniture_fasteners", None, "nut")
        return ("furniture_fasteners", None, "bolt_screw")

    return (None, None, None)


# ── Rule matching ─────────────────────────────────────────────────────────────

def find_matching_rule(
    category_code: str,
    subcategory_code: str | None,
    item_type_code: str | None,
    rules: list[BaseValidationRule],
) -> BaseValidationRule | None:
    """Find the best matching rule for the given classification.

    Scoring: more specific match = higher score.
    - category_code match: +1
    - subcategory_code match: +2
    - item_type_code match: +4
    """
    best: BaseValidationRule | None = None
    best_score = -1

    for rule in rules:
        if rule.category_code != category_code:
            continue

        score = 1

        # Subcategory matching
        if rule.subcategory_code and subcategory_code:
            if rule.subcategory_code == subcategory_code:
                score += 2
            else:
                continue  # subcategory mismatch — skip
        elif rule.subcategory_code and not subcategory_code:
            continue  # rule requires subcategory but we don't have one

        # Item type matching
        if rule.item_type_code and item_type_code:
            if rule.item_type_code == item_type_code:
                score += 4
            else:
                continue  # item_type mismatch — skip
        elif rule.item_type_code and not item_type_code:
            continue  # rule requires item_type but we don't have one

        if score > best_score:
            best_score = score
            best = rule

    return best


# ── Exception matching ────────────────────────────────────────────────────────

def _normalize_for_match(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def find_exception(
    rule_id: int,
    row_dict: dict,
    exceptions: list[ValidationRuleException],
) -> ValidationRuleException | None:
    """Find the best matching exception for the given rule and row."""
    raw_name = _normalize_for_match(row_dict.get("name_raw") or row_dict.get("name") or "")
    item_type = _normalize_for_match(row_dict.get("item_type") or "")
    combined_name = f"{item_type} {raw_name}"

    standard_text = ""
    for k in ("gost", "din", "iso"):
        v = (row_dict.get(k) or "").strip()
        if v:
            standard_text = v.upper()
            break
    # Also check raw text for standard
    if not standard_text:
        raw = (row_dict.get("name_raw") or row_dict.get("name") or "").upper()
        m = re.search(r"(?:DIN|ISO|ГОСТ)\s*[\dА-Яа-я][\d\-. ]*", raw)
        if m:
            standard_text = m.group(0).strip()

    best: ValidationRuleException | None = None
    best_score = -1

    for exc in exceptions:
        if exc.base_rule_id != rule_id:
            continue

        score = 0

        # Match by type name (substring search in combined name)
        if exc.match_type_name:
            pattern = _normalize_for_match(exc.match_type_name)
            if pattern in combined_name:
                score += 1
            else:
                continue  # type name doesn't match

        # Match by standard
        if exc.match_standard:
            exc_std = exc.match_standard.strip().upper()
            if exc_std in standard_text:
                score += 2
            else:
                continue  # standard doesn't match

        if not exc.match_type_name and not exc.match_standard:
            continue  # exception has no criteria — skip

        if score > best_score:
            best_score = score
            best = exc

    return best


# ── Field extraction mapping ─────────────────────────────────────────────────

def _get_field_value(row_dict: dict, field_key: str) -> str:
    """Get the value of a validation field from the row dict.

    Maps the canonical field names (type, standard, etc.) to the
    actual keys used in the row_dict from extractors.
    """
    # Direct mapping for fields that match extractor keys
    _FIELD_MAP = {
        "type": "item_type",
        "name": "name",
        "standard": None,  # special: check gost/din/iso
        "execution_type": "execution_type",
        "material": "material",
        "steel_grade": "steel_grade",
        "coating": "coating",
        "strength_class": "strength",
        "diameter": "diameter",
        "length": "length",
        "width": "width",
        "thickness": "thickness",
        "size": "size",
        "load_capacity": "load_capacity",
        "shape": "shape",
        "flange_type": "flange_type",
    }

    if field_key == "standard":
        # Any of gost/din/iso counts
        for k in ("gost", "din", "iso"):
            v = (row_dict.get(k) or "").strip()
            if v:
                return v
        return ""

    mapped = _FIELD_MAP.get(field_key, field_key)
    if mapped is None:
        return ""
    return (row_dict.get(mapped) or "").strip()


# ── Main validation function ─────────────────────────────────────────────────

def validate_row(
    row_dict: dict,
    rules: list[BaseValidationRule] | None = None,
    exceptions: list[ValidationRuleException] | None = None,
) -> CategoryValidationResult | None:
    """Validate a row against category-based rules.

    Returns None if the row cannot be classified (fallback to old logic).
    """
    if rules is None:
        rules = load_base_rules()
    if exceptions is None:
        exceptions = load_exceptions()

    if not rules:
        return None

    # Step 1: Classify
    cat_code, subcat_code, type_code = classify_row(row_dict)
    if cat_code is None:
        return None

    # Step 2: Find matching rule
    rule = find_matching_rule(cat_code, subcat_code, type_code, rules)
    if rule is None:
        return None

    # Step 3: Check exceptions
    exc = find_exception(rule.id, row_dict, exceptions)

    # Step 4: Determine required fields
    if exc:
        required = exc.override_required_fields_list
        exc_note = exc.note
    else:
        required = rule.required_fields_list
        exc_note = None

    # Step 5: Check which fields are missing
    missing = []
    for f in required:
        val = _get_field_value(row_dict, f)
        if not val:
            missing.append(f)

    # Step 6: Determine status
    if not missing:
        status = "ok"
    elif any(f in ("size", "diameter") for f in missing):
        status = "manual_required"
    else:
        status = "needs_review"

    return CategoryValidationResult(
        category_name=rule.category_name,
        subcategory_name=rule.subcategory_name,
        item_type_name=rule.item_type_name,
        rule_display_name=rule.display_name,
        exception_note=exc_note,
        required_fields=required,
        missing_fields=missing,
        status=status,
    )
