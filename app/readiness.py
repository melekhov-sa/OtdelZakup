"""Readiness evaluation engine.

Loads active rules from the database and evaluates each DataFrame row
to determine its readiness status and missing fields.

Status sources (applied in order):
  1. Empty name → manual, "Пустое наименование"  (always)
  2. Readiness rules  → base status + missing-field reasons
     If no active readiness rules → base_status = "ok"
  3. Validation rules → can worsen status, add reasons
     If no active validation rules → skipped

Priority scale: ok < review < manual  (use worsen_status to combine).
"""

import logging
import re

import pandas as pd

from app.database import get_db_session
from app.display_labels import display_label
from app.extractors import EXTRACTORS, _RAW_COL_FIELDS, _concat_row, _str, extract_item_type
from app.models import ReadinessRule, StandardRef, ValidationRule

_log = logging.getLogger(__name__)

# ── Status ordering ───────────────────────────────────────────

_STATUS_ORDER: dict[str, int] = {"ok": 0, "review": 1, "manual": 2}


def worsen_status(current: str, new: str) -> str:
    """Return the worse of the two statuses (ok < review < manual)."""
    if _STATUS_ORDER.get(new, 0) > _STATUS_ORDER.get(current, 0):
        return new
    return current


def load_active_rules():
    """Load all active readiness rules, ordered by priority ASC."""
    session = get_db_session()
    try:
        rules = (
            session.query(ReadinessRule)
            .filter(ReadinessRule.is_active.is_(True))
            .order_by(ReadinessRule.priority.asc())
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def load_active_validation_rules():
    """Load all active validation rules, ordered by priority ASC."""
    session = get_db_session()
    try:
        rules = (
            session.query(ValidationRule)
            .filter(ValidationRule.is_active.is_(True))
            .order_by(ValidationRule.priority.asc())
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def _check_val_rule(row_dict: dict, vr) -> tuple[bool, list]:
    """Return (fired, reason_parts) for one validation rule against row_dict."""
    if vr.item_type and vr.item_type != row_dict.get("item_type", ""):
        return False, []
    reasons = []
    for f in vr.require_fields_list:
        if not row_dict.get(f, ""):
            reasons.append(f"отсутствует {display_label(f)}")
    for f in vr.forbid_fields_list:
        if row_dict.get(f, ""):
            reasons.append(f"запрещено: {display_label(f)}")
    return bool(reasons), reasons


def load_active_standards():
    """Load all active standard refs as a dict: (kind, code) -> StandardRef."""
    session = get_db_session()
    try:
        refs = session.query(StandardRef).filter(StandardRef.is_active.is_(True)).all()
        result = {}
        for r in refs:
            result[(r.standard_kind, r.standard_code)] = (r.item_type, r.title)
        return result
    finally:
        session.close()


def _parse_standard_to_kind_code(standard_str: str):
    """Parse a normalised standard string to (kind, code) or None.

    Accepts: 'DIN 931', 'ISO 4017', 'ГОСТ 7798-70', 'ГОСТ Р ИСО 4014'
    Returns (kind, code) tuple or None if unrecognised.
    """
    s = standard_str.strip()
    # DIN
    m = re.match(r"^DIN\s+(\S+)$", s, re.IGNORECASE)
    if m:
        return ("DIN", m.group(1))
    # ISO (standalone, not ГОСТ Р ИСО)
    m = re.match(r"^ISO\s+(\S+)$", s, re.IGNORECASE)
    if m:
        return ("ISO", m.group(1))
    # ГОСТ (plain, not ГОСТ Р ИСО)
    m = re.match(r"^ГОСТ\s+(\S+)$", s)
    if m:
        return ("GOST", m.group(1))
    # ГОСТ Р ИСО — look up as ISO
    m = re.match(r"^ГОСТ\s+Р\s+ИСО\s+(\S+)$", s)
    if m:
        return ("ISO", m.group(1))
    return None


def _find_matching_rule(item_type, rules):
    """Find the first rule matching item_type, then fallback to default (NULL)."""
    for rule in rules:
        if rule.item_type and rule.item_type == item_type:
            return rule
    for rule in rules:
        if rule.item_type is None:
            return rule
    return None


def _build_row_dict(text, transformed_row, original_row):
    """Build a dict of field_key -> value for readiness evaluation.

    Checks extractors on raw text, base columns from original row,
    and signal-merged columns from transformed row.
    """
    values = {}

    # Run each extractor on the concatenated row text
    for key, (_, func) in EXTRACTORS.items():
        if key not in _RAW_COL_FIELDS:
            values[key] = func(text)

    # Base columns from original row
    for col in ("name", "qty", "uom", "code"):
        if col in original_row.index:
            values[col] = _str(original_row[col]).strip()

    # Override with signal-merged values from transformed row (they may be better)
    for key, (display_name, _) in EXTRACTORS.items():
        if key in _RAW_COL_FIELDS:
            continue
        if display_name in transformed_row.index:
            merged = _str(transformed_row[display_name]).strip()
            if merged and not values.get(key):
                values[key] = merged

    return values


def _enrich_with_standards(row_dict: dict, standards_cache: dict) -> tuple[dict, list[str]]:
    """Enrich row_dict using StandardRef lookups.

    Returns (enriched_row_dict, extra_reasons).
    - If item_type is empty and a matching standard has item_type → fill it in
      and record item_type_source = "из стандарта".
    - If item_type is set but conflicts with standard → add a reason note and
      ensure status will be at least "review".
    """
    extra_reasons = []

    for std_key in ("gost", "iso", "din"):
        std_val = row_dict.get(std_key, "")
        if not std_val:
            continue
        parsed = _parse_standard_to_kind_code(std_val)
        if parsed is None:
            continue
        entry = standards_cache.get(parsed)
        if entry is None:
            continue
        ref_item_type, _ref_title = entry
        if not ref_item_type:
            continue

        current_item_type = row_dict.get("item_type", "")
        if not current_item_type:
            row_dict = dict(row_dict)
            row_dict["item_type"] = ref_item_type
            row_dict["item_type_source"] = "из стандарта"
        elif current_item_type != ref_item_type:
            extra_reasons.append(
                f"Тип изделия не совпадает со стандартом {std_val}: ожидалось «{ref_item_type}»"
            )
        # Use first matching standard found
        break

    return row_dict, extra_reasons


def evaluate_readiness(row_dict, rules):
    """Evaluate a single row against readiness rules.

    Returns (status, missing_fields_keys, applied_rule_name).
    """
    item_type = row_dict.get("item_type", "")
    rule = _find_matching_rule(item_type, rules)

    if rule is None:
        return ("manual", [], "")

    required = rule.require_fields_list
    missing = []
    for field_key in required:
        val = row_dict.get(field_key, "")
        if not val or not str(val).strip():
            missing.append(field_key)

    if not missing:
        return ("ok", [], rule.name)

    if "size" in missing or "qty" in missing or "uom" in missing:
        return ("manual", missing, rule.name)

    return ("review", missing, rule.name)


def apply_readiness(df_original, df_transformed, rules=None, standards_cache=None):
    """Apply readiness and validation rules to a transformed DataFrame.

    Always overwrites 'status' and 'reason' columns.  Status sources:
      1. Empty name → manual, "Пустое наименование"
      2. Readiness rules (if any active) → base status + missing-field reasons
         No active readiness rules → base_status = "ok"
      3. Validation rules (if any active) → may worsen status, add reasons
    """
    if rules is None:
        rules = load_active_rules()
    if standards_cache is None:
        standards_cache = load_active_standards()

    val_rules = load_active_validation_rules()
    readiness_disabled = len(rules) == 0

    statuses = []
    reasons = []
    item_type_sources = []
    autofilled_item_types = []

    for idx in df_transformed.index:
        original_row = df_original.loc[idx] if idx in df_original.index else pd.Series()
        text = _concat_row(original_row) if len(original_row) > 0 else ""
        transformed_row = df_transformed.loc[idx]

        row_dict = _build_row_dict(text, transformed_row, original_row)
        row_dict, extra_reasons = _enrich_with_standards(row_dict, standards_cache)

        src = row_dict.get("item_type_source", "из текста" if row_dict.get("item_type") else "")
        item_type_sources.append(src)
        autofilled_item_types.append(
            row_dict.get("item_type", "") if src == "из стандарта" else None
        )

        # ── 1. Empty name check (applies regardless of rules) ────────────
        name_val = str(row_dict.get("name", "")).strip()
        if not name_val or name_val in ("—", "-"):
            statuses.append("manual")
            reasons.append("Пустое наименование")
            continue

        # ── 2. Readiness base status ──────────────────────────────────────
        reason_parts: list[str] = []
        if readiness_disabled:
            status = "ok"
        else:
            status, missing, _rule_name = evaluate_readiness(row_dict, rules)
            if extra_reasons and status == "ok":
                status = "review"
            if missing:
                labels = [display_label(f) for f in missing]
                reason_parts.append("Не хватает: " + ", ".join(labels))
            reason_parts.extend(extra_reasons)

        # ── 3. Validation rules ───────────────────────────────────────────
        for vr in val_rules:
            fired, vr_reasons = _check_val_rule(row_dict, vr)
            if fired:
                reason_parts.extend(vr_reasons)
                if vr.force_status:
                    status = worsen_status(status, vr.force_status)

        # ── Safety net: non-ok status must have a reason ─────────────────
        if status != "ok" and not reason_parts:
            reason_parts.append(
                "Требуется проверка: причина не определена (ошибка настройки)"
            )
            _log.warning(
                "Row %s: non-ok status %r with no reason — check rule configuration",
                idx, status,
            )

        statuses.append(status)
        reasons.append("; ".join(reason_parts))

    df_transformed["status"] = statuses
    df_transformed["reason"] = reasons
    df_transformed["item_type_source"] = item_type_sources

    # Write back autofilled item_type into display column (Тип изделия) where applicable
    item_type_col = "Тип изделия"
    if item_type_col in df_transformed.columns:
        for i, idx in enumerate(df_transformed.index):
            val = autofilled_item_types[i]
            if val is not None:
                df_transformed.at[idx, item_type_col] = val

    return df_transformed
