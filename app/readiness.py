"""Readiness evaluation engine.

Loads active rules from the database and evaluates each DataFrame row
to determine its readiness status and missing fields.
"""

import pandas as pd

from app.database import get_db_session
from app.display_labels import display_label
from app.extractors import EXTRACTORS, _RAW_COL_FIELDS, _concat_row, _str, extract_item_type
from app.models import ReadinessRule


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
    for col in ("name", "qty", "code"):
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

    if "size" in missing or "qty" in missing:
        return ("manual", missing, rule.name)

    return ("review", missing, rule.name)


def apply_readiness(df_original, df_transformed, rules=None):
    """Apply readiness rules to a transformed DataFrame (post-processing).

    Overwrites 'status' column and adds 'reason' column.
    The 'confidence' column from transform_dataframe is preserved.
    """
    if rules is None:
        rules = load_active_rules()

    if not rules:
        if "reason" not in df_transformed.columns:
            df_transformed["reason"] = ""
        return df_transformed

    statuses = []
    reasons = []

    for idx in df_transformed.index:
        original_row = df_original.loc[idx] if idx in df_original.index else pd.Series()
        text = _concat_row(original_row) if len(original_row) > 0 else ""
        transformed_row = df_transformed.loc[idx]

        row_dict = _build_row_dict(text, transformed_row, original_row)
        status, missing, _rule_name = evaluate_readiness(row_dict, rules)

        statuses.append(status)
        if missing:
            labels = [display_label(f) for f in missing]
            reasons.append("Не хватает: " + ", ".join(labels))
        else:
            reasons.append("")

    df_transformed["status"] = statuses
    df_transformed["reason"] = reasons
    return df_transformed
