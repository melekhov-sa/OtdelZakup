"""DB-backed product type matching.

Used by extract_item_type() and run_column_scorer() to find the best
matching product type from the managed product_type directory.

Matching priority: exact word match (\\b…\\b) > substring match.
Case-insensitive throughout.
"""
from __future__ import annotations

import re
from typing import Optional


def load_active_product_types() -> list:
    """Load all active ProductType ORM objects from DB."""
    from app.database import get_db_session  # noqa: PLC0415
    from app.models import ProductType  # noqa: PLC0415

    db = get_db_session()
    try:
        return db.query(ProductType).filter_by(is_active=True).all()
    finally:
        db.close()


def get_product_type_words(types=None) -> set[str]:
    """Return all active product type names and aliases as lowercase strings."""
    if types is None:
        types = load_active_product_types()
    words: set[str] = set()
    for pt in types:
        words.add(pt.name.lower())
        for alias in pt.aliases:
            words.add(alias.lower())
    return words


def get_item_types_for_ui() -> list[str]:
    """Return sorted list of item type names for UI dropdowns.

    Combines active ProductType names with target_item_type values from active
    KEYWORD_TO_ITEM_TYPE inference rules so that inference-derived subtypes
    (e.g. 'болт фундаментальный') appear as selectable options in rule forms.
    Falls back to an empty list on any DB error.
    """
    import json as _json

    try:
        from app.database import get_db_session  # noqa: PLC0415
        from app.models import InferenceRule  # noqa: PLC0415

        types = load_active_product_types()
        names: set[str] = {pt.name for pt in types}

        db = get_db_session()
        try:
            inf_rules = (
                db.query(InferenceRule)
                .filter_by(is_active=True, mode="KEYWORD_TO_ITEM_TYPE")
                .all()
            )
            for rule in inf_rules:
                try:
                    cond = _json.loads(rule.conditions_json or "{}")
                    t = cond.get("target_item_type", "").strip()
                    if t:
                        names.add(t)
                except (ValueError, TypeError):
                    pass
        finally:
            db.close()

        return sorted(names)
    except Exception:
        return []


def match_product_type(text: str, types=None) -> str:
    """Find the best matching product type in *text*.

    Pass 1: exact whole-word match (case-insensitive) — returns primary name.
    Pass 2: substring match — returns primary name.
    Returns "" when no type matches.

    *types* — pre-loaded list of ProductType objects (avoids per-call DB query).
    """
    if types is None:
        types = load_active_product_types()

    text_lower = text.lower()

    # Pass 1: exact word boundary match
    for pt in types:
        for term in [pt.name] + list(pt.aliases):
            pattern = r"\b" + re.escape(term.lower()) + r"\b"
            if re.search(pattern, text_lower):
                return pt.name

    # Pass 2: substring match (no word boundaries)
    for pt in types:
        for term in [pt.name] + list(pt.aliases):
            if term.lower() in text_lower:
                return pt.name

    return ""
