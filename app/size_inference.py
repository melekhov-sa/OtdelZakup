"""Size inference rules engine.

Applies active SizeInferenceRule records to fill in a missing `size` field
based on already-extracted diameter and/or length values.

This module uses lazy imports to avoid circular dependency chains.
"""
from __future__ import annotations


def load_active_inference_rules():
    """Load active size inference rules ordered by priority."""
    from app.database import get_db_session  # lazy
    from app.models import SizeInferenceRule  # lazy

    session = get_db_session()
    try:
        rules = (
            session.query(SizeInferenceRule)
            .filter(SizeInferenceRule.is_active.is_(True))
            .order_by(SizeInferenceRule.priority.asc())
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def apply_size_inference(row_dict: dict, rules: list) -> tuple[dict, dict]:
    """Apply size inference rules if the `size` field is missing.

    Rules are tried in priority order; the first matching rule wins.

    Returns (updated_row_dict, inference_trace).

    inference_trace keys:
      applied         – bool: True if a rule fired
      applied_rule    – str: rule name (only when applied=True)
      applied_rule_id – int (only when applied=True)
      mode            – str (only when applied=True)
      result_size     – str (only when applied=True)
      reason          – str: human-readable explanation
    """
    if row_dict.get("size"):
        return row_dict, {"applied": False, "reason": "размер уже заполнен из текста"}

    item_type = (row_dict.get("item_type") or "").lower().strip()
    diameter = (row_dict.get("diameter") or "").strip()
    length = (row_dict.get("length") or "").strip()

    for rule in rules:
        item_types = rule.item_types_list
        if item_types and item_type not in [t.lower() for t in item_types]:
            continue

        mode = rule.mode
        if mode == "DIAMETER_AS_SIZE":
            if diameter:
                new_size = diameter
                row_dict = dict(row_dict)
                row_dict["size"] = new_size
                return row_dict, {
                    "applied": True,
                    "applied_rule": rule.name,
                    "applied_rule_id": rule.id,
                    "mode": mode,
                    "result_size": new_size,
                    "reason": f"размер = диаметр ({diameter})",
                }
        elif mode == "DIAMETER_X_LENGTH":
            if diameter and length:
                new_size = f"{diameter}x{length}"
                row_dict = dict(row_dict)
                row_dict["size"] = new_size
                return row_dict, {
                    "applied": True,
                    "applied_rule": rule.name,
                    "applied_rule_id": rule.id,
                    "mode": mode,
                    "result_size": new_size,
                    "reason": f"размер = диаметр × длина ({diameter}x{length})",
                }

    return row_dict, {"applied": False, "reason": "нет подходящего правила для вывода размера"}
