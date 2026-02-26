"""Inference engine — computes missing fields from other extracted data.

Pipeline position: after extractors, before readiness evaluation.
Policy: inference only fires when the target field is empty (never overrides).
"""
from __future__ import annotations


def load_active_inference_rules() -> list:
    """Load all active InferenceRules ordered by priority ASC."""
    from app.database import get_db_session  # lazy
    from app.models import InferenceRule  # lazy

    session = get_db_session()
    try:
        rules = (
            session.query(InferenceRule)
            .filter(InferenceRule.is_active.is_(True))
            .order_by(InferenceRule.priority.asc())
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def apply_inference(row_dict: dict, rules: list) -> tuple[dict, dict]:
    """Apply inference rules to row_dict, returning (updated_dict, trace).

    Rules are tried in priority order; the first matching rule wins.
    Only fires when the target field is currently empty.

    trace keys:
      applied       – bool
      applied_rule  – str (if applied)
      target_field  – field name
      field_before  – value before inference (empty string when applied)
      field_after   – computed value (if applied)
      mode          – mode string (if applied)
      result_size   – alias for field_after (backwards-compat with view_result.html)
      reason        – human-readable explanation
    """
    target_field = "size"  # only size supported currently
    field_before = (row_dict.get(target_field) or "").strip()

    if field_before:
        return row_dict, {
            "applied": False,
            "target_field": target_field,
            "field_before": field_before,
            "reason": "поле уже заполнено из текста",
        }

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
                row_dict = dict(row_dict)
                row_dict[target_field] = diameter
                return row_dict, {
                    "applied": True,
                    "applied_rule": rule.name,
                    "applied_rule_id": rule.id,
                    "target_field": target_field,
                    "field_before": "",
                    "field_after": diameter,
                    "mode": mode,
                    "result_size": diameter,
                    "reason": f"размер = диаметр ({diameter})",
                }
        elif mode == "DIAMETER_X_LENGTH_AS_SIZE":
            if diameter and length:
                new_size = f"{diameter}x{length}"
                row_dict = dict(row_dict)
                row_dict[target_field] = new_size
                return row_dict, {
                    "applied": True,
                    "applied_rule": rule.name,
                    "applied_rule_id": rule.id,
                    "target_field": target_field,
                    "field_before": "",
                    "field_after": new_size,
                    "mode": mode,
                    "result_size": new_size,
                    "reason": f"размер = диаметр × длина ({new_size})",
                }

    return row_dict, {
        "applied": False,
        "target_field": target_field,
        "field_before": "",
        "reason": "нет подходящего правила вычисления",
    }
