"""Inference engine — computes missing fields from other extracted data.

Pipeline position: after extractors, before readiness evaluation.

Two independent passes per row:
  Pass 1 (KEYWORD_TO_ITEM_TYPE): reclassify item_type based on name keywords.
           Fires even when item_type is already set (overrides parent type).
  Pass 2 (size modes): compute size from diameter/length.
           Only fires when size is empty.
In each pass the first matching rule (by priority) wins.
"""
from __future__ import annotations

import json
import re


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

    trace keys:
      applied       – bool
      applied_rule  – str (if applied)
      target_field  – field name
      field_before  – value before inference
      field_after   – computed value (if applied)
      mode          – mode string (if applied)
      result_size   – alias for field_after when target is size (backwards-compat)
      reason        – human-readable explanation
    """
    trace: dict = {
        "applied": False,
        "target_field": "size",
        "field_before": (row_dict.get("size") or "").strip(),
        "reason": "нет подходящего правила вычисления",
    }

    # ── Pass 1: KEYWORD_TO_ITEM_TYPE (reclassification) ───────────────────
    item_type = (row_dict.get("item_type") or "").lower().strip()
    for rule in rules:
        if rule.mode != "KEYWORD_TO_ITEM_TYPE":
            continue
        item_types_filter = rule.item_types_list
        if item_types_filter and item_type not in [t.lower() for t in item_types_filter]:
            continue
        try:
            cond = json.loads(rule.conditions_json or "{}")
        except (ValueError, TypeError):
            continue
        keyword = cond.get("keyword", "").strip()
        target_item_type = cond.get("target_item_type", "").strip()
        if not keyword or not target_item_type:
            continue
        name = (row_dict.get("name") or "").lower()
        if re.search(re.escape(keyword.lower()), name):
            row_dict = dict(row_dict)
            row_dict["item_subtype"] = target_item_type
            trace = {
                "applied": True,
                "applied_rule": rule.name,
                "applied_rule_id": rule.id,
                "target_field": "item_subtype",
                "field_before": "",
                "field_after": target_item_type,
                "mode": rule.mode,
                "result_size": None,
                "reason": f"подтип → «{target_item_type}» (ключевое слово: «{keyword}»)",
            }
            break  # first match wins

    # ── Pass 2: size modes (only when size is empty) ──────────────────────
    size_before = (row_dict.get("size") or "").strip()
    if not size_before:
        item_type = (row_dict.get("item_type") or "").lower().strip()  # may have been updated
        diameter = (row_dict.get("diameter") or "").strip()
        length = (row_dict.get("length") or "").strip()

        for rule in rules:
            if rule.mode not in ("DIAMETER_AS_SIZE", "DIAMETER_X_LENGTH_AS_SIZE"):
                continue
            item_types_filter = rule.item_types_list
            if item_types_filter and item_type not in [t.lower() for t in item_types_filter]:
                continue

            if rule.mode == "DIAMETER_AS_SIZE" and diameter:
                row_dict = dict(row_dict)
                row_dict["size"] = diameter
                trace = {
                    "applied": True,
                    "applied_rule": rule.name,
                    "applied_rule_id": rule.id,
                    "target_field": "size",
                    "field_before": "",
                    "field_after": diameter,
                    "mode": rule.mode,
                    "result_size": diameter,
                    "reason": f"размер = диаметр ({diameter})",
                }
                break
            elif rule.mode == "DIAMETER_X_LENGTH_AS_SIZE" and diameter and length:
                new_size = f"{diameter}x{length}"
                row_dict = dict(row_dict)
                row_dict["size"] = new_size
                trace = {
                    "applied": True,
                    "applied_rule": rule.name,
                    "applied_rule_id": rule.id,
                    "target_field": "size",
                    "field_before": "",
                    "field_after": new_size,
                    "mode": rule.mode,
                    "result_size": new_size,
                    "reason": f"размер = диаметр × длина ({new_size})",
                }
                break

    return row_dict, trace
