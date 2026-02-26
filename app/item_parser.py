"""Service: parse a single internal catalog item name using the existing pipeline.

Reuses the same extractors + inference + standards enrichment as the supplier
row processing, so behaviour is identical and deterministic.
"""
from __future__ import annotations

import pandas as pd


def parse_internal_item_name(name_full: str) -> dict:
    """Parse a full item name string into structured fields.

    Runs the text through:
      1. All field extractors (same as transform_dataframe)
      2. Inference rules (e.g. size = diameter for nuts)
      3. Standards enrichment (may fill item_type from standards DB)

    Returns dict with keys:
        item_type, size, diameter, length, standard_text,
        strength_class, material_coating, parse_status, parse_reason
    """
    from app.extractors import ALL_FIELD_KEYS, transform_dataframe
    from app.inference_engine import apply_inference, load_active_inference_rules
    from app.readiness import (
        _build_row_dict,
        _enrich_with_standards,
        load_active_standards,
    )

    text = (name_full or "").strip()
    if not text:
        return {
            "item_type": "", "size": "", "diameter": "", "length": "",
            "standard_text": "", "strength_class": "", "material_coating": "",
            "parse_status": "manual", "parse_reason": "Пустое наименование",
        }

    # Step 1: build 1-row DataFrame and run all extractors
    df = pd.DataFrame([{"name": text}])
    df.index = [0]
    transformed = transform_dataframe(df, ALL_FIELD_KEYS)

    # Step 2: build field-keyed row_dict (reusing the same logic as the readiness pipeline)
    row_dict = _build_row_dict(text, transformed.iloc[0], df.iloc[0])

    # Step 3: apply inference rules (fills size from diameter/length when empty)
    inference_rules = load_active_inference_rules()
    row_dict, _ = apply_inference(row_dict, inference_rules)

    # Step 4: enrich with standards (may update item_type from StandardRef)
    standards_cache = load_active_standards()
    row_dict, _ = _enrich_with_standards(dict(row_dict), standards_cache)

    # Step 5: build standard_text from extracted standard fields
    # extract_gost/din/iso already return full strings like "DIN 934", "ГОСТ 7798-70"
    standard_parts = [
        v for k in ("gost", "din", "iso")
        for v in [(row_dict.get(k) or "").strip()]
        if v
    ]
    standard_text = " / ".join(standard_parts)

    # Step 6: determine parse_status
    item_type = (row_dict.get("item_type") or "").strip()
    size = (row_dict.get("size") or "").strip()
    diameter = (row_dict.get("diameter") or "").strip()
    length = (row_dict.get("length") or "").strip()
    strength = (row_dict.get("strength") or "").strip()
    coating = (row_dict.get("coating") or "").strip()

    has_meaningful_fields = bool(size or diameter or standard_text or strength or coating)

    if item_type and has_meaningful_fields:
        parse_status = "ok"
        parse_reason = ""
    elif item_type:
        parse_status = "review"
        parse_reason = "Нет размера, стандарта и других параметров"
    else:
        parse_status = "manual"
        parse_reason = "Тип изделия не распознан"

    return {
        "item_type": item_type,
        "size": size,
        "diameter": diameter,
        "length": length,
        "standard_text": standard_text,
        "strength_class": strength,
        "material_coating": coating,
        "parse_status": parse_status,
        "parse_reason": parse_reason,
    }


def bulk_parse(names: list[str], skip_empty: bool = True, dedup: bool = True) -> list[dict]:
    """Parse a list of name strings, returning one result dict per unique name.

    Each result dict has all keys from parse_internal_item_name plus "name_full".
    """
    if skip_empty:
        names = [n.strip() for n in names if n.strip()]
    if dedup:
        seen: set[str] = set()
        unique: list[str] = []
        for n in names:
            key = n.strip().lower()
            if key not in seen:
                seen.add(key)
                unique.append(n.strip())
        names = unique

    results = []
    for name_full in names:
        parsed = parse_internal_item_name(name_full)
        results.append({"name_full": name_full, **parsed})
    return results
