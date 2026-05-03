"""Per-row trace data for transparency / explain feature.

Builds a trace dict for each row that captures:
- raw inputs from the original file
- what the extractors found
- standard-reference enrichment details
- which readiness rule was applied and what was missing
- which validation rules fired
- the final authoritative status and reasons
"""

import json

import pandas as pd

from app.display_labels import display_label
from app.extractors import _str

_STATUS_LABELS = {
    "ok": "Не требует проверки",
    "review": "Требуется просмотреть",
    "manual": "Требуется вручную разобрать",
}

_TRACE_FIELD_KEYS = [
    "item_type", "size", "diameter", "length",
    "strength", "coating", "gost", "iso", "din", "wrench_size",
]


# ── Storage helpers ────────────────────────────────────────────────────────────

def _traces_dir(file_id: str):
    """Return the cache directory for file_id (reads CACHE_DIR at call time)."""
    import app.cache as _cache  # lazy: picks up monkeypatched value in tests
    return _cache.CACHE_DIR / file_id


def save_traces(file_id: str, traces: list) -> None:
    """Persist trace list as JSON in the file's cache directory."""
    p = _traces_dir(file_id)
    p.mkdir(parents=True, exist_ok=True)
    (p / "traces.json").write_text(
        json.dumps(traces, ensure_ascii=False, default=str), encoding="utf-8"
    )


def load_traces(file_id: str) -> list | None:
    """Load traces from cache. Returns None if the file has not been transformed yet."""
    p = _traces_dir(file_id) / "traces.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


# ── Safe column accessor for pandas Series ─────────────────────────────────────

def _col(series: pd.Series, name: str) -> str:
    if len(series) == 0 or name not in series.index:
        return ""
    return _str(series[name]).strip()


# ── Main trace builder ─────────────────────────────────────────────────────────

def build_traces(
    df_original: pd.DataFrame,
    df_transformed: pd.DataFrame,
    rules=None,
    standards_cache=None,
    inference_rules=None,
    match_results=None,
) -> list:
    """Build one trace dict per row. Returns a list (0-based index = row_number - 1)."""
    from app.extractors import _concat_row
    from app.readiness import (
        _build_row_dict,
        _check_val_rule,
        _enrich_with_standards,
        _find_matching_rule,
        _parse_standard_to_kind_code,
        evaluate_readiness,
        load_active_rules,
        load_active_standards,
        load_active_validation_rules,
    )
    from app.inference_engine import apply_inference, load_active_inference_rules

    if rules is None:
        rules = load_active_rules()
    if standards_cache is None:
        standards_cache = load_active_standards()
    if inference_rules is None:
        inference_rules = load_active_inference_rules()

    val_rules = load_active_validation_rules()
    readiness_disabled = len(rules) == 0
    validation_disabled = len(val_rules) == 0

    traces = []
    for row_number, (idx, _) in enumerate(df_transformed.iterrows(), start=1):
        original_row = (
            df_original.loc[idx] if idx in df_original.index else pd.Series()
        )
        # Use raw_text for extraction — excludes code column that would otherwise
        # pollute size/length detection (e.g. "BULX10773" → length=10773 via x\d+ regex)
        text = _col(original_row, "raw_text") or (
            _concat_row(original_row) if len(original_row) > 0 else ""
        )
        transformed_row = df_transformed.loc[idx]

        # ── A. Raw inputs from original file ───────────────────────────────
        raw_inputs = {
            "raw_name":        _col(original_row, "name"),
            "standard_raw":    _col(original_row, "standard_raw"),
            "strength_raw":    _col(original_row, "strength_raw"),
            "note_raw":        _col(original_row, "note_raw"),
            "raw_text":        _col(original_row, "raw_text"),
            "qty_uom_source":  _col(original_row, "qty_uom_source"),
            "tail_qty_expr":   _col(original_row, "tail_qty_expr"),
            "qty_multiplier":  _col(original_row, "qty_multiplier"),
            "tail_phrase_cut": _col(original_row, "tail_phrase_cut"),
            "qty_fail_reason": _col(original_row, "qty_fail_reason"),
        }

        # ── B. Extracted fields (extractors + signal merging) ─────────────
        row_dict = _build_row_dict(text, transformed_row, original_row)
        extracted = {k: row_dict.get(k, "") for k in _TRACE_FIELD_KEYS}

        # ── B2. Coating detection detail ─────────────────────────────────
        coating_trace = _build_coating_trace(text)

        # ── B3. Strength detection detail ────────────────────────────────
        strength_trace = _build_strength_trace(text)

        # ── B4. Size detection detail ────────────────────────────────────
        size_trace = _build_size_trace(text)

        # ── C. Standard-reference enrichment ──────────────────────────────
        enriched_dict, extra_reasons = _enrich_with_standards(
            dict(row_dict), standards_cache
        )

        std_ref_match = False
        std_ref_info = None
        for std_key in ("gost", "iso", "din"):
            std_val = row_dict.get(std_key, "")
            if not std_val:
                continue
            parsed = _parse_standard_to_kind_code(std_val)
            if not parsed:
                continue
            entry = standards_cache.get(parsed)
            if not entry:
                continue
            ref_item_type, ref_title = entry
            if not ref_item_type:
                continue
            std_ref_match = True
            std_ref_info = {
                "kind": parsed[0],
                "code": parsed[1],
                "item_type": ref_item_type,
                "title": ref_title or "",
            }
            break

        item_type_source = enriched_dict.get(
            "item_type_source",
            "из текста" if row_dict.get("item_type") else "",
        )

        # ── C2. Inference (compute missing fields) ─────────────────────────
        enriched_dict, size_inf_trace = apply_inference(enriched_dict, inference_rules)

        # ── D. Readiness evaluation ────────────────────────────────────────
        name_val = str(enriched_dict.get("name", "")).strip()
        empty_name = not name_val or name_val in ("—", "-")

        if readiness_disabled:
            if empty_name:
                rd_base_status = "manual"
                rd_applied_rule = "—"
            else:
                rd_base_status = "ok"
                rd_applied_rule = "Правила готовности отключены"
            rd_required_fields: list = []
            rd_missing: list = []
            rd_rule_id = None
        else:
            rule = _find_matching_rule(enriched_dict.get("item_type", ""), rules)
            if rule:
                rd_base_status, rd_missing, rd_rule_name = evaluate_readiness(
                    enriched_dict, rules
                )
                rd_required_fields = rule.require_fields_list
                rd_rule_id = rule.id
                rd_applied_rule = rd_rule_name
            else:
                rd_base_status = "manual"
                rd_missing = []
                rd_applied_rule = "Нет подходящего правила"
                rd_required_fields = []
                rd_rule_id = None

            if extra_reasons and rd_base_status == "ok":
                rd_base_status = "review"

        readiness_trace = {
            "applied_rule": rd_applied_rule,
            "applied_rule_id": rd_rule_id,
            "required_fields": [
                {"key": f, "label": display_label(f)} for f in rd_required_fields
            ],
            "missing_fields": [
                {"key": f, "label": display_label(f)} for f in rd_missing
            ],
            "base_status": rd_base_status,
            "disabled": readiness_disabled,
        }

        # ── E. Validation rules ────────────────────────────────────────────
        val_applied = []
        for vr in val_rules:
            fired, vr_reasons = _check_val_rule(enriched_dict, vr, standards_cache)
            if fired:
                val_applied.append({
                    "id": vr.id,
                    "name": vr.name,
                    "description": vr.description or "",
                    "force_status": vr.force_status or "",
                    "force_status_label": vr.force_status_label,
                    "reason": "; ".join(vr_reasons),
                })

        # ── F. Final (authoritative from df_transformed) ──────────────────
        final_status = (
            _str(transformed_row["status"]).strip()
            if "status" in transformed_row.index
            else rd_base_status
        )
        final_reason = (
            _str(transformed_row["reason"]).strip()
            if "reason" in transformed_row.index
            else ""
        )

        # ── Ж. Internal catalog matching ───────────────────────────────────
        matching_trace = {}
        if match_results is not None and (row_number - 1) < len(match_results):
            matching_trace = match_results[row_number - 1]

        # ── З. Category-based validation ──────────────────────────────────
        cat_val_trace = _build_category_validation_trace(enriched_dict)

        traces.append({
            "row_number": row_number,
            "raw_inputs": raw_inputs,
            "extracted_fields": extracted,
            "coating_detection": coating_trace,
            "strength_detection": strength_trace,
            "size_detection": size_trace,
            "enrichment": {
                "standard_ref_match": std_ref_match,
                "standard_ref": std_ref_info,
                "item_type_source": item_type_source,
                "conflict_reasons": extra_reasons,
            },
            "inference": size_inf_trace,
            "readiness": readiness_trace,
            "validation": {
                "applied_rules": val_applied,
                "disabled": validation_disabled,
            },
            "category_validation": cat_val_trace,
            "final": {
                "status": final_status,
                "status_label": _STATUS_LABELS.get(final_status, final_status),
                "reasons": final_reason,
            },
            "matching": matching_trace,
        })

    return traces


def _build_category_validation_trace(row_dict: dict) -> dict:
    """Run category-based validation and build a trace dict."""
    try:
        from app.category_validator import validate_row  # noqa: PLC0415
        result = validate_row(row_dict)
    except Exception:
        return {"available": False}

    if result is None:
        return {"available": False}

    return {
        "available": True,
        "category": result.category_name,
        "subcategory": result.subcategory_name or "",
        "item_type": result.item_type_name or "",
        "rule_name": result.rule_display_name,
        "exception_note": result.exception_note or "",
        "required_fields": result.required_labels,
        "required_field_keys": result.required_fields,
        "missing_fields": result.missing_labels,
        "missing_field_keys": result.missing_fields,
        "status": result.status,
    }


def _build_coating_trace(text: str) -> dict:
    """Run coating detection via unified service, fallback to old detector."""
    try:
        from app.services.normalization_service import detect_coating  # noqa: PLC0415
        result = detect_coating(text)
        if result is not None:
            return {
                "detected": True,
                "raw_match": result.raw_match,
                "coating_code": result.normalized_code,
                "coating_name": result.normalized_name,
                "rule_id": result.rule_id,
                "pattern_raw": result.pattern_raw,
                "match_type": result.match_type,
                "rule_display": f'{result.match_type}("{result.pattern_raw}") → {result.normalized_name}',
            }
    except Exception:
        pass
    try:
        from app.services.coating_detector import detect_coating as _old  # noqa: PLC0415
        result = _old(text)
        if result is not None:
            return {
                "detected": True,
                "raw_match": result.raw_match,
                "coating_code": result.coating_code,
                "coating_name": result.coating_name,
                "rule_id": result.rule_id,
                "pattern_raw": result.pattern_raw,
                "match_type": result.match_type,
                "rule_display": f'{result.match_type}("{result.pattern_raw}") → {result.coating_name}',
            }
    except Exception:
        pass
    return {"detected": False}


def _build_strength_trace(text: str) -> dict:
    """Run strength class detection via unified service, fallback to old detector."""
    try:
        from app.services.normalization_service import detect_strength  # noqa: PLC0415
        result = detect_strength(text)
        if result is not None:
            extra = result.extra
            return {
                "detected": True,
                "raw_match": result.raw_match,
                "strength_code": result.normalized_code,
                "strength_name": result.normalized_name,
                "strength_family": extra.get("family", ""),
                "rule_id": result.rule_id,
                "pattern_raw": result.pattern_raw,
                "match_type": result.match_type,
                "rule_display": f'{result.match_type}("{result.pattern_raw}") → {result.normalized_name}',
            }
    except Exception:
        pass
    try:
        from app.services.strength_detector import detect_strength_class  # noqa: PLC0415
        result = detect_strength_class(text)
        if result is not None:
            return {
                "detected": True,
                "raw_match": result.raw_match,
                "strength_code": result.strength_code,
                "strength_name": result.strength_name,
                "strength_family": result.strength_family,
                "rule_id": result.rule_id,
                "pattern_raw": result.pattern_raw,
                "match_type": result.match_type,
                "rule_display": f'{result.match_type}("{result.pattern_raw}") → {result.strength_name}',
            }
    except Exception:
        pass
    return {"detected": False}


def _build_size_trace(text: str) -> dict:
    """Run size detection via unified service, fallback to old detector."""
    try:
        from app.services.normalization_service import detect_size  # noqa: PLC0415
        result = detect_size(text)
        if result is not None:
            extra = result.extra
            fields = {}
            for k in ("diameter", "length", "width", "thickness", "pitch", "tolerance"):
                v = extra.get(k, "")
                if v:
                    fields[k] = v
            return {
                "detected": True,
                "raw_match": result.raw_match,
                "size_kind": extra.get("size_kind", ""),
                "size_norm": extra.get("size_norm", result.normalized_code),
                "fields": fields,
                "rule_id": result.rule_id,
                "pattern_raw": result.pattern_raw,
                "match_type": result.match_type,
                "rule_display": f'{result.match_type}("{result.pattern_raw}") → {extra.get("size_norm", result.normalized_code)}',
            }
    except Exception:
        pass
    try:
        from app.services.size_detector import detect_size as _old  # noqa: PLC0415
        result = _old(text)
        if result is not None:
            fields = {}
            if result.diameter:
                fields["diameter"] = result.diameter
            if result.length:
                fields["length"] = result.length
            if result.width:
                fields["width"] = result.width
            if result.thickness:
                fields["thickness"] = result.thickness
            if result.pitch:
                fields["pitch"] = result.pitch
            if result.tolerance:
                fields["tolerance"] = result.tolerance
            return {
                "detected": True,
                "raw_match": result.raw_match,
                "size_kind": result.size_kind,
                "size_norm": result.size_norm,
                "fields": fields,
                "rule_id": result.rule_id,
                "pattern_raw": result.pattern_raw,
                "match_type": result.match_type,
                "rule_display": f'{result.match_type}("{result.pattern_raw}") → {result.size_norm}',
            }
    except Exception:
        pass
    return {"detected": False}
