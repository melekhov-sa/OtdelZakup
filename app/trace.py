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
    "strength", "coating", "gost", "iso", "din",
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


# ── Validation-rule check (trace-only, informational) ─────────────────────────

def _load_active_validation_rules():
    from app.database import get_db_session
    from app.models import ValidationRule

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
) -> list:
    """Build one trace dict per row. Returns a list (0-based index = row_number - 1)."""
    from app.extractors import _concat_row
    from app.readiness import (
        _build_row_dict,
        _enrich_with_standards,
        _find_matching_rule,
        _parse_standard_to_kind_code,
        evaluate_readiness,
        load_active_rules,
        load_active_standards,
    )

    if rules is None:
        rules = load_active_rules()
    if standards_cache is None:
        standards_cache = load_active_standards()

    val_rules = _load_active_validation_rules()

    traces = []
    for row_number, (idx, _) in enumerate(df_transformed.iterrows(), start=1):
        original_row = (
            df_original.loc[idx] if idx in df_original.index else pd.Series()
        )
        text = _concat_row(original_row) if len(original_row) > 0 else ""
        transformed_row = df_transformed.loc[idx]

        # ── A. Raw inputs from original file ───────────────────────────────
        raw_inputs = {
            "raw_name":     _col(original_row, "name"),
            "standard_raw": _col(original_row, "standard_raw"),
            "strength_raw": _col(original_row, "strength_raw"),
            "note_raw":     _col(original_row, "note_raw"),
        }

        # ── B. Extracted fields (extractors + signal merging) ─────────────
        row_dict = _build_row_dict(text, transformed_row, original_row)
        extracted = {k: row_dict.get(k, "") for k in _TRACE_FIELD_KEYS}

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

        # ── D. Readiness evaluation ────────────────────────────────────────
        rule = _find_matching_rule(enriched_dict.get("item_type", ""), rules)
        if rule:
            base_status, missing, rule_name = evaluate_readiness(enriched_dict, rules)
            required_fields = rule.require_fields_list
            rule_id = rule.id
        else:
            base_status, missing, rule_name = "manual", [], ""
            required_fields = []
            rule_id = None

        if extra_reasons and base_status == "ok":
            base_status = "review"

        readiness_trace = {
            "applied_rule": rule_name,
            "applied_rule_id": rule_id,
            "required_fields": [
                {"key": f, "label": display_label(f)} for f in required_fields
            ],
            "missing_fields": [
                {"key": f, "label": display_label(f)} for f in missing
            ],
            "base_status": base_status,
        }

        # ── E. Validation rules ────────────────────────────────────────────
        val_applied = []
        for vr in val_rules:
            fired, reasons = _check_val_rule(enriched_dict, vr)
            if fired:
                val_applied.append({
                    "id": vr.id,
                    "name": vr.name,
                    "description": vr.description or "",
                    "force_status": vr.force_status or "",
                    "force_status_label": vr.force_status_label,
                    "reason": "; ".join(reasons),
                })

        # ── F. Final (authoritative from df_transformed) ──────────────────
        final_status = (
            _str(transformed_row["status"]).strip()
            if "status" in transformed_row.index
            else base_status
        )
        final_reason = (
            _str(transformed_row["reason"]).strip()
            if "reason" in transformed_row.index
            else ""
        )

        traces.append({
            "row_number": row_number,
            "raw_inputs": raw_inputs,
            "extracted_fields": extracted,
            "enrichment": {
                "standard_ref_match": std_ref_match,
                "standard_ref": std_ref_info,
                "item_type_source": item_type_source,
                "conflict_reasons": extra_reasons,
            },
            "readiness": readiness_trace,
            "validation": {"applied_rules": val_applied},
            "final": {
                "status": final_status,
                "status_label": _STATUS_LABELS.get(final_status, final_status),
                "reasons": final_reason,
            },
        })

    return traces
