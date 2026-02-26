"""Unified row parser.

parse_row() converts a dict of raw cell values (keyed by column header) plus
a mapping dict (semantic role → header key) into a canonical row dict for the
downstream extract/enrich/readiness pipeline.

qty/uom policy (strict, no defaults):
  1a. Try qty column (combined "1500 шт" or plain numeric).
  1b. If plain numeric qty found but uom still missing, check dedicated uom_col.
  1c. If uom still missing, check UOM embedded in the qty column header
      (e.g. "Кол-во, шт" with numeric cells).
      If pair is complete after 1b or 1c → skip fallback steps.
  2. If uom still missing: try suffix extraction from name_raw.
  3. If still missing: try suffix from last segment of raw_text.
  4. If either qty or uom is still None after all steps → both become None.
     We never default uom to "шт".
"""
from __future__ import annotations

import math
import re
from typing import Any, Optional


def _s(val: Any) -> str:
    """Safe string: None/NaN → empty string, otherwise stripped str."""
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    return str(val).strip()


def parse_row(
    cells: dict[str, Any],
    mapping: dict[str, Any],
    tail_phrases: "list[str] | None" = None,
) -> dict:
    """Parse one Excel row into a canonical dict.

    Args:
        cells:        raw cell values keyed by column header (or sentinel key).
        mapping:      column routing (name_col, qty_col, uom_col, code_col, …).
        tail_phrases: list of active stop-phrases to strip from the end of the
                      name after qty/uom extraction (None = skip stripping).

    Returns a dict with keys:
        raw_text, name_raw, name, qty, uom, code, standard_raw, strength_raw,
        note_raw, qty_uom_source, tail_phrase_cut, tail_qty_expr,
        qty_multiplier, qty_fail_reason.
    """
    # Lazy imports to avoid circular dependency (row_parser → parser_excel)
    from app.parser_excel import extract_qty_uom_suffix, normalize_uom, parse_qty_uom  # noqa: PLC0415
    from app.parsing.tail_extractor import extract_qty_uom_from_tail, strip_tail_phrase  # noqa: PLC0415

    name_col = mapping.get("name_col")
    qty_col = mapping.get("qty_col")
    uom_col = mapping.get("uom_col")
    code_col = mapping.get("code_col")
    standard_col = mapping.get("standard_col")
    strength_col = mapping.get("strength_col")
    note_col = mapping.get("note_col")
    qty_is_combined = bool(mapping.get("qty_is_combined", False))

    known_cols = {name_col, qty_col, uom_col, code_col, standard_col, strength_col, note_col} - {None}

    # ── Extract named column values ──────────────────────────────────────────
    name_raw = _s(cells.get(name_col)) if name_col else ""
    qty_cell = cells.get(qty_col) if qty_col else None
    qty_cell_str = _s(qty_cell)
    uom_cell_str = _s(cells.get(uom_col)) if uom_col else ""
    code = _s(cells.get(code_col)) if code_col else ""
    standard_raw = _s(cells.get(standard_col)) if standard_col else ""
    strength_raw = _s(cells.get(strength_col)) if strength_col else ""
    note_raw = _s(cells.get(note_col)) if note_col else ""

    # ── Build raw_text ────────────────────────────────────────────────────────
    # Order: name | qty_cell | semantic extras | unmapped columns
    parts: list[str] = []
    if name_raw:
        parts.append(name_raw)
    if qty_cell_str and qty_cell_str not in parts:
        parts.append(qty_cell_str)
    for extra in (standard_raw, strength_raw, note_raw):
        if extra and extra not in parts:
            parts.append(extra)
    # Unmapped columns: add values not already covered (code excluded — it's an ID)
    for col_key, val in cells.items():
        if col_key in known_cols:
            continue
        vs = _s(val)
        if vs and vs not in parts:
            parts.append(vs)
    raw_text = " | ".join(parts)

    # ── qty/uom extraction ────────────────────────────────────────────────────
    qty: Optional[float] = None
    uom: Optional[str] = None
    name = name_raw
    qty_uom_source = "не найдено"

    # Extra metadata for trace / analysis
    tail_phrase_cut: Optional[str] = None
    tail_qty_expr: Optional[str] = None
    qty_multiplier: int = 1
    qty_fail_reason: Optional[str] = None

    # Step 1: from qty column (and optionally dedicated uom_col)
    if qty_cell_str:
        if qty_is_combined:
            # Cell expected to contain "1500 шт" format
            q, u, _ = parse_qty_uom(qty_cell_str)
            if q is not None and u is not None:
                qty, uom = q, u
                qty_uom_source = "из колонки количества"
        else:
            # Plain numeric column — attempt float parse
            clean = qty_cell_str.replace(",", ".").replace("\xa0", "").replace(" ", "")
            try:
                qty = float(clean)
                # uom still unknown; check dedicated uom_col before falling back
            except ValueError:
                # Cell has non-numeric text (might be "1500 шт" despite not being
                # marked as combined — parse it anyway)
                q, u, _ = parse_qty_uom(qty_cell_str)
                if q is not None and u is not None:
                    qty, uom = q, u
                    qty_uom_source = "из колонки количества"

    # Step 1b: if plain numeric qty found but uom still missing, check dedicated uom_col
    if qty is not None and uom is None and uom_cell_str:
        uom_norm = normalize_uom(uom_cell_str)
        if uom_norm:
            uom = uom_norm
            qty_uom_source = "из отдельных колонок"

    # Step 1c: if qty found but uom still missing, check UOM embedded in column header
    # (e.g. header "Кол-во, шт" with numeric cells → uom = "шт")
    qty_header_uom = mapping.get("qty_header_uom")
    if qty is not None and uom is None and qty_header_uom:
        uom = qty_header_uom
        qty_uom_source = "из заголовка"

    # Step 2: from name_raw tail — enhanced extractor (тыс., no-space, unknown UOM)
    if name_raw:
        t_clean, t_qty, t_uom, t_mult, t_expr, t_reason = extract_qty_uom_from_tail(name_raw)
        tail_qty_expr = t_expr
        if t_reason:
            qty_fail_reason = t_reason

        if t_qty is not None and t_uom is not None:
            qty_was_none = qty is None
            if qty is None:
                qty = t_qty
                qty_multiplier = t_mult
            if uom is None:
                uom = t_uom
            if qty_was_none:
                # Tail provided the qty — strip the matched expression from name
                name = t_clean
                qty_uom_source = "из наименования"
        else:
            # No qty/uom from tail, but still use the cleaned text as base for name
            name = t_clean

        # Phase B: strip tail phrase from the name (after qty/uom removed)
        if tail_phrases:
            name_stripped, phrase_cut = strip_tail_phrase(name, tail_phrases)
            if phrase_cut:
                tail_phrase_cut = phrase_cut
                name = name_stripped

    # Step 3: from last segment of raw_text (only if different from name_raw)
    if (qty is None or uom is None) and raw_text:
        last_seg = raw_text.split("|")[-1].strip()
        if last_seg and last_seg != name_raw:
            q3, u3, _ = extract_qty_uom_suffix(last_seg)
            if q3 is not None and u3 is not None:
                if qty is None:
                    qty = q3
                if uom is None:
                    uom = u3
                qty_uom_source = "из объединённого текста"

    # Step 4: strict — both or none
    if qty is None or uom is None:
        qty = None
        uom = None
        qty_uom_source = "не найдено"

    # ── Clean name ────────────────────────────────────────────────────────────
    name = re.sub(r" {2,}", " ", name).strip()

    # Use integer qty where the value is whole
    if qty is not None and qty == int(qty):
        qty = int(qty)

    return {
        "raw_text": raw_text,
        "name_raw": name_raw,
        "name": name,
        "qty": qty,
        "uom": uom,
        "code": code or None,
        "standard_raw": standard_raw or None,
        "strength_raw": strength_raw or None,
        "note_raw": note_raw or None,
        "qty_uom_source": qty_uom_source,
        "tail_phrase_cut": tail_phrase_cut,
        "tail_qty_expr": tail_qty_expr,
        "qty_multiplier": qty_multiplier,
        "qty_fail_reason": qty_fail_reason,
    }
