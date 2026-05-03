"""Unified qty/uom preprocessing — single source of truth.

preprocess_row_text() is called by both row_parser (Excel) and
text_input.parser (textarea) to ensure identical qty/uom extraction
logic across all input paths.

Priority:
  1. Dedicated qty column (qty_cell_text), with optional combined "N uom" format.
  2. Tail extraction from name_text (тыс., no-space "6шт", decimal comma).
  3. Header UOM fallback (only when qty was already found).
Strict: both qty AND uom must be present, or both are None.
"""
from __future__ import annotations

import re
from typing import Optional


def preprocess_row_text(
    name_text: str,
    qty_cell_text: str = "",
    uom_cell_text: str = "",
    header_uom: str = "",
    tail_phrases: "list[str] | None" = None,
    qty_is_combined: bool = False,
) -> dict:
    """Extract qty/uom and produce a cleaned name.

    Args:
        name_text:       Raw name / position text (the "name" column or textarea line).
        qty_cell_text:   Content of a dedicated qty column, if any (empty = no column).
        uom_cell_text:   Content of a dedicated uom column, if any.
        header_uom:      UOM embedded in the column header (e.g. "Кол-во, шт" → "шт").
        tail_phrases:    Active stop-phrases to strip from the end after qty/uom removed.
        qty_is_combined: True when the qty column is expected to hold "N uom" text.

    Returns dict with keys:
        cleaned_name   (str)             — name with qty/uom tail stripped when sourced from it
        qty            (int|float|None)
        uom            (str|None)
        source         (str)             — "из колонки количества" | "из отдельных колонок" |
                                           "из заголовка" | "из наименования" | "не найдено"
        fail_reason    (str|None)        — set when a number was found but UOM not recognised
        tail_qty_expr  (str|None)        — raw tail expression scanned (e.g. "6 шт")
        tail_phrase_cut (str|None)       — stop-phrase that was stripped from name
        qty_multiplier (int)             — 1000 if тыс. multiplier was used, else 1
    """
    # Lazy imports to avoid circular dependency and allow test monkeypatching
    from app.parser_excel import normalize_uom, parse_qty_uom  # noqa: PLC0415
    from app.parsing.tail_extractor import (  # noqa: PLC0415
        extract_qty_uom_from_tail,
        strip_tail_phrase,
    )

    name = name_text.strip()
    qty: Optional[float] = None
    uom: Optional[str] = None
    source = "не найдено"
    fail_reason: Optional[str] = None
    tail_qty_expr: Optional[str] = None
    tail_phrase_cut: Optional[str] = None
    qty_multiplier: int = 1

    # ── Step 1: Dedicated qty column ──────────────────────────────────────────
    if qty_cell_text:
        if qty_is_combined:
            q, u, _ = parse_qty_uom(qty_cell_text)
            if q is not None and u is not None:
                qty, uom = q, u
                source = "из колонки количества"
        else:
            # Plain numeric cell — attempt float parse
            clean_num = (
                qty_cell_text.replace(",", ".").replace("\xa0", "").replace(" ", "")
            )
            try:
                qty = float(clean_num)
            except ValueError:
                # Non-numeric text in qty cell — parse as "N uom" anyway
                q, u, _ = parse_qty_uom(qty_cell_text)
                if q is not None and u is not None:
                    qty, uom = q, u
                    source = "из колонки количества"

        # Step 1b: uom from dedicated uom column
        if qty is not None and uom is None and uom_cell_text:
            uom_norm = normalize_uom(uom_cell_text)
            if uom_norm:
                uom = uom_norm
                source = "из отдельных колонок"

        # Step 1c: uom from column header
        if qty is not None and uom is None and header_uom:
            uom = header_uom
            source = "из заголовка"

    # ── Step 2: Tail extraction (always run for tail_qty_expr; use result only if
    #           qty not yet found from a dedicated column) ─────────────────────
    t_clean, t_qty, t_uom, t_mult, t_expr, t_reason = extract_qty_uom_from_tail(name)
    tail_qty_expr = t_expr
    if t_reason:
        fail_reason = t_reason

    if qty is None:
        if t_qty is not None and t_uom is not None:
            qty = t_qty
            uom = t_uom
            qty_multiplier = t_mult
            name = t_clean          # strip the matched expression from name
            source = "из наименования"
        else:
            name = t_clean          # cleaned even if no qty found (space normalization)
    # If qty came from columns: name stays as name_text (not modified by tail)

    # ── Step 3: Header UOM fallback (qty found, uom still missing) ────────────
    if qty is not None and uom is None and header_uom:
        uom = header_uom
        source = "из заголовка"

    # ── qty without uom is allowed: keep qty, uom stays None ─────────────────
    if qty is None:
        uom = None
        source = "не найдено"

    # Integer conversion for whole numbers
    if qty is not None and float(qty) == int(float(qty)):
        qty = int(float(qty))

    # ── Phase B: strip active tail phrases ────────────────────────────────────
    if tail_phrases:
        name_stripped, phrase_cut = strip_tail_phrase(name, tail_phrases)
        if phrase_cut:
            tail_phrase_cut = phrase_cut
            name = name_stripped

    # Final name cleanup
    name = re.sub(r"[ \t]+", " ", name).strip()

    return {
        "cleaned_name":    name,
        "qty":             qty,
        "uom":             uom,
        "source":          source,
        "fail_reason":     fail_reason,
        "tail_qty_expr":   tail_qty_expr,
        "tail_phrase_cut": tail_phrase_cut,
        "qty_multiplier":  qty_multiplier,
    }
