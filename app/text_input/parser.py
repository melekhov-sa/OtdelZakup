"""Parse free-form text input into structured position rows.

Supported formats:
  A) Numbered list: "1. ...", "2) ..."
  B) Dash qty: "<name> - <N> <uom>" or "<name> — <N> <uom>"
  C) Two-line: position line then "Количество: <N> <uom>" on the next line
  D) Service lines ("все в цинке", etc.) → collected as note_raw
  E) Inline qty+unit: "шт. 8", "8 шт", "pcs 30", "30 pcs" etc.

Strict requirement: qty AND uom must both be present to be extracted.
If only one is found, both remain None.
"""

import re
from typing import Optional

from app.parser_excel import parse_qty_uom
from app.parsing.preprocess import preprocess_row_text
from app.parsing.tail_extractor import strip_tail_phrase


# ── Inline qty/unit extractor (parse_text_line) ─────────────

_KNOWN_UNITS: set[str] = {
    "шт", "кг", "г", "т", "м", "мм", "м2", "м3", "уп", "пач", "кор",
    "компл", "pcs", "pc", "л",
}

_UNIT_NORM: dict[str, str] = {
    "шт": "шт", "шт.": "шт", "штук": "шт", "штука": "шт", "штуки": "шт",
    "кг": "кг", "кг.": "кг", "kg": "кг",
    "г": "г", "г.": "г", "гр": "г", "гр.": "г",
    "т": "т", "т.": "т",
    "м": "м", "м.": "м",
    "мм": "мм", "мм.": "мм", "mm": "мм",
    "м2": "м2", "м²": "м2", "кв.м": "м2",
    "м3": "м3", "м³": "м3", "куб.м": "м3",
    "уп": "уп", "уп.": "уп", "упак": "уп",
    "пач": "пач", "пач.": "пач",
    "кор": "кор", "кор.": "кор",
    "компл": "компл", "компл.": "компл", "комплект": "компл",
    "pcs": "pcs", "pcs.": "pcs", "pc": "pcs",
    "л": "л", "л.": "л",
}

# Build alternation for regex — longest first to prevent partial matches
_UNIT_ALTS = sorted(_UNIT_NORM.keys(), key=len, reverse=True)
_UNIT_RE_PART = "|".join(re.escape(u) for u in _UNIT_ALTS)

# Pattern 1: unit then qty — "шт. 8", "шт 24", "pcs 30", "шт:8", "шт : 8"
# Require whitespace before unit. Between unit and qty require a separator
# (space, dot, colon, dash) to avoid "М10" (thread size) being parsed as "м" + "10".
_UNIT_THEN_QTY_RE = re.compile(
    r"(?<=\s)(" + _UNIT_RE_PART + r")(?:\.|\s|[:\-])\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*$",
    re.IGNORECASE,
)

# Pattern 2: qty then unit — "8 шт", "24шт.", "30 pcs"
# Require whitespace before qty to avoid matching numbers inside product codes
_QTY_THEN_UNIT_RE = re.compile(
    r"(?<=\s)(\d+(?:[.,]\d+)?)\s*(" + _UNIT_RE_PART + r")\.?\s*$",
    re.IGNORECASE,
)


def _normalize_unit(raw: str) -> str | None:
    """Normalize a unit string, return None if not recognized."""
    key = raw.lower().strip()
    return _UNIT_NORM.get(key)


def _clean_name(name: str) -> str:
    """Clean up name after qty/unit removal."""
    name = re.sub(r"\s*[,\-]+\s*$", "", name)   # trailing commas/dashes
    name = re.sub(r"^\s*[,\-]+\s*", "", name)   # leading commas/dashes
    name = re.sub(r"\s{2,}", " ", name)          # collapse spaces
    return name.strip()


def parse_text_line(line: str) -> dict:
    """Parse a single free-text line into {name, qty, unit}.

    Supports formats:
        "Гайка М16 шт. 10"     → qty=10, unit="шт"
        "Болт М16х50 20 шт"    → qty=20, unit="шт"
        "Винт M12x35 pcs 30"   → qty=30, unit="pcs"
        "Шайба DIN125 50"      → qty=None, unit=None (no unit)
        "Анкер М10"             → qty=None, unit=None

    Returns: {"name": str, "qty": float|None, "unit": str|None}
    """
    # Step 1: preprocess
    s = line.strip()
    s = re.sub(r"\t", " ", s)
    s = re.sub(r" {2,}", " ", s)

    # Step 2: try unit-then-qty pattern ("шт. 8", "pcs 30")
    m = _UNIT_THEN_QTY_RE.search(s)
    if m:
        unit_raw, qty_raw = m.group(1), m.group(2)
        unit = _normalize_unit(unit_raw)
        if unit:
            qty = float(qty_raw.replace(",", "."))
            if qty == int(qty):
                qty = int(qty)
            name = _clean_name(s[: m.start()])
            return {"name": name, "qty": qty, "unit": unit}

    # Step 3: try qty-then-unit pattern ("20 шт", "24шт.")
    m = _QTY_THEN_UNIT_RE.search(s)
    if m:
        qty_raw, unit_raw = m.group(1), m.group(2)
        unit = _normalize_unit(unit_raw)
        if unit:
            qty = float(qty_raw.replace(",", "."))
            if qty == int(qty):
                qty = int(qty)
            name = _clean_name(s[: m.start()])
            return {"name": name, "qty": qty, "unit": unit}

    # Step 4: fallback — no qty/unit found
    return {"name": s, "qty": None, "unit": None}

# ── Patterns ──────────────────────────────────────────────────

_NUMBERED_RE = re.compile(r"^(\d+)[.)]\s+(.+)", re.UNICODE)

_QTY_LABEL_RE = re.compile(
    r"^(?:количество|кол(?:[.\-\s]?во)?)\s*:?\s*(.+)",
    re.IGNORECASE | re.UNICODE,
)

_NOTE_PATTERNS = [
    re.compile(p, re.IGNORECASE | re.UNICODE)
    for p in [
        r"^все\b",
        r"^можно\b",
        r"^особые\b",
        r"^требование",
        r"^примечание",
        r"^комментарий",
        r"^итого\b",
        r"^p\.?\s*s\.?\b",
        r"^п\.?\s*с\.?\b",
    ]
]


# ── Helpers ───────────────────────────────────────────────────

def _is_note_line(line: str) -> bool:
    """Return True if the line looks like a service/note rather than a position."""
    for pat in _NOTE_PATTERNS:
        if pat.match(line):
            return True
    return False


def _normalize_thousands(text: str) -> str:
    """Remove spaces used as thousand separators in numbers: '10 000' → '10000'."""
    return re.sub(r"(?<=\d)\s+(?=\d{3}\b)", "", text)


def _to_int_if_whole(v: float):
    """Return int if v is a whole number, otherwise return float."""
    try:
        i = int(v)
        return i if float(i) == v else v
    except (ValueError, OverflowError):
        return v


def _split_dash_qty(text: str) -> tuple[str, Optional[object], Optional[str]]:
    """Extract 'name - N uom' or 'name — N uom' pattern.

    Returns (name, qty, uom) if both qty and uom are found,
    or (original_text, None, None) otherwise.
    Strict: right side must be *only* qty+uom with no other text.
    """
    for sep in (" — ", " - ", "—", "-"):
        idx = text.rfind(sep)
        if idx <= 0:
            continue
        left = text[:idx].strip()
        right = text[idx + len(sep) :].strip()
        if not left or not right:
            continue
        right_norm = _normalize_thousands(right)
        qty, uom, rest = parse_qty_uom(right_norm)
        if qty is not None and uom is not None and rest == "":
            return left, _to_int_if_whole(qty), uom
    return text, None, None


# ── Main parser ───────────────────────────────────────────────

def parse_text_to_rows(
    text: str,
    tail_phrases: "list[str] | None" = None,
) -> list[dict]:
    """Parse free-form text into a list of structured row dicts.

    Each dict has keys:
      row_number      (int, 1-based)
      name            (str, cleaned — without tail qty/uom and stop-phrases)
      qty             (int|float|None — None if not found)
      uom             (str|None     — None if not found)
      qty_uom_source  (str)
      tail_qty_expr   (str|None)
      tail_phrase_cut (str|None)
      qty_multiplier  (int)
      qty_fail_reason (str|None)
      source_line     (str, original line)
      raw_text        (str, same as original line for text input)
      note_raw        (str, service-line text applied to all rows)

    Both qty and uom must be present for either to be stored.
    tail_phrases: active stop-phrases to strip from name after qty/uom extraction.
    """
    lines = [ln.strip() for ln in text.splitlines()]

    # ── Phase 1: find "Количество: N uom" labels and their parent lines ──
    qty_for_line: dict[int, tuple] = {}  # parent_line_idx → (qty, uom)
    qty_label_indices: set[int] = set()
    prev_nonempty = -1

    for i, line in enumerate(lines):
        if not line:
            continue
        m = _QTY_LABEL_RE.match(line)
        if m and prev_nonempty >= 0:
            norm = _normalize_thousands(m.group(1).strip())
            qty, uom, _ = parse_qty_uom(norm)
            if qty is not None and uom is not None:
                qty_for_line[prev_nonempty] = (_to_int_if_whole(qty), uom)
                qty_label_indices.add(i)
                continue  # do not update prev_nonempty for label lines
        prev_nonempty = i

    # ── Phase 2: process all lines ────────────────────────────
    notes: list[str] = []
    rows: list[dict] = []
    row_counter = 0

    def _make_row(
        row_num: int,
        source_line: str,
        name: str,
        qty,
        uom,
        qty_source: str,
    ) -> dict:
        """Build a complete row dict.

        When qty/uom were already found by an existing pattern (dash, label),
        we only apply tail-phrase stripping.
        When qty is None, delegate entirely to preprocess_row_text() which
        runs tail extraction + phrase stripping.
        """
        if qty is not None and uom is not None:
            # qty/uom found by existing patterns — just strip tail phrases
            tail_phrase_cut = None
            if tail_phrases:
                name, tail_phrase_cut = strip_tail_phrase(name, tail_phrases)
            return {
                "row_number":      row_num,
                "name":            name.strip(),
                "qty":             qty,
                "uom":             uom,
                "qty_uom_source":  qty_source,
                "tail_qty_expr":   None,
                "tail_phrase_cut": tail_phrase_cut,
                "qty_multiplier":  1,
                "qty_fail_reason": None,
                "source_line":     source_line,
                "raw_text":        source_line,
                "note_raw":        "",
            }

        # qty is None — use preprocess_row_text() for tail extraction
        pp = preprocess_row_text(name, tail_phrases=tail_phrases)
        return {
            "row_number":      row_num,
            "name":            pp["cleaned_name"],
            "qty":             pp["qty"],
            "uom":             pp["uom"],
            "qty_uom_source":  pp["source"],
            "tail_qty_expr":   pp["tail_qty_expr"],
            "tail_phrase_cut": pp["tail_phrase_cut"],
            "qty_multiplier":  pp["qty_multiplier"],
            "qty_fail_reason": pp["fail_reason"],
            "source_line":     source_line,
            "raw_text":        source_line,
            "note_raw":        "",
        }

    for i, line in enumerate(lines):
        if not line:
            continue

        # Skip qty-label lines that were successfully attached
        if i in qty_label_indices:
            continue

        # Line is the parent of a "Количество:" label
        if i in qty_for_line:
            qty, uom = qty_for_line[i]
            row_counter += 1
            rows.append(_make_row(row_counter, line, line, qty, uom, "из текста"))
            continue

        # Numbered item: "1. ..." or "2) ..."
        m_num = _NUMBERED_RE.match(line)
        if m_num:
            item_text = m_num.group(2).strip()
            name, qty, uom = _split_dash_qty(item_text)
            row_counter += 1
            rows.append(_make_row(row_counter, line, name, qty, uom, "из текста"))
            continue

        # Line with inline dash qty: "text - N uom"
        name, qty, uom = _split_dash_qty(line)
        if qty is not None and uom is not None:
            row_counter += 1
            rows.append(_make_row(row_counter, line, name, qty, uom, "из текста"))
            continue

        # Service / note line
        if _is_note_line(line):
            notes.append(line)
            continue

        # Plain text line — try tail extraction via preprocess
        row_counter += 1
        rows.append(_make_row(row_counter, line, line, None, None, "не найдено"))

    # Apply collected notes to all rows
    note_text = "; ".join(notes)
    for row in rows:
        row["note_raw"] = note_text

    return rows
