"""Parse free-form text input into structured position rows.

Supported formats:
  A) Numbered list: "1. ...", "2) ..."
  B) Dash qty: "<name> - <N> <uom>" or "<name> — <N> <uom>"
  C) Two-line: position line then "Количество: <N> <uom>" on the next line
  D) Service lines ("все в цинке", etc.) → collected as note_raw

Strict requirement: qty AND uom must both be present to be extracted.
If only one is found, both remain None.
"""

import re
from typing import Optional

from app.parser_excel import parse_qty_uom

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

def parse_text_to_rows(text: str) -> list[dict]:
    """Parse free-form text into a list of structured row dicts.

    Each dict has keys:
      row_number  (int, 1-based)
      name        (str)
      qty         (int|float|None — None if not explicitly found)
      uom         (str|None     — None if not explicitly found)
      source_line (str, original line)
      note_raw    (str, service-line text applied to all rows)

    Both qty and uom must be present for either to be stored.
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
            rows.append(
                {
                    "row_number": row_counter,
                    "name": line,
                    "qty": qty,
                    "uom": uom,
                    "source_line": line,
                    "note_raw": "",
                }
            )
            continue

        # Numbered item: "1. ..." or "2) ..."
        m_num = _NUMBERED_RE.match(line)
        if m_num:
            item_text = m_num.group(2).strip()
            name, qty, uom = _split_dash_qty(item_text)
            row_counter += 1
            rows.append(
                {
                    "row_number": row_counter,
                    "name": name,
                    "qty": qty,
                    "uom": uom,
                    "source_line": line,
                    "note_raw": "",
                }
            )
            continue

        # Line with inline dash qty: "text - N uom"
        name, qty, uom = _split_dash_qty(line)
        if qty is not None and uom is not None:
            row_counter += 1
            rows.append(
                {
                    "row_number": row_counter,
                    "name": name,
                    "qty": qty,
                    "uom": uom,
                    "source_line": line,
                    "note_raw": "",
                }
            )
            continue

        # Service / note line
        if _is_note_line(line):
            notes.append(line)
            continue

        # Plain text line — treat as position without qty/uom (→ manual status)
        row_counter += 1
        rows.append(
            {
                "row_number": row_counter,
                "name": line,
                "qty": None,
                "uom": None,
                "source_line": line,
                "note_raw": "",
            }
        )

    # Apply collected notes to all rows
    note_text = "; ".join(notes)
    for row in rows:
        row["note_raw"] = note_text

    return rows
