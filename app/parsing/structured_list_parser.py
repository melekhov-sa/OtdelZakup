"""Parsers for OCR text in structured-list and free-text modes.

parse_structured_list(text) — numbered items like "1.1) M12x50 - 296 шт."
parse_free_text(text)       — any lines containing a recognizable qty+uom tail.

Both return a list of ParsedRow dataclasses.
Both use extract_qty_uom_from_tail from tail_extractor for qty/uom extraction.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from app.parsing.tail_extractor import extract_qty_uom_from_tail

# ── Numbering prefix pattern ───────────────────────────────────────────────────
# Matches optional leading whitespace followed by a hierarchical number
# (e.g. "1.", "1.1.", "1.1.2.") ending in a non-decimal separator (")." or space).
# Does NOT match "3.14 " (decimal number without a letter-separator).
#   Positive lookahead (?=\d) keeps "1.1" from matching "1." when next char is digit.
_PREFIX_RE = re.compile(
    r"^\s*(?:\d+\.(?=\d))*\d+[.)]\s+",
    re.UNICODE,
)

# Trailing separator before the qty expression: " - ", " — ", " : ", " | "
# Removed from end of name_raw after qty tail is stripped.
_SEP_RE = re.compile(r"[\s\-—:|]+$")


@dataclass
class ParsedRow:
    raw_line: str
    name_raw: str
    qty: Optional[float]
    uom: Optional[str]
    qty_multiplier: int = 1
    matched_expr: Optional[str] = None
    debug: dict = field(default_factory=dict)


# ── Internal helper ────────────────────────────────────────────────────────────

def _parse_line(raw_line: str, strip_prefix: bool) -> Optional[ParsedRow]:
    """Parse a single line. Returns None if no qty+uom found (skip row)."""
    line = raw_line.strip()
    if not line:
        return None

    # Strip numbering prefix for structured list mode
    working = _PREFIX_RE.sub("", line) if strip_prefix else line
    working = working.strip()

    # Extract qty/uom from tail
    clean, qty, uom, multiplier, expr, reason = extract_qty_uom_from_tail(working)

    if qty is None or uom is None:
        # No qty+uom found → skip row
        return None

    # Strip trailing separator from name portion
    name_raw = _SEP_RE.sub("", clean).strip()

    debug: dict = {"source": "structured_list" if strip_prefix else "free_text"}
    if reason:
        debug["reason"] = reason

    return ParsedRow(
        raw_line=raw_line,
        name_raw=name_raw,
        qty=qty,
        uom=uom,
        qty_multiplier=multiplier,
        matched_expr=expr,
        debug=debug,
    )


# ── Public API ─────────────────────────────────────────────────────────────────

def parse_structured_list(text: str) -> list[ParsedRow]:
    """Parse OCR text as a structured (numbered) list.

    Each line is expected to start with a numbering prefix like "1)", "1.1.",
    "2. ", etc.  The prefix is stripped before qty/uom extraction.
    Lines without a recognizable qty+uom are silently skipped.
    """
    rows: list[ParsedRow] = []
    for line in text.splitlines():
        row = _parse_line(line, strip_prefix=True)
        if row is not None:
            rows.append(row)
    return rows


def parse_free_text(text: str) -> list[ParsedRow]:
    """Parse OCR text as free-form text lines.

    No prefix stripping is done.  Only lines that contain a recognizable
    qty+uom tail are included; all others are silently skipped.
    """
    rows: list[ParsedRow] = []
    for line in text.splitlines():
        row = _parse_line(line, strip_prefix=False)
        if row is not None:
            rows.append(row)
    return rows


def parsed_rows_to_df_data(rows: list[ParsedRow]) -> list[dict]:
    """Convert ParsedRow list to list-of-dicts suitable for pd.DataFrame."""
    result = []
    for r in rows:
        result.append({
            "name":           r.name_raw,
            "qty":            r.qty,
            "uom":            r.uom,
            "qty_uom_source": "из наименования",
            "raw_line":       r.raw_line,
        })
    return result
