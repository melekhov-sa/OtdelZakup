"""Heuristic detection of table header rows for Google Document AI output.

detect_header_row(rows, docai_had_header) → HeaderDecision

Determines whether the first row of *rows* is a column header or data.
Uses a scored heuristic (confidence threshold 0.65):

  +0.40  header vocabulary tokens found (Наименование, Кол-во, Ед.изм…)
  -0.55  item patterns found (M/М\d+, \d+×\d+, ГОСТ 1234, strength class)
  -0.30  high numeric density in first row
  +0.08  cells are short (avg < 15 chars)
  +0.07  data rows have clearly numeric columns not matched in first row
  +0.05  DocAI explicitly marked the row as a headerRow

Designed to prevent "first item misidentified as header" silent data loss.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# ── Vocabulary ─────────────────────────────────────────────────────────────────

_HEADER_TOKENS: frozenset[str] = frozenset({
    "наименование", "товар", "номенклатура", "позиция", "поз",
    "ед", "ед.", "ед.изм", "ед.изм.", "единица", "изм",
    "кол-во", "кол.", "количество", "qty", "uom", "amount",
    "price", "цена", "сумма", "итого",
    "код", "артикул", "арт", "арт.",
    "описание", "материал", "марка", "обозначение",
    "покрытие", "примечание", "стандарт", "класс",
    "№", "п/п", "п.п", "n", "no",
    "name", "item", "description", "unit",
})

# Substring matches for partial header keywords
_HEADER_PARTIAL: tuple[str, ...] = (
    "наимен", "ед.изм", "ед изм", "арт.", "обозн", "кол-во",
)

# Item / data patterns — strong indicator the row is product data, not a header
_ITEM_RE: list[re.Pattern] = [
    re.compile(r"\b(?:M|М)\d+\b"),                           # bolt size M10 / М10
    re.compile(r"\d+[xXхХ×]\d+"),                            # dimensions 12x50
    re.compile(r"(?:ГОСТ|DIN|ISO|ИСО)\s*\d", re.IGNORECASE),# standard + number
    re.compile(r"\b(?:8\.8|10\.9|12\.9|4\.8|5\.8|6\.8)\b"),  # strength class
]

_THRESHOLD = 0.65


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class HeaderDecision:
    has_header: bool
    header_row_index: int | None   # always 0 when has_header=True
    confidence: float              # 0..1
    reasons: list[str] = field(default_factory=list)
    header_was_guessed: bool = True  # True = heuristic; False = DocAI-confirmed


# ── Internal helpers ────────────────────────────────────────────────────────────

def _is_header_token(cell: str) -> bool:
    c = cell.lower().strip().rstrip(".")
    if c in _HEADER_TOKENS:
        return True
    cl = c.lower()
    return any(p in cl for p in _HEADER_PARTIAL)


def _has_item_pattern(cell: str) -> bool:
    return any(p.search(cell) for p in _ITEM_RE)


def _is_numeric(cell: str) -> bool:
    try:
        float(cell.strip().replace(",", "."))
        return True
    except (ValueError, AttributeError):
        return False


# ── Public API ─────────────────────────────────────────────────────────────────

def detect_header_row(
    rows: list[list[str]],
    docai_had_header: bool = False,
) -> HeaderDecision:
    """Determine whether the first row of *rows* is a column header.

    Args:
        rows:              All table rows in order (DocAI headerRows first,
                           then bodyRows).  May be empty.
        docai_had_header:  True if DocAI explicitly marked a headerRow.

    Returns:
        HeaderDecision with ``has_header``, ``confidence``, and ``reasons``.
        Never raises — safe to call on any input.
    """
    if not rows:
        return HeaderDecision(False, None, 0.0, ["no_rows"])

    first = [c.strip() for c in rows[0]]
    cells = [c for c in first if c]
    if not cells:
        return HeaderDecision(False, None, 0.0, ["empty_first_row"])

    reasons: list[str] = []
    confidence = 0.5

    # ── DocAI explicitly marked row as header → small boost ──────────────────
    if docai_had_header:
        confidence += 0.05
        reasons.append("docai_marked_header")

    # ── A) Header vocabulary tokens ───────────────────────────────────────────
    token_hits = sum(1 for c in cells if _is_header_token(c))
    token_ratio = token_hits / len(cells)
    if token_ratio > 0:
        bonus = min(token_ratio * 0.45, 0.40)
        confidence += bonus
        reasons.append(f"header_tokens({token_hits}/{len(cells)})")

    # ── B) Item patterns — strong negative signal ────────────────────────────
    item_count = sum(1 for c in cells if _has_item_pattern(c))
    if item_count > 0:
        confidence -= 0.55
        reasons.append(f"item_pattern({item_count}_cells)")

    # ── C) Numeric density ───────────────────────────────────────────────────
    num_count = sum(1 for c in cells if _is_numeric(c))
    num_ratio = num_count / len(cells)
    if num_ratio > 0.3:
        confidence -= num_ratio * 0.30
        reasons.append(f"high_numeric({num_ratio:.0%})")

    # ── D) Short cells → header-like ─────────────────────────────────────────
    avg_len = sum(len(c) for c in cells) / len(cells)
    if avg_len < 15:
        confidence += 0.08
        reasons.append(f"short_cells(avg={avg_len:.0f})")
    elif avg_len > 40:
        confidence -= 0.15
        reasons.append(f"long_cells(avg={avg_len:.0f})")

    # ── E) Data rows have clearly numeric columns not matched in first row ────
    if len(rows) > 2:
        data_rows = rows[1:]
        n_cols = len(rows[0])
        header_signal_cols = 0
        for ci in range(n_cols):
            data_vals = [
                r[ci].strip()
                for r in data_rows
                if ci < len(r) and r[ci].strip()
            ]
            if not data_vals:
                continue
            data_num_ratio = sum(1 for v in data_vals if _is_numeric(v)) / len(data_vals)
            first_cell = first[ci] if ci < len(first) else ""
            if data_num_ratio > 0.5 and not _is_numeric(first_cell):
                header_signal_cols += 1
        if header_signal_cols >= 1:
            confidence += 0.07
            reasons.append(f"data_numeric_cols({header_signal_cols})")

    confidence = round(min(max(confidence, 0.0), 1.0), 4)
    has_header = confidence >= _THRESHOLD
    header_was_guessed = not (docai_had_header and has_header)

    return HeaderDecision(
        has_header=has_header,
        header_row_index=0 if has_header else None,
        confidence=confidence,
        reasons=reasons,
        header_was_guessed=header_was_guessed,
    )
