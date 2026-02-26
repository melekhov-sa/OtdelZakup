"""Universal qty/uom tail extractor and tail phrase stripper.

extract_qty_uom_from_tail() — enhanced qty/uom extraction from the end of a string.
strip_tail_phrase()         — remove a known stop-phrase from the end of a string.
load_active_tail_phrases()  — load active TailPhrase strings from DB.

Design goals:
- No defaults: if qty found but uom not recognised → return reason, not defaults.
- Handles "тыс." multiplier ("10 тыс. шт." → qty=10000, uom="шт", multiplier=1000).
- Handles no-space format "6шт".
- Handles international pcs/pc; "набор" → "компл"; "пар" etc.
"""

from __future__ import annotations

import re
from typing import Optional

# ── UOM map (superset of parser_excel._UOM_MAP) ───────────────────────────────

_TAIL_UOM_NORMALIZED: dict[str, list[str]] = {
    "шт":    ["штук", "штука", "штуки", "шт", "шт.", "pcs", "pc", "штк"],
    "кг":    ["кг", "кг.", "kg", "килограмм", "килограмма", "килограммов"],
    "г":     ["г", "г.", "гр", "гр.", "граммов", "грамма", "грамм"],
    "м":     ["м", "м.", "метров", "метра", "метр"],
    "мм":    ["мм", "мм.", "mm"],
    "л":     ["л", "л.", "литров", "литра", "литр"],
    "уп":    ["упаковок", "упаковки", "упаковка", "упак", "уп", "уп."],
    "компл": ["комплектов", "комплекта", "комплект", "компл",
              "набор", "набора", "наборов"],
    "пач":   ["пачек", "пачки", "пачка", "пач"],
    "пар":   ["пар", "пара", "пары"],
    "м²":    ["кв.м", "м²", "м2", "кв.м."],
    "м³":    ["куб.м", "м³", "м3", "куб.м."],
    "т":     ["т", "т.", "тонн", "тонна", "тонны", "ton", "tons"],
}

_TAIL_UOM_MAP: dict[str, str] = {}
for _norm_key, _raw_list in _TAIL_UOM_NORMALIZED.items():
    for _raw in _raw_list:
        _TAIL_UOM_MAP[_raw.lower().rstrip(".")] = _norm_key

# Sorted longest-first so "мм" matches before "м", "шт." before "шт", etc.
_TAIL_UOM_ALTS = sorted(_TAIL_UOM_MAP.keys(), key=len, reverse=True)

_NUM_PAT = r"(\d+(?:[.,]\d+)?)"

# ── Pre-compiled regex patterns ───────────────────────────────────────────────

# Matches "<num> тыс[.] <word>" at end of string.
# Group 1 = number, Group 2 = UOM word (may or may not be recognized)
_THOUS_RE = re.compile(
    r"(?<!\S)" + _NUM_PAT + r"\s+тыс\.?\s+(\w+)\.?\s*$"
    + r"|(?<!\S)" + _NUM_PAT + r"\s+тыс\.(шт|кг|г|м|мм|л|уп|компл|пач|пар)\s*$",
    re.IGNORECASE | re.UNICODE,
)

# A simpler, cleaner тыс. pattern — two alternatives:
# A: "10 тыс. шт."  (space between тыс. and UOM)
# B: "10 тыс.шт."   (no space between тыс. and UOM)
_THOUS_RE = re.compile(
    r"(?<!\S)" + _NUM_PAT + r"\s+тыс\.?\s*(\w+)\.?\s*$",
    re.IGNORECASE | re.UNICODE,
)

# Matches "<num><space?><recognized-uom>" at end. The number may be glued to uom.
# Longest-first alternatives prevent "м" from consuming "мм".
_TAIL_UOM_PAT = (
    r"(?<!\S)" + _NUM_PAT
    + r"\s*("
    + "|".join(re.escape(u) for u in _TAIL_UOM_ALTS)
    + r")\.?\s*$"
)
_TAIL_QTY_UOM_RE = re.compile(_TAIL_UOM_PAT, re.IGNORECASE | re.UNICODE)

# Matches "<num> <any-short-word>" at end — used to detect "unknown UOM" condition.
# \s+ (space required) so lone numbers at tail don't trigger.
_UNKNOWN_UOM_RE = re.compile(
    r"(?<!\S)" + _NUM_PAT + r"\s+([а-яёa-zA-Z]{2,12})\.?\s*$",
    re.IGNORECASE | re.UNICODE,
)


def extract_qty_uom_from_tail(
    text: str,
) -> tuple[str, Optional[float], Optional[str], int, Optional[str], Optional[str]]:
    """Extract qty and unit-of-measure from the trailing end of *text*.

    Returns:
        (clean_text, qty, uom, qty_multiplier, matched_expr, reason)

        - clean_text:     text with the matched tail removed (original if no match)
        - qty:            numeric quantity (int if whole number) or None
        - uom:            normalized unit string or None
        - qty_multiplier: 1000 if "тыс." multiplier was used, else 1
        - matched_expr:   the raw matched expression e.g. "10 тыс. шт." or None
        - reason:         "Не распознана единица измерения в хвосте" when a number
                          was found next to an unknown unit; None otherwise

    When qty/uom cannot be determined: qty=None, uom=None are returned.
    The function NEVER substitutes a default UOM.
    """
    t = re.sub(r"[ \t]+", " ", text).strip()

    # ── 1. тыс. (thousand multiplier) ────────────────────────────────────────
    m = _THOUS_RE.search(t)
    if m:
        qty_s = m.group(1).replace(",", ".")
        uom_token = m.group(2).lower().rstrip(".")
        expr = m.group(0).strip()
        uom_norm = _TAIL_UOM_MAP.get(uom_token)
        clean = t[: m.start()].rstrip()

        if uom_norm is not None:
            try:
                qty_raw = float(qty_s) * 1000
                qty: int | float = int(qty_raw) if qty_raw == int(qty_raw) else qty_raw
                return clean, qty, uom_norm, 1000, expr, None
            except ValueError:
                pass
        # Found "тыс" but UOM not recognized
        return t, None, None, 1, expr, "Не распознана единица измерения в хвосте"

    # ── 2. Recognized UOM (with or without space between number and UOM) ─────
    m = _TAIL_QTY_UOM_RE.search(t)
    if m:
        qty_s = m.group(1).replace(",", ".")
        uom_raw = m.group(2).lower().rstrip(".")
        uom_norm = _TAIL_UOM_MAP.get(uom_raw, uom_raw)
        expr = m.group(0).strip()
        clean = t[: m.start()].rstrip()
        try:
            qty_raw = float(qty_s)
            qty = int(qty_raw) if qty_raw == int(qty_raw) else qty_raw
            return clean, qty, uom_norm, 1, expr, None
        except ValueError:
            return t, None, None, 1, None, None

    # ── 3. Number + unrecognized short word → flag as UOM error ──────────────
    m = _UNKNOWN_UOM_RE.search(t)
    if m:
        expr = m.group(0).strip()
        return t, None, None, 1, expr, "Не распознана единица измерения в хвосте"

    return t, None, None, 1, None, None


def strip_tail_phrase(
    text: str,
    phrases: list[str],
) -> tuple[str, Optional[str]]:
    """Strip the longest matching active tail phrase from the end of *text*.

    Comparison is case-insensitive and ignores leading/trailing whitespace.

    Returns:
        (clean_text, matched_phrase_or_None)
    """
    t = text.strip()
    t_lower = t.lower()
    for phrase in sorted(phrases, key=len, reverse=True):
        p = phrase.strip()
        if not p:
            continue
        if t_lower.endswith(p.lower()):
            return t[: -len(p)].rstrip(), phrase
    return t, None


def load_active_tail_phrases() -> list[str]:
    """Return a list of active tail phrase strings from the database.

    Returns an empty list if the table is not yet populated or an error occurs.
    """
    try:
        from app.database import get_db_session  # lazy — avoids circular at module level
        from app.models import TailPhrase
        session = get_db_session()
        try:
            rows = session.query(TailPhrase).filter_by(is_active=True).all()
            return [r.phrase for r in rows]
        finally:
            session.close()
    except Exception:
        return []
