"""DocAI table column detection and OCR-aware qty/uom parsing.

Canonical pipeline for Google Document AI tabular output:
  1. ExtractResult.rows (body rows) + header_row (list[str])
  2. detect_columns(headers, rows) → col_map
  3. build_canonical_df(col_df, headers, col_map) → proper DataFrame
     with name / qty / uom / qty_uom_source / _docai_extra_* columns

Extra columns (_docai_extra_*) are used by field extractors via _concat_row
but are hidden from the display table (added to _INTERNAL_COLS in main.py).
"""
from __future__ import annotations

import re
from typing import Optional

# ── UOM vocabulary ────────────────────────────────────────────────────────────

_UOM_NORMALIZED: dict[str, list[str]] = {
    "шт":    ["штук", "штука", "штуки", "шт", "шт.", "pcs", "pc", "штк"],
    "кг":    ["кг", "кг.", "kg", "килограмм", "килограмма", "килограммов"],
    "г":     ["г", "г.", "гр", "гр.", "граммов", "грамма", "грамм"],
    "м":     ["м", "м.", "метров", "метра", "метр"],
    "мм":    ["мм", "мм.", "mm"],
    "л":     ["л", "л.", "литров", "литра", "литр"],
    "уп":    ["упаковок", "упаковки", "упаковка", "упак", "уп", "уп."],
    "компл": ["комплектов", "комплекта", "комплект", "компл", "набор", "набора", "наборов"],
    "пач":   ["пачек", "пачки", "пачка", "пач"],
    "пар":   ["пар", "пара", "пары"],
    "м²":    ["кв.м", "м²", "м2", "кв.м."],
    "м³":    ["куб.м", "м³", "м3", "куб.м."],
    "т":     ["т", "т.", "тонн", "тонна", "тонны", "ton", "tons"],
    "рул":   ["рул", "рул.", "рулон", "рулонов"],
}

_UOM_MAP: dict[str, str] = {}
for _k, _vv in _UOM_NORMALIZED.items():
    for _v in _vv:
        _UOM_MAP[_v.lower().rstrip(".")] = _k

# Sorted longest-first so "мм" beats "м", "шт." beats "шт", etc.
_UOM_ALTS = sorted(_UOM_MAP.keys(), key=len, reverse=True)

# ── Regex patterns ────────────────────────────────────────────────────────────

_NUM_PAT = r"(\d+(?:[.,]\d+)?)"

# тыс. multiplier: "2,5 тыс. шт"  "10 тыс шт"
_THOUS_RE = re.compile(
    _NUM_PAT + r"\s+тыс\.?\s*(\w+)\.?\s*$",
    re.IGNORECASE | re.UNICODE,
)

# qty then UOM (with or without space): "250 кг" "250кг"
_QTY_UOM_RE = re.compile(
    r"(?:^|\s)" + _NUM_PAT
    + r"\s*(" + "|".join(re.escape(u) for u in _UOM_ALTS) + r")\.?\s*$",
    re.IGNORECASE | re.UNICODE,
)

# UOM then qty: "КГ 4"  "ШТ 250"
_UOM_QTY_RE = re.compile(
    r"^(" + "|".join(re.escape(u) for u in _UOM_ALTS) + r")\.?\s+" + _NUM_PAT + r"\s*$",
    re.IGNORECASE | re.UNICODE,
)

# OCR junk prefix before a lone UOM: "N КГ"  "- кг"  "№ шт"
_JUNK_UOM_RE = re.compile(
    r"^[NnNАа№\-\s]+(" + "|".join(re.escape(u) for u in _UOM_ALTS) + r")\.?\s*$",
    re.IGNORECASE | re.UNICODE,
)

# Prefix for extra (non-name/qty/uom) DocAI columns in the canonical df
EXTRA_COL_PREFIX = "_docai_extra_"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_float(s: str) -> Optional[float]:
    try:
        return float(s.strip().replace(",", "."))
    except (ValueError, AttributeError):
        return None


def _uom_lookup(s: str) -> Optional[str]:
    return _UOM_MAP.get(s.lower().strip().rstrip("."))


def _extract_uom_from_text(text: str) -> Optional[str]:
    """Find a UOM keyword in arbitrary text (used for header hints)."""
    t = text.lower()
    for raw_uom in _UOM_ALTS:
        if re.search(r"(?<!\w)" + re.escape(raw_uom) + r"(?!\w)", t):
            return _UOM_MAP[raw_uom]
    return None


# ── Public: qty/uom parser ────────────────────────────────────────────────────

def parse_qty_uom(
    qty_raw: Optional[str],
    uom_raw: Optional[str],
    header_hint: Optional[str] = None,
) -> tuple[Optional[float], Optional[str], str]:
    """Parse quantity and unit from raw DocAI cell values.

    Handles OCR errors, transposed columns, тыс. multiplier, and header hints.

    Args:
        qty_raw:     raw value of the quantity cell (may be None / empty).
        uom_raw:     raw value of the UOM cell (may be None / empty).
        header_hint: text of the qty column header, e.g. "Кол-во, шт".

    Returns:
        (qty, uom, source) where source is one of:
          "direct"            — qty_raw numeric, uom_raw recognized
          "uom_before_qty"    — "КГ 4" pattern in uom cell
          "ocr_junk_uom"      — "N КГ" pattern (junk prefix stripped)
          "num_uom_in_uom_col"— "3 КГ" / "250 шт" in uom cell
          "split_qty_col"     — "250 кг" in qty cell
          "thous_mult"        — тыс. in uom cell, qty from qty cell
          "thous_in_uom"      — "2,5 тыс. шт" fully in uom cell
          "thous_in_qty"      — "2,5 тыс. шт" fully in qty cell
          "header_hint"       — qty numeric, uom from header text
          "not_found"         — could not determine both qty and uom
    """
    qs = (qty_raw or "").strip()
    us = (uom_raw or "").strip()

    # ── 1. тыс. multiplier inside the uom cell ───────────────────────────────
    if "тыс" in us.lower():
        m = _THOUS_RE.search(us)
        if m:
            qty_v = _to_float(m.group(1))
            uom_n = _uom_lookup(m.group(2))
            if qty_v is not None and uom_n:
                return qty_v * 1000, uom_n, "thous_in_uom"
        # тыс. keyword in uom cell; try qty from qty_col
        m2 = re.search(r"тыс\.?\s*(\w+)", us, re.IGNORECASE)
        uom_token = m2.group(1) if m2 else ""
        uom_n = _uom_lookup(uom_token)
        qty_v = _to_float(qs)
        if qty_v is not None and uom_n:
            return qty_v * 1000, uom_n, "thous_mult"

    # ── 2. тыс. multiplier inside the qty cell ───────────────────────────────
    if "тыс" in qs.lower():
        m = _THOUS_RE.search(qs)
        if m:
            qty_v = _to_float(m.group(1))
            uom_n = _uom_lookup(m.group(2))
            if qty_v is not None and uom_n:
                return qty_v * 1000, uom_n, "thous_in_qty"

    # ── 3. UOM before qty: "КГ 4"  "ШТ 250" ─────────────────────────────────
    m = _UOM_QTY_RE.match(us)
    if m:
        uom_n = _uom_lookup(m.group(1))
        qty_v = _to_float(m.group(2))
        if uom_n and qty_v is not None:
            return qty_v, uom_n, "uom_before_qty"

    # ── 4. Direct: pure numeric qty + recognized UOM ─────────────────────────
    qty_v = _to_float(qs) if qs else None
    uom_n = _uom_lookup(us) if us else None
    if qty_v is not None and uom_n is not None:
        return qty_v, uom_n, "direct"

    # ── 5. OCR junk prefix in uom cell: "N КГ"  "- кг" ─────────────────────
    m = _JUNK_UOM_RE.match(us)
    if m and qty_v is not None:
        uom_n = _uom_lookup(m.group(1))
        if uom_n:
            return qty_v, uom_n, "ocr_junk_uom"

    # ── 6. qty+uom together in uom cell: "3 КГ"  "250 шт" ───────────────────
    m = _QTY_UOM_RE.search(us)
    if m:
        qty_v2 = _to_float(m.group(1))
        uom_n2 = _uom_lookup(m.group(2))
        if qty_v2 is not None and uom_n2:
            return qty_v2, uom_n2, "num_uom_in_uom_col"

    # ── 7. qty+uom together in qty cell: "250 кг" ────────────────────────────
    m = _QTY_UOM_RE.search(qs)
    if m:
        qty_v2 = _to_float(m.group(1))
        uom_n2 = _uom_lookup(m.group(2))
        if qty_v2 is not None and uom_n2:
            return qty_v2, uom_n2, "split_qty_col"

    # ── 8. Header hint (qty col header contains UOM like "Кол-во, шт") ───────
    if header_hint and qty_v is not None:
        hint_uom = _extract_uom_from_text(header_hint)
        if hint_uom:
            return qty_v, hint_uom, "header_hint"

    return None, None, "not_found"


# ── Public: column detection ──────────────────────────────────────────────────

def detect_columns(
    headers: list[str],
    rows: list[list[str]],
) -> dict:
    """Auto-detect name/qty/uom column indices from headers and body rows.

    Returns dict with:
        name_idx: int   (always set; defaults to longest-text column)
        qty_idx:  int | None
        uom_idx:  int | None
        header_hints: dict[str, str]   (col_idx_str → normalized_uom)
    """
    if not rows:
        return {"name_idx": 0, "qty_idx": None, "uom_idx": None, "header_hints": {}}

    n_cols = max(len(r) for r in rows)
    if n_cols == 0:
        return {"name_idx": 0, "qty_idx": None, "uom_idx": None, "header_hints": {}}

    padded = [r + [""] * (n_cols - len(r)) for r in rows]

    text_lengths: list[float] = []
    numeric_ratio: list[float] = []
    uom_ratio: list[float] = []

    for ci in range(n_cols):
        vals = [r[ci].strip() for r in padded if r[ci].strip()]
        if not vals:
            text_lengths.append(0.0)
            numeric_ratio.append(0.0)
            uom_ratio.append(0.0)
            continue

        avg_len = sum(len(v) for v in vals) / len(vals)
        num_count = sum(1 for v in vals if _to_float(v) is not None)
        uom_count = sum(
            1 for v in vals
            if _uom_lookup(v) is not None
            or _JUNK_UOM_RE.match(v) is not None
            or _UOM_QTY_RE.match(v) is not None
            or _QTY_UOM_RE.search(v) is not None
        )
        text_lengths.append(avg_len)
        numeric_ratio.append(num_count / len(vals))
        uom_ratio.append(uom_count / len(vals))

    # Header-based bonuses and UOM hints
    uom_bonus = [0.0] * n_cols
    qty_bonus = [0.0] * n_cols
    header_hints: dict[str, str] = {}

    for ci, h in enumerate(headers[:n_cols]):
        hl = h.lower()
        if any(kw in hl for kw in ["ед.", "ед.изм", "unit", "единиц"]):
            uom_bonus[ci] = 0.5
        if any(kw in hl for kw in ["кол", "кол-во", "qty", "количеств", "count"]):
            qty_bonus[ci] = 0.5
            hint = _extract_uom_from_text(h)
            if hint:
                header_hints[str(ci)] = hint

    uom_scores = [uom_ratio[ci] + uom_bonus[ci] for ci in range(n_cols)]
    qty_scores = [numeric_ratio[ci] + qty_bonus[ci] for ci in range(n_cols)]

    # Pick UOM column
    uom_idx: Optional[int] = None
    if n_cols > 1:
        best = max(range(n_cols), key=lambda i: uom_scores[i])
        if uom_scores[best] > 0.2:
            uom_idx = best

    # Pick QTY column (must differ from uom_idx)
    qty_idx: Optional[int] = None
    if n_cols > 1:
        cands = [i for i in range(n_cols) if i != uom_idx]
        if cands:
            best = max(cands, key=lambda i: qty_scores[i])
            if qty_scores[best] > 0.2:
                qty_idx = best

    # Pick NAME column: longest avg text, excluding qty/uom
    excluded = {i for i in (uom_idx, qty_idx) if i is not None}
    name_cands = [i for i in range(n_cols) if i not in excluded]
    name_idx = max(name_cands, key=lambda i: text_lengths[i]) if name_cands else 0

    return {
        "name_idx": name_idx,
        "qty_idx": qty_idx,
        "uom_idx": uom_idx,
        "header_hints": header_hints,
    }


# ── Public: canonical DataFrame builder ──────────────────────────────────────

def build_canonical_df(
    col_df: "pd.DataFrame",
    headers: list[str],
    col_map: dict,
) -> "pd.DataFrame":
    """Convert a raw multi-column DocAI DataFrame into a canonical DataFrame.

    Input:  col_df with columns col_0, col_1, ..., col_N-1.
    Output: DataFrame with columns:
              name, qty, uom, qty_uom_source,
              _docai_extra_<i>  (one per non-mapped column).

    The extra columns contribute to _concat_row() in transform_dataframe so
    that field extractors (coating, standard, etc.) can use any column value.
    They are listed in _INTERNAL_COLS in main.py and hidden from the display.
    """
    import pandas as pd  # lazy import

    name_idx: int = col_map.get("name_idx") or 0
    qty_idx: Optional[int] = col_map.get("qty_idx")
    uom_idx: Optional[int] = col_map.get("uom_idx")
    hints: dict[str, str] = col_map.get("header_hints") or {}

    n_cols = len(col_df.columns)

    def _get(row: "pd.Series", idx: Optional[int]) -> str:
        if idx is None or idx >= n_cols:
            return ""
        v = row.get(f"col_{idx}", "")
        return str(v).strip() if v is not None and str(v) != "nan" else ""

    rows_out: list[dict] = []
    for _, row in col_df.iterrows():
        name_raw = _get(row, name_idx)
        qty_raw  = _get(row, qty_idx)
        uom_raw  = _get(row, uom_idx)
        header_hint = hints.get(str(qty_idx)) if qty_idx is not None else None

        qty, uom, source = parse_qty_uom(qty_raw, uom_raw, header_hint)
        if qty is not None and qty == int(qty):
            qty = int(qty)

        out: dict = {
            "name":           name_raw,
            "qty":            qty,
            "uom":            uom,
            "qty_uom_source": source,
        }

        # Extra columns (not name/qty/uom) → passed to field extractors
        for ci in range(n_cols):
            if ci in (name_idx, qty_idx, uom_idx):
                continue
            val = _get(row, ci)
            out[f"{EXTRA_COL_PREFIX}{ci}"] = val

        rows_out.append(out)

    return pd.DataFrame(rows_out) if rows_out else pd.DataFrame(
        columns=["name", "qty", "uom", "qty_uom_source"]
    )
