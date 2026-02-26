"""Deterministic content-based column scorer for procurement Excel files.

Analyses raw cell values (not header names) to detect which columns are most
likely to contain item names, quantities, and codes.  Used as a pre-fill
source for the manual column-selection UI and to compute detection confidence.

No imports from app.parser_excel — avoids circular dependencies.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


# ── Pattern definitions ────────────────────────────────────────────────────────

# Fastener / hardware item patterns (strong name-column indicator)
_HW_RE = re.compile(
    r"(?:"
    r"[Мм]\s*\d+(?:[.,]?\s*[xх×x]\s*\d+)+"  # M12x80, М12×80
    r"|[Мм]\d+"                                # M10, М12 alone
    r"|ГОСТ\s*\d+"                             # ГОСТ 7798
    r"|DIN\s*\d+"                              # DIN 933
    r"|ISO\s*\d+"                              # ISO 4017
    r"|(?:8|10|4|5|6|12)\.\s*(?:8|9|6)"       # 8.8, 10.9, 4.8 …
    r"|болт|гайка|шайба|винт|анкер|саморез|шпилька|заклёпка|шуруп"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# Quantity + unit-of-measure (simplified, no dependency on parser_excel)
_QTY_UOM_RE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*"
    r"(?:шт(?:ук)?|кг|г(?:р)?|мм|м\d*|л(?:итр)?|уп(?:ак)?|компл(?:ект)?|пач(?:ек)?)\b",
    re.IGNORECASE | re.UNICODE,
)

_PURE_NUM_RE = re.compile(r"^\s*\d+(?:[.,\s]\d+)?\s*$")
_CODE_LIKE_RE = re.compile(r"^[A-Za-zА-Яа-яЁё0-9\-_./]{1,20}$")

# Vocabulary tokens for header-row detection
_HDR_NAME_TOKENS = frozenset(["наименование", "номенклатура", "товар", "позиция"])
_HDR_QTY_TOKENS = frozenset(["количество", "кол-во", "кол во", "колво", "заказ", "потребность"])
_HDR_CODE_TOKENS = frozenset(["код", "артикул", "арт"])


def _s(val: object) -> str:
    """Safe stringify — None / NaN / 'nan' → ''."""
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("none", "nan", "null") else s


# ── Per-column scoring ─────────────────────────────────────────────────────────


def score_name_col(
    col_values: list[str],
    product_type_words: "frozenset[str] | None" = None,
) -> tuple[float, str]:
    """Score a column for likelihood of being the item-name column.

    Args:
        col_values:          Non-empty string values for the column.
        product_type_words:  Optional set of lowercase product-type names/aliases
                             loaded from the managed directory.  When provided,
                             adds a small boost for columns that contain them.

    Returns (score 0..1, human-readable Russian reason string).
    """
    vals = [v for v in col_values if v]
    if not vals:
        return 0.0, ""
    n = len(vals)

    alpha_count = sum(1 for v in vals if re.search(r"[а-яА-ЯёЁa-zA-Z]", v))
    alpha_ratio = alpha_count / n

    avg_len = sum(len(v) for v in vals) / n

    hw_count = sum(1 for v in vals if _HW_RE.search(v))
    hw_ratio = hw_count / n

    num_count = sum(1 for v in vals if _PURE_NUM_RE.match(v))
    num_ratio = num_count / n

    qty_uom_count = sum(1 for v in vals if _QTY_UOM_RE.search(v))
    qty_uom_ratio = qty_uom_count / n

    dim_count = sum(1 for v in vals if _DIM_RE.search(v))
    dim_ratio = dim_count / n

    score = (
        min(alpha_ratio, 1.0) * 0.35
        + min(avg_len / 25.0, 1.0) * 0.25
        + hw_ratio * 0.25
        + (1.0 - num_ratio) * 0.10
        - qty_uom_ratio * 0.10  # penalise combined qty+uom columns
        + dim_ratio * 0.10      # boost for columns with dimensional values
    )

    # Optional boost from managed product-type vocabulary
    if product_type_words:
        pt_count = sum(
            1 for v in vals
            if any(w in v.lower() for w in product_type_words)
        )
        score += (pt_count / n) * 0.10

    score = max(0.0, min(1.0, score))

    reasons: list[str] = []
    if alpha_ratio >= 0.7:
        reasons.append("текстовая")
    if hw_ratio >= 0.2:
        reasons.append("содержит метизы")
    elif hw_ratio > 0:
        reasons.append("есть метизы")
    if dim_ratio >= 0.3:
        reasons.append("есть размеры")
    if avg_len >= 15:
        reasons.append("длинные значения")
    return score, ", ".join(reasons) if reasons else "наиболее текстовая"


def score_qty_col(col_values: list[str]) -> tuple[float, str]:
    """Score a column for likelihood of being the quantity column."""
    vals = [v for v in col_values if v]
    if not vals:
        return 0.0, ""
    n = len(vals)

    qty_uom_count = sum(1 for v in vals if _QTY_UOM_RE.search(v))
    qty_uom_ratio = qty_uom_count / n

    num_count = sum(1 for v in vals if _PURE_NUM_RE.match(v))
    num_ratio = num_count / n

    avg_len = sum(len(v) for v in vals) / n

    hw_count = sum(1 for v in vals if _HW_RE.search(v))
    hw_ratio = hw_count / n

    score = (
        qty_uom_ratio * 0.60
        + num_ratio * 0.25
        + (1.0 - min(avg_len / 20.0, 1.0)) * 0.10
        - hw_ratio * 0.15
    )
    score = max(0.0, min(1.0, score))

    reasons: list[str] = []
    if qty_uom_ratio >= 0.5:
        reasons.append("количество+ед.изм.")
    elif qty_uom_ratio > 0:
        reasons.append("есть кол-во+ед.")
    if num_ratio >= 0.6 and qty_uom_ratio < 0.3:
        reasons.append("числовые значения")
    return score, ", ".join(reasons) if reasons else "числовые данные"


def score_code_col(col_values: list[str]) -> tuple[float, str]:
    """Score a column for likelihood of being the item-code column."""
    vals = [v for v in col_values if v]
    if not vals:
        return 0.0, ""
    n = len(vals)

    short_count = sum(1 for v in vals if len(v) <= 15)
    short_ratio = short_count / n

    code_like_count = sum(1 for v in vals if _CODE_LIKE_RE.match(v))
    code_like_ratio = code_like_count / n

    cyrillic_long = sum(1 for v in vals if re.search(r"[а-яА-ЯёЁ]{4,}", v))
    cyrillic_ratio = cyrillic_long / n

    hw_count = sum(1 for v in vals if _HW_RE.search(v))
    hw_ratio = hw_count / n

    avg_len = sum(len(v) for v in vals) / n

    score = (
        code_like_ratio * 0.45
        + short_ratio * 0.25
        - cyrillic_ratio * 0.20
        - hw_ratio * 0.10
        + (1.0 - min(avg_len / 15.0, 1.0)) * 0.10
    )
    score = max(0.0, min(1.0, score))

    reasons: list[str] = []
    if code_like_ratio >= 0.7:
        reasons.append("код/артикул")
    if short_ratio >= 0.9:
        reasons.append("короткие значения")
    return score, ", ".join(reasons) if reasons else "краткие идентификаторы"


# Dimension patterns — strong name-column signal (M12, 12x80, мм)
_DIM_RE = re.compile(
    r"[Мм]\d+"           # М12, M6 (metric thread prefix)
    r"|\d+[xхxX×]\d+"   # 12x80, 12х80, 12×80
    r"|(?<!\w)мм(?!\w)", # standalone "мм"
    re.UNICODE | re.IGNORECASE,
)

# Known UOM tokens used for uom_col detection
_KNOWN_UOMS = frozenset([
    "шт", "кг", "г", "гр", "м", "мм", "л", "уп", "компл", "пач", "м²", "м³",
    "штук", "штука", "штуки",
    "килограмм", "килограмма",
    "грамм", "граммов", "грамма",
    "метр", "метра", "метров",
    "литр", "литра", "литров",
    "упак", "упаковка", "упаковки",
    "комплект", "комплекта", "комплектов",
    "пачка", "пачки", "пачек",
])


def score_uom_col(col_values: list[str]) -> tuple[float, str]:
    """Score a column for likelihood of being a dedicated unit-of-measure column.

    High score: almost all values are short recognized UOM strings (шт, кг, м, ...).
    Low score: contains digits, long text, or unrecognized tokens.
    Returns (score 0..1, human-readable Russian reason string).
    """
    vals = [v for v in col_values if v]
    if not vals:
        return 0.0, ""
    n = len(vals)

    uom_count = sum(1 for v in vals if v.strip().lower().rstrip(".") in _KNOWN_UOMS)
    uom_ratio = uom_count / n

    short_count = sum(1 for v in vals if len(v) <= 8)
    short_ratio = short_count / n

    num_count = sum(1 for v in vals if _PURE_NUM_RE.match(v))
    num_ratio = num_count / n

    hw_count = sum(1 for v in vals if _HW_RE.search(v))
    hw_ratio = hw_count / n

    score = (
        uom_ratio * 0.70
        + short_ratio * 0.20
        - num_ratio * 0.10
        - hw_ratio * 0.10
    )
    score = max(0.0, min(1.0, score))

    reasons: list[str] = []
    if uom_ratio >= 0.7:
        reasons.append("единицы измерения")
    return score, ", ".join(reasons) if reasons else "короткие значения"


# ── Index-column detection ─────────────────────────────────────────────────────


def detect_index_column(col_values: list[str]) -> bool:
    """Return True if a column looks like a sequential row-number index.

    Criteria (all must pass):
    - No letter characters in any non-empty value.
    - ≥80 % of non-empty values parse as whole integers (no decimals).
    - At least 2 integer values present.
    - The integer sequence is nearly monotonically increasing
      (≤10 % pair violations allowed).
    """
    vals = [v.strip() for v in col_values if v.strip()]
    if not vals:
        return False

    # Reject columns that contain any letter characters
    if any(re.search(r"[A-Za-zА-Яа-яЁё]", v) for v in vals):
        return False

    # Parse to integers (tolerate trailing period "1." and thousand spaces "1 000")
    ints: list[int] = []
    for v in vals:
        clean = v.rstrip(".").replace("\xa0", "").replace(" ", "").replace(",", ".")
        try:
            f = float(clean)
        except ValueError:
            continue  # non-numeric — counts against the 80 % threshold
        if f != int(f):
            return False  # decimal fraction → not a row index
        ints.append(int(f))

    # Need ≥80 % numeric values
    if not ints or len(ints) / len(vals) < 0.80:
        return False

    # Need at least 2 values to verify monotonicity
    if len(ints) < 2:
        return False

    # Check nearly monotonically increasing (allow ≤10 % violations)
    violations = sum(1 for i in range(1, len(ints)) if ints[i] <= ints[i - 1])
    if violations / (len(ints) - 1) > 0.10:
        return False

    return True


# ── Header-row detection ───────────────────────────────────────────────────────


def detect_header_row_from_content(
    values_2d: list[list], max_scan: int = 10
) -> Optional[int]:
    """Scan first max_scan rows for a row that looks like a table header.

    Uses vocabulary matching (наименование, кол-во, код, etc.).
    Returns 0-based row index, or None if no header-like row found with score >= 2.
    """
    best_idx: Optional[int] = None
    best_score = 0

    for i, row in enumerate(values_2d[:max_scan]):
        matched: set[str] = set()
        for val in row:
            s = str(val).strip().lower() if val is not None else ""
            if not s:
                continue
            if any(t in s for t in _HDR_NAME_TOKENS):
                matched.add("name")
            if any(t in s for t in _HDR_QTY_TOKENS):
                matched.add("qty")
            if any(t in s for t in _HDR_CODE_TOKENS):
                matched.add("code")
        # Give extra weight when a name-type token is found
        score = len(matched) * 2 if "name" in matched else len(matched)
        if score > best_score:
            best_score = score
            best_idx = i

    return best_idx if best_score >= 2 else None


# ── Result type ────────────────────────────────────────────────────────────────


@dataclass
class ScorerResult:
    """Result of content-based column scoring."""
    name_idx: Optional[int] = None
    qty_idx: Optional[int] = None
    uom_idx: Optional[int] = None
    code_idx: Optional[int] = None
    header_row: Optional[int] = None
    confidence: float = 0.0
    low_confidence: bool = True
    reasons: dict = field(default_factory=dict)
    index_cols: list = field(default_factory=list)  # columns detected as row-number indices


# ── Main scorer ────────────────────────────────────────────────────────────────


def run_column_scorer(
    values_2d: list[list],
    data_start: Optional[int] = None,
    product_type_words: "frozenset[str] | None" = None,
) -> ScorerResult:
    """Run content-based column scoring on raw 2D cell values.

    If data_start is None, first tries to detect a header row and sets
    data_start = header_row + 1.  If no header found, data_start = 0.

    Args:
        values_2d:           Raw sheet values (list of rows, each a list of cells).
        data_start:          First data row index.  Auto-detected if None.
        product_type_words:  Optional lowercase vocabulary from the managed
                             product-type directory, used to boost name_col scoring.

    Returns ScorerResult with best column assignments, confidence, and index_cols.
    """
    if not values_2d:
        return ScorerResult()

    detected_header: Optional[int] = None
    if data_start is None:
        detected_header = detect_header_row_from_content(values_2d)
        data_start = (detected_header + 1) if detected_header is not None else 0

    data_rows = values_2d[data_start:]
    if not data_rows:
        return ScorerResult(header_row=detected_header)

    num_cols = max((len(row) for row in data_rows), default=0)
    if num_cols == 0:
        return ScorerResult(header_row=detected_header)

    # Build per-column string lists from data rows
    cols: list[list[str]] = []
    for col_idx in range(num_cols):
        col_vals = [
            _s(row[col_idx] if col_idx < len(row) else None)
            for row in data_rows
        ]
        cols.append(col_vals)

    # ── Detect index columns and pre-exclude them from role assignment ─────────
    index_col_flags = [detect_index_column(c) for c in cols]
    index_col_indices = [i for i, flag in enumerate(index_col_flags) if flag]

    # Score each column for each role
    name_scores = [score_name_col(c, product_type_words=product_type_words) for c in cols]
    qty_scores = [score_qty_col(c) for c in cols]
    uom_scores = [score_uom_col(c) for c in cols]
    code_scores = [score_code_col(c) for c in cols]

    # ── Greedy assignment: name first, then qty, then uom, then code ──────────
    # Index columns are pre-excluded: they cannot be assigned any semantic role.
    assigned: set[int] = set(index_col_indices)

    # Name: best name_score column (no minimum threshold — always assign)
    name_idx: Optional[int] = None
    best_name_score = 0.0
    for i, (sc, _) in enumerate(name_scores):
        if sc > best_name_score:
            best_name_score = sc
            name_idx = i
    if name_idx is not None:
        assigned.add(name_idx)

    # Qty: best qty_score among unassigned (threshold 0.15)
    qty_idx: Optional[int] = None
    best_qty_score = 0.15
    for i, (sc, _) in enumerate(qty_scores):
        if i in assigned:
            continue
        if sc > best_qty_score:
            best_qty_score = sc
            qty_idx = i
    if qty_idx is not None:
        assigned.add(qty_idx)

    # UOM: best uom_score among unassigned (threshold 0.50 — must be confident)
    uom_idx: Optional[int] = None
    best_uom_score = 0.50
    for i, (sc, _) in enumerate(uom_scores):
        if i in assigned:
            continue
        if sc > best_uom_score:
            best_uom_score = sc
            uom_idx = i
    if uom_idx is not None:
        assigned.add(uom_idx)

    # Code: best code_score among unassigned (threshold 0.25)
    code_idx: Optional[int] = None
    best_code_score = 0.25
    for i, (sc, _) in enumerate(code_scores):
        if i in assigned:
            continue
        if sc > best_code_score:
            best_code_score = sc
            code_idx = i

    # One-column case: no separate qty, uom or code column
    if num_cols == 1:
        qty_idx = None
        uom_idx = None
        code_idx = None

    # ── Confidence ─────────────────────────────────────────────────────────────
    ns = name_scores[name_idx][0] if name_idx is not None else 0.0
    qs = qty_scores[qty_idx][0] if qty_idx is not None else 0.0
    overall_conf = round(ns * 0.65 + qs * 0.35, 3)
    low_confidence = overall_conf < 0.40 or name_idx is None

    # ── Reasons ────────────────────────────────────────────────────────────────
    reasons: dict[str, str] = {}
    if name_idx is not None and name_scores[name_idx][1]:
        reasons["name_col"] = name_scores[name_idx][1]
    if qty_idx is not None and qty_scores[qty_idx][1]:
        reasons["qty_col"] = qty_scores[qty_idx][1]
    if uom_idx is not None and uom_scores[uom_idx][1]:
        reasons["uom_col"] = uom_scores[uom_idx][1]
    if code_idx is not None and code_scores[code_idx][1]:
        reasons["code_col"] = code_scores[code_idx][1]

    return ScorerResult(
        name_idx=name_idx,
        qty_idx=qty_idx,
        uom_idx=uom_idx,
        code_idx=code_idx,
        header_row=detected_header,
        confidence=overall_conf,
        low_confidence=low_confidence,
        reasons=reasons,
        index_cols=index_col_indices,
    )
