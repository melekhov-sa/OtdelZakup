"""Deterministic regex-based field extractors for fastener descriptions."""

import re

import pandas as pd


def _str(value: object) -> str:
    """Safely cast a cell value to string."""
    if pd.isna(value):
        return ""
    return str(value)


def _concat_row(row: pd.Series) -> str:
    """Join all cell values of a row into a single search string."""
    return " ".join(_str(v) for v in row)


# ── Normalization ────────────────────────────────────────────


def preprocess(text: str) -> str:
    """Normalize text for extraction: lowercase, unify chars, fix numbers."""
    s = text.lower()
    # Cyrillic м before digits -> latin m (metric size context)
    s = re.sub(r"м(\d)", r"m\1", s)
    # Cyrillic х, × -> latin x (size separator)
    s = s.replace("х", "x").replace("×", "x")
    # * → x only between digits (size separator like 8*20); leading * is a bullet marker
    s = re.sub(r"(\d)\*(\d)", r"\1x\2", s)
    # Commas to dots in numbers (4,2 -> 4.2)
    s = re.sub(r"(\d),(\d)", r"\1.\2", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ── Encoded strength tail ─────────────────────────────────────
# Patterns like M20x45.58 → ".58" is strength class 5.8 appended to size
_STRENGTH_TAIL_RE = re.compile(r"\.(129|109|88|98|68|58|56|48|46)(?!\d)$")
_STRENGTH_TAIL_TEXT_RE = re.compile(r"\.(129|109|88|98|68|58|56|48|46)(?!\d)")

_STRENGTH_TAIL_MAP = {
    "129": "12.9", "109": "10.9", "88": "8.8", "98": "9.8",
    "68": "6.8", "58": "5.8", "56": "5.6", "48": "4.8", "46": "4.6",
}

# ── Coded fastener format M{d}.{strength}.{coating_code} ──────
# Examples: M12.6.019, M20.8.016, M16.10.019
# strength = single/double-digit class (4,5,6,8,10,12 for nuts; or compound like 88,109)
# coating_code = 3-digit code (016=zinc, 019=zinc variant)
_CODED_FORMAT_RE = re.compile(
    r"(?<![a-z])m\d{1,2}"            # M + diameter
    r"(?:x\d+)?"                      # optional xLength
    r"\.(\d{1,3})"                    # .strength (1-3 digits)
    r"\.(\d{3})"                      # .coating_code (3 digits)
    r"(?!\d)"
)
# Valid single/double-digit strength classes for nuts/bolts
_CODED_STRENGTH_MAP = {
    "4": "4", "5": "5", "6": "6", "8": "8", "10": "10", "12": "12",
}
# Coating codes (3 digits): 01x = zinc variants
_CODED_COATING_MAP = {
    "016": "цинк 6мкм",
    "019": "цинк хром. 9мкм",
}


def _strip_strength_tail(val: str) -> str:
    """Strip encoded strength class suffix from size/length value.

    '45.58' → '45', '70.129' → '70', '50' → '50' (unchanged).
    """
    return _STRENGTH_TAIL_RE.sub("", val)


# ── Individual extractors ────────────────────────────────────


def extract_diameter(text: str) -> str:
    """Recognise diameter using unified normalization rules, with hardcoded fallback."""
    try:
        from app.services.normalization_service import detect_size as _detect_size  # noqa: PLC0415
        result = _detect_size(text)
        if result is not None and result.extra.get("diameter"):
            return result.extra["diameter"]
    except Exception:
        pass
    try:
        from app.services.size_detector import detect_size  # noqa: PLC0415
        result = detect_size(text)
        if result is not None and result.diameter:
            return result.diameter
    except Exception:
        pass
    # Fallback to hardcoded patterns
    s = preprocess(text)
    m = re.search(r"(?<![a-z.])m\s*(\d{1,2})(?!\d)", s)
    if m:
        return f"M{m.group(1)}"
    # Bare integer NxL without M prefix: "8x20", "14x130" → diameter = M8, M14
    m = re.search(r"(?<!\d[.,])(?<!\d)(\d{1,2})x\d{1,4}(?!\.\d)", s)
    if m:
        return f"M{m.group(1)}"
    return ""


def extract_length(text: str) -> str:
    """Recognise length using unified normalization rules, with hardcoded fallback."""
    try:
        from app.services.normalization_service import detect_size as _detect_size  # noqa: PLC0415
        result = _detect_size(text)
        if result is not None and result.extra.get("length"):
            return _strip_strength_tail(result.extra["length"])
    except Exception:
        pass
    try:
        from app.services.size_detector import detect_size  # noqa: PLC0415
        result = detect_size(text)
        if result is not None and result.length:
            return _strip_strength_tail(result.length)
    except Exception:
        pass
    # Fallback to hardcoded patterns
    s = preprocess(text)
    m = re.search(r"x(\d+)", s)
    return m.group(1) if m else ""


def extract_width(text: str) -> str:
    """Extract width from text using size detection services."""
    try:
        from app.services.normalization_service import detect_size as _detect_size  # noqa: PLC0415
        result = _detect_size(text)
        if result is not None and result.extra.get("width"):
            return result.extra["width"]
    except Exception:
        pass
    try:
        from app.services.size_detector import detect_size  # noqa: PLC0415
        result = detect_size(text)
        if result is not None and result.width:
            return result.width
    except Exception:
        pass
    return ""


def extract_thickness(text: str) -> str:
    """Extract thickness from text using size detection services."""
    try:
        from app.services.normalization_service import detect_size as _detect_size  # noqa: PLC0415
        result = _detect_size(text)
        if result is not None and result.extra.get("thickness"):
            return result.extra["thickness"]
    except Exception:
        pass
    try:
        from app.services.size_detector import detect_size  # noqa: PLC0415
        result = detect_size(text)
        if result is not None and result.thickness:
            return result.thickness
    except Exception:
        pass
    return ""


def extract_screw_diameter(text: str) -> str:
    """Recognise non-metric screw diameters like 4.2 in '4.2x16'.

    Only matches when there is NO metric M-prefix diameter in the text.
    """
    s = preprocess(text)
    if re.search(r"(?<![a-z.])m\s*\d{1,2}(?!\d)", s):
        return ""
    m = re.search(r"(\d+\.\d+)x\d+", s)
    return m.group(1) if m else ""


def extract_thread_type(text: str) -> str:
    """Determine thread type: 'метрическая' or 'саморез'."""
    if extract_diameter(text):
        return "метрическая"
    if extract_screw_diameter(text):
        return "саморез"
    return ""


def _ensure_m_prefix(size_norm: str) -> str:
    """Add M prefix to bare integer DxL sizes: '8x20' → 'M8x20'.

    Only applies when size_norm starts with a digit and has no decimal point
    in the first component (i.e., metric bolt size, not screw 4.2x70).
    """
    if size_norm and re.match(r"\d+x\d", size_norm, re.IGNORECASE) and "." not in size_norm.split("x")[0].split("X")[0]:
        return f"M{size_norm}"
    return size_norm


def extract_size(text: str) -> str:
    """Extract size from text using unified normalization rules, with hardcoded fallback."""
    try:
        from app.services.normalization_service import detect_size as _detect_size  # noqa: PLC0415
        result = _detect_size(text)
        if result is not None:
            raw = result.extra.get("size_norm", result.normalized_code)
            return _strip_strength_tail(_ensure_m_prefix(raw))
    except Exception:
        pass
    try:
        from app.services.size_detector import detect_size  # noqa: PLC0415
        result = detect_size(text)
        if result is not None:
            return _strip_strength_tail(_ensure_m_prefix(result.size_norm))
    except Exception:
        pass
    # Fallback to hardcoded patterns
    d = extract_diameter(text)
    l_ = extract_length(text)
    if d and l_:
        return f"{d}x{l_}"
    sd = extract_screw_diameter(text)
    if sd and l_:
        return f"{sd}x{l_}"
    # Bare integer NxL without M prefix: "8*20", "14x130", "22*150"
    # Common shorthand for metric fasteners (M8x20, M14x130, M22x150)
    s = preprocess(text)
    m = re.search(r"(?<!\d[.,])(?<!\d)(\d{1,3})x(\d{1,4})(?!\.\d)", s)
    if m:
        return f"M{m.group(1)}x{m.group(2)}"
    return ""


def extract_strength(text: str) -> str:
    """Recognise strength class from text using unified normalization rules, with hardcoded fallback."""
    try:
        from app.services.normalization_service import detect_strength  # noqa: PLC0415
        result = detect_strength(text)
        if result is not None:
            return result.normalized_name
    except Exception:
        pass
    try:
        from app.services.strength_detector import detect_strength_class  # noqa: PLC0415
        result = detect_strength_class(text)
        if result is not None:
            return result.strength_name
    except Exception:
        pass
    # Fallback to hardcoded patterns if DB is unavailable
    s = preprocess(text)
    # Explicit class notation — highest priority
    m = re.search(r"(?<!\d)(8\.8|10\.9|12\.9)(?!\d)", s)
    if m:
        return m.group(1)
    # Space-separated variants
    if re.search(r"\b12\s+9\b", s):
        return "12.9"
    if re.search(r"\b10\s+9\b", s):
        return "10.9"
    if re.search(r"\b8\s+8\b", s):
        return "8.8"
    # Encoded tail form (e.g., M20x45.58 → 5.8)
    m = _STRENGTH_TAIL_TEXT_RE.search(s)
    if m:
        return _STRENGTH_TAIL_MAP[m.group(1)]
    # Coded fastener format: M12.6.019 → strength "6"
    m = _CODED_FORMAT_RE.search(s)
    if m:
        code = m.group(1)
        if code in _CODED_STRENGTH_MAP:
            return _CODED_STRENGTH_MAP[code]
        if code in _STRENGTH_TAIL_MAP:
            return _STRENGTH_TAIL_MAP[code]
    return ""


# ── Coating ──────────────────────────────────────────────────

_COATING_PATTERNS = [
    (re.compile(r"нерж|(?<![a-zа-яё0-9])[aа][24](?![a-zа-яё0-9])"), "нержавейка"),
    (re.compile(r"оцинк|оц(?:\b|\.)|(?<![a-zа-яё])цинк"), "цинк"),
    (re.compile(r"латун"), "латунь"),
    (re.compile(r"фосфат"), "фосфат"),
    (re.compile(r"(?<![a-zа-яё])черн|оксид|ворон"), "оксид"),
]
_COATING_CODE_ZINC = re.compile(r"\.016(?!\d)")


def extract_coating(text: str) -> str:
    """Recognise coating from text using unified normalization rules, with hardcoded fallback."""
    try:
        from app.services.normalization_service import detect_coating as _detect_coating  # noqa: PLC0415
        result = _detect_coating(text)
        if result is not None:
            return result.normalized_name
    except Exception:
        pass
    try:
        from app.services.coating_detector import detect_coating  # noqa: PLC0415
        result = detect_coating(text)
        if result is not None:
            return result.coating_name
    except Exception:
        pass
    # Fallback to hardcoded patterns if DB is unavailable
    s = preprocess(text)
    for pattern, result in _COATING_PATTERNS:
        if pattern.search(s):
            return result
    if _COATING_CODE_ZINC.search(s):
        return "цинк"
    # Coded fastener format: M12.6.019 → coating from 3-digit code
    m = _CODED_FORMAT_RE.search(s)
    if m:
        coat_code = m.group(2)
        if coat_code in _CODED_COATING_MAP:
            return _CODED_COATING_MAP[coat_code]
    return ""


# ── Standards ────────────────────────────────────────────────


def extract_gost(text: str) -> str:
    """Extract ГОСТ / gost, normalize. Handles ГОСТ Р, ГОСТ Р ИСО patterns.

    Also detects bare ГОСТ numbers (e.g. "7798-70") when no explicit standard
    keyword is present — common in shortened fastener descriptions like
    "Болт 8*70 8,8 кл.пр. 7798-70".
    """
    s = preprocess(text)
    m = re.search(r"(?:гост|gost)\s*(р\s*)?(?:(исо|iso)\s*)?(\d+[-.]?\d*)", s)
    if m:
        has_r = m.group(1) is not None
        has_iso = m.group(2) is not None
        num = m.group(3)
        if has_r and has_iso:
            return f"ГОСТ Р ИСО {num}"
        if has_r:
            return f"ГОСТ Р {num}"
        return f"ГОСТ {num}"
    # No explicit ГОСТ keyword — try bare ГОСТ numbers (NNNN-NN or NNNNN-NN)
    # Only when no DIN/ISO keyword is present (avoid false positives)
    if not re.search(r"(?:din|iso|исо)\s*\d", s):
        m = re.search(r"(?<!\d)(\d{4,5}-\d{2})(?!\d)", s)
        if m:
            return f"ГОСТ {m.group(1)}"
    return ""


def extract_iso(text: str) -> str:
    """Extract standalone ISO / ИСО, normalize to 'ISO {num}'."""
    s = preprocess(text)
    m = re.search(r"(?:iso|исо)\s*(\d+)", s)
    if not m:
        return ""
    num = m.group(1)
    # Skip if this ISO is part of a ГОСТ Р ИСО reference
    if re.search(r"(?:гост|gost)\s*(?:р\s*)?(?:исо|iso)\s*" + re.escape(num), s):
        return ""
    return f"ISO {num}"


def extract_din(text: str) -> str:
    """Extract DIN standard, normalize to 'DIN {num}'."""
    s = preprocess(text)
    m = re.search(r"din\s*(\d+)", s)
    return f"DIN {m.group(1)}" if m else ""


def extract_tail_code(text: str) -> str:
    """Extract tail code like .88.016 or .109.016."""
    m = re.search(r"\.\d{2,3}\.\d{2,3}", text)
    return m.group(0) if m else ""


def extract_wrench_size(text: str) -> str:
    """Extract wrench size (размер под ключ) from notation like (S27) → '27'.

    Matches S followed by 1-3 digits, typically in parentheses or after a space.
    Examples: (S27) → 27, S27 → 27, (S41) → 41
    """
    s = preprocess(text)
    m = re.search(r"(?<![a-z])s(\d{1,3})(?!\d)", s)
    return m.group(1) if m else ""


# ── Item type ────────────────────────────────────────────────

_ITEM_TYPES = [
    (re.compile(r"саморез"), "саморез"),
    (re.compile(r"шуруп"), "шуруп"),
    (re.compile(r"болт"), "болт"),
    (re.compile(r"винт"), "винт"),
    (re.compile(r"гайк"), "гайка"),
    (re.compile(r"шайб"), "шайба"),
    (re.compile(r"шпильк"), "шпилька"),
    (re.compile(r"анкер"), "анкер"),
]


def extract_item_type(text: str) -> str:
    """Determine item type using the managed product type directory.

    Falls back to hardcoded patterns when the DB is unavailable.
    """
    try:
        from app.product_type_matcher import match_product_type  # noqa: PLC0415
        result = match_product_type(text)
        if result:
            return result
    except Exception:
        pass
    # Hardcoded fallback (used when DB is unavailable or returns no match)
    s = preprocess(text)
    for pattern, name in _ITEM_TYPES:
        if pattern.search(s):
            return name
    return ""


# ── Multi-source helpers ─────────────────────────────────────


def _normalize_strength_raw(text: str) -> str:
    """Parse a strength_raw column value into standard form."""
    if not text:
        return ""
    s = preprocess(text).strip()
    # Direct match
    m = re.search(r"(?<!\d)(8\.8|10\.9|12\.9)(?!\d)", s)
    if m:
        return m.group(1)
    # Single number shorthand
    if s in ("8", "8.0"):
        return "8.8"
    if s in ("10", "10.0"):
        return "10.9"
    if s in ("12", "12.0"):
        return "12.9"
    return extract_strength(text)


def _parse_standard_raw(text: str) -> dict:
    """Parse standard_raw value into {gost: ..., iso: ..., din: ...}."""
    if not text:
        return {}
    result = {}
    g = extract_gost(text)
    if g:
        result["gost"] = g
    i = extract_iso(text)
    if i:
        result["iso"] = i
    d = extract_din(text)
    if d:
        result["din"] = d
    # Bare number like "11371-78" → assume ГОСТ
    if not result:
        s = preprocess(text).strip()
        if re.match(r"^\d+[-.]?\d*$", s):
            result["gost"] = f"ГОСТ {s}"
    return result


# ── Registry ─────────────────────────────────────────────────

EXTRACTORS: dict[str, tuple[str, callable]] = {
    "diameter":        ("Диаметр",          extract_diameter),
    "length":          ("Длина",            extract_length),
    "width":           ("Ширина",           extract_width),
    "thickness":       ("Толщина",          extract_thickness),
    "size":            ("Размер MxL",       extract_size),
    "strength":        ("Класс прочности",  extract_strength),
    "coating":         ("Покрытие",         extract_coating),
    "gost":            ("ГОСТ",             extract_gost),
    "iso":             ("ISO",              extract_iso),
    "din":             ("DIN",              extract_din),
    "tail_code":       ("Хвост-код",        extract_tail_code),
    "wrench_size":     ("Размер под ключ",  extract_wrench_size),
    "screw_diameter":  ("Диаметр самореза", extract_screw_diameter),
    "thread_type":     ("Тип резьбы",       extract_thread_type),
    "item_type":       ("Тип изделия",      extract_item_type),
    "standard_raw":    ("Стандарт (из файла)",      lambda t: ""),
    "strength_raw":    ("Класс пр. (из файла)",     lambda t: ""),
    "note_raw":        ("Примечание (из файла)",     lambda t: ""),
}

# Keys for raw pass-through columns (not real extractors)
_RAW_COL_FIELDS = {"standard_raw", "strength_raw", "note_raw"}

# Field keys shown as UI checkboxes
DEFAULT_FIELD_KEYS = [
    "diameter", "length", "size", "strength", "coating",
    "gost", "iso", "din", "tail_code", "wrench_size",
    "standard_raw", "strength_raw", "note_raw",
]

# All available field keys (including advanced)
ALL_FIELD_KEYS = list(EXTRACTORS.keys())


# ── Confidence & status ──────────────────────────────────────


def compute_confidence(text: str) -> int:
    """Compute simple confidence score (0-5) for a fastener description.

    +1 diameter, +1 length, +1 strength, +1 coating, +1 gost-or-din.
    """
    score = 0
    if extract_diameter(text):
        score += 1
    if extract_length(text):
        score += 1
    if extract_strength(text):
        score += 1
    if extract_coating(text):
        score += 1
    if extract_gost(text) or extract_din(text):
        score += 1
    return score


def compute_status(confidence: int) -> str:
    """Map confidence score to internal status key."""
    if confidence >= 4:
        return "ok"
    if confidence >= 2:
        return "review"
    return "manual"


def _merge_signals(result: pd.DataFrame, df: pd.DataFrame) -> None:
    """Enhance extracted fields using dedicated raw columns (in-place).

    Priority: dedicated column > name extraction > note_raw.
    """
    has_str_raw = "strength_raw" in df.columns
    has_std_raw = "standard_raw" in df.columns
    has_note_raw = "note_raw" in df.columns

    # Strength merging
    if "Класс прочности" in result.columns:
        empty = result["Класс прочности"] == ""
        if has_str_raw and empty.any():
            fill = df.loc[empty, "strength_raw"].apply(
                lambda v: _normalize_strength_raw(_str(v))
            )
            result.loc[empty, "Класс прочности"] = fill
            empty = result["Класс прочности"] == ""
        if has_note_raw and empty.any():
            fill = df.loc[empty, "note_raw"].apply(
                lambda v: extract_strength(_str(v))
            )
            result.loc[empty, "Класс прочности"] = fill

    # Standards merging (gost / iso / din)
    _STD_FIELDS = [("gost", "ГОСТ"), ("iso", "ISO"), ("din", "DIN")]
    for std_key, col_name in _STD_FIELDS:
        if col_name not in result.columns:
            continue
        empty = result[col_name] == ""
        if has_std_raw and empty.any():
            for idx in result.index[empty]:
                raw = _str(df.at[idx, "standard_raw"])
                if raw:
                    parsed = _parse_standard_raw(raw)
                    val = parsed.get(std_key, "")
                    if val:
                        result.at[idx, col_name] = val
            empty = result[col_name] == ""
        if has_note_raw and empty.any():
            extract_fn = {"gost": extract_gost, "iso": extract_iso, "din": extract_din}[std_key]
            fill = df.loc[empty, "note_raw"].apply(lambda v, fn=extract_fn: fn(_str(v)))
            result.loc[empty, col_name] = fill


def transform_dataframe(
    df: pd.DataFrame,
    fields: list[str],
) -> pd.DataFrame:
    """Apply selected extractors to every row and return an augmented DataFrame.

    Uses multi-source signal merging when dedicated columns are present.
    Always appends confidence (0-5) and status (ok/warning/error) columns.
    """
    result = df.copy()

    # Step 1: Run normal extractors (skip raw pass-through keys)
    # Use raw_text column for extraction — it contains name + context but excludes
    # the code column, preventing codes like "BULX10773" from polluting length/size
    # detection via the "x\d+" pattern.
    extract_texts = df["raw_text"].astype(str) if "raw_text" in df.columns else df.apply(_concat_row, axis=1)
    for key in fields:
        if key in _RAW_COL_FIELDS or key not in EXTRACTORS:
            continue
        col_name, func = EXTRACTORS[key]
        result[col_name] = extract_texts.apply(func)

    # Step 2: Multi-source signal merging
    _merge_signals(result, df)

    # Step 3: Confidence & status
    result["confidence"] = extract_texts.apply(compute_confidence)
    result["status"] = result["confidence"].apply(compute_status)

    # Step 4: Handle raw column visibility
    for raw_key in _RAW_COL_FIELDS:
        if raw_key in result.columns:
            if raw_key in fields:
                display = EXTRACTORS[raw_key][0]
                result.rename(columns={raw_key: display}, inplace=True)
            else:
                result.drop(columns=[raw_key], inplace=True)

    return result
