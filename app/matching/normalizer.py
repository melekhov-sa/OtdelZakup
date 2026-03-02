"""Text/size normalization utilities for catalog matching."""
from __future__ import annotations

import re

# Cyrillic look-alike → Latin transliteration table for size strings.
# Covers: М/м (looks like M/m), Х/х (looks like X/x).
# Applied before lowercasing so that "М 12x60" and "M12x60" normalise identically.
_CYR_TO_LAT_SIZE = str.maketrans("МмХх", "MmXx")

# Volume pattern: "310 мл", "0.5 л" (converted to ml), "310ml"
_VOLUME_ML_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:мл|ml)\b",
    re.IGNORECASE | re.UNICODE,
)
_VOLUME_L_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:л(?!ента)|литр[а-яё]*|litr[a-z]*)\b",
    re.IGNORECASE | re.UNICODE,
)

# Meaningful word extractor (min 3 chars)
_KW_RE = re.compile(r"[а-яёА-ЯЁa-zA-Z]{3,}", re.UNICODE)
_STOP_WORDS = frozenset({
    "для", "при", "без", "над", "под", "про", "все",
    "the", "and", "for", "not",
})

# Excel XML escape sequences like _x0002_, _x0009_ produced by openpyxl
_EXCEL_ESCAPE_RE = re.compile(r"_x[0-9A-Fa-f]{4}_")
# ASCII control characters (except tab \x09 and newline \x0a/\x0d)
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def clean_excel_escapes(text: str) -> str:
    """Remove Excel XML escape sequences and ASCII control chars from text.

    Handles sequences like _x0002_ (STX), _x0009_ (TAB-as-escape), etc.
    that openpyxl may produce when reading files with XML-escaped characters.
    """
    if not text:
        return text
    text = _EXCEL_ESCAPE_RE.sub(" ", text)
    text = _CONTROL_CHAR_RE.sub("", text)
    text = re.sub(r"  +", " ", text).strip()
    return text


def normalize_size(size: str) -> str:
    """Return a canonical uppercase size string for comparison.

    Transforms:
        "M12 x 80"      → "M12X80"
        "М12×80"        → "M12X80"  (Cyrillic М and ×)
        "M-24"          → "M24"     (dash between prefix and digit removed)
        "m 24"          → "M24"     (space removed, uppercased)
        "4,2x70"        → "4.2X70"
        "125x1,6x22мм"  → "125X1.6X22"
        "4.2 x 70мм"    → "4.2X70"
    """
    if not size:
        return ""
    s = clean_excel_escapes(size).strip()
    # Transliterate Cyrillic look-alike letters → Latin (М→M, м→m, Х→X, х→x)
    # Must happen BEFORE upper() so that both "М12x60" and "M12x60" produce "M12X60".
    s = s.translate(_CYR_TO_LAT_SIZE)
    # Normalize Unicode × (U+00D7) → ASCII X
    s = s.replace("\u00d7", "X")
    # Uppercase everything (normalises m→M, x→X, etc.)
    s = s.upper()
    # Remove space or dash between M prefix and following digit: "M 12", "M-12" → "M12"
    s = re.sub(r"M[\s\-]+(\d)", r"M\1", s)
    # Normalize decimal comma → point
    s = s.replace(",", ".")
    # Strip trailing unit MM (Latin; Cyrillic мм already transliterated → mm → MM)
    s = re.sub(r"\s*MM\s*$", "", s)
    # Remove spaces around X separator
    s = re.sub(r"\s*X\s*", "X", s)
    # Remove any remaining whitespace
    s = s.replace(" ", "")
    return s


def parse_size_tokens(normalized_size: str) -> list[float]:
    """Extract numeric components from a normalized size string.

    "m12x80"       → [12.0, 80.0]
    "4.2x70"       → [4.2, 70.0]
    "125x22.2x1.6" → [125.0, 22.2, 1.6]
    "m12"          → [12.0]
    """
    # Remove leading bolt-diameter prefix "m"
    s = re.sub(r"^m", "", normalized_size.lower())
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    try:
        return [float(n) for n in nums]
    except ValueError:
        return []


def sizes_close(a_tokens: list[float], b_tokens: list[float], tol: float = 0.02) -> bool:
    """Return True if all token pairs (sorted) are within `tol` relative tolerance.

    Sorting allows matching reordered dimensions, e.g., [125, 22.2, 1.6] vs [125, 1.6, 22].
    tol=0.02 means ≤ 2% difference per dimension.
    """
    if len(a_tokens) != len(b_tokens) or not a_tokens:
        return False
    for a, b in zip(sorted(a_tokens), sorted(b_tokens)):
        m = max(a, b)
        if m == 0:
            continue
        if abs(a - b) / m > tol:
            return False
    return True


def extract_volume_ml(text: str) -> float | None:
    """Extract volume in ml from text.

    "310 мл" → 310.0
    "0.5 л"  → 500.0
    Returns None if not found.
    """
    if not text:
        return None
    m = _VOLUME_ML_RE.search(text)
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except ValueError:
            pass
    m = _VOLUME_L_RE.search(text)
    if m:
        try:
            return float(m.group(1).replace(",", ".")) * 1000
        except ValueError:
            pass
    return None


def extract_name_keywords(text: str) -> set[str]:
    """Extract meaningful lowercase words (≥ 3 chars) from name text."""
    text = clean_excel_escapes(text)
    words = _KW_RE.findall(text.lower())
    return {w for w in words if w not in _STOP_WORDS}


def extract_row_features(row_dict: dict) -> dict:
    """Extract scoring-relevant features from a row dict for diagnostics.

    Returns a plain dict suitable for JSON serialisation:
        item_type, size_raw, size_tokens, keywords (capped at 10), volume_ml.
    """
    def _n(v: object) -> str:
        return str(v or "").strip().lower()

    item_type = _n(row_dict.get("item_type"))
    size_raw = _n(row_dict.get("size"))
    size_norm = normalize_size(size_raw)
    size_tokens = parse_size_tokens(size_norm) if size_norm else []
    text = _n(row_dict.get("name_raw") or row_dict.get("name") or "")
    keywords = sorted(extract_name_keywords(text)) if text else []
    volume = extract_volume_ml(text)
    return {
        "item_type": item_type,
        "size_raw": size_raw,
        "size_tokens": size_tokens,
        "keywords": keywords[:10],
        "volume_ml": volume,
    }
