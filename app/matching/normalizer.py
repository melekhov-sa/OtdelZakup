"""Text/size normalization utilities for catalog matching."""
from __future__ import annotations

import re

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


def normalize_size(size: str) -> str:
    """Return a canonical lowercase size string for comparison.

    Transforms:
        "M12 x 80"      → "m12x80"
        "М12×80"        → "m12x80"  (Cyrillic М and ×)
        "4,2x70"        → "4.2x70"
        "125x1,6x22мм"  → "125x1.6x22"
        "4.2 x 70мм"    → "4.2x70"
    """
    if not size:
        return ""
    s = size.strip().lower()
    # Normalize multiplication cross variants → ASCII x
    # × (U+00D7), х (U+0445 Cyrillic), and upper Cyrillic Х already lowered to х
    s = s.replace("\u00d7", "x").replace("\u0445", "x")
    # Normalize decimal comma → point
    s = s.replace(",", ".")
    # Strip trailing unit (мм, mm)
    s = re.sub(r"\s*мм\s*$", "", s)
    s = re.sub(r"\s*mm\s*$", "", s)
    # Remove spaces around x (ASCII only at this point)
    s = re.sub(r"\s*x\s*", "x", s)
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
    words = _KW_RE.findall(text.lower())
    return {w for w in words if w not in _STOP_WORDS}
