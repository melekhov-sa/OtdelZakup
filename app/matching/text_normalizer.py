"""Unified text normalization and tokenization for MinHash signatures.

Provides functions to normalize item text, extract structured prefix tokens,
generate character n-gram shingles, and combine them into token sets for
MinHash-based similarity search.

Does NOT import from app.matcher or app.matching.scorer (avoids circular deps).
"""
from __future__ import annotations

import re

from app.matching.normalizer import normalize_size

# Cyrillic ё/Ё → е/Е
_YO_TABLE = str.maketrans("ёЁ", "еЕ")

# Keep letters (Cyrillic + Latin), digits, spaces, dots, x
_CLEAN_RE = re.compile(r"[^а-яёa-z0-9\s.x]", re.UNICODE)

# Stop words removed from word tokens
_STOP_WORDS = frozenset({
    "для", "при", "без", "над", "под", "про", "все", "или", "как",
    "the", "and", "for", "not", "with",
})

# ── MinHash v2: aggressive normalization for char n-grams ────────────────────

# Cyrillic х/Х → Latin x, Unicode × (U+00D7) → x, * → x
_CYR_X_TABLE = str.maketrans("хХ\u00d7*", "xxxx")

# Strip мм / мм. unit suffix after a digit
_MM_SUFFIX_RE = re.compile(r"(\d)\s*мм\.?(?=\s|$)", re.UNICODE)

# Keep: Cyrillic а-я, Latin a-z, digits 0-9, whitespace, dots, x
_MINHASH_CLEAN_RE = re.compile(r"[^а-яa-z0-9\s.x]", re.UNICODE)


def normalize_for_minhash(text: str) -> str:
    """Aggressive text normalization for MinHash char n-gram signatures.

    Steps:
    1. lowercase
    2. ё → е
    3. Cyrillic х/Х, ×, * → Latin x
    4. Decimal comma between digits → point: '4,8' → '4.8'
    5. Strip мм/мм. unit suffix next to numbers
    6. Hyphens and underscores → space
    7. Remove garbage chars (keep а-я, a-z, 0-9, spaces, dots, x)
    8. Collapse whitespace
    """
    if not text:
        return ""
    s = text.lower()
    s = s.translate(_YO_TABLE)        # ё → е
    s = s.translate(_CYR_X_TABLE)     # х,Х,×,* → x
    s = re.sub(r"(\d),(\d)", r"\1.\2", s)  # decimal comma → point
    s = _MM_SUFFIX_RE.sub(r"\1", s)   # strip мм suffix
    s = s.replace("-", " ").replace("_", " ")  # hyphens/underscores → space
    s = _MINHASH_CLEAN_RE.sub(" ", s)  # remove garbage
    s = re.sub(r"\s+", " ", s).strip()
    return s


def char_ngrams(text: str, n: int = 4) -> set[str]:
    """Generate character n-grams from normalized text.

    Spaces are replaced with '_' to preserve word boundary information
    in the n-gram overlap. For short text (< n chars), returns the text
    itself as a single shingle.
    """
    if not text:
        return set()
    s = text.replace(" ", "_")
    if len(s) < n:
        return {s}
    return {s[i:i + n] for i in range(len(s) - n + 1)}


# ── Legacy functions (kept for backward compatibility) ────────────────────────


def normalize_text(text: str) -> str:
    """Normalize text for MinHash tokenization.

    Steps:
    - lowercase
    - ё → е
    - strip punctuation (keep letters, digits, spaces, dots, x)
    - collapse whitespace
    """
    if not text:
        return ""
    s = text.lower()
    s = s.translate(_YO_TABLE)
    s = _CLEAN_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_shingles(text: str, k: int = 3) -> set[str]:
    """Generate character k-shingles from normalized text."""
    if not text or len(text) < k:
        return {text} if text else set()
    return {text[i:i + k] for i in range(len(text) - k + 1)}


def build_minhash_tokens(
    item_name: str,
    item_type: str = "",
    size: str = "",
    standard_text: str = "",
) -> set[str]:
    """Build a combined token set for MinHash signature construction.

    Combines:
    1. Word tokens from normalized item name
    2. Structural prefix tokens (TYPE:, SIZE:, STD:)
    3. Character 3-shingles for fuzzy coverage
    """
    tokens: set[str] = set()
    norm = normalize_text(item_name)

    # 1. Word tokens (≥ 2 chars, no stop words)
    words = norm.split()
    for w in words:
        if len(w) >= 2 and w not in _STOP_WORDS:
            tokens.add(w)

    # 2. Structural prefix tokens
    if item_type:
        tokens.add(f"TYPE:{item_type.strip().lower()}")

    if size:
        ns = normalize_size(size)
        if ns:
            tokens.add(f"SIZE:{ns}")

    if standard_text:
        std_key = _extract_std_key(standard_text)
        if std_key:
            tokens.add(f"STD:{std_key}")

    # 3. Character 3-shingles
    shingles = build_shingles(norm, k=3)
    tokens.update(shingles)

    return tokens


def _extract_std_key(text: str) -> str | None:
    """Extract standard key from text (lazy import to avoid circular deps)."""
    try:
        from app.standard_normalizer import standard_key_from_text
        return standard_key_from_text(text)
    except Exception:
        return None
