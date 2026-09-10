"""Standard normalization and analog lookup for matching.

Provides:
- normalize_standard(raw) -> canonical key like "GOST-7798-70", "DIN-933", "ISO-4017"
- get_standard_analogs(canonical, max_depth=1) -> list of analog canonical keys
- canonical_to_display(canonical) -> display form for query augmentation
- build_analog_queries(raw_text, row_dict) -> list of AnalogQuery for MinHash rewriting
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.request_cache import TTLCache

_analogs_cache: TTLCache = TTLCache()


def _load_analogs_from_db() -> dict[str, list[str]]:
    from app.database import get_db_session
    from app.models import StandardEquivalent
    session = get_db_session()
    try:
        rows = session.query(StandardEquivalent).filter_by(is_active=True).all()
        result: dict[str, list[str]] = {}

        def _add(key: str, value: str) -> None:
            # Index under both the written key and its year-stripped form, so a
            # row typed as "ГОСТ 7798" still finds the pair stored as
            # "GOST-7798-70".  Which edition was written down does not change
            # which standard is meant.
            for k in {key, strip_edition_year(key)}:
                if not k:
                    continue
                bucket = result.setdefault(k, [])
                if value not in bucket:
                    bucket.append(value)

        for row in rows:
            _add(row.src_canonical, row.dst_canonical)
            _add(row.dst_canonical, row.src_canonical)
        return result
    finally:
        session.close()


def invalidate_standard_analogs_cache() -> None:
    _analogs_cache.invalidate()

# Maps display prefix patterns → canonical prefix (ordered longest-first)
# Order matters: the longest prefix wins. "ГОСТ Р ИСО 4014" is the Russian
# republication of ISO 4014 — the same standard, so it must key as ISO and not
# as a GOST whose number happens to start with the letters "ИСО".
_PREFIX_MAP: list[tuple[str, str]] = [
    ("гост р исо", "ISO"),
    ("гост р iso", "ISO"),
    ("гост исо",   "ISO"),
    ("гост iso",   "ISO"),
    ("гост р",     "GOST"),
    ("гост",       "GOST"),
    ("iso",        "ISO"),
    ("исо",        "ISO"),
    ("din",        "DIN"),
]


def normalize_standard(raw: str) -> str | None:
    """Convert a raw standard string to a canonical key.

    Examples::

        "ГОСТ 7798-70"  -> "GOST-7798-70"
        "ГОСТ Р 52627"  -> "GOST-52627"
        "DIN 933"       -> "DIN-933"
        "ISO 4017"      -> "ISO-4017"
        "DIN  933-A"    -> "DIN-933-A"

    Returns None if no recognized prefix is found.
    """
    if not raw:
        return None
    s = raw.strip()
    sl = s.lower()

    prefix_key: str | None = None
    remainder: str | None = None
    for pat, key in _PREFIX_MAP:
        if sl.startswith(pat):
            prefix_key = key
            remainder = s[len(pat):].strip()
            break

    if prefix_key is None:
        return None

    # Collapse internal spaces to hyphens, strip leading/trailing hyphens
    code = re.sub(r"\s+", "-", remainder.strip()).strip("-")
    if not code:
        return None

    return f"{prefix_key}-{code}"


def _looks_like_year(part: str) -> bool:
    if not part.isdigit():
        return False
    if len(part) == 2:
        return True
    return len(part) == 4 and part[:2] in ("19", "20")


def strip_edition_year(key: str) -> str:
    """Drop the edition year from a canonical key.

    "GOST-7798-70" -> "GOST-7798",  "ISO-4014-2013" -> "ISO-4014".

    The year is only ever the third segment onwards: a key is PREFIX-NUMBER,
    and the number itself must survive — "ISO-4014" has to stay whole rather
    than lose 4014 for looking like a year. A trailing letter is a variant,
    not a year, so "DIN-933-A" is left alone.
    """
    if not key:
        return ""
    parts = key.strip().split("-")
    if len(parts) >= 3 and _looks_like_year(parts[-1]):
        return "-".join(parts[:-1])
    return key.strip()


def same_standard(first: str, second: str) -> bool:
    """Whether two canonical keys name the same standard.

    The catalog holds the same document both with and without its year —
    "GOST-4014" next to "GOST-4014-2013" — because that is how people typed
    it. Which edition was written down does not change which standard is meant.
    """
    if not first or not second:
        return False
    if first == second:
        return True
    return strip_edition_year(first) == strip_edition_year(second)


def canonical_to_display(canonical: str) -> str:
    """Convert a canonical standard key to a display form.

    Used to build augmented query texts for MinHash analog search.

    Examples::

        "GOST-7798-70" -> "ГОСТ 7798-70"  (hyphens in code preserved)
        "DIN-933"      -> "DIN 933"
        "DIN-931-A"    -> "DIN 931-A"    (suffix preserved)
        "ISO-4017"     -> "ISO 4017"
    """
    if canonical.startswith("GOST-"):
        code = canonical[5:]
        return f"ГОСТ {code}"
    if canonical.startswith("DIN-"):
        code = canonical[4:]
        return f"DIN {code}"
    if canonical.startswith("ISO-"):
        code = canonical[4:]
        return f"ISO {code}"
    # Unknown prefix — split on first hyphen only
    parts = canonical.split("-", 1)
    if len(parts) == 2:
        return f"{parts[0]} {parts[1]}"
    return canonical


def get_standard_analogs(standard_norm: str, max_depth: int = 1) -> list[str]:
    """Return list of analogue canonical keys for the given canonical standard.

    Uses an in-process cache of the full standard_equivalents table so that
    repeated calls within a request are O(1) dict lookups instead of DB queries.
    Falls back to the year-stripped key, so "GOST-7798" finds the pair stored
    as "GOST-7798-70".
    """
    if not standard_norm:
        return []
    try:
        data = _analogs_cache.get_or_load(_load_analogs_from_db)
    except Exception:
        return []
    hit = data.get(standard_norm)
    if hit is None:
        hit = data.get(strip_edition_year(standard_norm))
    return list(hit or [])


# ── Analog query rewriting ────────────────────────────────────────────────────

@dataclass
class AnalogQuery:
    """A rewritten query text with one standard replaced by its analog."""
    rewritten_text: str       # raw text with standard substituted
    original_canonical: str   # e.g. "GOST-7798-70"
    analog_canonical: str     # e.g. "DIN-931"
    analog_display: str       # e.g. "DIN 931" (for UI badges)


# Regex patterns to find standard references in raw text (case-insensitive)
_STD_PATTERNS = [
    # ГОСТ Р ИСО / ГОСТ Р ISO — must come before plain ГОСТ
    re.compile(
        r"(?:ГОСТ|гост|GOST|gost)\s*[Рр]\s*(?:ИСО|исо|ISO|iso)\s*(\d[\d.]*(?:-\d+)?)",
        re.UNICODE,
    ),
    # plain ГОСТ
    re.compile(
        r"(?:ГОСТ|гост|GOST|gost)\s*(\d[\d.]*(?:-\d+)?)",
        re.UNICODE,
    ),
    # DIN
    re.compile(r"[Dd][Ii][Nn]\s*(\d[\d.]*(?:-\d+)?)"),
    # ISO / ИСО
    re.compile(
        r"(?:ISO|iso|ИСО|исо)\s*(\d[\d.]*(?:-\d+)?)",
        re.UNICODE,
    ),
]


def build_analog_queries(raw_text: str, row_dict: dict | None = None) -> list[AnalogQuery]:
    """Build rewritten query texts by substituting each standard with its analogs.

    For each standard found in *raw_text*, looks up analogs via
    ``get_standard_analogs()`` and produces one ``AnalogQuery`` per analog
    where the original standard substring is replaced with the analog display
    form (e.g. "ГОСТ 7798-70" → "DIN 931").

    If *row_dict* is provided, also checks the ``gost``/``din``/``iso`` fields
    for standards not present in the raw text itself.

    Returns an empty list when no standards or no analogs are found.
    """
    if not raw_text:
        return []

    # Collect (match_span, canonical_key) from the raw text
    found: list[tuple[re.Match, str]] = []
    used_spans: set[tuple[int, int]] = set()

    for pat in _STD_PATTERNS:
        for m in pat.finditer(raw_text):
            span = (m.start(), m.end())
            # Avoid overlapping matches (e.g. ГОСТ Р ИСО vs plain ГОСТ)
            if any(s[0] <= span[0] < s[1] or s[0] < span[1] <= s[1] for s in used_spans):
                continue
            canonical = normalize_standard(m.group(0).strip())
            if canonical:
                found.append((m, canonical))
                used_spans.add(span)

    results: list[AnalogQuery] = []
    seen_pairs: set[tuple[str, str]] = set()

    for m, canonical in found:
        analogs = get_standard_analogs(canonical)
        for analog_key in analogs:
            pair = (canonical, analog_key)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            display = canonical_to_display(analog_key)
            rewritten = raw_text[:m.start()] + display + raw_text[m.end():]
            results.append(AnalogQuery(
                rewritten_text=rewritten,
                original_canonical=canonical,
                analog_canonical=analog_key,
                analog_display=display,
            ))

    return results


# ── DIN search mode ───────────────────────────────────────────────────────────

# Order matters: a row that already names a DIN stays on that DIN, whatever
# else is written next to it.  Mirrors the field order din -> gost -> iso used
# elsewhere in the matcher.
_DIN_FIRST_PATTERNS = [_STD_PATTERNS[2], _STD_PATTERNS[0], _STD_PATTERNS[1], _STD_PATTERNS[3]]


def row_standard_canonical(row_dict: dict) -> str | None:
    """The canonical standard key of a row: its columns first, then its text."""
    for key in ("din", "gost", "iso"):
        value = str(row_dict.get(key) or "").strip()
        if value:
            canonical = normalize_standard(value)
            if canonical:
                return canonical

    text = str(row_dict.get("name_raw") or row_dict.get("name") or "").strip()
    if not text:
        return None
    for pattern in _DIN_FIRST_PATTERNS:
        m = pattern.search(text)
        if m:
            canonical = normalize_standard(m.group(0).strip())
            if canonical:
                return canonical
    return None


def din_targets_for_row(row_dict: dict) -> list[str]:
    """DIN keys this row must be searched by in the "Poisk po DIN" mode.

    An empty list means "search this row the ordinary way": either the row has
    no recognizable standard, or it is already a DIN, or the reference book
    holds no DIN counterpart for it.
    """
    canonical = row_standard_canonical(row_dict)
    if not canonical or canonical.startswith("DIN-"):
        return []
    return [a for a in get_standard_analogs(canonical) if a.startswith("DIN-")]
