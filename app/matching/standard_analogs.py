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

# Maps display prefix patterns → canonical prefix (ordered longest-first)
_PREFIX_MAP: list[tuple[str, str]] = [
    ("гост р", "GOST"),
    ("гост",   "GOST"),
    ("iso",    "ISO"),
    ("исо",    "ISO"),
    ("din",    "DIN"),
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

    Queries both src->dst and dst->src directions from the standard_equivalents
    table.  max_depth=1 means direct analogs only (no transitive resolution).

    Returns an empty list if no analogs are found or the table does not yet exist.
    """
    if not standard_norm:
        return []
    try:
        from app.database import get_db_session
        from app.models import StandardEquivalent

        session = get_db_session()
        try:
            rows = (
                session.query(StandardEquivalent)
                .filter(
                    StandardEquivalent.is_active.is_(True),
                    (
                        (StandardEquivalent.src_canonical == standard_norm)
                        | (StandardEquivalent.dst_canonical == standard_norm)
                    ),
                )
                .all()
            )
            result = []
            for row in rows:
                if row.src_canonical == standard_norm:
                    result.append(row.dst_canonical)
                else:
                    result.append(row.src_canonical)
            return result
        finally:
            session.close()
    except Exception:
        return []


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
