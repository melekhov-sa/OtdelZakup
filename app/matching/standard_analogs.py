"""Standard normalization and analog lookup for matching.

Provides:
- normalize_standard(raw) -> canonical key like "GOST-7798-70", "DIN-933", "ISO-4017"
- get_standard_analogs(canonical, max_depth=1) -> list of analog canonical keys
- canonical_to_display(canonical) -> display form for query augmentation
"""
from __future__ import annotations

import re

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

        "GOST-7798-70" -> "ГОСТ 7798-70"
        "DIN-933"      -> "DIN 933"
        "ISO-4017"     -> "ISO 4017"
    """
    if canonical.startswith("GOST-"):
        code = canonical[5:].replace("-", " ")
        return f"ГОСТ {code}"
    if canonical.startswith("DIN-"):
        code = canonical[4:].replace("-", " ")
        return f"DIN {code}"
    if canonical.startswith("ISO-"):
        code = canonical[4:].replace("-", " ")
        return f"ISO {code}"
    # Unknown prefix — just replace hyphens with spaces
    return canonical.replace("-", " ")


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
