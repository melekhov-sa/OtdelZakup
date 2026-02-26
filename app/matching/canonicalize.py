"""Canonical key computation for InternalItem records.

canonical_key format — pipe-separated, non-empty components only:

    type=болт|std=GOST-7798-70|size=12x60
    type=диск|size=1.6x22x125
    type=саморез|size=4.2x70

Components:
    type  — normalized item_type (lowercase, stripped)
    std   — canonical standard key from standard_normalizer ("GOST-7798-70")
    size  — numeric tokens extracted from size field, sorted ascending, joined by "x"
            "M12x60"        → tokens [12, 60]  → "12x60"
            "125x1,6x22мм"  → tokens [1.6, 22, 125] → "1.6x22x125"

Deduplication semantics:
    Same canonical_key means near-duplicate → show only one in candidate list.
    "М 12x60 ГОСТ 7798-70" and "M12x60 ГОСТ 7798-70" produce identical keys.
"""
from __future__ import annotations

from app.matching.normalizer import normalize_size, parse_size_tokens


def _norm_type(item_type: str | None) -> str:
    return str(item_type or "").strip().lower()


def _size_key(size: str | None) -> str:
    """Return sorted numeric tokens as "1.6x22x125"."""
    if not size:
        return ""
    toks = sorted(parse_size_tokens(normalize_size(size)))
    return "x".join(f"{t:g}" for t in toks) if toks else ""


def compute_canonical_key(item) -> str:
    """Compute canonical dedup key for an InternalItem ORM object.

    Uses stored structural fields; falls back to on-the-fly extraction
    from item.name when all fields are empty (items created before the
    extractor pipeline was in place).
    """
    itype   = _norm_type(item.item_type)
    std_key = str(item.standard_key or "").strip()
    sz_key  = _size_key(item.size)

    # Fallback: extract from name when all structural fields are empty
    if not itype and not std_key and not sz_key:
        name_text = ((item.name or "") + " " + (item.name_full or "")).strip()
        if name_text:
            try:
                from app.extractors import extract_item_type, extract_size          # noqa: PLC0415
                from app.standard_normalizer import extract_standards               # noqa: PLC0415
                itype  = (extract_item_type(name_text) or "").strip().lower()
                raw_sz = extract_size(name_text) or ""
                sz_key = _size_key(raw_sz) if raw_sz else ""
                stds   = extract_standards(name_text)
                std_key = stds[0].key if stds else ""
            except Exception:  # noqa: BLE001
                pass

    parts: list[str] = []
    if itype:
        parts.append(f"type={itype}")
    if std_key:
        parts.append(f"std={std_key}")
    if sz_key:
        parts.append(f"size={sz_key}")
    return "|".join(parts)


def compute_canonical_key_from_row(row_dict: dict) -> str:
    """Compute canonical key from a supplier row dict (for diagnostics / trace).

    Mirrors the same component logic as compute_canonical_key but reads
    from the extracted row fields instead of an InternalItem record.
    """
    from app.standard_normalizer import standard_key_from_text  # noqa: PLC0415

    itype = str(row_dict.get("item_type") or "").strip().lower()

    std_key = ""
    for col in ("gost", "iso", "din"):
        val = str(row_dict.get(col) or "").strip()
        if val:
            sk = standard_key_from_text(val)
            if sk:
                std_key = sk
                break

    sz_key = _size_key(str(row_dict.get("size") or ""))

    parts: list[str] = []
    if itype:
        parts.append(f"type={itype}")
    if std_key:
        parts.append(f"std={std_key}")
    if sz_key:
        parts.append(f"size={sz_key}")
    return "|".join(parts)
