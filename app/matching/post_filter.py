"""Post-filter + rerank on top of MinHash candidates.

After MinHash retrieves a raw candidate list, this module applies hard
field-level filters to prune candidates that clearly don't match the row's
extracted fields (size, standard, item_type).

Fallback ladder (when filters remove all candidates):
  Level 0 — all three filters applied (size + standard + type)
  Level 1 — drop standard filter   (size + type only)
  Level 2 — drop type filter        (size only)
  Level 3 — no filters              (return all candidates unfiltered)

filter_log keys
---------------
  minhash_total     : int       — candidate count entering the filter
  fallback_level    : int       — 0=full filters, 3=no filters applied
  steps             : list      — sequential step dicts for UI display
  row_size          : str       — normalized size used for comparison
  row_standard      : str|None  — canonical standard key (e.g. "DIN-933")
  row_type          : str       — item_type from the row
  best_filtered_out : bool      — True when top-Jaccard candidate was removed
                                  by level-0 (full) filter

Each candidate dict passed to / returned from this module gets an added
'field_badges' key::

    {
      "size":     {"match": True|False|None, "label": str},
      "standard": {"match": True|False|None, "label": str},
      "type":     {"match": True|False|None, "label": str},
    }

    match=None  → field absent on row or catalog item; no verdict possible
    match=True  → fields match (or analogs match when use_analogs=True)
    match=False → definite mismatch (both sides have data, they conflict)
"""
from __future__ import annotations


# ── Standard helpers ──────────────────────────────────────────────────────────

def _item_std_canonical(item) -> str | None:
    """Return canonical standard key for a catalog item, or None."""
    from app.matching.standard_analogs import normalize_standard  # noqa: PLC0415
    std_text = str(getattr(item, "standard_text", "") or "").strip()
    return normalize_standard(std_text) if std_text else None


def _row_std_canonical(row_dict: dict) -> str | None:
    """Return canonical standard key extracted from a row dict, or None."""
    from app.matching.standard_analogs import normalize_standard  # noqa: PLC0415
    for k in ("gost", "iso", "din"):
        v = str(row_dict.get(k) or "").strip()
        if v:
            cn = normalize_standard(v)
            if cn:
                return cn
    return None


def _std_group(canonical: str, use_analogs: bool) -> set[str]:
    """Return the equivalence set for a canonical standard key.

    Always contains the canonical itself.  When use_analogs=True, also
    includes direct analogs from the standard_equivalents table.
    """
    group: set[str] = {canonical}
    if use_analogs:
        try:
            from app.matching.standard_analogs import get_standard_analogs  # noqa: PLC0415
            group.update(get_standard_analogs(canonical))
        except Exception:
            pass
    return group


# ── Badge computation ─────────────────────────────────────────────────────────

def compute_candidate_badges(
    candidate: dict,
    row_dict: dict,
    item_by_id: dict,
    row_size_norm: str,
    row_std_canon: str | None,
    use_analogs: bool,
) -> dict:
    """Return field match badges for one candidate dict.

    Each badge: ``{"match": True|False|None, "label": str}``.
    ``None`` means field absent on one side — no verdict possible.
    """
    from app.matching.normalizer import normalize_size  # noqa: PLC0415

    item = item_by_id.get(candidate.get("item_id"))
    if item is None:
        return {}

    badges: dict = {}

    # ── Size ──────────────────────────────────────────────────────────────────
    item_size_raw = str(getattr(item, "size", "") or "").strip()
    item_size_norm = normalize_size(item_size_raw)
    if not row_size_norm or not item_size_norm:
        badges["size"] = {"match": None, "label": item_size_raw or "—"}
    elif row_size_norm == item_size_norm:
        badges["size"] = {"match": True, "label": item_size_raw}
    else:
        badges["size"] = {"match": False, "label": item_size_raw}

    # ── Standard ──────────────────────────────────────────────────────────────
    item_std_canon = _item_std_canonical(item)
    item_std_display = str(getattr(item, "standard_text", "") or "").strip() or "—"
    if not row_std_canon or not item_std_canon:
        badges["standard"] = {"match": None, "label": item_std_display}
    else:
        r_group = _std_group(row_std_canon, use_analogs)
        i_group = _std_group(item_std_canon, use_analogs)
        badges["standard"] = {"match": bool(r_group & i_group), "label": item_std_display}

    # ── Type ──────────────────────────────────────────────────────────────────
    row_type = str(row_dict.get("item_type") or "").strip().lower()
    item_type_raw = str(getattr(item, "item_type", "") or "").strip()
    item_type_norm = item_type_raw.lower()
    if not row_type or not item_type_norm:
        badges["type"] = {"match": None, "label": item_type_raw or "—"}
    elif row_type == item_type_norm:
        badges["type"] = {"match": True, "label": item_type_raw}
    else:
        badges["type"] = {"match": False, "label": item_type_raw}

    return badges


# ── Filter helpers ────────────────────────────────────────────────────────────

def _passes_filter(candidate: dict, field: str) -> bool:
    """True if the candidate passes the given field filter.

    ``match=None`` (no data on one side) always passes — we can't rule it out.
    ``match=False`` (definite conflict) does not pass.
    """
    m = candidate.get("field_badges", {}).get(field, {}).get("match")
    return m is not False


def _apply_filters(
    candidates: list,
    use_size: bool,
    use_std: bool,
    use_type: bool,
) -> list:
    result = []
    for c in candidates:
        if use_size and not _passes_filter(c, "size"):
            continue
        if use_std and not _passes_filter(c, "standard"):
            continue
        if use_type and not _passes_filter(c, "type"):
            continue
        result.append(c)
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def post_filter_candidates(
    all_candidates: list,
    minhash_raw: list,
    row_dict: dict,
    item_by_id: dict,
    settings,
    use_analogs: bool = False,
) -> tuple[list, dict]:
    """Post-filter MinHash candidates using field-level hard filters.

    Parameters
    ----------
    all_candidates:
        Candidates already built by ``_build_minhash_candidates`` (with
        ``score``, ``name``, ``item_id``, ``reasons``, etc.).
    minhash_raw:
        Raw MinHash results sorted by Jaccard desc — used only to track
        which item_id has the highest Jaccard score.
    row_dict:
        Extracted fields for the row (item_type, size, gost, iso, din, …).
    item_by_id:
        Mapping of item_id → InternalItem.
    settings:
        MatchSettings (used for ``use_standard_analogs_in_main_match``).
    use_analogs:
        Whether to treat analog standards as equivalent when filtering.

    Returns
    -------
    (filtered_list, filter_log)
        filtered_list — candidates that survived the filter (each has added
        ``field_badges`` key).  Never empty if all_candidates is non-empty
        (fallback level 3 returns all).
        filter_log — diagnostic dict (see module docstring).
    """
    from app.matching.normalizer import normalize_size  # noqa: PLC0415

    row_size_norm = normalize_size(str(row_dict.get("size") or "").strip())
    row_std_canon = _row_std_canonical(row_dict)
    row_type = str(row_dict.get("item_type") or "").strip().lower()

    filter_log: dict = {
        "minhash_total": len(minhash_raw),
        "fallback_level": 3,
        "steps": [],
        "row_size": row_size_norm,
        "row_standard": row_std_canon,
        "row_type": row_type,
        "best_filtered_out": False,
    }

    if not all_candidates:
        return [], filter_log

    # Annotate each candidate with field_badges (in-place)
    for c in all_candidates:
        c["field_badges"] = compute_candidate_badges(
            c, row_dict, item_by_id, row_size_norm, row_std_canon, use_analogs
        )

    has_size = bool(row_size_norm)
    has_std = bool(row_std_canon)
    has_type = bool(row_type)
    steps = filter_log["steps"]

    # ── Sequential filter counts for UI display ────────────────────────────────
    # Apply filters one at a time to show meaningful before/after counts.
    after_size = [c for c in all_candidates if not has_size or _passes_filter(c, "size")]
    after_size_std = [c for c in after_size if not has_std or _passes_filter(c, "standard")]
    after_all = [c for c in after_size_std if not has_type or _passes_filter(c, "type")]

    if has_size:
        steps.append({
            "name": "size", "value": row_size_norm,
            "before": len(all_candidates), "after": len(after_size),
        })
    if has_std:
        steps.append({
            "name": "standard", "value": row_std_canon,
            "before": len(after_size), "after": len(after_size_std),
        })
    if has_type:
        steps.append({
            "name": "type", "value": row_type,
            "before": len(after_size_std), "after": len(after_all),
        })

    # Track whether the top-Jaccard candidate was removed at level 0
    if minhash_raw:
        top_id = minhash_raw[0]["item_id"]
        filter_log["best_filtered_out"] = all(c["item_id"] != top_id for c in after_all)

    # ── Fallback ladder ────────────────────────────────────────────────────────

    # Level 0: full filter (size + standard + type)
    if after_all:
        filter_log["fallback_level"] = 0
        return after_all, filter_log

    # Level 1: drop standard filter → size + type
    level1 = [c for c in after_size if not has_type or _passes_filter(c, "type")]
    if level1:
        steps.append({
            "name": "fallback_drop_standard",
            "before": len(all_candidates), "after": len(level1),
        })
        filter_log["fallback_level"] = 1
        return level1, filter_log

    # Level 2: drop type filter too → size only
    if after_size:
        steps.append({
            "name": "fallback_drop_type",
            "before": len(all_candidates), "after": len(after_size),
        })
        filter_log["fallback_level"] = 2
        return after_size, filter_log

    # Level 3: no filters — return all candidates
    steps.append({
        "name": "fallback_no_filter",
        "before": len(all_candidates), "after": len(all_candidates),
    })
    filter_log["fallback_level"] = 3
    return all_candidates, filter_log
