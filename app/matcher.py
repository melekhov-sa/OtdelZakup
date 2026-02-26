"""Deterministic matching of supplier rows to our internal catalog items."""

import hashlib

import pandas as pd

from app.database import get_db_session
from app.models import InternalItem, SupplierInternalMatch

_MATCH_THRESHOLD = 80
_FINGERPRINT_KEYS = ("item_type", "size", "diameter", "length", "gost", "iso", "din", "strength", "coating")


def _norm(val) -> str:
    return str(val or "").strip().lower()


def build_fingerprint(row_dict: dict) -> str:
    """Build a deterministic SHA-1 fingerprint from extracted row fields."""
    parts = []
    for key in _FINGERPRINT_KEYS:
        val = _norm(row_dict.get(key, ""))
        if val:
            parts.append(f"{key}={val}")
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def score_candidate(row_dict: dict, item: InternalItem) -> int:
    """Score a catalog item against an extracted row dict. Higher = better match."""
    score = 0

    r_size = _norm(row_dict.get("size"))
    r_diameter = _norm(row_dict.get("diameter"))
    r_length = _norm(row_dict.get("length"))
    r_item_type = _norm(row_dict.get("item_type"))
    r_strength = _norm(row_dict.get("strength"))
    r_coating = _norm(row_dict.get("coating"))

    r_std_parts = [_norm(row_dict.get(k)) for k in ("gost", "iso", "din") if _norm(row_dict.get(k))]
    r_standard = " ".join(r_std_parts)

    i_size = _norm(item.size)
    i_diameter = _norm(item.diameter)
    i_length = _norm(item.length)
    i_item_type = _norm(item.item_type)
    i_strength = _norm(item.strength_class)
    i_coating = _norm(item.material_coating)
    i_standard = _norm(item.standard_text)

    if r_size and i_size and r_size == i_size:
        score += 50
    if r_diameter and i_diameter and r_diameter == i_diameter:
        score += 40
    if r_length and i_length and r_length == i_length:
        score += 40
    if r_standard and i_standard and (r_standard in i_standard or i_standard in r_standard):
        score += 30
    if r_item_type and i_item_type and r_item_type == i_item_type:
        score += 20
    if r_strength and i_strength and r_strength == i_strength:
        score += 10
    if r_coating and i_coating and (r_coating in i_coating or i_coating in r_coating):
        score += 5

    return score


def find_match(row_dict: dict, session=None) -> dict:
    """Find the best matching internal catalog item for a row.

    Returns:
        {
          "source": "memory" | "scored" | "none",
          "fingerprint": str,
          "best": InternalItem | None,
          "score": int,
          "candidates": [{"item_id": int, "name": str, "score": int}, ...]  # top 5
        }
    """
    close_session = False
    if session is None:
        session = get_db_session()
        close_session = True
    try:
        fp = build_fingerprint(row_dict)

        # Check memory first
        mem = session.query(SupplierInternalMatch).filter_by(fingerprint=fp).first()
        if mem:
            item = session.get(InternalItem, mem.internal_item_id)
            if item and item.is_active:
                return {
                    "source": "memory",
                    "fingerprint": fp,
                    "best": item,
                    "score": 999,
                    "candidates": [{"item_id": item.id, "name": item.name, "score": 999}],
                }

        # Score all active items
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
        scored = []
        for item in all_items:
            s = score_candidate(row_dict, item)
            if s > 0:
                scored.append((s, item))

        scored.sort(key=lambda x: -x[0])
        top5 = [{"item_id": item.id, "name": item.name, "score": s} for s, item in scored[:5]]

        best = None
        best_score = 0
        if scored and scored[0][0] >= _MATCH_THRESHOLD:
            best_score, best = scored[0]

        return {
            "source": "scored" if best else "none",
            "fingerprint": fp,
            "best": best,
            "score": best_score,
            "candidates": top5,
        }
    finally:
        if close_session:
            session.close()


def _row_to_dict(row: pd.Series) -> dict:
    """Extract matching-relevant fields from a DataFrame row."""
    result = {}
    for k in _FINGERPRINT_KEYS:
        if k in row.index:
            v = row[k]
            result[k] = "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
        else:
            result[k] = ""
    return result


def add_internal_matches(df_trans: pd.DataFrame) -> tuple:
    """Add 'internal_match' column to df_trans and return (df_trans, match_results).

    match_results is a list (aligned with df_trans rows) of dicts:
        {"source", "fingerprint", "score", "candidates"}
    The "best" key is excluded (item objects can't be serialised).
    """
    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
        all_mem = {
            m.fingerprint: m.internal_item_id
            for m in session.query(SupplierInternalMatch).all()
        }
        item_by_id = {item.id: item for item in all_items}

        match_names: list[str] = []
        match_results: list[dict] = []

        for _, row in df_trans.iterrows():
            row_dict = _row_to_dict(row)
            fp = build_fingerprint(row_dict)

            # Memory hit
            if fp in all_mem:
                iid = all_mem[fp]
                item = item_by_id.get(iid)
                if item:
                    match_names.append(item.name)
                    match_results.append({
                        "source": "memory",
                        "fingerprint": fp,
                        "score": 999,
                        "candidates": [{"item_id": item.id, "name": item.name, "score": 999}],
                    })
                    continue

            if not all_items:
                match_names.append("")
                match_results.append({"source": "none", "fingerprint": fp, "score": 0, "candidates": []})
                continue

            # Score candidates
            scored = [(score_candidate(row_dict, item), item) for item in all_items]
            scored.sort(key=lambda x: -x[0])
            top5 = [{"item_id": item.id, "name": item.name, "score": s} for s, item in scored[:5] if s > 0]

            if scored and scored[0][0] >= _MATCH_THRESHOLD:
                best_score, best_item = scored[0]
                match_names.append(best_item.name)
                match_results.append({
                    "source": "scored",
                    "fingerprint": fp,
                    "score": best_score,
                    "candidates": top5,
                })
            else:
                match_names.append("")
                match_results.append({
                    "source": "none",
                    "fingerprint": fp,
                    "score": scored[0][0] if scored else 0,
                    "candidates": top5,
                })

        df_out = df_trans.copy()
        df_out["internal_match"] = match_names
        return df_out, match_results
    finally:
        session.close()
