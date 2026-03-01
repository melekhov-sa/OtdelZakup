"""Deterministic matching of supplier rows to our internal catalog items."""

import hashlib

import pandas as pd

from app.database import get_db_session
from app.models import InternalItem, SupplierInternalMatch

# Kept for backwards compatibility with existing tests / find_match()
_MATCH_THRESHOLD = 80

_FINGERPRINT_KEYS = (
    "item_type", "size", "diameter", "length",
    "gost", "iso", "din", "strength", "coating",
)

# ── Match mode constants ──────────────────────────────────────────────────────

MATCH_MODE_AUTO_MEMORY  = "AUTO_MEMORY"
MATCH_MODE_AUTO_SCORE   = "AUTO_SCORE"
MATCH_MODE_AUTO_MINHASH = "AUTO_MINHASH"
MATCH_MODE_SUGGESTED    = "SUGGESTED"
MATCH_MODE_NONE         = "NONE"
MATCH_MODE_MANUAL       = "MANUAL_SELECTED"
MATCH_MODE_CONFIRMED    = "CONFIRMED"

MATCH_MODE_LABELS = {
    MATCH_MODE_AUTO_MEMORY:  "Авто (память)",
    MATCH_MODE_AUTO_SCORE:   "Авто (скоринг)",
    MATCH_MODE_AUTO_MINHASH: "Авто (MinHash J)",
    MATCH_MODE_SUGGESTED:    "Предложено",
    MATCH_MODE_NONE:         "Нет",
    MATCH_MODE_MANUAL:       "Вручную",
    MATCH_MODE_CONFIRMED:    "Подтверждено",
}


def _norm(val) -> str:
    return str(val or "").strip().lower()


def _score_item(row_dict: dict, item: InternalItem, settings=None) -> dict:
    """Return score_match result for row_dict vs item (lazy import avoids circular)."""
    from app.matching.scorer import score_match
    return score_match(row_dict, item, settings=settings)


def _row_std_keys(row_dict: dict) -> set[str]:
    """Compute canonical standard keys from a row's gost/iso/din fields.

    Falls back to extracting standards from name_raw / name when all three
    standard columns are empty (mirrors the behaviour in scorer._get_row_std_keys).
    """
    from app.standard_normalizer import extract_standards, standard_key_from_text
    keys: set[str] = set()
    for k in ("gost", "iso", "din"):
        val = _norm(row_dict.get(k))
        if val:
            sk = standard_key_from_text(val)
            if sk:
                keys.add(sk)
    if not keys:
        r_text = str(row_dict.get("name_raw") or row_dict.get("name") or "").strip()
        if r_text:
            keys = {t.key for t in extract_standards(r_text)}
    return keys


def _query_minhash(row_dict: dict, item_by_id: dict, settings) -> list:
    """Query MinHash index and return deduped candidates sorted by Jaccard desc.

    Returns list of {"item_id": int, "name": str, "jaccard": float}.
    """
    if not settings.enable_minhash:
        return []

    from app.matching.minhash_index import is_index_ready, query_index_with_scores  # noqa: PLC0415
    if not is_index_ready():
        return []

    r_text = str(row_dict.get("name_raw") or row_dict.get("name") or "").strip()
    r_type = _norm(row_dict.get("item_type"))
    r_size = _norm(row_dict.get("size"))
    r_std  = ""
    for k in ("gost", "iso", "din"):
        v = _norm(row_dict.get(k))
        if v:
            r_std = v
            break

    mh_results = query_index_with_scores(
        r_text, item_type=r_type, size=r_size, standard_text=r_std,
        top_k=settings.minhash_top_k,
        use_type_buckets=settings.use_type_buckets,
        min_candidates_before_fallback=settings.min_candidates_before_fallback,
    )

    raw = []
    for r in mh_results:
        iid = r["item_id"]
        it  = item_by_id.get(iid)
        if it:
            raw.append({"item_id": iid, "name": it.name, "jaccard": r["jaccard"]})
    return _dedup_minhash_raw(raw)


def _dedup_minhash_raw(minhash_raw: list) -> list:
    """Deduplicate MinHash candidates by item_id, keeping highest Jaccard score."""
    seen: dict[int, dict] = {}
    for r in minhash_raw:
        iid = r["item_id"]
        if iid not in seen or r["jaccard"] > seen[iid]["jaccard"]:
            seen[iid] = r
    return sorted(seen.values(), key=lambda x: -x["jaccard"])


def _build_minhash_candidates(minhash_raw: list, item_by_id: dict, limit: int = 10) -> list:
    """Build deduplicated candidate dicts from MinHash results.

    Deduplicates by canonical_key so near-duplicate catalog entries collapse
    into a single representative (same behaviour as the old _build_top10).
    """
    candidates: list[dict] = []
    seen_ck: set[str] = set()
    for c in minhash_raw[:limit * 3]:  # over-scan to absorb dedup losses
        it = item_by_id.get(c["item_id"])
        if it is None:
            continue
        ck = _canonical_item_key(it)
        if ck and ck in seen_ck:
            continue
        seen_ck.add(ck)
        candidates.append({
            "item_id": c["item_id"],
            "name": c["name"],
            "score": round(c["jaccard"] * 100),
            "reasons": [f"MinHash J={c['jaccard']:.3f}"],
            "warn_reasons": [],
            "breakdown": {},
        })
        if len(candidates) >= limit:
            break
    return candidates


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
    """Score a catalog item against an extracted row dict. Higher = better match.

    Delegates to score_match() in app.matching.scorer; returns 0..100 int.
    """
    return _score_item(row_dict, item)["score"]


def find_match(row_dict: dict, session=None) -> dict:
    """Find the best matching internal catalog item for a row (legacy API).

    Returns:
        {
          "source": "memory" | "scored" | "none",
          "fingerprint": str,
          "best": InternalItem | None,
          "score": int,
          "candidates": [{"item_id": int, "name": str, "score": int}, ...]
        }
    """
    close_session = False
    if session is None:
        session = get_db_session()
        close_session = True
    try:
        fp = build_fingerprint(row_dict)

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

        all_items = session.query(InternalItem).filter_by(is_active=True).all()
        scored = []
        for item in all_items:
            r = _score_item(row_dict, item)  # find_match uses default settings
            s = r["score"]
            if s > 0:
                scored.append((s, item, r["reasons"], r["warn_reasons"], r.get("breakdown", {})))

        scored.sort(key=lambda x: (-x[0], x[1].id))
        top5 = _build_top10(scored)

        best = None
        best_score = 0
        if scored and scored[0][0] >= _MATCH_THRESHOLD:
            best_score, best = scored[0][0], scored[0][1]

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


def decide_match(row_dict: dict, settings, session=None) -> dict:
    """Apply threshold-based decision and return a MatchDecision dict.

    Returns dict with keys:
        mode, internal_item_id, name, score, reason, fingerprint, candidates
    """
    close_session = False
    if session is None:
        session = get_db_session()
        close_session = True
    try:
        fp = build_fingerprint(row_dict)
        r_std_keys = _row_std_keys(row_dict)

        # Step 1: Memory hit
        if settings.enable_auto_match_memory:
            mem = session.query(SupplierInternalMatch).filter_by(fingerprint=fp).first()
            if mem:
                item = session.get(InternalItem, mem.internal_item_id)
                if item and item.is_active:
                    mode = MATCH_MODE_AUTO_MEMORY
                    if settings.always_require_confirmation:
                        mode = MATCH_MODE_SUGGESTED
                    return {
                        "mode": mode,
                        "internal_item_id": item.id,
                        "name": item.name,
                        "score": 100,
                        "reason": "Совпадение по памяти (fingerprint)",
                        "fingerprint": fp,
                        "candidates": [{"item_id": item.id, "name": item.name, "score": 100}],
                        "source": "memory",
                        "standard_keys_row": sorted(r_std_keys),
                    }

        # Step 2: MinHash candidates
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
        if not all_items:
            return {
                "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                "score": 0, "reason": "Каталог пуст",
                "fingerprint": fp, "candidates": [], "source": "none",
                "standard_keys_row": sorted(r_std_keys),
            }

        item_by_id = {item.id: item for item in all_items}
        minhash_raw = _query_minhash(row_dict, item_by_id, settings)
        std_keys_list = sorted(r_std_keys)

        candidates = _build_minhash_candidates(minhash_raw, item_by_id)

        if not minhash_raw:
            return {
                "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                "score": 0, "reason": "MinHash не нашёл кандидатов",
                "fingerprint": fp, "candidates": [], "source": "none",
                "standard_keys_row": std_keys_list,
            }

        best_j    = minhash_raw[0]["jaccard"]
        best_item = item_by_id.get(minhash_raw[0]["item_id"])
        best_score = round(best_j * 100)

        if settings.auto_apply_enabled and best_j >= settings.auto_apply_jaccard_threshold and best_item:
            mode = MATCH_MODE_AUTO_MINHASH
            if settings.always_require_confirmation:
                mode = MATCH_MODE_SUGGESTED
            return {
                "mode": mode, "internal_item_id": best_item.id,
                "name": best_item.name, "score": best_score,
                "reason": f"Автоподстановка по MinHash (J={best_j:.3f} ≥ {settings.auto_apply_jaccard_threshold})",
                "fingerprint": fp, "candidates": candidates, "source": "minhash",
                "standard_keys_row": std_keys_list,
            }

        if best_item:
            return {
                "mode": MATCH_MODE_SUGGESTED, "internal_item_id": best_item.id,
                "name": best_item.name, "score": best_score,
                "reason": f"MinHash нашёл кандидата, J={best_j:.3f} < {settings.auto_apply_jaccard_threshold} (нужно подтверждение)",
                "fingerprint": fp, "candidates": candidates, "source": "minhash",
                "standard_keys_row": std_keys_list,
            }

        return {
            "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
            "score": 0, "reason": "Нет совпадений",
            "fingerprint": fp, "candidates": candidates, "source": "none",
            "standard_keys_row": std_keys_list,
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
    # Include name_raw for volume/keyword scoring
    for extra in ("name_raw", "name"):
        if extra in row.index:
            v = row[extra]
            result[extra] = "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
    return result


def _canonical_item_key(item: InternalItem) -> str:
    """Canonical deduplication key for a catalog item.

    Reads the pre-computed item.canonical_key from the DB when available
    (populated by the migration and kept in sync by create/update routes).
    Falls back to on-the-fly computation for items that haven't been
    backfilled yet (e.g. during tests that create items without routes).
    """
    if item.canonical_key:
        return item.canonical_key
    from app.matching.canonicalize import compute_canonical_key  # noqa: PLC0415
    return compute_canonical_key(item)


def _build_top10(scored_with_reasons: list) -> list[dict]:
    """Build top-10 candidate list, collapsing near-duplicate items.

    Input list must already be sorted by (-score, item.id).
    Items with the same _canonical_item_key are deduplicated — only the
    first (highest-scoring / lowest-id) representative is kept.
    Items with score == 0 AND no signals are silently dropped.
    """
    seen_keys: set[str] = set()
    result: list[dict] = []
    for s, it, reas, warn, bdwn in scored_with_reasons:
        if s <= 0 and not reas and not warn:
            continue
        ck = _canonical_item_key(it)
        if ck and ck in seen_keys:
            continue
        seen_keys.add(ck)
        result.append({
            "item_id": it.id,
            "name": it.name,
            "score": s,
            "reasons": reas,
            "warn_reasons": warn,
            "breakdown": bdwn,
        })
        if len(result) >= 10:
            break
    return result


def _build_match_debug(
    row_dict: dict,
    all_items: list,
    scored: list,
    top5: list,
    best_score: int,
    minhash_raw: list | None = None,
    applied_mode: str = "NONE",
    threshold_used: float = 0.0,
) -> dict:
    """Build a diagnostics dict for a single row's match attempt."""
    from app.matching.normalizer import extract_row_features  # noqa: PLC0415
    nonzero = sum(1 for s, *_ in scored if s > 0)
    any_signal = sum(1 for s, _it, r, w, *_ in scored if s > 0 or r or w)
    features = extract_row_features(row_dict)

    zero_reason: str | None = None
    if best_score == 0:
        if not all_items:
            zero_reason = "Каталог пуст"
        elif nonzero == 0 and any_signal == 0:
            zero_reason = f"Ни один из {len(all_items)} товаров не дал сигналов совпадения"
        elif nonzero == 0:
            zero_reason = f"Совпадений с ненулевым score нет (есть предупреждения у {any_signal} товаров)"

    minhash_raw = minhash_raw or []
    best_jaccard = minhash_raw[0]["jaccard"] if minhash_raw else 0.0
    top_minhash = [
        {"item_id": c["item_id"], "name": c["name"], "jaccard": c["jaccard"]}
        for c in minhash_raw[:5]
    ]

    return {
        "total_scanned": len(all_items),
        "nonzero_scored": nonzero,
        "any_signal": any_signal,
        "top5_count": len(top5),
        "best_score": best_score,
        "extracted": features,
        "zero_reason": zero_reason,
        # MinHash auto-apply diagnostics
        "best_jaccard": round(best_jaccard, 3),
        "applied_mode": applied_mode,
        "threshold_used": threshold_used,
        "top_minhash_candidates": top_minhash,
    }


def add_internal_matches(df_trans: pd.DataFrame, settings=None) -> tuple:
    """Add 'internal_match' column to df_trans and return (df_trans, match_results).

    match_results is a list (aligned with df_trans rows) of dicts with keys:
        mode, internal_item_id, name, score, reason, fingerprint, candidates, source
    """
    from app.match_settings import load_match_settings
    if settings is None:
        settings = load_match_settings()

    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
        all_mem = {
            m.fingerprint: m.internal_item_id
            for m in session.query(SupplierInternalMatch).all()
        }
        item_by_id = {item.id: item for item in all_items}

        match_names: list[str]  = []
        match_results: list[dict] = []

        for _, row in df_trans.iterrows():
            row_dict = _row_to_dict(row)
            fp = build_fingerprint(row_dict)
            r_std_keys = _row_std_keys(row_dict)
            std_keys_list = sorted(r_std_keys)

            # Memory hit
            if settings.enable_auto_match_memory and fp in all_mem:
                iid  = all_mem[fp]
                item = item_by_id.get(iid)
                if item:
                    mode = MATCH_MODE_AUTO_MEMORY
                    if settings.always_require_confirmation:
                        mode = MATCH_MODE_SUGGESTED
                    match_names.append(item.name)
                    match_results.append({
                        "mode": mode, "internal_item_id": item.id,
                        "name": item.name, "score": 100,
                        "reason": "Совпадение по памяти (fingerprint)",
                        "fingerprint": fp,
                        "candidates": [{"item_id": item.id, "name": item.name, "score": 100}],
                        "source": "memory",
                        "standard_keys_row": std_keys_list,
                        "match_debug": _build_match_debug(row_dict, all_items, [], [{"item_id": item.id, "name": item.name, "score": 100}], 100, minhash_raw=[], applied_mode="AUTO", threshold_used=0.0),
                    })
                    continue

            if not all_items:
                match_names.append("")
                match_results.append({
                    "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                    "score": 0, "reason": "Каталог пуст",
                    "fingerprint": fp, "candidates": [], "source": "none",
                    "standard_keys_row": std_keys_list,
                    "match_debug": _build_match_debug(row_dict, [], [], [], 0, minhash_raw=[], applied_mode="NONE", threshold_used=settings.auto_apply_jaccard_threshold),
                })
                continue

            minhash_raw = _query_minhash(row_dict, item_by_id, settings)

            best_j = minhash_raw[0]["jaccard"] if minhash_raw else 0.0
            best_minhash_item = item_by_id.get(minhash_raw[0]["item_id"]) if minhash_raw else None

            candidates = _build_minhash_candidates(minhash_raw, item_by_id)

            if settings.auto_apply_enabled and best_j >= settings.auto_apply_jaccard_threshold and best_minhash_item:
                applied_mode_str = "AUTO"
            elif minhash_raw:
                applied_mode_str = "SUGGEST"
            else:
                applied_mode_str = "NONE"

            debug = _build_match_debug(
                row_dict, all_items, [], candidates, round(best_j * 100),
                minhash_raw=minhash_raw,
                applied_mode=applied_mode_str,
                threshold_used=settings.auto_apply_jaccard_threshold,
            )

            # ── Decision: MinHash J-based auto-apply ──────────────────────────
            if settings.auto_apply_enabled and best_j >= settings.auto_apply_jaccard_threshold and best_minhash_item:
                mode = MATCH_MODE_AUTO_MINHASH
                if settings.always_require_confirmation:
                    mode = MATCH_MODE_SUGGESTED
                reason = f"Автоподстановка по MinHash (J={best_j:.3f} ≥ {settings.auto_apply_jaccard_threshold})"
                match_names.append(best_minhash_item.name)
                match_results.append({
                    "mode": mode, "internal_item_id": best_minhash_item.id,
                    "name": best_minhash_item.name,
                    "score": round(best_j * 100),
                    "reason": reason,
                    "fingerprint": fp, "candidates": candidates, "source": "minhash",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                })

            elif minhash_raw and best_minhash_item:
                reason = f"MinHash нашёл кандидата, J={best_j:.3f} < {settings.auto_apply_jaccard_threshold} (нужно подтверждение)"
                match_names.append(best_minhash_item.name)
                match_results.append({
                    "mode": MATCH_MODE_SUGGESTED, "internal_item_id": best_minhash_item.id,
                    "name": best_minhash_item.name,
                    "score": round(best_j * 100),
                    "reason": reason,
                    "fingerprint": fp, "candidates": candidates, "source": "minhash",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                })

            else:
                match_names.append("")
                match_results.append({
                    "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                    "score": 0, "reason": "MinHash не нашёл кандидатов",
                    "fingerprint": fp, "candidates": candidates, "source": "none",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                })

        df_out = df_trans.copy()
        df_out["internal_match"] = match_names
        return df_out, match_results
    finally:
        session.close()
