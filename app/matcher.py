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

MATCH_MODE_AUTO_MEMORY = "AUTO_MEMORY"
MATCH_MODE_AUTO_SCORE  = "AUTO_SCORE"
MATCH_MODE_SUGGESTED   = "SUGGESTED"
MATCH_MODE_NONE        = "NONE"
MATCH_MODE_MANUAL      = "MANUAL_SELECTED"
MATCH_MODE_CONFIRMED   = "CONFIRMED"

MATCH_MODE_LABELS = {
    MATCH_MODE_AUTO_MEMORY: "Авто (память)",
    MATCH_MODE_AUTO_SCORE:  "Авто",
    MATCH_MODE_SUGGESTED:   "Предложено",
    MATCH_MODE_NONE:        "Нет",
    MATCH_MODE_MANUAL:      "Вручную",
    MATCH_MODE_CONFIRMED:   "Подтверждено",
}


def _norm(val) -> str:
    return str(val or "").strip().lower()


def _score_item(row_dict: dict, item: InternalItem) -> dict:
    """Return score_match result for row_dict vs item (lazy import avoids circular)."""
    from app.matching.scorer import score_match
    return score_match(row_dict, item)


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


def _filter_candidates_stage_a(row_dict: dict, all_items: list) -> list:
    """Stage A pre-filter: narrow the candidate pool before full scoring.

    For large catalogs (> 200 items) prioritises items that share the same
    standard number, then same standard kind (GOST/DIN/ISO), then same type.
    Always falls back to the full list when fewer than 20 candidates remain.
    """
    _MAX = 200
    if len(all_items) <= _MAX:
        return all_items  # small catalog — score everything

    r_std_keys = _row_std_keys(row_dict)
    r_type     = _norm(row_dict.get("item_type"))

    if not r_std_keys and not r_type:
        return all_items[:_MAX]

    seen: set[int] = set()
    result: list   = []

    def _add(items):
        for it in items:
            if it.id not in seen:
                seen.add(it.id)
                result.append(it)

    # Primary: exact standard key match
    if r_std_keys:
        _add(it for it in all_items if it.standard_key and it.standard_key in r_std_keys)

    # Secondary: same standard kind (GOST/DIN/ISO)
    if r_std_keys:
        r_kinds = {k.split("-")[0] for k in r_std_keys if "-" in k}
        _add(
            it for it in all_items
            if it.standard_key and "-" in it.standard_key
            and it.standard_key.split("-")[0] in r_kinds
        )

    # Tertiary: same item type
    if r_type:
        _add(
            it for it in all_items
            if _norm(it.item_type) == r_type
        )

    # Fallback: add remaining if too few
    if len(result) < 20:
        _add(all_items)

    return result[:_MAX]


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
            r = _score_item(row_dict, item)
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

        # Step 2: Score all active items
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
        if not all_items:
            return {
                "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                "score": 0, "reason": "Каталог пуст",
                "fingerprint": fp, "candidates": [], "source": "none",
                "standard_keys_row": sorted(r_std_keys),
            }

        scored = []
        for item in all_items:
            r = _score_item(row_dict, item)
            scored.append((r["score"], item, r["reasons"], r["warn_reasons"], r.get("breakdown", {})))
        scored.sort(key=lambda x: (-x[0], x[1].id))
        top5 = _build_top10(scored)

        best_score = scored[0][0] if scored else 0
        best_item  = scored[0][1] if scored else None

        std_keys_list = sorted(r_std_keys)

        if best_score <= 0 or not best_item:
            return {
                "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                "score": 0, "reason": "Нет совпадений",
                "fingerprint": fp, "candidates": [], "source": "none",
                "standard_keys_row": std_keys_list,
            }

        if settings.enable_auto_match and best_score >= settings.auto_match_threshold:
            mode = MATCH_MODE_AUTO_SCORE
            if settings.always_require_confirmation:
                mode = MATCH_MODE_SUGGESTED
            return {
                "mode": mode, "internal_item_id": best_item.id,
                "name": best_item.name, "score": best_score,
                "reason": "Высокая уверенность по скорингу",
                "fingerprint": fp, "candidates": top5, "source": "scored",
                "standard_keys_row": std_keys_list,
            }

        if best_score >= settings.suggest_threshold:
            return {
                "mode": MATCH_MODE_SUGGESTED, "internal_item_id": best_item.id,
                "name": best_item.name, "score": best_score,
                "reason": "Нужно подтверждение (средняя уверенность)",
                "fingerprint": fp, "candidates": top5, "source": "scored",
                "standard_keys_row": std_keys_list,
            }

        return {
            "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
            "score": best_score, "reason": "Не найдено подходящее соответствие",
            "fingerprint": fp, "candidates": top5, "source": "none",
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

    Items that differ only in Cyrillic/Latin spelling or spacing
    (e.g. "М 12x60" vs "M12x60") produce the same key and are collapsed
    in the candidate list so the operator does not see confusing duplicates.

    Key format: "<type>|<sorted-size-tokens>|<standard_key>"
    Empty key (no type, no size, no standard) → no deduplication applied.
    """
    from app.matching.normalizer import normalize_size, parse_size_tokens  # noqa: PLC0415
    size_toks = parse_size_tokens(normalize_size(item.size or ""))
    size_key  = "x".join(f"{t:g}" for t in sorted(size_toks)) if size_toks else ""
    type_key  = str(item.item_type or "").strip().lower()
    std_key   = str(item.standard_key or "")
    return f"{type_key}|{size_key}|{std_key}"


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

    return {
        "total_scanned": len(all_items),
        "nonzero_scored": nonzero,
        "any_signal": any_signal,
        "top5_count": len(top5),
        "best_score": best_score,
        "extracted": features,
        "zero_reason": zero_reason,
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
                        "match_debug": _build_match_debug(row_dict, all_items, [], [{"item_id": item.id, "name": item.name, "score": 100}], 100),
                    })
                    continue

            if not all_items:
                match_names.append("")
                match_results.append({
                    "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                    "score": 0, "reason": "Каталог пуст",
                    "fingerprint": fp, "candidates": [], "source": "none",
                    "standard_keys_row": std_keys_list,
                    "match_debug": _build_match_debug(row_dict, [], [], [], 0),
                })
                continue

            candidates_to_score = _filter_candidates_stage_a(row_dict, all_items)
            scored = []
            for item in candidates_to_score:
                r = _score_item(row_dict, item)
                scored.append((r["score"], item, r["reasons"], r["warn_reasons"], r.get("breakdown", {})))
            scored.sort(key=lambda x: (-x[0], x[1].id))
            top5 = _build_top10(scored)

            best_score = scored[0][0] if scored else 0
            best_item  = scored[0][1] if scored else None
            debug = _build_match_debug(row_dict, all_items, scored, top5, best_score)

            if not best_item or best_score <= 0:
                match_names.append("")
                match_results.append({
                    "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                    "score": 0, "reason": "Нет совпадений",
                    "fingerprint": fp, "candidates": top5, "source": "none",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                })
                continue

            if settings.enable_auto_match and best_score >= settings.auto_match_threshold:
                mode = MATCH_MODE_AUTO_SCORE
                if settings.always_require_confirmation:
                    mode = MATCH_MODE_SUGGESTED
                match_names.append(best_item.name)
                match_results.append({
                    "mode": mode, "internal_item_id": best_item.id,
                    "name": best_item.name, "score": best_score,
                    "reason": "Высокая уверенность по скорингу",
                    "fingerprint": fp, "candidates": top5, "source": "scored",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                })

            elif best_score >= settings.suggest_threshold:
                match_names.append(best_item.name)
                match_results.append({
                    "mode": MATCH_MODE_SUGGESTED, "internal_item_id": best_item.id,
                    "name": best_item.name, "score": best_score,
                    "reason": "Нужно подтверждение (средняя уверенность)",
                    "fingerprint": fp, "candidates": top5, "source": "scored",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                })

            else:
                match_names.append("")
                match_results.append({
                    "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                    "score": best_score, "reason": "Не найдено подходящее соответствие",
                    "fingerprint": fp, "candidates": top5, "source": "none",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                })

        df_out = df_trans.copy()
        df_out["internal_match"] = match_names
        return df_out, match_results
    finally:
        session.close()
