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

MATCH_MODE_AUTO_MEMORY       = "AUTO_MEMORY"
MATCH_MODE_AUTO_SCORE        = "AUTO_SCORE"
MATCH_MODE_AUTO_MINHASH      = "AUTO_MINHASH"
MATCH_MODE_AUTO_EXACT        = "AUTO_EXACT"
MATCH_MODE_AUTO_ANALOG       = "AUTO_ANALOG"
MATCH_MODE_SUGGESTED         = "SUGGESTED"
MATCH_MODE_SUGGESTED_ANALOG  = "SUGGESTED_ANALOG"
MATCH_MODE_NONE              = "NONE"
MATCH_MODE_MANUAL            = "MANUAL_SELECTED"
MATCH_MODE_CONFIRMED         = "CONFIRMED"

MATCH_MODE_LABELS = {
    MATCH_MODE_AUTO_MEMORY:      "Авто (память)",
    MATCH_MODE_AUTO_SCORE:       "Авто (скоринг)",
    MATCH_MODE_AUTO_MINHASH:     "Авто (MinHash J)",
    MATCH_MODE_AUTO_EXACT:       "Авто (точное)",
    MATCH_MODE_AUTO_ANALOG:      "Авто (аналог)",
    MATCH_MODE_SUGGESTED:        "Предложено",
    MATCH_MODE_SUGGESTED_ANALOG: "Предложено (аналог)",
    MATCH_MODE_NONE:             "Нет",
    MATCH_MODE_MANUAL:           "Вручную",
    MATCH_MODE_CONFIRMED:        "Подтверждено",
}


def _norm(val) -> str:
    return str(val or "").strip().lower()


def _analog_display(canonical: str | None) -> str:
    """Convert a canonical standard key to a Russian display string for UI messages."""
    if not canonical:
        return canonical or ""
    from app.matching.standard_analogs import canonical_to_display  # noqa: PLC0415
    return canonical_to_display(canonical)


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

    Returns list of {"item_id": int, "name": str, "jaccard": float,
                      "via_analog": str | None}.
    When use_standard_analogs_in_main_match is enabled, additional queries are
    performed for each known analog standard so that catalog items indexed with
    an equivalent-but-different standard notation still appear as candidates.
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

    raw = []
    analogs_only = getattr(settings, "analogs_only", False)

    # ── Direct (non-analog) query ─────────────────────────────────────────────
    if not analogs_only:
        mh_results = query_index_with_scores(
            r_text, item_type=r_type, size=r_size, standard_text=r_std,
            top_k=settings.minhash_top_k,
            use_type_buckets=settings.use_type_buckets,
            min_candidates_before_fallback=settings.min_candidates_before_fallback,
        )
        for r in mh_results:
            iid = r["item_id"]
            it  = item_by_id.get(iid)
            if it:
                raw.append({"item_id": iid, "name": it.name, "jaccard": r["jaccard"], "via_analog": None})

    # ── Analog standard augmentation ──────────────────────────────────────────
    if (settings.use_standard_analogs_in_main_match or analogs_only) and r_text:
        from app.matching.standard_analogs import build_analog_queries  # noqa: PLC0415

        analog_queries = build_analog_queries(r_text)
        for aq in analog_queries:
            aq_results = query_index_with_scores(
                aq.rewritten_text, item_type=r_type, size=r_size,
                standard_text="",  # std already embedded in rewritten text
                top_k=settings.minhash_top_k,
                use_type_buckets=settings.use_type_buckets,
                min_candidates_before_fallback=settings.min_candidates_before_fallback,
            )
            for r in aq_results:
                iid = r["item_id"]
                it  = item_by_id.get(iid)
                if it:
                    raw.append({
                        "item_id": iid, "name": it.name,
                        "jaccard": r["jaccard"],
                        "via_analog": aq.analog_canonical,
                    })

    return _dedup_minhash_raw(raw)


def _dedup_minhash_raw(minhash_raw: list) -> list:
    """Deduplicate MinHash candidates by item_id, keeping highest Jaccard score.

    Prefers a direct match (via_analog=None) over an analog match at equal Jaccard.
    """
    seen: dict[int, dict] = {}
    for r in minhash_raw:
        iid = r["item_id"]
        if iid not in seen:
            seen[iid] = r
        else:
            prev = seen[iid]
            # Prefer higher Jaccard; at equal Jaccard prefer direct over analog
            if r["jaccard"] > prev["jaccard"] or (
                r["jaccard"] == prev["jaccard"]
                and r.get("via_analog") is None
                and prev.get("via_analog") is not None
            ):
                seen[iid] = r
    return sorted(seen.values(), key=lambda x: -x["jaccard"])


def _build_minhash_candidates(
    minhash_raw: list, item_by_id: dict, limit: int = 10,
    row_std_keys: set[str] | None = None,
) -> list:
    """Build deduplicated candidate dicts from MinHash results.

    Deduplicates by canonical_key so near-duplicate catalog entries collapse
    into a single representative (same behaviour as the old _build_top10).

    When *row_std_keys* is provided, each candidate gets a ``match_standard_mode``
    field: ``"exact"`` if the item's standard_key matches any row standard,
    ``"analog"`` if found via analog search, or ``"none"`` otherwise.
    """
    from app.matching.standard_analogs import canonical_to_display as _ctd  # noqa: PLC0415

    if row_std_keys is None:
        row_std_keys = set()

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
        via = c.get("via_analog")
        reason_str = f"MinHash J={c['jaccard']:.3f}"
        via_display = ""
        if via:
            via_display = _ctd(via)
            reason_str += f" (аналог {via_display})"

        # Determine match_standard_mode
        item_std_key = getattr(it, "standard_key", None) or ""
        if item_std_key and item_std_key in row_std_keys:
            std_mode = "exact"
        elif via:
            std_mode = "analog"
        else:
            std_mode = "none"

        candidates.append({
            "item_id": c["item_id"],
            "name": c["name"],
            "score": round(c["jaccard"] * 100),
            "reasons": [reason_str],
            "warn_reasons": [],
            "breakdown": {},
            "via_analog": via,
            "via_analog_display": via_display,
            "match_standard_mode": std_mode,
            "folder_path": it.folder_path or "",
            "folder_name": it.folder_name or "",
        })
        if len(candidates) >= limit:
            break
    return candidates


def _build_exact_candidates(
    exact_items: list,
    row_dict: dict,
    settings,
    r_std_keys: set[str],
    item_by_id: dict,
    use_analogs: bool = False,
    analogs_only: bool = False,
) -> list[dict]:
    """Build scored candidates from items matching type + size_norm exactly.

    Score starts at 100 (type+size matched) with deductions for field mismatches.
    Used by Stage 1 of the matching pipeline.

    When analogs_only=True, only items whose standard matches via analog table
    (not direct) are included.
    When use_analogs=False and analogs_only=False, items with mismatched standard
    are excluded (only direct standard match or no-standard items pass).
    """
    from app.matching.normalizer import normalize_size  # noqa: PLC0415
    from app.matching.post_filter import compute_candidate_badges  # noqa: PLC0415
    from app.matching.standard_analogs import normalize_standard as _ns_std  # noqa: PLC0415

    row_size_norm = normalize_size(str(row_dict.get("size") or ""))
    # Compute row_std_canon for badge computation
    row_std_canon = None
    for k in ("gost", "iso", "din"):
        v = str(row_dict.get(k) or "").strip()
        if v:
            cn = _ns_std(v)
            if cn:
                row_std_canon = cn
                break

    r_strength = _norm(row_dict.get("strength"))
    r_coating = _norm(row_dict.get("coating"))

    all_scored: list[dict] = []

    for item in exact_items:
        score = 100  # type + size already verified
        reasons: list[str] = []

        # Standard check
        item_std_key = (item.standard_key or "").strip()
        _std_direct = False
        _std_analog = False
        if r_std_keys and item_std_key:
            if item_std_key in r_std_keys:
                _std_direct = True
                reasons.append("стандарт ✓")
            else:
                if use_analogs or analogs_only or settings.use_standard_analogs_in_main_match:
                    from app.matching.standard_analogs import get_standard_analogs  # noqa: PLC0415
                    _std_analog = any(
                        item_std_key in get_standard_analogs(k) for k in r_std_keys
                    )
                if _std_analog:
                    score -= 5
                    reasons.append("стандарт (аналог)")
                else:
                    score -= 20
                    reasons.append("стандарт ✗")
        elif r_std_keys and not item_std_key:
            score -= 3

        # Filter by analog mode when row has a standard
        if r_std_keys:
            if analogs_only:
                # "Только аналоги" — require analog standard match; skip all else
                if not _std_analog:
                    continue
            elif item_std_key and not use_analogs and not _std_direct:
                continue  # "Без аналогов" — skip analog and mismatched

        # Strength check
        i_strength = _norm(item.strength_class)
        if r_strength and i_strength:
            if r_strength != i_strength:
                score -= 10
                reasons.append("прочность ✗")
            else:
                reasons.append("прочность ✓")

        # Coating check
        i_coating = _norm(item.material_coating)
        if r_coating and i_coating:
            if r_coating != i_coating:
                score -= 5
                reasons.append("покрытие ✗")
            else:
                reasons.append("покрытие ✓")

        score = max(0, score)

        std_mode = "exact" if (item_std_key and item_std_key in r_std_keys) else "none"
        cand: dict = {
            "item_id": item.id,
            "name": item.name,
            "score": score,
            "reasons": [f"Точное: type+size ({', '.join(reasons)})" if reasons else "Точное: type+size"],
            "warn_reasons": [],
            "breakdown": {},
            "via_analog": None,
            "via_analog_display": "",
            "match_standard_mode": std_mode,
            "folder_path": item.folder_path or "",
            "folder_name": item.folder_name or "",
            "match_stage": "exact",
        }
        cand["field_badges"] = compute_candidate_badges(
            cand, row_dict, item_by_id, row_size_norm, row_std_canon,
            use_analogs or analogs_only,
        )
        all_scored.append(cand)

    # Group by canonical_key, pick the best representative per group.
    # Within the same score, prefer: higher folder_priority → primary folder
    # (not starting with "_") → shorter name (fewer extra attributes) → smaller id.
    ck_groups: dict[str, list[dict]] = {}
    no_ck: list[dict] = []
    for cand in all_scored:
        item = item_by_id.get(cand["item_id"])
        ck = _canonical_item_key(item) if item else ""
        if ck:
            ck_groups.setdefault(ck, []).append(cand)
        else:
            no_ck.append(cand)

    def _pick_best(group: list[dict]) -> dict:
        """Pick the best representative from a canonical group.

        When the row specifies strength/coating, prefer candidates that
        carry matching values over bare items (same score otherwise).
        """
        def _sort_key(c: dict) -> tuple:
            it = item_by_id.get(c["item_id"])
            prio = (it.folder_priority or 0) if it else 0
            path = (it.folder_path or "") if it else ""
            is_primary = 0 if path.startswith("_") else 1
            # Field affinity: count matching strength/coating fields.
            # Higher affinity → better representative for the row.
            affinity = 0
            if it:
                if r_strength and _norm(it.strength_class) == r_strength:
                    affinity += 1
                if r_coating and _norm(it.material_coating) == r_coating:
                    affinity += 1
            name_len = len(c.get("name") or "")
            return (-c["score"], -prio, -is_primary, -affinity, name_len, c["item_id"])
        return min(group, key=_sort_key)

    deduped = [_pick_best(g) for g in ck_groups.values()] + no_ck
    deduped.sort(key=lambda c: (-c["score"], c["item_id"]))
    return deduped[:10]


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

        from app.catalog_cache import get_snapshot  # noqa: PLC0415
        all_items, _ = get_snapshot()
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


def decide_match(row_dict: dict, settings, session=None, all_items=None, item_by_id=None) -> dict:
    """Apply threshold-based decision and return a MatchDecision dict.

    Args:
        all_items: Optional pre-loaded list of active InternalItem objects.
                   When provided, skips the per-call DB query (batch optimization).
        item_by_id: Optional pre-built {id: InternalItem} dict. When provided
                    together with all_items, avoids rebuilding on every call.

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
        if all_items is None:
            from app.catalog_cache import get_snapshot  # noqa: PLC0415
            all_items, _cached_by_id = get_snapshot()
            if item_by_id is None:
                item_by_id = _cached_by_id
        if not all_items:
            return {
                "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                "score": 0, "reason": "Каталог пуст",
                "fingerprint": fp, "candidates": [], "source": "none",
                "standard_keys_row": sorted(r_std_keys),
            }

        if item_by_id is None:
            item_by_id = {item.id: item for item in all_items}

        # Step 2: Exact field search (type + size_norm)
        from app.matching.normalizer import normalize_size as _nsz  # noqa: PLC0415
        _r_type = _norm(row_dict.get("item_type"))
        _r_size = _nsz(str(row_dict.get("size") or ""))
        if _r_type and _r_size:
            _exact_items = [
                it for it in all_items
                if _norm(it.item_type) == _r_type
                and (it.size_norm or _nsz(str(it.size or ""))) == _r_size
            ]
            if _exact_items:
                _analogs_only = getattr(settings, "analogs_only", False)
                _exact_cands = _build_exact_candidates(
                    _exact_items, row_dict, settings, r_std_keys, item_by_id,
                    use_analogs=settings.use_standard_analogs_in_main_match,
                    analogs_only=_analogs_only,
                )
                if _exact_cands:
                    _best_ex = _exact_cands[0]
                    _best_item = item_by_id.get(_best_ex["item_id"])
                    if _best_item:
                        _auto = (settings.auto_apply_enabled
                                 and _best_ex["score"] >= settings.auto_match_threshold
                                 and not settings.always_require_confirmation)
                        return {
                            "mode": MATCH_MODE_AUTO_EXACT if _auto else MATCH_MODE_SUGGESTED,
                            "internal_item_id": _best_item.id,
                            "name": _best_item.name,
                            "score": _best_ex["score"],
                            "reason": f"Точное совпадение type+size (score={_best_ex['score']})",
                            "fingerprint": fp,
                            "candidates": _exact_cands,
                            "source": "exact",
                            "standard_keys_row": sorted(r_std_keys),
                        }

        # Step 3: MinHash candidates + post-filter (same logic as add_internal_matches)
        minhash_raw = _query_minhash(row_dict, item_by_id, settings)
        std_keys_list = sorted(r_std_keys)

        min_score = getattr(settings, "min_display_score", 40)

        best_j = minhash_raw[0]["jaccard"] if minhash_raw else 0.0
        best_score_pct = round(best_j * 100)
        below_threshold = bool(minhash_raw) and best_score_pct < min_score

        # ── Post-filter candidates ─────────────────────────────────────────
        from app.matching.post_filter import post_filter_candidates as _pf  # noqa: PLC0415
        all_candidates_raw = _build_minhash_candidates(
            minhash_raw, item_by_id, row_std_keys=r_std_keys,
        )
        _use_analogs_pf = settings.use_standard_analogs_in_main_match or getattr(settings, "analogs_only", False)
        filtered_candidates, filter_log = _pf(
            all_candidates_raw, minhash_raw, row_dict, item_by_id,
            settings, use_analogs=_use_analogs_pf,
            analogs_only=getattr(settings, "analogs_only", False),
        )
        candidates = [c for c in filtered_candidates if c["score"] >= min_score]

        size_no_match = filter_log.get("size_no_match", False)
        candidates_other_size = (
            [c for c in all_candidates_raw if c["score"] >= min_score]
            if size_no_match else []
        )

        # Block auto-apply when the top-Jaccard candidate failed hard filters
        best_filtered_out = filter_log.get("best_filtered_out", False)

        best_item = item_by_id.get(minhash_raw[0]["item_id"]) if minhash_raw and not below_threshold else None
        best_via_analog = minhash_raw[0].get("via_analog") if minhash_raw and not below_threshold else None

        # Pick the best from FILTERED candidates (not raw minhash order)
        if candidates and not best_filtered_out:
            best_cand = candidates[0]
            best_item = item_by_id.get(best_cand["item_id"])
            best_via_analog = best_cand.get("via_analog")
            best_j = best_cand["score"] / 100.0  # use filtered best score

        if not minhash_raw or below_threshold:
            return {
                "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                "score": 0,
                "reason": "MinHash не нашёл кандидатов" if not minhash_raw else f"Лучший кандидат ниже порога отображения ({best_score_pct}% < {min_score}%)",
                "fingerprint": fp, "candidates": candidates, "source": "none",
                "standard_keys_row": std_keys_list,
                "candidates_other_size": candidates_other_size,
                "filter_log": filter_log,
            }

        if settings.auto_apply_enabled and best_j >= settings.auto_apply_jaccard_threshold and best_item and not best_filtered_out:
            if settings.always_require_confirmation:
                mode = MATCH_MODE_SUGGESTED_ANALOG if best_via_analog else MATCH_MODE_SUGGESTED
            else:
                mode = MATCH_MODE_AUTO_ANALOG if best_via_analog else MATCH_MODE_AUTO_MINHASH
            analog_note = f" (аналог {best_via_analog})" if best_via_analog else ""
            return {
                "mode": mode, "internal_item_id": best_item.id,
                "name": best_item.name, "score": round(best_j * 100),
                "reason": f"Автоподстановка по MinHash (J={best_j:.3f} ≥ {settings.auto_apply_jaccard_threshold}){analog_note}",
                "via_analog": best_via_analog,
                "fingerprint": fp, "candidates": candidates, "source": "minhash",
                "standard_keys_row": std_keys_list,
                "candidates_other_size": candidates_other_size,
                "filter_log": filter_log,
            }

        if best_item:
            mode = MATCH_MODE_SUGGESTED_ANALOG if best_via_analog else MATCH_MODE_SUGGESTED
            analog_note = f" (аналог {best_via_analog})" if best_via_analog else ""
            return {
                "mode": mode, "internal_item_id": best_item.id,
                "name": best_item.name, "score": round(best_j * 100),
                "reason": f"MinHash нашёл кандидата, J={best_j:.3f}{analog_note} (нужно подтверждение)",
                "via_analog": best_via_analog,
                "fingerprint": fp, "candidates": candidates, "source": "minhash",
                "standard_keys_row": std_keys_list,
                "candidates_other_size": candidates_other_size,
                "filter_log": filter_log,
            }

        return {
            "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
            "score": 0, "reason": "Нет совпадений (кандидаты отфильтрованы)" if best_filtered_out else "Нет совпадений",
            "fingerprint": fp, "candidates": candidates, "source": "none",
            "standard_keys_row": std_keys_list,
            "candidates_other_size": candidates_other_size,
            "filter_log": filter_log,
        }
    finally:
        if close_session:
            session.close()


def _row_to_dict(row: pd.Series) -> dict:
    """Extract matching-relevant fields from a DataFrame row.

    Handles both English keys (``size``, ``item_type``) and Russian display
    column names (``Размер MxL``, ``Тип изделия``) transparently.  When a
    field is missing from the DataFrame entirely, it is extracted on-the-fly
    from the row name text.
    """
    from app.extractors import EXTRACTORS  # noqa: PLC0415

    # Build key→display mapping once per call
    _key_to_display: dict[str, str] = {k: disp for k, (disp, _) in EXTRACTORS.items()}

    def _get(key: str) -> str:
        """Get a value by English key, falling back to Russian column name."""
        for col in (key, _key_to_display.get(key, "")):
            if col and col in row.index:
                v = row[col]
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    return str(v)
        return ""

    result = {}
    for k in _FINGERPRINT_KEYS:
        result[k] = _get(k)

    # Include name_raw for volume/keyword scoring
    for extra in ("name_raw", "name"):
        val = _get(extra)
        if val:
            result[extra] = val

    # Compute fields on-the-fly ONLY when the column is completely absent
    # from the DataFrame (neither English key nor Russian display name exist).
    # If the column IS present but empty, respect the empty value.
    name_text = result.get("name_raw") or result.get("name") or ""
    if name_text:
        for mk in _FINGERPRINT_KEYS:
            if result[mk]:
                continue
            # Check if the column exists in the DataFrame at all
            col_present = mk in row.index or _key_to_display.get(mk, "") in row.index
            if col_present:
                continue  # column exists but is empty — respect that
            entry = EXTRACTORS.get(mk)
            if entry:
                val = entry[1](name_text)
                if val:
                    result[mk] = val

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
    filter_log: dict | None = None,
    analog_info: dict | None = None,
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
        # Post-filter diagnostics
        "filter_log": filter_log or {},
        # Analog search diagnostics
        "analog_info": analog_info or {},
    }


_STD_MODE_PRIORITY = {"exact": 0, "analog": 1, "none": 2}


def _sort_candidates_by_std_mode(candidates: list) -> list:
    """Sort candidates: exact standard first, then analog, then none.

    Within the same standard mode, preserves the original order (by Jaccard desc).
    """
    return sorted(candidates, key=lambda c: _STD_MODE_PRIORITY.get(c.get("match_standard_mode", "none"), 2))


def _build_analog_info(row_dict: dict, use_analogs: bool) -> dict:
    """Build analog info dict for UI display.

    Returns:
        {
            "original_standard": "ГОСТ 7798-70" | None,
            "original_canonical": "GOST-7798-70" | None,
            "analogs_found": ["DIN 931", "ISO 4014"],
            "queries_built": 2,
            "enabled": True/False,
        }
    """
    from app.matching.standard_analogs import (  # noqa: PLC0415
        build_analog_queries,
        canonical_to_display,
        get_standard_analogs,
        normalize_standard,
    )

    r_text = str(row_dict.get("name_raw") or row_dict.get("name") or "").strip()
    info: dict = {
        "original_standard": None,
        "original_canonical": None,
        "analogs_found": [],
        "queries_built": 0,
        "enabled": use_analogs,
    }

    # Try to find the standard in the row
    for k in ("gost", "iso", "din"):
        val = _norm(row_dict.get(k))
        if val:
            canonical = normalize_standard(val)
            if canonical:
                info["original_standard"] = val
                info["original_canonical"] = canonical
                analogs = get_standard_analogs(canonical)
                info["analogs_found"] = [canonical_to_display(a) for a in analogs]
                break

    # If no standard found in fields, try raw text
    if not info["original_canonical"] and r_text:
        queries = build_analog_queries(r_text)
        if queries:
            aq = queries[0]
            info["original_canonical"] = aq.original_canonical
            info["original_standard"] = canonical_to_display(aq.original_canonical)
            # Collect all unique analog displays
            seen = set()
            for q in queries:
                if q.analog_display not in seen:
                    info["analogs_found"].append(q.analog_display)
                    seen.add(q.analog_display)
            info["queries_built"] = len(queries)

    if info["original_canonical"] and not info["analogs_found"]:
        analogs = get_standard_analogs(info["original_canonical"])
        info["analogs_found"] = [canonical_to_display(a) for a in analogs]

    info["queries_built"] = info["queries_built"] or len(info["analogs_found"])
    return info


def rematch_row(row_dict: dict, use_analogs: bool = False) -> dict:
    """Re-run matching for a single row with explicit analog toggle.

    Returns a dict compatible with decide_match() output, enriched with
    ``candidates``, ``filter_log``, and ``candidates_other_size``.

    Used by the UI "toggle analog search" feature in select_internal.html
    and choose_catalog.html.
    """
    import dataclasses  # noqa: PLC0415
    from app.match_settings import load_match_settings  # noqa: PLC0415

    settings = load_match_settings()
    settings = dataclasses.replace(settings, use_standard_analogs_in_main_match=use_analogs)

    from app.catalog_cache import get_snapshot  # noqa: PLC0415

    session = get_db_session()
    try:
        all_items, item_by_id = get_snapshot()
        r_std_keys = _row_std_keys(row_dict)

        # Stage 1: Exact field search (type + size_norm)
        from app.matching.normalizer import normalize_size as _nsz  # noqa: PLC0415
        _r_type = _norm(row_dict.get("item_type"))
        _r_size = _nsz(str(row_dict.get("size") or ""))
        if _r_type and _r_size:
            _exact_items = [
                it for it in all_items
                if _norm(it.item_type) == _r_type
                and (it.size_norm or _nsz(str(it.size or ""))) == _r_size
            ]
            if _exact_items:
                _exact_cands = _build_exact_candidates(
                    _exact_items, row_dict, settings, r_std_keys, item_by_id,
                    use_analogs=use_analogs,
                    analogs_only=getattr(settings, "analogs_only", False),
                )
                if _exact_cands:
                    return {
                        "candidates": _exact_cands,
                        "filter_log": {"match_stage": "exact", "fallback_level": -1, "steps": []},
                        "candidates_other_size": [],
                        "minhash_raw": [],
                        "analog_info": _build_analog_info(row_dict, use_analogs),
                    }

        # Stage 2-3: MinHash candidates
        minhash_raw = _query_minhash(row_dict, item_by_id, settings)
        min_score = getattr(settings, "min_display_score", 40)

        from app.matching.post_filter import post_filter_candidates  # noqa: PLC0415

        all_candidates_raw = _build_minhash_candidates(
            minhash_raw, item_by_id, row_std_keys=r_std_keys,
        )
        filtered_candidates, filter_log = post_filter_candidates(
            all_candidates_raw, minhash_raw, row_dict, item_by_id,
            settings, use_analogs=use_analogs,
        )
        candidates = _sort_candidates_by_std_mode(
            [c for c in filtered_candidates if c["score"] >= min_score]
        )

        size_no_match = filter_log.get("size_no_match", False)
        candidates_other_size = (
            [c for c in all_candidates_raw if c["score"] >= min_score]
            if size_no_match else []
        )

        # Build analog info for UI
        analog_info = _build_analog_info(row_dict, use_analogs)

        return {
            "candidates": candidates,
            "filter_log": filter_log,
            "candidates_other_size": candidates_other_size,
            "minhash_raw": minhash_raw,
            "analog_info": analog_info,
        }
    finally:
        session.close()


def add_internal_matches(df_trans: pd.DataFrame, settings=None, use_analogs: bool | None = None) -> tuple:
    """Add 'internal_match' column to df_trans and return (df_trans, match_results).

    match_results is a list (aligned with df_trans rows) of dicts with keys:
        mode, internal_item_id, name, score, reason, fingerprint, candidates, source

    use_analogs overrides settings.use_standard_analogs_in_main_match when not None.
    When True, analog standard augmentation is enabled for this run regardless of the
    global setting. When False, it is disabled. When None, the global setting is used.
    """
    import dataclasses
    from app.match_settings import load_match_settings
    if settings is None:
        settings = load_match_settings()

    if use_analogs is not None:
        settings = dataclasses.replace(settings, use_standard_analogs_in_main_match=use_analogs)

    from app.catalog_cache import get_snapshot  # noqa: PLC0415

    session = get_db_session()
    try:
        all_items, item_by_id = get_snapshot()
        all_mem = {
            m.fingerprint: m.internal_item_id
            for m in session.query(SupplierInternalMatch).all()
        }

        # Build type+size lookup for exact field search (Stage 1)
        from collections import defaultdict as _defaultdict  # noqa: PLC0415
        from app.matching.normalizer import normalize_size as _nsz_idx  # noqa: PLC0415
        type_size_idx: dict = _defaultdict(list)
        for _it in all_items:
            _ts_type = _norm(_it.item_type)
            _ts_size = (_it.size_norm or _nsz_idx(str(_it.size or ""))).strip()
            if _ts_type and _ts_size:
                type_size_idx[(_ts_type, _ts_size)].append(_it)

        # Load master-item memberships upfront for O(1) lookup per row
        master_by_guid: dict[str, dict] = {}
        try:
            from app.models import MasterItem, MasterItemMember  # noqa: PLC0415
            for mem_row, mi in (
                session.query(MasterItemMember, MasterItem)
                .join(MasterItem, MasterItem.id == MasterItemMember.master_item_id)
                .filter(MasterItem.is_active.is_(True))
                .all()
            ):
                master_by_guid[mem_row.onec_guid] = {
                    "master_item_id": mi.id,
                    "master_item_name": mi.name,
                }
        except Exception:
            pass  # non-fatal: master items not yet migrated

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
                    _mem_master = master_by_guid.get(item.uid_1c or "", {})
                    match_names.append(item.name)
                    match_results.append({
                        "mode": mode, "internal_item_id": item.id,
                        "name": item.name, "score": 100,
                        "reason": "Совпадение по памяти (fingerprint)",
                        "fingerprint": fp,
                        "candidates": [{"item_id": item.id, "name": item.name, "score": 100}],
                        "source": "memory",
                        "standard_keys_row": std_keys_list,
                        "master_item_id": _mem_master.get("master_item_id"),
                        "master_item_name": _mem_master.get("master_item_name"),
                        "match_debug": _build_match_debug(row_dict, all_items, [], [{"item_id": item.id, "name": item.name, "score": 100}], 100, minhash_raw=[], applied_mode="AUTO", threshold_used=0.0),
                    })
                    continue

            # ── Stage 1: Exact field search (type + size_norm) ────────────
            from app.matching.normalizer import normalize_size as _nsz  # noqa: PLC0415
            _r_type = _norm(row_dict.get("item_type"))
            _r_size = _nsz(str(row_dict.get("size") or ""))
            if _r_type and _r_size and (_r_type, _r_size) in type_size_idx:
                _exact_items = type_size_idx[(_r_type, _r_size)]
                _exact_cands = _build_exact_candidates(
                    _exact_items, row_dict, settings, r_std_keys, item_by_id,
                    use_analogs=settings.use_standard_analogs_in_main_match,
                    analogs_only=getattr(settings, "analogs_only", False),
                )
                if _exact_cands:
                    _best_ex = _exact_cands[0]
                    _best_ex_item = item_by_id.get(_best_ex["item_id"])
                    if _best_ex_item:
                        _best_ex_master = master_by_guid.get(_best_ex_item.uid_1c or "", {})
                        _auto = (settings.auto_apply_enabled
                                 and _best_ex["score"] >= settings.auto_match_threshold
                                 and not settings.always_require_confirmation)
                        _mode = MATCH_MODE_AUTO_EXACT if _auto else MATCH_MODE_SUGGESTED
                        _debug = _build_match_debug(
                            row_dict, all_items, [], _exact_cands,
                            _best_ex["score"], minhash_raw=[],
                            applied_mode="EXACT" if _auto else "SUGGEST",
                            threshold_used=settings.auto_match_threshold,
                        )
                        _debug["match_stage"] = "exact"
                        _debug["exact_candidates_count"] = len(_exact_items)
                        match_names.append(_best_ex_item.name)
                        match_results.append({
                            "mode": _mode,
                            "internal_item_id": _best_ex_item.id,
                            "name": _best_ex_item.name,
                            "score": _best_ex["score"],
                            "reason": f"Точное совпадение type+size (score={_best_ex['score']})",
                            "fingerprint": fp,
                            "candidates": _exact_cands,
                            "source": "exact",
                            "standard_keys_row": std_keys_list,
                            "master_item_id": _best_ex_master.get("master_item_id"),
                            "master_item_name": _best_ex_master.get("master_item_name"),
                            "match_debug": _debug,
                            "candidates_other_size": [],
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
            min_score = getattr(settings, "min_display_score", 40)

            best_j = minhash_raw[0]["jaccard"] if minhash_raw else 0.0
            best_score_pct = round(best_j * 100)
            # Treat as no match if best candidate is below the display threshold
            below_threshold = bool(minhash_raw) and best_score_pct < min_score

            # ── Post-filter candidates ─────────────────────────────────────────
            from app.matching.post_filter import post_filter_candidates  # noqa: PLC0415
            all_candidates_raw = _build_minhash_candidates(
                minhash_raw, item_by_id, row_std_keys=r_std_keys,
            )
            _use_analogs_pf2 = settings.use_standard_analogs_in_main_match or getattr(settings, "analogs_only", False)
            filtered_candidates, filter_log = post_filter_candidates(
                all_candidates_raw, minhash_raw, row_dict, item_by_id,
                settings, use_analogs=_use_analogs_pf2,
                analogs_only=getattr(settings, "analogs_only", False),
            )
            candidates = [c for c in filtered_candidates if c["score"] >= min_score]

            # When the strict size filter removed ALL candidates, preserve the
            # pre-filter list separately so the UI can show "похожие (другой размер)".
            size_no_match = filter_log.get("size_no_match", False)
            candidates_other_size = (
                [c for c in all_candidates_raw if c["score"] >= min_score]
                if size_no_match else []
            )

            # Best item is still driven by raw MinHash Jaccard ranking
            best_minhash_item = item_by_id.get(minhash_raw[0]["item_id"]) if minhash_raw and not below_threshold else None
            best_via_analog = minhash_raw[0].get("via_analog") if minhash_raw and not below_threshold else None
            best_master = master_by_guid.get((best_minhash_item.uid_1c or "") if best_minhash_item else "", {})

            # Block auto-apply when the top-Jaccard candidate failed hard filters
            best_filtered_out = filter_log.get("best_filtered_out", False)

            if settings.auto_apply_enabled and best_j >= settings.auto_apply_jaccard_threshold and best_minhash_item and not best_filtered_out:
                applied_mode_str = "AUTO"
            elif minhash_raw and not below_threshold:
                applied_mode_str = "SUGGEST"
            else:
                applied_mode_str = "NONE"

            debug = _build_match_debug(
                row_dict, all_items, [], candidates, round(best_j * 100),
                minhash_raw=minhash_raw,
                applied_mode=applied_mode_str,
                threshold_used=settings.auto_apply_jaccard_threshold,
                filter_log=filter_log,
            )

            # ── Decision: MinHash J-based auto-apply ──────────────────────────
            # Auto-apply is blocked when the best candidate failed the hard filter
            if settings.auto_apply_enabled and best_j >= settings.auto_apply_jaccard_threshold and best_minhash_item and not best_filtered_out:
                if settings.always_require_confirmation:
                    mode = MATCH_MODE_SUGGESTED_ANALOG if best_via_analog else MATCH_MODE_SUGGESTED
                else:
                    mode = MATCH_MODE_AUTO_ANALOG if best_via_analog else MATCH_MODE_AUTO_MINHASH
                analog_note = (f" (аналог {_analog_display(best_via_analog)})" if best_via_analog else "")
                reason = f"Автоподстановка по MinHash (J={best_j:.3f} ≥ {settings.auto_apply_jaccard_threshold}){analog_note}"
                match_names.append(best_minhash_item.name)
                match_results.append({
                    "mode": mode, "internal_item_id": best_minhash_item.id,
                    "name": best_minhash_item.name,
                    "score": round(best_j * 100),
                    "reason": reason,
                    "via_analog": best_via_analog,
                    "fingerprint": fp, "candidates": candidates, "source": "minhash",
                    "standard_keys_row": std_keys_list,
                    "master_item_id": best_master.get("master_item_id"),
                    "master_item_name": best_master.get("master_item_name"),
                    "match_debug": debug,
                    "candidates_other_size": candidates_other_size,
                })

            elif minhash_raw and best_minhash_item:
                mode = MATCH_MODE_SUGGESTED_ANALOG if best_via_analog else MATCH_MODE_SUGGESTED
                analog_note = (f" (аналог {_analog_display(best_via_analog)})" if best_via_analog else "")
                if best_filtered_out and best_j >= settings.auto_apply_jaccard_threshold:
                    reason = (
                        f"MinHash J={best_j:.3f} достаточен, но лучший кандидат не прошёл "
                        f"фильтр полей (уровень фолбэка: {filter_log.get('fallback_level', '?')})"
                    )
                else:
                    reason = f"MinHash нашёл кандидата, J={best_j:.3f} < {settings.auto_apply_jaccard_threshold}{analog_note} (нужно подтверждение)"
                match_names.append(best_minhash_item.name)
                match_results.append({
                    "mode": mode, "internal_item_id": best_minhash_item.id,
                    "name": best_minhash_item.name,
                    "score": round(best_j * 100),
                    "reason": reason,
                    "via_analog": best_via_analog,
                    "fingerprint": fp, "candidates": candidates, "source": "minhash",
                    "standard_keys_row": std_keys_list,
                    "master_item_id": best_master.get("master_item_id"),
                    "master_item_name": best_master.get("master_item_name"),
                    "match_debug": debug,
                    "candidates_other_size": candidates_other_size,
                })

            else:
                if below_threshold:
                    none_reason = f"Лучший кандидат ниже порога отображения ({best_score_pct}% < {min_score}%)"
                else:
                    none_reason = "MinHash не нашёл кандидатов"
                match_names.append("")
                match_results.append({
                    "mode": MATCH_MODE_NONE, "internal_item_id": None, "name": "",
                    "score": 0, "reason": none_reason,
                    "fingerprint": fp, "candidates": candidates, "source": "none",
                    "standard_keys_row": std_keys_list,
                    "match_debug": debug,
                    "candidates_other_size": candidates_other_size,
                })

        df_out = df_trans.copy()
        df_out["internal_match"] = match_names
        return df_out, match_results
    finally:
        session.close()
