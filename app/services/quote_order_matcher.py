"""QuoteLine -> OrderItem matching engine + comparison table builder.

Features:
- MinHash-based fuzzy matching (sole matching engine)
- Strict post-filters: size (hard), standard (with analog fallback), type
- Delta check: auto-match only when J1-J2 >= delta (or no second candidate)
- Uniqueness: each QuoteLine linked to at most one OrderItem per quote
- Classification-aware: only "item" QuoteLines participate
- Debug info for every match decision
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from datasketch import MinHash

from app.match_settings import MatchSettings, load_match_settings
from app.services.line_parser import build_features, build_minhash

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

TOP_K = 10
NUM_PERM = 128


# ── MinHash index ────────────────────────────────────────────────────────────

# In-memory cache: order_id -> (minhashes_dict, items_by_id, built_at)
_index_cache: dict[int, tuple[dict, dict, float]] = {}


def _build_order_item_minhashes(order_items) -> dict[int, MinHash]:
    """Build MinHash signatures for a list of OrderItems."""
    result: dict[int, MinHash] = {}
    for oi in order_items:
        feats = build_features(
            oi.tokens_norm or "", oi.type_norm or "",
            oi.size_norm or "", oi.std_norm or "",
        )
        if feats:
            result[oi.id] = build_minhash(feats)
    return result


def build_order_minhash_index(order_id: int, session: Session) -> tuple[dict[int, MinHash], dict]:
    """Build (or rebuild) MinHash index for an order's approved items.

    Returns (minhashes_dict, order_items_by_id).
    """
    from app.order_models import OrderItem
    order_items = session.query(OrderItem).filter_by(order_id=order_id).all()
    minhashes = _build_order_item_minhashes(order_items)
    items_by_id = {oi.id: oi for oi in order_items}
    _index_cache[order_id] = (minhashes, items_by_id, time.time())
    return minhashes, items_by_id


def get_order_minhash_index(order_id: int, session: Session) -> tuple[dict[int, MinHash], dict]:
    """Get cached index or build lazily."""
    if order_id in _index_cache:
        minhashes, items_by_id, _ = _index_cache[order_id]
        if minhashes:
            return minhashes, items_by_id
    return build_order_minhash_index(order_id, session)


def invalidate_index(order_id: int) -> None:
    """Remove cached index (call after re-approving items)."""
    _index_cache.pop(order_id, None)


# ── Post-filters ─────────────────────────────────────────────────────────────


def _standards_compatible(ql_std: str, oi_std: str, use_analogs: bool = True) -> bool:
    """Check if two standards match directly or via analog table."""
    if not ql_std or not oi_std:
        return True  # unknown standard -> don't filter
    if ql_std == oi_std:
        return True
    if not use_analogs:
        return False
    try:
        from app.matching.standard_analogs import get_standard_analogs
        analogs = get_standard_analogs(oi_std)
        return ql_std in analogs
    except Exception:
        return False


def _sizes_compatible(ql_size: str, oi_size: str) -> bool:
    """Strict size filter.

    When QuoteLine has a recognized size:
    - OI must also have a size, AND they must match.
    When QuoteLine has NO size: don't filter.
    """
    if not ql_size:
        return True  # QL has no size -> don't filter
    if not oi_size:
        return False  # QL has size but OI doesn't -> exclude
    return ql_size.upper() == oi_size.upper()


def _types_compatible(ql_type: str, oi_type: str) -> bool:
    """Strict type comparison. Both must match when both are known."""
    if not ql_type or not oi_type:
        return True
    return ql_type.strip().lower() == oi_type.strip().lower()


# ── Single-line matching ─────────────────────────────────────────────────────


def _score_exact_quote_match(ql_std: str, oi_std: str, use_analogs: bool) -> int:
    """Score an exact type+size match between QuoteLine and OrderItem.

    Base score 100, deductions for field mismatches (mirrors catalog matcher).
    """
    score = 100
    if ql_std and oi_std:
        if ql_std == oi_std:
            pass  # direct match, no deduction
        elif _standards_compatible(ql_std, oi_std, use_analogs=use_analogs):
            score -= 5  # analog match
        else:
            score -= 20  # standard mismatch
    elif ql_std and not oi_std:
        score -= 3  # OI has no standard
    return max(0, score)


def match_quote_line_to_items(
    ql,
    order_minhashes: dict[int, MinHash],
    order_items_by_id: dict,
    settings: MatchSettings | None = None,
    used_oi_ids: set[int] | None = None,
    top_k: int = TOP_K,
) -> dict:
    """Match a single QuoteLine against all OrderItems.

    Stage 1: Exact field match (type + size_norm) — high confidence, no MinHash needed.
    Stage 2: MinHash Jaccard + post-filters — fuzzy fallback.

    Returns: {
        best_order_item_id: int|None,
        match_mode: "auto"|"suggested"|None,
        jaccard: float,
        candidates: list[dict],
        debug: dict,
    }
    """
    if settings is None:
        settings = load_match_settings()

    threshold_auto = settings.auto_apply_jaccard_threshold
    threshold_suggest = settings.suggest_jaccard_threshold
    delta_min = settings.auto_match_delta_jaccard
    # Always enable analog matching for quote-to-order comparison:
    # suppliers often quote in GOST while order items use DIN/ISO (or vice versa).
    use_analogs = True

    # Skip non-item lines
    if getattr(ql, "line_class", None) and ql.line_class != "item":
        return {
            "best_order_item_id": None, "match_mode": None,
            "jaccard": 0.0, "candidates": [],
            "debug": {"skip_reason": "non_item_line", "line_class": ql.line_class},
        }

    ql_type = (ql.type_norm or "").strip().lower()
    ql_size = (ql.size_norm or "").strip().upper()
    ql_std = (ql.std_norm or "").strip()

    # ── Stage 1: Exact field match (type + size) ─────────────────────────────
    if ql_type and ql_size:
        exact_candidates: list[dict] = []
        for oi_id, oi in order_items_by_id.items():
            oi_type = (oi.type_norm or "").strip().lower()
            oi_size = (oi.size_norm or "").strip().upper()
            oi_std = (oi.std_norm or "").strip()

            if oi_type != ql_type or oi_size != ql_size:
                continue

            score = _score_exact_quote_match(ql_std, oi_std, use_analogs)
            exact_candidates.append({
                "order_item_id": oi_id,
                "jaccard": score / 100.0,  # normalize to 0..1 for compatibility
                "display_name": oi.display_name_snapshot,
                "size_norm": oi.size_norm or "",
                "type_norm": oi.type_norm or "",
                "std_norm": oi.std_norm or "",
                "passed_filters": True,
                "type_match": True,
                "size_match": True,
                "std_match": _standards_compatible(ql_std, oi_std, use_analogs),
                "match_stage": "exact",
                "score": score,
            })

        if exact_candidates:
            exact_candidates.sort(key=lambda c: -c["score"])
            best = exact_candidates[0]
            best_id = best["order_item_id"]
            already_taken = used_oi_ids is not None and best_id in used_oi_ids

            debug = {
                "ql_type": ql_type, "ql_size": ql_size, "ql_std": ql_std,
                "match_stage": "exact",
                "exact_candidates": len(exact_candidates),
                "best_score": best["score"],
            }

            if already_taken:
                mode = "suggested"
                debug["reason"] = "exact_match_order_item_already_taken"
            elif best["score"] >= settings.auto_match_threshold:
                mode = "auto"
                debug["reason"] = "exact_auto_match"
            else:
                mode = "suggested"
                debug["reason"] = f"exact_score={best['score']} < threshold={settings.auto_match_threshold}"

            return {
                "best_order_item_id": best_id if mode == "auto" else None,
                "match_mode": mode,
                "jaccard": best["jaccard"],
                "candidates": exact_candidates[:top_k],
                "debug": debug,
            }

    # ── Stage 2: MinHash fuzzy matching ──────────────────────────────────────
    feats = build_features(
        ql.tokens_norm or "", ql.type_norm or "",
        ql.size_norm or "", ql.std_norm or "",
    )
    if not feats:
        return {
            "best_order_item_id": None, "match_mode": None,
            "jaccard": 0.0, "candidates": [],
            "debug": {"skip_reason": "empty_features", "tokens_norm": ql.tokens_norm or ""},
        }

    ql_mh = build_minhash(feats)

    # Compute Jaccard for ALL OrderItems
    all_scored: list[dict] = []

    for oi_id, oi_mh in order_minhashes.items():
        oi = order_items_by_id[oi_id]
        oi_type = (oi.type_norm or "").strip().lower()
        oi_size = (oi.size_norm or "").strip().upper()
        oi_std = (oi.std_norm or "").strip()

        j = ql_mh.jaccard(oi_mh)

        type_ok = _types_compatible(ql_type, oi_type)
        size_ok = _sizes_compatible(ql_size, oi_size)
        std_ok = _standards_compatible(ql_std, oi_std, use_analogs=use_analogs)

        passed = type_ok and size_ok and std_ok

        all_scored.append({
            "order_item_id": oi_id,
            "jaccard": round(j, 4),
            "display_name": oi.display_name_snapshot,
            "size_norm": oi.size_norm or "",
            "type_norm": oi.type_norm or "",
            "std_norm": oi.std_norm or "",
            "passed_filters": passed,
            "type_match": type_ok,
            "size_match": size_ok,
            "std_match": std_ok,
        })

    all_scored.sort(key=lambda x: (-x["passed_filters"], -x["jaccard"]))

    # Candidates that passed all filters
    filtered = [c for c in all_scored if c["passed_filters"]]
    before_filter_count = len(all_scored)
    after_filter_count = len(filtered)

    # Standard relaxation: if QL has std and all candidates were filtered out,
    # allow candidates without std if J is high enough
    filters_failed = False
    if not filtered and ql_std:
        relaxed = [c for c in all_scored
                   if _types_compatible(ql_type, c["type_norm"].strip().lower() if c["type_norm"] else "")
                   and _sizes_compatible(ql_size, c["size_norm"].strip().upper() if c["size_norm"] else "")
                   and not (c["std_norm"] or "").strip()]  # OI has no std
        # Only include if J is well above threshold
        high_j = [c for c in relaxed if c["jaccard"] >= threshold_auto + 0.10]
        if high_j:
            filtered = high_j
            for c in filtered:
                c["passed_filters"] = True
                c["std_match"] = None  # relaxed

    if not filtered:
        filters_failed = True
        # Fallback: top by J without filters (for UI), but NO auto-match
        filtered = all_scored[:top_k]

    candidates = filtered[:top_k]

    debug: dict = {
        "ql_type": ql_type, "ql_size": ql_size, "ql_std": ql_std,
        "match_stage": "minhash",
        "candidates_before_filters": before_filter_count,
        "candidates_after_filters": after_filter_count,
        "filters_failed": filters_failed,
        "threshold_auto": threshold_auto,
        "threshold_suggest": threshold_suggest,
        "delta_min": delta_min,
    }

    if not candidates:
        debug["reason"] = "no_candidates"
        return {
            "best_order_item_id": None, "match_mode": None,
            "jaccard": 0.0, "candidates": [], "debug": debug,
        }

    best = candidates[0]
    j_best = best["jaccard"]
    j_second = candidates[1]["jaccard"] if len(candidates) > 1 else 0.0
    delta = j_best - j_second

    debug["j_best"] = j_best
    debug["j_second"] = j_second
    debug["delta"] = round(delta, 4)

    # Decision logic
    mode = None
    best_id = None

    if filters_failed:
        debug["reason"] = "filters_failed_no_auto"
    elif j_best < threshold_suggest:
        debug["reason"] = f"j_best={j_best:.3f} < suggest_threshold={threshold_suggest}"
    elif j_best < threshold_auto:
        mode = "suggested"
        debug["reason"] = f"j_best={j_best:.3f} < auto_threshold={threshold_auto}"
    elif delta < delta_min and len(candidates) > 1:
        mode = "suggested"
        debug["reason"] = f"delta={delta:.3f} < delta_min={delta_min} (ambiguous)"
    elif used_oi_ids is not None and best["order_item_id"] in used_oi_ids:
        mode = "suggested"
        debug["reason"] = "order_item_already_taken"
    else:
        mode = "auto"
        best_id = best["order_item_id"]
        debug["reason"] = "auto_match"

    return {
        "best_order_item_id": best_id,
        "match_mode": mode,
        "jaccard": j_best,
        "candidates": candidates,
        "debug": debug,
    }


# ── Batch matching ───────────────────────────────────────────────────────────


def match_quote_to_order_items(quote_id: int, session: Session) -> dict:
    """Match all QuoteLines in a quote against the order's OrderItems.

    Returns: {matched_auto, suggested, unmatched, filtered_out, total_lines}
    """
    from app.order_models import OrderItem, Quote, QuoteLine, QuoteMatch

    quote = session.get(Quote, quote_id)
    if not quote:
        return {"error": "quote_not_found"}

    order_items = session.query(OrderItem).filter_by(order_id=quote.order_id).all()
    quote_lines = session.query(QuoteLine).filter_by(quote_id=quote_id).all()

    if not order_items or not quote_lines:
        return {
            "matched_auto": 0, "suggested": 0,
            "unmatched": len(quote_lines), "filtered_out": 0,
            "total_lines": len(quote_lines),
        }

    settings = load_match_settings()

    order_minhashes, order_items_by_id = get_order_minhash_index(quote.order_id, session)

    if not order_minhashes:
        # Rebuild if cache was empty
        order_minhashes = _build_order_item_minhashes(order_items)
        order_items_by_id = {oi.id: oi for oi in order_items}

    logger.info(
        "match_quote(quote_id=%d): %d order_items, %d minhashes, %d quote_lines",
        quote_id, len(order_items), len(order_minhashes), len(quote_lines),
    )

    # Remove old matches for this quote's lines
    ql_ids = [ql.id for ql in quote_lines]
    session.query(QuoteMatch).filter(QuoteMatch.quote_line_id.in_(ql_ids)).delete(
        synchronize_session=False
    )

    # Track which OrderItem IDs are already matched by this quote
    matched_oi_ids: set[int] = set()

    stats = {
        "matched_auto": 0, "suggested": 0,
        "unmatched": 0, "filtered_out": 0,
        "total_lines": len(quote_lines),
    }

    item_lines = [ql for ql in quote_lines if (ql.line_class or "item") == "item"]
    non_item_lines = [ql for ql in quote_lines if ql.line_class and ql.line_class != "item"]
    stats["filtered_out"] = len(non_item_lines)

    for ql in item_lines:
        result = match_quote_line_to_items(
            ql, order_minhashes, order_items_by_id,
            settings=settings,
            used_oi_ids=matched_oi_ids,
        )

        debug = result.get("debug", {})
        if result["match_mode"] == "auto" and result["best_order_item_id"]:
            oi_id = result["best_order_item_id"]
            session.add(QuoteMatch(
                order_item_id=oi_id,
                quote_line_id=ql.id,
                jaccard=result["jaccard"],
                match_mode="auto",
            ))
            matched_oi_ids.add(oi_id)
            stats["matched_auto"] += 1
            logger.info(
                "AUTO match QL#%d '%s' -> OI#%d J=%.3f",
                ql.id, ql.raw_text[:60], oi_id, result["jaccard"],
            )
        elif result["match_mode"] == "suggested":
            stats["suggested"] += 1
            logger.info(
                "SUGGESTED QL#%d '%s' J=%.3f reason=%s",
                ql.id, ql.raw_text[:60], result["jaccard"], debug.get("reason", "?"),
            )
        else:
            stats["unmatched"] += 1
            logger.info(
                "UNMATCHED QL#%d '%s' reason=%s",
                ql.id, ql.raw_text[:60], debug.get("reason", "?"),
            )

    session.flush()
    session.commit()
    logger.info(
        "match_quote_to_order_items(quote_id=%d): auto=%d suggested=%d unmatched=%d filtered=%d",
        quote_id, stats["matched_auto"], stats["suggested"],
        stats["unmatched"], stats["filtered_out"],
    )
    return stats


def match_all_quotes_for_order(order_id: int, session: Session) -> dict:
    """Match ALL quotes for an order. Returns aggregate summary."""
    from app.order_models import Quote

    t0 = time.time()
    quotes = session.query(Quote).filter_by(order_id=order_id).all()

    totals = {
        "total_lines": 0, "matched_auto": 0,
        "suggested": 0, "unmatched": 0, "filtered_out": 0,
        "quotes_processed": 0, "time_ms": 0,
    }

    # Pre-build index once for all quotes
    build_order_minhash_index(order_id, session)

    for q in quotes:
        stats = match_quote_to_order_items(q.id, session)
        if "error" in stats:
            continue
        totals["quotes_processed"] += 1
        for k in ("total_lines", "matched_auto", "suggested", "unmatched", "filtered_out"):
            totals[k] += stats.get(k, 0)

    totals["time_ms"] = int((time.time() - t0) * 1000)
    return totals


# ── Comparison table ─────────────────────────────────────────────────────────


def build_comparison_table(order_id: int, session: Session) -> dict:
    """Build comparison table for /orders/{id}/comparison.

    Returns:
        {
            "suppliers": [name, ...],
            "rows": [{order_item, cells: {supplier_name: {price, currency, unit, jaccard, mode, ql_id, quote_id}}}],
            "unmatched": {supplier_name: [QuoteLine, ...]},
            "filtered": {supplier_name: [QuoteLine, ...]},
        }
    """
    from app.order_models import OrderItem, Quote, QuoteLine, QuoteMatch, Supplier

    order_items = session.query(OrderItem).filter_by(order_id=order_id).order_by(OrderItem.id).all()
    quotes = session.query(Quote).filter_by(order_id=order_id).all()

    supplier_ids = list({q.supplier_id for q in quotes})
    suppliers = session.query(Supplier).filter(Supplier.id.in_(supplier_ids)).all() if supplier_ids else []
    supplier_by_id = {s.id: s for s in suppliers}
    supplier_names = [supplier_by_id[q.supplier_id].name for q in quotes if q.supplier_id in supplier_by_id]
    seen = set()
    unique_suppliers = []
    for n in supplier_names:
        if n not in seen:
            seen.add(n)
            unique_suppliers.append(n)

    quote_by_id = {q.id: q for q in quotes}

    quote_ids = [q.id for q in quotes]
    if not quote_ids:
        return {
            "suppliers": unique_suppliers,
            "rows": [{"order_item": oi, "cells": {}} for oi in order_items],
            "unmatched": {},
            "filtered": {},
        }

    quote_lines = session.query(QuoteLine).filter(QuoteLine.quote_id.in_(quote_ids)).all()
    ql_by_id = {ql.id: ql for ql in quote_lines}

    ql_ids = [ql.id for ql in quote_lines]
    matches = session.query(QuoteMatch).filter(
        QuoteMatch.quote_line_id.in_(ql_ids)
    ).all() if ql_ids else []

    oi_cells: dict[int, dict[str, dict]] = {oi.id: {} for oi in order_items}
    matched_ql_ids: set[int] = set()

    for m in matches:
        ql = ql_by_id.get(m.quote_line_id)
        if not ql:
            continue
        quote = quote_by_id.get(ql.quote_id)
        if not quote:
            continue
        supplier = supplier_by_id.get(quote.supplier_id)
        if not supplier:
            continue
        matched_ql_ids.add(ql.id)
        if m.order_item_id in oi_cells:
            oi_cells[m.order_item_id][supplier.name] = {
                "price": ql.price,
                "price_total": getattr(ql, "price_total", None),
                "currency": ql.currency or "RUB",
                "qty": ql.qty,
                "unit": ql.unit or "",
                "raw_text": ql.raw_text,
                "jaccard": m.jaccard,
                "mode": m.match_mode,
                "ql_id": ql.id,
                "quote_id": ql.quote_id,
            }

    rows = [{"order_item": oi, "cells": oi_cells.get(oi.id, {})} for oi in order_items]

    # Split unmatched into real items vs filtered (non-items)
    unmatched: dict[str, list] = {}
    filtered: dict[str, list] = {}

    for ql in quote_lines:
        if ql.id in matched_ql_ids:
            continue
        quote = quote_by_id.get(ql.quote_id)
        if not quote:
            continue
        supplier = supplier_by_id.get(quote.supplier_id)
        if not supplier:
            continue

        if ql.line_class and ql.line_class != "item":
            filtered.setdefault(supplier.name, []).append(ql)
        else:
            unmatched.setdefault(supplier.name, []).append(ql)

    return {
        "suppliers": unique_suppliers,
        "rows": rows,
        "unmatched": unmatched,
        "filtered": filtered,
    }
