"""In-memory MinHash LSH index for fuzzy candidate retrieval.

Module-level singleton: the index is built once at startup and updated
incrementally on catalog CRUD operations.

Supports two retrieval modes:
- Global index (_lsh_all): searches across all catalog items
- Type-bucketed indices (_lsh_by_type): separate LSH per item_type for
  higher precision; falls back to global when too few candidates found
"""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import TYPE_CHECKING

from datasketch import MinHash, MinHashLSH

from app.matching.text_normalizer import char_ngrams, normalize_for_minhash

if TYPE_CHECKING:
    from app.models import InternalItem

logger = logging.getLogger(__name__)

# ── Module-level singleton state ─────────────────────────────────────────────

_lsh_all: MinHashLSH | None = None
_lsh_by_type: dict[str, MinHashLSH] = {}
_minhashes: dict[int, MinHash] = {}
_item_types: dict[int, str] = {}       # item_id → normalized type
_num_perm: int = 128
_ngram_n: int = 4
_threshold: float = 0.3


def _make_minhash(tokens: set[str], num_perm: int) -> MinHash:
    """Build a MinHash signature from a set of string tokens."""
    mh = MinHash(num_perm=num_perm)
    for t in tokens:
        mh.update(t.encode("utf-8"))
    return mh


def _item_text(item: InternalItem) -> str:
    """Build normalized text string from an InternalItem for MinHash."""
    parts = [item.name or ""]
    # Append type/standard only if they add info not already in the name
    name_lower = (item.name or "").lower()
    if item.item_type and item.item_type.lower() not in name_lower:
        parts.append(item.item_type)
    if item.standard_text and item.standard_text.lower() not in name_lower:
        parts.append(item.standard_text)
    return normalize_for_minhash(" ".join(parts))


def _item_ngrams(item: InternalItem, ngram_n: int) -> set[str]:
    """Extract char n-gram set + special exact-match tokens from an InternalItem.

    Special tokens (SIZE:, TYPE:, STD:) are added alongside n-grams so that
    items with the same size/type/standard get higher Jaccard similarity when
    queried with matching special tokens.  This improves MinHash recall for
    exact field values without weakening the character-level fuzzy matching.
    """
    from app.matching.normalizer import normalize_size  # noqa: PLC0415

    tokens = char_ngrams(_item_text(item), n=ngram_n)

    size_norm = normalize_size(str(item.size or ""))
    if size_norm:
        tokens.add(f"SIZE:{size_norm}")

    itype = (item.item_type or "").strip().lower()
    if itype:
        tokens.add(f"TYPE:{itype}")

    std_key = (item.standard_key or "").strip()
    if std_key:
        tokens.add(f"STD:{std_key}")

    return tokens


# ── Public API ───────────────────────────────────────────────────────────────


def rebuild_index(
    items: list[InternalItem],
    num_perm: int = 128,
    threshold: float = 0.3,
    ngram_n: int = 4,
    use_type_buckets: bool = True,
) -> None:
    """(Re-)build the LSH index from a list of active catalog items."""
    global _lsh_all, _lsh_by_type, _minhashes, _item_types
    global _num_perm, _ngram_n, _threshold
    _num_perm = num_perm
    _ngram_n = ngram_n
    _threshold = threshold
    _minhashes = {}
    _item_types = {}

    _lsh_all = MinHashLSH(threshold=threshold, num_perm=num_perm)
    type_buckets: dict[str, list[tuple[str, MinHash]]] = {}

    total = len(items)
    log_every = max(total // 20, 1)  # ~5% steps
    for i, item in enumerate(items):
        if i % log_every == 0 or i == total - 1:
            pct = (i + 1) * 100 // total
            print(f"\r  MinHash index: {i + 1}/{total} ({pct}%)", end="", flush=True)
        ngrams = _item_ngrams(item, ngram_n)
        if not ngrams:
            continue
        mh = _make_minhash(ngrams, num_perm)
        key = str(item.id)
        itype = (item.item_type or "").strip().lower()
        try:
            _lsh_all.insert(key, mh)
            _minhashes[item.id] = mh
            _item_types[item.id] = itype
            if use_type_buckets and itype:
                type_buckets.setdefault(itype, []).append((key, mh))
        except ValueError:
            pass
    if total:
        print()  # newline after progress

    # Build per-type LSH indices
    _lsh_by_type = {}
    if use_type_buckets:
        for t, entries in type_buckets.items():
            lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
            for k, mh in entries:
                try:
                    lsh.insert(k, mh)
                except ValueError:
                    pass
            _lsh_by_type[t] = lsh

    _cached_query.cache_clear()
    logger.info(
        "MinHash index rebuilt: %d items, %d type buckets, ngram=%d, perm=%d, threshold=%.2f",
        len(_minhashes), len(_lsh_by_type), ngram_n, num_perm, threshold,
    )


def get_state() -> dict:
    """Export current index state for disk caching."""
    return {
        "lsh_all": _lsh_all,
        "lsh_by_type": _lsh_by_type,
        "minhashes": _minhashes,
        "item_types": _item_types,
        "num_perm": _num_perm,
        "ngram_n": _ngram_n,
        "threshold": _threshold,
    }


def restore_state(state: dict) -> None:
    """Restore index state from disk cache."""
    global _lsh_all, _lsh_by_type, _minhashes, _item_types
    global _num_perm, _ngram_n, _threshold
    _lsh_all = state["lsh_all"]
    _lsh_by_type = state["lsh_by_type"]
    _minhashes = state["minhashes"]
    _item_types = state["item_types"]
    _num_perm = state["num_perm"]
    _ngram_n = state["ngram_n"]
    _threshold = state["threshold"]
    _cached_query.cache_clear()


def query_index(
    text: str,
    item_type: str = "",
    size: str = "",
    standard_text: str = "",
    top_k: int = 20,
    use_type_buckets: bool = True,
    min_candidates_before_fallback: int = 5,
) -> list[int]:
    """Query the LSH index and return up to top_k item IDs."""
    return [r["item_id"] for r in query_index_with_scores(
        text, item_type, size, standard_text, top_k,
        use_type_buckets, min_candidates_before_fallback,
    )]


def query_index_with_scores(
    text: str,
    item_type: str = "",
    size: str = "",
    standard_text: str = "",
    top_k: int = 20,
    use_type_buckets: bool = True,
    min_candidates_before_fallback: int = 5,
) -> list[dict]:
    """Query the LSH index and return up to top_k results with Jaccard similarity.

    Returns list of {"item_id": int, "jaccard": float} sorted by jaccard desc.

    When use_type_buckets is True and item_type matches a known bucket,
    queries the type-specific index first.  Falls back to the global index
    when the type bucket yields fewer than min_candidates_before_fallback results.
    """
    if _lsh_all is None:
        return []

    norm = normalize_for_minhash(text)
    ngrams = char_ngrams(norm, n=_ngram_n)
    if not ngrams:
        return []

    # Add special exact-match tokens to mirror what _item_ngrams() adds for catalog items
    from app.matching.normalizer import normalize_size  # noqa: PLC0415
    query_size_norm = normalize_size(size or "")
    if query_size_norm:
        ngrams.add(f"SIZE:{query_size_norm}")
    if item_type:
        ngrams.add(f"TYPE:{item_type.strip().lower()}")
    if standard_text:
        try:
            from app.matching.standard_analogs import normalize_standard  # noqa: PLC0415
            std_key = normalize_standard(standard_text)
            if std_key:
                ngrams.add(f"STD:{std_key}")
        except Exception:
            pass

    frozen = frozenset(ngrams)
    query_type = (item_type or "").strip().lower()
    return _cached_query(frozen, query_type, use_type_buckets, min_candidates_before_fallback, top_k)


@lru_cache(maxsize=256)
def _cached_query(
    frozen_ngrams: frozenset,
    query_type: str,
    use_type_buckets: bool,
    min_candidates_before_fallback: int,
    top_k: int,
) -> list[dict]:
    """Cached LSH query — avoids re-building MinHash for identical ngram sets."""
    query_mh = _make_minhash(frozen_ngrams, _num_perm)
    result_keys: set[str] = set()

    # Step 1: query type-specific bucket if available
    if use_type_buckets and query_type and query_type in _lsh_by_type:
        try:
            bucket_result = _lsh_by_type[query_type].query(query_mh)
            result_keys.update(bucket_result)
        except ValueError:
            pass

    # Step 2: fallback to global if not enough from type bucket
    if len(result_keys) < min_candidates_before_fallback:
        try:
            global_result = _lsh_all.query(query_mh)
            result_keys.update(global_result)
        except ValueError:
            pass

    # Compute Jaccard similarity for each candidate
    scored = []
    for key in result_keys:
        try:
            item_id = int(key)
        except (TypeError, ValueError):
            continue
        item_mh = _minhashes.get(item_id)
        if item_mh is not None:
            jaccard = query_mh.jaccard(item_mh)
        else:
            jaccard = 0.0
        scored.append({"item_id": item_id, "jaccard": round(jaccard, 3)})

    scored.sort(key=lambda x: -x["jaccard"])
    return scored[:top_k]


def add_to_index(item: InternalItem) -> None:
    """Add or update a single item in the LSH index (global + type bucket)."""
    if _lsh_all is None:
        return

    key = str(item.id)
    old_type = _item_types.get(item.id, "")

    # Remove old entry if exists
    if item.id in _minhashes:
        try:
            _lsh_all.remove(key)
        except ValueError:
            pass
        if old_type and old_type in _lsh_by_type:
            try:
                _lsh_by_type[old_type].remove(key)
            except ValueError:
                pass
        del _minhashes[item.id]
        _item_types.pop(item.id, None)

    ngrams = _item_ngrams(item, _ngram_n)
    if not ngrams:
        return

    mh = _make_minhash(ngrams, _num_perm)
    itype = (item.item_type or "").strip().lower()
    try:
        _lsh_all.insert(key, mh)
        _minhashes[item.id] = mh
        _item_types[item.id] = itype
    except ValueError:
        pass

    # Add to type bucket (create if needed)
    if itype:
        if itype not in _lsh_by_type:
            _lsh_by_type[itype] = MinHashLSH(threshold=_threshold, num_perm=_num_perm)
        try:
            _lsh_by_type[itype].insert(key, mh)
        except ValueError:
            pass
    _cached_query.cache_clear()


def remove_from_index(item_id: int) -> None:
    """Remove an item from the LSH index (global + type bucket)."""
    if _lsh_all is None:
        return

    key = str(item_id)
    itype = _item_types.get(item_id, "")

    if item_id in _minhashes:
        try:
            _lsh_all.remove(key)
        except ValueError:
            pass
        if itype and itype in _lsh_by_type:
            try:
                _lsh_by_type[itype].remove(key)
            except ValueError:
                pass
        del _minhashes[item_id]
        _item_types.pop(item_id, None)
    _cached_query.cache_clear()


def is_index_ready() -> bool:
    """Return True if the LSH index has been built."""
    return _lsh_all is not None
