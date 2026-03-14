"""Disk cache for the MinHash LSH index.

Fingerprint = SHA-1(catalog_version + num_perm + threshold + ngram_n + use_type_buckets).
If the fingerprint matches the saved one, the index is loaded from disk instead of rebuilt.
The cache is invalidated (fingerprint file deleted) whenever catalog items change.
"""
from __future__ import annotations

import hashlib
import logging
import pickle
from pathlib import Path

logger = logging.getLogger(__name__)


def compute_fingerprint(
    catalog_version: int,
    num_perm: int,
    threshold: float,
    ngram_n: int,
    use_type_buckets: bool,
) -> str:
    key = f"{catalog_version}:{num_perm}:{threshold:.6f}:{ngram_n}:{use_type_buckets}"
    return hashlib.sha1(key.encode()).hexdigest()


def _cache_file(cache_dir: Path) -> Path:
    return cache_dir / "minhash_index.pkl"


def _fp_file(cache_dir: Path) -> Path:
    return cache_dir / "minhash_index.fp"


def save(cache_dir: Path, fingerprint: str, state: dict) -> None:
    """Serialize index state to disk."""
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        _cache_file(cache_dir).write_bytes(pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL))
        _fp_file(cache_dir).write_text(fingerprint, encoding="utf-8")
        logger.info("MinHash cache saved: %d items, fp=%s", len(state.get("minhashes", {})), fingerprint[:8])
    except Exception:
        logger.exception("MinHash cache save failed")


def load(cache_dir: Path, fingerprint: str) -> dict | None:
    """Load index state from disk if fingerprint matches. Returns None on any mismatch."""
    fp_path = _fp_file(cache_dir)
    cache_path = _cache_file(cache_dir)

    if not fp_path.exists() or not cache_path.exists():
        return None

    saved_fp = fp_path.read_text(encoding="utf-8").strip()
    if saved_fp != fingerprint:
        logger.info("MinHash cache miss (saved=%s want=%s) — rebuilding", saved_fp[:8], fingerprint[:8])
        return None

    try:
        state = pickle.loads(cache_path.read_bytes())
        logger.info("MinHash cache hit: %d items, fp=%s", len(state.get("minhashes", {})), fingerprint[:8])
        return state
    except Exception:
        logger.exception("MinHash cache load failed — rebuilding")
        return None


def invalidate(cache_dir: Path) -> None:
    """Delete fingerprint so cache is ignored on next startup."""
    try:
        _fp_file(cache_dir).unlink(missing_ok=True)
    except Exception:
        pass
