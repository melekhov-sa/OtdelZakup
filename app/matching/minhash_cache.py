"""Disk cache for the MinHash LSH index.

Fingerprint = SHA-1(catalog_version + num_perm + threshold + ngram_n + use_type_buckets).
If the fingerprint matches the saved one, the index is loaded from disk instead of rebuilt.
The cache is invalidated (fingerprint file deleted) whenever catalog items change.
"""
from __future__ import annotations

import hashlib
import logging
import os
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


def _atomic_write_bytes_via_pickle(target: Path, state: dict) -> None:
    """Stream-pickle state to a temp file and atomically rename to target.

    Streaming via `pickle.dump` avoids the ~2× peak memory spike of
    `pickle.dumps(...) + write_bytes(...)` on large indices (hundreds of MB).
    The temp-file + rename pattern guarantees readers never see a half-written file.
    """
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        with tmp.open("wb") as f:
            pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _atomic_write_text(target: Path, text: str) -> None:
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def save(cache_dir: Path, fingerprint: str, state: dict) -> None:
    """Serialize index state to disk atomically.

    Failures are logged with a stack trace but do not raise — a failed save
    simply forces a rebuild on the next startup (same behaviour as if the
    file was missing).
    """
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = _cache_file(cache_dir)
        # Write fingerprint first so a crash between fp and pkl leaves no orphaned pkl
        _atomic_write_text(_fp_file(cache_dir), fingerprint)
        _atomic_write_bytes_via_pickle(target, state)
        size_mb = target.stat().st_size / (1024 * 1024)
        logger.info(
            "MinHash cache saved: %d items, %.1f MB, fp=%s",
            len(state.get("minhashes", {})), size_mb, fingerprint[:8],
        )
    except Exception:
        logger.exception(
            "MinHash cache save failed (cache_dir=%s, items=%d) — "
            "next startup will rebuild the index",
            cache_dir, len(state.get("minhashes", {})),
        )


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
        with cache_path.open("rb") as f:
            state = pickle.load(f)
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
