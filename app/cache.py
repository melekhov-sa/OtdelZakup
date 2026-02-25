"""File-based cache and shared directory config."""

import hashlib
import json
import os
from pathlib import Path

import pandas as pd

UPLOAD_DIR = Path(os.environ.get("OTDELZAKUP_UPLOAD_DIR", "./data/uploads"))
CACHE_DIR = Path(os.environ.get("OTDELZAKUP_CACHE_DIR", "./data/cache"))


def file_id_from_bytes(data: bytes) -> str:
    """SHA-256 hex digest (first 16 chars) of raw file content."""
    return hashlib.sha256(data).hexdigest()[:16]


def _cache_path(fid: str) -> Path:
    return CACHE_DIR / fid


def save_cache(fid: str, filename: str, df: pd.DataFrame) -> None:
    """Persist DataFrame and metadata to disk."""
    p = _cache_path(fid)
    p.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p / "raw.parquet", index=False, engine="pyarrow")
    meta = {
        "file_id": fid,
        "filename": filename,
        "rows_total": len(df),
        "columns": list(df.columns),
    }
    (p / "meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


def load_meta(fid: str) -> dict | None:
    """Load metadata for a cached file. Returns None if not found."""
    meta_file = _cache_path(fid) / "meta.json"
    if not meta_file.exists():
        return None
    return json.loads(meta_file.read_text(encoding="utf-8"))


def load_dataframe(fid: str) -> pd.DataFrame | None:
    """Load cached DataFrame. Returns None if not found."""
    pq = _cache_path(fid) / "raw.parquet"
    if not pq.exists():
        return None
    return pd.read_parquet(pq, engine="pyarrow")
