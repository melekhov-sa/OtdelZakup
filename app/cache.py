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


def save_cache(
    fid: str,
    filename: str,
    df: pd.DataFrame,
    detected_columns: dict | None = None,
    manual_override: bool = False,
    source_kind: str | None = None,
    docai_headers: list | None = None,
) -> None:
    """Persist DataFrame and metadata to disk.

    Extra kwargs for Google Document AI table sources:
        source_kind:   "docai_table" | "docai_text" | None (→ Excel/other)
        docai_headers: list of column header strings extracted by Document AI
    """
    p = _cache_path(fid)
    p.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p / "raw.parquet", index=False, engine="pyarrow")
    meta = {
        "file_id": fid,
        "filename": filename,
        "rows_total": len(df),
        "columns": list(df.columns),
        "detected_columns": detected_columns or {},
        "manual_override": manual_override,
    }
    if source_kind:
        meta["source_kind"] = source_kind
    if docai_headers is not None:
        meta["docai_headers"] = docai_headers
    (p / "meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


def save_raw_cache(fid: str, filename: str, values_2d: list[list], detected_info: dict) -> None:
    """Save raw 2D cell values for manual column selection (no parquet yet)."""
    p = _cache_path(fid)
    p.mkdir(parents=True, exist_ok=True)

    # Serialize raw values as JSON (mixed types, no column names)
    (p / "raw_values.json").write_text(
        json.dumps(values_2d, ensure_ascii=False, default=str), encoding="utf-8"
    )

    meta = {
        "file_id": fid,
        "filename": filename,
        "rows_total": 0,
        "columns": [],
        "detected_columns": detected_info,
        "manual_override": False,
        "needs_column_selection": True,
    }
    (p / "meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


def load_raw_values(fid: str) -> list[list] | None:
    """Load raw 2D cell values saved for manual column selection."""
    raw_file = _cache_path(fid) / "raw_values.json"
    if not raw_file.exists():
        return None
    return json.loads(raw_file.read_text(encoding="utf-8"))


def update_cache_with_columns(
    fid: str,
    df: pd.DataFrame,
    detected_columns: dict | None = None,
    manual_override: bool = True,
) -> None:
    """Write parquet and update meta.json after manual column selection."""
    p = _cache_path(fid)
    p.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p / "raw.parquet", index=False, engine="pyarrow")

    # Load existing meta to preserve filename
    meta_file = p / "meta.json"
    if meta_file.exists():
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
    else:
        meta = {"file_id": fid, "filename": "unknown.xlsx"}

    meta.update({
        "rows_total": len(df),
        "columns": list(df.columns),
        "detected_columns": detected_columns or {},
        "manual_override": manual_override,
        "needs_column_selection": False,
    })
    meta_file.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


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


def _fields_hash(fields: list[str]) -> str:
    """Short hash for a sorted list of field keys."""
    key = ",".join(sorted(fields))
    return hashlib.sha256(key.encode()).hexdigest()[:8]


def make_download_token(fid: str, fields: list[str]) -> str:
    """Create a deterministic download token from file_id + fields."""
    return f"{fid}_{_fields_hash(fields)}"


def save_result(token: str, fid: str, df: pd.DataFrame) -> None:
    """Save transformed result DataFrame to cache."""
    p = _cache_path(fid)
    p.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p / f"result_{token}.parquet", index=False, engine="pyarrow")


def load_result(token: str, fid: str) -> pd.DataFrame | None:
    """Load a previously saved result DataFrame."""
    pq = _cache_path(fid) / f"result_{token}.parquet"
    if not pq.exists():
        return None
    return pd.read_parquet(pq, engine="pyarrow")
