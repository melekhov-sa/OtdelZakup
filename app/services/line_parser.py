"""Shared parsing and MinHash utilities for both client lines and quote lines.

Reuses existing normalizers and extractors — no duplication.
"""
from __future__ import annotations

import csv
import io
import logging

from datasketch import MinHash

from app.matching.normalizer import normalize_size
from app.matching.text_normalizer import char_ngrams, normalize_for_minhash

logger = logging.getLogger(__name__)

NUM_PERM = 128
NGRAM_N = 4


# ── Field extraction ────────────────────────────────────────────────────────


def parse_raw_line(raw_text: str) -> dict:
    """Extract all structured fields from raw text.

    Returns dict with keys: item_type, size, size_norm, diameter, length,
    gost, din, iso, std_norm, strength, coating, tokens_norm.
    """
    from app.extractors import (
        extract_coating, extract_diameter, extract_din,
        extract_gost, extract_iso, extract_item_type,
        extract_length, extract_size, extract_strength,
    )
    from app.matching.standard_analogs import normalize_standard

    item_type = extract_item_type(raw_text) or ""
    size_raw = extract_size(raw_text) or ""
    size_norm = normalize_size(size_raw) if size_raw else ""
    diameter = extract_diameter(raw_text) or ""
    length = extract_length(raw_text) or ""
    strength = extract_strength(raw_text) or ""
    coating = extract_coating(raw_text) or ""

    gost = extract_gost(raw_text) or ""
    din = extract_din(raw_text) or ""
    iso = extract_iso(raw_text) or ""

    std_norm = ""
    for std_raw in (gost, din, iso):
        if std_raw:
            key = normalize_standard(std_raw)
            if key:
                std_norm = key
                break

    tokens_norm = normalize_for_minhash(raw_text)

    return {
        "item_type": item_type,
        "size": size_raw,
        "size_norm": size_norm,
        "diameter": diameter,
        "length": length,
        "gost": gost,
        "din": din,
        "iso": iso,
        "std_norm": std_norm,
        "strength": strength,
        "coating": coating,
        "tokens_norm": tokens_norm,
    }


def norm_fields_from_parsed(parsed: dict) -> dict:
    """Extract the 4 norm columns from a parsed dict."""
    return {
        "type_norm": parsed.get("item_type") or "",
        "size_norm": parsed.get("size_norm") or "",
        "std_norm": parsed.get("std_norm") or "",
        "tokens_norm": parsed.get("tokens_norm") or "",
    }


# ── MinHash building ───────────────────────────────────────────────────────


def build_features(
    tokens_norm: str,
    type_norm: str = "",
    size_norm: str = "",
    std_norm: str = "",
    ngram_n: int = NGRAM_N,
) -> set[str]:
    """Char n-grams + special tokens (TYPE:, SIZE:, STD:)."""
    tokens = char_ngrams(tokens_norm, n=ngram_n)
    if size_norm:
        tokens.add(f"SIZE:{size_norm}")
    if type_norm:
        tokens.add(f"TYPE:{type_norm.strip().lower()}")
    if std_norm:
        tokens.add(f"STD:{std_norm}")
    return tokens


def build_minhash(features: set[str], num_perm: int = NUM_PERM) -> MinHash:
    """Build a MinHash from a set of string tokens."""
    mh = MinHash(num_perm=num_perm)
    for t in features:
        mh.update(t.encode("utf-8"))
    return mh


# ── File parsing ──────────────────────────────────────────────────────────


def read_tabular_file(file_bytes: bytes, filename: str) -> list[list[str]]:
    """Read Excel or CSV file into a list of rows (list of string cells)."""
    ext = (filename.rsplit(".", 1)[-1] if "." in filename else "").lower()

    if ext == "csv":
        for encoding in ("utf-8-sig", "cp1251"):
            try:
                text = file_bytes.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            text = file_bytes.decode("utf-8", errors="replace")
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|")
        reader = csv.reader(io.StringIO(text), dialect)
        return [[cell for cell in row] for row in reader]

    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    rows = []
    for row in ws.iter_rows(values_only=True):
        rows.append([str(c) if c is not None else "" for c in row])
    wb.close()
    return rows


def parse_quote_file(file_bytes: bytes, filename: str) -> tuple[list[str], list[list]]:
    """Parse a file and return (headers, data_rows) for wizard preview."""
    rows_raw = read_tabular_file(file_bytes, filename)
    if not rows_raw:
        return [], []
    return rows_raw[0], rows_raw[1:]


def parse_client_file(file_bytes: bytes, filename: str) -> list[dict]:
    """Parse client request file: auto-detect columns.

    Returns list of {name, qty, unit}.
    """
    rows_raw = read_tabular_file(file_bytes, filename)
    if len(rows_raw) < 2:
        return []

    headers = [h.strip().lower() for h in rows_raw[0]]
    name_col = qty_col = unit_col = None
    for i, h in enumerate(headers):
        if any(kw in h for kw in ("наименование", "название", "позиция", "товар", "name")):
            name_col = i
        elif any(kw in h for kw in ("кол", "количество", "qty")):
            qty_col = i
        elif any(kw in h for kw in ("ед", "единиц", "unit", "изм")):
            unit_col = i

    if name_col is None:
        name_col = 0

    result = []
    for row in rows_raw[1:]:
        name = row[name_col].strip() if name_col < len(row) else ""
        if not name:
            continue
        qty = None
        if qty_col is not None and qty_col < len(row) and row[qty_col].strip():
            try:
                qty = float(row[qty_col].replace(",", "."))
            except (ValueError, AttributeError):
                pass
        unit = row[unit_col].strip() if unit_col is not None and unit_col < len(row) else ""
        result.append({"name": name, "qty": qty, "unit": unit})
    return result
