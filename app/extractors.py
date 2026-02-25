"""Deterministic regex-based field extractors for fastener descriptions."""

import re

import pandas as pd


def _str(value: object) -> str:
    """Safely cast a cell value to string."""
    if pd.isna(value):
        return ""
    return str(value)


def _concat_row(row: pd.Series) -> str:
    """Join all cell values of a row into a single search string."""
    return " ".join(_str(v) for v in row)


# ── Individual extractors ────────────────────────────────────

def extract_diameter(text: str) -> str:
    m = re.search(r"[MМ](\d+)", text)
    return f"M{m.group(1)}" if m else ""


def extract_length(text: str) -> str:
    m = re.search(r"[xхXХ](\d+)", text)
    return m.group(1) if m else ""


def extract_size(text: str) -> str:
    d = extract_diameter(text)
    l_ = extract_length(text)
    if d and l_:
        return f"{d}x{l_}"
    return ""


def extract_strength(text: str) -> str:
    # Explicit class notation: 8.8  10.9  12.9
    m = re.search(r"\b(8\.8|10\.9|12\.9)\b", text)
    if m:
        return m.group(1)
    # Encoded tail form: .88 → 8.8, .109 → 10.9
    m = re.search(r"\.88(?!\d)", text)
    if m:
        return "8.8"
    m = re.search(r"\.109(?!\d)", text)
    if m:
        return "10.9"
    return ""


def extract_coating(text: str) -> str:
    lower = text.lower()
    if re.search(r"оцинк|оц\b|цинк", lower):
        return "оцинк."
    return ""


def extract_gost(text: str) -> str:
    m = re.search(r"ГОСТ\s*\d+[\-\.]\d+", text)
    if m:
        return m.group(0)
    m = re.search(r"ISO\s*\d+", text, re.IGNORECASE)
    if m:
        return m.group(0)
    return ""


def extract_din(text: str) -> str:
    m = re.search(r"DIN\s*\d+", text, re.IGNORECASE)
    return m.group(0) if m else ""


def extract_tail_code(text: str) -> str:
    m = re.search(r"\.\d{2,3}\.\d{2,3}", text)
    return m.group(0) if m else ""


# ── Registry ─────────────────────────────────────────────────

EXTRACTORS: dict[str, tuple[str, callable]] = {
    "diameter":  ("Диаметр",          extract_diameter),
    "length":    ("Длина",            extract_length),
    "size":      ("Размер MxL",       extract_size),
    "strength":  ("Класс прочности",  extract_strength),
    "coating":   ("Покрытие",         extract_coating),
    "gost":      ("ГОСТ/ISO",         extract_gost),
    "din":       ("DIN",              extract_din),
    "tail_code": ("Хвост-код",        extract_tail_code),
}

ALL_FIELD_KEYS = list(EXTRACTORS.keys())


def transform_dataframe(
    df: pd.DataFrame,
    fields: list[str],
) -> pd.DataFrame:
    """Apply selected extractors to every row and return an augmented DataFrame."""
    result = df.copy()

    for key in fields:
        if key not in EXTRACTORS:
            continue
        col_name, func = EXTRACTORS[key]
        result[col_name] = df.apply(lambda row, fn=func: fn(_concat_row(row)), axis=1)

    return result
