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
    """Recognise М12, M12, М12-, М12x, М12х etc. Always return latin M."""
    m = re.search(r"[MМ](\d+)", text)
    return f"M{m.group(1)}" if m else ""


def extract_length(text: str) -> str:
    """Recognise x150, х150, ×150 — the number after the cross-sign."""
    m = re.search(r"[xхXХ×](\d+)", text)
    return m.group(1) if m else ""


def extract_size(text: str) -> str:
    """Combine diameter + length into 'M12x150'."""
    d = extract_diameter(text)
    l_ = extract_length(text)
    if d and l_:
        return f"{d}x{l_}"
    return ""


def extract_strength(text: str) -> str:
    """Recognise 8.8, 10.9, 12.9 (explicit has priority) or codes .88/.109/.129."""
    # Explicit class notation — highest priority
    m = re.search(r"\b(8\.8|10\.9|12\.9)\b", text)
    if m:
        return m.group(1)
    # Encoded tail form
    if re.search(r"\.129(?!\d)", text):
        return "12.9"
    if re.search(r"\.109(?!\d)", text):
        return "10.9"
    if re.search(r"\.88(?!\d)", text):
        return "8.8"
    return ""


_COATING_WORDS = re.compile(r"оцинк|оц(?:\b|\.)|цинк", re.IGNORECASE)
_COATING_CODE = re.compile(r"\.016(?!\d)")


def extract_coating(text: str) -> str:
    """Recognise оц/оцинк/цинк or code .016 → 'цинк'."""
    if _COATING_WORDS.search(text):
        return "цинк"
    if _COATING_CODE.search(text):
        return "цинк"
    return ""


def extract_gost(text: str) -> str:
    """Extract ГОСТ only (not ISO/DIN)."""
    m = re.search(r"ГОСТ\s*[\d]+[\-\.]\d+", text)
    return m.group(0) if m else ""


def extract_iso(text: str) -> str:
    """Extract ISO standard number."""
    m = re.search(r"ISO\s*\d+", text, re.IGNORECASE)
    return m.group(0) if m else ""


def extract_din(text: str) -> str:
    """Extract DIN standard number."""
    m = re.search(r"DIN\s*\d+", text, re.IGNORECASE)
    return m.group(0) if m else ""


def extract_tail_code(text: str) -> str:
    """Extract tail code like .88.016 or .109.016."""
    m = re.search(r"\.\d{2,3}\.\d{2,3}", text)
    return m.group(0) if m else ""


# ── Registry ─────────────────────────────────────────────────

EXTRACTORS: dict[str, tuple[str, callable]] = {
    "diameter":  ("Диаметр",          extract_diameter),
    "length":    ("Длина",            extract_length),
    "size":      ("Размер MxL",       extract_size),
    "strength":  ("Класс прочности",  extract_strength),
    "coating":   ("Покрытие",         extract_coating),
    "gost":      ("ГОСТ",             extract_gost),
    "iso":       ("ISO",              extract_iso),
    "din":       ("DIN",              extract_din),
    "tail_code": ("Хвост-код",        extract_tail_code),
}

ALL_FIELD_KEYS = list(EXTRACTORS.keys())


def compute_confidence(text: str) -> int:
    """Compute simple confidence score (0–5) for a fastener description.

    +1 diameter, +1 length, +1 strength, +1 coating, +1 gost-or-din.
    """
    score = 0
    if extract_diameter(text):
        score += 1
    if extract_length(text):
        score += 1
    if extract_strength(text):
        score += 1
    if extract_coating(text):
        score += 1
    if extract_gost(text) or extract_din(text):
        score += 1
    return score


def compute_status(confidence: int) -> str:
    """Map confidence score to status label."""
    if confidence >= 4:
        return "ok"
    if confidence >= 2:
        return "warning"
    return "error"


def transform_dataframe(
    df: pd.DataFrame,
    fields: list[str],
) -> pd.DataFrame:
    """Apply selected extractors to every row and return an augmented DataFrame.

    Always appends confidence (0–5) and status (ok/warning/error) columns.
    """
    result = df.copy()

    for key in fields:
        if key not in EXTRACTORS:
            continue
        col_name, func = EXTRACTORS[key]
        result[col_name] = df.apply(lambda row, fn=func: fn(_concat_row(row)), axis=1)

    # Always compute confidence & status
    texts = df.apply(_concat_row, axis=1)
    result["confidence"] = texts.apply(compute_confidence)
    result["status"] = result["confidence"].apply(compute_status)

    return result
