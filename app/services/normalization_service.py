"""Unified normalization service for coating, strength, and size detection.

All three detection functions use the same normalization_rules table,
filtered by rule_type. Rules are applied in priority order (highest first).
First match wins.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from app.database import get_db_session
from app.models import NormalizationRule


# ── Shared result dataclass ──────────────────────────────────────────────────

@dataclass
class NormMatch:
    rule_id: int
    rule_type: str
    raw_match: str
    normalized_code: str
    normalized_name: str
    extra: dict
    pattern_raw: str
    match_type: str


# ── Size text preprocessing ─────────────────────────────────────────────────

_CYR_TO_LAT = str.maketrans({
    "\u041c": "M",  # М → M
    "\u043c": "m",  # м → m
    "\u0425": "x",  # Х → x
    "\u0445": "x",  # х → x
})


def _preprocess_size_text(text: str) -> str:
    """Normalize text for size matching: Cyrillic → Latin, * → x, comma → dot."""
    s = text.translate(_CYR_TO_LAT)
    s = s.replace("\u00d7", "x")  # × → x
    # * → x only between digits (size separator like 8*20); leading * is a bullet marker
    s = re.sub(r"(\d)\*(\d)", r"\1x\2", s)
    s = s.replace(",", ".")
    return s


def _normalize_decimal(val: str) -> str:
    if not val:
        return val
    return val.replace(",", ".")


# ── Rule loader ─────────────────────────────────────────────────────────────

def load_rules(rule_type: str) -> list[NormalizationRule]:
    """Load active rules of given type, ordered by priority desc."""
    session = get_db_session()
    try:
        rules = (
            session.query(NormalizationRule)
            .filter(
                NormalizationRule.rule_type == rule_type,
                NormalizationRule.is_active.is_(True),
            )
            .order_by(NormalizationRule.priority.desc(), NormalizationRule.id)
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


# ── Generic matching engine ────────────────────────────────────────────────

def _match_rule(
    rule: NormalizationRule,
    text_lower: str,
    text_original: str,
    use_ignorecase: bool = False,
) -> tuple[bool, str, dict]:
    """Try to match a single rule against text.

    Returns (matched, raw_match, regex_groups).
    """
    matched = False
    raw_match = ""
    groups: dict[str, str] = {}

    if rule.match_type == "regex":
        try:
            if use_ignorecase:
                m = re.search(rule.pattern_raw, text_original, re.IGNORECASE)
            else:
                m = re.search(rule.pattern_raw.lower(), text_lower)
            if m:
                matched = True
                raw_match = text_original[m.start():m.end()]
                groups = {k: (v or "") for k, v in m.groupdict().items()}
        except re.error:
            pass
    elif rule.match_type == "exact":
        pattern = rule.pattern_raw.lower()
        for word in re.split(r"[\s,;/()]+", text_lower):
            if word == pattern:
                matched = True
                raw_match = word
                break
    else:  # contains
        pattern = rule.pattern_raw.lower()
        pos = text_lower.find(pattern)
        if pos >= 0:
            matched = True
            raw_match = text_original[pos:pos + len(pattern)]

    return matched, raw_match, groups


# ── Coating detection ────────────────────────────────────────────────────────

def detect_coating(
    text: str,
    rules: list[NormalizationRule] | None = None,
) -> NormMatch | None:
    """Detect coating from text using normalization_rules (rule_type='coating')."""
    if rules is None:
        rules = load_rules("coating")

    if not text or not rules:
        return None

    text_lower = text.lower()

    for rule in rules:
        matched, raw_match, _ = _match_rule(rule, text_lower, text)
        if matched:
            return NormMatch(
                rule_id=rule.id,
                rule_type="coating",
                raw_match=raw_match,
                normalized_code=rule.normalized_code,
                normalized_name=rule.normalized_name,
                extra=rule.extra,
                pattern_raw=rule.pattern_raw,
                match_type=rule.match_type,
            )

    return None


# ── Strength detection ──────────────────────────────────────────────────────

def detect_strength(
    text: str,
    rules: list[NormalizationRule] | None = None,
) -> NormMatch | None:
    """Detect strength class from text using normalization_rules (rule_type='strength')."""
    if rules is None:
        rules = load_rules("strength")

    if not text or not rules:
        return None

    text_lower = text.lower()

    for rule in rules:
        matched, raw_match, _ = _match_rule(rule, text_lower, text)
        if matched:
            return NormMatch(
                rule_id=rule.id,
                rule_type="strength",
                raw_match=raw_match,
                normalized_code=rule.normalized_code,
                normalized_name=rule.normalized_name,
                extra=rule.extra,
                pattern_raw=rule.pattern_raw,
                match_type=rule.match_type,
            )

    return None


# ── Size detection ──────────────────────────────────────────────────────────

def detect_size(
    text: str,
    item_type: str | None = None,
    rules: list[NormalizationRule] | None = None,
) -> NormMatch | None:
    """Detect size/diameter/length from text using normalization_rules (rule_type='size').

    Size rules use regex named groups for structured extraction.
    Extra JSON stores: size_kind, normalize_template.
    The result extra dict contains: size_kind, diameter, length, width,
    thickness, pitch, tolerance, size_norm.
    """
    if rules is None:
        rules = load_rules("size")

    if not text or not rules:
        return None

    preprocessed = _preprocess_size_text(text)

    for rule in rules:
        extra = rule.extra
        normalize_template = extra.get("normalize_template", "")
        size_kind = extra.get("size_kind", "custom")

        # Size rules use IGNORECASE on preprocessed text to preserve case in groups
        matched, raw_match, groups = _match_rule(
            rule, preprocessed.lower(), preprocessed, use_ignorecase=True,
        )

        if not matched:
            continue

        # Extract structured fields from named groups
        d = _normalize_decimal(groups.get("d", ""))
        l = _normalize_decimal(groups.get("l", ""))
        w = _normalize_decimal(groups.get("w", ""))
        t = _normalize_decimal(groups.get("t", ""))
        pitch = _normalize_decimal(groups.get("pitch", ""))
        tol = groups.get("tol", "")

        # Build normalized size from template or raw match
        size_norm = raw_match
        if normalize_template:
            try:
                fmt_vars = {
                    "d": d, "l": l, "w": w, "t": t,
                    "pitch": pitch, "tol": tol,
                    "raw": raw_match,
                }
                size_norm = normalize_template.format_map(fmt_vars)
            except (KeyError, ValueError):
                size_norm = raw_match

        # Derive diameter prefix from normalize_template
        if d and size_kind in ("diameter", "diameter_length", "thread"):
            pos = normalize_template.find("{d}") if normalize_template else -1
            prefix = normalize_template[:pos] if pos > 0 else ""
            if prefix:
                if not d.upper().startswith(prefix.upper()):
                    d = f"{prefix}{d}"
            elif "." not in d and not d.upper().startswith(("M", "D", "\u00d8")):
                d = f"M{d}"

        result_extra = {
            "size_kind": size_kind,
            "diameter": d,
            "length": l,
            "width": w,
            "thickness": t,
            "pitch": pitch,
            "tolerance": tol,
            "size_norm": size_norm,
        }

        return NormMatch(
            rule_id=rule.id,
            rule_type="size",
            raw_match=raw_match,
            normalized_code=size_norm,
            normalized_name=size_norm,
            extra=result_extra,
            pattern_raw=rule.pattern_raw,
            match_type=rule.match_type,
        )

    return None
