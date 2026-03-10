"""DB-backed size/diameter/length detection from product name text.

Regex patterns use named groups for structured extraction:
  (?P<d>...)    — diameter
  (?P<l>...)    — length
  (?P<w>...)    — width
  (?P<t>...)    — thickness
  (?P<pitch>...)— thread pitch
  (?P<tol>...)  — tolerance (e.g. 7H)

normalize_template uses Python str.format_map with the matched groups,
e.g. "M{d}x{l}" → "M12x50".
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from app.database import get_db_session
from app.models import SizeRule


# Cyrillic → Latin transliteration for size text
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


@dataclass
class SizeMatchResult:
    raw_match: str
    size_kind: str
    size_norm: str
    diameter: str
    length: str
    width: str
    thickness: str
    pitch: str
    tolerance: str
    rule_id: int
    pattern_raw: str
    match_type: str


def load_active_size_rules() -> list[SizeRule]:
    """Load active size rules ordered by priority desc."""
    session = get_db_session()
    try:
        rules = (
            session.query(SizeRule)
            .filter(SizeRule.is_active.is_(True))
            .order_by(SizeRule.priority.desc(), SizeRule.id)
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def _normalize_decimal(val: str) -> str:
    """Normalize comma to dot in decimal, strip trailing zeros."""
    if not val:
        return val
    return val.replace(",", ".")


def detect_size(
    text: str,
    item_type: str | None = None,
    rules: list[SizeRule] | None = None,
) -> SizeMatchResult | None:
    """Detect size from text using DB rules.

    Rules are applied in priority order (highest first).
    First match wins.
    """
    if rules is None:
        rules = load_active_size_rules()

    if not text or not rules:
        return None

    preprocessed = _preprocess_size_text(text)
    text_lower = preprocessed.lower()

    for rule in rules:
        matched = False
        raw_match = ""
        groups: dict[str, str] = {}

        if rule.match_type == "regex":
            try:
                m = re.search(rule.pattern_raw, preprocessed, re.IGNORECASE)
                if m:
                    matched = True
                    raw_match = preprocessed[m.start():m.end()]
                    groups = {k: (v or "") for k, v in m.groupdict().items()}
            except re.error:
                continue
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
                raw_match = preprocessed[pos:pos + len(pattern)]

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
        if rule.normalize_template:
            try:
                fmt_vars = {
                    "d": d, "l": l, "w": w, "t": t,
                    "pitch": pitch, "tol": tol,
                    "raw": raw_match,
                }
                size_norm = rule.normalize_template.format_map(fmt_vars)
            except (KeyError, ValueError):
                size_norm = raw_match

        # Derive diameter prefix from normalize_template (text before {d})
        # e.g. "M{d}x{l}" → "M", "d{d}" → "d", "Ø{d}" → "Ø", "{d}x{l}" → ""
        if d and rule.size_kind in ("diameter", "diameter_length", "thread"):
            tmpl = rule.normalize_template or ""
            pos = tmpl.find("{d}")
            prefix = tmpl[:pos] if pos > 0 else ""
            if prefix:
                # Template specifies a prefix — use it (e.g. "M", "d", "Ø")
                if not d.upper().startswith(prefix.upper()):
                    d = f"{prefix}{d}"
            elif "." not in d and not d.upper().startswith(("M", "D", "Ø")):
                # No template prefix + plain integer → default M
                d = f"M{d}"

        return SizeMatchResult(
            raw_match=raw_match,
            size_kind=rule.size_kind,
            size_norm=size_norm,
            diameter=d,
            length=l,
            width=w,
            thickness=t,
            pitch=pitch,
            tolerance=tol,
            rule_id=rule.id,
            pattern_raw=rule.pattern_raw,
            match_type=rule.match_type,
        )

    return None
