"""DB-backed strength class detection from product name text."""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.database import get_db_session
from app.models import StrengthRule


@dataclass
class StrengthMatchResult:
    raw_match: str
    strength_code: str
    strength_name: str
    strength_family: str
    rule_id: int
    pattern_raw: str
    match_type: str


def load_active_strength_rules() -> list[StrengthRule]:
    """Load active strength rules ordered by priority desc."""
    session = get_db_session()
    try:
        rules = (
            session.query(StrengthRule)
            .filter(StrengthRule.is_active.is_(True))
            .order_by(StrengthRule.priority.desc(), StrengthRule.id)
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def detect_strength_class(
    text: str,
    rules: list[StrengthRule] | None = None,
) -> StrengthMatchResult | None:
    """Detect strength class from text using DB rules.

    Rules are applied in priority order (highest first).
    First match wins.
    """
    if rules is None:
        rules = load_active_strength_rules()

    if not text or not rules:
        return None

    text_lower = text.lower()

    for rule in rules:
        pattern = rule.pattern_raw.lower()
        matched = False
        raw_match = ""

        if rule.match_type == "exact":
            for word in re.split(r"[\s,;/()]+", text_lower):
                if word == pattern:
                    matched = True
                    raw_match = word
                    break
        elif rule.match_type == "regex":
            try:
                m = re.search(pattern, text_lower)
                if m:
                    matched = True
                    raw_match = text[m.start():m.end()]
            except re.error:
                continue
        else:  # contains (default)
            pos = text_lower.find(pattern)
            if pos >= 0:
                matched = True
                raw_match = text[pos:pos + len(pattern)]

        if matched:
            return StrengthMatchResult(
                raw_match=raw_match,
                strength_code=rule.strength_code,
                strength_name=rule.strength_name,
                strength_family=rule.strength_family,
                rule_id=rule.id,
                pattern_raw=rule.pattern_raw,
                match_type=rule.match_type,
            )

    return None
