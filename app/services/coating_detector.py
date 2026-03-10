"""DB-backed coating detection from product name text."""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.database import get_db_session
from app.models import CoatingRule


@dataclass
class CoatingMatchResult:
    raw_match: str
    coating_code: str
    coating_name: str
    rule_id: int
    pattern_raw: str
    match_type: str


def load_active_coating_rules() -> list[CoatingRule]:
    """Load active coating rules ordered by priority desc."""
    session = get_db_session()
    try:
        rules = (
            session.query(CoatingRule)
            .filter(CoatingRule.is_active.is_(True))
            .order_by(CoatingRule.priority.desc(), CoatingRule.id)
            .all()
        )
        session.expunge_all()
        return rules
    finally:
        session.close()


def detect_coating(
    text: str,
    rules: list[CoatingRule] | None = None,
) -> CoatingMatchResult | None:
    """Detect coating from text using DB rules.

    Rules are applied in priority order (highest first).
    First match wins.
    """
    if rules is None:
        rules = load_active_coating_rules()

    if not text or not rules:
        return None

    text_lower = text.lower()

    for rule in rules:
        pattern = rule.pattern_raw.lower()
        matched = False
        raw_match = ""

        if rule.match_type == "exact":
            # Word-boundary exact match
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
                    raw_match = m.group(0)
            except re.error:
                continue
        else:  # contains (default)
            pos = text_lower.find(pattern)
            if pos >= 0:
                matched = True
                # Extract the original-case match from the source text
                raw_match = text[pos:pos + len(pattern)]

        if matched:
            return CoatingMatchResult(
                raw_match=raw_match,
                coating_code=rule.coating_code,
                coating_name=rule.coating_name,
                rule_id=rule.id,
                pattern_raw=rule.pattern_raw,
                match_type=rule.match_type,
            )

    return None
