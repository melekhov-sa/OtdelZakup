"""Canonical normalization for fastener standard references.

Converts raw standard strings like "DIN438", "DIN 438", "ГОСТ 7798-70"
into canonical StandardToken objects with a stable key ("DIN-438", "GOST-7798-70").

The key is used for reliable cross-format matching in the matcher.
"""

import re
from dataclasses import dataclass
from typing import Optional

# Number pattern: "438", "7798-70", "4017", "16.1"
_NUM = r"\d[\d.]*(?:-\d+)?"

_GOST_SCAN = re.compile(
    r"(?:ГОСТ|гост|GOST|gost)\s*(?:[Рр]\s*(?:ИСО|исо|ISO)\s*)?" + f"({_NUM})",
    re.UNICODE,
)
_DIN_SCAN = re.compile(r"[Dd][Ii][Nn]\s*" + f"({_NUM})")
_ISO_SCAN = re.compile(r"(?:ISO|iso|ИСО|исо)\s*" + f"({_NUM})", re.UNICODE)


@dataclass
class StandardToken:
    system: str   # "GOST" | "ISO" | "DIN"
    number: str   # "7798-70" | "4017" | "438"
    key: str      # "GOST-7798-70" | "ISO-4017" | "DIN-438"
    display: str  # "ГОСТ 7798-70" | "ISO 4017" | "DIN 438"


def normalize_standard_token(text: str) -> Optional[StandardToken]:
    """Parse a single standard string into a canonical StandardToken.

    Examples:
        "DIN438"      → StandardToken("DIN", "438", "DIN-438", "DIN 438")
        "DIN 438"     → StandardToken("DIN", "438", "DIN-438", "DIN 438")
        "ГОСТ 7798-70"→ StandardToken("GOST", "7798-70", "GOST-7798-70", "ГОСТ 7798-70")
        "ISO4032"     → StandardToken("ISO", "4032", "ISO-4032", "ISO 4032")
    """
    t = text.strip()
    if not t:
        return None

    # ГОСТ — match before ISO to avoid ГОСТ Р ИСО ambiguity
    m = re.fullmatch(
        r"(?:ГОСТ|гост|GOST|gost)\s*(?:[Рр]\s*(?:ИСО|исо|ISO)\s*)?" + f"({_NUM})",
        t, re.UNICODE,
    )
    if m:
        num = m.group(1)
        return StandardToken("GOST", num, f"GOST-{num}", f"ГОСТ {num}")

    # DIN
    m = re.fullmatch(r"[Dd][Ii][Nn]\s*" + f"({_NUM})", t)
    if m:
        num = m.group(1)
        return StandardToken("DIN", num, f"DIN-{num}", f"DIN {num}")

    # ISO
    m = re.fullmatch(r"(?:ISO|iso|ИСО|исо)\s*" + f"({_NUM})", t, re.UNICODE)
    if m:
        num = m.group(1)
        return StandardToken("ISO", num, f"ISO-{num}", f"ISO {num}")

    return None


def extract_standards(text: str) -> list[StandardToken]:
    """Find all standard references in a text string.

    Returns tokens in order of first appearance, de-duped by key.

    Example:
        "DIN438/ ГОСТ1479 M10x20" →
            [StandardToken("DIN","438","DIN-438","DIN 438"),
             StandardToken("GOST","1479","GOST-1479","ГОСТ 1479")]
    """
    hits: list[tuple[int, StandardToken]] = []
    seen: set[str] = set()

    for m in _GOST_SCAN.finditer(text):
        num = m.group(1)
        key = f"GOST-{num}"
        if key not in seen:
            seen.add(key)
            hits.append((m.start(), StandardToken("GOST", num, key, f"ГОСТ {num}")))

    for m in _DIN_SCAN.finditer(text):
        num = m.group(1)
        key = f"DIN-{num}"
        if key not in seen:
            seen.add(key)
            hits.append((m.start(), StandardToken("DIN", num, key, f"DIN {num}")))

    for m in _ISO_SCAN.finditer(text):
        num = m.group(1)
        # Skip ISO that's part of a "ГОСТ Р ИСО" prefix
        before = text[max(0, m.start() - 30): m.start()]
        if re.search(r"(?:ГОСТ|гост|GOST|gost)\s*[Рр]\s*$", before, re.UNICODE):
            continue
        key = f"ISO-{num}"
        if key not in seen:
            seen.add(key)
            hits.append((m.start(), StandardToken("ISO", num, key, f"ISO {num}")))

    hits.sort(key=lambda x: x[0])
    return [tok for _, tok in hits]


def standard_key_from_text(text: str) -> Optional[str]:
    """Get the primary standard key from a raw standard string.

    "DIN 438"   → "DIN-438"
    "ГОСТ 7798-70" → "GOST-7798-70"
    Returns None if no standard recognized.
    """
    if not text or not text.strip():
        return None
    tok = normalize_standard_token(text)
    if tok:
        return tok.key
    # Fallback: scan for embedded standard in longer text
    tokens = extract_standards(text)
    return tokens[0].key if tokens else None
