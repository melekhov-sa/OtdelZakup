"""Rebuild the DIN <-> ГОСТ / ISO pair list from the tdm-neva.ru tables.

The six pages hold a DIN-per-row table with a ГОСТ column and an ISO column.
This script turns them into the literal that lives in
``app/seed_standard_equivalents.py`` — run it when the source tables change and
paste the output over ``DIN_GOST_PAIRS`` there.

Usage:
    python scripts/fetch_din_gost_table.py
"""
import html
import re
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.matching.standard_analogs import normalize_standard  # noqa: E402

BASE = "https://tdm-neva.ru/information/"
PAGES = [
    "din-1-399-gost.htm",
    "din-400-699-gost.htm",
    "din-700-999-gost.htm",
    "din-1000-3999-gost.htm",
    "din-4000-6999-gost.htm",
    "din-7000-82999-gost.htm",
]

# "DIN 933 SZ", "DIN 125 А", "DIN 7504 M (N)" all describe a variant of one
# standard, and the catalog keeps them under the bare number.  The numeric part
# after a hyphen is different — "DIN 439-2" is its own standard, so it stays.
_DIN_NUMBER = re.compile(r"^DIN\s*(\d+(?:-\d+)?)", re.IGNORECASE)
_GOST = re.compile(r"ГОСТ(?:\s+Р)?(?:\s+ИСО)?\s+\d[\d.]*(?:-\d+)?")
_ISO = re.compile(r"ISO\s+\d[\d.]*(?:-\d+)?")


def _cells(row_html: str) -> list[str]:
    out = []
    for cell in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.S | re.I):
        text = re.sub(r"<[^>]+>", " ", cell)
        out.append(re.sub(r"\s+", " ", html.unescape(text)).strip())
    return out


def _fetch(page: str) -> str:
    request = urllib.request.Request(BASE + page, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", "replace")


def collect_pairs() -> list[tuple[str, str]]:
    """Return deduplicated (DIN key, other key) pairs in canonical form."""
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for page in PAGES:
        raw = _fetch(page)
        for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", raw, re.S | re.I):
            cells = _cells(row_html)
            if len(cells) < 4 or not cells[1].upper().startswith("DIN"):
                continue

            number = _DIN_NUMBER.match(cells[1])
            if not number:
                continue
            din_key = normalize_standard(f"DIN {number.group(1)}")
            if not din_key:
                continue

            others = [m.group(0) for m in _GOST.finditer(cells[3])]
            if len(cells) > 4:
                others += [m.group(0) for m in _ISO.finditer(cells[4])]

            for raw_other in others:
                other_key = normalize_standard(raw_other)
                if not other_key or other_key == din_key:
                    continue
                pair = (din_key, other_key)
                if pair in seen:
                    continue
                seen.add(pair)
                pairs.append(pair)

        time.sleep(0.4)

    return pairs


def main() -> int:
    pairs = collect_pairs()
    gost = sum(1 for _, other in pairs if other.startswith("GOST-"))
    iso = len(pairs) - gost

    print(f"# {len(pairs)} пар: {gost} DIN-ГОСТ, {iso} DIN-ISO")
    print("DIN_GOST_PAIRS = [")
    for din_key, other_key in pairs:
        print(f'    ("{din_key}", "{other_key}"),')
    print("]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
