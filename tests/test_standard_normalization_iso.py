"""ГОСТ Р ИСО is a republication of an ISO standard, not a separate GOST.

The prefix list matched "гост р" before it ever looked at "исо", so
"ГОСТ Р ИСО 4014-2013" became "GOST-ИСО-4014-2013" — a key that equals
nothing: neither the plain "ISO-4014" a supplier writes, nor the
"GOST-4014-2013" sitting in the catalog. One standard, three spellings,
no matches between them.
"""

import pytest

from app.matching.standard_analogs import normalize_standard


@pytest.mark.parametrize("raw, expected", [
    ("ГОСТ Р ИСО 4014-2013", "ISO-4014-2013"),
    ("ГОСТ Р ИСО 4017-2013", "ISO-4017-2013"),
    ("ГОСТ ИСО 4032", "ISO-4032"),
    ("ГОСТ Р ISO 4014", "ISO-4014"),
    ("гост р исо 4014", "ISO-4014"),
])
def test_iso_republished_as_gost_normalizes_to_iso(raw, expected):
    assert normalize_standard(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("ГОСТ 7798-70", "GOST-7798-70"),
    ("ГОСТ Р 52627", "GOST-52627"),
    ("ISO 4017", "ISO-4017"),
    ("ИСО 4014", "ISO-4014"),
    ("DIN 933", "DIN-933"),
    ("DIN  933-A", "DIN-933-A"),
])
def test_other_standards_are_unaffected(raw, expected):
    assert normalize_standard(raw) == expected


@pytest.mark.parametrize("a, b", [
    ("ISO-4014-2013", "ISO-4014"),
    ("GOST-7798-70", "GOST-7798"),
    ("GOST-7798-70", "GOST-7798-85"),
    ("ISO-4014", "ISO-4014"),
])
def test_edition_year_does_not_make_a_different_standard(a, b):
    """The catalog holds both "GOST-4014" and "GOST-4014-2013" — one standard.

    Whether the edition year was written down is a matter of how somebody
    typed it, not of which document is meant.
    """
    from app.matching.standard_analogs import same_standard

    assert same_standard(a, b) is True


@pytest.mark.parametrize("a, b", [
    ("ISO-4014", "ISO-4017"),
    ("DIN-933-A", "DIN-933"),
    ("GOST-7798-70", "ISO-4014"),
    ("DIN-933", "DIN-931"),
])
def test_genuinely_different_standards_stay_different(a, b):
    """A trailing letter is a variant, not a year — DIN 933-A is its own thing."""
    from app.matching.standard_analogs import same_standard

    assert same_standard(a, b) is False


def test_quote_and_order_match_across_edition_years():
    """A quote saying ISO 4014 and an order item saying ISO 4014-2013 agree.

    Both spellings live in the catalog side by side, so a direct match must
    not be downgraded to "standards differ" over a written-down year.
    """
    from app.services.quote_order_matcher import (
        _score_exact_quote_match, _standards_compatible,
    )

    assert _standards_compatible("ISO-4014", "ISO-4014-2013", use_analogs=False) is True
    assert _score_exact_quote_match("ISO-4014", "ISO-4014-2013", use_analogs=False) == 100


def test_different_standards_still_lose_points():
    """The year rule must not quietly equate genuinely different documents."""
    from app.services.quote_order_matcher import (
        _score_exact_quote_match, _standards_compatible,
    )

    assert _standards_compatible("ISO-4014", "GOST-7798-70", use_analogs=False) is False
    assert _score_exact_quote_match("ISO-4014", "GOST-7798-70", use_analogs=False) == 80
