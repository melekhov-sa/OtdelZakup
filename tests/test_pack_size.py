"""Tests for reading pack size out of a supplier's own line text.

Suppliers often price a package but write its contents in the name:
"Болт М12х80, уп. 100 шт".  That number is more trustworthy than our
nomenclature weight, so it drives price conversion when present.
"""


def test_pack_size_read_from_explicit_packaging_note():
    from app.services.comparison_service import extract_pack_size

    assert extract_pack_size("Болт М12х80 оц, уп. 100 шт") == 100


def test_bare_quantity_is_not_a_pack_size():
    """"100 шт" without a packaging word is an amount, not packaging.

    Reading it as a pack would divide the supplier's price by a number the
    customer merely asked for — a silent, plausible-looking error.
    """
    from app.services.comparison_service import extract_pack_size

    assert extract_pack_size("Болт М12х80 ГОСТ 7798-70 оц 100 шт") is None
    assert extract_pack_size("Болт М12х80") is None


def test_pack_size_accepts_common_spellings():
    from app.services.comparison_service import extract_pack_size

    assert extract_pack_size("Саморез 4.2х16, упаковка 500 шт") == 500
    assert extract_pack_size("Гайка М8 уп 250шт") == 250
    assert extract_pack_size("Шайба 12, в упаковке 1000 шт") == 1000
