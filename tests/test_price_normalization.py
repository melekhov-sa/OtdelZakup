"""Tests for converting supplier prices to the unit of our position.

Suppliers quote in штуки, килограммы and упаковки. Comparing those numbers
as-is is worse than showing nothing, so every converted price carries the
basis it was derived from.
"""


def test_same_unit_needs_no_conversion():
    from app.services.comparison_service import normalize_price

    assert normalize_price(14.20, quote_unit="шт", position_unit="шт") == (14.20, "same_unit")


def test_kilograms_convert_via_item_weight():
    """167 ₽/кг for a bolt weighing 85 g is 14.195 ₽/шт."""
    from app.services.comparison_service import normalize_price

    value, basis = normalize_price(167.0, quote_unit="кг", position_unit="шт", weight_kg=0.085)

    assert basis == "weight"
    assert round(value, 3) == 14.195


def test_missing_weight_yields_no_price_rather_than_a_wrong_one():
    """Without a weight there is no honest conversion — the value must be empty.

    Passing the kilogram price through would look like a comparable number
    and mislead worse than an empty cell.
    """
    from app.services.comparison_service import normalize_price

    assert normalize_price(167.0, quote_unit="кг", position_unit="шт", weight_kg=None) == (None, "none")
    assert normalize_price(167.0, quote_unit="кг", position_unit="шт", weight_kg=0) == (None, "none")


def test_pack_price_divides_by_pack_size():
    """1420 ₽ for a 100-piece pack is 14.20 ₽/шт."""
    from app.services.comparison_service import normalize_price

    value, basis = normalize_price(1420.0, quote_unit="уп", position_unit="шт", pack_size=100)

    assert basis == "pack"
    assert round(value, 2) == 14.20


def test_pack_size_outranks_item_weight():
    """When both are known, trust the supplier's own packaging over our card.

    A price in kilograms whose text also states "уп. 100 шт" must use the pack:
    our nomenclature weight is known to be imperfect.
    """
    from app.services.comparison_service import normalize_price

    value, basis = normalize_price(
        1420.0, quote_unit="кг", position_unit="шт", weight_kg=0.085, pack_size=100,
    )

    assert basis == "pack"
    assert round(value, 2) == 14.20


def test_supplier_own_restatement_beats_our_weight():
    """When the quote itself says 15 кг = 1152 шт, use that, not our card.

    The supplier restates the amount per line, so the factor is exact for that
    line. Our nomenclature weight is an average and known to be imperfect.
    """
    from app.services.comparison_service import normalize_price

    value, basis = normalize_price(
        251.95, quote_unit="кг", position_unit="шт",
        qty=15, ref_qty=1152, ref_unit="шт",
        weight_kg=0.085,          # would give a very different answer
    )

    assert basis == "supplier_qty"
    assert round(value, 2) == 3.28


def test_restatement_in_a_different_unit_is_ignored():
    """A reference column in metres says nothing about a price per piece."""
    from app.services.comparison_service import normalize_price

    value, basis = normalize_price(
        251.95, quote_unit="кг", position_unit="шт",
        qty=15, ref_qty=1152, ref_unit="м",
    )

    assert basis == "none"
    assert value is None


def test_thousands_are_an_exact_conversion_not_an_estimate():
    """A position in тыс. шт against a price per шт is a plain ×1000.

    Nothing external is needed — no weight, no packaging — so this must not
    fall through to "не сравнимо" the way a genuinely unknown factor does.
    """
    from app.services.comparison_service import normalize_price

    value, basis = normalize_price(14.20, quote_unit="шт", position_unit="тыс. шт")

    assert basis == "scale"
    assert round(value, 2) == 14200.00


def test_thousands_convert_the_other_way_too():
    from app.services.comparison_service import normalize_price

    value, basis = normalize_price(14200.0, quote_unit="тыс. шт", position_unit="шт")

    assert basis == "scale"
    assert round(value, 2) == 14.20


def test_thousands_of_different_units_do_not_convert():
    """тыс. шт and кг still share nothing."""
    from app.services.comparison_service import normalize_price

    assert normalize_price(100.0, quote_unit="тыс. шт", position_unit="кг") == (None, "none")


def test_offered_quantity_understands_thousands():
    """2 тыс. шт offered against a request in штуки is 2000."""
    from app.services.comparison_service import offered_quantity

    assert offered_quantity(2, quote_unit="тыс. шт", position_unit="шт") == 2000
