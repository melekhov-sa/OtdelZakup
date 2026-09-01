"""Tests for locating quantity columns in a supplier price list.

Suppliers quote in one unit and often restate the amount in another —
"15 кг" alongside "1 152,00 шт (справочно)". The first quantity column is
the one the price belongs to; anything after it is the supplementary figure.
"""


def test_finds_main_and_reference_quantity_columns():
    from app.services.comparison_service import detect_quantity_columns

    headers = ["№", "Товары (работы, услуги)", "Кол-во", "Ед.",
               "Цена", "Кол-во шт. (справочно)", "Сумма"]

    cols = detect_quantity_columns(headers)

    assert cols.qty_idx == 2
    assert cols.unit_idx == 3
    assert cols.ref_qty_idx == 5
    assert cols.ref_unit == "шт"


def test_unit_may_live_inside_the_quantity_header():
    from app.services.comparison_service import detect_quantity_columns

    cols = detect_quantity_columns(["Наименование", "Кол-во, кг", "Цена"])

    assert cols.qty_idx == 1
    assert cols.unit_idx is None
    assert cols.unit == "кг"
    assert cols.ref_qty_idx is None


def test_price_list_without_quantity_columns():
    from app.services.comparison_service import detect_quantity_columns

    cols = detect_quantity_columns(["Наименование", "Цена"])

    assert cols.qty_idx is None
    assert cols.ref_qty_idx is None


def test_unit_column_before_the_quantity_is_still_found():
    """Some suppliers put "Ед." to the left of "Кол-во".

    The main unit is the one standing next to the main amount, on whichever
    side. Missing it leaves the price without a unit while the supplementary
    column still has one — and шт would silently stand in for кг.
    """
    from app.services.comparison_service import detect_quantity_columns

    cols = detect_quantity_columns(
        ["№", "Наименование", "Ед.", "Кол-во", "Цена", "Кол-во шт", "Сумма"]
    )

    assert cols.qty_idx == 3
    assert cols.unit_idx == 2
    assert cols.ref_qty_idx == 5
    assert cols.ref_unit == "шт"


def test_the_column_after_still_wins_when_both_sides_have_one():
    """The usual layout keeps its meaning: amount then unit."""
    from app.services.comparison_service import detect_quantity_columns

    cols = detect_quantity_columns(["Ед. изм", "Кол-во", "Ед.", "Цена"])

    assert cols.qty_idx == 1
    assert cols.unit_idx == 2
