"""«2,5 тыс. шт» keeps the unit the customer asked in.

The customer requests in thousands and must be invoiced in thousands, so the
multiplier is part of the unit, not something to be flattened into pieces:
2,5 тыс. шт stays 2,5 with the unit "тыс. шт". Expanding it to 2500 шт would
answer a question nobody asked and lose the wording the invoice needs.
"""

import pytest

from app.text_input.parser import parse_text_line


@pytest.mark.parametrize("line", [
    "Болт М12х80 ГОСТ 7798-70  2,5 тыс. шт",
    "Болт М12х80 ГОСТ 7798-70  2,5 тыс шт",
    "Болт М12х80 ГОСТ 7798-70  2.5 тыс.шт",
])
def test_thousands_keep_quantity_and_unit_as_asked(line):
    result = parse_text_line(line)

    assert result["qty"] == 2.5
    assert result["unit"] == "тыс. шт"
    assert "тыс" not in result["name"]


def test_whole_thousands_stay_whole():
    result = parse_text_line("Болт М12х80 ГОСТ 7798-70  10 тыс. шт")

    assert result["qty"] == 10
    assert result["unit"] == "тыс. шт"


def test_the_unit_is_spelled_the_same_however_it_was_written():
    """1C looks the packaging up by this string, so it must be predictable."""
    spellings = ["3 тыс. шт", "3 тыс шт", "3 тыс.шт", "3 ТЫС. ШТ"]
    units = {parse_text_line("Болт М12х80  " + s)["unit"] for s in spellings}

    assert units == {"тыс. шт"}


def test_plain_quantities_are_untouched():
    assert parse_text_line("Болт М12х80  2500 шт")["qty"] == 2500
    assert parse_text_line("Болт М12х80  2500 шт")["unit"] == "шт"
    assert parse_text_line("Болт М12х80  15кг")["qty"] == 15
    assert parse_text_line("Болт М12х80  15кг")["unit"] == "кг"


def test_a_name_containing_thousands_without_a_unit_is_left_alone():
    """"тыс" on its own is not a quantity — do not invent one."""
    assert parse_text_line("Болт М12х80 партия 5 тыс")["qty"] is None


@pytest.mark.parametrize("qty_cell, uom_cell, text_line", [
    ("2,5", "тыс. шт", "Болт М12х80  2,5 тыс. шт"),
    ("10", "тыс. кг", "Болт М12х80  10 тыс. кг"),
])
def test_pdf_table_and_text_read_thousands_the_same(qty_cell, uom_cell, text_line):
    """One заявка must not change meaning by arriving as a scan.

    The PDF branch used to expand the multiplier into base units while the
    text branch kept it, so the same order read as 2500 шт or as 2,5 тыс. шт
    depending on the file format it came in.
    """
    from app.parsing.docai_table_parser import parse_qty_uom

    pdf_qty, pdf_uom, _ = parse_qty_uom(qty_cell, uom_cell)
    text = parse_text_line(text_line)

    assert (pdf_qty, pdf_uom) == (text["qty"], text["unit"])
