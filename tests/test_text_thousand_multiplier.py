"""«2,5 тыс. шт» must be read as a quantity, not thrown away.

The multiplier was already understood when a quantity came out of a PDF
table, but the text and file entry points did not know it — a заявка saying
"тыс. шт" arrived with no quantity at all, not merely with a wrong one.
"""

import pytest

from app.text_input.parser import parse_text_line


@pytest.mark.parametrize("line, qty", [
    ("Болт М12х80 ГОСТ 7798-70  2,5 тыс. шт", 2500),
    ("Болт М12х80 ГОСТ 7798-70  2,5 тыс шт", 2500),
    ("Болт М12х80 ГОСТ 7798-70  2.5 тыс.шт", 2500),
    ("Болт М12х80 ГОСТ 7798-70  10 тыс. шт", 10000),
])
def test_thousands_are_expanded_to_pieces(line, qty):
    result = parse_text_line(line)

    assert result["qty"] == qty
    assert result["unit"] == "шт"
    assert "тыс" not in result["name"]


def test_plain_quantities_are_untouched():
    """The existing readings must not shift."""
    assert parse_text_line("Болт М12х80  2500 шт")["qty"] == 2500
    assert parse_text_line("Болт М12х80  15кг")["qty"] == 15
    assert parse_text_line("Болт М12х80  15кг")["unit"] == "кг"


def test_a_name_containing_thousands_without_a_unit_is_left_alone():
    """"тыс" on its own is not a quantity — do not invent one."""
    result = parse_text_line("Болт М12х80 партия 5 тыс")

    assert result["qty"] is None
