"""Units written out in full must be understood, and answered in short form.

"кг" was recognised, "килограмм" was not — the same amount vanished depending
on how the customer chose to write it. The reply is always the short canonical
form, so 1C looks the packaging up by one predictable string.
"""

import pytest

from app.text_input.parser import parse_text_line


@pytest.mark.parametrize("line, unit", [
    ("Болт М12х80  50 килограмм", "кг"),
    ("Болт М12х80  50 килограммов", "кг"),
    ("Болт М12х80  2,5 килограмма", "кг"),
    ("Болт М12х80  10 метров", "м"),
    ("Болт М12х80  10 метра", "м"),
    ("Болт М12х80  3 тонны", "т"),
    ("Болт М12х80  5 литров", "л"),
    ("Болт М12х80  4 упаковки", "уп"),
    ("Болт М12х80  100 штук", "шт"),
])
def test_spelled_out_units_are_read_and_shortened(line, unit):
    result = parse_text_line(line)

    assert result["unit"] == unit
    assert result["qty"] is not None


def test_short_forms_keep_working():
    for line, unit in [("Болт М12х80  15кг", "кг"), ("Болт М12х80  20 шт", "шт"),
                       ("Болт М12х80  10 м", "м"), ("Болт М12х80  5 т", "т")]:
        assert parse_text_line(line)["unit"] == unit


def test_thread_size_is_not_a_unit():
    """"М10" is a thread, not 10 metres — the old guard must hold."""
    assert parse_text_line("Анкер М10")["qty"] is None
