"""Tests for flagging converted prices that cannot be right.

Our nomenclature weight is known to be imperfect, so a conversion based on it
can produce a plausible-looking but wildly wrong number. Such cells are flagged
rather than hidden — МЗ decides, and a bad weight in the catalog gets noticed.
"""


def test_price_far_from_the_others_is_flagged():
    from app.services.comparison_service import mark_suspicious

    cells = {
        "Ромашка": {"price_normalized": 14.20},
        "Василёк": {"price_normalized": 15.00},
        "Одуванчик": {"price_normalized": 1400.00},
    }

    result = mark_suspicious(cells)

    assert result["Одуванчик"]["suspicious"] is True
    assert result["Ромашка"]["suspicious"] is False
    assert result["Василёк"]["suspicious"] is False
