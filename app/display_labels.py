"""Human-readable Russian labels for DataFrame column names."""

import math
from typing import Optional

COLUMN_LABELS: dict[str, str] = {
    "code": "Код",
    "name": "Наименование",
    "qty": "Количество",
    "uom": "Ед.",
    "item_type": "Тип изделия",
    "diameter": "Диаметр",
    "length": "Длина",
    "size": "Размер",
    "strength": "Класс прочности",
    "coating": "Покрытие",
    "gost": "ГОСТ",
    "iso": "ISO",
    "din": "DIN",
    "status": "Статус",
    "confidence": "Уверенность",
    "standard_raw": "Стандарт (из файла)",
    "strength_raw": "Класс пр. (из файла)",
    "note_raw": "Примечание (из файла)",
    "reason": "Причина",
    "item_type_source": "Источник типа",
}


def display_label(col: str) -> str:
    """Return Russian display label for a column, or the column name itself."""
    return COLUMN_LABELS.get(col, col)


def format_qty(value) -> str:
    """Format a quantity value for human display.

    Rules:
      - None / NaN                    → "" (empty)
      - Whole number (64.0, 1000)     → "64", "1000"  (no decimal point)
      - Fractional (2.5, 1.250)       → "2.5" (trailing zeros stripped, ≤ 3 places)
    """
    if value is None:
        return ""
    try:
        if isinstance(value, float) and math.isnan(value):
            return ""
        f = float(value)
        if f == int(f):
            return str(int(f))
        return f"{f:.3f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(value) if value else ""
