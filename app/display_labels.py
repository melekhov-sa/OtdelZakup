"""Human-readable Russian labels for DataFrame column names."""

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
}


def display_label(col: str) -> str:
    """Return Russian display label for a column, or the column name itself."""
    return COLUMN_LABELS.get(col, col)
