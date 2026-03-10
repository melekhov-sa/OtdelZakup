"""Classify QuoteLine rows: item vs header/total/requisites/garbage.

is_non_item_row() filters out rows that are clearly NOT product positions:
 - totals: "Итого...", "Всего..."
 - headers: "Наименование | Кол-во | Цена | Сумма"
 - requisites: "ИНН", "КПП", "р/с", "БИК"
 - too short without item indicators

Returns a classification with a reason string for UI display.
"""
from __future__ import annotations

import re

# ── Keywords: totals, requisites, headers ───────────────────────────────────

_TOTAL_KEYWORDS = {
    "итого", "итог", "всего", "к оплате", "оплата",
    "задолженность", "баланс", "текущая", "просроченная",
    "погашение", "долг клиента",
}

_REQUISITES_KEYWORDS = {
    "инн", "кпп", "банк", "р/с", "корр", "бик",
    "поставщик", "покупатель", "адрес", "тел",
    "email", "дог.", "договор", "основание",
    "грузоотправитель", "грузополучатель", "плательщик",
    "расчетный счет", "корреспондентский",
}

_TAX_KEYWORDS = {"налог", "ндс", "без ндс", "в т.ч. ндс", "в том числе ндс", "nds"}

_HEADER_KEYWORDS = {
    "наименование", "название", "позиция", "номенклатура",
    "кол-во", "количество", "ед.", "единица",
    "цена", "стоимость", "сумма",
    "артикул", "код", "номер",
}

_DATE_KEYWORDS = {"дата", "счет", "счёт", "от №", "счет-фактура", "накладная"}

# ── Product indicators: if present, row is likely an item ───────────────────

# Sizes: M12, M16x60, 125x1.6x22, d10, 0.5x12, 10мм etc
_SIZE_RE = re.compile(
    r"(?:^|\s|/)"
    r"(?:"
    r"[Mm][Мм]?\s*\d+"          # M12, М16
    r"|\d+(?:[.,]\d+)?[xх×]\d+"  # 12x60, 125x1.6x22
    r"|[dDдД]\s*\d+"             # d10
    r"|\d+\s*мм"                 # 10мм
    r")",
    re.IGNORECASE,
)

# Standards: DIN, ГОСТ, GOST, ISO, EN, ТУ
_STD_RE = re.compile(
    r"\b(?:DIN|ГОСТ\s*Р?\s*(?:ИСО)?|GOST|ISO|EN\s*\d|ТУ)\s*[\-\s]?\d",
    re.IGNORECASE,
)

# Product type words (fasteners, construction, etc.)
_ITEM_TYPE_WORDS = {
    "болт", "гайка", "шайба", "винт", "саморез", "шуруп", "шпилька",
    "анкер", "дюбель", "заклепка", "гвоздь", "скоба",
    "диск", "круг", "лента", "пена", "герметик", "пистолет",
    "ключ", "труба", "фланец", "муфта", "хомут", "кабель", "провод",
    "электрод", "сверло", "бур", "коронка", "патрон",
    "пластина", "ролик", "втулка", "кольцо", "ось",
    "washer", "bolt", "nut", "screw", "anchor", "rivet", "nail",
}

_ITEM_TYPE_RE = re.compile(
    r"\b(" + "|".join(re.escape(w) for w in sorted(_ITEM_TYPE_WORDS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)


# ── Public API ──────────────────────────────────────────────────────────────


def has_item_indicators(text: str) -> bool:
    """Return True if text contains product indicators (size, standard, item type)."""
    if _SIZE_RE.search(text):
        return True
    if _STD_RE.search(text):
        return True
    if _ITEM_TYPE_RE.search(text):
        return True
    return False


def classify_quote_line(raw_text: str) -> tuple[str, str]:
    """Classify a raw text line from a supplier quote.

    Returns (line_class, reason):
        line_class: "item" | "header" | "total" | "requisites" | "garbage"
        reason: human-readable explanation (Russian)
    """
    text = raw_text.strip()
    if not text:
        return "garbage", "Пустая строка"

    text_lower = text.lower()

    # Short lines without item indicators → garbage
    word_count = len(text.split())
    if word_count <= 1 and not has_item_indicators(text):
        return "garbage", "Слишком короткая строка"

    has_item = has_item_indicators(text)

    # Check totals
    for kw in _TOTAL_KEYWORDS:
        if kw in text_lower and not has_item:
            return "total", f"Итоговая строка ('{kw}')"

    # Check header-like rows: multiple header keywords
    header_hits = sum(1 for kw in _HEADER_KEYWORDS if kw in text_lower)
    if header_hits >= 3 and not has_item:
        return "header", f"Заголовок таблицы ({header_hits} ключ. слов)"

    # Check requisites
    for kw in _REQUISITES_KEYWORDS:
        if kw in text_lower and not has_item:
            return "requisites", f"Реквизиты ('{kw}')"

    # Check date/invoice headers
    for kw in _DATE_KEYWORDS:
        if kw in text_lower and not has_item:
            return "requisites", f"Реквизиты документа ('{kw}')"

    # Check tax lines
    for kw in _TAX_KEYWORDS:
        if kw in text_lower and not has_item:
            return "total", f"Налоговая строка ('{kw}')"

    # If very few words and no item indicators, suspicious
    if word_count <= 2 and not has_item:
        # Could be a short label like "Примечание" — check if all words are stopwords
        return "garbage", "Короткая строка без признаков товара"

    return "item", ""


def is_non_item_row(text: str) -> bool:
    """Return True if the row is NOT a product item (header/total/requisites/garbage)."""
    cls, _ = classify_quote_line(text)
    return cls != "item"
