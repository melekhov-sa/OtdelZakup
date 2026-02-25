"""Normalized name builder.

Provides build_normalized_name() (pure function on row_dict + template_string)
and apply_normalized_names() (DataFrame-level helper).
"""

import re

import pandas as pd

# Variables supported in template strings
TEMPLATE_VARS = ["item_type", "size", "strength", "standard", "coating", "qty", "code", "name"]

TEMPLATE_VAR_HINTS = {
    "item_type": "тип изделия (болт, гайка…)",
    "size": "размер (M12x80)",
    "strength": "класс прочности (8.8)",
    "standard": "стандарт из ГОСТ / ISO / DIN",
    "coating": "покрытие (цинк, нержавейка…)",
    "qty": "количество",
    "code": "код позиции",
    "name": "исходное наименование",
}


def build_normalized_name(row_dict: dict, template_string: str) -> str:
    """Build a normalized name string from row_dict values and a template.

    Supports variables: {item_type}, {size}, {strength}, {standard},
    {coating}, {qty}, {code}, {name}.

    {standard} is resolved from gost → iso → din (first non-empty).
    Empty values are removed and extra whitespace is collapsed.
    """
    standard = (
        str(row_dict.get("gost") or "")
        or str(row_dict.get("iso") or "")
        or str(row_dict.get("din") or "")
    )

    values = {
        "item_type": str(row_dict.get("item_type") or ""),
        "size": str(row_dict.get("size") or ""),
        "strength": str(row_dict.get("strength") or ""),
        "standard": standard,
        "coating": str(row_dict.get("coating") or ""),
        "qty": str(row_dict.get("qty") or ""),
        "code": str(row_dict.get("code") or ""),
        "name": str(row_dict.get("name") or ""),
    }

    result = template_string
    for key, val in values.items():
        result = result.replace(f"{{{key}}}", val)

    # Collapse multiple spaces and trim
    result = re.sub(r"\s+", " ", result).strip()
    return result


def apply_normalized_names(df_original: pd.DataFrame, df_transformed: pd.DataFrame, template_string: str) -> pd.DataFrame:
    """Add 'Нормализованное наименование' column to df_transformed."""
    # Import here to avoid circular imports
    from app.readiness import _build_row_dict, _concat_row  # noqa: PLC0415

    names = []
    for idx in df_transformed.index:
        original_row = df_original.loc[idx] if idx in df_original.index else pd.Series()
        text = _concat_row(original_row) if len(original_row) > 0 else ""
        transformed_row = df_transformed.loc[idx]
        row_dict = _build_row_dict(text, transformed_row, original_row)
        names.append(build_normalized_name(row_dict, template_string))

    df_transformed["Нормализованное наименование"] = names
    return df_transformed


def load_active_template():
    """Return the active NameTemplate with lowest priority, or None."""
    from app.database import get_db_session  # noqa: PLC0415
    from app.models import NameTemplate  # noqa: PLC0415

    session = get_db_session()
    try:
        return (
            session.query(NameTemplate)
            .filter(NameTemplate.is_active.is_(True))
            .order_by(NameTemplate.priority.asc())
            .first()
        )
    finally:
        session.close()
