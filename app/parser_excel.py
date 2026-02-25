import pandas as pd
from pathlib import Path


def load_excel(file_path: str | Path) -> pd.DataFrame:
    """Read an .xlsx file and return a DataFrame."""
    return pd.read_excel(file_path, engine="openpyxl")


def dataframe_preview(df: pd.DataFrame, limit: int = 200) -> pd.DataFrame:
    """Return the first `limit` rows of the DataFrame."""
    return df.head(limit)


def dataframe_to_html(df: pd.DataFrame) -> str:
    """Convert DataFrame to an HTML table string."""
    return df.to_html(
        index=False,
        classes="table",
        border=0,
        na_rep="",
    )
