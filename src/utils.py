from pathlib import Path
import pandas as pd


def ensure_directory(path: Path) -> None:
    """
    Create a directory if it does not exist.
    """
    path.mkdir(parents=True, exist_ok=True)


def validate_dataframe_not_empty(df: pd.DataFrame, context: str = "DataFrame") -> None:
    """
    Raise an error if the DataFrame is empty.
    """
    if df is None or df.empty:
        raise ValueError(f"{context} is empty.")


def validate_required_columns(df: pd.DataFrame, required_columns: list, context: str = "DataFrame") -> None:
    """
    Raise an error if required columns are missing.
    """
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"{context} is missing required columns: {missing_cols}")


def convert_date_column(df: pd.DataFrame, column_name: str = "Date") -> pd.DataFrame:
    """
    Convert a date column to datetime.
    """
    if column_name in df.columns:
        df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    return df


def sort_by_date(df: pd.DataFrame, column_name: str = "Date") -> pd.DataFrame:
    """
    Sort DataFrame by date column.
    """
    if column_name in df.columns:
        df = df.sort_values(by=column_name).reset_index(drop=True)
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic missing value handling:
    - forward fill
    - backward fill
    """
    df = df.ffill().bfill()
    return df