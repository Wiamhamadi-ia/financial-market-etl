import pandas as pd
import pytest
from pathlib import Path

from src.utils import (
    ensure_directory,
    validate_dataframe_not_empty,
    validate_required_columns,
    convert_date_column,
    sort_by_date,
    handle_missing_values,
)


def test_ensure_directory(tmp_path):
    test_dir = tmp_path / "new_folder"
    ensure_directory(test_dir)
    assert test_dir.exists()
    assert test_dir.is_dir()


def test_validate_dataframe_not_empty_pass():
    df = pd.DataFrame({"A": [1, 2, 3]})
    validate_dataframe_not_empty(df, context="Test DF")  # should not raise


def test_validate_dataframe_not_empty_fail():
    df = pd.DataFrame()
    with pytest.raises(ValueError, match="is empty"):
        validate_dataframe_not_empty(df, context="Empty DF")


def test_validate_required_columns_pass():
    df = pd.DataFrame({"A": [1], "B": [2], "C": [3]})
    validate_required_columns(df, ["A", "B"], context="Test DF")  # should not raise


def test_validate_required_columns_fail():
    df = pd.DataFrame({"A": [1], "B": [2]})
    with pytest.raises(ValueError, match="missing required columns"):
        validate_required_columns(df, ["A", "B", "C"], context="Test DF")


def test_convert_date_column():
    df = pd.DataFrame({"Date": ["2024-01-01", "2024-01-02"]})
    result = convert_date_column(df, "Date")
    assert pd.api.types.is_datetime64_any_dtype(result["Date"])


def test_sort_by_date():
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"]),
        "Value": [3, 1, 2]
    })
    result = sort_by_date(df, "Date")
    assert list(result["Value"]) == [1, 2, 3]


def test_handle_missing_values():
    df = pd.DataFrame({
        "A": [1, None, 3],
        "B": [None, 2, None]
    })
    result = handle_missing_values(df)
    assert result.isnull().sum().sum() == 0