import pandas as pd
import pytest
from unittest.mock import patch

from src.extract import extract_market_data


def create_mock_yfinance_data():
    dates = pd.date_range(start="2024-01-01", periods=5, freq="D")
    df = pd.DataFrame({
        "Open": [100, 101, 102, 103, 104],
        "High": [101, 102, 103, 104, 105],
        "Low": [99, 100, 101, 102, 103],
        "Close": [100.5, 101.5, 102.5, 103.5, 104.5],
        "Adj Close": [100.4, 101.4, 102.4, 103.4, 104.4],
        "Volume": [1000000, 1100000, 1200000, 1300000, 1400000],
    }, index=dates)
    df.index.name = "Date"
    return df


@patch("src.extract.yf.download")
def test_extract_market_data_success(mock_download):
    mock_download.return_value = create_mock_yfinance_data()

    result = extract_market_data("AAPL", "2024-01-01", "2024-01-10")

    assert isinstance(result, pd.DataFrame)
    assert not result.empty

    expected_columns = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Ticker"]
    for col in expected_columns:
        assert col in result.columns

    assert (result["Ticker"] == "AAPL").all()


@patch("src.extract.yf.download")
def test_extract_market_data_empty_raises_error(mock_download):
    mock_download.return_value = pd.DataFrame()

    with pytest.raises(ValueError, match="is empty"):
        extract_market_data("AAPL", "2024-01-01", "2024-01-10")


@patch("src.extract.yf.download")
def test_extract_market_data_calls_yfinance_with_correct_arguments(mock_download):
    mock_download.return_value = create_mock_yfinance_data()

    extract_market_data("MSFT", "2023-01-01", "2023-12-31")

    mock_download.assert_called_once_with(
        "MSFT",
        start="2023-01-01",
        end="2023-12-31",
        auto_adjust=False,
        progress=False
    )