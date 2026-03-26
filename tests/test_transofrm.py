import pandas as pd
import numpy as np
import pytest

from src.transform import transform_market_data


def create_sample_market_data(n_rows=250):
    dates = pd.date_range(start="2024-01-01", periods=n_rows, freq="D")
    close_prices = np.linspace(100, 200, n_rows)

    df = pd.DataFrame({
        "Date": dates,
        "Open": close_prices - 1,
        "High": close_prices + 2,
        "Low": close_prices - 2,
        "Close": close_prices,
        "Volume": np.random.randint(1000000, 5000000, n_rows),
        "Ticker": ["AAPL"] * n_rows
    })
    return df


def test_transform_market_data_returns_dataframe():
    df = create_sample_market_data()
    result = transform_market_data(df)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_transform_market_data_creates_expected_columns():
    df = create_sample_market_data()
    result = transform_market_data(df)

    expected_columns = [
        "Daily Return",
        "Log Return",
        "SMA_20",
        "SMA_50",
        "SMA_200",
        "Rolling Volatility",
        "Annualized Volatility",
        "Cumulative Return",
        "Running Max",
        "Drawdown",
        "Max Drawdown",
    ]

    for col in expected_columns:
        assert col in result.columns


def test_transform_market_data_preserves_original_columns():
    df = create_sample_market_data()
    result = transform_market_data(df)

    original_columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]

    for col in original_columns:
        assert col in result.columns


def test_transform_market_data_empty_dataframe_raises_error():
    df = pd.DataFrame()

    with pytest.raises(ValueError, match="is empty"):
        transform_market_data(df)


def test_transform_market_data_missing_columns_raises_error():
    df = pd.DataFrame({
        "Date": pd.date_range(start="2024-01-01", periods=10),
        "Close": np.linspace(100, 110, 10),
    })

    with pytest.raises(ValueError, match="missing required columns"):
        transform_market_data(df)


def test_drawdown_is_zero_or_negative():
    df = create_sample_market_data()
    result = transform_market_data(df)

    # Drawdown should be <= 0 (or NaN at first rows if any)
    non_null_drawdown = result["Drawdown"].dropna()
    assert (non_null_drawdown <= 0).all()


def test_running_max_is_greater_or_equal_close():
    df = create_sample_market_data()
    result = transform_market_data(df)

    comparison = result["Running Max"] >= result["Close"]
    assert comparison.all()