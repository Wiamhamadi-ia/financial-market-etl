import pandas as pd
import numpy as np
import logging

from src.config import SMA_WINDOWS, VOLATILITY_WINDOW, TRADING_DAYS_PER_YEAR


def transform_stock_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich stock data with quantitative indicators.
    """

    logging.info("Starting quantitative transformation")

    try:
        df = data.copy()

        # Reset index to convert Date from index to column
        df.reset_index(inplace=True)

        # Handle MultiIndex columns from yfinance
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        # Standardize column names
        df.columns = [col.lower() for col in df.columns]

        # Remove missing values
        df.dropna(inplace=True)

        # =========================
        # RETURNS
        # =========================
        df["daily_return"] = df["close"].pct_change()
        df["log_return"] = np.log(df["close"] / df["close"].shift(1))

        # =========================
        # MOVING AVERAGES (DYNAMIC)
        # =========================
        for window in SMA_WINDOWS:
            df[f"sma_{window}"] = df["close"].rolling(window=window).mean()

        # =========================
        # VOLATILITY (DYNAMIC)
        # =========================
        df[f"volatility_{VOLATILITY_WINDOW}"] = (
            df["daily_return"].rolling(VOLATILITY_WINDOW).std()
        )

        df["volatility_annual"] = (
            df[f"volatility_{VOLATILITY_WINDOW}"] * np.sqrt(TRADING_DAYS_PER_YEAR)
        )

        # =========================
        # DRAWDOWN
        # =========================
        df["cumulative_return"] = (1 + df["daily_return"]).cumprod()
        df["rolling_max"] = df["cumulative_return"].cummax()
        df["drawdown"] = (
            df["cumulative_return"] - df["rolling_max"]
        ) / df["rolling_max"]

        df["max_drawdown"] = df["drawdown"].cummin()

        logging.info("Quantitative transformation completed successfully")

        return df

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise