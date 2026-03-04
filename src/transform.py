import pandas as pd
import numpy as np
import logging


def transform_stock_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich stock data with quantitative indicators.
    """

    logging.info("Starting quantitative transformation")

    try:
        df = data.copy()

        # Reset index
        df.reset_index(inplace=True)

        # Handle MultiIndex columns (from yfinance)
        if isinstance(df.columns, pd.MultiIndex):
              df.columns = [col[0] for col in df.columns]

        df.columns = [col.lower() for col in df.columns]

        # Remove missing values
        df.dropna(inplace=True)

        # =========================
        # RETURNS
        # =========================

        df["daily_return"] = df["close"].pct_change()
        df["log_return"] = np.log(df["close"] / df["close"].shift(1))

        # =========================
        # MOVING AVERAGES
        # =========================

        df["sma_20"] = df["close"].rolling(20).mean()
        df["sma_50"] = df["close"].rolling(50).mean()
        df["sma_200"] = df["close"].rolling(200).mean()

        # =========================
        # VOLATILITY
        # =========================

        df["volatility_20"] = df["daily_return"].rolling(20).std()

        # Annualized volatility (252 trading days)
        df["volatility_annual"] = df["volatility_20"] * np.sqrt(252)

        # =========================
        # DRAWDOWN
        # =========================

        df["cumulative_return"] = (1 + df["daily_return"]).cumprod()
        df["rolling_max"] = df["cumulative_return"].cummax()
        df["drawdown"] = (
            df["cumulative_return"] - df["rolling_max"]
        ) / df["rolling_max"]

        df["max_drawdown"] = df["drawdown"].cummin()

        logging.info("Quantitative transformation completed")

        return df

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise