import numpy as np
import pandas as pd

from src.logger import setup_logger
from src.config import SMA_WINDOWS, VOLATILITY_WINDOW, TRADING_DAYS_PER_YEAR
from src.utils import (
    validate_dataframe_not_empty,
    validate_required_columns,
    convert_date_column,
    sort_by_date,
    handle_missing_values
)

logger = setup_logger()


def transform_market_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform market data by computing key financial indicators.
    """
    logger.info("Starting data transformation...")

    try:
        validate_dataframe_not_empty(df, context="Input market data")

        required_columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]
        validate_required_columns(df, required_columns, context="Market data")

        df = df.copy()

        # Standardize and sort
        df = convert_date_column(df, "Date")
        df = sort_by_date(df, "Date")
        df = handle_missing_values(df)

        # Daily return
        df["Daily Return"] = df["Close"].pct_change()

        # Log return
        df["Log Return"] = np.log(df["Close"] / df["Close"].shift(1))

        # Simple Moving Averages
        for window in SMA_WINDOWS:
            df[f"SMA_{window}"] = df["Close"].rolling(window=window).mean()

        # Rolling volatility
        df["Rolling Volatility"] = df["Daily Return"].rolling(window=VOLATILITY_WINDOW).std()

        # Annualized volatility
        df["Annualized Volatility"] = df["Rolling Volatility"] * np.sqrt(TRADING_DAYS_PER_YEAR)

        # Cumulative return
        df["Cumulative Return"] = (1 + df["Daily Return"]).cumprod() - 1

        # Drawdown
        df["Running Max"] = df["Close"].cummax()
        df["Drawdown"] = (df["Close"] - df["Running Max"]) / df["Running Max"]

        # Maximum drawdown (expanding minimum drawdown)
        df["Max Drawdown"] = df["Drawdown"].cummin()

        logger.info("Transformation completed successfully.")
        return df

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise