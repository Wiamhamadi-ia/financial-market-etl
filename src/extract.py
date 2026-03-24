import pandas as pd
import yfinance as yf

from src.logger import setup_logger
from src.utils import validate_dataframe_not_empty

logger = setup_logger()


def extract_market_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extract historical market data for a given ticker from Yahoo Finance.
    """
    logger.info(f"Starting data extraction for ticker={ticker} from {start_date} to {end_date}")

    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False
        )

        validate_dataframe_not_empty(df, context=f"Extracted data for {ticker}")

        # Reset index to make Date a normal column
        df = df.reset_index()

        # Flatten MultiIndex columns if they exist
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(level) for level in col if str(level) != ""]).strip("_")
                for col in df.columns.values
            ]

        # Standardize columns for single ticker case
        rename_map = {}
        for col in df.columns:
            if col.startswith("Date"):
                rename_map[col] = "Date"
            elif col.startswith("Open"):
                rename_map[col] = "Open"
            elif col.startswith("High"):
                rename_map[col] = "High"
            elif col.startswith("Low"):
                rename_map[col] = "Low"
            elif col.startswith("Close"):
                rename_map[col] = "Close"
            elif col.startswith("Adj Close"):
                rename_map[col] = "Adj Close"
            elif col.startswith("Volume"):
                rename_map[col] = "Volume"

        df = df.rename(columns=rename_map)

        # Add ticker column
        df["Ticker"] = ticker

        logger.info(f"Extraction successful for {ticker}: {len(df)} rows retrieved.")
        return df

    except Exception as e:
        logger.error(f"Error extracting data for {ticker}: {e}")
        raise