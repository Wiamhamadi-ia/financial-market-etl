import yfinance as yf
import pandas as pd
import os
from datetime import datetime
import logging


def extract_stock_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extract historical stock data from Yahoo Finance.
    """

    logging.info(f"Starting data extraction for {ticker}")

    try:
        data = yf.download(ticker, start=start_date, end=end_date)

        if data.empty:
            raise ValueError("No data returned from API.")

        logging.info(f"Extraction successful for {ticker}")
        return data

    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise


def save_raw_data(data: pd.DataFrame, ticker: str):
    """
    Save raw data to CSV file in data/raw directory.
    """

    os.makedirs("data/raw", exist_ok=True)

    today = datetime.today().strftime("%Y-%m-%d")
    filename = f"data/raw/{ticker}_{today}.csv"

    data.to_csv(filename)

    logging.info(f"Raw data saved to {filename}")