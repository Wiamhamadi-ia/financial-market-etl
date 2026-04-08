import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from src.logger import setup_logger
from src.config import FIGURES_DIR
from src.utils import validate_dataframe_not_empty, validate_required_columns

logger = setup_logger()


def load_processed_data(file_path: str | Path) -> pd.DataFrame:
    """
    Load processed CSV file into a DataFrame.
    """
    try:
        df = pd.read_csv(file_path)

        validate_dataframe_not_empty(df, "Processed data")

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])

        logger.info(f"Processed data loaded successfully from {file_path}")
        return df

    except Exception as e:
        logger.error(f"Failed to load processed data from {file_path}: {e}")
        raise


def _prepare_output_dir():
    """
    Ensure figures directory exists.
    """
    os.makedirs(FIGURES_DIR, exist_ok=True)


def _save_plot(output_path: Path):
    """
    Save and close current matplotlib figure.
    """
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(f"Chart saved to {output_path}")


def plot_close_price(df: pd.DataFrame, ticker: str):
    """
    Plot closing price over time.
    """
    validate_required_columns(df, ["Date", "Close"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["Date"], df["Close"])
    plt.title(f"{ticker} - Closing Price")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.grid(True)

    output_path = FIGURES_DIR / f"{ticker}_close_price.png"
    _save_plot(output_path)


def plot_returns(df: pd.DataFrame, ticker: str):
    """
    Plot daily returns over time.
    """
    validate_required_columns(df, ["Date", "Daily Return"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["Date"], df["Daily Return"])
    plt.title(f"{ticker} - Daily Returns")
    plt.xlabel("Date")
    plt.ylabel("Daily Return")
    plt.grid(True)

    output_path = FIGURES_DIR / f"{ticker}_daily_returns.png"
    _save_plot(output_path)


def plot_moving_averages(df: pd.DataFrame, ticker: str):
    """
    Plot close price with moving averages.
    """
    validate_required_columns(df, ["Date", "Close", "SMA_20", "SMA_50", "SMA_200"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["Date"], df["Close"], label="Close")
    plt.plot(df["Date"], df["SMA_20"], label="SMA 20")
    plt.plot(df["Date"], df["SMA_50"], label="SMA 50")
    plt.plot(df["Date"], df["SMA_200"], label="SMA 200")

    plt.title(f"{ticker} - Close Price & Moving Averages")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)

    output_path = FIGURES_DIR / f"{ticker}_moving_averages.png"
    _save_plot(output_path)


def plot_volatility(df: pd.DataFrame, ticker: str):
    """
    Plot rolling volatility over time.
    """
    validate_required_columns(df, ["Date", "Rolling Volatility"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["Date"], df["Rolling Volatility"])
    plt.title(f"{ticker} - 20-Day Rolling Volatility")
    plt.xlabel("Date")
    plt.ylabel("Volatility")
    plt.grid(True)

    output_path = FIGURES_DIR / f"{ticker}_Rolling_Volatility.png"
    _save_plot(output_path)




def generate_all_charts_for_ticker(file_path: str | Path, ticker: str):
    """
    Generate all charts for one processed ticker file.
    """
    logger.info(f"Generating charts for {ticker}")

    df = load_processed_data(file_path)

    plot_close_price(df, ticker)
    plot_returns(df, ticker)
    plot_moving_averages(df, ticker)
    plot_volatility(df, ticker)
   

    logger.info(f"All charts generated successfully for {ticker}")