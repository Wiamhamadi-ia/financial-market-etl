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

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

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
    validate_required_columns(df, ["date", "close"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["close"])
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
    validate_required_columns(df, ["date", "daily_return"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["daily_return"])
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
    validate_required_columns(df, ["date", "close", "sma_10", "sma_20", "sma_50"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["close"], label="Close")
    plt.plot(df["date"], df["sma_10"], label="SMA 10")
    plt.plot(df["date"], df["sma_20"], label="SMA 20")
    plt.plot(df["date"], df["sma_50"], label="SMA 50")

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
    validate_required_columns(df, ["date", "volatility_20"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["volatility_20"])
    plt.title(f"{ticker} - 20-Day Rolling Volatility")
    plt.xlabel("Date")
    plt.ylabel("Volatility")
    plt.grid(True)

    output_path = FIGURES_DIR / f"{ticker}_volatility_20.png"
    _save_plot(output_path)


def plot_rsi(df: pd.DataFrame, ticker: str):
    """
    Plot RSI indicator over time.
    """
    validate_required_columns(df, ["date", "rsi_14"])

    _prepare_output_dir()

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["rsi_14"], label="RSI 14")
    plt.axhline(70, linestyle="--", label="Overbought (70)")
    plt.axhline(30, linestyle="--", label="Oversold (30)")

    plt.title(f"{ticker} - RSI (14)")
    plt.xlabel("Date")
    plt.ylabel("RSI")
    plt.legend()
    plt.grid(True)

    output_path = FIGURES_DIR / f"{ticker}_rsi_14.png"
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
    plot_rsi(df, ticker)

    logger.info(f"All charts generated successfully for {ticker}")