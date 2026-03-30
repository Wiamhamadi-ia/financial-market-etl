import os
from pathlib import Path
from typing import List, Optional

import pandas as pd

from src.logger import setup_logger
from src.utils import (
    validate_dataframe_not_empty,
    validate_required_columns,
    convert_date_column,
    sort_by_date,
)

logger = setup_logger()

REQUIRED_COLUMNS = [
    "Date",
    "Ticker",
    "Close",
    "Daily_Return",
    "Volatility",
    "RSI",
]


def ensure_directory_exists(directory: Path) -> None:
    """
    Create directory if it does not exist.
    """
    directory.mkdir(parents=True, exist_ok=True)


def prepare_dataframe_for_reporting(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and prepare dataframe for reporting.
    """
    validate_dataframe_not_empty(df, "Reporting dataframe")
    validate_required_columns(df, REQUIRED_COLUMNS)

    df = df.copy()
    df = convert_date_column(df, "Date")
    df = sort_by_date(df, "Date")

    return df


def generate_ticker_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate summary metrics per ticker.

    Returns a dataframe with one row per ticker.
    """
    df = prepare_dataframe_for_reporting(df)

    summary_rows = []

    for ticker in df["Ticker"].dropna().unique():
        ticker_df = df[df["Ticker"] == ticker].copy()

        if ticker_df.empty:
            logger.warning(f"No data available for ticker {ticker}. Skipping.")
            continue

        ticker_df = ticker_df.sort_values("Date").reset_index(drop=True)

        first_row = ticker_df.iloc[0]
        last_row = ticker_df.iloc[-1]

        summary = {
            "Ticker": ticker,
            "Start_Date": ticker_df["Date"].min(),
            "End_Date": ticker_df["Date"].max(),
            "Observations": len(ticker_df),

            # Price metrics
            "First_Close": ticker_df["Close"].iloc[0],
            "Last_Close": ticker_df["Close"].iloc[-1],
            "Min_Close": ticker_df["Close"].min(),
            "Max_Close": ticker_df["Close"].max(),
            "Mean_Close": ticker_df["Close"].mean(),
            "Median_Close": ticker_df["Close"].median(),
            "Std_Close": ticker_df["Close"].std(),

            # Return metrics
            "Mean_Daily_Return": ticker_df["Daily_Return"].mean(),
            "Median_Daily_Return": ticker_df["Daily_Return"].median(),
            "Std_Daily_Return": ticker_df["Daily_Return"].std(),
            "Min_Daily_Return": ticker_df["Daily_Return"].min(),
            "Max_Daily_Return": ticker_df["Daily_Return"].max(),

            # Volatility metrics
            "Mean_Volatility": ticker_df["Volatility"].mean(),
            "Max_Volatility": ticker_df["Volatility"].max(),
            "Latest_Volatility": last_row["Volatility"],

            # RSI metrics
            "Mean_RSI": ticker_df["RSI"].mean(),
            "Min_RSI": ticker_df["RSI"].min(),
            "Max_RSI": ticker_df["RSI"].max(),
            "Latest_RSI": last_row["RSI"],

            # Trend / performance
            "Absolute_Price_Change": last_row["Close"] - first_row["Close"],
            "Pct_Price_Change": ((last_row["Close"] / first_row["Close"]) - 1)
            if first_row["Close"] != 0 else None,
        }

        # Optional moving averages if available
        for sma_col in ["SMA_10", "SMA_20", "SMA_50"]:
            if sma_col in ticker_df.columns:
                summary[f"Latest_{sma_col}"] = last_row[sma_col]

        # Optional signals
        if "SMA_10" in ticker_df.columns and "SMA_50" in ticker_df.columns:
            summary["Trend_Signal"] = (
                "Bullish" if last_row["SMA_10"] > last_row["SMA_50"] else "Bearish"
            )

        if pd.notna(last_row["RSI"]):
            if last_row["RSI"] > 70:
                summary["RSI_Signal"] = "Overbought"
            elif last_row["RSI"] < 30:
                summary["RSI_Signal"] = "Oversold"
            else:
                summary["RSI_Signal"] = "Neutral"
        else:
            summary["RSI_Signal"] = None

        summary_rows.append(summary)

    summary_df = pd.DataFrame(summary_rows)

    if summary_df.empty:
        raise ValueError("No ticker summaries could be generated.")

    return summary_df


def save_individual_ticker_summaries(
    df: pd.DataFrame,
    output_dir: Path
) -> List[Path]:
    """
    Save one summary CSV per ticker.
    """
    df = prepare_dataframe_for_reporting(df)
    ensure_directory_exists(output_dir)

    saved_files = []

    for ticker in df["Ticker"].dropna().unique():
        ticker_df = df[df["Ticker"] == ticker].copy()

        if ticker_df.empty:
            continue

        summary_df = generate_ticker_summary(ticker_df)
        output_path = output_dir / f"{ticker}_summary.csv"
        summary_df.to_csv(output_path, index=False)

        saved_files.append(output_path)
        logger.info(f"Saved summary for {ticker}: {output_path}")

    return saved_files


def save_global_summary(
    df: pd.DataFrame,
    output_path: Path
) -> Path:
    """
    Save a global summary CSV for all tickers.
    """
    summary_df = generate_ticker_summary(df)

    ensure_directory_exists(output_path.parent)
    summary_df.to_csv(output_path, index=False)

    logger.info(f"Saved global summary: {output_path}")
    return output_path


def load_processed_files(input_dir: Path) -> pd.DataFrame:
    """
    Load and concatenate all processed CSV files from a directory.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    csv_files = list(input_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {input_dir}")

    dataframes = []

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                logger.warning(f"Skipping empty file: {file_path}")
                continue

            dataframes.append(df)
            logger.info(f"Loaded file: {file_path}")

        except Exception as e:
            logger.error(f"Error loading file {file_path}: {e}")

    if not dataframes:
        raise ValueError("No valid processed CSV files could be loaded.")

    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Combined {len(dataframes)} processed files into one dataframe.")

    return combined_df


def run_reporting_pipeline(
    input_dir: Path,
    output_dir: Path,
    global_summary_filename: str = "all_tickers_summary.csv",
) -> Optional[pd.DataFrame]:
    """
    Full reporting pipeline:
    - load processed files
    - save individual ticker summaries
    - save global summary
    - return global summary dataframe
    """
    logger.info("Starting reporting pipeline...")

    df = load_processed_files(input_dir)

    save_individual_ticker_summaries(df, output_dir)

    global_summary_path = output_dir / global_summary_filename
    save_global_summary(df, global_summary_path)

    global_summary_df = pd.read_csv(global_summary_path)

    logger.info("Reporting pipeline completed successfully.")
    return global_summary_df