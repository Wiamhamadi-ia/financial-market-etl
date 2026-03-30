import argparse

from src.logger import setup_logger
from src.config import PROCESSED_DATA_DIR
from src.visualization import generate_all_charts_for_ticker

logger = setup_logger()


def run_visualization_for_tickers(tickers):
    """
    Generate charts for a list of processed ticker files.
    """
    logger.info("========== STARTING VISUALIZATION PIPELINE ==========")

    for ticker in tickers:
        try:
            ticker = ticker.strip().upper()
            logger.info(f"----- Generating charts for ticker: {ticker} -----")

            processed_file_path = PROCESSED_DATA_DIR / f"{ticker}_processed.csv"

            generate_all_charts_for_ticker(processed_file_path, ticker)

            logger.info(f"Charts generated successfully for ticker {ticker}")

        except Exception as e:
            logger.error(f"Visualization failed for ticker {ticker}: {e}")

    logger.info("========== VISUALIZATION PIPELINE FINISHED ==========")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate financial charts from processed market data."
    )

    parser.add_argument(
        "--tickers",
        type=str,
        required=True,
        help='Comma-separated tickers, e.g. "AAPL,MSFT,TSLA"'
    )

    args = parser.parse_args()

    tickers_list = args.tickers.split(",")

    run_visualization_for_tickers(tickers=tickers_list)