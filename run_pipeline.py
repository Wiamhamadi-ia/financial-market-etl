import argparse

from src.logger import setup_logger
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.extract import extract_market_data
from src.transform import transform_market_data
from src.load import save_to_csv

logger = setup_logger()


def run_pipeline_for_tickers(tickers, start_date, end_date):
    """
    Run ETL pipeline for a list of tickers from CLI arguments.
    """
    logger.info("========== STARTING CLI ETL PIPELINE ==========")

    for ticker in tickers:
        try:
            ticker = ticker.strip().upper()
            logger.info(f"----- Processing ticker: {ticker} -----")

            # Extract
            raw_df = extract_market_data(ticker, start_date, end_date)

            # Save raw
            raw_output_path = RAW_DATA_DIR / f"{ticker}_raw.csv"
            save_to_csv(raw_df, raw_output_path)

            # Transform
            processed_df = transform_market_data(raw_df)

            # Save processed
            processed_output_path = PROCESSED_DATA_DIR / f"{ticker}_processed.csv"
            save_to_csv(processed_df, processed_output_path)

            logger.info(f"Ticker {ticker} processed successfully.")

        except Exception as e:
            logger.error(f"Pipeline failed for ticker {ticker}: {e}")

    logger.info("========== CLI ETL PIPELINE FINISHED ==========")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline for financial market data.")

    parser.add_argument(
        "--tickers",
        type=str,
        required=True,
        help='Comma-separated tickers, e.g. "AAPL,MSFT,TSLA"'
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date in YYYY-MM-DD format"
    )

    args = parser.parse_args()

    tickers_list = args.tickers.split(",")

    run_pipeline_for_tickers(
        tickers=tickers_list,
        start_date=args.start,
        end_date=args.end
    )