from src.config import TICKERS, START_DATE, END_DATE, RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.logger import setup_logger
from src.extract import extract_market_data
from src.transform import transform_market_data
from src.load import save_to_csv

logger = setup_logger()


def run_etl_pipeline():
    """
    Run ETL pipeline for all tickers defined in config.py
    """
    logger.info("========== STARTING MULTI-TICKER ETL PIPELINE ==========")

    all_processed_data = []

    for ticker in TICKERS:
        try:
            logger.info(f"----- Processing ticker: {ticker} -----")

            # 1. Extract
            raw_df = extract_market_data(ticker, START_DATE, END_DATE)

            # 2. Save raw data
            raw_output_path = RAW_DATA_DIR / f"{ticker}_raw.csv"
            save_to_csv(raw_df, raw_output_path)

            # 3. Transform
            processed_df = transform_market_data(raw_df)

            # 4. Save processed data
            processed_output_path = PROCESSED_DATA_DIR / f"{ticker}_processed.csv"
            save_to_csv(processed_df, processed_output_path)

            all_processed_data.append(processed_df)

            logger.info(f"Ticker {ticker} processed successfully.")

        except Exception as e:
            logger.error(f"Pipeline failed for ticker {ticker}: {e}")

    logger.info("========== MULTI-TICKER ETL PIPELINE FINISHED ==========")

    return all_processed_data


if __name__ == "__main__":
    run_etl_pipeline()