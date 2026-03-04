from src.logger import setup_logger
from src.extract import extract_stock_data, save_raw_data

if __name__ == "__main__":

    logger = setup_logger()

    ticker = "AAPL"
    start_date = "2023-01-01"
    end_date = "2024-01-01"

    logger.info("ETL pipeline started")

    data = extract_stock_data(ticker, start_date, end_date)
    save_raw_data(data, ticker)

    logger.info("ETL pipeline finished successfully")