from src.logger import setup_logger
from src.extract import extract_stock_data, save_raw_data
from src.transform import transform_stock_data

if __name__ == "__main__":

    logger = setup_logger()

    ticker = "AAPL"
    start_date = "2023-01-01"
    end_date = "2024-01-01"

    logger.info("ETL pipeline started")

    data = extract_stock_data(ticker, start_date, end_date)
    transformed_data = transform_stock_data(data)
    save_raw_data(transformed_data, ticker)
    

    logger.info("ETL pipeline finished successfully") 