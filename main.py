from src.logger import setup_logger
from src.extract import extract_stock_data, save_raw_data
from src.transform import transform_stock_data
from src.load import save_processed_data
from src.config import TICKER, START_DATE, END_DATE


if __name__ == "__main__":

    logger = setup_logger()

    logger.info("ETL pipeline started")

    # Extract
    raw_data = extract_stock_data(TICKER, START_DATE, END_DATE)

    # Save raw data
    save_raw_data(raw_data, TICKER)

    # Transform
    transformed_data = transform_stock_data(raw_data)

    # Load processed data
    save_processed_data(transformed_data, TICKER)

    logger.info("ETL pipeline finished successfully")