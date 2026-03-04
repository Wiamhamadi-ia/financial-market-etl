import logging
import os


def setup_logger() -> logging.Logger:
    """
    Configure and return a logger for the ETL pipeline.
    """

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger("financial_market_etl")
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Log format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler("logs/etl.log")
    file_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger