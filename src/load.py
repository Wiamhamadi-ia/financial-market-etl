import os
import pandas as pd
import logging
from datetime import datetime


def save_processed_data(data: pd.DataFrame, ticker: str) -> str:
    """
    Save processed stock data to CSV in data/processed directory.
    Returns the output file path.
    """

    logging.info(f"Saving processed data for {ticker}")

    try:
        os.makedirs("data/processed", exist_ok=True)

        today = datetime.today().strftime("%Y-%m-%d")
        filename = f"data/processed/{ticker}_processed_{today}.csv"

        data.to_csv(filename, index=False)

        logging.info(f"Processed data saved successfully to {filename}")
        return filename

    except Exception as e:
        logging.error(f"Failed to save processed data: {e}")
        raise