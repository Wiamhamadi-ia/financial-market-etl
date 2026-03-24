from pathlib import Path
import pandas as pd

from src.logger import setup_logger
from src.utils import ensure_directory

logger = setup_logger()


def save_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save DataFrame to CSV.
    """
    try:
        ensure_directory(output_path.parent)
        df.to_csv(output_path, index=False)
        logger.info(f"File saved successfully: {output_path}")
    except Exception as e:
        logger.error(f"Error saving file to {output_path}: {e}")
        raise