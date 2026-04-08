from pathlib import Path

import pandas as pd

from src.logger import setup_logger

logger = setup_logger()


def load_processed_files(input_dir: Path) -> pd.DataFrame:
    """
    Load all processed CSV files from the input directory
    and concatenate them into a single dataframe.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    csv_files = list(input_dir.glob("*_processed.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No processed CSV files found in: {input_dir}")

    dataframes = []

    for file_path in csv_files:
        # Avoid re-loading the combined file if it already exists
        if file_path.name == "all_tickers_processed.csv":
            continue

        try:
            df = pd.read_csv(file_path)

            if df.empty:
                logger.warning(f"Skipping empty file: {file_path}")
                continue

            dataframes.append(df)
            logger.info(f"Loaded processed file: {file_path}")

        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")

    if not dataframes:
        raise ValueError("No valid processed CSV files were loaded.")

    combined_df = pd.concat(dataframes, ignore_index=True)

    logger.info(
        f"Combined {len(dataframes)} processed files into a dataframe "
        f"with shape {combined_df.shape}."
    )

    return combined_df


def clean_combined_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional cleanup for the combined dataframe.
    """
    df = df.copy()

    # Convert Date if present
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Sort by Ticker + Date if both exist
    sort_columns = []
    if "Ticker" in df.columns:
        sort_columns.append("Ticker")
    if "Date" in df.columns:
        sort_columns.append("Date")

    if sort_columns:
        df = df.sort_values(sort_columns).reset_index(drop=True)

    return df


def export_combined_processed(
    input_dir: Path,
    output_file: Path
) -> Path:
    """
    Export all processed ticker files into one combined CSV.
    """
    logger.info("Starting export of combined processed dataset...")

    combined_df = load_processed_files(input_dir)
    combined_df = clean_combined_dataframe(combined_df)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(output_file, index=False)

    logger.info(f"Saved combined processed dataset to: {output_file}")
    logger.info(f"Final dataset shape: {combined_df.shape}")

    return output_file


def main():
    """
    Main entry point for exporting the combined processed dataset.
    """
    project_root = Path(__file__).resolve().parent

    input_dir = project_root / "data" / "processed"
    output_file = input_dir / "all_tickers_processed.csv"

    try:
        export_combined_processed(
            input_dir=input_dir,
            output_file=output_file
        )

    except Exception as e:
        logger.exception(f"Failed to export combined processed dataset: {e}")


if __name__ == "__main__":
    main()