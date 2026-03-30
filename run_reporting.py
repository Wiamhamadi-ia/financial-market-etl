from pathlib import Path

from src.logger import setup_logger
from src.reporting import run_reporting_pipeline

logger = setup_logger()


def main():
    """
    Run the reporting pipeline on all processed market data files.
    """
    project_root = Path(__file__).resolve().parents[1]

    input_dir = project_root / "data" / "processed"
    output_dir = project_root / "reports" / "summary"

    try:
        summary_df = run_reporting_pipeline(
            input_dir=input_dir,
            output_dir=output_dir,
            global_summary_filename="all_tickers_summary.csv",
        )

        logger.info("Reporting summary preview:")
        logger.info(f"\n{summary_df.head()}")

    except Exception as e:
        logger.exception(f"Reporting pipeline failed: {e}")


if __name__ == "__main__":
    main()