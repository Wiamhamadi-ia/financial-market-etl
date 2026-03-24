from pathlib import Path

# =========================
# Base paths
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
LOG_DIR = BASE_DIR / "logs"

# =========================
# Market data configuration
# =========================
TICKERS = ["AAPL", "MSFT", "TSLA"]   # Multi-tickers
START_DATE = "2020-01-01"
END_DATE = "2025-01-01"

# =========================
# Financial indicators
# =========================
SMA_WINDOWS = [20, 50, 200]
VOLATILITY_WINDOW = 20
TRADING_DAYS_PER_YEAR = 252