import logging
from datetime import datetime, time
from pathlib import Path
import pandas as pd
import pytz


class StockDataPreprocessor:
    def __init__(self):
        # Initialize logging
        self.setup_logging()

        # Market hours in ET
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET

        # Initialize timezone
        self.et_timezone = pytz.timezone("US/Eastern")

        # Define expected columns
        self.raw_columns = [
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

        self.logger.info("Initialized StockDataPreprocessor")

    def setup_logging(self):
        """Configure logging for preprocessor"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"preprocessor_{timestamp}.log"

        self.logger = logging.getLogger("StockPreprocessor")
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers to avoid duplicates
        if self.logger.handlers:
            for handler in self.logger.handlers:
                self.logger.removeHandler(handler)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def is_market_hours(self, timestamp: pd.Timestamp) -> bool:
        """Check if timestamp is within market hours"""
        try:
            # Convert to ET if not already
            if timestamp.tz is None:
                timestamp = timestamp.tz_localize("UTC").tz_convert(self.et_timezone)
            elif timestamp.tz != self.et_timezone:
                timestamp = timestamp.tz_convert(self.et_timezone)

            current_time = timestamp.time()
            return (
                self.market_open <= current_time <= self.market_close
                and timestamp.weekday() < 5  # Monday = 0, Friday = 4
            )
        except Exception as e:
            self.logger.error(f"Error checking market hours for {timestamp}: {e}")
            return False

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean data"""
        self.logger.info(f"Starting data validation on {len(df)} records")

        try:
            # Make copy to avoid modifying original
            df = df.copy()

            # Ensure timestamp column exists and is datetime
            if "timestamp" not in df.columns:
                raise ValueError("DataFrame must contain 'timestamp' column")

            # Convert timestamp to datetime if it's not already
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Remove duplicates where all values in the timestamp column are identical
            initial_len = len(df)
            df = df.drop_duplicates(subset=["timestamp", "symbol"], keep="first")
            dupes_removed = initial_len - len(df)
            if dupes_removed > 0:
                self.logger.warning(f"Removed {dupes_removed} duplicate records")

            # Ensure raw columns are in correct order
            df = df.reindex(columns=self.raw_columns)

            self.logger.info(
                f"Data validation complete. {len(df)} valid records remaining"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error during data validation: {e}")
            raise

    def process_stock_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process stock data with various transformations and validations
        """
        try:
            self.logger.info(f"Starting data processing for {len(df)} records")

            # Validate data first
            df = self.validate_data(df)

            # Filter for market hours
            df["market_hours"] = df["timestamp"].map(self.is_market_hours)
            initial_len = len(df)
            df = df[df["market_hours"]]
            df = df.drop("market_hours", axis=1)
            filtered_count = initial_len - len(df)
            self.logger.info(
                f"Filtered out {filtered_count} records outside market hours"
            )

            self.logger.info(f"Data processing complete. Final record count: {len(df)}")
            return df

        except Exception as e:
            self.logger.error(f"Error during data processing: {e}")
            raise
