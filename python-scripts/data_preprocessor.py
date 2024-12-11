from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from config import GCP_CONFIG
from google.cloud import storage


class DataPreprocessor:
    def __init__(self):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(GCP_CONFIG["BUCKET_NAME"])

    def preprocess_time_series(self, time_series: Dict) -> Dict:
        """Process time series data and return processed records"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient="index")
            df.reset_index(inplace=True)
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

            # Clean numeric columns
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col].str.strip("1234. "))
            df["volume"] = pd.to_numeric(df["volume"].str.strip("5. "))

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Extract date and time
            df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
            df["time"] = df["timestamp"].dt.strftime("%H:%M:%S")

            # Calculate moving averages
            df["moving_average"] = df["close"].rolling(window=5, min_periods=1).mean()
            df["cumulative_average"] = df["close"].expanding().mean()

            # Convert to dictionary with timestamp as key
            processed_data = {}
            for _, row in df.iterrows():
                timestamp = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                processed_data[timestamp] = {
                    "date": row["date"],
                    "time": row["time"],
                    "moving_average": float(row["moving_average"]),
                    "cumulative_average": float(row["cumulative_average"]),
                }

            return processed_data

        except Exception as e:
            print(f"Error preprocessing time series: {e}")
            return {}

    def save_raw_csv(self, data: Dict, symbol: str, timestamp: str) -> None:
        """Save raw data as CSV to Google Cloud Storage"""
        try:
            # Extract time series data
            time_series = data["Time Series (5min)"]

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient="index")
            df.reset_index(inplace=True)

            # Rename columns
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

            # Clean numeric columns
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col].str.strip("1234. "))
            df["volume"] = pd.to_numeric(df["volume"].str.strip("5. "))

            # Save as CSV
            blob = self.bucket.blob(f"raw-data/{symbol}/{timestamp}.csv")
            blob.upload_from_string(df.to_csv(index=False))
            print(f"Saved raw CSV to GCS: {symbol} - {timestamp}")

        except Exception as e:
            print(f"Error saving raw CSV to GCS: {e}")

    def process_and_save_data(
        self, data: Dict, symbol: str, timestamp: str
    ) -> Optional[Dict]:
        """Process raw data and save processed version, return processed data dictionary"""
        try:
            # Extract time series data
            time_series = data["Time Series (5min)"]

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient="index")
            df.reset_index(inplace=True)

            # Rename columns
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

            # Clean numeric columns
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col].str.strip("1234. "))
            df["volume"] = pd.to_numeric(df["volume"].str.strip("5. "))

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Add date and time columns
            df["date"] = df["timestamp"].dt.date
            df["time"] = df["timestamp"].dt.time

            # Sort chronologically
            df = df.sort_values(by=["date", "time"]).reset_index(drop=True)

            # Calculate moving averages
            df["moving_average"] = df.groupby("date")["close"].transform(
                lambda x: x.rolling(window=5, min_periods=1).mean()
            )
            df["cumulative_average"] = df["close"].expanding().mean()

            # Save processed CSV
            blob = self.bucket.blob(
                f"processed-data/{symbol}/{timestamp}_processed.csv"
            )
            blob.upload_from_string(df.to_csv(index=False))
            print(f"Saved processed data to GCS: {symbol} - {timestamp}")

            # Create dictionary of processed data indexed by timestamp
            processed_data = {
                "date": df.set_index("timestamp")["date"].astype(str).to_dict(),
                "time": df.set_index("timestamp")["time"].astype(str).to_dict(),
                "moving_average": df.set_index("timestamp")["moving_average"].to_dict(),
                "cumulative_average": df.set_index("timestamp")[
                    "cumulative_average"
                ].to_dict(),
            }

            return processed_data

        except Exception as e:
            print(f"Error processing and saving data: {e}")
            return None
