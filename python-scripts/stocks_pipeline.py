# stocks_pipeline.py
import csv
import json
import os
import time
from datetime import datetime
from typing import Dict

import pandas as pd
import requests  # Added this import
import schedule
from config import GCP_CONFIG, STOCK_CONFIGS, get_api_url
from google.cloud import pubsub_v1, storage
from preprocessing_pipeline import StockDataPreprocessor


class StockDataPipeline:
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.storage_client = storage.Client()
        self.preprocessor = StockDataPreprocessor()
        self.topic_path = self.publisher.topic_path(
            GCP_CONFIG["PROJECT_ID"], GCP_CONFIG["TOPIC_NAME"]
        )
        self.bucket = self.storage_client.bucket(GCP_CONFIG["BUCKET_NAME"])

    def save_to_gcs(self, data: dict, symbol: str, timestamp: str) -> None:
        """Save raw data to Google Cloud Storage"""
        try:
            blob = self.bucket.blob(f"raw-data/{symbol}/{timestamp}.json")
            blob.upload_from_string(json.dumps(data))
            print(f"Saved raw data to GCS: {symbol} - {timestamp}")
        except Exception as e:
            print(f"Error saving to GCS: {e}")

    def publish_to_pubsub(self, record: dict) -> None:
        """Publish record to Pub/Sub"""
        try:
            # Ensure the record has all required fields
            if "timestamp" not in record:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Adding timestamp {current_time} to record")
                record["timestamp"] = current_time

            # Remove NaN fields before publishing
            cleaned_record = {
                "timestamp": record["timestamp"],
                "symbol": record["symbol"],
                "open": float(record["open"]),
                "high": float(record["high"]),
                "low": float(record["low"]),
                "close": float(record["close"]),
                "volume": int(record["volume"]),
            }

            # Create message and publish
            message = json.dumps(cleaned_record).encode("utf-8")
            future = self.publisher.publish(self.topic_path, data=message)
            message_id = future.result()
            print(
                f"Published message {message_id} for {record['symbol']} at {record['timestamp']}"
            )

        except Exception as e:
            print(f"Error publishing to Pub/Sub: {e}")
            raise

    def fetch_stock_data(self, symbol: str, config: dict) -> None:
        """Fetch and process data for a single stock"""
        api_url = get_api_url(
            symbol=symbol, interval=config["interval"], api_key=config["api_key"]
        )

        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Raise exception for bad status codes
            data = response.json()

            if "Time Series (5min)" in data:
                time_series = data["Time Series (5min)"]
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Save raw response to GCS
                self.save_to_gcs(data, symbol, current_time)

                # Process and publish each data point
                for timestamp_str, values in time_series.items():
                    record = {
                        "timestamp": timestamp_str,
                        "symbol": symbol,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"]),
                    }
                    self.publish_to_pubsub(record)

                print(f"Successfully processed data for {symbol}")
            else:
                print(f"Error: Time Series data not found in response for {symbol}")
                print("Response:", data)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
        finally:
            # Add delay to respect rate limits
            time.sleep(12)  # Alpha Vantage free tier allows 5 calls per minute


def process_all_stocks():
    """Process all configured stocks"""
    pipeline = StockDataPipeline()
    for symbol, config in STOCK_CONFIGS.items():
        try:
            pipeline.fetch_stock_data(symbol, config)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")


def main():
    # Schedule the task to run every hour
    import schedule

    schedule.every(1).hour.do(process_all_stocks)

    # Run immediately for the first time
    print("Starting initial data fetch...")
    process_all_stocks()

    # Keep the script running
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping the pipeline...")
            break
        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(60)  # Wait a minute before retrying


if __name__ == "__main__":
    main()
