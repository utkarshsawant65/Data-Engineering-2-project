# config.py
import os

# GCP Configuration
PROJECT_ID = "stock-data-pipeline-444011"
BUCKET_NAME = "stock-data-pipeline-bucket"
TOPIC_NAME = "stock-data"
DATASET_NAME = "stock_market"
TABLE_NAME = "amazon_stock"

# Alpha Vantage Configuration
API_KEY = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AMZN&interval=5min&outputsize=full&apikey=J30SRXLUMQK4EW8Y"
STOCK_SYMBOL = "AMZN"
