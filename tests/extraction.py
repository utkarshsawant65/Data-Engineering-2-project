import requests
import csv
import os

def fetch_and_save_stock_data():
    # Define the API endpoint and parameters
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=4R98MITURZRRFCAW'

    try:
        # Fetch data from the API
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        data = response.json()

        # Check if the expected data exists in the response
        if "Time Series (5min)" in data:
            time_series = data["Time Series (5min)"]

            # Define the output CSV file path
            output_file = os.path.join(os.getcwd(), "ibm_stock_data.csv")

            # Write the data to the CSV file
            with open(output_file, mode="w", newline="") as file:
                writer = csv.writer(file)

                # Write the header row
                header = ["Timestamp", "Open", "High", "Low", "Close", "Volume"]
                writer.writerow(header)

                # Write the time series data rows
                for timestamp, values in time_series.items():
                    row = [
                        timestamp,
                        values["1. open"],
                        values["2. high"],
                        values["3. low"],
                        values["4. close"],
                        values["5. volume"]
                    ]
                    writer.writerow(row)

            print(f"Data successfully written to {output_file}")
        else:
            print("Error: 'Time Series (5min)' data not found in the API response.")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")

if __name__ == "__main__":
    fetch_and_save_stock_data()
