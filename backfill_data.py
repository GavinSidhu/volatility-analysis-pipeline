# backfill_data.py

import time
from datetime import date, timedelta

import sys
import os

dags_path = os.path.abspath(os.path.join(os.getcwd(), 'dags'))
print("Looking for dags directory at:", dags_path)
print("Exists?", os.path.isdir(dags_path))

sys.path.insert(0, dags_path)
from scripts.ingest_data import ingest_data

def perform_backfill(start_date_str: str, end_date_str: str, symbol: str):
    """
    Loops through a date range and calls the ingestion function for each day.
    """
    start_date = date.fromisoformat(start_date_str)
    end_date = date.fromisoformat(end_date_str)

    current_date = start_date
    while current_date <= end_date:
        # We only care about weekdays for trading data
        if current_date.weekday() < 5: # Monday is 0 and Sunday is 6
            date_string = current_date.strftime("%Y-%m-%d")
            try:
                ingest_data(symbol=symbol, date_str=date_string)
            except Exception as e:
                print(f"Could not fetch data for {date_string}. Error: {e}")

            # IMPORTANT: Be respectful of the API and avoid rate limits
            time.sleep(1) # Wait for 1 second between each daily request

        current_date += timedelta(days=1)

if __name__ == "__main__":


    start = "2016-01-01"
    end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Starting historical backfill for SPY from {start} to {end}...")
    perform_backfill(start_date_str=start, end_date_str=end, symbol="SPY")
    print("Historical backfill complete.")