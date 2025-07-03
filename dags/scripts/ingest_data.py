import dlt
import os 
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

import logging
logger = logging.getLogger('airflow.task')

from dotenv import load_dotenv
load_dotenv()


API_KEY = os.getenv("API_KEY") 
SECRET_KEY = os.getenv("SECRET_KEY")

DATA_DIR = os.environ.get(
    "RAW_DATA_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw"))
)

def ingest_data(symbol: str, date_str: str):
    if not API_KEY or not SECRET_KEY: 
        raise ValueError("API_KEY or SECRET_KEY is not set")

    client = StockHistoricalDataClient(API_KEY, SECRET_KEY) 

    trade_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_time_utc = trade_date.replace(hour = 13, minute=30) #9:30 AM EST (Market open)
    end_time_utc = trade_date.replace(hour=20, minute = 0) #4:00 PM EST (Market close)

    request_params = StockBarsRequest(
        symbol_or_symbols = [symbol],
        timeframe=TimeFrame.Minute,
        start=start_time_utc,
        end=end_time_utc,
    )

    try:
        bars = client.get_stock_bars(request_params)
        logger.info("Bars object: %s", bars)
        if hasattr(bars, "df"):
            logger.info("DataFrame shape: %s", str(bars.df.shape))
            logger.info("DataFrame head:\n%s", str(bars.df.head()))
        else:
            logger.info("No .df attribute in bars object")

        output_dir = os.path.join(DATA_DIR, symbol)
        os.makedirs(output_dir, exist_ok=True)

        file_path = os.path.join(output_dir, f"{date_str}.parquet")
        logger.info("Output directory: %s", output_dir)
        logger.info("File path: %s", file_path)
        logger.info("DataFrame shape before saving: %s", str(bars.df.shape))

        logger.info("Saving to file: %s", file_path)
        
        #logger.info("Saving to file: %s", file_path)
        bars.df.to_parquet(file_path)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise 

#if __name__ == "__main__":
#    target_date = datetime.now() - timedelta(days=2)
#    date_string = target_date.strftime("%Y-%m-%d")
#    ingest_data("SPY", date_string)