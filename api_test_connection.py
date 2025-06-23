import os
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta

# Securely load environment variables from .env
load_dotenv()

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

#Check if keys are loaded
if not API_KEY or not SECRET_KEY:
    raise ValueError("API_KEY and SECRET_KEY must be set in the .env file.")
else:
    print("API keys loaded successfully.")



#Create the data client
data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

# Define the parameters for our request - let's get yesterday's data
# Note: Alpaca free tier data has a 15-minute delay.
yesterday = datetime.now() - timedelta(days=3)
start_time_utc = yesterday.replace(hour=13, minute=30, second=0, microsecond=0) # 9:30 AM NYC
end_time_utc = yesterday.replace(hour=13, minute=35, second=0, microsecond=0)   # 9:35 AM NYC

request_params = StockBarsRequest(
    symbol_or_symbols=["SPY"],
    timeframe=TimeFrame.Minute,
    start=start_time_utc,
    end=end_time_utc
)

# Fetch the data and print it
try:
    bars = data_client.get_stock_bars(request_params)
    print("Data fetched successfully:")
    print(bars.df)
except Exception as e:
    print(f"An error occurred while fetching data: {e}")