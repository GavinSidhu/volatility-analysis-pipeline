"""
dlt pipeline for extracting SPY stock data from Alpaca and loading to S3
"""
import os
import dlt
import boto3
import io
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Optional
from botocore.exceptions import NoCredentialsError, ClientError

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# Configure detailed logging
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add boto3 specific logging
boto3_logger = logging.getLogger('boto3')
boto3_logger.setLevel(logging.DEBUG)
botocore_logger = logging.getLogger('botocore')
botocore_logger.setLevel(logging.DEBUG)

def check_aws_credentials():
    """Check if AWS credentials are properly configured"""
    try:
        # Try to list S3 buckets as a test
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        
        # Log the buckets
        logger.info("AWS credentials are valid. Available buckets:")
        for bucket in response['Buckets']:
            logger.info(f"  - {bucket['Name']}")
        
        return True
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        return False
    except ClientError as e:
        logger.error(f"AWS credentials error: {e}")
        return False

def test_s3_write(bucket_name):
    """Test writing a small file to S3"""
    try:
        # Create a small test file
        test_data = io.BytesIO(b"This is a test file")
        
        # Try to upload it
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(test_data, bucket_name, "test/test_file.txt")
        
        logger.info(f"Successfully wrote test file to s3://{bucket_name}/test/test_file.txt")
        return True
    except ClientError as e:
        logger.error(f"Failed to write test file to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in test_s3_write: {e}")
        return False

@dlt.resource(name="alpaca_spy_data")
def alpaca_stock_data_source(
    date_str: str,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None
) -> Iterator[Dict]:
    """
    Source function for dlt that extracts SPY stock data from Alpaca API
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        api_key: Alpaca API key (defaults to env var API_KEY)
        secret_key: Alpaca secret key (defaults to env var SECRET_KEY)
    
    Yields:
        Dictionary with stock data
    """
    symbol = "SPY"  # Hardcoded to SPY as that's our focus
    
    # Get API credentials
    api_key = api_key or os.getenv("ALPACA_API_KEY")
    secret_key = secret_key or os.getenv("ALPACA_SECRET_KEY")
    
    if not api_key or not secret_key:
        raise ValueError("ALPACA_API_KEY or ALPACA_SECRET_KEY is not set")
    
    logger.info(f"API credentials loaded successfully")
    
    # Initialize Alpaca client
    client = StockHistoricalDataClient(api_key, secret_key)
    
    # Parse date and set time range
    trade_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    # Skip weekends
    if trade_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        logger.info(f"Skipping weekend date: {date_str}")
        return
    
    # Set market hours (in UTC)
    start_time_utc = trade_date.replace(hour=13, minute=30)  # 9:30 AM EST (Market open)
    end_time_utc = trade_date.replace(hour=20, minute=0)     # 4:00 PM EST (Market close)
    
    # Create request parameters
    request_params = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame.Minute,
        start=start_time_utc,
        end=end_time_utc,
    )
    
    try:
        # Fetch data
        logger.info(f"Fetching {symbol} data for {date_str}")
        bars = client.get_stock_bars(request_params)
        
        # Check if we got data
        if hasattr(bars, "df") and not bars.df.empty:
            df = bars.df.reset_index()  # Convert multi-index to columns
            
            # Log data shape and sample
            logger.info(f"Data shape: {df.shape}")
            logger.info(f"Data sample: \n{df.head(2)}")
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Add metadata
            for record in records:
                record['date'] = date_str
                record['symbol'] = symbol if 'symbol' not in record else record['symbol']
                
                # Yield each record for dlt processing
                yield record
                
            logger.info(f"Fetched {len(records)} records for {symbol} on {date_str}")
        else:
            logger.warning(f"No data received for {symbol} on {date_str}")
    
    except Exception as e:
        logger.error(f"Error fetching data for {symbol} on {date_str}: {e}")
        raise

@dlt.destination
def s3_destination(bucket: str, prefix: str, date_str: str, symbol: str = "SPY"):
    """
    Define S3 destination for the pipeline
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix (folder path)
        date_str: Date string for partitioning
        symbol: Stock symbol (default: SPY)
        
    Returns:
        dlt filesystem destination configuration
    """
    # Construct S3 bucket URL
    bucket_url = f"s3://{bucket}/{prefix}/{symbol}/{date_str}"
    logger.info(f"Configuring S3 destination: {bucket_url}")
    
    # Return filesystem destination with S3 configuration
    return dlt.destinations.filesystem(
        bucket_url=bucket_url
    )

def direct_upload_to_s3(data, bucket, key_prefix, date_str, symbol="SPY"):
    """
    Directly upload data to S3 using boto3
    
    Args:
        data: List of dictionaries with data
        bucket: S3 bucket name
        key_prefix: S3 key prefix
        date_str: Date string
        symbol: Symbol (default: SPY)
        
    Returns:
        Success flag
    """
    if not data:
        logger.warning("No data to upload")
        return False
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Convert to parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        # Upload to S3
        s3_key = f"{key_prefix}/{symbol}/{date_str}.parquet"
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(parquet_buffer, bucket, s3_key)
        
        logger.info(f"Successfully uploaded {len(data)} records to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Error in direct upload: {e}")
        return False

def run_alpaca_to_s3_pipeline(
    date_str: str,
    s3_bucket: Optional[str] = None,
) -> Dict:
    """
    Run the pipeline to extract SPY data and load to S3
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        s3_bucket: S3 bucket name (defaults to env var S3_BUCKET_NAME)
        
    Returns:
        Dictionary with load info
    """
    symbol = "SPY"  # Hardcoded to SPY
    s3_prefix = "raw"  # Standard prefix for raw data
    
    # Get S3 bucket
    s3_bucket = s3_bucket or os.getenv("S3_BUCKET_NAME")
    if not s3_bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    
    logger.info(f"Using S3 bucket: {s3_bucket}")
    
    # Check AWS credentials
    if not check_aws_credentials():
        logger.error("AWS credentials check failed")
        return {
            "symbol": symbol,
            "date": date_str,
            "destination": f"s3://{s3_bucket}/{s3_prefix}/{symbol}/{date_str}",
            "records_loaded": 0,
            "error": "AWS credentials check failed"
        }
    
    # Test S3 write permission
    if not test_s3_write(s3_bucket):
        logger.error("S3 write permission test failed")
        return {
            "symbol": symbol,
            "date": date_str,
            "destination": f"s3://{s3_bucket}/{s3_prefix}/{symbol}/{date_str}",
            "records_loaded": 0,
            "error": "S3 write permission test failed"
        }
    
    # Create the S3 destination with explicit credentials
    destination = s3_destination(
        bucket=s3_bucket,
        prefix=s3_prefix,
        date_str=date_str,
        symbol=symbol
    )
    
    # First collect the data to verify we have some
    logger.info("Collecting data from Alpaca...")
    data = list(alpaca_stock_data_source(date_str=date_str))
    
    if not data:
        logger.warning(f"No data returned from Alpaca for {date_str}")
        return {
            "symbol": symbol,
            "date": date_str,
            "destination": f"s3://{s3_bucket}/{s3_prefix}/{symbol}/{date_str}",
            "records_loaded": 0,
            "error": "No data returned from Alpaca"
        }
    
    logger.info(f"Collected {len(data)} records from Alpaca")
    logger.info(f"Sample data: {data[0] if data else 'None'}")
    
    # Initialize pipeline with the destination
    pipeline = dlt.pipeline(
        pipeline_name=f"alpaca_spy",
        destination=destination,
        dataset_name="alpaca_spy_data"
    )
    
    # Run pipeline to extract and load data
    logger.info("Running pipeline to load data to S3...")
    load_info = pipeline.run(
        data,  # Pass the collected data
        table_name="minute_bars",
        write_disposition="replace"
    )
    
    logger.info(f"Pipeline completed with load info: {load_info}")
    if hasattr(load_info, '__dict__'):
        logger.info(f"Load info details: {load_info.__dict__}")
    
    # Calculate records loaded
    records_loaded = 0
    if hasattr(load_info, 'load_package') and hasattr(load_info.load_package, 'loaded_records_count'):
        records_loaded = load_info.load_package.loaded_records_count
        logger.info(f"Records loaded: {records_loaded}")
    
    return {
        "symbol": symbol,
        "date": date_str,
        "destination": f"s3://{s3_bucket}/{s3_prefix}/{symbol}/{date_str}",
        "load_info": load_info,
        "records_loaded": records_loaded
    }

if __name__ == "__main__":
    # Example usage
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Use yesterday's date for regular operation
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # For testing, you might want to use a date you know has data:
    test_date = "2023-06-01"  # Example date with data
    
    # Print environment variable status
    logger.info(f"AWS_ACCESS_KEY_ID set: {'Yes' if os.getenv('AWS_ACCESS_KEY_ID') else 'No'}")
    logger.info(f"AWS_SECRET_ACCESS_KEY set: {'Yes' if os.getenv('AWS_SECRET_ACCESS_KEY') else 'No'}")
    logger.info(f"S3_BUCKET_NAME set: {'Yes' if os.getenv('S3_BUCKET_NAME') else 'No'}")
    
    # Try both dates
    for date_str in [yesterday, test_date]:
        logger.info(f"Testing with date: {date_str}")
        
        # Collect data first
        data = list(alpaca_stock_data_source(date_str=date_str))
        
        if not data:
            logger.warning(f"No data for {date_str}")
            continue
        
        logger.info(f"Found {len(data)} records for {date_str}")
        
        # Try DLT pipeline
        try:
            logger.info("Trying DLT pipeline...")
            result = run_alpaca_to_s3_pipeline(date_str=date_str)
            logger.info(f"DLT pipeline result: {result}")
        except Exception as e:
            logger.error(f"DLT pipeline failed: {e}")
        
        # Try direct upload as backup
        try:
            logger.info("Trying direct upload...")
            s3_bucket = os.getenv("S3_BUCKET_NAME")
            success = direct_upload_to_s3(
                data=data,
                bucket=s3_bucket,
                key_prefix="raw",
                date_str=date_str
            )
            logger.info(f"Direct upload result: {success}")
        except Exception as e:
            logger.error(f"Direct upload failed: {e}")