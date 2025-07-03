"""
DAG for ingesting stock data from Alpaca API to S3 using dlt
"""
from __future__ import annotations

import os
import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# Import our dlt pipeline
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from scripts.alpaca_to_s3_pipeline import run_alpaca_to_s3_pipeline

@dag(
    dag_id="alpaca_to_s3",
    start_date=pendulum.datetime(2025, 6, 20, tz="UTC"),
    catchup=False,
    schedule="0 1 * * 1-5",  # Run at 1 AM UTC Monday-Friday
    tags=["data_ingestion", "alpaca", "s3"],
    description="Ingest stock data from Alpaca API to S3"
)
def alpaca_to_s3_dag():
    
    @task
    def get_symbols():
        """Get list of symbols to ingest"""
        # Default to SPY if no symbols are configured
        symbols = Variable.get("stock_symbols", default="SPY")
        return symbols.split(',')
    
    @task
    def get_date():
        """Get the date to ingest (previous business day)"""
        # Get current date in UTC
        today = pendulum.now().date()
        
        # If today is Monday, we need to get Friday's data
        if today.weekday() == 0:  # Monday
            return (today - timedelta(days=3)).strftime("%Y-%m-%d")
        else:
            return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    
    @task
    def ingest_symbol_data(symbol: str, date_str: str):
        """Ingest data for a single symbol using dlt pipeline"""
        # Get S3 bucket from environment or Variable
        s3_bucket = os.getenv("S3_BUCKET_NAME") or Variable.get("s3_bucket_name", default="stock-data-pipeline")
        
        # Run the pipeline
        result = run_alpaca_to_s3_pipeline(
            symbol=symbol,
            date_str=date_str,
            s3_bucket=s3_bucket,
            s3_prefix="raw"
        )
        
        return {
            "symbol": symbol,
            "date": date_str,
            "records_loaded": result["records_loaded"],
            "s3_path": result["destination"]
        }
    
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_ingestion(results):
        """Summarize the ingestion results"""
        total_records = sum(result["records_loaded"] for result in results)
        successful_symbols = sum(1 for result in results if result["records_loaded"] > 0)
        
        return {
            "date": results[0]["date"] if results else None,
            "total_symbols": len(results),
            "successful_symbols": successful_symbols,
            "total_records": total_records
        }
    
    # Get list of symbols and date
    symbols = get_symbols()
    date_str = get_date()
    
    # Ingest data for each symbol
    ingestion_results = []
    for symbol in symbols:
        result = ingest_symbol_data(symbol, date_str)
        ingestion_results.append(result)
    
    # Summarize results
    summary = summarize_ingestion(ingestion_results)

# Instantiate the DAG
alpaca_to_s3_dag()