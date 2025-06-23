# Import necessary libraries from Airflow
from __future__ import annotations
import pendulum
import os
import sys
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

# Add the current directory to Python path to import local modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from scripts.ingest_data import ingest_data

@dag(
    dag_id="daily_data_ingestion",
    start_date=pendulum.datetime(2025, 6, 20, tz="UTC"),
    catchup=False,
    schedule="@daily",  # Run once per day
    tags=["data_ingestion", "alpaca"],
    description="Daily ingestion of SPY data from Alpaca API"
)
def ingestion():
    
    @task
    def ingest_spy_data(symbol: str, ds: str):
        ingest_data(symbol=symbol, date_str=ds)
    ingest_spy_data(symbol="SPY")

# Instantiate the DAG
ingestion()