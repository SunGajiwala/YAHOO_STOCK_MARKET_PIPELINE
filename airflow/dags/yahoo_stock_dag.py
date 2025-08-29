from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# DAG Configuration
default_args = {
    'owner': 'sun',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 8, 22),
    'catchup': False
}

# Stock tickers to process
tickers = ["AAPL","HOOD"]

def fetch_and_store_stock(ticker):
    """Fetch and store stock data - imports done inside function to avoid DAG parsing issues"""
    import yfinance as yf
    from google.cloud import storage
    import datetime
    import pandas as pd
    
    # Initialize GCP client
    storage_client = storage.Client()
    bucket_name = "stock_data_bucket_yahoo"
    bucket = storage_client.bucket(bucket_name)
    
    def upload_to_gcs(df, ticker):
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        blob_name = f"stock/{ticker}/{ts}.json"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(df.to_json(orient="records"), content_type="application/json")
        print(f"Uploaded {blob_name}")
    
    # Fetch stock data
    stock = yf.Ticker(ticker)
    df = stock.history(period="1d", interval="1m").tail(1)  # latest row
    upload_to_gcs(df, ticker)
    return f"Successfully processed {ticker}"

# Create DAG
with DAG(
    'yahoo_stock_pipeline',
    default_args=default_args,
    description='Fetch and store Yahoo Finance stock data',
    schedule='@hourly',
    tags=['stocks', 'yfinance', 'gcp']
) as dag:

    # Create tasks dynamically for each ticker
    for ticker in tickers:
        PythonOperator(
            task_id=f'fetch_{ticker.lower()}',
            python_callable=fetch_and_store_stock,
            op_kwargs={'ticker': ticker},
            dag=dag
        ) 