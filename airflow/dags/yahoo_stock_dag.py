# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta

# # DAG Configuration
# default_args = {
#     'owner': 'sun',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2025, 8, 22),
#     'catchup': False
# }

# # Stock tickers to process
# tickers = ["AAPL","HOOD"]

# def fetch_and_store_stock(ticker):
#     """Fetch and store stock data - imports done inside function to avoid DAG parsing issues"""
#     import yfinance as yf
#     from google.cloud import storage
#     import datetime
#     import pandas as pd
    
#     # Initialize GCP client
#     storage_client = storage.Client()
#     bucket_name = "stock_data_bucket_yahoo"
#     bucket = storage_client.bucket(bucket_name)
    
#     def upload_to_gcs(df, ticker):
#         ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
#         blob_name = f"stock/{ticker}/{ts}.json"
#         blob = bucket.blob(blob_name)
#         blob.upload_from_string(df.to_json(orient="records"), content_type="application/json")
#         print(f"Uploaded {blob_name}")
    
#     # Fetch stock data
#     stock = yf.Ticker(ticker)
#     df = stock.history(period="1d", interval="1m").tail(1)  # latest row
#     upload_to_gcs(df, ticker)
#     return f"Successfully processed {ticker}"

# # Create DAG
# with DAG(
#     'yahoo_stock_pipeline',
#     default_args=default_args,
#     description='Fetch and store Yahoo Finance stock data',
#     schedule='@hourly',
#     tags=['stocks', 'yfinance', 'gcp']
# ) as dag:

#     # Create tasks dynamically for each ticker
#     for ticker in tickers:
#         PythonOperator(
#             task_id=f'fetch_{ticker.lower()}',
#             python_callable=fetch_and_store_stock,
#             op_kwargs={'ticker': ticker},
#             dag=dag
#         ) 


import os
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
tickers = ["AAPL", "HOOD"]

def fetch_and_store_stock(ticker, **kwargs):
    """Fetch stock data and store to RAW zone in GCS"""
    import yfinance as yf
    from google.cloud import storage
    import datetime
    import pandas as pd

    storage_client = storage.Client()
    bucket = storage_client.bucket("stock_data_bucket_yahoo")

    now = datetime.datetime.now(datetime.timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%d_%H%M%S")

    stock = yf.Ticker(ticker)
    df = stock.history(period="1d", interval="1m").tail(1)

    raw_blob_name = f"raw/stock/{ticker}/{date_str}/{ts_str}.json"
    blob = bucket.blob(raw_blob_name)
    blob.upload_from_string(df.to_json(orient="records"), content_type="application/json")
    print(f"Uploaded RAW file: {raw_blob_name}")

    # Push the raw_blob_name to XCom
    kwargs['ti'].xcom_push(key='raw_blob_name', value=raw_blob_name)
    return raw_blob_name

def transform_to_parquet(ticker, **kwargs):
    """Transform JSON (RAW) to Parquet (CURATED)"""
    from google.cloud import storage
    import pandas as pd
    import io
    import datetime

    ti = kwargs['ti']
    raw_blob_name = ti.xcom_pull(key='raw_blob_name', task_ids=f'fetch_{ticker.lower()}')

    storage_client = storage.Client()
    bucket = storage_client.bucket("stock_data_bucket_yahoo")

    raw_blob = bucket.blob(raw_blob_name)
    raw_data = raw_blob.download_as_text()
    df = pd.read_json(io.StringIO(raw_data), orient="records")

    now = datetime.datetime.now(datetime.timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%d_%H%M%S")

    curated_blob_name = f"curated/stock/{ticker}/{date_str}/{ts_str}.parquet"
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    curated_blob = bucket.blob(curated_blob_name)
    curated_blob.upload_from_string(parquet_buffer.getvalue(), content_type="application/octet-stream")
    print(f"Uploaded CURATED file: {curated_blob_name}")

    # Push curated_blob_name to XCom
    ti.xcom_push(key='curated_blob_name', value=curated_blob_name)
    return curated_blob_name

def load_to_bigquery(ticker, **kwargs):
    """Load Curated Parquet from GCS into BigQuery"""
    from google.cloud import bigquery

    ti = kwargs['ti']
    curated_blob_name = ti.xcom_pull(key='curated_blob_name', task_ids=f'transform_{ticker.lower()}')

    client = bigquery.Client()
    table_id = "yahoo-stock-market-pipeline.stock_analytics.fact_stock_prices"
    uri = f"gs://stock_data_bucket_yahoo/{curated_blob_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Waits for job to finish
    print(f"Loaded {uri} into BigQuery table {table_id}")

# Create DAG
with DAG(
    'yahoo_stock_pipeline',
    default_args=default_args,
    description='Fetch, transform, and load Yahoo Finance stock data',
    schedule='@hourly',
    tags=['stocks', 'yfinance', 'gcp']
) as dag:

    for ticker in tickers:
        fetch_task = PythonOperator(
            task_id=f'fetch_{ticker.lower()}',
            python_callable=fetch_and_store_stock,
            op_kwargs={'ticker': ticker}
        )

        transform_task = PythonOperator(
            task_id=f'transform_{ticker.lower()}',
            python_callable=transform_to_parquet,
            op_kwargs={'ticker': ticker}
        )

        load_task = PythonOperator(
            task_id=f'load_{ticker.lower()}_to_bigquery',
            python_callable=load_to_bigquery,
            op_kwargs={'ticker': ticker}
        )

        # Define task order: fetch → transform → load
        fetch_task >> transform_task >> load_task
