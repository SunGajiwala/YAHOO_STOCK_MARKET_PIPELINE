import yfinance as yf
from google.cloud import storage
import pandas as pd
import datetime
import json

# Initialize GCP client
storage_client = storage.Client()
bucket_name = "stock_data_bucket_yahoo"   # make sure this bucket exists
bucket = storage_client.bucket(bucket_name)

def upload_to_gcs(df, ticker):
    # Use datetime.now() with UTC timezone instead of deprecated utcnow()
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = f"stock/{ticker}/{ts}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_json(orient="records"), content_type="application/json")
    print(f"Uploaded {blob_name}")

def fetch_and_store(ticker):
    stock = yf.Ticker(ticker)
    df = stock.history(period="1d", interval="1m").tail(1)  # latest row
    upload_to_gcs(df, ticker)

if __name__ == "__main__":
    tickers = ["AAPL", "HOOD"]

    for ticker in tickers:
        try:
            fetch_and_store(ticker)
        except Exception as e:
            print(f"❌ Failed to fetch {ticker}: {e}")
