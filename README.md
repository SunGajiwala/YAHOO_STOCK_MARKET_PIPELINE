# Yahoo Stock Market Pipeline

![Project Logo](https://via.placeholder.com/150)

## Overview

The **Yahoo Stock Market Pipeline** is an end-to-end data engineering project designed to automate the extraction, transformation, and loading (ETL) of real-time stock market data into Google Cloud Platform (GCP). Leveraging tools like Apache Airflow, Google Cloud Storage, BigQuery, and the Yahoo Finance API, this pipeline facilitates scalable and efficient data processing for financial analytics.

## Architecture

![Architecture Diagram](https://via.placeholder.com/800x400)

The pipeline follows a medallion architecture:

* **Raw Zone**: Ingests raw stock data from the Yahoo Finance API.
* **Curated Zone**: Transforms raw data into Parquet format for optimized storage.
* **Analytics Zone**: Loads curated data into BigQuery for analysis and visualization.

## Technologies Used

* **Data Ingestion**: [yfinance](https://pypi.org/project/yfinance/) (Yahoo Finance API)
* **Orchestration**: [Apache Airflow](https://airflow.apache.org/)
* **Storage**: [Google Cloud Storage](https://cloud.google.com/storage)
* **Data Warehousing**: [Google BigQuery](https://cloud.google.com/bigquery)
* **Data Transformation**: [Pandas](https://pandas.pydata.org/), [Parquet](https://parquet.apache.org/)
* **Visualization**: [Google Looker Studio](https://lookerstudio.google.com/)

## Pipeline Workflow

1. **Data Extraction**: Fetches minute-level stock data for specified tickers (e.g., AAPL, HOOD) using the Yahoo Finance API.
2. **Data Storage**: Stores raw JSON data in Google Cloud Storage.
3. **Data Transformation**: Converts JSON data to Parquet format for efficient storage and processing.
4. **Data Loading**: Loads transformed data into BigQuery for analytics.
5. **Scheduling**: Utilizes Apache Airflow to schedule and manage the ETL workflow.

## Getting Started

### Prerequisites

* Python 3.12+
* Apache Airflow 2.7+
* Google Cloud SDK
* GCP Project with BigQuery and Cloud Storage enabled
* Service Account with appropriate IAM roles

### Setup Instructions

1. Clone the repository:

   ```bash
   git clone https://github.com/SunGajiwala/YAHOO_STOCK_MARKET_PIPELINE.git
   cd YAHOO_STOCK_MARKET_PIPELINE
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure Airflow connections for GCP and Yahoo Finance API.

4. Deploy the DAGs in Airflow.

5. Trigger the DAGs manually or set up a schedule.

## Data Schema

The BigQuery dataset `stock_analytics` contains the following table:

* `fact_stock_prices`: Stores minute-level stock price data with the following schema:

  | Field     | Type      |
  | --------- | --------- |
  | timestamp | TIMESTAMP |
  | ticker    | STRING    |
  | open      | FLOAT64   |
  | high      | FLOAT64   |
  | low       | FLOAT64   |
  | close     | FLOAT64   |
  | volume    | INT64     |

## Visualizations

Dashboards have been created in Google Looker Studio to visualize:

* Stock price trends over time
* OHLC (Open, High, Low, Close) patterns
* Volume analysis

## Future Enhancements

* Implement dbt for data transformations.
* Integrate real-time data streaming.
* Add anomaly detection for stock price fluctuations.
* Expand to include additional financial indicators.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

* Inspired by various open-source data pipeline projects.
* Thanks to the contributors of the yfinance library for providing easy access to Yahoo Finance data.
