# process.py
# PySpark job that reads raw JSON files from all three sources,
# cleans and standardizes the data, joins everything together
# on company ticker + date, and writes clean Parquet files
# to data/processed/
#
# This is the "T" in our ELT pipeline — Transform.
#
# Input:  data/raw/stocks/    → Alpha Vantage daily prices
#         data/raw/polygon/   → Polygon intraday aggregates
#         data/raw/github/    → GitHub developer activity
#
# Output: data/processed/combined/ → joined, clean Parquet files

import os

# Fix: tell Spark which Python to use so it matches our conda environment
# Without this, Spark workers pick up system Python instead of our venv
os.environ["PYSPARK_PYTHON"] = "/opt/miniconda3/envs/de-pipeline/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/miniconda3/envs/de-pipeline/bin/python"


import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("spark_processor")


def create_spark_session():
    """
    Creates and returns a local Spark session.
    We use local mode for development — in production
    this would point to a real Spark cluster on EC2.
    """
    spark = SparkSession.builder \
        .appName("equity-dev-analytics-processor") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "2g") \
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        ) \
        .getOrCreate()

    # Reduce Spark log noise during development
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Spark session created — version {spark.version}")
    return spark


def read_stocks_data(spark, raw_path):
    """
    Reads all Alpha Vantage stock JSON files and flattens
    the nested time series structure into a clean flat table.

    Raw structure:
        {
            "Meta Data": {...},
            "Time Series (Daily)": {
                "2026-03-17": {
                    "1. open": "399.66",
                    "2. high": "404.40",
                    ...
                }
            },
            "_ticker": "MSFT"
        }

    Output columns:
        ticker, date, open, high, low, close, volume
    """
    logger.info("Reading stocks data")

    stocks_path = str(Path(raw_path) / "stocks" / "*.json")

    # Read all stock JSON files at once
    raw_df = spark.read.option("multiline", "true").json(stocks_path)

    # The time series data is deeply nested — we need to flatten it
    # We do this by reading files one at a time with Python
    # and building a list of rows, then creating a DataFrame
    import json
    import glob

    rows = []
    files = glob.glob(str(Path(raw_path) / "stocks" / "*.json"))

    for file_path in files:
        with open(file_path) as f:
            data = json.load(f)

        ticker = data.get("_ticker", "")
        time_series = data.get("Time Series (Daily)", {})

        for date, values in time_series.items():
            rows.append({
                "ticker": ticker,
                "date": date,
                "open_price": float(values.get("1. open", 0)),
                "high_price": float(values.get("2. high", 0)),
                "low_price": float(values.get("3. low", 0)),
                "close_price": float(values.get("4. close", 0)),
                "volume": int(values.get("5. volume", 0))
            })

    # Convert to Spark DataFrame
    if not rows:
        logger.warning("No stock data rows found")
        return None

    stocks_df = spark.createDataFrame(rows)

    # Cast date column to proper date type
    stocks_df = stocks_df.withColumn(
        "date",
        F.to_date(F.col("date"), "yyyy-MM-dd")
    )

    # Add daily return percentage
    # We use a window function to get previous day's close
    from pyspark.sql.window import Window

    window_spec = Window \
        .partitionBy("ticker") \
        .orderBy("date")

    stocks_df = stocks_df.withColumn(
        "prev_close",
        F.lag("close_price", 1).over(window_spec)
    ).withColumn(
        "daily_return_pct",
        F.round(
            ((F.col("close_price") - F.col("prev_close"))
             / F.col("prev_close")) * 100,
            4
        )
    ).drop("prev_close")

    logger.info(
        f"Stocks data loaded — "
        f"{stocks_df.count()} rows, "
        f"{len(stocks_df.columns)} columns"
    )

    return stocks_df


def read_polygon_data(spark, raw_path):
    """
    Reads Polygon.io aggregate JSON files and extracts
    OHLCV plus additional metrics like VWAP and transactions.

    Output columns:
        ticker, date, open, high, low, close,
        volume, vwap, transactions
    """
    logger.info("Reading Polygon data")

    import json
    import glob

    rows = []
    files = glob.glob(str(Path(raw_path) / "polygon" / "*.json"))

    for file_path in files:
        with open(file_path) as f:
            data = json.load(f)

        ticker = data.get("_ticker", "")
        trading_date = data.get("_trading_date", "")
        results = data.get("results", [])

        # Polygon returns results as a list — usually just one item
        # for a daily aggregate
        for result in results:
            rows.append({
                "ticker": ticker,
                "date": trading_date,
                "poly_open": float(result.get("o", 0)),
                "poly_high": float(result.get("h", 0)),
                "poly_low": float(result.get("l", 0)),
                "poly_close": float(result.get("c", 0)),
                "poly_volume": float(result.get("v", 0)),
                "vwap": float(result.get("vw", 0)),
                "transactions": int(result.get("n", 0))
            })

    if not rows:
        logger.warning("No Polygon data rows found")
        return None

    polygon_df = spark.createDataFrame(rows)

    polygon_df = polygon_df.withColumn(
        "date",
        F.to_date(F.col("date"), "yyyy-MM-dd")
    )

    logger.info(
        f"Polygon data loaded — "
        f"{polygon_df.count()} rows, "
        f"{len(polygon_df.columns)} columns"
    )

    return polygon_df


def read_github_data(spark, raw_path):
    """
    Reads GitHub activity JSON files.
    These are already flat since we aggregated them
    during ingestion — easy to load directly.

    Output columns:
        ticker, date, total_commits_today,
        total_open_prs, total_stars, total_forks
    """
    logger.info("Reading GitHub data")

    import json
    import glob

    rows = []
    files = glob.glob(str(Path(raw_path) / "github" / "*.json"))

    for file_path in files:
        with open(file_path) as f:
            data = json.load(f)

        rows.append({
            "ticker": data.get("ticker", ""),
            "date": data.get("date", ""),
            "total_commits_today": int(
                data.get("total_commits_today", 0)
            ),
            "total_open_prs": int(
                data.get("total_open_prs", 0)
            ),
            "total_stars": int(
                data.get("total_stars", 0)
            ),
            "total_forks": int(
                data.get("total_forks", 0)
            ),
            "repos_tracked": int(
                data.get("repos_tracked", 0)
            )
        })

    if not rows:
        logger.warning("No GitHub data rows found")
        return None

    github_df = spark.createDataFrame(rows)

    github_df = github_df.withColumn(
        "date",
        F.to_date(F.col("date"), "yyyy-MM-dd")
    )

    logger.info(
        f"GitHub data loaded — "
        f"{github_df.count()} rows, "
        f"{len(github_df.columns)} columns"
    )

    return github_df


def join_all_sources(stocks_df, polygon_df, github_df):
    """
    Joins all three DataFrames on ticker + date.

    We use a left join from stocks as the base because:
    - Stocks data has the most complete date coverage
    - Polygon data may be missing for market holidays
    - GitHub data is daily so should match stocks dates

    The result is one wide table with all metrics
    for each company on each trading day.
    """
    logger.info("Joining all three data sources")

    # Start with stocks as the base
    combined_df = stocks_df

    # Left join Polygon data
    if polygon_df is not None:
        combined_df = combined_df.join(
            polygon_df,
            on=["ticker", "date"],
            how="left"
        )

    # Left join GitHub data
    if github_df is not None:
        combined_df = combined_df.join(
            github_df,
            on=["ticker", "date"],
            how="left"
        )

    # Add a processing timestamp so we know when this ran
    combined_df = combined_df.withColumn(
        "processed_at",
        F.current_timestamp()
    )

    # Sort by ticker and date for readability
    combined_df = combined_df.orderBy("ticker", "date")

    logger.info(
        f"Join complete — "
        f"{combined_df.count()} rows, "
        f"{len(combined_df.columns)} columns"
    )

    return combined_df


def write_processed_data(combined_df, processed_path):
    """
    Writes the final joined DataFrame to Parquet format.
    Partitioned by ticker so queries on a single company are fast.

    Parquet is the standard format for analytical workloads —
    it's columnar, compressed, and much faster than CSV or JSON.
    """
    output_path = str(Path(processed_path) / "combined")

    logger.info(f"Writing processed data to {output_path}")

    combined_df.write \
        .mode("overwrite") \
        .partitionBy("ticker") \
        .parquet(output_path)

    logger.info("Processed data written successfully")


def run_processing():
    """
    Main function that orchestrates the full processing job.
    Called by Airflow after ingestion completes.
    """
    logger.info("Starting Spark processing job")

    # Define paths
    raw_path = "data/raw"
    processed_path = "data/processed"

    # Create Spark session
    spark = create_spark_session()

    try:
        # Read all three sources
        stocks_df = read_stocks_data(spark, raw_path)
        polygon_df = read_polygon_data(spark, raw_path)
        github_df = read_github_data(spark, raw_path)

        # Make sure we have at least stocks data
        if stocks_df is None:
            logger.error("No stocks data found — cannot continue")
            return

        # Join everything together
        combined_df = join_all_sources(
            stocks_df, polygon_df, github_df
        )

        # Show a sample of what we built
        logger.info("Sample of processed data:")
        combined_df.select(
            "ticker", "date", "close_price",
            "daily_return_pct", "vwap",
            "total_commits_today", "total_open_prs"
        ).show(10, truncate=False)

        # Write to Parquet
        write_processed_data(combined_df, processed_path)

        logger.info("Spark processing job completed successfully")

    finally:
        # Always stop the Spark session cleanly
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    run_processing()