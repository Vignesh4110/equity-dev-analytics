# fetch_stocks.py
# Fetches daily stock price data from Alpha Vantage API
# for the 5 major tech companies we are tracking.
#
# What this script does:
# 1. Loops through each company ticker
# 2. Checks if we already fetched today's data (idempotency)
# 3. Hits the Alpha Vantage API with retry logic
# 4. Saves raw JSON response to data/raw/stocks/

import time
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
import logging

from ingestion.utils import (
    get_logger,
    get_env_variable,
    save_locally,
    file_already_exists,
    get_today
)

# Set up logger for this script
logger = get_logger("fetch_stocks")

# The 5 tech companies we are tracking
# These map to their public GitHub organizations as well
COMPANIES = ["MSFT", "GOOGL", "META", "AAPL", "AMZN"]

# Alpha Vantage API base URL
BASE_URL = "https://www.alphavantage.co/query"


@retry(
    # Retry up to 3 times if the API call fails
    stop=stop_after_attempt(3),
    # Wait 4 seconds on first retry, then doubles each time (4s, 8s, 16s)
    wait=wait_exponential(multiplier=1, min=4, max=16),
    # Log a message before each retry so we know what's happening
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def fetch_daily_prices(ticker, api_key):
    """
    Fetches daily OHLCV (Open, High, Low, Close, Volume) price data
    for a single stock ticker from Alpha Vantage.

    Args:
        ticker: stock symbol e.g. 'MSFT'
        api_key: Alpha Vantage API key from .env

    Returns:
        dict: raw JSON response from Alpha Vantage
    """
    logger.info(f"Fetching stock data for {ticker}")

    # Build the API request parameters
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",  # Returns last 100 data points
        "apikey": api_key
    }

    # Make the API call
    response = requests.get(BASE_URL, params=params, timeout=30)

    # Raise an error if the response was not successful
    response.raise_for_status()

    # Parse the JSON response
    data = response.json()

    # Alpha Vantage returns an error message inside the JSON
    # instead of using HTTP error codes — we need to check for this
    if "Error Message" in data:
        raise ValueError(f"Alpha Vantage error for {ticker}: {data['Error Message']}")

    if "Note" in data:
        # This means we hit the API rate limit (25 calls/day on free tier)
        logger.warning(f"Alpha Vantage rate limit hit: {data['Note']}")
        raise Exception("Rate limit reached")

    return data


def run_stock_ingestion():
    """
    Main function that loops through all companies
    and fetches their stock data for today.
    """
    logger.info("Starting stock price ingestion")

    # Load API key from environment
    api_key = get_env_variable("ALPHA_VANTAGE_API_KEY")

    # Track results
    success_count = 0
    skip_count = 0
    error_count = 0

    for ticker in COMPANIES:

        # Build filename for today's data
        today = get_today()
        filename = f"{ticker}_{today}.json"

        # Idempotency check — skip if already fetched today
        if file_already_exists("stocks", filename):
            logger.info(f"Already fetched {ticker} for {today} — skipping")
            skip_count += 1
            continue

        try:
            # Fetch the data
            data = fetch_daily_prices(ticker, api_key)

            # Add metadata to the raw file so we know when it was ingested
            data["_ingested_at"] = today
            data["_ticker"] = ticker

            # Save to local data/raw/stocks/
            file_path = save_locally(data, "stocks", filename)
            logger.info(f"Saved {ticker} data to {file_path}")
            success_count += 1

            # Alpha Vantage free tier allows 25 requests per day
            # but rate limits to ~5 per minute — wait between calls
            time.sleep(12)

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
            error_count += 1

    # Summary log at the end
    logger.info(
        f"Stock ingestion complete. "
        f"Success: {success_count}, "
        f"Skipped: {skip_count}, "
        f"Errors: {error_count}"
    )


# This allows the script to be run directly
# OR imported and called by Airflow
if __name__ == "__main__":
    run_stock_ingestion()