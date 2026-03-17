# fetch_polygon.py
# Fetches daily aggregate trade data from Polygon.io API
# for the 5 major tech companies we are tracking.
#
# NOTE: Polygon.io free tier only supports previous trading day data
# Real-time data requires a paid plan
# For our portfolio project, previous day data is perfectly fine

import time
import requests
from datetime import datetime, timedelta
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
logger = get_logger("fetch_polygon")

# Same 5 companies as fetch_stocks.py
COMPANIES = ["MSFT", "GOOGL", "META", "AAPL", "AMZN"]

# Polygon.io API base URL for aggregates
BASE_URL = "https://api.polygon.io/v2/aggs/ticker"


def get_last_trading_day():
    """
    Returns the most recent weekday date as a string.
    Polygon free tier only supports previous trading day.
    Skips weekends since markets are closed.
    """
    today = datetime.today()

    # If today is Monday, go back to Friday
    if today.weekday() == 0:
        last_trading_day = today - timedelta(days=3)
    # If today is Sunday, go back to Friday
    elif today.weekday() == 6:
        last_trading_day = today - timedelta(days=2)
    # Any other day, just go back one day
    else:
        last_trading_day = today - timedelta(days=1)

    return last_trading_day.strftime("%Y-%m-%d")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=16),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def fetch_daily_aggregates(ticker, date, api_key):
    """
    Fetches daily aggregate bar data for a single ticker
    from Polygon.io for a specific date.

    Args:
        ticker: stock symbol e.g. 'MSFT'
        date: date string in YYYY-MM-DD format
        api_key: Polygon.io API key from .env

    Returns:
        dict: raw JSON response from Polygon.io
    """
    logger.info(f"Fetching Polygon data for {ticker} on {date}")

    # Build the API URL
    url = f"{BASE_URL}/{ticker}/range/1/day/{date}/{date}"

    # Request parameters
    params = {
        "adjusted": "true",
        "sort": "asc",
        "apiKey": api_key
    }

    # Make the API call
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    # Parse JSON response
    data = response.json()

    # Polygon returns status in the response body
    if data.get("status") == "ERROR":
        raise ValueError(f"Polygon error for {ticker}: {data.get('error')}")

    if data.get("resultsCount", 0) == 0:
        logger.warning(
            f"No Polygon data for {ticker} on {date} — market may be closed"
        )

    return data


def run_polygon_ingestion():
    """
    Main function that loops through all companies
    and fetches their aggregate data for the last trading day.
    """
    logger.info("Starting Polygon.io ingestion")

    # Load API key from environment
    api_key = get_env_variable("POLYGON_API_KEY")

    # Use last trading day since free tier doesn't support same-day data
    target_date = get_last_trading_day()
    logger.info(f"Fetching data for trading date: {target_date}")

    # Track results
    success_count = 0
    skip_count = 0
    error_count = 0

    for ticker in COMPANIES:

        # Build filename using the target date not today
        filename = f"{ticker}_{target_date}.json"

        # Idempotency check
        if file_already_exists("polygon", filename):
            logger.info(
                f"Already fetched Polygon data for {ticker} "
                f"on {target_date} — skipping"
            )
            skip_count += 1
            continue

        try:
            # Fetch the aggregate data
            data = fetch_daily_aggregates(ticker, target_date, api_key)

            # Add metadata
            data["_ingested_at"] = get_today()
            data["_ticker"] = ticker
            data["_trading_date"] = target_date

            # Save locally
            file_path = save_locally(data, "polygon", filename)
            logger.info(f"Saved {ticker} Polygon data to {file_path}")
            success_count += 1

            # Small delay between calls
            time.sleep(2)

        except Exception as e:
            logger.error(f"Failed to fetch Polygon data for {ticker}: {e}")
            error_count += 1

    # Summary
    logger.info(
        f"Polygon ingestion complete. "
        f"Success: {success_count}, "
        f"Skipped: {skip_count}, "
        f"Errors: {error_count}"
    )


if __name__ == "__main__":
    run_polygon_ingestion()