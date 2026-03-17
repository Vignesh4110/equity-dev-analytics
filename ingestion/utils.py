# utils.py
# Shared helper functions used by all ingestion scripts.
# Things like loading environment variables, setting up S3 connections,
# and saving data locally during development.

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging so we can see what's happening when scripts run
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def get_logger(name):
    """
    Returns a logger with the given name.
    Use this in every ingestion script instead of print statements.
    """
    return logging.getLogger(name)


def get_env_variable(var_name):
    """
    Safely loads an environment variable.
    Raises a clear error if it's missing instead of failing silently.
    """
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def save_locally(data, folder, filename):
    """
    Saves raw JSON data to local data/raw/ folder during development.
    Later we'll swap this to save to S3 instead.
    
    Args:
        data: Python dict or list to save as JSON
        folder: subfolder inside data/raw/ e.g. 'stocks' or 'github'
        filename: name of the file e.g. 'AAPL_2024-01-15.json'
    """
    # Build the full path
    output_dir = Path("data/raw") / folder
    
    # Create the folder if it doesn't exist yet
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Build full file path
    file_path = output_dir / filename
    
    # Write the data as formatted JSON so it's readable
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
    
    return file_path


def file_already_exists(folder, filename):
    """
    Checks if we already ingested this file today.
    This is our idempotency check — don't re-ingest what already exists.
    
    Args:
        folder: subfolder inside data/raw/
        filename: filename to check
    
    Returns:
        True if file exists, False if it doesn't
    """
    file_path = Path("data/raw") / folder / filename
    return file_path.exists()


def get_today():
    """
    Returns today's date as a string in YYYY-MM-DD format.
    Used for naming files and partitioning data.
    """
    return datetime.today().strftime("%Y-%m-%d")