# load_duckdb.py
# Reads the processed Parquet files from data/processed/
# and loads them into DuckDB as a table called raw_combined.
#
# DuckDB acts as our local warehouse during development.
# When we move to production, this step gets replaced
# by loading into Snowflake instead.
#
# Think of DuckDB as a local Snowflake — same SQL,
# same dbt models, just runs on your laptop for free.

import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Where our DuckDB database file lives
# This is set in .env as DUCKDB_PATH
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")

# Where our processed Parquet files are
PARQUET_PATH = "data/processed/combined"


def load_parquet_to_duckdb():
    """
    Connects to DuckDB and loads all Parquet files
    into a table called raw_combined.

    DuckDB can read partitioned Parquet directories
    directly — no manual file looping needed.
    """
    print(f"Connecting to DuckDB at {DUCKDB_PATH}")

    # Connect to DuckDB — creates the file if it doesn't exist
    conn = duckdb.connect(DUCKDB_PATH)

    print(f"Reading Parquet files from {PARQUET_PATH}")

    # DuckDB reads the entire partitioned directory in one shot
    # The ** glob means "read all parquet files recursively"
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_combined AS
        SELECT *
        FROM read_parquet('{PARQUET_PATH}/**/*.parquet', hive_partitioning=true)
    """)

    # Verify the load worked
    count = conn.execute(
        "SELECT COUNT(*) FROM raw_combined"
    ).fetchone()[0]

    print(f"Loaded {count} rows into raw_combined table")

# Show a quick sample so we can verify the data looks right
    print("\nSample data:")
    result = conn.execute("""
        SELECT
            ticker,
            date,
            close_price,
            daily_return_pct,
            total_commits_today,
            total_open_prs
        FROM raw_combined
        ORDER BY ticker, date
        LIMIT 10
    """).fetchall()

    for row in result:
        print(row)

    # Show table schema
    print("\nTable schema:")
    schema = conn.execute(
        "DESCRIBE raw_combined"
    ).fetchall()
    for col in schema:
        print(col)

    conn.close()
    print("\nDuckDB load complete")


if __name__ == "__main__":
    load_parquet_to_duckdb()