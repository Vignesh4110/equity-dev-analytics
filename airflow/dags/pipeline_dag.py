# pipeline_dag.py
# Main Airflow DAG that orchestrates our entire pipeline.
# Runs every weekday at 6pm ET (after US market close).
#
# Task order:
# ingest_stocks → ingest_polygon → ingest_github
#       ↓
# spark_process
#       ↓
# load_duckdb
#       ↓
# dbt_run → dbt_test

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments applied to all tasks
default_args = {
    "owner": "vignesh",
    "depends_on_past": False,
    # Retry once if a task fails, wait 5 minutes before retrying
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Define the DAG
with DAG(
    dag_id="equity_dev_analytics_pipeline",
    description="Daily pipeline ingesting stock and GitHub data",
    # Run at 6pm UTC Monday to Friday (after US market close)
    schedule_interval="0 18 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["finance", "github", "data-engineering"]
) as dag:

    # ── Task 1: Ingest stock prices from Alpha Vantage ──────────────
    def run_stock_ingestion():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.fetch_stocks import run_stock_ingestion
        run_stock_ingestion()

    ingest_stocks = PythonOperator(
        task_id="ingest_stocks",
        python_callable=run_stock_ingestion,
    )

    # ── Task 2: Ingest trade data from Polygon.io ───────────────────
    def run_polygon_ingestion():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.fetch_polygon import run_polygon_ingestion
        run_polygon_ingestion()

    ingest_polygon = PythonOperator(
        task_id="ingest_polygon",
        python_callable=run_polygon_ingestion,
    )

    # ── Task 3: Ingest GitHub developer activity ────────────────────
    def run_github_ingestion():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.fetch_github import run_github_ingestion
        run_github_ingestion()

    ingest_github = PythonOperator(
        task_id="ingest_github",
        python_callable=run_github_ingestion,
    )

    # ── Task 4: Run PySpark processing job ──────────────────────────
    # We use BashOperator here because Spark needs specific
    # environment variables set before it starts
    spark_process = BashOperator(
        task_id="spark_process",
        bash_command="""
            export PYSPARK_PYTHON=/usr/local/bin/python &&
            export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python &&
            cd /opt/airflow &&
            python spark/process.py
        """,
    )

    # ── Task 5: Load processed Parquet into DuckDB ──────────────────
    def run_duckdb_load():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from scripts.load_duckdb import load_parquet_to_duckdb
        load_parquet_to_duckdb()

    load_duckdb = PythonOperator(
        task_id="load_duckdb",
        python_callable=run_duckdb_load,
    )

    # ── Task 6: Run dbt models ───────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /opt/airflow/dbt_project &&
            dbt run --profiles-dir .
        """,
    )

    # ── Task 7: Run dbt tests ────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /opt/airflow/dbt_project &&
            dbt test --profiles-dir .
        """,
    )

    # ── Define task dependencies ─────────────────────────────────────
    # Ingestion tasks run in parallel — no dependency between them
    # Spark only starts after ALL three ingestion tasks finish
    [ingest_stocks, ingest_polygon, ingest_github] >> spark_process
    spark_process >> load_duckdb
    load_duckdb >> dbt_run
    dbt_run >> dbt_test