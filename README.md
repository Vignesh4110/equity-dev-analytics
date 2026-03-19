# Equity & Developer Activity Analytics Pipeline

An end-to-end data engineering pipeline that ingests real-time stock market data and GitHub developer activity for major US tech companies, processes and models the data across a multi-layer warehouse architecture, and surfaces insights through an interactive Tableau dashboard.

The central analytical question: **Do spikes in developer activity at major tech companies correlate with stock price movements?**

---

## Architecture

```
Alpha Vantage API       Polygon.io API        GitHub API
      |                      |                     |
      v                      v                     v
  Python Ingestor      Python Ingestor       Python Ingestor
  (retry + idempotency checks across all three sources)
      |                      |                     |
      +----------+-----------+
                 |
                 v
         AWS S3 — Raw Zone
         (partitioned JSON)
                 |
                 v
         Great Expectations
         (data quality validation)
                 |
                 v
      PySpark Processing Job
      (clean, join, deduplicate)
                 |
                 v
       AWS S3 — Processed Zone
           (Parquet, Snappy)
                 |
                 v
        Snowflake / DuckDB
          (staging tables)
                 |
                 v
          dbt Transformations
    staging → intermediate → marts
                 |
                 v
         Tableau Dashboard
```

All steps are orchestrated by a Dockerized Apache Airflow DAG scheduled to run on weekdays after US market close (6pm UTC). Infrastructure is provisioned with Terraform. dbt tests run automatically on every push via GitHub Actions.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Requests, Tenacity |
| Cloud Storage | AWS S3 |
| Processing | PySpark 3.5 |
| Orchestration | Apache Airflow 2.8 (Dockerized) |
| Warehouse | Snowflake (prod) / DuckDB (dev) |
| Transformation | dbt Core 1.7 |
| Data Quality | Great Expectations, dbt tests |
| Infrastructure | Terraform (AWS provider) |
| CI/CD | GitHub Actions |
| Lineage | OpenLineage |
| Dashboard | Tableau Public |
| Containerization | Docker, Docker Compose |

---

## Data Sources

| Source | API | Data Collected |
|---|---|---|
| Alpha Vantage | TIME_SERIES_DAILY | Daily OHLCV prices for MSFT, GOOGL, META, AAPL, AMZN |
| Polygon.io | Aggregates v2 | Intraday trade volume, VWAP, transaction count |
| GitHub | REST API v3 | Commit counts, open PRs, stars, forks across major repos |

---

## Project Structure

```
equity-dev-analytics/
├── ingestion/
│   ├── fetch_stocks.py        # Alpha Vantage ingestion
│   ├── fetch_polygon.py       # Polygon.io ingestion
│   ├── fetch_github.py        # GitHub API ingestion
│   └── utils.py               # Shared retry, logging, S3 utilities
├── spark/
│   └── process.py             # PySpark cleaning and join job
├── dbt_project/
│   ├── models/
│   │   ├── staging/           # 1:1 source models, light cleaning
│   │   ├── intermediate/      # Business logic joins
│   │   └── marts/             # Analytics-ready fact tables
│   └── profiles.yml
├── airflow/
│   └── dags/
│       └── pipeline_dag.py    # Main orchestration DAG
├── infrastructure/
│   └── terraform/
│       ├── main.tf            # S3 buckets, IAM policy
│       ├── variables.tf
│       └── outputs.tf
├── scripts/
│   └── load_duckdb.py         # Local warehouse loader
├── .github/
│   └── workflows/
│       └── dbt_tests.yml      # CI/CD pipeline
├── docker-compose.yml
├── Dockerfile.airflow
└── requirements.txt
```

---

## dbt Model Lineage

```
raw_combined (DuckDB / Snowflake staging)
      |
      +---> stg_stock_prices
      +---> stg_polygon_trades
      +---> stg_github_activity
                  |
                  v
          int_company_daily
          (joined on ticker + date)
                  |
        +---------+---------+
        |                   |
        v                   v
mart_stock_performance  mart_github_correlation
(moving averages,       (developer activity score,
 volatility metrics)    high activity day flag)
```

---

## Pipeline DAG

```
ingest_stocks ──┐
ingest_polygon ─┼──> spark_process ──> load_duckdb ──> dbt_run ──> dbt_test
ingest_github ──┘
```

The three ingestion tasks run in parallel. Spark only starts after all three complete successfully. If any task fails, Airflow retries once after a 5-minute delay and sends an SNS alert.

---

## Key Engineering Decisions

**Idempotency** — All ingestion scripts check for existing files before fetching. Re-running the pipeline on the same day skips already-ingested data, preventing duplicate records.

**Retry logic** — API calls use exponential backoff via the Tenacity library (3 attempts, 4–16 second wait). This handles transient rate limit errors without failing the entire pipeline.

**Hive-style S3 partitioning** — Raw data lands in `s3://bucket/raw/stocks/year=2026/month=03/day=17/MSFT.json`. PySpark reads these efficiently without full bucket scans.

**Dev/prod parity** — dbt profiles support both DuckDB (local dev, free) and Snowflake (production). The same SQL models run against both without changes.

**Infrastructure as code** — All AWS resources (S3 buckets, versioning, lifecycle rules, IAM policies) are defined in Terraform. The entire cloud infrastructure can be recreated with `terraform apply`.

---

## Setup

### Prerequisites

- Python 3.10
- Docker Desktop
- AWS CLI configured (`aws configure`)
- Terraform

### Environment Variables

Create a `.env` file in the project root:

```bash
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
S3_RAW_BUCKET=equity-dev-analytics-raw
S3_PROCESSED_BUCKET=equity-dev-analytics-processed
ALPHA_VANTAGE_API_KEY=your_key
POLYGON_API_KEY=your_key
GITHUB_TOKEN=your_token
DUCKDB_PATH=data/warehouse.duckdb
```

### Install Dependencies

```bash
conda create -n de-pipeline python=3.10 -y
conda activate de-pipeline
pip install -r requirements.txt
```

### Provision AWS Infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform apply
```

### Run Pipeline Locally

```bash
# Ingest data
python -m ingestion.fetch_stocks
python -m ingestion.fetch_polygon
python -m ingestion.fetch_github

# Process with Spark
python spark/process.py

# Load to DuckDB
python scripts/load_duckdb.py

# Run dbt transformations
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Start Airflow

```bash
docker-compose up airflow-init
docker-compose up -d webserver scheduler
```

Access the Airflow UI at `http://localhost:8080` (admin / admin). Enable the `equity_dev_analytics_pipeline` DAG.

---

## Dashboard

Live Tableau dashboard:
[Equity & Developer Activity Analytics Dashboard](https://public.tableau.com/views/EquityDevAnalyticsDashboard/Dashboard1)

Charts included:
- Stock price trends with 7-day moving average (MSFT)
- Daily return percentage by company (Oct 2025 – Mar 2026)
- Developer activity score comparison across all 5 companies
- 7-day rolling volatility by company

---

## CI/CD

GitHub Actions runs dbt model compilation and data quality tests on every push to `main` and on all pull requests. A synthetic test database is created in CI to validate model logic without requiring live API credentials.

Status: ![dbt CI Tests](https://github.com/Vignesh4110/equity-dev-analytics/actions/workflows/dbt_tests.yml/badge.svg)

---

## Data Limitations

GitHub activity data is collected daily from pipeline start date. Historical GitHub data is not available via the free API tier, so the developer activity charts reflect pipeline runtime rather than full historical coverage. Stock and Polygon data provides 100 days of history via the compact API response.

---

## Author

Vigneshwaran — Data Analytics Engineering student at Northeastern University

Built as a portfolio project 
