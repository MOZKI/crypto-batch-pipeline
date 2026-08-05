# Crypto Market Monitoring Pipeline

An end-to-end batch data pipeline that extracts, cleans, and transforms daily cryptocurrency market data from the CoinGecko API to support market monitoring use cases.

## Background & Goal
This project simulates the data needs of an analyst/trading team (hypothetical stakeholder) that requires daily visibility into crypto market movements. The pipeline is built to answer three core business questions:

1. **Which coins moved significantly** (up or down) today? (top gainers/losers)
2. **Which coins are highly volatile** over a given period? (risk indicator)
3. **What is the short-to-medium term price trend** for a given coin? (moving average)

**Scope**: the top 15 coins by market cap. This is a deliberate design decision — sufficient to demonstrate pipeline depth (data quality, incremental loading, transformation logic) without the unnecessary complexity of large-scale data for a portfolio project.

## Architecture

Pattern: **ELT (Extract - Load - Transform)** using a medallion architecture (Bronze - Silver - Gold).
<img width="1600" height="900" alt="DIAGRAM" src="https://github.com/user-attachments/assets/0eda309a-4b1d-4132-b3df-18c72a809798" />
The entire flow is orchestrated and scheduled by Airflow (`@daily`), with every service running in Docker containers.

## Tech Stack

| Layer | Tool |
|---|---|
| Containerization | Docker + Docker Compose |
| Data Lake / Landing Zone | MinIO |
| Orchestration | Apache Airflow (LocalExecutor) |
| Data Warehouse | PostgreSQL |
| Transformation | dbt |
| Data Quality | Great Expectations |
| Visualization | Metabase |
| Version Control | Git / GitHub |
| Secrets Management | `.env` (Docker) + Airflow Connections/Variables |

## Project Structure

```
crypto-batch-pipeline/
├── dags/
│   ├── config.py                      # tracked coin list, MinIO constants
│   ├── alerting.py                    # lightweight on-failure alerting
│   ├── gx_validation.py               # GX validation for raw data (Bronze)
│   ├── gx_validation_gold.py          # GX validation for the Gold layer
│   ├── dag_backfill_historical.py     # one-time backfill of 30 days of history
│   ├── dag_daily_pipeline.py          # extract -> land -> validate -> load Bronze (daily)
│   └── dag_transform_dbt.py           # dbt run -> dbt test -> validate Gold (daily)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/
│   │   │   ├── silver_coin_market_cleaned.sql
│   │   │   └── schema.yml
│   │   └── gold/
│   │       ├── gold_daily_price_summary.sql
│   │       ├── gold_moving_average_7d.sql
│   │       ├── gold_volatility_metric.sql
│   │       ├── gold_top_gainers_losers.sql
│   │       └── schema.yml
│   └── tests/
│       ├── assert_volatility_not_negative.sql
│       └── assert_price_not_negative.sql
├── sql/
│   └── create_bronze_tables.sql
├── docs/
│   ├── architecture-diagram.png
│   └── dashboard-screenshot.png
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## Data Flow (Medallion Architecture)

- **Bronze**: raw daily snapshot from CoinGecko, no transformation applied. Idempotent via `PRIMARY KEY (id, ingestion_date)` combined with `ON CONFLICT DO UPDATE`.
- **Silver**: cleaned data (deduplication, column renaming, type casting, `is_supply_unlimited` flag).
- **Gold**: business-ready metrics — `gold_daily_price_summary`, `gold_moving_average_7d`, `gold_volatility_metric`, `gold_top_gainers_losers`.

> **Note**: dbt's default custom-schema naming convention concatenates the target schema with the custom schema (e.g. `public_gold`, `public_silver`) rather than using the custom schema name alone. This is reflected in the schema names actually created in Postgres.

## How to Run

### 1. Prerequisites
- Docker & Docker Compose
- Python 3 (to generate a Fernet key)
- A CoinGecko Demo API key (free — sign up at [coingecko.com](https://www.coingecko.com))

### 2. Setup
```bash
git clone https://github.com/MOZKI/crypto-batch-pipeline.git
cd crypto-batch-pipeline
cp .env.example .env
```
Fill in `.env` with real values, including a generated Fernet key:
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Start all services
```bash
docker compose up -d
```
Allow a few minutes for `great_expectations` and `dbt-postgres` to install inside the Airflow container.

### 4. One-time manual setup
- Create a `coingecko-raw` bucket in the MinIO Console (`localhost:9001`)
- Run `sql/create_bronze_tables.sql` via DBeaver (`localhost:5433`, database `dwh`)
- In the Airflow UI (`localhost:8080`):
  - Set the Airflow Variable `COINGECKO_API_KEY` (Admin -> Variables)
  - Set the Airflow Connection `postgres_dwh` (Postgres, host `postgres-dwh`)
  - Set the Airflow Connection `minio_s3` (Amazon Web Services, endpoint `http://minio:9000`)

### 5. Run the pipeline
- Trigger `dag_backfill_historical` once (manually) to populate 30 days of historical data
- `dag_daily_pipeline` and `dag_transform_dbt` run automatically on schedule (`@daily`), or can be triggered manually for testing

### 6. Access the services
| Service | URL |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Metabase | http://localhost:3000 |
| Postgres DWH (via DBeaver) | localhost:5433 |

## Dashboard

The "Crypto Market Monitoring Dashboard" in Metabase includes:
- Price Trend vs 7-Day Moving Average (interactive, filterable by coin)
- Market Cap Ranking (filterable by date)
- Top 5 Gainers (daily)
- Top 5 Losers (daily)
- Volatility Comparison Across Coins

<p align="center">
  <img width="652" height="559" alt="Screenshot 2026-08-05 at 21 19 55" src="https://github.com/user-attachments/assets/7b596287-eae5-43b0-b466-6d532a1b0852" />
</p>

## Key Design Decisions

- **ELT, not ETL**: transformation happens inside the warehouse (Postgres) via dbt, leveraging the database's own compute power instead of processing data externally.
- **Idempotency**: the Bronze layer uses `UPSERT`; the Gold layer is materialized as a `table` (full refresh on every run), making re-runs safe without duplicating data.
- **Backfill separated from incremental daily load**: the `/coins/{id}/market_chart` endpoint is used for a one-time 30-day historical backfill, while `/coins/markets` is used for the recurring daily snapshot — ensuring moving average and volatility metrics have representative data from day one.
- **Secrets management**: credentials are accessed via Airflow Connections/Variables or environment variables, never hardcoded and never read via `python-dotenv` inside DAG code (to avoid unnecessary I/O overhead on every scheduler parse cycle).
- **DAG dependency**: `dag_transform_dbt` waits for `dag_daily_pipeline` to complete via an `ExternalTaskSensor`, ensuring consistent ordering on every scheduled run.
- **Retry & alerting**: every task has automatic retries (2x, 5-minute delay) and logs a clear alert once retries are exhausted.

## Limitations & Future Work

- Current scope: 15 coins (extendable via `COIN_IDS` in `config.py`)
- Development environment: local Docker Compose (the scheduler must be running for automated runs to trigger)
- Stretch goals: migrate the Gold layer to BigQuery, deploy to a cloud VM for 24/7 scheduling, add lightweight CI/CD for `dbt test`

## Author

Mohammad Zaki Iskandar — Information Systems & Technology student at Universitas Negeri Jakarta, Data Engineering Bootcamp participant at Dibimbing.id.
