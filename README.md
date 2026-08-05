# Crypto Market Monitoring Pipeline

Batch data pipeline end-to-end yang mengumpulkan, membersihkan, dan mentransformasi data pasar cryptocurrency harian dari CoinGecko API, untuk mendukung kebutuhan monitoring pasar sehari-hari.

## Latar Belakang & Tujuan

Project ini dibangun sebagai portfolio data engineering, mensimulasikan kebutuhan monitoring pasar bagi tim analis/trader (stakeholder hipotetis). Pipeline menjawab 3 pertanyaan bisnis utama:

1. **Coin mana yang paling naik/turun signifikan** pada suatu hari (top gainers/losers)?
2. **Coin mana yang volatilitasnya tinggi** dalam periode tertentu (indikator risiko)?
3. **Bagaimana tren harga** suatu coin dalam 7/30 hari terakhir (moving average)?

Scope: 15 coin dengan market cap terbesar. Keputusan scope ini disengaja — cukup untuk mendemonstrasikan pipeline logic secara mendalam (data quality, incremental load, transformasi) tanpa kompleksitas data volume besar yang tidak perlu untuk skala portfolio.

## Arsitektur

Pola: **ELT (Extract - Load - Transform)** dengan medallion architecture (Bronze - Silver - Gold).

```
CoinGecko API
  -> EXTRACT (Airflow hit API)
  -> LAND (raw data JSON ke MinIO, partisi by date)
  -> VALIDATE (Great Expectations: schema, null check, tipe data)
  -> LOAD (raw data ke Postgres Bronze, as-is)
  -> TRANSFORM (DBT: Bronze -> Silver -> Gold)
  -> VALIDATE (Great Expectations: business logic di Gold layer)
  -> VISUALIZE (Metabase, dashboard interaktif)
```

Seluruh proses di-orchestrate dan dijadwalkan oleh Airflow (`@daily`), dan seluruh service dijalankan dalam container Docker.

## Tech Stack

| Layer | Tools |
|---|---|
| Containerization | Docker + docker-compose |
| Data Lake / Landing Zone | MinIO |
| Orchestration | Apache Airflow (LocalExecutor) |
| Data Warehouse | PostgreSQL |
| Transformation | DBT |
| Data Quality | Great Expectations |
| Visualization | Metabase |
| Version Control | Git/GitHub |
| Secrets Management | `.env` (Docker) + Airflow Connections/Variables |

## Struktur Project

```
crypto-batch-pipeline/
├── dags/
│   ├── config.py                      # daftar coin, konstanta MinIO
│   ├── alerting.py                    # alerting sederhana on task failure
│   ├── gx_validation.py               # validasi GX untuk raw data (Bronze)
│   ├── gx_validation_gold.py          # validasi GX untuk Gold layer
│   ├── dag_backfill_historical.py     # one-time backfill 30 hari historis
│   ├── dag_daily_pipeline.py          # extract -> land -> validate -> load Bronze (harian)
│   └── dag_transform_dbt.py           # dbt run -> dbt test -> validate Gold (harian)
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
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## Alur Data (Medallion)

- **Bronze**: raw snapshot harian dari CoinGecko, tanpa transformasi. Idempotent lewat `PRIMARY KEY (id, ingestion_date)` + `ON CONFLICT DO UPDATE`.
- **Silver**: data yang sudah dibersihkan (dedup, rename kolom, type casting, flag `is_supply_unlimited`).
- **Gold**: metric business-ready — `gold_daily_price_summary`, `gold_moving_average_7d`, `gold_volatility_metric`, `gold_top_gainers_losers`.

## Cara Menjalankan

### 1. Prasyarat
- Docker & Docker Compose
- Python 3 (untuk generate Fernet key)
- API key CoinGecko (Demo, gratis — daftar di [coingecko.com](https://www.coingecko.com))

### 2. Setup
```bash
git clone <repo-url>
cd crypto-batch-pipeline
cp .env.example .env
```
Isi `.env` dengan value asli, termasuk generate Fernet key:
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Jalankan seluruh service
```bash
docker compose up -d
```
Tunggu beberapa menit (instalasi `great_expectations`, `dbt-postgres` di dalam container Airflow).

### 4. Setup manual (sekali di awal)
- Bikin bucket `coingecko-raw` di MinIO Console (`localhost:9001`)
- Jalankan `sql/create_bronze_tables.sql` lewat DBeaver (`localhost:5433`, database `dwh`)
- Di Airflow UI (`localhost:8080`):
  - Set Airflow Variable `COINGECKO_API_KEY` (Admin -> Variables)
  - Set Airflow Connection `postgres_dwh` (Postgres, host `postgres-dwh`)
  - Set Airflow Connection `minio_s3` (Amazon Web Services, endpoint `http://minio:9000`)

### 5. Jalankan pipeline
- Trigger `dag_backfill_historical` sekali (manual) untuk mengisi data historis 30 hari
- `dag_daily_pipeline` dan `dag_transform_dbt` berjalan otomatis sesuai schedule (`@daily`), atau bisa di-trigger manual untuk testing

### 6. Akses service
| Service | URL |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Metabase | http://localhost:3000 |
| Postgres DWH (DBeaver) | localhost:5433 |

## Dashboard

Dashboard "Crypto Market Monitoring" di Metabase berisi:
1. Top 5 Gainers Harian
2. Top 5 Losers Harian
3. Price Trend vs Moving Average 7 Hari (interaktif, filter per coin)
4. Volatility Comparison Antar Coin
5. Market Cap Ranking Harian

*(Screenshot dashboard: lihat `docs/dashboard-screenshot.png`)*

## Desain Keputusan Penting

- **ELT, bukan ETL**: transformasi dilakukan di dalam warehouse (Postgres) menggunakan DBT, memanfaatkan kekuatan komputasi database.
- **Idempotency**: Bronze layer menggunakan `UPSERT`; Gold layer di-materialize sebagai `table` (full refresh tiap run), sehingga aman di-run ulang tanpa duplikasi data.
- **Backfill terpisah dari incremental daily load**: endpoint `/coins/{id}/market_chart` untuk histori 30 hari (one-time), endpoint `/coins/markets` untuk snapshot harian (recurring) — memastikan moving average dan volatility punya data representatif sejak awal.
- **Secrets management**: kredensial diakses lewat Airflow Connections/Variables atau environment variable, bukan hardcode maupun `python-dotenv` di dalam kode DAG (menghindari overhead re-parsing scheduler).
- **DAG dependency**: `dag_transform_dbt` menunggu `dag_daily_pipeline` selesai lewat `ExternalTaskSensor`, memastikan urutan proses yang konsisten pada setiap scheduled run.
- **Retry & alerting**: setiap task punya retry otomatis (2x, jeda 5 menit) dan alert log ketika gagal setelah retry habis.

## Batasan & Pengembangan Selanjutnya

- Scope saat ini: 15 coin (dapat di-extend lewat `COIN_IDS` di `config.py`)
- Development environment: local Docker Compose (scheduler perlu aktif untuk automated run)
- Stretch goal: migrasi Gold layer ke BigQuery, deployment ke Cloud VM untuk scheduling 24/7, CI/CD sederhana untuk `dbt test`
