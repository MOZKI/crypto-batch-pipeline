"""
> DAG Daily Pipeline <
Notes:
DAG ini dijalankan harian dan mencakup beberapa task: extract snapshot dari CoinGecko, land ke MinIO,
validasi dengan Great Expectations, lalu load ke Postgres Bronze.
"""

import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from config import COINGECKO_BASE_URL, DAILY_PREFIX, MINIO_BUCKET, MINIO_CONN_ID
from gx_validation import RawDataValidationError, validate_raw_snapshot
from alerting import task_failure_alert

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}

POSTGRES_CONN_ID = "postgres_dwh"

INSERT_SQL = """
INSERT INTO bronze.coingecko_market_raw (
    id, symbol, name, current_price, market_cap, market_cap_rank,
    total_volume, high_24h, low_24h, price_change_percentage_24h,
    circulating_supply, total_supply, max_supply, ath, ath_date,
    last_updated, extracted_at, ingestion_date
) VALUES (
    %(id)s, %(symbol)s, %(name)s, %(current_price)s, %(market_cap)s, %(market_cap_rank)s,
    %(total_volume)s, %(high_24h)s, %(low_24h)s, %(price_change_percentage_24h)s,
    %(circulating_supply)s, %(total_supply)s, %(max_supply)s, %(ath)s, %(ath_date)s,
    %(last_updated)s, %(extracted_at)s, %(ingestion_date)s
)
ON CONFLICT (id, ingestion_date) DO UPDATE SET
    current_price = EXCLUDED.current_price,
    market_cap = EXCLUDED.market_cap,
    market_cap_rank = EXCLUDED.market_cap_rank,
    total_volume = EXCLUDED.total_volume,
    high_24h = EXCLUDED.high_24h,
    low_24h = EXCLUDED.low_24h,
    price_change_percentage_24h = EXCLUDED.price_change_percentage_24h,
    circulating_supply = EXCLUDED.circulating_supply,
    total_supply = EXCLUDED.total_supply,
    max_supply = EXCLUDED.max_supply,
    ath = EXCLUDED.ath,
    ath_date = EXCLUDED.ath_date,
    last_updated = EXCLUDED.last_updated,
    extracted_at = EXCLUDED.extracted_at;
"""

def _get_api_key() -> str:
    return Variable.get("COINGECKO_API_KEY")

def extract_and_land(**context):
    api_key = _get_api_key()
    url = f"{COINGECKO_BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 15,
        "page": 1,
    }
    headers = {"x-cg-demo-api-key": api_key}

    response = requests.get(url, params=params, headers=headers, timeout=30)
    if response.status_code != 200:
        raise AirflowException(
            f"Gagal fetch snapshot: HTTP {response.status_code} - {response.text[:200]}"
        )

    data = response.json()
    extracted_at = datetime.now().isoformat()
    payload = {"extracted_at": extracted_at, "raw_data": data}

    ingestion_date = context["ds"]
    object_key = f"{DAILY_PREFIX}/dt={ingestion_date}/snapshot.json"

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3_hook.load_string(
        string_data=json.dumps(payload),
        key=object_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )
    print(f"Snapshot tersimpan di s3://{MINIO_BUCKET}/{object_key}")

    context["ti"].xcom_push(key="records", value=data)
    context["ti"].xcom_push(key="extracted_at", value=extracted_at)

def validate_raw(**context):
    records = context["ti"].xcom_pull(key="records", task_ids="extract_and_land")
    try:
        validate_raw_snapshot(records)
    except RawDataValidationError as e:
        raise AirflowException(f"Validasi GX gagal, load ke Bronze dibatalkan: {e}")

def load_to_bronze(**context):
    records = context["ti"].xcom_pull(key="records", task_ids="extract_and_land")
    extracted_at = context["ti"].xcom_pull(
        key="extracted_at", task_ids="extract_and_land"
    )
    ingestion_date = context["ds"]

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    for r in records:
        cursor.execute(
            INSERT_SQL,
            {
                "id": r.get("id"),
                "symbol": r.get("symbol"),
                "name": r.get("name"),
                "current_price": r.get("current_price"),
                "market_cap": r.get("market_cap"),
                "market_cap_rank": r.get("market_cap_rank"),
                "total_volume": r.get("total_volume"),
                "high_24h": r.get("high_24h"),
                "low_24h": r.get("low_24h"),
                "price_change_percentage_24h": r.get("price_change_percentage_24h"),
                "circulating_supply": r.get("circulating_supply"),
                "total_supply": r.get("total_supply"),
                "max_supply": r.get("max_supply"),
                "ath": r.get("ath"),
                "ath_date": r.get("ath_date"),
                "last_updated": r.get("last_updated"),
                "extracted_at": extracted_at,
                "ingestion_date": ingestion_date,
            },
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Berhasil load {len(records)} baris ke bronze.coingecko_market_raw")

with DAG(
    dag_id="dag_daily_pipeline",
    description="Extract, land, validate, load ke Bronze - 1 DAG gabungan harian",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fase2", "fase3", "daily"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_land",
        python_callable=extract_and_land,
    )

    validate_task = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw,
    )

    load_task = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_to_bronze,
    )

    extract_task >> validate_task >> load_task