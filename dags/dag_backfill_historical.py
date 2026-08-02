"""
> DAG Backfill Historical Data < 
Notes:
Backfill data historis ~30 hari per coin ke MinIO, 
menggunakan endpoint /coins/{id}/market_chart. 
Dengan tujuan supaya metric moving average 7 hari dan volatility 30 hari
punya data historis representatif sejak awal pipeline berjalan.

How to use: 
trigger MANUAL sekali dari Airflow UI, cukup 1x saja.
Jangan schedule DAG ini.
"""

import json
from datetime import datetime, timezone

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from config import (
    BACKFILL_DAYS,
    BACKFILL_PREFIX,
    COIN_IDS,
    COINGECKO_BASE_URL,
    MINIO_BUCKET,
    MINIO_CONN_ID,
)


def _get_api_key() -> str:
    return Variable.get("COINGECKO_API_KEY")


def backfill_coin(coin_id: str, **context):
    api_key = _get_api_key()
    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": BACKFILL_DAYS}
    headers = {"x-cg-demo-api-key": api_key}

    response = requests.get(url, params=params, headers=headers, timeout=30)
    if response.status_code != 200:
        raise AirflowException(
            f"Gagal fetch {coin_id}: HTTP {response.status_code} - {response.text[:200]}"
        )

    data = response.json()
    extracted_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "coin_id": coin_id,
        "extracted_at": extracted_at,
        "raw_data": data,
    }

    ingestion_date = context["ds"]  
    object_key = f"{BACKFILL_PREFIX}/dt={ingestion_date}/{coin_id}.json"

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3_hook.load_string(
        string_data=json.dumps(payload),
        key=object_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )
    print(f"Backfill {coin_id} tersimpan di s3://{MINIO_BUCKET}/{object_key}")


with DAG(
    dag_id="dag_backfill_historical",
    description="One-time backfill 30 hari data historis per coin ke MinIO",
    start_date=datetime(2026, 1, 1),
    schedule=None,  
    catchup=False,
    tags=["backfill", "fase2"],
) as dag:

    for coin in COIN_IDS:
        PythonOperator(
            task_id=f"backfill_{coin}",
            python_callable=backfill_coin,
            op_kwargs={"coin_id": coin},
        )