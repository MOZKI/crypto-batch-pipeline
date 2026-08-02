"""
> DAG Transform DBT <
Notes:
DAG untuk mentransform data dari Bronze -> Silver -> Gold menggunakan dbt, 
lalu dbt test, dan validasi Gold layer dengan Great Expectations. Dijalankan harian
jika Bronze layer sudah terisi (task load_to_bronze di dag_daily_pipeline selesai).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from alerting import task_failure_alert
from gx_validation_gold import GoldValidationError, validate_gold_layer

DBT_PROJECT_DIR = "/opt/airflow/dbt"

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}


def _validate_gold(**context):
    try:
        validate_gold_layer(postgres_conn_id="postgres_dwh")
    except GoldValidationError as e:
        raise AirflowException(f"Validasi GX Gold layer gagal: {e}")


with DAG(
    dag_id="dag_transform_dbt",
    description="Transform Bronze -> Silver -> Gold pakai DBT, dbt test, lalu validasi GX Gold",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fase4", "fase5", "fase6", "transform", "daily"],
) as dag:

    wait_for_bronze_load = ExternalTaskSensor(
        task_id="wait_for_bronze_load",
        external_dag_id="dag_daily_pipeline",
        external_task_id="load_to_bronze",
        timeout=600,
        poke_interval=30,
        mode="reschedule",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=_validate_gold,
    )

    wait_for_bronze_load >> dbt_run >> dbt_test >> validate_gold