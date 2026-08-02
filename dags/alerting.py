"""
> Alerting for DAG <
Notes:
Dipanggil lewat on_failure_callback otomatis jalan setiap kali 
ada task yang gagal (setelah retry habis).
"""

import logging

from airflow.models import Variable

logger = logging.getLogger("airflow.task")


def task_failure_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["ds"]
    exception = context.get("exception")

    message = (
        f"[ALERT] Task GAGAL setelah semua retry habis\n"
        f"DAG       : {dag_id}\n"
        f"Task      : {task_id}\n"
        f"Tanggal   : {execution_date}\n"
        f"Error     : {exception}"
    )
    logger.error(message)

    try:
        webhook_url = Variable.get("ALERT_WEBHOOK_URL", default_var=None)
    except Exception:
        webhook_url = None

    if webhook_url:
        import requests

        try:
            requests.post(webhook_url, json={"text": message}, timeout=10)
        except Exception as e:
            logger.error(f"Gagal kirim alert ke webhook: {e}")