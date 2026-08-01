"""
Modul alerting sederhana untuk semua DAG.
Dipanggil lewat on_failure_callback -- otomatis jalan setiap kali
ada task yang gagal (setelah retry habis).

Default: nulis alert jelas ke Airflow log (selalu bisa dicek dari UI).
Opsional: kirim ke Slack/Discord webhook kalau AIRFLOW_VAR_ALERT_WEBHOOK_URL
di-set sebagai Airflow Variable (kalau tidak di-set, bagian ini di-skip).
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

    # Selalu muncul di Airflow log (Admin -> bisa dicek lewat log task manapun
    # atau lewat 'docker compose logs airflow-scheduler')
    logger.error(message)

    # Opsional: kirim ke webhook kalau ada yang di-set
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