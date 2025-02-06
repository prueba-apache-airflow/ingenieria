from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import prod.utils as utils

"""
__author__ = Team Operacion del dato  
__description__ = Modulo que elimina archivos log con antiguedad mayor a x dias
"""

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    "log_cleanup",
    description="Elimina archivos log de tareas antiguas",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=utils.get_format_date('Thu, 17 Oct 2024 00:00:00 GMT'),
    catchup=False,
    tags=["config_core"],
)

delete_files_log = BashOperator(
    task_id='remove_log_files',
    bash_command="""
        find /opt/airflow/logs -type f -mtime +30 -delete
    """,
    dag=dag
    )

delete_folder_log = BashOperator(
    task_id='remove_folder_empty',
    bash_command="""
        find /opt/airflow/logs -type d -empty -delete
    """,
    dag=dag
    )

delete_files_log >> delete_folder_log