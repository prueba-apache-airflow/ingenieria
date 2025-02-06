from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import prod.utils as utils

"""
__author__ = Team Operacion del dato  
__description__ = Modulo que ejecuta la funcion de eliminacion de archivos json desde backup
"""

dag_args = {
    "depends_on_past": False,
    "email": ["test@test.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10), 
}

dag = DAG(
    "clean_old_files_json",
    description="Limpia o elimina archivos que superaron el tiempo establecido en backup",
    default_args=dag_args,
    schedule_interval='0 0 * * *',
    start_date=utils.get_format_date('Thu, 17 Oct 2024 00:00:00 GMT'),
    catchup=False,
    tags=["config_core"],
)

clean_metadata_backup = PythonOperator(
    task_id='clean_old_file',
    python_callable=utils.clean_files_backup,
    dag=dag
)


clean_metadata_backup