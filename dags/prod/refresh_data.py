from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import prod.utils as utils

"""
__author__ = Team Operacion del dato  
__description__ = Modulo que ejecuta la funcion para actualizar la metadata
"""

dag_args = {
    "depends_on_past": False,
    "email": ["test@test.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3), 
}

dag = DAG(
    "refresh_data_dag",
    description="Actualiza el archivo JSON con la nueva data de DB",
    default_args=dag_args,
    schedule_interval=timedelta(minutes=2),
    start_date=utils.get_format_date('Thu, 17 Oct 2024 00:00:00 GMT'),
    catchup=False,
    tags=["config_core"],
)

get_metadata_dag = PythonOperator(
    task_id='get_data_api_and_create_json',
    python_callable=utils.generate_metadata_dag,
    dag=dag
)

get_metadata_dag