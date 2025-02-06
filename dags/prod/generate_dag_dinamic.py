from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
#from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import prod.utils as utils

"""
__author__ = Team Operacion del dato  
__description__ = Modulo de generacion de DAGs dinamicos
"""

def on_retry_callback_function(context):
    utils.range_on_execution_hours(context, Variable)

list_file_json= utils.get_list_files_metadata()

for file in list_file_json:
    metadata_dag = utils.get_metadata_dag(file)
    if metadata_dag is None:
        continue
    
    dag_info = metadata_dag.get('dag')

    Variable.set(dag_info['dag_name'], f"{dag_info['hour_start']}|{dag_info['hour_end']}")

    default_args = {
        "owner": dag_info['owner'],
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False
    }

    dag = DAG(
        dag_id=dag_info['dag_name'], 
        default_args=default_args,
        description=dag_info['description'],
        schedule_interval=utils.schedule_interval_crontab(\
            dag_info['hour_start'],\
            dag_info['hour_end'],\
            dag_info['frequency'],\
            dag_info['freq_interval']),
        tags=dag_info['tags'].split(","),
        start_date=utils.get_format_date(dag_info['start_date']),
        end_date=utils.get_format_date(dag_info['end_date']),
        catchup=bool(dag_info['catchup']),
    ) 
 
    tasks = {}
    task_list  = metadata_dag.get('tasks', [])

    for task_info in task_list:
        task_id = task_info['task_name']

        command =f"cd {task_info['path']} && {task_info['python_path']} {task_info['path']}{task_info['script_task']} {task_info['layout']}"
       
        tasks[task_id] = SSHOperator(
            task_id=task_id,
            ssh_conn_id=task_info['connection_id'],
            command=command,
            cmd_timeout=520,
            retries=task_info['retries'],
            queue=task_info['queue_task'],
            pool=task_info['pool_name'],
            depends_on_past=bool(task_info['depends_on_past']),
            retry_delay=timedelta(seconds=task_info['retry_delay']),
            on_retry_callback=on_retry_callback_function,
            do_xcom_push=True,
            dag=dag,
        )

    for task_info in task_list:
        task_name = task_info['task_name']
        if task_info['predecessor']:
            predecessors = task_info['predecessor'].split('|')
            for predecessor in predecessors:
                task_predecessor = next((element for element in task_list if element['task_name'] == predecessor), None)
                if task_predecessor:
                    tasks[predecessor] >> tasks[task_name]

    globals()[dag.dag_id] = dag
