from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def importFilesServerFedora():
    print(f"Conectando al servidor remoto 192.168.2.16")
    time.sleep(2)
    print(f"Listando los archivos")
    time.sleep(2)
    print(f"Transfiriendo archivos..")
    print(f"   ....   ")
    time.sleep(5)
    print(f"Archivos transferidos con exito")
    time.sleep(2)
    print(f"Cerrando conexion")
    print(f"Proceso terminado")

def joinFiles():
    print(f"Iniciando conexion con la BD")
    time.sleep(2)
    print(f"Listando los archivos")
    time.sleep(2)
    print(f"Archivos listados con exito")
    print(f"Uniendo los archivos")
    print(f"   ....   ")
    time.sleep(5)
    print(f"Archivos unidos con exito")
    time.sleep(2)
    print(f"Cerrando conexion")
    print(f"Proceso terminado")

def fastLoadProcess():
    print(f"Listando los archivos a cargar")
    time.sleep(2)
    print(f"Archivos obtenidos")
    time.sleep(2)
    print(f"Cargando a las tablas de la BD")
    print(f"   ....   ")
    time.sleep(6)
    print(f"Archivos Cargados con exito")
    print(f"Cerrando conexion")
    print(f"Proceso terminado")

def transformProcess():
    print(f"Estableciendo conexion con la base de datos")
    time.sleep(2)
    print(f"Ejecutando la query")
    time.sleep(2)
    print(f"Transformando la data")
    print(f"   ....   ")
    time.sleep(7)
    print(f"Transformacion termino con exito")
    time.sleep(2)
    print(f"Cerrando conexion con base de datos")
    time.sleep(2)
    print(f"Proceso terminado")

def extractProcess():
    print(f"Conectando con la base de datos")
    time.sleep(2)
    print(f"Conexion exitosa")
    time.sleep(2)
    print(f"Ejecutando query para la extraccion")
    print(f"   ....   ")
    time.sleep(10)
    print(f"Datos obtenidos")
    time.sleep(2)
    print(f"Cerrando conexion con la bd")
    time.sleep(2)
    print(f"Escribiendo la data en un archivo csv")
    print(f"Iniciando conexion con el servidor 192.168.2.78")
    time.sleep(10)
    print(f"Trasnfiriendo archivos")
    time.sleep(2)
    print(f"Archivo enviado con exito")
    time.sleep(2)
    print(f"Cerrando conexion")
    time.sleep(2)
    print(f"Proceso terminado con exito")

with DAG(
    dag_id="process_data_transform",
    start_date=datetime(2024, 8, 14),
    schedule_interval="@daily",
    catchup=False,
    default_args={'owner': 'regulatorio', 'retries': 1}
) as dag:

    task_importFilesServerFedora = PythonOperator(
        task_id="importFilesServerFedora",
        python_callable=importFilesServerFedora
    )

    task_joinFiles = PythonOperator(
        task_id="joinFiles",
        python_callable=joinFiles
    )

    task_fastLoadProcess = PythonOperator(
        task_id="fastLoadProcess",
        python_callable=fastLoadProcess
    )

    task_transformProcess = PythonOperator(
        task_id="transformProcess",
        python_callable=transformProcess
    )

    task_extractProcess = PythonOperator(
        task_id="extractProcess",
        python_callable=extractProcess
    )

    task_importFilesServerFedora >> task_joinFiles >> task_fastLoadProcess >> task_transformProcess >> task_extractProcess