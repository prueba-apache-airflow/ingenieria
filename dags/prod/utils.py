from datetime import datetime, timedelta
import calendar
import json
import logging
import os
import pendulum
import pytz
from pathlib import Path
import shutil
import sys
import time
import prod.api as api
import prod.configuration as configuration

"""
__author__ = Team Operacion del dato  
__description__ = Modulo de funciones comunes
"""

timeZone = configuration.TIME_ZONE
local_tz = pendulum.timezone(timeZone)

def get_format_date(star_date):

    date_format = None
    if star_date:
        date_format = datetime.strptime(star_date, "%a, %d %b %Y %H:%M:%S %Z")
        date_format = date_format.replace(tzinfo=pytz.utc).astimezone(local_tz)

    return date_format

def message_formatting(message):
    system = "AIRFLOW: "
    logging.info(f"({system}) {message}")

def schedule_interval_crontab(time_start, time_end, frequency, freq_interval):
    """
    @Param time_str: Hora de inicio.
    @Param time_end: Hora de fin.
    @Param frequency: Frecuencia ya sea(hourly, daily, weekly, monthly).
    @Param freq_interval: Dependera de la frecuencia hourly(minutos: 0 - 59), daily(horas: 0 - 23), 
    weekly(dias de la semana: 0 - 6), monthly(dias del mes: 1 - 31).
    @Return: Devuelve un string en formato crontab.   
    """
    if time_start is None:
        hour_start = 0
        minute_start = 0
    else:    
        time_dt = datetime.strptime(time_start, "%H:%M:%S")
        hour_start = time_dt.hour
        minute_start = time_dt.minute
   
    if time_end is None:
        hour_end = 23
        minute_end = 59
    else:    
        time_dt = datetime.strptime(time_end, "%H:%M:%S")
        hour_end = time_dt.hour
        minute_end = time_dt.minute


    if frequency == 'hourly':
        crontab_expression = f'{freq_interval} {hour_start}-{hour_end} * * *'

    elif frequency == 'daily':
        if hour_start != hour_end:
            crontab_expression = f'{minute_start} {hour_start}-{hour_end} * * *'
        else: 
            crontab_expression = f'{minute_start} {freq_interval} * * *'   
           
    elif frequency == 'weekly':
        if str(hour_start) == str(hour_end):
            crontab_expression = f'{minute_start} {hour_start} * * {freq_interval}'
        else:    
            crontab_expression = f'{minute_start} {hour_start}-{hour_end} * * {freq_interval}'
   
    elif frequency == 'monthly':
        if '31' in freq_interval.split(','):
            today = datetime.today()
            year = today.year
            month = today.month
            last_day = calendar.monthrange(year, month)[1]
            freq_interval = freq_interval.replace('31', str(last_day))
        
        if hour_start == hour_end:
            crontab_expression = f'{minute_start} {hour_start} {freq_interval} * *'
        else :
            crontab_expression = f'{minute_start} {hour_start}-{hour_end} {freq_interval} * *'       
    else:
        raise ValueError("Frequency not supported. Use hourly, daily, weekly, monthly")

    return crontab_expression

def range_on_execution_hours(context, variable):
    """
    @Param context: Contiene los parametros de configuracion para el dag actual.
    @Param variable: Contiene la hora de inicio y fin para el dag actual.
    @Description: Funcion que se ejecuta cuando una tarea se reintenta, controla si el proximo reintento estara
    dentro del rango de horas establecido.   
    """
    task_instance = context.get('task_instance')
    task = task_instance.task
    retry_delay = task.retry_delay
    dag_id = task_instance.dag_id
    dag_run = context['dag_run']
    status = False

    execution_date = dag_run.execution_date.astimezone(pytz.timezone(timeZone)).date()

    satart_date = dag_run.start_date.astimezone(pytz.timezone(timeZone)).date()

    today_date = datetime.now(pytz.timezone(timeZone)).date()

    message_formatting("===============Inicio de ejecucion de funcion de reintento=================")
    message_formatting(f"Fecha programada:{dag_run.execution_date.astimezone(pytz.timezone(timeZone))}")
    message_formatting(f"Fecha de inicio de ejecucion:{dag_run.start_date.astimezone(pytz.timezone(timeZone))}")
    message_formatting(f"Fecha actual:{datetime.now(pytz.timezone(timeZone))}")

    message_formatting("==================================================")
    message_formatting(f"Fecha programada:{execution_date}")
    message_formatting(f"Fecha de inicio de ejecucion:{satart_date}")
    message_formatting(f"Fecha actual:{today_date}")

    if satart_date == today_date:
        current_time = datetime.now(pytz.timezone(timeZone)).time()
        time_range = variable.get(dag_id)
        message_formatting(f"Hora actual: {current_time}")
        message_formatting(f"Rango de horas: {time_range}")

        start_time_str, end_time_str = time_range.split("|")

        if  end_time_str == '':
            end_time_str = "23:59:00"
            message_formatting(f"Hora de Fin no establecida se asignara {end_time_str}")

        if start_time_str == end_time_str:
            end_time_str = "23:59:00"
            message_formatting(f"Hora de inicio es igual a la hora de fin, se reintentara hasta las {end_time_str}")

        start_time = datetime.strptime(start_time_str, "%H:%M:%S").time()  
        end_time = datetime.strptime(end_time_str, "%H:%M:%S").time()   

        #Agregando a la hora actual el tiempo de reintento
        current_datetime = datetime.combine(datetime.today(), current_time)
        time_add = current_datetime + retry_delay
        new_time_retry = time_add.time()

        message_formatting(f"Hora Inicio:{start_time} Hora fin: {end_time}")
        message_formatting(f"Hora Actual:{current_time} Hora de proximo reintento: {new_time_retry}")
        message_formatting(f"Tiempo sumado:{new_time_retry} ")

        if start_time <= new_time_retry <= end_time:
            status = False
            message_formatting(f"El proximo reintento sera a las: {new_time_retry}")
        else:
            status = True
    else:
        status = True            
    

    if status:
        task_instance.max_tries = task_instance.try_number
        task_instance.state = 'failed'
        message_formatting("Se aborta las ejecuciones por estar fuera del rango de horas programadas")
    message_formatting("===============Fin de ejecucion de funcion de reintento=================")

def generate_metadata_dag():
    """
    @Description: Obtiene la metadata desde la api y escribe cada DAG con sus respectivos TASKs en arhivos JSON   
    """
    try:
        path = configuration.PAHT_JSON_METADATA
        if not os.path.exists(path):
            os.makedirs(path)
            logging.info(f"Directory created at path: {path}")

        list_json_initial = get_list_files_metadata() 
        list_json_final = []  

        logging.info("Get full metadata")
        metadata = api.get_dag_and_tasks()
        
        if metadata is None:
            logging.warning("Metadata DAGs not found...")
            raise Exception("Metadata DAGs not found...")

        logging.info("Metadata DAGs obtained successfully")
        for data in metadata:
            if data:
                dag = data["dag"]
                name_file = f"{dag['dag_name']}.json"
                file_json = f"{path}/{name_file}"
                with open(file_json, 'w') as f:
                    json.dump(data, f, indent=4)
                    list_json_final.append(name_file)      

        move_files_json(list_json_initial, list_json_final)           
        logging.info(f"File generated successfully in path: {path}")
    except:
        logging.error(f"Error updating metadata in path: {path}")
        logging.error(sys.exc_info()[0:])  
        sys.exit(1)  

def get_metadata_dag(file_name):
    """
    @Param file_name: Nombre del archivo JSON
    @Return: Devuelve la data del archivo en la variable data   
    """
    path = configuration.PAHT_JSON_METADATA
    path_file = f"{path}/{file_name}"
    try:
        with open(path_file, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        logging.error(f"The {path_file} file not found.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding json file: {path_file}.")
        return None    

def get_list_files_metadata():
    """
    @Return: Devuelve la lista de archivos JSON del directorio json
    """
    path = Path(configuration.PAHT_JSON_METADATA)
    list_json = [f.name for f in path.iterdir() 
                 if f.suffix == '.json' and f.stat().st_size > 0]
    return list_json

def move_files_json(list_json_initial, list_json_final):
    """
    @Param list_json_initial: Lista de json antes de la actualizacion de metadata
    @Param list_json_final: Lista de json despues de la actualizacion de metadata
    @Description: Mueve los archivos json con dag que fueron eliminados en BD hacia el directorio json_backup   
    """
    logging.info("Update metadata file json...")
    files_move = set(list_json_initial) - set(list_json_final)
    if len(files_move) == 0:
        logging.info("No changes in json files...")
        return

    for file in files_move:
        try:
            file_path_init = f"{configuration.PAHT_JSON_METADATA}/{file}"
            if os.path.exists(file_path_init):
                file_path_final = configuration.PAHT_JSON_BACKUP
                if not os.path.exists(file_path_final):
                    os.makedirs(file_path_final)
                    logging.info(f"Directory backup create: {file_path_final}")
                shutil.move(file_path_init, file_path_final)
                logging.info(f"File move to: {file}")
            else:
                logging.warning(f"File not found: {file_path_init}")
        except Exception as e:
            logging.info(f"Error move file: {file_path_init}: {str(e)}")            
    logging.info("Move files json metadata successfully...")

def clean_files_backup():
    """
    @Description: Funcion que elimina archivos json con una antiguedad mayor a X dias   
    """
    try:
        logging.info(f"Delete files older than {configuration.MAX_DAYS_BACKUP} days")
        cout_file_delete = 0
        current_date = datetime.now()
        for file in os.listdir(configuration.PAHT_JSON_BACKUP):
            path_file = os.path.join(configuration.PAHT_JSON_BACKUP, file)
            if os.path.isfile(path_file):
                last_modification = datetime.fromtimestamp(os.path.getmtime(path_file))
                logging.info(f"File: {file} last modification: {last_modification.astimezone(local_tz)} ")
                if (current_date - last_modification) > timedelta(days=configuration.MAX_DAYS_BACKUP):
                    os.remove(path_file)
                    cout_file_delete +=1
                    logging.info(f"File deleted: {path_file}")
        if cout_file_delete == 0:
            logging.info(f"There are no files older than {configuration.MAX_DAYS_BACKUP} days")
        logging.info(f"Number of files deleted: {cout_file_delete}")                 
        logging.info("Cleaning old files was successful")               
    except:
        logging.info("Error cleaning up old files")
        logging.error(sys.exc_info()[0:])  
        sys.exit(1)

