import logging
import requests
from requests.exceptions import RequestException
import prod.configuration as configuration

"""
__author__ = Team Operacion del dato  
__description__ = Modulo api para obtener metadata de DAGs
"""

API_URL = configuration.API_BASE_URL

def get_dags():
    """
    @Return: Devuelve todos los DAGs desde la base de datos   
    """    
    try:
        url = f"{API_URL}/api/v1/dag"
        response = requests.get(url)
        if response.status_code == 404:
            logging.warning(f"DAGs not found.")
            return None
        response.raise_for_status()  # Maneja errores HTTP
        
        data = response.json()
        if data:
            return data
        else:
            logging.warning(f"Dag list is empty or invalid {url}")
            return None
        
    except RequestException as e:
        logging.error(f"Error call to {url}: {str(e)}")
        raise Exception("An error occurred while getting the list of dags...")
    except ValueError as e:
        logging.error(f"Error parse JSON from {url}: {str(e)}")
        raise Exception("An error occurred while parsing the JSON response...")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise Exception("An unexpected error occurred while getting the list of dags...")

def get_tasks_by_dag_id(dag_id):
    """
    @Param dag_id: Identificador del DAG
    @Return: Devuelve la lista de TASKs para un DAG en especifico   
    """    
    try:
        url = f"{API_URL}/api/v1/dag/{str(dag_id)}/tasks"
        response_task = requests.get(url)
        if response_task.status_code == 404:
            logging.warning(f"Tasks not found for DAG ID {dag_id}.")
            return None
        response_task.raise_for_status()  # Maneja errores HTTP
        
        data = response_task.json()
        if data:  
            return data
        else:
            logging.warning(f"Task list is empty or invalid: {url} ")
            return None

    except RequestException as e:
        logging.error(f"Error call to {url}: {str(e)}")
        raise Exception(f"An error occurred while getting the list of tasks by dag_id: {dag_id}")
    except ValueError as e:
        logging.error(f"Error parse JSON from {url}: {str(e)}")
        raise Exception(f"An error occurred while parsing the JSON response for dag_id: {dag_id}")
    except Exception as e:
        logging.error(f"unexpected error: {str(e)}")
        raise Exception(f"An unexpected error occurred while getting the list of tasks for dag_id: {dag_id}")

def get_dag_and_tasks():
    """
    @Return: Devuelve la todos los DAGs con sus respectivos TASKs desde la BD   
    """    
    try:
        url = f"{API_URL}/api/v1/fulldag"
        response_task = requests.get(url)
        if response_task.status_code == 404:
            logging.warning(f"Data not found...")
            return None
        response_task.raise_for_status()
        
        data = response_task.json()
        if data:  
            return data
        else:
            logging.warning(f"Task list is empty or invalid: {url} ")
            return None

    except RequestException as e:
        logging.error(f"Error call to {url}: {str(e)}")
        raise Exception(f"An error occurred while getting the list of DAGs")
    except ValueError as e:
        logging.error(f"Error parse JSON from {url}: {str(e)}")
        raise Exception(f"An error occurred while parsing the JSON response")
    except Exception as e:
        logging.error(f"unexpected error: {str(e)}")
        raise Exception(f"An unexpected error occurred while getting the list of DAGs")
