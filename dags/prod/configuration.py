import yaml

"""
__author__ = Team Operacion del dato.  
__description__ = Modulo de configuracion.
"""

# Cargar el archivo YAML
with open('/usr/local/airflow/dags/prod/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Variables
API_BASE_URL=''
PAHT_JSON_METADATA = config['airflow']['file_json']
PAHT_JSON_BACKUP = config['airflow']['file_json_backup']
MAX_DAYS_BACKUP = config['airflow']['max_days_backup']
TIME_ZONE= config['airflow']['time_zone']

if config['airflow']['prod']:
    API_BASE_URL = config['api']['prod']['base_url']

else:
    API_BASE_URL = config['api']['desa']['base_url']


