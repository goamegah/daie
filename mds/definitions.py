""" def """

import os
from pathlib import Path

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Répertoire parent/config contenant les JSON de config
CONFIG_DIR = os.path.join(Path(ROOT_DIR).parent.absolute(), 'config')
DATABRICKS_INSTANCES_FILE = os.path.join(CONFIG_DIR, '.local_dev_conf_databricks_instances.json')
DATABRICKS_CONNECT_FILE = os.path.join(CONFIG_DIR, '.local_dev_conf_databricks_connect.json')
AZURE_STORAGE_FILE = os.path.join(CONFIG_DIR, '.local_dev_conf_azure_storage.json')

print(f"Configuration directory: {CONFIG_DIR}")
# Chemin vers le fichier de configuration des instances Databricks
# if not os.path.exists(CONFIG_DIR):
#     raise FileNotFoundError(f"Configuration directory does not exist: {CONFIG_DIR}")
# if not os.path.exists(DATABRICKS_INSTANCES_FILE):
#     raise FileNotFoundError(f"Databricks instances file does not exist: {DATABRICKS_INSTANCES_FILE}")
# if not os.path.exists(DATABRICKS_CONNECT_FILE):
#     raise FileNotFoundError(f"Databricks connect file does not exist: {DATABRICKS_CONNECT_FILE}")
# if not os.path.exists(AZURE_STORAGE_FILE):
#     raise FileNotFoundError(f"Azure storage file does not exist: {AZURE_STORAGE_FILE}")
