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
print(f"DATABRICKS_INSTANCES_FILE: {DATABRICKS_INSTANCES_FILE}")
