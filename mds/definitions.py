""" def """

import os
from pathlib import Path

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Répertoire parent/config contenant les JSON de config
CONFIG_DIR = os.path.join(Path(ROOT_DIR).parent.absolute(), 'config')
INSTANCES_FILE = CONFIG_DIR / '.databricks_instances.json'
CONNECT_FILE = CONFIG_DIR / '.databricks_connect.json'
