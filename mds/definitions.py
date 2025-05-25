""" This module defines the paths to configuration files used in the application.
It sets up the directory structure for configuration files and provides paths to specific JSON files
used for Databricks instances and Azure storage configurations."""

import os
from pathlib import Path

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the configuration directory and file paths
CONFIG_DIR = os.path.join(Path(ROOT_DIR).parent.absolute(), 'config')
DATABRICKS_INSTANCES_FILE = os.path.join(CONFIG_DIR, '.local_dev_conf_databricks_instances.json')
DATABRICKS_CONNECT_FILE = os.path.join(CONFIG_DIR, '.local_dev_conf_databricks_connect.json')
AZURE_STORAGE_FILE = os.path.join(CONFIG_DIR, '.local_dev_conf_azure_storage.json')

if __name__ == "__main__":
    print(f"Configuration directory: {CONFIG_DIR}")
    print(f"DATABRICKS_INSTANCES_FILE: {DATABRICKS_INSTANCES_FILE}")

