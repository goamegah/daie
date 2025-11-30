""" This module defines the paths to configuration files used in the application.
It sets up the directory structure for configuration files and provides paths to specific JSON files
used for Databricks instances and Azure storage configurations."""

# daie/definitions.py
import os
from pathlib import Path

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the configuration directory and file paths
CONFIG_DIR = os.path.join(Path(ROOT_DIR).parent.absolute(), 'config')
LOCAL_DATABRICKS_CONNECT_CONFIG_FILE = os.path.join(CONFIG_DIR, '.databricks_connect.json')
AZURE_STORAGE_FILE = os.path.join(CONFIG_DIR, '.azure_storage.json')

if __name__ == "__main__":
    print(f"Configuration directory: {CONFIG_DIR}")