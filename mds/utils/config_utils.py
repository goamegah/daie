"""Config manager for Databricks and Azure."""

# mds/utils/config_utils.py
import json
import logging
from pathlib import Path
from typing import Dict, Any
from typing import Final
from mds.definitions import DATABRICKS_INSTANCES_FILE, AZURE_STORAGE_FILE

ENVIRONMENTS: Final[set[str]] = {'dev', 'test', 'prod'}

logger = logging.getLogger(__name__)

def read_json(path: Path) -> Dict[str, Any]:
    """Load a JSON file and return a dictionary."""
    with path.open('r') as f:
        parsed_json = json.load(f)
    return parsed_json

def validate_env(env: str) -> None:
    """
    Validate the given environment.
    Raises a ValueError if the environment is not valid.
    """
    if env not in ENVIRONMENTS:
        raise ValueError(f"Invalid environment '{env}'. Choose from {ENVIRONMENTS}.")

def read_databricks_instance_secret_key_config(env: str) -> str:
    """
    Get the secret key for the given environment from .databricks_instances.json.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['secret_key']

def read_databricks_instance_scope_name_config(env: str) -> str:
    """
    Get the scope name for the given environment from .databricks_instances.json.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['scope_name']

def read_databricks_instance_id_config(env: str) -> str:
    """
    Get the Databricks instance ID for the given environment.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['databricks_instance_id']

def read_azure_tenant_id_config() -> str:
    """
    Get the Azure tenant ID from the Azure Storage config file.
    """
    data = read_json(AZURE_STORAGE_FILE)
    return data.get('tenant_id', '')

def read_azure_storage_account_config() -> str:
    """
    Get the Azure storage account name from the Azure Storage config file.
    """
    data = read_json(AZURE_STORAGE_FILE)
    return data.get('storage_account', '')
