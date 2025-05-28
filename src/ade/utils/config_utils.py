"""Config manager for Databricks and Azure."""

# mds/utils/config_utils.py
import json
import logging
from pathlib import Path
from typing import Dict, Any
from typing import Final
from ade.definitions import DATABRICKS_INSTANCES_FILE, AZURE_STORAGE_FILE

ENVIRONMENTS: Final[set[str]] = {'dev', 'test', 'prod'}

logger = logging.getLogger(__name__)

def load_json(path: Path) -> Dict[str, Any]:
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

def get_databricks_secret_key_from_config_file(env: str) -> str:
    """
    Get the secret key for the given environment from .databricks_instances.json.
    """
    validate_env(env)
    data = load_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['secret_key']

def get_databricks_scope_name_from_config_file(env: str) -> str:
    """
    Get the scope name for the given environment from .databricks_instances.json.
    """
    validate_env(env)
    data = load_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['scope_name']

def get_databricks_instance_id_from_config_file(env: str) -> str:
    """
    Get the Databricks instance ID for the given environment.
    """
    validate_env(env)
    data = load_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['databricks_instance_id']

def get_azure_tenant_id_from_config_file() -> str:
    """
    Get the Azure tenant ID from the Azure Storage config file.
    Returns an empty string if the tenant ID is not found.
    """
    if not Path(AZURE_STORAGE_FILE).exists():
        logger.warning("Azure Storage config file '%s' does not exist.", AZURE_STORAGE_FILE)
        return ""
    data = load_json(AZURE_STORAGE_FILE)
    tenant_id = data.get("tenant_id")
    if not tenant_id:
        logger.warning("No 'tenant_id' found in Azure config.")
    return tenant_id or ""

def get_azure_storage_account_from_config_file() -> str:
    """
    Get the Azure storage account name from the Azure Storage config file.
    """
    data = load_json(AZURE_STORAGE_FILE)
    return data.get('storage_account', '')
