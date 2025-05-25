"""Gestionnaire de config pour Databricks et Azure."""

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
    """Charge un JSON et renvoie un dictionnaire."""
    with path.open('r') as f:
        parsed_json = json.load(f)
    return parsed_json

def validate_env(env: str) -> None:
    """
    Valide l'environnement donné.
    Lève une ValueError si l'environnement n'est pas valide.
    """
    if env not in ENVIRONMENTS:
        raise ValueError(f"Environnement invalide '{env}'. Choisir parmi {ENVIRONMENTS}.")

def read_databricks_instance_secret_key_config(env: str) -> str:
    """
    Récupère la clé secrète pour l'env donné dans .databricks_instances.json.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['secret_key']

def read_databricks_instance_scope_name_config(env: str) -> str:
    """
    Récupère la clé secrète pour l'env donné dans .databricks_instances.json.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['scope_name']

def read_databricks_instance_id_config(env: str) -> str:
    """
    Récupère l'ID d'instance Databricks pour l'env donné.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['databricks_instance_id']

def read_azure_tenant_id_config() -> str:
    """
    Récupère l'ID du locataire Azure à partir du fichier de configuration Azure Storage.
    """
    data = read_json(AZURE_STORAGE_FILE)
    return data.get('tenant_id', '')

def read_azure_storage_account_config() -> str:
    """
    Récupère le nom du compte de stockage Azure à partir du fichier de configuration Azure Storage.
    """
    data = read_json(AZURE_STORAGE_FILE)
    return data.get('storage_account', '')
