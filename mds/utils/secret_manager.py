"""Gestionnaire de secrets pour Databricks."""

# mds/utils/config.py
import logging
from typing import Final, Dict, Any
from pathlib import Path
from mds.definitions import DATABRICKS_INSTANCES_FILE, AZURE_STORAGE_FILE


ENVIRONMENTS: Final[set[str]] = {'dev', 'test', 'prod'}

logger = logging.getLogger(__name__)

def validate_env(env: str) -> None:
    """
    Valide l'environnement donné.
    Lève une ValueError si l'environnement n'est pas valide.
    """
    if env not in ENVIRONMENTS:
        raise ValueError(f"Environnement invalide '{env}'. Choisir parmi {ENVIRONMENTS}.")

def get_databricks_instance_secret_key(env: str) -> str:
    """
    Récupère la clé secrète pour l'env donné dans .databricks_instances.json.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['secret_key']

def get_databricks_instance_scope_name(env: str) -> str:
    """
    Récupère la clé secrète pour l'env donné dans .databricks_instances.json.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['scope_name']

def get_databricks_instance_id(env: str) -> str:
    """
    Récupère l'ID d'instance Databricks pour l'env donné.
    """
    validate_env(env)
    data = read_json(DATABRICKS_INSTANCES_FILE)
    return data[env]['databricks_instance_id']
    
def get_azure_tenant_id() -> str:
    """
    Récupère l'ID du locataire Azure à partir du fichier de configuration Azure Storage.
    """
    data = read_json(AZURE_STORAGE_FILE)
    return data.get('tenant_id', '')

def get_azure_storage_account() -> str:
    """
    Récupère le nom du compte de stockage Azure à partir du fichier de configuration Azure Storage.
    """
    data = read_json(AZURE_STORAGE_FILE)
    return data.get('storage_account', '')
