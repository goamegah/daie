"""Gestionnaire de secrets pour Databricks."""

# mds/utils/config.py
import json
import logging
from typing import Final, Dict, Any
from pathlib import Path
from pyspark.dbutils import DBUtils
from mds.definitions import INSTANCES_FILE


ENVIRONMENTS: Final[set[str]] = {'dev', 'test', 'prod'}

logger = logging.getLogger(__name__)

def read_json(path: Path) -> Dict[str, Any]:
    """Charge un JSON et renvoie un dictionnaire."""
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)

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
    data = read_json(INSTANCES_FILE)
    return data[env]['secret_key']

def get_databricks_instance_scope_name(env: str) -> str:
    """
    Récupère la clé secrète pour l'env donné dans .databricks_instances.json.
    """
    validate_env(env)
    data = read_json(INSTANCES_FILE)
    return data[env]['scope_name']

def get_databricks_instance_id(env: str) -> str:
    """
    Récupère l'ID d'instance Databricks pour l'env donné.
    """
    validate_env(env)
    data = read_json(INSTANCES_FILE)
    return data[env]['databricks_instance_id']

def resolve_main_env(dbutils: DBUtils) -> str:
    """
    Détermine l'environnement principal en testant l'existence des clés
    dans le scope Databricks (test → dev → prod).
    """
    for env in ('test', 'dev', 'prod'):
        key = get_databricks_instance_secret_key(env)
        scope_name = get_databricks_instance_scope_name(env)
        try:
            dbutils.secrets.get(scope=scope_name, key=key)
            logger.info("Environnement détecté : %s", env)
            return env
        except dbutils.secrets.SecretNotFoundException:
            logger.debug("Clé manquante pour l'env : %s (scope: %s, key: %s)", env, scope_name, key)
        except RuntimeError as e:
            logger.warning("Erreur d'exécution pour l'env %s : %s", env, e)
        except ValueError as e:
            logger.warning("Valeur invalide pour l'env %s : %s", env, e)
    raise ValueError('Aucun environnement valide trouvé via les secrets.')
