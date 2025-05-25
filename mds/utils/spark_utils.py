"""spark utils module.
This module provides utilities for managing Spark sessions and configurations,
including integration with Azure OAuth and Databricks Connect.
It includes a singleton SparkManager class that initializes and manages
a shared SparkSession and DBUtils instance.
It also provides helper functions to access the Spark session and DBUtils,
check the existence of Databricks scopes and keys, and determine the current
Databricks environment (dev, test, prod).
It is designed to be used in a Databricks environment or with Databricks Connect.
It handles the configuration of Azure storage accounts and OAuth authentication
for accessing Azure Data Lake Storage (ADLS) Gen2.
This module is part of the MDS (Modular Data Science) project."""

# mds/utils/spark_utils.py


import logging
import os
from pyspark.sql import SparkSession
from mds.definitions import (DATABRICKS_CONNECT_FILE, AZURE_STORAGE_FILE)
from mds.utils.config_utils import (
    read_json,
    read_databricks_instance_secret_key_config,
    read_databricks_instance_scope_name_config,
    read_databricks_instance_id_config,
    read_azure_tenant_id_config,
    read_azure_storage_account_config
)

logger = logging.getLogger(__name__)
TENANT_ID = read_azure_tenant_id_config() if os.path.exists(AZURE_STORAGE_FILE) else ''
STORAGE_ACCOUNT = read_azure_storage_account_config() if os.path.exists(AZURE_STORAGE_FILE) else ''
OAUTH_ENDPOINT = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"

DEV = 'dev'
TEST = 'test'
PROD = 'prod'


class SparkManager:
    """Gère la création/config de la SparkSession + Azure OAuth."""

    def __init__(self) -> None:
        self._spark: SparkSession | None = None
        self._dbutils = None

    @property
    def spark(self) -> SparkSession:
        """
        Retourne l'unique SparkSession partagée.
        Si la session n'existe pas, elle est créée.
        """
        if self._spark is None:
            self._spark = self._init_session()
        return self._spark

    @property
    def dbutils(self):
        """
        Retourne l'unique DBUtils lié à la SparkSession.
        Si DBUtils n'existe pas, il est créé.
        """
        from pyspark.dbutils import DBUtils #pylint: disable=no-name-in-module disable=import-error,import-outside-toplevel
        if self._dbutils is None:
            self._dbutils = DBUtils(self.spark)
        return self._dbutils

    def _init_session(self) -> SparkSession:
        if os.path.exists(DATABRICKS_CONNECT_FILE):
            from databricks.connect import DatabricksSession #pylint: disable=import-error,disable=no-name-in-module,disable=import-outside-toplevel
            from databricks.sdk.core import Config #pylint: disable=import-error,disable=no-name-in-module,disable=import-outside-toplevel

            cfg = Config(**read_json(DATABRICKS_CONNECT_FILE))
            logger.info('Mode Databricks Connect activé.')
            spark = DatabricksSession.builder.sdkConfig(cfg).getOrCreate()
        else:
            logger.info('Mode Spark standard.')
            spark = SparkSession.builder.appName("mds").getOrCreate()
        self._configure_azure_storage_account(spark)
        return spark

    def _configure_azure_storage_account(self, spark: SparkSession) -> None:
        try:
            _ = self.dbutils  #pylint: disable=redefined-outer-name
            env = get_databricks_env()
            instance_id = read_databricks_instance_id_config(env)
            secret = self.dbutils.secrets.get(
                scope=read_databricks_instance_scope_name_config(env),
                key=read_databricks_instance_secret_key_config(env)
            )

            auth_configs = {
                f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net": "OAuth",
                f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net":
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net":
                    instance_id,
                f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net":
                    secret,
                f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net":
                    OAUTH_ENDPOINT,
            }
            for key, val in auth_configs.items():
                spark.conf.set(key, val)
                logger.debug("Spark conf set: %s", key)
        except ModuleNotFoundError as e:
            if e.msg == "No module named 'pyspark.dbutils'":
                logger.info("No storage account configuration needed for unit tests.")
                print("No storage account configuration needed for unit tests.")
            else:
                logger.error("ModuleNotFoundError: %s", e)
                raise e


# ───── Singleton et helpers ─────
_spark_manager = SparkManager()


def get_spark_session() -> SparkSession:
    """
    Retourne l'unique SparkSession partagée.
    """
    return _spark_manager.spark


def get_dbutils():
    """
    Retourne l'unique DBUtils lié à la SparkSession.
    """
    return _spark_manager.dbutils


def scope_exists(scope_name: str, key: str) -> bool:
    """
    Vérifie si un scope et une clé existent dans Databricks.
    """
    try:
        dbutils = get_dbutils()
        dbutils.secrets.get(scope=scope_name, key=key)
        return True
    except Exception as e: # pylint: disable=broad-except
        logger.error("Erreur lors de la vérification du scope %s et de la clé %s : %s", scope_name, key, e)
        return False

def get_databricks_env() -> str:
    """
    Retourne l'environnement Databricks actuel. (dev, test, prod)
    Si l'environnement n'est pas défini, retourne 'dev' par défaut.
    """
    if scope_exists(read_databricks_instance_scope_name_config(DEV), read_databricks_instance_secret_key_config(DEV)):
        return DEV
    if scope_exists(read_databricks_instance_scope_name_config(TEST), read_databricks_instance_secret_key_config(TEST)):
        return TEST
    if scope_exists(read_databricks_instance_scope_name_config(PROD), read_databricks_instance_secret_key_config(PROD)):
        return PROD
    return DEV  # Valeur par défaut si aucun scope n'est trouvé
