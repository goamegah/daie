"""spark manager"""

# mds/utils/spark_utils.py


import logging
import os
from pyspark.sql import SparkSession
from mds.definitions import (DATABRICKS_CONNECT_FILE, AZURE_STORAGE_FILE)
from mds.utils.config import (
    read_json,
    get_databricks_instance_secret_key,
    get_databricks_instance_scope_name,
    get_databricks_instance_id,
    get_azure_tenant_id,
    get_azure_storage_account
)

logger = logging.getLogger(__name__)
TENANT_ID = get_azure_tenant_id() if os.path.exists(AZURE_STORAGE_FILE) else ''
STORAGE_ACCOUNT = get_azure_storage_account() if os.path.exists(AZURE_STORAGE_FILE) else ''
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
    def get_spark_session(self) -> SparkSession:
        """
        Retourne l'unique SparkSession partagée.
        Si la session n'existe pas, elle est créée.
        """
        if self._spark is None:
            self._spark = self._init_session()
        return self._spark

    @property
    def get_dbutils(self):
        from pyspark.dbutils import DBUtils #pylint: disable=no-name-in-module disable=import-error,import-outside-toplevel
        """
        Retourne l'unique DBUtils lié à la SparkSession.
        Si DBUtils n'existe pas, il est créé.
        """
        if self._dbutils is None:
            self._dbutils = DBUtils(self.get_spark_session)
        return self._dbutils

    def _init_session(self) -> SparkSession:
        if os.path.exists(DATABRICKS_CONNECT_FILE):
            from databricks.connect import DatabricksSession #pylint: disable=import-error,disable=no-name-in-module,disable=import-outside-toplevel
            from databricks.sdk.core import Config #pylint: disable=import-error,disable=no-name-in-module,disable=import-outside-toplevel

            cfg = Config(**read_json(DATABRICKS_CONNECT_FILE))
            logger.info('Mode Databricks Connect activé.')
            spark = DatabricksSession.builder.sdkConfig(cfg).getOrCreate()
            self._configure_azure(spark)
            return spark

        logger.info('Mode Spark standard.')
        spark = SparkSession.builder.appName("mds").getOrCreate()
        self._configure_azure(spark)
        return spark

    def _configure_azure(self, spark: SparkSession) -> None:
        try:
            env = get_databricks_env()
            instance_id = get_databricks_instance_id(env)
            secret = self.get_dbutils.secrets.get(
                scope=get_databricks_instance_scope_name(env),
                key=get_databricks_instance_secret_key(env)
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
    return _spark_manager.get_spark_session


def get_dbutils():
    """
    Retourne l'unique DBUtils lié à la SparkSession.
    """
    return _spark_manager.get_dbutils


def scope_exists(scope_name: str, key: str) -> bool:
    """
    Vérifie si un scope et une clé existent dans Databricks.
    """
    try:
        dbutils = get_dbutils()
        dbutils.secrets.get(scope=scope_name, key=key)
        return True
    except Exception as e:
        logger.error("Erreur lors de la vérification du scope %s et de la clé %s : %s", scope_name, key, e)
        return False
    
def get_databricks_env() -> str:
    """
    Retourne l'environnement Databricks actuel. (dev, test, prod)
    Si l'environnement n'est pas défini, retourne 'dev' par défaut.
    """
    if scope_exists(get_databricks_instance_scope_name(DEV), get_databricks_instance_secret_key(DEV)):
        return DEV
    if scope_exists(get_databricks_instance_scope_name(TEST), get_databricks_instance_secret_key(TEST)):
        return TEST
    if scope_exists(get_databricks_instance_scope_name(PROD), get_databricks_instance_secret_key(PROD)):
        return PROD
    return DEV  # Valeur par défaut si aucun scope n'est trouvé
    
