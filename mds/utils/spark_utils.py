"""spark manager"""

# mds/utils/spark_utils.py


import logging
import os
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from mds.definitions import CONNECT_FILE
from mds.utils.config import (
    read_json,
    get_databricks_instance_secret_key,
    get_databricks_instance_scope_name,
    get_databricks_instance_id,
    resolve_main_env,
)

logger = logging.getLogger(__name__)
TENANT_ID = os.getenv('TENANT_ID', '')
STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT', '')
OAUTH_ENDPOINT = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"


class SparkManager:
    """Gère la création/config de la SparkSession + Azure OAuth."""

    def __init__(self) -> None:
        self._spark: SparkSession | None = None
        self._dbutils: DBUtils | None = None

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
    def dbutils(self) -> DBUtils:
        """
        Retourne l'unique DBUtils lié à la SparkSession.
        Si DBUtils n'existe pas, il est créé.
        """
        if self._dbutils is None:
            self._dbutils = DBUtils(self.spark)
        return self._dbutils

    def _init_session(self) -> SparkSession:
        if os.path.exists(CONNECT_FILE):
            from databricks.connect import DatabricksSession #pylint: disable=import-outside-toplevel
            from databricks.sdk.core import Config #pylint: disable=import-outside-toplevel

            cfg = Config(**read_json(CONNECT_FILE))
            logger.info('Mode Databricks Connect activé.')
            spark = DatabricksSession.builder.sdkConfig(cfg).getOrCreate()
            self._configure_azure(spark)
            return spark

        logger.info('Mode Spark standard.')
        spark = SparkSession.builder.getOrCreate()
        self._configure_azure(spark)
        return spark

    def _configure_azure(self, spark: SparkSession) -> None:
        env = resolve_main_env(self.dbutils)
        instance_id = get_databricks_instance_id(env)
        secret = self.dbutils.secrets.get(
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


# ───── Singleton et helpers ─────
_spark_manager = SparkManager()


def get_spark_session() -> SparkSession:
    """
    Retourne l'unique SparkSession partagée.
    """
    return _spark_manager.spark


def get_dbutils() -> DBUtils:
    """
    Retourne l'unique DBUtils lié à la SparkSession.
    """
    return _spark_manager.dbutils
