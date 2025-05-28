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
from ade.definitions import (DATABRICKS_CONNECT_FILE, AZURE_STORAGE_FILE)
from ade.utils.config_utils import (
    load_json,
    get_databricks_secret_key_from_config_file,
    get_databricks_scope_name_from_config_file,
    get_databricks_instance_id_from_config_file,
    get_azure_tenant_id_from_config_file,
    get_azure_storage_account_from_config_file
)

logger = logging.getLogger(__name__)
TENANT_ID = get_azure_tenant_id_from_config_file() if os.path.exists(AZURE_STORAGE_FILE) else ''
STORAGE_ACCOUNT = get_azure_storage_account_from_config_file() if os.path.exists(AZURE_STORAGE_FILE) else ''
OAUTH_ENDPOINT = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"

DEV = 'dev'
TEST = 'test'
PROD = 'prod'


class SparkManager:
    """Handles creation/configuration of SparkSession + Azure OAuth."""

    def __init__(self) -> None:
        self._spark: SparkSession | None = None
        self._dbutils = None

    def reset(self) -> None:
        """Reset SparkSession and DBUtils (for testing only)."""
        self._spark = None
        self._dbutils = None

    @property
    def spark(self) -> SparkSession:
        """
        Returns the unique shared SparkSession.
        If the session does not exist, it is created.
        """
        if self._spark is None:
            self._spark = self._init_session()
        return self._spark

    @property
    def dbutils(self):
        """
        Returns the unique DBUtils linked to the SparkSession.
        If DBUtils does not exist, it is created.
        """
        from pyspark.dbutils import DBUtils #pylint: disable=no-name-in-module disable=import-error,import-outside-toplevel
        if self._dbutils is None:
            self._dbutils = DBUtils(self.spark)
        return self._dbutils

    def _init_session(self) -> SparkSession:
        if os.path.exists(DATABRICKS_CONNECT_FILE):
            from databricks.connect import DatabricksSession #pylint: disable=import-error,disable=no-name-in-module,disable=import-outside-toplevel
            from databricks.sdk.core import Config #pylint: disable=import-error,disable=no-name-in-module,disable=import-outside-toplevel

            cfg = Config(**load_json(DATABRICKS_CONNECT_FILE))
            logger.info('Databricks Connect mode enabled.')
            spark = DatabricksSession.builder.sdkConfig(cfg).getOrCreate()
        else:
            logger.info('Standard Spark mode.')
            spark = SparkSession.builder.appName("mds").getOrCreate()
        self._configure_azure_storage_account(spark)
        return spark

    def _configure_azure_storage_account(self, spark: SparkSession) -> None:
        try:
            _ = self.dbutils  #pylint: disable=redefined-outer-name
            env = get_databricks_env()
            instance_id = get_databricks_instance_id_from_config_file(env)
            secret = self.dbutils.secrets.get(
                scope=get_databricks_scope_name_from_config_file(env),
                key=get_databricks_secret_key_from_config_file(env)
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


# ───── Singleton and helpers ─────
_spark_manager = SparkManager()


def get_spark_session() -> SparkSession:
    """
    Returns the unique shared SparkSession.
    """
    return _spark_manager.spark


def get_dbutils():
    """
    Returns the unique DBUtils linked to the SparkSession.
    """
    return _spark_manager.dbutils


def reset_spark_manager():
    _spark_manager.reset()



def scope_exists(scope_name: str, key: str) -> bool:
    """
    Checks if a scope and key exist in Databricks.
    """
    try:
        dbutils = get_dbutils()
        dbutils.secrets.get(scope=scope_name, key=key)
        return True
    except Exception as e: # pylint: disable=broad-except
        logger.error("Error checking scope %s and key %s: %s", scope_name, key, e)
        return False

def get_databricks_env() -> str:
    """
    Returns the current Databricks environment. (dev, test, prod)
    If the environment is not defined, returns 'dev' by default.
    """
    if scope_exists(get_databricks_scope_name_from_config_file(DEV), get_databricks_secret_key_from_config_file(DEV)):
        return DEV
    if scope_exists(get_databricks_scope_name_from_config_file(TEST), get_databricks_secret_key_from_config_file(TEST)):
        return TEST
    if scope_exists(get_databricks_scope_name_from_config_file(PROD), get_databricks_secret_key_from_config_file(PROD)):
        return PROD
    return DEV  # Default value if no scope is found
