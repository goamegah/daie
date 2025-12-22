import importlib.util
import os
import json
from typing import Dict, Any, Optional
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark import errors as E
from daie.utils.constants.metadata import RESOURCE_GROUP, TENANT_ID, STORAGE_ACCOUNT, CLIENT_ID, CLIENT_SECRET, SUBSCRIPTION_ID
from daie.definitions import LOCAL_DATABRICKS_CONNECT_CONFIG_FILE

def get_main_env() -> str:
    """
    This function verifies wich environment databricks is in

    Returns:
    str: The environment name, 'dev', 'test' or 'prod'
    """
    if check_if_scope_exists(SCOPE_NAME, get_client_secret_key(env=TEST)):
        verified_env = TEST
    elif check_if_scope_exists(SCOPE_NAME, get_client_secret_key(env=PROD)):
        verified_env = PROD
    else:
        verified_env = DEV
    return verified_env

# Volume folders
KAFKA_FOLDER = "kafka"
CHECKPOINT_FOLDER = "checkpoint"

# Permission groups

DELTA = '`delta`.`'
DELTA_PATH_END = "`"

DEV = 'dev'
TEST = 'test'
PROD = 'prod'

SCOPE_NAME = "dataengineer"

BASE_DIR ="abfss://{container}@{storage_account}.dfs.core.windows.net"

spark = None
dbutils = None

env_config = {
    DEV: {
        CLIENT_ID: "cid-daie-chn-dev",
        CLIENT_SECRET: "cst-daie-chn-dev",
        STORAGE_ACCOUNT: "sta-daie-chn-dev",
        SUBSCRIPTION_ID: "sid-daie-chn-dev",
        TENANT_ID: "tid-daie-chn-dev",
        RESOURCE_GROUP: "rg-daie-chn-dev"
    },
    TEST: {
        CLIENT_ID: "cid-daie-chn-test",
        CLIENT_SECRET: "cst-daie-chn-test",
        STORAGE_ACCOUNT: "sta-daie-chn-test",
        SUBSCRIPTION_ID: "sid-daie-chn-test",
        TENANT_ID: "tid-daie-chn-test",
        RESOURCE_GROUP: "rg-daie-chn-test"
    },
    PROD: {
        CLIENT_ID: "cid-daie-chn-prod",
        CLIENT_SECRET: "cst-daie-chn-prod",
        STORAGE_ACCOUNT: "sta-daie-chn-prod",
        SUBSCRIPTION_ID: "sid-daie-chn-prod",
        TENANT_ID: "tid-daie-chn-prod",
        RESOURCE_GROUP: "rg-daie-chn-prod"
    }
}

def read_json_file_as_dict(file_path: Path) -> Dict[str, Any]:
    """Read a JSON file and return it as a dictionary."""
    with file_path.open('r') as file:
        return json.load(file)
    
def check_if_scope_exists(scope, input_value):
    try:
        dbutils = get_dbutils() #pylint: disable=redefined-outer-name
        dbutils.secrets.get(scope=scope, key=input_value)
        return True
    except: #pylint: disable=bare-except
        return False

def does_path_exist_in_databricks(path):
    try:
        dbutils = get_dbutils() #pylint: disable=redefined-outer-name
        dbutils.fs.ls(path)
        return True
    except: #pylint: disable=bare-except
        return False

def get_base_dir():
    storage_account = get_storage_account(get_main_env())
    return BASE_DIR.format(storage_account=storage_account)

# def get_client_secret(env):
#     return dbutils.secrets.get(scope=SCOPE_NAME, key=get_client_secret_key(env))

def get_client_id(env):
    return dbutils.secrets.get(scope=SCOPE_NAME, key=get_client_id_key(env))

def get_tenant_id(env):
    return dbutils.secrets.get(scope=SCOPE_NAME, key=get_tenant_id_key(env))

def get_subscription_id(env):
    return dbutils.secrets.get(scope=SCOPE_NAME, key=get_subscription_id_key(env))

def get_resource_group(env):
    return dbutils.secrets.get(scope=SCOPE_NAME, key=get_resource_group_key(env))

def get_storage_account(env):
    return dbutils.secrets.get(scope=SCOPE_NAME, key=get_storage_account_key(env))

def get_storage_account_key(env):
    config = env_config.get(env)
    if config is None:
        raise ValueError(f"Environment '{env}' not found in env_config. Available: {list(env_config.keys())}")
    return config.get(STORAGE_ACCOUNT)

def get_client_secret_key(env):
    config = env_config.get(env)
    if config is None:
        raise ValueError(f"Environment '{env}' not found in env_config. Available: {list(env_config.keys())}")
    return config.get(CLIENT_SECRET)

def get_client_id_key(env):
    config = env_config.get(env)
    if config is None:
        raise ValueError(f"Environment '{env}' not found in env_config. Available: {list(env_config.keys())}")
    return config.get(CLIENT_ID)

def get_tenant_id_key(env):
    config = env_config.get(env)
    if config is None:
        raise ValueError(f"Environment '{env}' not found in env_config. Available: {list(env_config.keys())}")
    return config.get(TENANT_ID)

def get_resource_group_key(env):
    config = env_config.get(env)
    if config is None:
        raise ValueError(f"Environment '{env}' not found in env_config. Available: {list(env_config.keys())}")
    return config.get(RESOURCE_GROUP)

def get_subscription_id_key(env):
    config = env_config.get(env)
    if config is None:
        raise ValueError(f"Environment '{env}' not found in env_config. Available: {list(env_config.keys())}")
    return config.get(SUBSCRIPTION_ID)


def get_endpoint(env):
    return f"https://login.microsoftonline.com/{get_tenant_id(env)}/oauth2/token"


# ENDPOINT = f"https://login.microsoftonline.com/{get_tenant_id(DEV)}/oauth2/token"


def get_dbutils():
    global dbutils, spark #pylint: disable=global-statement
    from pyspark.dbutils import DBUtils #pylint: disable=import-error disable=no-name-in-module,import-outside-toplevel
    if not dbutils:
        if not spark:
            spark = get_spark_session()
        dbutils = DBUtils(spark)
    return dbutils


def get_spark_session() -> SparkSession:
    """
    This function configures the spark session to be able to access a specific Azure storage account.
    """
    global spark #pylint: disable=global-statement

    if importlib.util.find_spec("pyspark.dbutils"):
        if os.path.exists(LOCAL_DATABRICKS_CONNECT_CONFIG_FILE):
            # Local spark session for Unity Catalog 'Databricks Connect
            print("\n Bulding Local spark for Unity \n")
            from databricks.sdk.core import Config  #pylint:disable=import-error,disable=import-outside-toplevel
            from databricks.connect import DatabricksSession    #pylint:disable=import-error,disable=import-outside-toplevel,disable=no-name-in-module
            from databricks.sdk import WorkspaceClient  #pylint:disable=import-error,disable=import-outside-toplevel,disable=no-name-in-module
            config_file = read_json_file_as_dict(Path(LOCAL_DATABRICKS_CONNECT_CONFIG_FILE))
            config = Config(
                host       = config_file["host"],
                token      = config_file["token"],
                cluster_id = config_file["cluster_id"]
            )
            spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
            workspace_client = WorkspaceClient(host=config_file["host"], token=config_file["token"])
            dbutils = workspace_client.dbutils
        else:
            # Spark session on Databricks Cluster
            spark = SparkSession.builder.appName("daie").getOrCreate()


        dbutils = get_dbutils() #pylint: disable=redefined-outer-name
        env = get_main_env()
        storage_account = get_storage_account(env)
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", get_client_id(env))
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", dbutils.secrets.get(scope=SCOPE_NAME, key=get_client_secret_key(env)))
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", get_endpoint(env))
    else:
        # Spark session for Unit Tests
        # specific configuration to optimise perfs for unit tests
        spark = (
            SparkSession
                .builder
                .config(
                    'spark.driver.extraJavaOptions',
                    '-Ddelta.log.cacheSize=3 -XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops'
                )
                .config("spark.sql.shuffle.partitions", "1")
                .config('spark.databricks.delta.snapshotPartitions', '2')
                .config('spark.ui.showConsoleProgress', 'false')
                .config('spark.ui.enabled', 'false')
                .config('spark.ui.dagGraph.retainedRootRDDs', '1')
                .config('spark.ui.retainedJobs', '1')
                .config('spark.ui.retainedStages', '1')
                .config('spark.ui.retainedTasks', '1')
                .config('spark.sql.ui.retainedExecutions', '1')
                .config('spark.worker.ui.retainedExecutors', '1')
                .config('spark.worker.ui.retainedDrivers', '1')
                .config('spark.driver.memory', '2g')
                .master('local[1]')
                .appName("daie")
                .getOrCreate()

        )
    return spark

spark: SparkSession = get_spark_session()

def read_delta_table(table_identifier) -> DataFrame:
    """
    The lastest version will be read.
    """
    table_identifier = validate_table_identifier(table_identifier)
    return (
        spark
            .read
            .option("versionAsOf", get_delta_table_lastest_version(table_identifier))
            .format("delta")
            .table(table_identifier)
    )

def dbfs_path_exists(path):
    try:
        get_dbutils().fs.ls(path)
        return True
    except Exception as e:
        # SparkFileNotFoundException is error from dbconnect
        if ('java.io.FileNotFoundException' in str(e)) or ('org.apache.spark.SparkFileNotFoundException' in str(e)):
            return False
        else:
            raise

def get_delta_table_lastest_version(table_identifier):
    """
    Get the latest version of delta table.
    """
    sql_statement = f"DESCRIBE HISTORY {table_identifier}"
    describe_history_df = spark.sql(sql_statement)
    latest_version = describe_history_df.agg(F.max("version")).collect()[0][0]
    return latest_version

def check_if_table_identifier_is_path(table_identifier):
    return ("abfss://" in table_identifier)

def validate_table_identifier(table_identifier):
    is_path = check_if_table_identifier_is_path(table_identifier)
    # Note some functions accept both full_table_name or `delta`.`<path>`
    # Our existing code miss "`delta`" keyword thus we need to ensure to pass `delta``.`<path>`
    if is_path:
        if not (DELTA in table_identifier):
            table_identifier=f"{DELTA}{table_identifier}`"
    return table_identifier


def extract_storage_path(table_identifier:str):
    return table_identifier.split(DELTA)[-1].split(DELTA_PATH_END)[0]

def check_if_table_exists(table_identifier):
    # Note the tableExists function accept both full_table_name or `delta`.`<path>`
    # Our existing code miss "`delta`" keyword thus we need to ensure to pass `delta``.`<path>`
    try:
        return spark.catalog.tableExists(validate_table_identifier(table_identifier))
    except: #pylint: disable=bare-except
        return False


def schema_exists(full_schema_name: str) -> bool:
    """
    Check if a schema exists in the given catalog in Databricks Unity Catalog.
    Returns:
        bool: True if schema exists, False otherwise.
    """
    try:
        spark.sql(f"DESCRIBE SCHEMA {full_schema_name}")
        return True
    except: #pylint: disable=bare-except
        return False


def read_delta_table_if_exists(table_identifier) -> DataFrame:
    return read_delta_table(table_identifier) if check_if_table_exists(table_identifier) else None

def read_delta_table_with_condition(table_identifier, condition) -> DataFrame:
    return read_delta_table(table_identifier).filter(condition) if check_if_table_exists(table_identifier) else None


def write_delta_table(dataframe, table_identifier, mode="append", partitions=[], options:Optional[dict]=None, path = None):
    if check_if_table_identifier_is_path(table_identifier):
        path = extract_storage_path(table_identifier)
        write_delta_table_by_path(dataframe, path, mode, partitions)
    else:
        write_delta_table_by_name(dataframe, table_identifier,mode, partitions, options, path)


def write_delta_stream(new_df, table_identifier:str, partitions):
    if check_if_table_identifier_is_path(table_identifier):
        path=extract_storage_path(table_identifier)
        write_delta_stream_by_path(new_df, path, partitions)
    else:
        checkpoint_path = get_or_create_volume_location(
            sub_folder=f"{KAFKA_FOLDER}/{CHECKPOINT_FOLDER}",
            table_identifier=table_identifier
        )
        write_delta_stream_by_name(new_df, table_identifier, checkpoint_path, partitions)

def get_or_create_volume_location(sub_folder, table_identifier=None, catalog=None, schema=None, entity=None):
    if table_identifier:
        catalog, schema, entity = table_identifier.split(".")
    full_schema_name = f"{catalog}.{schema}"
    spark.sql(f"create schema if not exists {full_schema_name};")
    grant_manage_on_schema_to(full_schema_name) # on raw tables in bronze catalog we can only grant permission to DE/DS
    spark.sql(f"create volume if not exists {catalog}.{schema}.{entity};")

    return f"/Volumes/{catalog}/{schema}/{entity}/{sub_folder}"

def write_delta_table_by_path(dataframe: DataFrame, path, mode="append", partitions = []): #pylint: disable=dangerous-default-value
    """
    This function is used to write DF to delta table directly to the storage, without a need to have a registrated table in
    """
    (
        dataframe
            .write
            .format("delta")
            .mode(mode)
            .partitionBy(partitions)
            .save(path)
    )

def write_delta_table_by_name(dataframe: DataFrame, table_identifier, mode="append", partitions = [], options:Optional[dict]=None, path=None): #pylint: disable=dangerous-default-value
    catalog, schema, _ = table_identifier.split(".")
    data_writer = (
        dataframe
            .write
            .format("delta")
            .mode(mode)
            .partitionBy(partitions)
            # TODO: writing to existing table with partitionBy will be ignored by Databricks.
            # It writes data to proper partitions by using info from table, but does not show partitionBy info in "operation parameters"
    )
    data_writer = data_writer.options(**options) if options else data_writer
    spark.sql(f'create schema if not exists {catalog}.{schema};')
    if path:
        data_writer.saveAsTable(table_identifier, path=path)
    else:
        grant_manage_on_schema_to(f"{catalog}.{schema}") # on raw tables in bronze catalog we can only grant permission to DE/SR
        data_writer.saveAsTable(table_identifier)


def write_delta_stream_by_path(dataframe: DataFrame, path, partitions = []): #pylint: disable=dangerous-default-value
    (
        dataframe
            .writeStream
            .format("delta")
            .option("checkpointLocation", path +'/checkpoint')
            .trigger(once=True)
            .partitionBy(partitions)
            .start(path)
            .awaitTermination()
    )

def write_delta_stream_by_name(dataframe: DataFrame, table_name:str, checkpoint_path:str, partitions = []): #pylint: disable=dangerous-default-value
    (
        dataframe
            .writeStream
            .option("checkpointLocation", checkpoint_path)
            .trigger(once=True)
            .partitionBy(partitions)
            .toTable(table_name)
            .awaitTermination()
            #if you run kafka stream and in next run change environment
            #you should restart cluster as it caches the previous truststore location and will not be able to read the new one.
    )


def grant_manage_on_schema(full_schema_name:str, group_name:str):
    #if we run locally, the table do not need permissions (because we run it not as service principal but as a User)
    if not os.path.exists(LOCAL_DATABRICKS_CONNECT_CONFIG_FILE):
        grant_sql = f"GRANT MANAGE ON SCHEMA {full_schema_name} TO {group_name};"
        spark.sql(grant_sql)

        # to be able to read the data in PROD:
        grant_use_schema = f"""GRANT USE SCHEMA ON SCHEMA {full_schema_name} TO {group_name};"""
        spark.sql(grant_use_schema)
        grant_select_schema = f"""GRANT SELECT ON SCHEMA {full_schema_name} TO {group_name};"""
        spark.sql(grant_select_schema)
        grant_select_schema = f"""GRANT REFRESH ON SCHEMA {full_schema_name} TO {group_name};"""
        spark.sql(grant_select_schema)

def resolve_group(group:str):                                                     #pylint: disable=invalid-name
    dbw_env = get_main_env()
    if group == "DE":
        return f"G-DE-daie-chn-{dbw_env}"
    elif group == "DS":
        return f"G-DS-daie-chn-{dbw_env}"

def grant_manage_on_schema_to(full_schema_name:str, group:str="DS"):                     #pylint: disable=invalid-name
    group_name = resolve_group(group)
    grant_manage_on_schema(full_schema_name, group_name)
