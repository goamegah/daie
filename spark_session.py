import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from constants import *


def get_spark(app_name: str):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("Spark:", spark.version)
    print("Scala:", spark.sparkContext._jvm.scala.util.Properties.versionNumberString())
    return spark
